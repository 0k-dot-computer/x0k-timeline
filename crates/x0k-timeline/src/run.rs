//! `x0k-timeline run -- <command>`: record one unmodified program.
//!
//! The program's output still reaches the terminal, and each line is also a
//! record. Its start and its end are process observations
//! (`timeline-public-surface` §8), whose own process is this one, the observer,
//! so a reader can tell what the program said from what was seen happening to
//! it. Records go through the emit crate's forwarder, which keeps them in a
//! segment while the service is down, so a program that dies during an outage
//! is still recorded. It does no restarts: something else decides that.

use std::io::Write as _;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde_json::{json, Map, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use x0k_timeline_emit::span_tracing::{run_timeline_forwarder_task, TimelineEndpoint};
use x0k_timeline_record::process::{self, CAUSE, COMMAND, EXIT_CODE, OOM, PID, SIGNAL};
use x0k_timeline_record::record::{now_micros, LogEventData, TimelineRecord};

/// The row's own process on an observation, as opposed to the subject's.
const OBSERVER: &str = "x0k-timeline run";
/// The record's `target`.
const TARGET: &str = "process";
/// A line past this is cut, so a program that never ends a line cannot grow
/// without bound here.
const MAX_LINE: usize = 64 * 1024;
/// How long a stream may stay open once the program has gone: a daemon's child
/// can hold the pipe forever.
const STREAM_GRACE: Duration = Duration::from_secs(1);
/// How long the forwarder gets to deliver or persist what is left.
const DELIVERY_GRACE: Duration = Duration::from_secs(10);

/// What each line of a program's output is recorded under.
#[derive(Clone)]
struct Subject {
    name: String,
    pid: Option<u32>,
    records: mpsc::Sender<TimelineRecord>,
}

/// Run the program `args` name and answer the exit code to leave with.
pub async fn run_observed(args: &[String]) -> Result<i32> {
    let (name, command) = parse_run_args(args)?;
    let (records, received) = mpsc::channel(10_000);
    let endpoint =
        TimelineEndpoint::for_profile(crate::config::ingest_socket(), &crate::config::state_dir());
    let forwarder = tokio::spawn(run_timeline_forwarder_task(received, endpoint));

    let mut child = match Command::new(&command[0])
        .args(&command[1..])
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            let said = format!("{}: {error}", command[0]);
            let _ = writeln!(std::io::stderr(), "{said}");
            let _ = records.send(log("ERROR", said, OBSERVER, Some(std::process::id()), None)).await;
            drop(records);
            let _ = tokio::time::timeout(DELIVERY_GRACE, forwarder).await;
            // A shell's codes for a command it cannot run.
            return Ok(if error.kind() == std::io::ErrorKind::NotFound { 127 } else { 126 });
        }
    };

    let pid = child.id();
    let spawned = attributes_of(pid, [(COMMAND, json!(command.join(" ")))]);
    let _ = records.send(observation(process::SPAWNED, "INFO", "spawned", spawned)).await;

    let subject = Subject { name, pid, records: records.clone() };
    let streams = [
        child.stdout.take().map(|from| {
            tokio::spawn(copy_lines(from, tokio::io::stdout(), "stdout", subject.clone()))
        }),
        child.stderr.take().map(|from| {
            tokio::spawn(copy_lines(from, tokio::io::stderr(), "stderr", subject.clone()))
        }),
    ];
    drop(subject);

    let status = wait_forwarding_signals(&mut child).await?;
    for stream in streams.into_iter().flatten() {
        let _ = tokio::time::timeout(STREAM_GRACE, stream).await;
    }

    let (ended, code) = how_it_ended(status);
    let mut attributes = attributes_of(pid, []);
    attributes.extend(ended);
    let level = if code == 0 { "INFO" } else { "ERROR" };
    let _ = records.send(observation(process::EXIT, level, "exited", attributes)).await;

    drop(records);
    if tokio::time::timeout(DELIVERY_GRACE, forwarder).await.is_err() {
        let _ = writeln!(std::io::stderr(), "{OBSERVER}: records were still being delivered at exit");
    }
    Ok(code)
}

/// `[--name <name>] [--] <command> [args...]`: the name a program's lines are
/// recorded under, else its file name, and the command.
fn parse_run_args(args: &[String]) -> Result<(String, Vec<String>)> {
    let (name, rest) = match args {
        [flag, name, rest @ ..] if flag == "--name" => (Some(name.clone()), rest),
        rest => (None, rest),
    };
    let command = rest.strip_prefix(&["--".to_string()]).unwrap_or(rest).to_vec();
    let Some(program) = command.first() else {
        return Err(anyhow!("run needs a command: x0k-timeline run [--name <name>] -- <command>"));
    };
    let name = name.unwrap_or_else(|| {
        std::path::Path::new(program)
            .file_name()
            .map_or_else(|| program.clone(), |file| file.to_string_lossy().into_owned())
    });
    Ok((name, command))
}

/// The attributes of an observation of `pid`, plus `more`.
fn attributes_of<const N: usize>(pid: Option<u32>, more: [(&str, Value); N]) -> Map<String, Value> {
    let mut attributes: Map<String, Value> = more.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    attributes.insert(PID.to_string(), json!(pid.unwrap_or_default()));
    attributes
}

/// What the exit status says, as attributes, and the code a shell would report.
fn how_it_ended(status: ExitStatus) -> (Map<String, Value>, i32) {
    if let Some(code) = status.code() {
        // An OOM kill is a signal, so a process that exited was not killed by one.
        let ended = [(CAUSE, json!("exited")), (EXIT_CODE, json!(code)), (OOM, json!("no"))];
        return (ended.into_iter().map(|(k, v)| (k.to_string(), v)).collect(), code);
    }
    #[cfg(unix)]
    if let Some(signal) = std::os::unix::process::ExitStatusExt::signal(&status) {
        // The OOM killer sends SIGKILL, and so can anyone else.
        let oom = if signal == libc::SIGKILL { "unknown" } else { "no" };
        let ended = [(CAUSE, json!("signalled")), (SIGNAL, json!(signal_name(signal))), (OOM, json!(oom))];
        return (ended.into_iter().map(|(k, v)| (k.to_string(), v)).collect(), 128 + signal);
    }
    let ended = [(CAUSE, json!("unknown")), (OOM, json!("unknown"))];
    (ended.into_iter().map(|(k, v)| (k.to_string(), v)).collect(), 1)
}

#[cfg(unix)]
fn signal_name(signal: i32) -> String {
    match signal {
        libc::SIGHUP => "SIGHUP".to_string(),
        libc::SIGINT => "SIGINT".to_string(),
        libc::SIGQUIT => "SIGQUIT".to_string(),
        libc::SIGILL => "SIGILL".to_string(),
        libc::SIGABRT => "SIGABRT".to_string(),
        libc::SIGBUS => "SIGBUS".to_string(),
        libc::SIGFPE => "SIGFPE".to_string(),
        libc::SIGKILL => "SIGKILL".to_string(),
        libc::SIGSEGV => "SIGSEGV".to_string(),
        libc::SIGPIPE => "SIGPIPE".to_string(),
        libc::SIGTERM => "SIGTERM".to_string(),
        other => format!("signal {other}"),
    }
}

/// Wait for the program. SIGTERM and SIGHUP go on to it, so `run` can be stopped
/// like the program it wraps. SIGINT needs no help: a terminal sends it to both,
/// and `run` stays to record how the program ended.
#[cfg(unix)]
async fn wait_forwarding_signals(child: &mut tokio::process::Child) -> Result<ExitStatus> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    let mut hangup = signal(SignalKind::hangup())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    loop {
        let forward = tokio::select! {
            status = child.wait() => return Ok(status?),
            _ = term.recv() => Some(libc::SIGTERM),
            _ = hangup.recv() => Some(libc::SIGHUP),
            _ = interrupt.recv() => None,
        };
        if let (Some(signal), Some(pid)) = (forward, child.id()) {
            // SAFETY: `kill` takes plain integers and touches no memory; `pid`
            // is our own unreaped child, so it cannot name another process.
            unsafe { libc::kill(pid as libc::pid_t, signal) };
        }
    }
}

#[cfg(not(unix))]
async fn wait_forwarding_signals(child: &mut tokio::process::Child) -> Result<ExitStatus> {
    Ok(child.wait().await?)
}

/// Copy `from` to `to` as it arrives, and record each line of it. Bytes are
/// passed on untouched, and a line's record replaces what is not UTF-8.
async fn copy_lines(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    stream: &'static str,
    subject: Subject,
) {
    let mut chunk = [0u8; 8 * 1024];
    let mut line = Vec::new();
    loop {
        let read = match from.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        // A closed terminal must not stop the record of what the program says.
        let _ = to.write_all(&chunk[..read]).await;
        let _ = to.flush().await;
        for byte in &chunk[..read] {
            if *byte != b'\n' {
                line.push(*byte);
            }
            if *byte == b'\n' || line.len() >= MAX_LINE {
                record_line(&mut line, stream, &subject).await;
            }
        }
    }
    if !line.is_empty() {
        record_line(&mut line, stream, &subject).await;
    }
}

async fn record_line(line: &mut Vec<u8>, stream: &'static str, subject: &Subject) {
    let text = String::from_utf8_lossy(line).trim_end_matches('\r').to_string();
    line.clear();
    let attributes = Map::from_iter([("log.iostream".to_string(), json!(stream))]);
    let record = log("INFO", text, &subject.name, subject.pid, Some(attributes));
    let _ = subject.records.send(record).await;
}

fn observation(
    event_type: &str,
    level: &str,
    description: &str,
    attributes: Map<String, Value>,
) -> TimelineRecord {
    let record = log(level, description.to_string(), OBSERVER, Some(std::process::id()), Some(attributes));
    match record {
        TimelineRecord::LogEvent(event) => TimelineRecord::LogEvent(LogEventData {
            event_type: Some(event_type.to_string()),
            ..event
        }),
        other => other,
    }
}

fn log(
    level: &str,
    message: String,
    module: &str,
    pid: Option<u32>,
    attributes: Option<Map<String, Value>>,
) -> TimelineRecord {
    TimelineRecord::LogEvent(LogEventData {
        id: uuid::Uuid::new_v4().to_string(),
        timestamp_us: now_micros(),
        level: level.to_string(),
        message: Some(message),
        fields_json: attributes.map(|fields| Value::Object(fields).to_string()),
        target: TARGET.to_string(),
        software_module_id: Some(module.to_string()),
        pid,
        ..LogEventData::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    fn checked(status: ExitStatus) -> Map<String, Value> {
        let (ended, _) = how_it_ended(status);
        let mut attributes = attributes_of(Some(7), []);
        attributes.extend(ended);
        process::check_observation(process::EXIT, &attributes).expect("a process.exit that fits");
        attributes
    }

    #[test]
    fn every_end_is_an_observation_that_fits() {
        assert_eq!(checked(ExitStatus::from_raw(3 << 8))[EXIT_CODE], 3);
        assert_eq!(checked(ExitStatus::from_raw(libc::SIGKILL))[OOM], "unknown");
        assert_eq!(checked(ExitStatus::from_raw(libc::SIGTERM))[OOM], "no");
    }

    #[test]
    fn a_name_and_the_separator_are_optional() {
        let of = |args: &[&str]| parse_run_args(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>()).unwrap();
        assert_eq!(of(&["--", "/opt/worker", "-l"]).0, "worker");
        assert_eq!(of(&["/opt/worker"]).1, ["/opt/worker"]);
        assert_eq!(of(&["--name", "web", "--", "/opt/worker"]).0, "web");
        assert!(parse_run_args(&[]).is_err());
    }
}
