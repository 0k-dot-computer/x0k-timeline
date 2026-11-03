//! The published binary on its own: `x0k-timeline` under a scratch home takes
//! an OTLP log, returns it from a query, and answers MCP.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing_subscriber::layer::SubscriberExt;
use x0k_timeline_emit::observability::ObservabilityLayer;
use x0k_timeline_emit::span_tracing::{
    run_timeline_forwarder_task, ChannelSpanSink, TimelineEndpoint,
};
use x0k_timeline_emit::timeline_channel::take_timeline_receiver;

const BIN: &str = env!("CARGO_BIN_EXE_x0k-timeline");

struct Service(Child);

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn scratch_env<'a>(command: &'a mut Command, home: &Path) -> &'a mut Command {
    command
        .env_clear()
        .env("HOME", home)
        .env("X0K_HOME", home)
        .env("X0K_PROFILE", "standalone")
}

/// POST `body` to `path` and read the response as text, or `None` while the
/// port is not yet taking connections.
fn post(addr: SocketAddr, path: &str, headers: &str, body: &str) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n{headers}\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).ok()?;
    let mut response = Vec::new();
    // A streamed MCP response stays open, so a read timeout ends the read.
    let _ = stream.read_to_end(&mut response);
    Some(String::from_utf8_lossy(&response).into_owned())
}

fn until<T>(what: &str, mut attempt: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(found) = attempt() {
            return found;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn the_published_binary_serves_otlp_queries_and_mcp_on_a_scratch_home() {
    let home = tempfile::tempdir().unwrap();
    let otlp: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mcp: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let _service = Service(
        scratch_env(&mut Command::new(BIN), home.path())
            .env("X0K_TIMELINE_OTLP_PORT", otlp.port().to_string())
            .env("X0K_TIMELINE_QUERY_MCP_LISTEN", mcp.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );

    let log = r#"{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"standalone"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1793420000000000000","severityText":"INFO","body":{"stringValue":"standalone service record"}}]}]}]}"#;
    let posted = until("the OTLP port", || {
        post(otlp, "/v1/logs", "Content-Type: application/json\r\n", log)
    });
    assert!(posted.starts_with("HTTP/1.1 200"), "{posted}");

    let sql = "SELECT description FROM timeline WHERE entry_kind = 'log'";
    until("the row in a query", || {
        let output = scratch_env(&mut Command::new(BIN), home.path())
            .args(["query", sql, "--base-path"])
            .arg(home.path().join("activities"))
            .output()
            .unwrap();
        String::from_utf8_lossy(&output.stdout)
            .contains("standalone service record")
            .then_some(())
    });

    let initialize = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"standalone-test","version":"0"}}}"#;
    let answered = until("the MCP port", || {
        post(
            mcp,
            "/mcp",
            "Content-Type: application/json\r\nAccept: application/json, text/event-stream\r\n",
            initialize,
        )
    });
    assert!(answered.contains("x0k timeline service"), "{answered}");
}

#[test]
fn describe_timeline_says_what_a_sent_span_carried() {
    let home = tempfile::tempdir().unwrap();
    let otlp: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mcp: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let _service = Service(
        scratch_env(&mut Command::new(BIN), home.path())
            .env("X0K_TIMELINE_OTLP_PORT", otlp.port().to_string())
            .env("X0K_TIMELINE_QUERY_MCP_LISTEN", mcp.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );

    let span = |id: &str, attribute: &str| {
        format!(
            r#"{{"resourceSpans":[{{"resource":{{"attributes":[]}},"scopeSpans":[{{"spans":[{{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"{id}","name":"checkout","startTimeUnixNano":"1700000000000000000","endTimeUnixNano":"1700000001000000000","attributes":[{attribute}]}}]}}]}}]}}"#
        )
    };
    let headers = "Content-Type: application/json\r\n";
    let items = span(
        "eee19b7ec3c1b174",
        r#"{"key":"items","value":{"intValue":"3"}},{"key":"coupon","value":{"stringValue":"SPRING"}}"#,
    );
    let posted = until("the OTLP port", || post(otlp, "/v1/traces", headers, &items));
    assert!(posted.starts_with("HTTP/1.1 200"), "{posted}");
    let many = span("eee19b7ec3c1b175", r#"{"key":"items","value":{"stringValue":"many"}}"#);
    let posted = post(otlp, "/v1/traces", headers, &many).expect("the second span");
    assert!(posted.starts_with("HTTP/1.1 200"), "{posted}");

    let mcp_headers = "Content-Type: application/json\r\nAccept: application/json, text/event-stream\r\n";
    let initialize = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}"#;
    let answered = until("the MCP port", || post(mcp, "/mcp", mcp_headers, initialize));
    let session = answered
        .lines()
        .find_map(|line| line.to_ascii_lowercase().strip_prefix("mcp-session-id:").map(|id| id.trim().to_string()));
    let session_headers = match session {
        Some(id) => format!("{mcp_headers}Mcp-Session-Id: {id}\r\n"),
        None => mcp_headers.to_string(),
    };
    let initialized = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    post(mcp, "/mcp", &session_headers, initialized).expect("initialized");
    let describe = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"describe_timeline","arguments":{"include_examples":false,"family":"checkout"}}}"#;
    let described = until("both spans in the family", || {
        let answer = post(mcp, "/mcp", &session_headers, describe)?;
        answer.contains(r#"\"rows\":2"#).then_some(answer)
    });
    for expected in [r#"\"name\":\"checkout\""#, r#"\"coupon\""#, r#"\"data_type\":\"text\""#, r#"\"conflict\":[\"unsigned\",\"text\"]"#] {
        assert!(described.contains(expected), "{expected} in {described}");
    }
}

#[test]
fn a_process_linking_only_the_emit_crate_writes_across_an_outage() {
    let home = tempfile::tempdir().unwrap();
    let socket = home.path().join("timeline.sock");
    let endpoint = TimelineEndpoint::for_profile(
        socket.display().to_string(),
        &home.path().join("profiles").join("standalone"),
    );

    // The service is down when this process starts, as a daemon's may be at boot.
    let receiver = take_timeline_receiver().expect("the timeline channel");
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(run_timeline_forwarder_task(receiver, endpoint));
    });
    let layer = ObservabilityLayer::builder().with_span_sink(Arc::new(ChannelSpanSink::new())).build();
    let subscriber = Arc::new(tracing_subscriber::registry().with(layer));
    let emit = |name: &'static str| {
        tracing::subscriber::with_default(Arc::clone(&subscriber), || match name {
            "before" => tracing::info_span!("before_the_service_came_up", items = 3u64).in_scope(|| {}),
            _ => tracing::info_span!("after_the_service_came_up").in_scope(|| {}),
        });
    };
    emit("before");
    let profile = home.path().join("profiles").join("standalone");
    let segments = x0k_activity_sink::segment::segment_dir(&profile);
    until("the span in a segment on disk", || {
        let written = |entry: std::fs::DirEntry| entry.metadata().is_ok_and(|file| file.len() > 0);
        let found = std::fs::read_dir(&segments).ok()?.flatten().any(written);
        found.then_some(())
    });

    let otlp: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mcp: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let _service = Service(
        scratch_env(&mut Command::new(BIN), home.path())
            .env("X0K_TIMELINE_SOCKET", &socket)
            .env("X0K_TIMELINE_OTLP_PORT", otlp.port().to_string())
            .env("X0K_TIMELINE_QUERY_MCP_LISTEN", mcp.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );

    let sees = |name: &str| {
        let sql = format!("SELECT span_name FROM timeline WHERE span_name = '{name}'");
        let output = scratch_env(&mut Command::new(BIN), home.path())
            .args(["query", &sql, "--base-path"])
            .arg(home.path().join("activities"))
            .output()
            .unwrap();
        String::from_utf8_lossy(&output.stdout).contains(name).then_some(())
    };
    until("the span from the outage", || sees("before_the_service_came_up"));
    emit("after");
    until("the span sent after it", || sees("after_the_service_came_up"));
}

/// `x0k-timeline run` with this home's socket, so a test can have the service
/// up or down around it.
fn observed(home: &Path, socket: &Path, program: &str) -> std::process::Output {
    scratch_env(&mut Command::new(BIN), home)
        .env("X0K_TIMELINE_SOCKET", socket)
        .args(["run", "--", "/bin/sh", "-c", program])
        .output()
        .expect("run x0k-timeline run")
}

fn start_service(home: &Path, socket: &Path) -> Service {
    let otlp = free_port();
    Service(
        scratch_env(&mut Command::new(BIN), home)
            .env("X0K_TIMELINE_SOCKET", socket)
            .env("X0K_TIMELINE_OTLP_PORT", otlp.to_string())
            .env("X0K_TIMELINE_QUERY_MCP_LISTEN", format!("127.0.0.1:{}", free_port()))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start the service"),
    )
}

/// The rows of this home's store matching `filter`, as JSON text, once any exist.
fn rows(home: &Path, filter: &str) -> String {
    let sql = format!("SELECT event_type, description, fields_json FROM timeline WHERE {filter}");
    until(filter, || {
        let output = scratch_env(&mut Command::new(BIN), home)
            .args(["query", &sql, "--base-path"])
            .arg(home.join("activities"))
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&output.stdout).into_owned();
        (output.status.success() && text.contains("event_type")).then_some(text)
    })
}

/// The attributes of the only row `text` holds.
fn attributes(text: &str) -> serde_json::Value {
    let rows: serde_json::Value = serde_json::from_str(text).expect("rows as JSON");
    let [row] = rows.as_array().expect("an array of rows").as_slice() else {
        panic!("one row, in {text}");
    };
    row["fields_json"].clone()
}

#[test]
fn a_program_under_run_is_recorded_and_keeps_its_exit_code() {
    let home = tempfile::tempdir().unwrap();
    let socket = home.path().join("timeline.sock");
    let _service = start_service(home.path(), &socket);
    until("the service's socket", || socket.exists().then_some(()));

    let output = observed(home.path(), &socket, "echo said-out; echo said-err >&2; exit 3");

    assert_eq!(output.status.code(), Some(3), "run exits as its program did");
    assert!(String::from_utf8_lossy(&output.stdout).contains("said-out"), "stdout still reaches the terminal");
    assert!(String::from_utf8_lossy(&output.stderr).contains("said-err"), "stderr still reaches the terminal");
    let exit = attributes(&rows(home.path(), "event_type = 'process.exit'"));
    assert_eq!(exit["timeline.process.cause"], "exited");
    assert_eq!(exit["process.exit.code"], 3);
    assert_eq!(exit["timeline.process.oom"], "no");
    let spawned = attributes(&rows(home.path(), "event_type = 'process.spawned'"));
    assert!(spawned["process.command"].as_str().unwrap().starts_with("/bin/sh"));
    assert_eq!(exit["process.pid"], spawned["process.pid"], "both name the program");
    let said = attributes(&rows(home.path(), "description = 'said-err'"));
    assert_eq!(said["log.iostream"], "stderr", "a line says which stream it came from");
}

#[test]
fn a_program_that_ran_while_the_service_was_down_is_recorded_when_it_comes_back() {
    let home = tempfile::tempdir().unwrap();
    let socket = home.path().join("timeline.sock");

    let output = observed(home.path(), &socket, "echo alone; exit 4");
    assert_eq!(output.status.code(), Some(4), "the outage costs the program nothing");

    let _service = start_service(home.path(), &socket);
    let exit = attributes(&rows(home.path(), "event_type = 'process.exit'"));
    assert_eq!(exit["process.exit.code"], 4);
    rows(home.path(), "description = 'alone'");
}

#[test]
fn a_killed_program_is_recorded_as_signalled_with_oom_unknown() {
    let home = tempfile::tempdir().unwrap();
    let socket = home.path().join("timeline.sock");
    let _service = start_service(home.path(), &socket);
    until("the service's socket", || socket.exists().then_some(()));

    let output = observed(home.path(), &socket, "kill -9 $$");

    assert_eq!(output.status.code(), Some(137), "128 plus the signal, as a shell reports it");
    let exit = attributes(&rows(home.path(), "event_type = 'process.exit'"));
    assert_eq!(exit["timeline.process.cause"], "signalled");
    assert_eq!(exit["timeline.process.signal"], "SIGKILL");
    assert_eq!(exit["timeline.process.oom"], "unknown", "SIGKILL cannot rule out the OOM killer");
}

#[test]
fn a_program_that_cannot_start_is_named_and_exits_as_a_shell_would() {
    let home = tempfile::tempdir().unwrap();
    let output = scratch_env(&mut Command::new(BIN), home.path())
        .env("X0K_TIMELINE_SOCKET", home.path().join("timeline.sock"))
        .args(["run", "--", "/no/such/program"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(127));
    assert!(String::from_utf8_lossy(&output.stderr).contains("/no/such/program"));
}

/// Uninstalls on the way out, so a failed run leaves no service behind.
struct Installed {
    home: tempfile::TempDir,
    profile: String,
    otlp: u16,
    mcp: u16,
}

impl Installed {
    fn service(&self, verb: &str) -> std::process::Output {
        Command::new(BIN)
            .env("X0K_HOME", self.home.path())
            .env("X0K_PROFILE", &self.profile)
            .env("X0K_TIMELINE_SOCKET", self.home.path().join("timeline.sock"))
            .env("X0K_TIMELINE_OTLP_PORT", self.otlp.to_string())
            .env("X0K_TIMELINE_QUERY_MCP_LISTEN", format!("127.0.0.1:{}", self.mcp))
            .args(["service", verb])
            .output()
            .expect("run x0k-timeline service")
    }

    fn main_pid(&self) -> Option<String> {
        let unit = format!("0k-x0k-timeline-{}", self.profile);
        let output = Command::new("systemctl")
            .args(["--user", "show", "--property=MainPID", "--value", &unit])
            .output()
            .ok()?;
        let pid = String::from_utf8_lossy(&output.stdout).trim().to_string();
        (!pid.is_empty() && pid != "0").then_some(pid)
    }
}

impl Drop for Installed {
    fn drop(&mut self) {
        let _ = self.service("uninstall");
    }
}

#[test]
#[ignore = "installs a real user service; run by hand where systemd --user runs"]
fn an_installed_service_comes_back_from_a_crash_and_uninstalls_clean() {
    let installed = Installed {
        home: tempfile::tempdir().unwrap(),
        profile: format!("probe{}", std::process::id()),
        otlp: free_port(),
        mcp: free_port(),
    };
    let said = |output: &std::process::Output| String::from_utf8_lossy(&output.stdout).into_owned();

    let install = installed.service("install");
    assert!(install.status.success(), "{}", String::from_utf8_lossy(&install.stderr));
    until("the service running", || said(&installed.service("status")).contains("running").then_some(()));
    let first = until("a main pid", || installed.main_pid());

    assert!(Command::new("kill").args(["-9", &first]).status().unwrap().success());
    until("the OS to restart it", || installed.main_pid().filter(|pid| *pid != first));
    until("the service running again", || said(&installed.service("status")).contains("running").then_some(()));

    assert!(installed.service("uninstall").status.success());
    assert!(said(&installed.service("status")).contains("not installed"));
}
