//! What a service binary answers before it starts anything.

use std::io::Write;

use anyhow::Result;

/// A stderr subscriber, filtered by `RUST_LOG`, else by `default_filter`.
pub fn log_to_stderr(default_filter: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .with_writer(std::io::stderr)
        .try_init();
}

/// Answer `argv` if it asks for anything; `None` means no arguments, so the
/// caller is the resident service. A gesture never touches the service's
/// lock: a probe of `--help` must not sit on it and read as a wedged holder.
pub async fn run_gesture(name: &str, argv: &[String]) -> Option<Result<()>> {
    let first = argv.get(1)?;
    if first == "run" {
        // The program's own stderr is passing through, so stay quiet in it.
        log_to_stderr("error");
        return Some(match crate::run::run_observed(&argv[2..]).await {
            Ok(code) => std::process::exit(code),
            Err(error) => Err(error),
        });
    }
    log_to_stderr("warn");
    if first == "service" {
        return Some(crate::service_install::run_service_command(&argv[2..]));
    }
    if first == "query" {
        return Some(run_query_cli(name, &argv[2..]).await);
    }
    let _ = writeln!(
        std::io::stderr(),
        "{name} — the resident timeline service (writer + query).\n\
         \n\
         Run WITHOUT arguments only as the service: it takes the singleton lock\n\
         and binds the timeline socket and the MCP port.\n\
         \n\
         Ad-hoc queries never need the lock:\n\
         \x20 {name} query \"<SELECT ...>\" [--base-path <dir>] [--table]\n\
         \n\
         Record an unmodified program's output and how it ended:\n\
         \x20 {name} run [--name <name>] -- <command> [args...]\n\
         \n\
         Keep the service running under this machine's service manager:\n\
         \x20 {name} service install|uninstall|start|stop|status"
    );
    Some(if matches!(first.as_str(), "-h" | "--help") {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "unexpected argument {first:?}; ad-hoc queries use the `query` subcommand"
        ))
    })
}

/// `<name> query "<SELECT ...>" [--base-path <dir>] [--table]`
///
/// Runs one validated SELECT/WITH against a timeline parquet store (WAL-pending
/// rows included, so recent activity is visible) and prints the rows as JSON
/// (default) or a text table (`--table`). `--base-path <dir>` points at any
/// node's `<dir>/.0k/activities/timeline` — pass the eval node's host-side
/// state dir to read a container daemon's timeline without the MCP surface.
async fn run_query_cli(name: &str, args: &[String]) -> Result<()> {
    let mut sql: Option<String> = None;
    let mut base_path: Option<String> = None;
    let mut table = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--base-path" => {
                base_path = Some(
                    args.get(i + 1)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("--base-path needs a directory"))?,
                );
                i += 2;
            }
            "--table" => {
                table = true;
                i += 1;
            }
            "-h" | "--help" => {
                let _ = writeln!(
                    std::io::stderr(),
                    "usage: {name} query \"<SELECT ...>\" [--base-path <dir>] [--table]\n\
                     \n  <dir> is a node's `.0k/activities` parent; the store is <dir>/timeline.\n\
                       reads WAL-pending rows too, so recent activity is visible."
                );
                return Ok(());
            }
            other if !other.starts_with("--") && sql.is_none() => {
                sql = Some(other.to_string());
                i += 1;
            }
            other => anyhow::bail!("unexpected arg: {other}"),
        }
    }
    let sql = sql.ok_or_else(|| {
        anyhow::anyhow!(
            "missing SQL; usage: {name} query \"<SELECT ...>\" [--base-path <dir>] [--table]"
        )
    })?;
    // `timeline_dir()` reads `X0K_ACTIVITIES_BASE_PATH` → `<base>/timeline`.
    if let Some(base) = base_path {
        std::env::set_var("X0K_ACTIVITIES_BASE_PATH", base);
    }
    crate::validate_select_only(&sql).map_err(|e| anyhow::anyhow!(e))?;
    let engine = crate::TimelineEngine::new();
    let batches = engine.query(&sql).await.map_err(|e| anyhow::anyhow!(e))?;
    if table {
        let rendered = arrow::util::pretty::pretty_format_batches(&batches)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        writeln!(std::io::stdout(), "{rendered}")?;
    } else {
        let (rows, _columns) = crate::batches_to_rows(&batches);
        writeln!(std::io::stdout(), "{}", serde_json::to_string_pretty(&rows)?)?;
    }
    Ok(())
}
