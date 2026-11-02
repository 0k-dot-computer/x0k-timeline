//! The timeline service on its own: one writer on the timeline socket, OTLP
//! on loopback, and the query tools over MCP. Run without arguments it is the
//! resident service; [`x0k_timeline_query::config`] says what it reads.

use std::time::Duration;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use x0k_timeline_query::resident::{instance_lock, Resident, ResidentOptions};
use x0k_timeline_query::{cli, config, mcp_http};

/// How long open MCP connections get to close at shutdown.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<()> {
    let argv: Vec<String> = std::env::args().collect();
    if let Some(done) = cli::run_gesture("x0k-timeline", &argv).await {
        return done;
    }

    cli::log_to_stderr("info");
    let _lock = instance_lock()?;
    let service = Resident::start(ResidentOptions::standalone()).await?;
    let closing = CancellationToken::new();
    let mcp = mcp_http::serve_query_tools(config::mcp_addr()?, closing.clone()).await?;

    service.stopped().await;
    closing.cancel();
    let _ = tokio::time::timeout(CLOSE_GRACE, mcp).await;
    service.finish().await;
    Ok(())
}
