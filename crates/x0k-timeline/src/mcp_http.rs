//! The query tools over MCP on Streamable HTTP.

use std::net::SocketAddr;

use axum::serve::ListenerExt;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpService,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::TimelineQueryHandler;

/// Serve the query tools at `/mcp` on `addr` until `shutdown` is cancelled.
pub async fn serve_query_tools(
    addr: SocketAddr,
    shutdown: CancellationToken,
) -> std::io::Result<JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "timeline query MCP listener bound");
    // An HTTP response's headers and body leave as two writes; with Nagle on,
    // the body waits on a delayed ACK.
    let listener = listener.tap_io(|stream| {
        if let Err(error) = stream.set_nodelay(true) {
            warn!(error = %error, "could not set TCP_NODELAY on an accepted connection");
        }
    });
    let handler = TimelineQueryHandler::new();
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        LocalSessionManager::default().into(),
        Default::default(),
    );
    let router = axum::Router::new().nest_service("/mcp", service);
    Ok(tokio::spawn(async move {
        let serving = axum::serve(listener, router)
            .with_graceful_shutdown(async move { shutdown.cancelled().await });
        if let Err(error) = serving.await {
            warn!(error = %error, "the timeline query MCP listener stopped");
        }
    }))
}
