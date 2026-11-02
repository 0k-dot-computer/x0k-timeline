//! The resident service: the writer, its socket and its OTLP port, started and
//! stopped as one thing. A main binary decides how it logs, where its records
//! come from and how it serves the query tools, then hands the rest to this.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use x0k_activity_sink::{TimelineRecord, WriterPause};
use x0k_timeline_record::pause::PauseTimeline;

use crate::instance::InstanceLock;
use crate::writer::{
    spawn_timeline_pipeline, PipelineConfig, TimelineWriter, TIMELINE_CHANNEL_CAPACITY,
};
use crate::{config, otlp, rules, trace_override_registry};

/// The service's name on its lock file.
const SERVICE_NAME: &str = "timeline-query";

/// How long a restart waits for its predecessor to drain and let go of the lock.
const LOCK_WAIT: Duration = Duration::from_secs(5);

/// How long the flush task gets to drain the timeline channel at shutdown.
const FLUSH_GRACE: Duration = Duration::from_secs(3);

/// Trace-override heartbeat GC: sweep this often, and drop an MCP client's
/// overrides once it has not heartbeated for the threshold (clients beat every
/// 10 s, so a transient socket hiccup does not trip cleanup).
const OVERRIDE_GC_INTERVAL: Duration = Duration::from_secs(15);
const OVERRIDE_GC_DEAD_AFTER: Duration = Duration::from_secs(30);

/// Hold this service to one instance per state directory, or `X0K_LOCK_DIR`.
pub fn instance_lock() -> Result<InstanceLock> {
    let dir = std::env::var_os("X0K_LOCK_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(config::state_dir);
    Ok(InstanceLock::acquire(
        &dir.join(format!("{SERVICE_NAME}.lock")),
        LOCK_WAIT,
    )?)
}

/// What a main binary hands the service.
pub struct ResidentOptions {
    /// The sender the writer feeds, which an embedder may share with its own
    /// tracing layer so the service's records reach its own store.
    pub timeline_tx: mpsc::Sender<TimelineRecord>,
    /// The flush task's end of that channel.
    pub timeline_rx: mpsc::Receiver<TimelineRecord>,
    /// Start paused, for this reason.
    pub paused_at_start: Option<String>,
}

impl ResidentOptions {
    /// A private channel, with [`config::paused_at_start`] deciding the start.
    pub fn standalone() -> Self {
        let (timeline_tx, timeline_rx) = mpsc::channel(TIMELINE_CHANNEL_CAPACITY);
        Self {
            timeline_tx,
            timeline_rx,
            paused_at_start: config::paused_at_start()
                .then(|| "X0K_TIMELINE_PAUSED is set".to_string()),
        }
    }
}

/// The running service.
pub struct Resident {
    shutdown: Arc<Notify>,
    control: TimelineWriter,
    writer: JoinHandle<()>,
    flush: Option<JoinHandle<()>>,
}

impl Resident {
    /// Create the state directory, start the writer and bind its socket and
    /// OTLP port. A socket that cannot bind is an error: a writer that cannot
    /// take anything in should exit so its supervisor restarts it.
    pub async fn start(options: ResidentOptions) -> Result<Self> {
        // Pre-create the store's directory so the first query never races its
        // creation, and a fresh node answers with an empty result.
        let dir = crate::timeline_dir();
        if let Err(e) = std::fs::create_dir_all(dir.join("wal")) {
            warn!(path = %dir.display(), error = %e, "failed to pre-create timeline state dir (continuing)");
        } else {
            info!(path = %dir.display(), "timeline state dir ready");
        }

        let shutdown = Arc::new(Notify::new());
        spawn_signal_handlers(shutdown.clone());

        // The writer: the flush task owns the store, the WAL and segment
        // intake; the socket feeds it what clients forward.
        let pause = Arc::new(WriterPause::default());
        let flush = spawn_timeline_pipeline(
            options.timeline_rx,
            shutdown.clone(),
            pause.clone(),
            PipelineConfig::from_env(),
        )
        .await;

        let (rules_tx, rules_rx) = rules::rules_channel();
        tokio::spawn(rules::run_rules_evaluation_task(
            rules_rx,
            rules::default_rules(),
        ));
        let writer = TimelineWriter::new(options.timeline_tx)
            .with_rules_tap(rules_tx)
            .with_pause(pause);
        if let Some(reason) = options.paused_at_start {
            writer.pause(&PauseTimeline { reason }).await?;
        }
        trace_override_registry::spawn_heartbeat_gc_task(
            writer.trace_overrides().clone(),
            OVERRIDE_GC_INTERVAL,
            OVERRIDE_GC_DEAD_AFTER,
        );

        let socket_path = config::ingest_socket();
        let listener = TimelineWriter::bind(&socket_path)
            .await
            .map_err(|e| anyhow!("timeline socket {socket_path} failed to bind: {e}"))?;
        info!(socket = %socket_path, "timeline writer listening");

        // OTLP over HTTP on localhost, for a process that cannot link the emit
        // role (timeline-public-surface §2). The timeline socket stays the
        // writer's own way in: without the OTLP port the writer still takes
        // every other record, so a port another process holds is logged, not
        // fatal.
        let otlp_addr = otlp::listen_addr(config::otlp_port()?);
        match tokio::net::TcpListener::bind(otlp_addr).await {
            Ok(otlp_listener) => {
                info!(%otlp_addr, "timeline OTLP receiver listening");
                tokio::spawn(otlp::serve(otlp_listener, writer.clone(), shutdown.clone()));
            }
            Err(e) => {
                warn!(event_type = "timeline.otlp.bind_failed", %otlp_addr, error = %e,
                    "the OTLP receiver could not bind; the writer runs without it");
            }
        }
        let control = writer.clone();
        let writer = tokio::spawn(writer.serve(listener, shutdown.clone()));
        Ok(Self {
            shutdown,
            control,
            writer,
            flush,
        })
    }

    /// The writer, for an embedder that pauses and resumes it directly.
    pub fn writer(&self) -> &TimelineWriter {
        &self.control
    }

    /// Wait for SIGINT or SIGTERM.
    pub async fn stopped(&self) {
        self.shutdown.notified().await;
    }

    /// Stop taking records and let the flush task write what it holds.
    pub async fn finish(self) {
        self.shutdown.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(1), self.writer).await;
        if let Some(handle) = self.flush {
            match tokio::time::timeout(FLUSH_GRACE, handle).await {
                Ok(Ok(())) => debug!("timeline flush task completed"),
                Ok(Err(e)) => warn!("timeline flush task panicked: {e}"),
                Err(_) => warn!("timeline flush task did not finish within {FLUSH_GRACE:?}"),
            }
        }
    }
}

/// Notify `shutdown` on SIGINT or SIGTERM (a supervisor stops a service with
/// SIGTERM).
fn spawn_signal_handlers(shutdown: Arc<Notify>) {
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                info!("received SIGINT");
                shutdown.notify_waiters();
                shutdown.notify_one();
            }
        });
    }
    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
                info!("received SIGTERM");
                shutdown.notify_waiters();
                shutdown.notify_one();
            }
            Err(e) => warn!(error = %e, "failed to install SIGTERM handler"),
        }
    });
}
