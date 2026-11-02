//! Periodic poll task that pulls runtime trace level overrides from the
//! timeline writer and applies them to this process's local `SpanFilterState`.
//!
//! Every x0k process that emits spans already maintains an IPC connection to
//! the timeline writer (`x0k-timeline`) for timeline ingestion. This task piggybacks on the same
//! transport: it opens its *own* short-lived connection (independent from the
//! timeline forwarder) and sends a `dev.get_effective_overrides` request
//! every ~2 seconds. When the daemon's effective set changes, this task
//! rebuilds the local `SpanFilterState` from config + overrides and atomically
//! swaps it via `arc_swap::ArcSwap`.
//!
//! # Why polling, not push
//!
//! x0k's IPC transport is request-response only — the daemon never initiates
//! messages to clients. Adding a push channel would require duplexing the
//! protocol layer for marginal latency benefit. Polling at 2s gives us a
//! bounded ~4s end-to-end propagation latency across every x0k process for
//! one small RPC per client, which is more than fast enough for a debugging
//! feature.
//!
//! # Failure modes
//!
//! - **Daemon down**: poll RPCs fail, the task logs at debug, retries on the
//!   next tick. Local filter state is left untouched (whatever the last
//!   successful poll computed).
//! - **Daemon restarted**: comes back with empty registry; next poll observes
//!   `version = 0` and applies the empty set, returning local state to its
//!   TOML baseline.
//! - **Process restarted**: starts polling fresh, picks up whatever overrides
//!   are currently active in the daemon on its first successful tick.
//!
//! All paths are idempotent — the local state is always rebuilt from the
//! authoritative TOML config plus the daemon's authoritative override set.

use crate::span_tracing::config::{parse_level, ModuleRule, SpanFilterState};
use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::Level;
use x0k_ipc_transport::framing;
use x0k_ipc_transport::local_socket::LocalSocketTransport;
use x0k_ipc_transport::{IpcTransport, TransportConfig};
use x0k_timeline_record::overrides::{
    EffectiveOverride, GetEffectiveOverrides, GetEffectiveOverridesResult,
    GET_EFFECTIVE_OVERRIDES_METHOD,
};

/// Wire-compatible envelope types for the daemon IPC.
///
/// These mirror `x0k_transport::socket_transport::{RequestEnvelope, ResponseEnvelope}`
/// exactly. Defined inline here, the same way `auto_persist.rs` does it, to
/// avoid the circular dependency `x0k-timeline-emit → x0k-transport → x0k-env → x0k-timeline-emit`.
#[derive(serde::Serialize, serde::Deserialize)]
struct RequestEnvelope {
    id: u64,
    method: String,
    payload: Vec<u8>,
    traceparent: Option<String>,
    tracestate: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ResponseEnvelope {
    id: u64,
    result: Result<Vec<u8>, String>,
    traceparent: Option<String>,
}

/// Spawn the trace override sync task.
///
/// Polls the timeline writer every 2 seconds for the current effective override
/// set and applies any changes to `filter_state`. Runs forever on the current
/// tokio runtime.
///
/// `socket_path` is the timeline writer's socket. `base_state` builds the
/// filter state the overrides apply on top of, the same state the embedder
/// installed at startup; it is rebuilt on every change so an override that
/// expires leaves that baseline. `on_change` sees each new state before it is
/// stored, so the embedder can let its global tracing filter admit what the
/// state asks for: an event the global filter drops never reaches this
/// crate's layer.
pub fn spawn_override_sync_task(
    filter_state: Arc<ArcSwap<SpanFilterState>>,
    socket_path: String,
    base_state: fn() -> SpanFilterState,
    on_change: fn(&SpanFilterState),
) {
    if tokio::runtime::Handle::try_current().is_err() {
        // No runtime — skip silently. Same fallback as the TOML hot-reload
        // task in lib.rs and the timeline forwarder. Binaries that don't run
        // a tokio runtime won't pick up runtime overrides, which is the same
        // tradeoff every other async-only feature in x0k-env makes.
        return;
    }
    tokio::spawn(async move {
        run_sync_loop(filter_state, socket_path, base_state, on_change).await;
    });
}

async fn run_sync_loop(
    filter_state: Arc<ArcSwap<SpanFilterState>>,
    socket_path: String,
    base_state: fn() -> SpanFilterState,
    on_change: fn(&SpanFilterState),
) {
    let request_id = AtomicU64::new(0);
    let mut last_version: u64 = 0;
    // 2s interval — bounded propagation delay across the system, low traffic.
    let mut ticker = tokio::time::interval(Duration::from_secs(2));
    // Skip the immediate first tick so startup races (init_logging in
    // progress, daemon socket not yet open) don't generate noise.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        match poll_once(&socket_path, last_version, &request_id).await {
            Ok(resp) => {
                if resp.unchanged && resp.version == last_version {
                    continue;
                }
                last_version = resp.version;
                let mut state = base_state();
                apply_effective_overrides(&mut state, &resp.overrides);
                on_change(&state);
                filter_state.store(Arc::new(state));
                tracing::debug!(
                    version = resp.version,
                    count = resp.overrides.len(),
                    "applied trace overrides from the timeline writer"
                );
            }
            Err(e) => {
                tracing::debug!(error = %e, "trace override poll failed");
            }
        }
    }
}

async fn poll_once(
    socket_path: &str,
    since_version: u64,
    request_id: &AtomicU64,
) -> anyhow::Result<GetEffectiveOverridesResult> {
    let config = TransportConfig::LocalSocket {
        path: socket_path.to_string(),
    };
    let conn = LocalSocketTransport.connect(&config).await?;
    let (mut reader, mut writer) = conn.split();

    let req = GetEffectiveOverrides { since_version };
    let payload = x0k_serialization::serialize::to_bytes_unchecked(&req)
        .map_err(|e| anyhow::anyhow!("serialize: {}", e))?;

    let envelope = RequestEnvelope {
        id: request_id.fetch_add(1, Ordering::SeqCst),
        method: GET_EFFECTIVE_OVERRIDES_METHOD.to_string(),
        payload,
        traceparent: None,
        tracestate: None,
    };

    framing::send(&mut writer, &envelope).await?;
    let resp: ResponseEnvelope = framing::recv(&mut reader)
        .await?
        .ok_or_else(|| anyhow::anyhow!("daemon disconnected"))?;

    let result_bytes = resp
        .result
        .map_err(|e| anyhow::anyhow!("daemon error: {}", e))?;
    let parsed: GetEffectiveOverridesResult =
        x0k_serialization::serialize::from_bytes_unchecked(&result_bytes)
            .map_err(|e| anyhow::anyhow!("deserialize: {}", e))?;
    Ok(parsed)
}

/// Layer the daemon's effective override set on top of `state`.
///
/// `state` should be a fresh `build_filter_state_from_config()` (i.e. the
/// TOML baseline). Each override is merged via most-verbose-wins: an override
/// only changes the rule if it's *more* verbose than what's already there.
/// This ensures the configured baseline always wins when it's already strict.
pub(crate) fn apply_effective_overrides(
    state: &mut SpanFilterState,
    overrides: &[EffectiveOverride],
) {
    for o in overrides {
        let level = match parse_level(&o.level) {
            Some(l) => l,
            None => {
                tracing::warn!(level = %o.level, "ignoring trace override with unknown level");
                continue;
            }
        };
        if let Some(rule) = state.rules.iter_mut().find(|r| r.prefix == o.module_prefix) {
            if more_verbose(level, rule.level) {
                rule.level = level;
                rule.enabled = true;
            }
        } else {
            state.rules.push(ModuleRule {
                prefix: o.module_prefix.clone(),
                enabled: true,
                level,
            });
        }
    }
    // Re-sort longest-first so most-specific match still wins. This matches
    // what `build_filter_state_from_config` does for its base set.
    state
        .rules
        .sort_by_key(|rule| std::cmp::Reverse(rule.prefix.len()));
}

/// `a` is strictly more verbose than `b`.
///
/// `tracing::Level`'s `Ord` impl is *inverted* relative to verbosity:
/// `Level::ERROR < Level::WARN < ... < Level::TRACE` (because that's the
/// natural enum discriminant order). "More verbose" therefore means
/// "greater-than" in `Ord` terms — but reading `a > b` at the call site is
/// confusing, so we wrap it in this named helper.
#[inline]
fn more_verbose(a: Level, b: Level) -> bool {
    a > b
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Level;

    fn empty_state() -> SpanFilterState {
        SpanFilterState {
            min_level: Level::INFO,
            rules: Vec::new(),
            exclude_modules: Vec::new(),
            capture_events: true,
        }
    }

    #[test]
    fn apply_effective_overrides_inserts_new_rule() {
        let mut state = empty_state();
        apply_effective_overrides(
            &mut state,
            &[EffectiveOverride {
                module_prefix: "x0k_agent".to_string(),
                level: "debug".to_string(),
            }],
        );
        assert_eq!(state.rules.len(), 1);
        assert_eq!(state.rules[0].prefix, "x0k_agent");
        assert_eq!(state.rules[0].level, Level::DEBUG);
        assert!(state.rules[0].enabled);
    }

    #[test]
    fn apply_effective_overrides_only_strengthens_existing_rule() {
        let mut state = empty_state();
        // Existing rule at TRACE — already strictly more verbose.
        state.rules.push(ModuleRule {
            prefix: "x0k_agent".to_string(),
            enabled: true,
            level: Level::TRACE,
        });
        // Override tries to set DEBUG; it's less verbose, so should be a no-op.
        apply_effective_overrides(
            &mut state,
            &[EffectiveOverride {
                module_prefix: "x0k_agent".to_string(),
                level: "debug".to_string(),
            }],
        );
        assert_eq!(state.rules[0].level, Level::TRACE);
    }

    #[test]
    fn apply_effective_overrides_strengthens_when_override_is_more_verbose() {
        let mut state = empty_state();
        state.rules.push(ModuleRule {
            prefix: "x0k_agent".to_string(),
            enabled: true,
            level: Level::INFO,
        });
        apply_effective_overrides(
            &mut state,
            &[EffectiveOverride {
                module_prefix: "x0k_agent".to_string(),
                level: "trace".to_string(),
            }],
        );
        assert_eq!(state.rules[0].level, Level::TRACE);
        assert!(state.rules[0].enabled);
    }

    #[test]
    fn apply_effective_overrides_skips_unknown_levels() {
        let mut state = empty_state();
        apply_effective_overrides(
            &mut state,
            &[EffectiveOverride {
                module_prefix: "foo".to_string(),
                level: "verbose".to_string(),
            }],
        );
        assert!(state.rules.is_empty());
    }

    #[test]
    fn apply_effective_overrides_keeps_longest_prefix_first() {
        let mut state = empty_state();
        state.rules.push(ModuleRule {
            prefix: "x0k_agent".to_string(),
            enabled: true,
            level: Level::INFO,
        });
        apply_effective_overrides(
            &mut state,
            &[EffectiveOverride {
                module_prefix: "x0k_agent::sdk_sidecar".to_string(),
                level: "trace".to_string(),
            }],
        );
        assert_eq!(state.rules.len(), 2);
        // Most-specific prefix sorted first so should_track picks it up first.
        assert_eq!(state.rules[0].prefix, "x0k_agent::sdk_sidecar");
        assert_eq!(state.rules[0].level, Level::TRACE);
        assert_eq!(state.rules[1].prefix, "x0k_agent");
        assert_eq!(state.rules[1].level, Level::INFO);
    }
}
