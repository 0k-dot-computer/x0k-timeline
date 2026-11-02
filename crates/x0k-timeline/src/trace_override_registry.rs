//! Canonical registry for runtime trace level overrides.
//!
//! This is the source of truth for trace-level overrides across every x0k
//! process. It lives in the timeline writer because every process that emits
//! spans already maintains an IPC connection to the writer for timeline
//! ingestion, so there's exactly one place every process already talks to.
//!
//! # Scope model
//!
//! Overrides carry an [`OverrideOwner`] that controls cleanup:
//!
//! - [`OverrideOwner::McpClient`] — bound to an MCP server process. Cleaned
//!   up by [`spawn_heartbeat_gc_task`] when the client stops heartbeating.
//! - [`OverrideOwner::Session`] — bound to an x0k agent session id. Cleaned
//!   up by the orchestrator's `SessionRegistry::remove` calling
//!   `dev.clear_overrides_for_session`.
//! - [`OverrideOwner::Timer`] — bound to a wall-clock timer. Cleaned up by a
//!   tokio sleep task spawned inside [`TraceOverrideRegistry::add_duration`].
//!
//! # Composition
//!
//! If multiple overrides target the same module prefix at different levels,
//! the most-verbose wins and all records stay alive. Removing the more-verbose
//! one falls back to the less-verbose; removing the less-verbose one is a
//! no-op for filter decisions until the more-verbose one is removed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info};
use uuid::Uuid;
use x0k_timeline_record::now_micros;
use x0k_timeline_record::overrides::{ActiveOverrideWire, EffectiveOverride, OverrideOwnerWire};

/// Effective ordering of trace levels from most-verbose to least-verbose.
/// Matches `tracing::Level` semantics (TRACE is the most verbose).
const LEVELS_MOST_TO_LEAST_VERBOSE: &[&str] = &["trace", "debug", "info", "warn", "error"];

/// `a` is strictly more verbose than `b`.
/// Returns false when either is unknown or equal.
fn level_more_verbose(a: &str, b: &str) -> bool {
    let rank = |s: &str| LEVELS_MOST_TO_LEAST_VERBOSE.iter().position(|v| *v == s);
    match (rank(a), rank(b)) {
        (Some(ra), Some(rb)) => ra < rb,
        _ => false,
    }
}

/// Who owns an override — drives which cleanup mechanism applies.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum OverrideOwner {
    /// MCP server process, identified by UUID. Cleaned up by heartbeat GC.
    McpClient(Uuid),
    /// x0k agent session id. Cleaned up by orchestrator on session removal.
    Session(String),
    /// Wall-clock timer id. Cleaned up by a tokio sleep task.
    Timer(Uuid),
}

impl OverrideOwner {
    fn to_wire(&self) -> OverrideOwnerWire {
        match self {
            OverrideOwner::McpClient(u) => OverrideOwnerWire::McpClient(u.to_string()),
            OverrideOwner::Session(s) => OverrideOwnerWire::Session(s.clone()),
            OverrideOwner::Timer(u) => OverrideOwnerWire::Timer(u.to_string()),
        }
    }
}

/// A single override entry in the registry.
#[derive(Debug, Clone)]
pub struct ActiveOverride {
    pub id: Uuid,
    pub owner: OverrideOwner,
    pub module_prefix: String,
    pub level: String,
    pub created_at_us: u64,
}

impl ActiveOverride {
    fn to_wire(&self) -> ActiveOverrideWire {
        ActiveOverrideWire {
            id: self.id.to_string(),
            owner: self.owner.to_wire(),
            module_prefix: self.module_prefix.clone(),
            level: self.level.clone(),
            created_at_us: self.created_at_us,
        }
    }
}

/// The canonical trace override registry, owned by the timeline writer.
///
/// Clone is cheap: all internal state is behind `Arc` and plain `Mutex`es.
/// Heartbeats happen at ~1 call per 10s per MCP client so contention is not a
/// concern — a single `Mutex<HashMap>` is plenty.
#[derive(Debug, Clone)]
pub struct TraceOverrideRegistry {
    inner: Arc<RegistryInner>,
}

#[derive(Debug)]
struct RegistryInner {
    /// All live overrides.
    active: Mutex<Vec<ActiveOverride>>,
    /// MCP client last-heartbeat timestamps (microseconds since epoch),
    /// keyed by the same UUID used in `McpClient(uuid)` owners.
    mcp_clients: Mutex<HashMap<Uuid, u64>>,
    /// Monotonic registry version bumped on every mutation.
    version: AtomicU64,
}

impl TraceOverrideRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                active: Mutex::new(Vec::new()),
                mcp_clients: Mutex::new(HashMap::new()),
                version: AtomicU64::new(0),
            }),
        }
    }

    fn bump_version(&self) -> u64 {
        self.inner.version.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Add a new override owned by the given scope.
    ///
    /// Returns the override's stable id.
    pub fn add(&self, owner: OverrideOwner, module_prefix: String, level: String) -> Uuid {
        let id = Uuid::new_v4();
        let entry = ActiveOverride {
            id,
            owner: owner.clone(),
            module_prefix: module_prefix.clone(),
            level: level.clone(),
            created_at_us: now_micros(),
        };
        {
            let mut active = self.inner.active.lock().expect("poisoned");
            active.push(entry);
        }
        let v = self.bump_version();
        debug!(
            %id,
            ?owner,
            module_prefix,
            level,
            version = v,
            "trace override added"
        );
        id
    }

    /// Add an override bound to a `Timer(uuid)` owner that expires after the
    /// given duration. Spawns a tokio task on the current runtime; if no
    /// runtime is available the timer never fires and the override must be
    /// cleared explicitly.
    pub fn add_duration(&self, module_prefix: String, level: String, duration: Duration) -> Uuid {
        let timer_id = Uuid::new_v4();
        let owner = OverrideOwner::Timer(timer_id);
        let override_id = self.add(owner.clone(), module_prefix, level);

        if tokio::runtime::Handle::try_current().is_ok() {
            let registry = self.clone();
            let owner = owner.clone();
            tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                let removed = registry.remove_by_owner(&owner);
                info!(
                    %timer_id,
                    removed,
                    "trace override timer fired"
                );
            });
        }

        override_id
    }

    /// Remove an override by its stable id.
    /// Returns `true` if an entry was actually removed.
    pub fn remove_by_id(&self, id: Uuid) -> bool {
        let removed = {
            let mut active = self.inner.active.lock().expect("poisoned");
            let before = active.len();
            active.retain(|o| o.id != id);
            before != active.len()
        };
        if removed {
            let v = self.bump_version();
            debug!(%id, version = v, "trace override removed by id");
        }
        removed
    }

    /// Remove every override owned by the given scope.
    /// Returns the number of entries removed.
    pub fn remove_by_owner(&self, owner: &OverrideOwner) -> usize {
        let n = {
            let mut active = self.inner.active.lock().expect("poisoned");
            let before = active.len();
            active.retain(|o| &o.owner != owner);
            before - active.len()
        };
        if n > 0 {
            let v = self.bump_version();
            debug!(
                ?owner,
                removed = n,
                version = v,
                "trace overrides removed by owner"
            );
        }
        n
    }

    /// Record a heartbeat from an MCP client. Creates the liveness entry if
    /// this is the first heartbeat from that id.
    pub fn heartbeat_mcp_client(&self, client_id: Uuid) {
        let now = now_micros();
        self.inner
            .mcp_clients
            .lock()
            .expect("poisoned")
            .insert(client_id, now);
    }

    /// Drop any `McpClient(id)` overrides whose last heartbeat is older than
    /// the given threshold. Returns the number of overrides removed.
    ///
    /// Called by [`spawn_heartbeat_gc_task`] on a timer, but exposed as a
    /// public method for tests that want to drive the GC manually.
    pub fn gc_dead_mcp_clients(&self, dead_threshold_us: u64) -> usize {
        let now = now_micros();
        let dead: Vec<Uuid> = {
            let clients = self.inner.mcp_clients.lock().expect("poisoned");
            clients
                .iter()
                .filter(|(_, last)| now.saturating_sub(**last) > dead_threshold_us)
                .map(|(id, _)| *id)
                .collect()
        };
        let mut total_removed = 0usize;
        if !dead.is_empty() {
            let mut clients = self.inner.mcp_clients.lock().expect("poisoned");
            for id in &dead {
                clients.remove(id);
            }
        }
        for id in &dead {
            let owner = OverrideOwner::McpClient(*id);
            total_removed += self.remove_by_owner(&owner);
        }
        if !dead.is_empty() {
            info!(
                clients = dead.len(),
                overrides_removed = total_removed,
                "GC swept dead MCP clients"
            );
        }
        total_removed
    }

    /// Snapshot of every active override, suitable for `list_trace_overrides`.
    pub fn list_active_wire(&self) -> Vec<ActiveOverrideWire> {
        self.inner
            .active
            .lock()
            .expect("poisoned")
            .iter()
            .map(|o| o.to_wire())
            .collect()
    }

    /// Collapse the active set to one effective (prefix, level) pair per
    /// prefix using most-verbose-wins. The returned version is the current
    /// registry version.
    ///
    /// When `since_version == current_version`, the returned result is marked
    /// `unchanged = true` with an empty list so clients can skip the apply
    /// step.
    pub fn effective_set(&self, since_version: u64) -> (u64, bool, Vec<EffectiveOverride>) {
        let current = self.inner.version.load(Ordering::SeqCst);
        if since_version == current {
            return (current, true, Vec::new());
        }
        let active = self.inner.active.lock().expect("poisoned");
        let mut max_level: HashMap<&str, &str> = HashMap::new();
        for o in active.iter() {
            let prefix: &str = &o.module_prefix;
            let level: &str = &o.level;
            let entry = max_level.entry(prefix).or_insert(level);
            if level_more_verbose(level, entry) {
                *entry = level;
            }
        }
        let out: Vec<EffectiveOverride> = max_level
            .into_iter()
            .map(|(prefix, level)| EffectiveOverride {
                module_prefix: prefix.to_string(),
                level: level.to_string(),
            })
            .collect();
        (current, false, out)
    }
}

impl Default for TraceOverrideRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn the heartbeat GC background task.
///
/// Sweeps every `interval` and removes overrides whose MCP client owner
/// hasn't heartbeated in the last `dead_threshold` duration. Runs forever on
/// the current tokio runtime.
pub fn spawn_heartbeat_gc_task(
    registry: TraceOverrideRegistry,
    interval: Duration,
    dead_threshold: Duration,
) {
    if tokio::runtime::Handle::try_current().is_err() {
        tracing::warn!("no tokio runtime available for trace override GC task");
        return;
    }
    let threshold_us = dead_threshold.as_micros() as u64;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            registry.gc_dead_mcp_clients(threshold_us);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace_level() -> String {
        "trace".to_string()
    }
    fn debug_level() -> String {
        "debug".to_string()
    }
    fn info_level() -> String {
        "info".to_string()
    }

    #[test]
    fn add_and_effective_set_single() {
        let reg = TraceOverrideRegistry::new();
        let client = Uuid::new_v4();
        reg.add(
            OverrideOwner::McpClient(client),
            "x0k_agent::sdk_sidecar".to_string(),
            trace_level(),
        );
        let (v, unchanged, effective) = reg.effective_set(0);
        assert_eq!(v, 1);
        assert!(!unchanged);
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].module_prefix, "x0k_agent::sdk_sidecar");
        assert_eq!(effective[0].level, "trace");
    }

    #[test]
    fn effective_set_unchanged_when_version_matches() {
        let reg = TraceOverrideRegistry::new();
        reg.add(
            OverrideOwner::McpClient(Uuid::new_v4()),
            "foo".to_string(),
            debug_level(),
        );
        let (v1, _, _) = reg.effective_set(0);
        let (v2, unchanged, effective) = reg.effective_set(v1);
        assert_eq!(v1, v2);
        assert!(unchanged);
        assert!(effective.is_empty());
    }

    #[test]
    fn most_verbose_wins_on_same_prefix() {
        let reg = TraceOverrideRegistry::new();
        reg.add(
            OverrideOwner::Session("sess-a".to_string()),
            "grove".to_string(),
            debug_level(),
        );
        reg.add(
            OverrideOwner::Session("sess-b".to_string()),
            "grove".to_string(),
            trace_level(),
        );
        let (_, _, effective) = reg.effective_set(0);
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].level, "trace");
    }

    #[test]
    fn removing_less_verbose_is_noop_for_effective_set() {
        let reg = TraceOverrideRegistry::new();
        let a = reg.add(
            OverrideOwner::Session("sess-a".to_string()),
            "grove".to_string(),
            debug_level(),
        );
        reg.add(
            OverrideOwner::Session("sess-b".to_string()),
            "grove".to_string(),
            trace_level(),
        );
        assert!(reg.remove_by_id(a));
        let (_, _, effective) = reg.effective_set(0);
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].level, "trace");
    }

    #[test]
    fn removing_more_verbose_falls_back_to_less_verbose() {
        let reg = TraceOverrideRegistry::new();
        reg.add(
            OverrideOwner::Session("sess-a".to_string()),
            "grove".to_string(),
            debug_level(),
        );
        let b = reg.add(
            OverrideOwner::Session("sess-b".to_string()),
            "grove".to_string(),
            trace_level(),
        );
        assert!(reg.remove_by_id(b));
        let (_, _, effective) = reg.effective_set(0);
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].level, "debug");
    }

    #[test]
    fn remove_by_owner_drops_only_matching() {
        let reg = TraceOverrideRegistry::new();
        let client_a = Uuid::new_v4();
        let client_b = Uuid::new_v4();
        reg.add(
            OverrideOwner::McpClient(client_a),
            "foo".to_string(),
            info_level(),
        );
        reg.add(
            OverrideOwner::McpClient(client_b),
            "bar".to_string(),
            info_level(),
        );
        let removed = reg.remove_by_owner(&OverrideOwner::McpClient(client_a));
        assert_eq!(removed, 1);
        let (_, _, effective) = reg.effective_set(0);
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].module_prefix, "bar");
    }

    #[test]
    fn gc_drops_overrides_for_dead_clients() {
        // We can't easily mock SystemTime::now() so we drive the GC by passing
        // a dead_threshold_us of 0 — meaning "anything not heartbeated this
        // exact microsecond is dead". Since the heartbeat happens before the
        // GC sweep, the timestamp delta is non-zero, so the client is GC'd.
        let reg = TraceOverrideRegistry::new();
        let client = Uuid::new_v4();
        reg.heartbeat_mcp_client(client);
        reg.add(
            OverrideOwner::McpClient(client),
            "foo".to_string(),
            trace_level(),
        );
        // Sleep one millisecond to ensure now > heartbeat_at by at least 1µs.
        std::thread::sleep(Duration::from_millis(1));
        let removed = reg.gc_dead_mcp_clients(0);
        assert_eq!(removed, 1);
        let (_, _, effective) = reg.effective_set(0);
        assert!(effective.is_empty());
    }

    #[test]
    fn gc_keeps_live_clients() {
        // Inverse of above: with an effectively-infinite threshold, no client
        // is ever considered dead, so the GC sweep is a no-op.
        let reg = TraceOverrideRegistry::new();
        let client = Uuid::new_v4();
        reg.heartbeat_mcp_client(client);
        reg.add(
            OverrideOwner::McpClient(client),
            "foo".to_string(),
            trace_level(),
        );
        let removed = reg.gc_dead_mcp_clients(u64::MAX);
        assert_eq!(removed, 0);
        let (_, _, effective) = reg.effective_set(0);
        assert_eq!(effective.len(), 1);
    }

    #[test]
    fn level_more_verbose_ordering() {
        assert!(level_more_verbose("trace", "debug"));
        assert!(level_more_verbose("debug", "info"));
        assert!(level_more_verbose("info", "warn"));
        assert!(level_more_verbose("warn", "error"));
        assert!(!level_more_verbose("info", "debug"));
        assert!(!level_more_verbose("error", "trace"));
        assert!(!level_more_verbose("trace", "trace"));
    }
}
