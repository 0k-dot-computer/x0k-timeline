//! Configuration for the activity sink
//!
//! Handles path discovery for the unified timeline storage
//! and span batching configuration.

use anyhow::Result;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

/// Parse an env var as u64, returning the default if unset or unparseable.
fn parse_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    retention: FileRetentionConfig,
}

#[derive(Debug, Default, Deserialize)]
struct FileRetentionConfig {
    span_hours: Option<u64>,
    log_hours: Option<u64>,
    event_hours: Option<u64>,
    record_hours: Option<u64>,
}

fn find_config_dir() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("X0K_CONFIG_DIR") {
        let path = PathBuf::from(dir);
        if path.exists() {
            return Some(path);
        }
    }

    let mut path = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    loop {
        let config_path = path.join("config");
        if config_path.exists()
            && config_path.is_dir()
            && (config_path.join("defaults.toml").exists()
                || config_path.join("x0k-timeline-sink.toml").exists())
        {
            return Some(config_path);
        }

        if !path.pop() {
            break;
        }
    }

    None
}

fn load_retention_file_config() -> FileRetentionConfig {
    let Some(config_dir) = find_config_dir() else {
        return FileRetentionConfig::default();
    };
    let path = config_dir.join("x0k-timeline-sink.toml");
    let Ok(contents) = std::fs::read_to_string(path) else {
        return FileRetentionConfig::default();
    };
    toml::from_str::<FileConfig>(&contents)
        .map(|cfg| cfg.retention)
        .unwrap_or_default()
}

/// Get the x0k home directory (`~/.0k/`).
fn x0k_home_dir() -> PathBuf {
    // Mirror x0k_env::profile::x0k_root() without the dependency (x0k-env
    // sits above this crate): X0K_HOME, then legacy C0K_HOME, then ~/.0k.
    // A bare home-dir join here made a forked stack (data-isolation
    // host-profile lane) write its activity timeline WAL into the LIVE
    // ~/.0k — caught 2026-08-30 by the V2 fd-scope proof.
    for var in ["X0K_HOME", "C0K_HOME"] {
        if let Some(explicit) = std::env::var_os(var) {
            if !explicit.is_empty() {
                return PathBuf::from(explicit);
            }
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".0k")
}

/// Environment variable for span batch size
pub const ENV_SPAN_BATCH_SIZE: &str = "X0K_SPAN_BATCH_SIZE";

/// Environment variable for span batch timeout in milliseconds
pub const ENV_SPAN_BATCH_TIMEOUT_MS: &str = "X0K_SPAN_BATCH_TIMEOUT_MS";

/// Default subdirectory for activity storage under ~/.0k/
pub const DEFAULT_ACTIVITIES_SUBDIR: &str = "activities";

/// Environment variable for flush size in megabytes
pub const ENV_FLUSH_SIZE_MB: &str = "X0K_FLUSH_SIZE_MB";

/// Environment variable for flush interval in seconds
pub const ENV_FLUSH_INTERVAL_SECS: &str = "X0K_FLUSH_INTERVAL_SECS";

/// Environment variable for the maximum age of data waiting for parquet.
pub const ENV_INGEST_LAG_WARN_SECS: &str = "X0K_TIMELINE_INGEST_LAG_WARN_SECS";

/// Environment variable for repeated ingest-lag warning cadence.
pub const ENV_INGEST_LAG_WARN_INTERVAL_SECS: &str = "X0K_TIMELINE_INGEST_LAG_WARN_INTERVAL_SECS";

/// Default flush size: 64 MB
pub const DEFAULT_FLUSH_SIZE_MB: usize = 64;

/// Default flush interval: 60 seconds
pub const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 60;

/// Default ingest lag threshold: five flush intervals.
pub const DEFAULT_INGEST_LAG_WARN_SECS: u64 = 300;

/// Default cadence while ingest remains behind.
pub const DEFAULT_INGEST_LAG_WARN_INTERVAL_SECS: u64 = 300;

/// Default batch size for span processing
pub const DEFAULT_SPAN_BATCH_SIZE: usize = 100;

/// Default batch timeout in milliseconds
pub const DEFAULT_SPAN_BATCH_TIMEOUT_MS: u64 = 25;

/// Configuration for the activity sink
#[derive(Debug, Clone)]
pub struct SinkConfig {
    /// Path to unified activity storage
    pub db_path: PathBuf,
}

impl SinkConfig {
    /// Create configuration from environment variables
    ///
    /// DB path: `~/.0k/activities/`
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            db_path: Self::default_db_path()?,
        })
    }

    /// Get the default DB path
    ///
    /// Returns `~/.0k/activities/`
    pub fn default_db_path() -> Result<PathBuf> {
        Ok(x0k_home_dir().join(DEFAULT_ACTIVITIES_SUBDIR))
    }

    /// Create configuration with explicit path (for testing)
    pub fn with_path(path: PathBuf) -> Self {
        Self { db_path: path }
    }
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self::from_env().unwrap_or_else(|_| {
            // Fallback to current directory if home dir not available
            Self {
                db_path: PathBuf::from(".0k/activities"),
            }
        })
    }
}

/// Configuration for span batching
///
/// Controls how spans are batched before writing to Parquet.
/// Batching reduces lock contention and improves throughput.
#[derive(Debug, Clone)]
pub struct SpanBatchConfig {
    /// Maximum number of spans in a batch before flushing
    ///
    /// Default: 100
    pub batch_size: usize,

    /// Maximum time to wait for a full batch in milliseconds
    ///
    /// If this timeout expires and the batch is non-empty, it will be flushed
    /// even if it hasn't reached `batch_size`.
    ///
    /// Default: 25ms. Override with `X0K_SPAN_BATCH_TIMEOUT_MS` env var.
    pub batch_timeout_ms: u64,
}

impl SpanBatchConfig {
    /// Create configuration from environment variables
    ///
    /// - `X0K_SPAN_BATCH_SIZE`: Maximum spans per batch (default: 100)
    /// - `X0K_SPAN_BATCH_TIMEOUT_MS`: Batch timeout in ms (default: 25)
    pub fn from_env() -> Self {
        let batch_size = std::env::var(ENV_SPAN_BATCH_SIZE)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_SPAN_BATCH_SIZE);

        let batch_timeout_ms = std::env::var(ENV_SPAN_BATCH_TIMEOUT_MS)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_SPAN_BATCH_TIMEOUT_MS);

        Self {
            batch_size,
            batch_timeout_ms,
        }
    }

    /// Create configuration with explicit values (for testing)
    pub fn new(batch_size: usize, batch_timeout_ms: u64) -> Self {
        Self {
            batch_size,
            batch_timeout_ms,
        }
    }
}

impl Default for SpanBatchConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_SPAN_BATCH_SIZE,
            batch_timeout_ms: DEFAULT_SPAN_BATCH_TIMEOUT_MS,
        }
    }
}

/// Default retention for spans: 1 hour
const DEFAULT_SPAN_RETENTION_HOURS: u64 = 1;
/// Default retention for logs: 6 hours
///
/// Deliberately short, and deliberately NOT raised to match the event tier:
/// this class is dominated by chatty `info!` from every subsystem, and a
/// week of it is bytes nobody reads. Rows that must survive an incident are
/// promoted out of this class at emission instead: `activity.type` explicitly
/// requests semantic retention, and every `warn!`/`error!` is promoted while
/// retaining its `event_type` when present. INFO `event_type` rows — including
/// routine `sync.*` telemetry — remain named log rows and are 6h-mortal by
/// policy. See `docs/agent/timeline-debugging.md`.
const DEFAULT_LOG_RETENTION_HOURS: u64 = 6;
/// Default retention for events (activities + conversations): 7 days
const DEFAULT_EVENT_RETENTION_HOURS: u64 = 168;

/// Tiered retention configuration for Parquet timeline files.
///
/// Different entry kinds are retained for different durations:
/// - **spans**: High-volume, low individual value (default: 1h)
/// - **logs**: Freestanding trace/debug/info events without `activity.type`
///   (default: 6h; `event_type` names a row but does not promote it)
/// - **events**: Activities and conversations, high debugging value (default: 7d)
#[derive(Debug, Clone)]
pub struct TieredRetentionConfig {
    pub span_max_age: Duration,
    pub log_max_age: Duration,
    pub event_max_age: Duration,
    /// Maximum age for record-tier files. `None` means never delete (default).
    pub record_max_age: Option<Duration>,
}

impl TieredRetentionConfig {
    /// Load retention config from `config/x0k-activity-sink.toml`, then apply
    /// explicit env var overrides.
    ///
    /// Env vars (hours):
    /// - `X0K_RETENTION_SPAN_HOURS` (default: 1)
    /// - `X0K_RETENTION_LOG_HOURS` (default: 6)
    /// - `X0K_RETENTION_EVENT_HOURS` (default: 168 = 7 days)
    pub fn from_config() -> Self {
        let file = load_retention_file_config();

        let span_hours = parse_env_u64(
            "X0K_RETENTION_SPAN_HOURS",
            file.span_hours.unwrap_or(DEFAULT_SPAN_RETENTION_HOURS),
        );
        let log_hours = parse_env_u64(
            "X0K_RETENTION_LOG_HOURS",
            file.log_hours.unwrap_or(DEFAULT_LOG_RETENTION_HOURS),
        );
        let event_hours = parse_env_u64(
            "X0K_RETENTION_EVENT_HOURS",
            file.event_hours.unwrap_or(DEFAULT_EVENT_RETENTION_HOURS),
        );

        let record_max_age = std::env::var("X0K_RETENTION_RECORD_HOURS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or(file.record_hours)
            .filter(|&h| h > 0)
            .map(|h| Duration::from_secs(h * 3600));

        Self {
            span_max_age: Duration::from_secs(span_hours * 3600),
            log_max_age: Duration::from_secs(log_hours * 3600),
            event_max_age: Duration::from_secs(event_hours * 3600),
            record_max_age,
        }
    }

    /// Build the tier cutoffs array for `retain_tiered`.
    ///
    /// Returns `(prefix, cutoff_micros)` pairs. Legacy files (no prefix) use
    /// the longest retention (event tier).
    pub fn tier_cutoffs(&self, now_micros: u64) -> Vec<(String, u64)> {
        let mut cutoffs = vec![
            (
                "span_".into(),
                now_micros.saturating_sub(self.span_max_age.as_micros() as u64),
            ),
            (
                "log_".into(),
                now_micros.saturating_sub(self.log_max_age.as_micros() as u64),
            ),
            (
                "event_".into(),
                now_micros.saturating_sub(self.event_max_age.as_micros() as u64),
            ),
        ];

        // Only include record_ cutoff if an explicit max age is configured.
        // When None (default), record_ files are omitted from cutoffs and
        // the reaper never touches them — permanent retention.
        if let Some(record_age) = self.record_max_age {
            cutoffs.push((
                "record_".into(),
                now_micros.saturating_sub(record_age.as_micros() as u64),
            ));
        }

        // Legacy files (no prefix) get the longest retention. This fallback
        // must stay last because retain_tiered uses the first matching prefix.
        cutoffs.push((
            String::new(),
            now_micros.saturating_sub(self.event_max_age.as_micros() as u64),
        ));

        cutoffs
    }
}

impl Default for TieredRetentionConfig {
    fn default() -> Self {
        Self {
            span_max_age: Duration::from_secs(DEFAULT_SPAN_RETENTION_HOURS * 3600),
            log_max_age: Duration::from_secs(DEFAULT_LOG_RETENTION_HOURS * 3600),
            event_max_age: Duration::from_secs(DEFAULT_EVENT_RETENTION_HOURS * 3600),
            record_max_age: None,
        }
    }
}

/// Configuration for WAL + sized parquet flushes
///
/// Controls when the memtable is flushed to a parquet file.
#[derive(Debug, Clone)]
pub struct FlushConfig {
    /// Memtable byte threshold before flushing to parquet
    pub flush_size_bytes: usize,
    /// Maximum time before flushing to parquet
    pub flush_interval: Duration,
    /// Whether WAL is enabled (crash recovery)
    pub wal_enabled: bool,
    /// Pending-data age after which ingest emits a durable tripwire
    pub ingest_lag_warn: Duration,
    /// Minimum interval between repeated lag tripwires
    pub ingest_lag_warn_interval: Duration,
    /// The local segment directory whose orphaned segments this writer takes
    /// in (`segment`), or `None` for a writer that takes in none.
    pub segment_dir: Option<std::path::PathBuf>,
}

impl FlushConfig {
    /// Create configuration from environment variables
    ///
    /// - `X0K_FLUSH_SIZE_MB`: Memtable size threshold in MB (default: 64)
    /// - `X0K_FLUSH_INTERVAL_SECS`: Max time before flush in seconds (default: 60)
    /// - `X0K_TIMELINE_INGEST_LAG_WARN_SECS`: Pending age warning threshold
    /// - `X0K_TIMELINE_INGEST_LAG_WARN_INTERVAL_SECS`: Repeated warning cadence
    pub fn from_env() -> Self {
        let size_mb = std::env::var(ENV_FLUSH_SIZE_MB)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_FLUSH_SIZE_MB);

        let interval_secs = std::env::var(ENV_FLUSH_INTERVAL_SECS)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_FLUSH_INTERVAL_SECS);

        let ingest_lag_warn_secs =
            parse_env_u64(ENV_INGEST_LAG_WARN_SECS, DEFAULT_INGEST_LAG_WARN_SECS);
        let ingest_lag_warn_interval_secs = parse_env_u64(
            ENV_INGEST_LAG_WARN_INTERVAL_SECS,
            DEFAULT_INGEST_LAG_WARN_INTERVAL_SECS,
        );

        Self {
            flush_size_bytes: size_mb * 1024 * 1024,
            flush_interval: Duration::from_secs(interval_secs),
            wal_enabled: true,
            ingest_lag_warn: Duration::from_secs(ingest_lag_warn_secs),
            ingest_lag_warn_interval: Duration::from_secs(ingest_lag_warn_interval_secs),
            segment_dir: None,
        }
    }
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            flush_size_bytes: DEFAULT_FLUSH_SIZE_MB * 1024 * 1024,
            flush_interval: Duration::from_secs(DEFAULT_FLUSH_INTERVAL_SECS),
            wal_enabled: true,
            ingest_lag_warn: Duration::from_secs(DEFAULT_INGEST_LAG_WARN_SECS),
            ingest_lag_warn_interval: Duration::from_secs(DEFAULT_INGEST_LAG_WARN_INTERVAL_SECS),
            segment_dir: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static RETENTION_CONFIG_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_default_db_path() {
        let path = SinkConfig::default_db_path().unwrap();
        assert!(path.to_string_lossy().contains(".0k"));
        assert!(path.to_string_lossy().contains("activities"));
    }

    #[test]
    fn test_with_path() {
        let path = PathBuf::from("/test/activities");
        let config = SinkConfig::with_path(path.clone());
        assert_eq!(config.db_path, path);
    }

    #[test]
    fn test_span_batch_config_defaults() {
        let config = SpanBatchConfig::default();
        assert_eq!(config.batch_size, DEFAULT_SPAN_BATCH_SIZE);
        assert_eq!(config.batch_timeout_ms, DEFAULT_SPAN_BATCH_TIMEOUT_MS);
    }

    #[test]
    fn test_span_batch_config_new() {
        let config = SpanBatchConfig::new(50, 100);
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.batch_timeout_ms, 100);
    }

    #[test]
    fn test_span_batch_config_from_env() {
        // Clear env vars first
        std::env::remove_var(ENV_SPAN_BATCH_SIZE);
        std::env::remove_var(ENV_SPAN_BATCH_TIMEOUT_MS);

        // Test defaults
        let config = SpanBatchConfig::from_env();
        assert_eq!(config.batch_size, DEFAULT_SPAN_BATCH_SIZE);
        assert_eq!(config.batch_timeout_ms, DEFAULT_SPAN_BATCH_TIMEOUT_MS);

        // Test custom values
        std::env::set_var(ENV_SPAN_BATCH_SIZE, "200");
        std::env::set_var(ENV_SPAN_BATCH_TIMEOUT_MS, "100");

        let config = SpanBatchConfig::from_env();
        assert_eq!(config.batch_size, 200);
        assert_eq!(config.batch_timeout_ms, 100);

        // Cleanup
        std::env::remove_var(ENV_SPAN_BATCH_SIZE);
        std::env::remove_var(ENV_SPAN_BATCH_TIMEOUT_MS);
    }

    #[test]
    fn test_retention_config_from_file() {
        let _env_guard = RETENTION_CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temp = tempfile::TempDir::new().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(config_dir.join("defaults.toml"), "").unwrap();
        std::fs::write(
            config_dir.join("x0k-timeline-sink.toml"),
            r#"
[retention]
span_hours = 2
log_hours = 12
event_hours = 72
record_hours = 336
"#,
        )
        .unwrap();

        std::env::set_var("X0K_CONFIG_DIR", &config_dir);
        std::env::remove_var("X0K_RETENTION_SPAN_HOURS");
        std::env::remove_var("X0K_RETENTION_LOG_HOURS");
        std::env::remove_var("X0K_RETENTION_EVENT_HOURS");
        std::env::remove_var("X0K_RETENTION_RECORD_HOURS");

        let config = TieredRetentionConfig::from_config();
        assert_eq!(config.span_max_age, Duration::from_secs(2 * 3600));
        assert_eq!(config.log_max_age, Duration::from_secs(12 * 3600));
        assert_eq!(config.event_max_age, Duration::from_secs(72 * 3600));
        assert_eq!(config.record_max_age, Some(Duration::from_secs(336 * 3600)));

        std::env::remove_var("X0K_CONFIG_DIR");
    }

    #[test]
    fn test_retention_env_overrides_file() {
        let _env_guard = RETENTION_CONFIG_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temp = tempfile::TempDir::new().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(config_dir.join("defaults.toml"), "").unwrap();
        std::fs::write(
            config_dir.join("x0k-timeline-sink.toml"),
            r#"
[retention]
span_hours = 2
log_hours = 12
event_hours = 72
"#,
        )
        .unwrap();

        std::env::set_var("X0K_CONFIG_DIR", &config_dir);
        std::env::set_var("X0K_RETENTION_SPAN_HOURS", "4");
        std::env::set_var("X0K_RETENTION_RECORD_HOURS", "24");

        let config = TieredRetentionConfig::from_config();
        assert_eq!(config.span_max_age, Duration::from_secs(4 * 3600));
        assert_eq!(config.log_max_age, Duration::from_secs(12 * 3600));
        assert_eq!(config.event_max_age, Duration::from_secs(72 * 3600));
        assert_eq!(config.record_max_age, Some(Duration::from_secs(24 * 3600)));

        std::env::remove_var("X0K_CONFIG_DIR");
        std::env::remove_var("X0K_RETENTION_SPAN_HOURS");
        std::env::remove_var("X0K_RETENTION_RECORD_HOURS");
    }

    /// The log class stays short on purpose. `activity.type` and severity earn
    /// promotion; an INFO `event_type` remains a named short-lived log row.
    #[test]
    fn log_class_stays_short_because_durability_comes_from_promotion() {
        assert_eq!(DEFAULT_LOG_RETENTION_HOURS, 6);
        const {
            assert!(
                DEFAULT_LOG_RETENTION_HOURS < DEFAULT_EVENT_RETENTION_HOURS,
                "durability is earned by tier promotion, not by inflating the log floor"
            );
        }
    }

    #[test]
    fn test_record_retention_cutoff_precedes_legacy_fallback() {
        let now = 1_800_000_000_000_000u64;
        let config = TieredRetentionConfig {
            span_max_age: Duration::from_secs(3600),
            log_max_age: Duration::from_secs(6 * 3600),
            event_max_age: Duration::from_secs(7 * 24 * 3600),
            record_max_age: Some(Duration::from_secs(24 * 3600)),
        };

        let cutoffs = config.tier_cutoffs(now);
        let record_pos = cutoffs
            .iter()
            .position(|(prefix, _)| prefix == "record_")
            .expect("record cutoff present");
        let fallback_pos = cutoffs
            .iter()
            .position(|(prefix, _)| prefix.is_empty())
            .expect("legacy fallback present");

        assert!(record_pos < fallback_pos);
        assert_eq!(
            cutoffs[record_pos].1,
            now - Duration::from_secs(24 * 3600).as_micros() as u64
        );
    }
}
