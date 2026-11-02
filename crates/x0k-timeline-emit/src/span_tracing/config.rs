//! Configuration for the span tracing layer
//!
//! Controls which spans and events are captured, filtering by level and module path.

use std::env;
use tracing::Level;

/// Maximum number of in-flight spans to track (prevents memory leaks from unclosed spans)
pub const DEFAULT_MAX_SPANS: usize = 10_000;

/// Configuration for span tracing behavior
#[derive(Debug, Clone)]
pub struct SpanTracingConfig {
    /// Minimum tracing level to capture (default: INFO)
    pub min_level: Level,

    /// Module prefixes to include (empty = use defaults)
    /// Defaults to x0k_*, grove*, dialog_*
    pub include_modules: Vec<String>,

    /// Module prefixes to exclude (high-frequency noise)
    /// Defaults to hyper, tokio, h2, tower, tonic
    pub exclude_modules: Vec<String>,

    /// Whether to capture events attached to spans (default: true)
    pub capture_events: bool,

    /// Track function history with causes (default: true)
    pub track_function_history: bool,

    /// Track source location history with causes (default: true)
    pub track_location_history: bool,

    /// Buffer events within spans instead of writing immediately (default: false)
    ///
    /// When false (default), events inside spans are emitted as freestanding LogEvents
    /// with trace_id from the parent span for correlation. This ensures events are
    /// always visible in the timeline, even inside long-lived spans.
    ///
    /// When true, events are buffered in memory and written as part of the span's
    /// events_json when the span closes. WARNING: events inside spans that never
    /// close (e.g. long-running listener loops) are silently lost.
    pub buffer_events: bool,

    /// Maximum number of in-flight spans to track (default: 10,000)
    /// When exceeded, oldest spans are evicted to prevent memory leaks
    pub max_spans: usize,
}

impl Default for SpanTracingConfig {
    fn default() -> Self {
        Self {
            min_level: Level::INFO,
            include_modules: vec!["x0k_".into(), "grove".into(), "dialog_".into()],
            exclude_modules: vec![
                "hyper".into(),
                "tokio".into(),
                "h2".into(),
                "tower".into(),
                "tonic".into(),
                "mio".into(),
            ],
            capture_events: true,
            track_function_history: true,
            track_location_history: true,
            buffer_events: false,
            max_spans: DEFAULT_MAX_SPANS,
        }
    }
}

impl SpanTracingConfig {
    /// Create configuration from environment variables
    ///
    /// # Environment Variables
    ///
    /// - `X0K_SPAN_TRACING_LEVEL`: Minimum level (trace, debug, info, warn, error)
    /// - `X0K_SPAN_TRACING_INCLUDE`: Comma-separated module prefixes to include
    /// - `X0K_SPAN_TRACING_EXCLUDE`: Comma-separated module prefixes to exclude
    /// - `X0K_SPAN_TRACING_CAPTURE_EVENTS`: "true" or "false"
    /// - `X0K_SPAN_TRACING_TRACK_FUNCTIONS`: "true" or "false"
    /// - `X0K_SPAN_TRACING_TRACK_LOCATIONS`: "true" or "false"
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Parse level
        if let Ok(level_str) = env::var("X0K_SPAN_TRACING_LEVEL") {
            config.min_level = parse_level(&level_str).unwrap_or(Level::INFO);
        }

        // Parse include modules
        if let Ok(includes) = env::var("X0K_SPAN_TRACING_INCLUDE") {
            config.include_modules = includes
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        // Parse exclude modules
        if let Ok(excludes) = env::var("X0K_SPAN_TRACING_EXCLUDE") {
            config.exclude_modules = excludes
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        // Parse flags
        if let Ok(val) = env::var("X0K_SPAN_TRACING_CAPTURE_EVENTS") {
            config.capture_events = val.eq_ignore_ascii_case("true") || val == "1";
        }
        if let Ok(val) = env::var("X0K_SPAN_TRACING_TRACK_FUNCTIONS") {
            config.track_function_history = val.eq_ignore_ascii_case("true") || val == "1";
        }
        if let Ok(val) = env::var("X0K_SPAN_TRACING_TRACK_LOCATIONS") {
            config.track_location_history = val.eq_ignore_ascii_case("true") || val == "1";
        }

        // Parse max spans limit
        if let Ok(val) = env::var("X0K_SPAN_TRACING_MAX_SPANS") {
            if let Ok(max) = val.parse::<usize>() {
                config.max_spans = max;
            }
        }

        config
    }

    /// Check if a span/event should be tracked based on its metadata
    pub fn should_track(&self, metadata: &tracing::Metadata<'_>) -> bool {
        // Check level filter
        if *metadata.level() > self.min_level {
            return false;
        }

        // Get module path
        let module_path = metadata.module_path().unwrap_or("");

        // Check excludes first (higher priority)
        for exclude in &self.exclude_modules {
            if module_path.starts_with(exclude) {
                return false;
            }
        }

        // Check includes (if empty, allow all non-excluded)
        if self.include_modules.is_empty() {
            return true;
        }

        for include in &self.include_modules {
            if module_path.starts_with(include) {
                return true;
            }
        }

        false
    }

    /// Builder-style method to set minimum level
    pub fn with_level(mut self, level: Level) -> Self {
        self.min_level = level;
        self
    }

    /// Builder-style method to add include prefixes
    pub fn with_includes(mut self, includes: Vec<String>) -> Self {
        self.include_modules = includes;
        self
    }

    /// Builder-style method to add exclude prefixes
    pub fn with_excludes(mut self, excludes: Vec<String>) -> Self {
        self.exclude_modules = excludes;
        self
    }

    /// Builder-style method to enable/disable event capture
    pub fn with_events(mut self, capture: bool) -> Self {
        self.capture_events = capture;
        self
    }

    /// Builder-style method to enable/disable function history tracking
    pub fn with_function_history(mut self, track: bool) -> Self {
        self.track_function_history = track;
        self
    }

    /// Builder-style method to enable/disable location history tracking
    pub fn with_location_history(mut self, track: bool) -> Self {
        self.track_location_history = track;
        self
    }

    /// Builder-style method to set maximum in-flight spans
    pub fn with_max_spans(mut self, max: usize) -> Self {
        self.max_spans = max;
        self
    }
}

/// A single module filter rule: prefix → (enabled, level)
#[derive(Debug, Clone)]
pub struct ModuleRule {
    pub prefix: String,
    pub enabled: bool,
    pub level: Level,
}

/// A known top-level module for UI display.
pub struct KnownModule {
    pub prefix: &'static str,
    pub display_name: &'static str,
    pub default_enabled: bool,
}

/// Legacy: Known top-level modules for UI display.
///
/// Previously used by the tree-checkbox module filter and as fallback rules
/// in `build_filter_state_from_config()`. Now only referenced by legacy UI
/// code (params.rs). Will be removed once per-module param defs are gone.
pub fn known_modules() -> Vec<KnownModule> {
    vec![
        KnownModule {
            prefix: "x0k_daemon",
            display_name: "Daemon (IPC, subprocess)",
            default_enabled: false,
        },
        KnownModule {
            prefix: "x0k_agent_core",
            display_name: "Agent Sessions",
            default_enabled: true,
        },
        KnownModule {
            prefix: "x0k_agent",
            display_name: "Agent",
            default_enabled: true,
        },
        KnownModule {
            prefix: "grove",
            display_name: "Grove Service",
            default_enabled: true,
        },
        KnownModule {
            prefix: "x0k_core",
            display_name: "Core (sync, documents)",
            default_enabled: false,
        },
        KnownModule {
            prefix: "x0k_dev_daemon",
            display_name: "Dev Daemon",
            default_enabled: false,
        },
        KnownModule {
            prefix: "x0k_activity_sink",
            display_name: "Activity Sink",
            default_enabled: false,
        },
        KnownModule {
            prefix: "x0k_ui",
            display_name: "UI Framework",
            default_enabled: false,
        },
    ]
}

/// Hot-swappable filter state read by the layer on every span
#[derive(Debug, Clone)]
pub struct SpanFilterState {
    pub min_level: Level,
    /// Sorted longest-prefix-first for most-specific match
    pub rules: Vec<ModuleRule>,
    pub exclude_modules: Vec<String>,
    pub capture_events: bool,
}

impl SpanFilterState {
    /// The filter state the environment alone gives: its level, excludes and
    /// event capture, with no module rules. A layer built without a filter
    /// state uses it; x0k-env layers config rules on the same base.
    pub fn from_env() -> Self {
        let env = SpanTracingConfig::from_env();
        Self {
            min_level: env.min_level,
            rules: Vec::new(),
            exclude_modules: env.exclude_modules,
            capture_events: env.capture_events,
        }
    }

    pub fn should_track(&self, metadata: &tracing::Metadata<'_>) -> bool {
        let module_path = metadata.module_path().unwrap_or("");

        // Check excludes first (highest priority)
        for exclude in &self.exclude_modules {
            if module_path.starts_with(exclude.as_str()) {
                return false;
            }
        }

        // Find most specific matching module rule (rules sorted longest-prefix-first).
        // Per-module level overrides the global min_level so that e.g.
        // `x0k_ui = "trace"` works even when the global level is "info".
        for rule in &self.rules {
            if module_path.starts_with(&rule.prefix) {
                return rule.enabled && *metadata.level() <= rule.level;
            }
        }

        // No module rule matched — fall back to global min_level
        *metadata.level() <= self.min_level
    }
}

/// Parse a level string into a tracing Level
pub fn parse_level(s: &str) -> Option<Level> {
    match s.to_lowercase().as_str() {
        "trace" => Some(Level::TRACE),
        "debug" => Some(Level::DEBUG),
        "info" => Some(Level::INFO),
        "warn" | "warning" => Some(Level::WARN),
        "error" => Some(Level::ERROR),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Level;

    #[test]
    fn test_parse_level() {
        assert_eq!(parse_level("trace"), Some(Level::TRACE));
        assert_eq!(parse_level("DEBUG"), Some(Level::DEBUG));
        assert_eq!(parse_level("Info"), Some(Level::INFO));
        assert_eq!(parse_level("warn"), Some(Level::WARN));
        assert_eq!(parse_level("warning"), Some(Level::WARN));
        assert_eq!(parse_level("error"), Some(Level::ERROR));
        assert_eq!(parse_level("invalid"), None);
    }
}
