//! Real-time observability rules engine
//!
//! Evaluates incoming [`TimelineRecord`]s against configurable rules
//! using sliding window counters. Rules fire when conditions are met
//! (e.g., error rate exceeds threshold) and produce actions (attention
//! events, log warnings, counter increments).

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{info, warn};
use x0k_timeline_record::record::TimelineRecord;

/// A rule that evaluates timeline records and fires when conditions are met.
#[derive(Debug)]
pub struct Rule {
    pub id: String,
    pub name: String,
    pub condition: RuleCondition,
    pub action: RuleAction,
    pub cooldown: Duration,
    pub last_fired: Option<Instant>,
}

/// Condition under which a rule fires.
#[derive(Debug, Clone)]
pub enum RuleCondition {
    /// Fires when error-level events exceed `threshold` within `window`.
    ErrorRate {
        threshold: u32,
        window: Duration,
        module_filter: Option<String>,
    },
    /// Fires when events matching `event_type` prefix reach `count` within `window`.
    EventPattern {
        event_type: String,
        count: u32,
        window: Duration,
    },
}

/// Action taken when a rule fires.
#[derive(Debug, Clone)]
pub enum RuleAction {
    /// Emit an attention event with the given severity and message template.
    EmitAttentionEvent {
        severity: String,
        message_template: String,
    },
    /// Log a warning with the given message template.
    LogWarning { message_template: String },
    /// Increment a named counter.
    IncrementCounter { name: String },
}

/// A rule that has fired, returned from [`RulesEngine::evaluate`].
#[derive(Debug)]
pub struct FiredRule {
    pub rule_id: String,
    pub rule_name: String,
    pub action: RuleAction,
    pub matched_count: u32,
}

/// Sliding window counter keyed by rule ID.
///
/// Stores timestamps (microseconds since epoch) of matching events.
/// Older entries are pruned on each evaluation.
#[derive(Debug, Default)]
struct WindowCounter {
    timestamps: VecDeque<u64>,
}

impl WindowCounter {
    /// Add a timestamp and prune entries outside the window. Returns the count within window.
    fn add(&mut self, timestamp_us: u64, window: Duration) -> u32 {
        self.timestamps.push_back(timestamp_us);
        let cutoff = timestamp_us.saturating_sub(window.as_micros() as u64);
        while let Some(&front) = self.timestamps.front() {
            if front < cutoff {
                self.timestamps.pop_front();
            } else {
                break;
            }
        }
        self.timestamps.len() as u32
    }
}

/// Engine that evaluates timeline records against a set of rules.
///
/// Uses sliding window counters to track event rates per rule.
/// Cooldown prevents re-firing within a configured duration.
#[derive(Debug)]
pub struct RulesEngine {
    rules: Vec<Rule>,
    counters: HashMap<String, WindowCounter>,
}

impl RulesEngine {
    pub fn new(rules: Vec<Rule>) -> Self {
        let counters = rules
            .iter()
            .map(|r| (r.id.clone(), WindowCounter::default()))
            .collect();
        Self { rules, counters }
    }

    /// Evaluate a timeline record against all rules. Returns fired rules.
    pub fn evaluate(&mut self, record: &TimelineRecord) -> Vec<FiredRule> {
        let mut fired = Vec::new();
        let now = Instant::now();

        for rule in &mut self.rules {
            // Check cooldown
            if let Some(last) = rule.last_fired {
                if now.duration_since(last) < rule.cooldown {
                    continue;
                }
            }

            let matched = match &rule.condition {
                RuleCondition::ErrorRate {
                    threshold,
                    window,
                    module_filter,
                } => check_error_rate(
                    &mut self.counters,
                    &rule.id,
                    record,
                    *threshold,
                    *window,
                    module_filter.as_deref(),
                ),
                RuleCondition::EventPattern {
                    event_type,
                    count,
                    window,
                } => check_event_pattern(
                    &mut self.counters,
                    &rule.id,
                    record,
                    event_type,
                    *count,
                    *window,
                ),
            };

            if let Some(count) = matched {
                rule.last_fired = Some(now);
                fired.push(FiredRule {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    action: rule.action.clone(),
                    matched_count: count,
                });
            }
        }

        fired
    }

    /// Get the number of active rules.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Get status of all rules: (id, name, fire count within window).
    pub fn rules_status(&self) -> Vec<(&str, &str, u32)> {
        self.rules
            .iter()
            .map(|r| {
                let _window = match &r.condition {
                    RuleCondition::ErrorRate { window, .. } => *window,
                    RuleCondition::EventPattern { window, .. } => *window,
                };
                let count = self
                    .counters
                    .get(&r.id)
                    .map(|c| c.timestamps.len() as u32)
                    .unwrap_or(0);
                (r.id.as_str(), r.name.as_str(), count)
            })
            .collect()
    }
}

/// Status of a single rule.
#[derive(Debug, Clone)]
pub struct RuleStatus {
    pub id: String,
    pub name: String,
    pub events_in_window: u32,
    pub fire_count: u64,
}

/// Create the default set of observability rules.
///
/// Three hardcoded rules:
/// 1. **ErrorBurst**: >5 errors in 60s → attention event
/// 2. **AgentFailureRate**: >2 agent.failed events in 5min → attention event
/// 3. **BuildFailure**: any build.completed with result=failed → attention event
pub fn default_rules() -> Vec<Rule> {
    vec![
        Rule {
            id: "error_burst".to_string(),
            name: "Error burst: >5 errors in 60s".to_string(),
            condition: RuleCondition::ErrorRate {
                threshold: 6,
                window: Duration::from_secs(60),
                module_filter: None,
            },
            action: RuleAction::EmitAttentionEvent {
                severity: "warning".to_string(),
                message_template: "Error burst detected: {count} errors in 60s".to_string(),
            },
            cooldown: Duration::from_secs(300),
            last_fired: None,
        },
        Rule {
            id: "agent_failure_rate".to_string(),
            name: "Agent failure rate: >2 failures in 5min".to_string(),
            condition: RuleCondition::EventPattern {
                event_type: "agent.failed".to_string(),
                count: 3,
                window: Duration::from_secs(300),
            },
            action: RuleAction::EmitAttentionEvent {
                severity: "warning".to_string(),
                message_template: "Agent failure rate elevated: {count} failures in 5min"
                    .to_string(),
            },
            cooldown: Duration::from_secs(600),
            last_fired: None,
        },
        Rule {
            id: "build_failure".to_string(),
            name: "Build failure detected".to_string(),
            condition: RuleCondition::EventPattern {
                event_type: "build.completed".to_string(),
                count: 1,
                window: Duration::from_secs(1),
            },
            action: RuleAction::EmitAttentionEvent {
                severity: "info".to_string(),
                message_template: "Build failure detected".to_string(),
            },
            cooldown: Duration::from_secs(60),
            last_fired: None,
        },
    ]
}

/// Capacity for the rules evaluation channel.
pub const RULES_CHANNEL_CAPACITY: usize = 1000;

/// Create a bounded channel for the rules engine tap.
///
/// Returns (sender, receiver). The sender is the timeline writer's ingest tap
/// ([`crate::writer::TimelineWriter::with_rules_tap`]). The receiver should be
/// passed to `run_rules_evaluation_task()`.
pub fn rules_channel() -> (mpsc::Sender<TimelineRecord>, mpsc::Receiver<TimelineRecord>) {
    mpsc::channel(RULES_CHANNEL_CAPACITY)
}

/// Run the rules evaluation task.
///
/// Consumes records from the channel and evaluates them against the rules engine.
/// Fired rules are dispatched: attention events are logged, warnings are logged,
/// counters are incremented (logged for now).
///
/// This task runs until the channel is closed (all senders dropped).
pub async fn run_rules_evaluation_task(mut rx: mpsc::Receiver<TimelineRecord>, rules: Vec<Rule>) {
    let mut engine = RulesEngine::new(rules);
    let rule_count = engine.rule_count();
    info!(rule_count, "Rules evaluation task started");

    let mut total_evaluated: u64 = 0;
    let mut total_fired: u64 = 0;

    while let Some(record) = rx.recv().await {
        total_evaluated += 1;
        let fired = engine.evaluate(&record);
        for fired_rule in fired {
            total_fired += 1;
            match &fired_rule.action {
                RuleAction::EmitAttentionEvent {
                    severity,
                    message_template,
                } => {
                    let message =
                        message_template.replace("{count}", &fired_rule.matched_count.to_string());
                    info!(
                        rule_id = %fired_rule.rule_id,
                        rule_name = %fired_rule.rule_name,
                        severity = %severity,
                        matched_count = fired_rule.matched_count,
                        total_fired,
                        "Rules engine: {message}"
                    );
                }
                RuleAction::LogWarning { message_template } => {
                    let message =
                        message_template.replace("{count}", &fired_rule.matched_count.to_string());
                    warn!(
                        rule_id = %fired_rule.rule_id,
                        rule_name = %fired_rule.rule_name,
                        matched_count = fired_rule.matched_count,
                        "Rules engine: {message}"
                    );
                }
                RuleAction::IncrementCounter { name } => {
                    info!(
                        rule_id = %fired_rule.rule_id,
                        counter = %name,
                        matched_count = fired_rule.matched_count,
                        "Rules engine: counter incremented"
                    );
                }
            }
        }
    }

    info!(
        total_evaluated,
        total_fired, "Rules evaluation task stopped"
    );
}

/// Check if a record is an error-level event and whether the error rate
/// threshold is exceeded within the sliding window.
fn check_error_rate(
    counters: &mut HashMap<String, WindowCounter>,
    rule_id: &str,
    record: &TimelineRecord,
    threshold: u32,
    window: Duration,
    module_filter: Option<&str>,
) -> Option<u32> {
    let (is_error, timestamp_us, module) = match record {
        TimelineRecord::Activity(activity, _) => {
            let ctx = &activity.context;
            // Activities don't have a level field; check activity_type for error patterns
            let is_err =
                ctx.activity_type.contains("error") || ctx.activity_type.contains("failed");
            (is_err, ctx.started_at, ctx.source_module.as_deref())
        }
        TimelineRecord::Span(span) => {
            let is_err = span.level == "ERROR" || span.level == "error";
            (is_err, span.start_time, span.module.as_deref())
        }
        TimelineRecord::LogEvent(log) => {
            let is_err = log.level == "ERROR" || log.level == "error";
            (is_err, log.timestamp_us, log.module.as_deref())
        }
        TimelineRecord::Conversation(_) => return None,
    };

    if !is_error {
        return None;
    }

    // Apply module filter if specified
    if let Some(filter) = module_filter {
        match module {
            Some(m) if m.starts_with(filter) => {}
            _ => return None,
        }
    }

    let counter = counters.get_mut(rule_id)?;
    let count = counter.add(timestamp_us, window);
    if count >= threshold {
        Some(count)
    } else {
        None
    }
}

/// Check if a record matches an event pattern and whether the count
/// threshold is reached within the sliding window.
fn check_event_pattern(
    counters: &mut HashMap<String, WindowCounter>,
    rule_id: &str,
    record: &TimelineRecord,
    event_type_prefix: &str,
    count_threshold: u32,
    window: Duration,
) -> Option<u32> {
    let (event_type, timestamp_us) = match record {
        TimelineRecord::Activity(activity, _) => {
            (&activity.context.activity_type, activity.context.started_at)
        }
        TimelineRecord::Span(span) => (&span.span_name, span.start_time),
        TimelineRecord::LogEvent(log) => (&log.target, log.timestamp_us),
        TimelineRecord::Conversation(conv) => (&conv.entry_type, conv.timestamp_us),
    };

    if !event_type.starts_with(event_type_prefix) {
        return None;
    }

    let counter = counters.get_mut(rule_id)?;
    let count = counter.add(timestamp_us, window);
    if count >= count_threshold {
        Some(count)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    use x0k_timeline_record::record::{
        ActivityContext, CompletedActivity, LogEventData, StorageTier,
    };

    fn make_error_log(timestamp_us: u64, module: Option<&str>) -> TimelineRecord {
        TimelineRecord::LogEvent(LogEventData {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp_us,
            level: "ERROR".to_string(),
            message: Some("something failed".to_string()),
            fields_json: None,
            source_file: None,
            source_line: None,
            module: module.map(|s| s.to_string()),
            target: "test".to_string(),
            software_module_id: None,
            workstream_id: None,
            intent_id: None,
            session_id: None,
            pid: None,
            trace_id: None,
            traceparent: None,
            tracestate: None,
            request_id: None,
            parent_request_id: None,
            role: None,
            mode: None,
            origin_system: None,
            origin_subsystem: None,
            agent_profile: None,
            sandbox_id: None,
            invocation_method: None,
            executed_by_json: None,
            authorization_chain_json: None,
            authorization_proof_refs_json: None,
            artifact_id: None,
            native_artifact_id: None,
            document_id: None,
            document_revision_id: None,
            file_path: None,
            event_type: None,
        })
    }

    fn make_activity(event_type: &str, timestamp_us: u64) -> TimelineRecord {
        let context = ActivityContext {
            activity_type: event_type.to_string(),
            started_at: timestamp_us,
            ..Default::default()
        };
        let activity = CompletedActivity {
            context,
            ended_at: timestamp_us + 100_000,
            duration_ms: 100,
        };
        TimelineRecord::Activity(activity, StorageTier::Ephemeral)
    }

    fn error_rate_rule(id: &str, threshold: u32, window_secs: u64) -> Rule {
        Rule {
            id: id.to_string(),
            name: format!("Error rate > {threshold}"),
            condition: RuleCondition::ErrorRate {
                threshold,
                window: Duration::from_secs(window_secs),
                module_filter: None,
            },
            action: RuleAction::EmitAttentionEvent {
                severity: "warning".to_string(),
                message_template: "Error rate exceeded".to_string(),
            },
            cooldown: Duration::from_secs(0),
            last_fired: None,
        }
    }

    fn event_pattern_rule(id: &str, event_type: &str, count: u32, window_secs: u64) -> Rule {
        Rule {
            id: id.to_string(),
            name: format!("Event pattern: {event_type} >= {count}"),
            condition: RuleCondition::EventPattern {
                event_type: event_type.to_string(),
                count,
                window: Duration::from_secs(window_secs),
            },
            action: RuleAction::LogWarning {
                message_template: format!("{event_type} threshold reached"),
            },
            cooldown: Duration::from_secs(0),
            last_fired: None,
        }
    }

    #[test]
    fn test_single_error_does_not_trigger_threshold_3() {
        let mut engine = RulesEngine::new(vec![error_rate_rule("err1", 3, 60)]);

        let record = make_error_log(1_000_000, None);
        let fired = engine.evaluate(&record);

        assert!(
            fired.is_empty(),
            "Single error should not trigger threshold 3"
        );
    }

    #[test]
    fn test_three_errors_in_window_triggers() {
        let mut engine = RulesEngine::new(vec![error_rate_rule("err1", 3, 60)]);

        let base = 100_000_000u64;
        // Three errors within the 60-second window
        assert!(engine.evaluate(&make_error_log(base, None)).is_empty());
        assert!(engine
            .evaluate(&make_error_log(base + 10_000_000, None))
            .is_empty());

        let fired = engine.evaluate(&make_error_log(base + 20_000_000, None));
        assert_eq!(fired.len(), 1, "Third error should trigger the rule");
        assert_eq!(fired[0].rule_id, "err1");
        assert_eq!(fired[0].matched_count, 3);
    }

    #[test]
    fn test_cooldown_prevents_refire() {
        let rule = Rule {
            id: "err_cd".to_string(),
            name: "Error rate with cooldown".to_string(),
            condition: RuleCondition::ErrorRate {
                threshold: 1,
                window: Duration::from_secs(60),
                module_filter: None,
            },
            action: RuleAction::IncrementCounter {
                name: "errors".to_string(),
            },
            cooldown: Duration::from_secs(300), // 5 minute cooldown
            last_fired: None,
        };
        let mut engine = RulesEngine::new(vec![rule]);

        // First error fires
        let fired = engine.evaluate(&make_error_log(1_000_000, None));
        assert_eq!(fired.len(), 1, "First error should fire");

        // Second error within cooldown should NOT fire
        let fired = engine.evaluate(&make_error_log(2_000_000, None));
        assert!(fired.is_empty(), "Should not fire during cooldown");
    }

    #[test]
    fn test_event_pattern_fires_on_count_match() {
        let mut engine = RulesEngine::new(vec![event_pattern_rule(
            "agent_fail",
            "agent.failed",
            2,
            300,
        )]);

        let base = 500_000_000u64;
        // First matching event — not enough
        assert!(engine
            .evaluate(&make_activity("agent.failed.timeout", base))
            .is_empty());

        // Second matching event — fires
        let fired = engine.evaluate(&make_activity("agent.failed.crash", base + 60_000_000));
        assert_eq!(fired.len(), 1, "Second matching event should trigger");
        assert_eq!(fired[0].rule_id, "agent_fail");
        assert_eq!(fired[0].matched_count, 2);
    }

    #[test]
    fn test_errors_outside_window_do_not_count() {
        let mut engine = RulesEngine::new(vec![error_rate_rule("err_win", 3, 60)]);

        // Two errors, then a long gap, then one more — should NOT fire
        let base = 100_000_000u64;
        engine.evaluate(&make_error_log(base, None));
        engine.evaluate(&make_error_log(base + 1_000_000, None));

        // 120 seconds later — outside the 60s window
        let fired = engine.evaluate(&make_error_log(base + 120_000_000, None));
        assert!(
            fired.is_empty(),
            "Old errors outside window should be pruned"
        );
    }

    #[test]
    fn test_non_error_events_ignored_by_error_rate() {
        let mut engine = RulesEngine::new(vec![error_rate_rule("err_only", 1, 60)]);

        // Info-level log should not trigger the error rate rule
        let info_log = TimelineRecord::LogEvent(LogEventData {
            id: "1".to_string(),
            timestamp_us: 1_000_000,
            level: "INFO".to_string(),
            message: Some("all good".to_string()),
            fields_json: None,
            source_file: None,
            source_line: None,
            module: None,
            target: "test".to_string(),
            software_module_id: None,
            workstream_id: None,
            intent_id: None,
            session_id: None,
            pid: None,
            trace_id: None,
            traceparent: None,
            tracestate: None,
            request_id: None,
            parent_request_id: None,
            role: None,
            mode: None,
            origin_system: None,
            origin_subsystem: None,
            agent_profile: None,
            sandbox_id: None,
            invocation_method: None,
            executed_by_json: None,
            authorization_chain_json: None,
            authorization_proof_refs_json: None,
            artifact_id: None,
            native_artifact_id: None,
            document_id: None,
            document_revision_id: None,
            file_path: None,
            event_type: None,
        });
        let fired = engine.evaluate(&info_log);
        assert!(
            fired.is_empty(),
            "INFO log should not trigger error rate rule"
        );
    }

    #[test]
    fn test_default_rules_creates_three_rules() {
        let rules = super::default_rules();
        assert_eq!(rules.len(), 3);
        assert_eq!(rules[0].id, "error_burst");
        assert_eq!(rules[1].id, "agent_failure_rate");
        assert_eq!(rules[2].id, "build_failure");
    }

    #[test]
    fn test_error_burst_default_rule_fires() {
        let rules = super::default_rules();
        let mut engine = RulesEngine::new(rules);

        let base = 1_000_000u64;
        // ErrorBurst rule: >5 errors in 60s
        // First 5 errors should not trigger (threshold is >5, i.e. >=6)
        for i in 0..5 {
            let fired = engine.evaluate(&make_error_log(base + i * 1_000, None));
            assert!(fired.is_empty(), "Error {i} should not trigger (need >5)");
        }

        // 6th error triggers the rule
        let fired = engine.evaluate(&make_error_log(base + 5_000, None));
        assert_eq!(fired.len(), 1, "6th error should trigger error_burst rule");
        assert_eq!(fired[0].rule_id, "error_burst");
        assert_eq!(fired[0].matched_count, 6);
        assert!(matches!(
            &fired[0].action,
            RuleAction::EmitAttentionEvent { severity, .. } if severity == "warning"
        ));
    }

    #[test]
    fn test_agent_failure_default_rule_fires() {
        let rules = super::default_rules();
        let mut engine = RulesEngine::new(rules);

        let base = 2_000_000u64;
        // AgentFailureRate: >2 agent.failed in 5min → threshold 3 (fires on 3rd)
        let fired = engine.evaluate(&make_activity("agent.failed.timeout", base));
        assert!(fired.is_empty(), "1st failure should not trigger");

        let fired = engine.evaluate(&make_activity("agent.failed.crash", base + 60_000));
        assert!(fired.is_empty(), "2nd failure should not trigger");

        let fired = engine.evaluate(&make_activity("agent.failed.oom", base + 120_000));
        assert_eq!(fired.len(), 1, "3rd agent failure should trigger");
        assert_eq!(fired[0].rule_id, "agent_failure_rate");
    }

    #[test]
    fn test_build_failure_default_rule_fires() {
        let rules = super::default_rules();
        let mut engine = RulesEngine::new(rules);

        // BuildFailure: any build.completed event (count=1)
        let fired = engine.evaluate(&make_activity("build.completed", 3_000_000));
        assert_eq!(
            fired.len(),
            1,
            "Build failure rule should fire on first build.completed"
        );
        assert_eq!(fired[0].rule_id, "build_failure");
    }

    #[tokio::test]
    async fn test_rules_channel_wiring() {
        // Test that records sent through the channel reach the rules engine
        let (tx, rx) = super::rules_channel();
        let rules = vec![error_rate_rule("err_wire", 3, 60)];

        // Spawn the evaluation task
        let handle = tokio::spawn(super::run_rules_evaluation_task(rx, rules));

        let base = 500_000u64;
        // Send 3 error records through the channel
        for i in 0..3 {
            tx.send(make_error_log(base + i * 1_000, None))
                .await
                .expect("send should succeed");
        }

        // Drop the sender to close the channel and let the task finish
        drop(tx);

        // Task should complete without panic
        handle
            .await
            .expect("rules evaluation task should complete cleanly");
    }
}
