//! `x0k-timeline service`: hand the service to the operating system, so
//! something restarts it after a crash or a reboot.
//!
//! The service manager is whatever the machine runs (`service-manager` covers
//! systemd, launchd and the Windows service manager), at user level where it
//! has one. How faithfully a restart policy is kept depends on that manager, and
//! the README says how.

use std::ffi::OsString;
use std::io::Write as _;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use service_manager::{
    RestartPolicy, ServiceInstallCtx, ServiceLabel, ServiceLevel, ServiceManager, ServiceStartCtx,
    ServiceStatus, ServiceStatusCtx, ServiceStopCtx, ServiceUninstallCtx,
};

/// How long a crashed service stays down, so a crash loop does not spin.
const RESTART_DELAY_SECS: u32 = 5;

/// Answer `service <install|uninstall|start|stop|status>`.
pub fn run_service_command(args: &[String]) -> Result<()> {
    let verb = args.first().map(String::as_str).unwrap_or_default();
    let label = service_label(&crate::config::profile())?;
    let manager = user_level_manager()?;
    match verb {
        "install" => {
            let program = std::env::current_exe().context("finding this program")?;
            let environment = service_environment(std::env::vars());
            manager
                .install(install_context(label.clone(), program, environment))
                .with_context(|| format!("installing {label}"))?;
            manager.start(ServiceStartCtx { label: label.clone() }).with_context(|| format!("starting {label}"))?;
            say(&format!("{label} installed and started"))
        }
        "uninstall" => {
            // A service that is already stopped is not a reason to stop here.
            let _ = manager.stop(ServiceStopCtx { label: label.clone() });
            manager.uninstall(ServiceUninstallCtx { label: label.clone() }).with_context(|| format!("uninstalling {label}"))?;
            say(&format!("{label} uninstalled"))
        }
        "start" => {
            manager.start(ServiceStartCtx { label: label.clone() }).with_context(|| format!("starting {label}"))
        }
        "stop" => {
            manager.stop(ServiceStopCtx { label: label.clone() }).with_context(|| format!("stopping {label}"))
        }
        "status" => {
            let status = manager.status(ServiceStatusCtx { label: label.clone() }).with_context(|| format!("reading {label}"))?;
            say(&format!("{label}: {}", status_text(&status)))
        }
        other => Err(anyhow!(
            "service takes install, uninstall, start, stop or status, not {other:?}"
        )),
    }
}

fn say(line: &str) -> Result<()> {
    writeln!(std::io::stdout(), "{line}").context("writing to stdout")
}

fn status_text(status: &ServiceStatus) -> String {
    match status {
        ServiceStatus::NotInstalled => "not installed".to_string(),
        ServiceStatus::Running => "running".to_string(),
        ServiceStatus::Stopped(Some(why)) => format!("stopped: {why}"),
        ServiceStatus::Stopped(None) => "stopped".to_string(),
    }
}

/// The machine's service manager, at user level where it has one. A manager
/// without user services stays at system level, which usually needs an
/// administrator, and says so when it refuses.
fn user_level_manager() -> Result<Box<dyn ServiceManager>> {
    let mut manager = <dyn ServiceManager>::native().context("finding this machine's service manager")?;
    let _ = manager.set_level(ServiceLevel::User);
    if !manager.available().context("asking whether the service manager is available")? {
        return Err(anyhow!("this machine's service manager is not available"));
    }
    Ok(manager)
}

/// One service per profile, as `X0K_PROFILE` gives one service per profile.
fn service_label(profile: &str) -> Result<ServiceLabel> {
    let name = match profile {
        "main" => "computer.0k.x0k-timeline".to_string(),
        other => format!("computer.0k.x0k-timeline-{other}"),
    };
    ServiceLabel::from_str(&name).with_context(|| format!("{name:?} as a service label"))
}

/// What the service reads, so it runs as it would have from this shell.
fn service_environment(variables: impl Iterator<Item = (String, String)>) -> Vec<(String, String)> {
    variables.filter(|(name, _)| name.starts_with("X0K_")).collect()
}

fn install_context(
    label: ServiceLabel,
    program: PathBuf,
    environment: Vec<(String, String)>,
) -> ServiceInstallCtx {
    ServiceInstallCtx {
        label,
        program,
        args: Vec::<OsString>::new(),
        contents: None,
        username: None,
        working_directory: None,
        environment: Some(environment),
        autostart: true,
        restart_policy: RestartPolicy::OnFailure {
            delay_secs: Some(RESTART_DELAY_SECS),
            max_retries: None,
            reset_after_secs: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_profile_is_its_own_service() {
        assert_eq!(service_label("main").unwrap().to_qualified_name(), "computer.0k.x0k-timeline");
        assert_eq!(service_label("work").unwrap().to_qualified_name(), "computer.0k.x0k-timeline-work");
    }

    #[test]
    fn the_service_gets_the_timeline_s_settings_and_nothing_else() {
        let shell = [("X0K_HOME", "/h"), ("PATH", "/b"), ("AWS_SECRET_ACCESS_KEY", "k"), ("X0K_TIMELINE_OTLP_PORT", "1")]
            .map(|(k, v)| (k.to_string(), v.to_string()));
        let kept = service_environment(shell.into_iter());
        assert_eq!(kept.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(), ["X0K_HOME", "X0K_TIMELINE_OTLP_PORT"]);
    }

    #[test]
    fn a_crash_restarts_after_a_delay_and_a_clean_stop_stays_stopped() {
        let context = install_context(service_label("main").unwrap(), PathBuf::from("/opt/x"), Vec::new());
        assert!(matches!(context.restart_policy, RestartPolicy::OnFailure { delay_secs: Some(RESTART_DELAY_SECS), .. }));
        assert!(context.autostart, "a reboot brings it back");
    }
}
