use std::{
    env,
    ffi::OsString,
    fmt,
    fs::{File, Metadata},
    io::{Read, Write},
    os::unix::{ffi::OsStrExt as _, fs::MetadataExt as _},
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use rustix::fs::{
    Access, AtFlags, FileType, Mode, OFlags, RenameFlags, accessat, mkdirat, open, openat,
    readlinkat, renameat_with, statat, unlinkat,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::{io::AsyncReadExt as _, process::Command};
use uuid::Uuid;

use super::{
    OnboardingProvider,
    journal::{ActionStatus, InstallAction, Journal, JournalError, Stage},
};
use crate::{hosts, install::INTEGRATIONS_ENV, protocol::WorkspaceName};

const OMP_PACKAGE: &str = "@agent-session-router/omp-integration";
const CLAUDE_MCP: &str = "agent-session-router-channel";
const CODEX_MCP: &str = "agent_session_router";
const MAX_BYTES: usize = 1024 * 1024;
const CLI_TIMEOUT: Duration = Duration::from_secs(30);
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOFOLLOW);
const FILE_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK);

/// Provider output is never retained in an error, including its Debug representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{code}")]
pub struct ProviderInstallError {
    code: &'static str,
    action: &'static str,
}

impl ProviderInstallError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub const fn next_action(&self) -> &str {
        self.action
    }
}

const CONFLICT: ProviderInstallError = ProviderInstallError {
    code: "provider_configuration_conflict",
    action: "Inspect the existing ASR skill, plugin and MCP definitions with the provider's native commands. Keep or explicitly remove the conflicting user configuration, then resume this invitation; ASR does not overwrite it.",
};
const PERMISSION: ProviderInstallError = ProviderInstallError {
    code: "host_permission_required",
    action: "Allow shell execution through your provider's normal approval flow. Ensure your own configuration directories are writable, not symlinked or group/world-writable; ASR private directories/files must be 0700/0600. Do not use sudo.",
};
const IO_ERROR: ProviderInstallError = ProviderInstallError {
    code: "provider_installation_io",
    action: "Restore access to the installed ASR distribution and your configuration directory, then resume this invitation. Do not replace or delete the pending journal.",
};
const COMMAND_FAILED: ProviderInstallError = ProviderInstallError {
    code: "host_command_failed",
    action: "Check the provider's native plugin/MCP commands and normal execution permissions, then resume this invitation. The recorded intent is preserved and ASR will inspect the registry before retrying.",
};
const OMP_DISABLED: ProviderInstallError = ProviderInstallError {
    code: "plugin_disabled",
    action: "If you want this integration enabled, run `omp plugin enable @agent-session-router/omp-integration --scope user`, then resume this invitation and restart OMP.",
};
const CLAUDE_DISABLED: ProviderInstallError = ProviderInstallError {
    code: "plugin_disabled",
    action: "If you want this integration enabled, run `claude plugin enable asr@asr-local --scope user`, then resume this invitation and restart Claude Code.",
};

fn requirement(provider: OnboardingProvider) -> ProviderInstallError {
    ProviderInstallError {
        code: "host_upgrade_required",
        action: match provider {
            OnboardingProvider::ClaudeCode => {
                "Install or upgrade Claude Code through its official instructions. This installer requires user-scoped local plugin marketplaces, plugin install/list --json, marketplace list --json, and stdio mcp add/get. Then resume this invitation."
            }
            OnboardingProvider::CodexCli => {
                "Install or upgrade Codex CLI through its official instructions. This installer requires mcp add and mcp get/list --json with stdio transport. Then resume this invitation."
            }
            OnboardingProvider::Omp => {
                "Install or upgrade Oh My Pi through its official instructions. This installer requires plugin list --json, user-scoped plugin link/enable, and packaged skill discovery. Then resume this invitation."
            }
        },
    }
}

fn journal_error(error: JournalError) -> ProviderInstallError {
    match error {
        JournalError::Permissions => PERMISSION,
        JournalError::Conflict | JournalError::Invalid => CONFLICT,
        _ => IO_ERROR,
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RecordedFile {
    path: PathBuf,
    sha256: String,
    private: bool,
}

impl RecordedFile {
    fn intent(&self) -> InstallAction {
        InstallAction::File {
            path: self.path.clone(),
            sha256: self.sha256.clone(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Ownership {
    version: u8,
    invite_id: Uuid,
    enrollment_id: Uuid,
    provider: OnboardingProvider,
    server_id: Uuid,
    profile: String,
    workspace: WorkspaceName,
    manifest_sha256: String,
    asr_version: String,
    executable: PathBuf,
    executable_sha256: String,
    integrations_dir: PathBuf,
    files: Vec<RecordedFile>,
    commands: Vec<Vec<String>>,
}

struct FileInstall {
    record: RecordedFile,
    bytes: Vec<u8>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Step {
    ClaudeMarketplace,
    ClaudePlugin,
    ClaudeMcp,
    CodexMcp,
    OmpLink,
    OmpEnable,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Registered {
    Absent,
    Exact,
    Disabled,
}

/// Holds only preflighted paths and public configuration; never invitation/credential tokens.
pub struct ProviderInstaller {
    config_path: PathBuf,
    private_root: PathBuf,
    integrations_dir: PathBuf,
    home: PathBuf,
    program: PathBuf,
    provider: OnboardingProvider,
    executable: PathBuf,
    invite_id: Uuid,
    enrollment_id: Uuid,
    executable_sha256: String,
    profile: String,
    version: String,
    files: Vec<FileInstall>,
    steps: Vec<Step>,
    native_destinations: Vec<PathBuf>,
    omp_link_destination: Option<PathBuf>,
}

impl fmt::Debug for ProviderInstaller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderInstaller")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

/// Does not publish any managed file, registration or journal intent. The caller holds
/// `ConfigurationLock`, and must complete this preflight before exchanging an invitation.
pub async fn preflight(
    config_path: &Path,
    journal: &Journal,
    integrations_dir: &Path,
) -> Result<ProviderInstaller, ProviderInstallError> {
    let state = journal.state();
    let config_path = absolute(config_path)?;
    let private_root = config_path.parent().ok_or(PERMISSION)?.to_owned();
    let home = environment_path("HOME", None)?;
    let integrations_dir = absolute(integrations_dir)?;
    let executable = absolute(&state.executable)?;
    let program = resolve_program(state.provider)?;
    let executable_sha256 = hash_file(&executable, None)?.ok_or(IO_ERROR)?;
    let mut installer = ProviderInstaller {
        config_path,
        private_root,
        integrations_dir,
        home,
        program,
        provider: state.provider,
        executable,
        invite_id: state.invite_id,
        enrollment_id: state.enrollment_id,
        executable_sha256,
        profile: state.profile_name.clone(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        files: Vec::new(),
        steps: Vec::new(),
        native_destinations: Vec::new(),
        omp_link_destination: None,
    };
    installer.prepare_layout(journal)?;
    installer.check_paths(journal)?;
    installer.check_features().await?;
    installer.check_registrations(journal).await?;
    if installer.provider == OnboardingProvider::Omp {
        // Reuse the host's checked link planner, including its packaged-path/legacy
        // collision checks. A pending own link is the only disabled recovery case.
        let environment = vec![(
            OsString::from(INTEGRATIONS_ENV),
            installer.integrations_dir.as_os_str().to_owned(),
        )];
        match hosts::setup_omp_checked_plan(
            installer.program.as_os_str().to_owned(),
            installer.home.clone(),
            &installer.executable,
            &environment,
        )
        .await
        {
            Ok(_) => {}
            Err(hosts::HostError::OmpEnableRequired) if installer.pending_link(journal) => {}
            Err(hosts::HostError::OmpEnableRequired) => return Err(OMP_DISABLED),
            Err(hosts::HostError::OmpSetupConflict) => return Err(CONFLICT),
            Err(_) => return Err(requirement(installer.provider)),
        }
    }
    installer.check_omp_link_destination(journal)?;
    Ok(installer)
}

impl ProviderInstaller {
    fn prepare_layout(&mut self, journal: &Journal) -> Result<(), ProviderInstallError> {
        match self.provider {
            OnboardingProvider::ClaudeCode => {
                let root = self.integrations_dir.join("claude-plugin");
                let marketplace = required_json(&root.join(".claude-plugin/marketplace.json"))?;
                let plugin = required_json(&root.join("plugins/asr/.claude-plugin/plugin.json"))?;
                if marketplace["name"] != "asr-local"
                    || marketplace["owner"]["name"] != "Agent Session Router"
                    || marketplace["plugins"]
                        != serde_json::json!([{
                            "name": "asr", "source": "./plugins/asr"
                        }])
                    || plugin["name"] != "asr"
                    || plugin["version"] != self.version
                    || plugin.get("mcpServers").is_some()
                {
                    return Err(CONFLICT);
                }
                if read_file(&root.join("plugins/asr/.mcp.json"), None)?.is_some() {
                    return Err(CONFLICT);
                }
                required_bytes(&root.join("plugins/asr/skills/workspace/SKILL.md"))?;
                self.steps = vec![Step::ClaudeMarketplace, Step::ClaudePlugin, Step::ClaudeMcp];
                let config =
                    environment_path("CLAUDE_CONFIG_DIR", Some(self.home.join(".claude")))?;
                self.native_destinations = vec![
                    config.join("settings.json"),
                    config.join("plugins/known_marketplaces.json"),
                    config.join("plugins/installed_plugins.json"),
                    config
                        .join("plugins/cache/asr-local/asr")
                        .join(&self.version)
                        .join(".claude-plugin/plugin.json"),
                    if env::var_os("CLAUDE_CONFIG_DIR").is_some() {
                        config.join(".claude.json")
                    } else {
                        self.home.join(".claude.json")
                    },
                ];
            }
            OnboardingProvider::CodexCli => {
                let bytes =
                    required_bytes(&self.integrations_dir.join("codex/skills/asr/SKILL.md"))?;
                self.files.push(file_install(
                    self.home.join(".agents/skills/asr/SKILL.md"),
                    bytes,
                    false,
                ));
                self.steps = vec![Step::CodexMcp];
                let config = environment_path("CODEX_HOME", Some(self.home.join(".codex")))?;
                self.native_destinations = vec![config.join("config.toml")];
            }
            OnboardingProvider::Omp => {
                let package = required_json(&self.integrations_dir.join("omp/package.json"))?;
                if package["name"] != OMP_PACKAGE || package["version"] != self.version {
                    return Err(CONFLICT);
                }
                required_bytes(&self.integrations_dir.join("omp/index.js"))?;
                required_bytes(&self.integrations_dir.join("omp/skills/asr/SKILL.md"))?;
                let descriptor = serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "executable": self.executable,
                    "profile": self.profile,
                    "workspace": journal.state().workspace,
                }))
                .map_err(|_| IO_ERROR)?;
                self.files.push(file_install(
                    self.private_root.join("hosts/omp.json"),
                    descriptor,
                    true,
                ));
                self.steps = vec![Step::OmpLink];
                let root = omp_config_root(&self.home)?;
                self.omp_link_destination =
                    Some(root.join("plugins/node_modules").join(OMP_PACKAGE));
                self.native_destinations = vec![
                    root.join("plugins/package.json"),
                    root.join("plugins/omp-plugins.lock.json"),
                ];
            }
        }
        Ok(())
    }

    fn check_paths(&self, journal: &Journal) -> Result<(), ProviderInstallError> {
        directory(&self.home, None, false, true)?.ok_or(PERMISSION)?;
        directory(&self.private_root, Some(&self.private_root), false, true)?.ok_or(PERMISSION)?;
        if hash_file(&self.executable, None)?.as_ref() != Some(&self.executable_sha256) {
            return Err(CONFLICT);
        }
        for target in &self.native_destinations {
            check_destination(target, None)?;
        }
        self.check_omp_link_destination(journal)?;
        if self.provider == OnboardingProvider::ClaudeCode {
            let config = environment_path("CLAUDE_CONFIG_DIR", Some(self.home.join(".claude")))?;
            let cache = config
                .join("plugins/cache/asr-local/asr")
                .join(&self.version);
            if directory(&cache, None, false, false)?.is_some()
                && action_status(journal, &self.command_intent(Step::ClaudePlugin)).is_none()
            {
                return Err(CONFLICT);
            }
        }
        for file in &self.files {
            let root = file.record.private.then_some(self.private_root.as_path());
            check_destination(&file.record.path, root)?;
            check_file(&file.record, journal, root)?;
        }
        let manifest = ownership_path(&self.private_root, self.provider);
        check_destination(&manifest, Some(&self.private_root))?;
        if read_file(&manifest, Some(&self.private_root))?.is_some() {
            verify_recorded(&self.config_path, journal)?;
        } else if action_for_file(journal, &manifest)
            .is_some_and(|(_, status)| status == ActionStatus::Applied)
        {
            return Err(CONFLICT);
        }
        Ok(())
    }

    async fn check_features(&self) -> Result<(), ProviderInstallError> {
        let features: &[(&[&str], &[&str])] = match self.provider {
            OnboardingProvider::ClaudeCode => &[
                (&["plugin", "marketplace", "add", "--help"], &["--scope"]),
                (&["plugin", "marketplace", "list", "--help"], &["--json"]),
                (&["plugin", "install", "--help"], &["--scope"]),
                (&["plugin", "list", "--help"], &["--json"]),
                (&["mcp", "add", "--help"], &["--scope", "--transport"]),
                (&["mcp", "get", "--help"], &["get"]),
            ],
            OnboardingProvider::CodexCli => &[
                (&["mcp", "add", "--help"], &["COMMAND"]),
                (&["mcp", "get", "--help"], &["--json"]),
                (&["mcp", "list", "--help"], &["--json"]),
            ],
            OnboardingProvider::Omp => &[
                (&["plugin", "link", "--help"], &["link", "--scope"]),
                (&["plugin", "enable", "--help"], &["enable", "--scope"]),
                (&["plugin", "list", "--help"], &["list", "--json"]),
            ],
        };
        for (args, required) in features {
            let output = self.run(args).await?;
            let help =
                std::str::from_utf8(&output.stdout).map_err(|_| requirement(self.provider))?;
            if !output.success || required.iter().any(|token| !help.contains(token)) {
                return Err(requirement(self.provider));
            }
        }
        Ok(())
    }

    async fn check_registrations(&self, journal: &Journal) -> Result<(), ProviderInstallError> {
        for step in &self.steps {
            let registered = self.inspect(*step).await?;
            self.accept_registration(*step, registered, journal)?;
            self.check_omp_link_destination(journal)?;
        }
        Ok(())
    }

    fn accept_registration(
        &self,
        step: Step,
        registered: Registered,
        journal: &Journal,
    ) -> Result<(), ProviderInstallError> {
        let status = action_status(journal, &self.command_intent(step));
        match registered {
            Registered::Absent if status == Some(ActionStatus::Applied) => Err(CONFLICT),
            Registered::Absent => Ok(()),
            Registered::Exact if status.is_some() => Ok(()),
            Registered::Disabled if step == Step::OmpLink && self.pending_link(journal) => Ok(()),
            Registered::Disabled if step == Step::OmpLink => Err(OMP_DISABLED),
            Registered::Disabled if step == Step::ClaudePlugin => Err(CLAUDE_DISABLED),
            Registered::Exact | Registered::Disabled => Err(CONFLICT),
        }
    }

    fn pending_link(&self, journal: &Journal) -> bool {
        action_status(journal, &self.command_intent(Step::OmpLink)) == Some(ActionStatus::Planned)
            && journal.state().stage != Stage::Configured
    }

    fn check_omp_link_destination(&self, journal: &Journal) -> Result<bool, ProviderInstallError> {
        let Some(destination) = &self.omp_link_destination else {
            return Ok(false);
        };
        let Some(parent) = directory(destination.parent().ok_or(PERMISSION)?, None, false, true)?
        else {
            return Ok(false);
        };
        let name = destination.file_name().ok_or(PERMISSION)?;
        let metadata = match statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(metadata) => metadata,
            Err(rustix::io::Errno::NOENT) => return Ok(false),
            Err(_) => return Err(PERMISSION),
        };
        // OMP's native link command recursively removes its destination, even
        // when plugin list omits it. An intent never authorizes deleting a real
        // directory, an unrelated link, or a link owned by another user.
        if action_status(journal, &self.command_intent(Step::OmpLink)).is_none()
            || FileType::from_raw_mode(metadata.st_mode) != FileType::Symlink
            || metadata.st_uid != rustix::process::getuid().as_raw()
            || metadata.st_nlink != 1
        {
            return Err(CONFLICT);
        }
        let target = readlinkat(&parent, name, Vec::new()).map_err(|_| CONFLICT)?;
        if target.to_bytes() != self.integrations_dir.join("omp").as_os_str().as_bytes() {
            return Err(CONFLICT);
        }
        let after = statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW).map_err(|_| CONFLICT)?;
        if after.st_dev != metadata.st_dev || after.st_ino != metadata.st_ino {
            return Err(CONFLICT);
        }
        Ok(true)
    }

    fn command_intent(&self, step: Step) -> InstallAction {
        InstallAction::Command {
            argv: self.argv(step),
        }
    }

    fn argv(&self, step: Step) -> Vec<String> {
        let mut args = vec![self.program.to_string_lossy().into_owned()];
        let add = match step {
            Step::ClaudeMarketplace => vec![
                "plugin".into(),
                "marketplace".into(),
                "add".into(),
                self.integrations_dir
                    .join("claude-plugin")
                    .to_string_lossy()
                    .into_owned(),
                "--scope".into(),
                "user".into(),
            ],
            Step::ClaudePlugin => vec![
                "plugin".into(),
                "install".into(),
                "asr@asr-local".into(),
                "--scope".into(),
                "user".into(),
            ],
            Step::ClaudeMcp => {
                let mut args = strings(&[
                    "mcp",
                    "add",
                    "--transport",
                    "stdio",
                    "--scope",
                    "user",
                    CLAUDE_MCP,
                    "--",
                ]);
                args.push(self.executable.to_string_lossy().into_owned());
                args.extend(self.mcp_arguments("claude-channel"));
                args
            }
            Step::CodexMcp => {
                let mut args = strings(&["mcp", "add", CODEX_MCP, "--"]);
                args.push(self.executable.to_string_lossy().into_owned());
                args.extend(self.mcp_arguments("codex-cli"));
                args
            }
            Step::OmpLink => vec![
                "plugin".into(),
                "link".into(),
                self.integrations_dir
                    .join("omp")
                    .to_string_lossy()
                    .into_owned(),
                "--scope".into(),
                "user".into(),
            ],
            Step::OmpEnable => strings(&["plugin", "enable", OMP_PACKAGE, "--scope", "user"]),
        };
        args.extend(add);
        args
    }

    fn mcp_arguments(&self, role: &str) -> Vec<String> {
        vec![
            "--profile".into(),
            self.profile.clone(),
            "mcp".into(),
            role.into(),
        ]
    }

    /// Every effect has a durable exact intent, then an independent inspection before Applied.
    /// `mark_configured` remains the orchestrator's responsibility.
    pub async fn configure(self, journal: &mut Journal) -> Result<(), ProviderInstallError> {
        if journal.state().stage == Stage::Prepared
            || journal.state().provider != self.provider
            || journal.state().executable != self.executable
            || journal.state().profile_name != self.profile
            || journal.state().invite_id != self.invite_id
            || journal.state().enrollment_id != self.enrollment_id
        {
            return Err(CONFLICT);
        }
        // External provider/configuration changes are not serialized by ASR's lock.
        self.check_paths(journal)?;
        self.check_registrations(journal).await?;
        for file in &self.files {
            publish_file(file, journal, &self.private_root)?;
        }
        for step in &self.steps {
            self.configure_step(*step, journal).await?;
        }
        let mut commands = self
            .steps
            .iter()
            .map(|step| self.argv(*step))
            .collect::<Vec<_>>();
        if self.provider == OnboardingProvider::Omp
            && action_status(journal, &self.command_intent(Step::OmpEnable)).is_some()
        {
            commands.push(self.argv(Step::OmpEnable));
        }
        let state = journal.state();
        let ownership = Ownership {
            version: 1,
            invite_id: state.invite_id,
            enrollment_id: state.enrollment_id,
            provider: self.provider,
            server_id: state.server_id,
            profile: self.profile.clone(),
            workspace: state.workspace.clone(),
            manifest_sha256: state.manifest_sha256.clone(),
            asr_version: self.version.clone(),
            executable: self.executable.clone(),
            executable_sha256: self.executable_sha256.clone(),
            integrations_dir: self.integrations_dir.clone(),
            files: self.files.iter().map(|file| file.record.clone()).collect(),
            commands,
        };
        let bytes = serde_json::to_vec(&ownership).map_err(|_| IO_ERROR)?;
        publish_file(
            &file_install(
                ownership_path(&self.private_root, self.provider),
                bytes,
                true,
            ),
            journal,
            &self.private_root,
        )?;
        verify_recorded(&self.config_path, journal)
    }

    async fn configure_step(
        &self,
        step: Step,
        journal: &mut Journal,
    ) -> Result<(), ProviderInstallError> {
        for target in &self.native_destinations {
            check_destination(target, None)?;
        }
        let before = self.inspect(step).await?;
        self.accept_registration(step, before, journal)?;
        self.check_omp_link_destination(journal)?;
        let index = journal
            .plan(self.command_intent(step))
            .map_err(journal_error)?;
        let after = if before == Registered::Absent {
            // A nonzero exit/timeout can still have committed. Always inspect first;
            // if inspection also fails the original planned action stays durable.
            let argv = self.argv(step);
            // Recheck after the durable intent and immediately before invoking
            // the provider's destructive link operation.
            self.check_omp_link_destination(journal)?;
            let effect = self.run(&argv[1..]).await;
            let observed = self.inspect(step).await?;
            if observed == Registered::Absent {
                return Err(effect.err().unwrap_or(COMMAND_FAILED));
            }
            observed
        } else {
            before
        };
        if step == Step::OmpLink && !self.check_omp_link_destination(journal)? {
            return Err(CONFLICT);
        }
        if after == Registered::Disabled && step == Step::OmpLink && self.pending_link(journal) {
            let enable = journal
                .plan(self.command_intent(Step::OmpEnable))
                .map_err(journal_error)?;
            let argv = self.argv(Step::OmpEnable);
            let effect = self.run(&argv[1..]).await;
            if self.inspect(Step::OmpLink).await? != Registered::Exact {
                return Err(effect.err().unwrap_or(OMP_DISABLED));
            }
            journal.mark_applied(enable).map_err(journal_error)?;
        } else if after != Registered::Exact {
            return Err(if step == Step::ClaudePlugin {
                CLAUDE_DISABLED
            } else {
                CONFLICT
            });
        }
        // Also reconcile an enable that committed before its Applied fsync.
        if step == Step::OmpLink
            && let Some(enable) = journal
                .state()
                .actions
                .iter()
                .position(|record| record.intent == self.command_intent(Step::OmpEnable))
        {
            journal.mark_applied(enable).map_err(journal_error)?;
        }
        journal.mark_applied(index).map_err(journal_error)
    }

    async fn run<S: AsRef<std::ffi::OsStr>>(
        &self,
        args: &[S],
    ) -> Result<CliOutput, ProviderInstallError> {
        run_cli(&self.program, &self.home, args, self.provider).await
    }

    async fn inspect(&self, step: Step) -> Result<Registered, ProviderInstallError> {
        match step {
            Step::ClaudeMarketplace => self.inspect_marketplace().await,
            Step::ClaudePlugin => self.inspect_claude_plugin().await,
            Step::ClaudeMcp => self.inspect_claude_mcp().await,
            Step::CodexMcp => self.inspect_codex_mcp().await,
            Step::OmpLink | Step::OmpEnable => self.inspect_omp().await,
        }
    }

    async fn json(&self, args: &[&str]) -> Result<Value, ProviderInstallError> {
        let output = self.run(args).await?;
        if !output.success {
            return Err(COMMAND_FAILED);
        }
        serde_json::from_slice(&output.stdout).map_err(|_| requirement(self.provider))
    }

    async fn inspect_marketplace(&self) -> Result<Registered, ProviderInstallError> {
        let value = self
            .json(&["plugin", "marketplace", "list", "--json"])
            .await?;
        let entries = value.as_array().ok_or(requirement(self.provider))?;
        if entries.iter().any(|entry| !entry["name"].is_string()) {
            return Err(requirement(self.provider));
        }
        let mut own = entries.iter().filter(|entry| entry["name"] == "asr-local");
        let Some(entry) = own.next() else {
            return Ok(Registered::Absent);
        };
        if own.next().is_some()
            || entry["source"] != "directory"
            || entry["path"].as_str() != self.integrations_dir.join("claude-plugin").to_str()
            || entry["installLocation"].as_str()
                != self.integrations_dir.join("claude-plugin").to_str()
        {
            return Err(CONFLICT);
        }
        Ok(Registered::Exact)
    }

    async fn inspect_claude_plugin(&self) -> Result<Registered, ProviderInstallError> {
        let value = self.json(&["plugin", "list", "--json"]).await?;
        let entries = value.as_array().ok_or(requirement(self.provider))?;
        if entries.iter().any(|entry| !entry["id"].is_string()) {
            return Err(requirement(self.provider));
        }
        let mut own = entries.iter().filter(|entry| {
            entry["id"]
                .as_str()
                .is_some_and(|id| id == "asr" || id.starts_with("asr@"))
        });
        let Some(entry) = own.next() else {
            return Ok(Registered::Absent);
        };
        if own.next().is_some()
            || entry["id"] != "asr@asr-local"
            || entry["scope"] != "user"
            || entry["version"] != self.version
        {
            return Err(CONFLICT);
        }
        let installed = absolute(Path::new(entry["installPath"].as_str().ok_or(CONFLICT)?))?;
        let source = self.integrations_dir.join("claude-plugin/plugins/asr");
        for relative in [".claude-plugin/plugin.json", "skills/workspace/SKILL.md"] {
            if required_bytes(&installed.join(relative))? != required_bytes(&source.join(relative))?
            {
                return Err(CONFLICT);
            }
        }
        if read_file(&installed.join(".mcp.json"), None)?.is_some() {
            return Err(CONFLICT);
        }
        match entry["enabled"].as_bool() {
            Some(true) => Ok(Registered::Exact),
            Some(false) => Ok(Registered::Disabled),
            None => Err(requirement(self.provider)),
        }
    }

    async fn inspect_codex_mcp(&self) -> Result<Registered, ProviderInstallError> {
        let output = self.run(&["mcp", "get", CODEX_MCP, "--json"]).await?;
        if !output.success {
            // A failed get is not proof of absence. Require a successful structured list.
            let value = self.json(&["mcp", "list", "--json"]).await?;
            let entries = value.as_array().ok_or(requirement(self.provider))?;
            if entries.iter().any(|entry| !entry["name"].is_string()) {
                return Err(requirement(self.provider));
            }
            if entries.iter().any(|entry| entry["name"] == CODEX_MCP) {
                return Err(CONFLICT);
            }
            return Ok(Registered::Absent);
        }
        let entry: Value =
            serde_json::from_slice(&output.stdout).map_err(|_| requirement(self.provider))?;
        let transport = &entry["transport"];
        if entry["name"] != CODEX_MCP
            || entry["enabled"] != true
            || !null_or_absent(&entry, "disabled_reason")
            || transport["type"] != "stdio"
            || transport["command"].as_str() != self.executable.to_str()
            || transport["args"] != serde_json::json!(self.mcp_arguments("codex-cli"))
            || !empty_or_null(&transport["env"])
            || !empty_or_null(&transport["env_vars"])
            || !null_or_absent(transport, "cwd")
            || !null_or_absent(&entry, "enabled_tools")
            || !null_or_absent(&entry, "disabled_tools")
        {
            return Err(CONFLICT);
        }
        Ok(Registered::Exact)
    }

    async fn inspect_claude_mcp(&self) -> Result<Registered, ProviderInstallError> {
        let output = self.run(&["mcp", "get", CLAUDE_MCP]).await?;
        if !output.success {
            let absent = format!("No MCP server found with name: {CLAUDE_MCP}");
            if output.stdout.is_empty()
                && std::str::from_utf8(&output.stderr).is_ok_and(|text| text.trim() == absent)
            {
                return Ok(Registered::Absent);
            }
            return Err(COMMAND_FAILED);
        }
        let text = std::str::from_utf8(&output.stdout).map_err(|_| requirement(self.provider))?;
        let lines = text.lines().map(str::trim).collect::<Vec<_>>();
        let command = format!("Command: {}", self.executable.display());
        let args = format!("Args: {}", self.mcp_arguments("claude-channel").join(" "));
        // Do not tokenize arbitrary CLI text or accept a similarly-looking foreign
        // definition. Provenance is checked separately against our exact argv intent.
        if lines.first().copied() != Some("agent-session-router-channel:")
            || lines
                .iter()
                .filter(|line| **line == "Scope: User config (available in all your projects)")
                .count()
                != 1
            || lines.iter().filter(|line| **line == "Type: stdio").count() != 1
            || lines.iter().filter(|line| **line == command).count() != 1
            || lines.iter().filter(|line| **line == args).count() != 1
        {
            return Err(CONFLICT);
        }
        for line in &lines {
            if line.is_empty()
                || *line == "agent-session-router-channel:"
                || *line == "Scope: User config (available in all your projects)"
                || *line == "Type: stdio"
                || *line == command
                || *line == args
                || *line == "Environment:"
                || line.starts_with("Status:")
                || line.starts_with("To remove this server, run: claude mcp remove ")
            {
                continue;
            }
            return Err(CONFLICT);
        }
        Ok(Registered::Exact)
    }

    async fn inspect_omp(&self) -> Result<Registered, ProviderInstallError> {
        let value = self.json(&["plugin", "list", "--json"]).await?;
        let entries = value["npm"].as_array().ok_or(requirement(self.provider))?;
        let marketplace = value["marketplace"]
            .as_array()
            .ok_or(requirement(self.provider))?;
        if entries.iter().any(|entry| !entry["name"].is_string()) {
            return Err(requirement(self.provider));
        }
        if marketplace.iter().any(|entry| {
            ["name", "id"].iter().any(|key| {
                entry[*key].as_str().is_some_and(|name| {
                    name == OMP_PACKAGE || name == "asr" || name.starts_with("asr@")
                })
            })
        }) {
            return Err(CONFLICT);
        }
        let mut own = entries.iter().filter(|entry| {
            matches!(
                entry["name"].as_str(),
                Some(OMP_PACKAGE | "agent-session-router")
            )
        });
        let Some(entry) = own.next() else {
            return Ok(Registered::Absent);
        };
        if own.next().is_some() || entry["name"] != OMP_PACKAGE || entry["version"] != self.version
        {
            return Err(CONFLICT);
        }
        // OMP deliberately links node_modules to the installed package. Only this
        // provider-created link is resolved; all directly managed files use no-follow.
        let installed =
            std::fs::canonicalize(entry["path"].as_str().ok_or(CONFLICT)?).map_err(|_| CONFLICT)?;
        let expected =
            std::fs::canonicalize(self.integrations_dir.join("omp")).map_err(|_| IO_ERROR)?;
        if installed != expected || !empty_or_null(&entry["enabledFeatures"]) {
            return Err(CONFLICT);
        }
        match entry["enabled"].as_bool() {
            Some(true) => Ok(Registered::Exact),
            Some(false) => Ok(Registered::Disabled),
            None => Err(requirement(self.provider)),
        }
    }
}

/// Only private ownership and directly managed file integrity: no provider CLI or MCP
/// connection. Registration and current-session activation deliberately remain distinct.
pub fn verify_recorded(config_path: &Path, journal: &Journal) -> Result<(), ProviderInstallError> {
    let config_path = absolute(config_path)?;
    let root = config_path.parent().ok_or(PERMISSION)?;
    let state = journal.state();
    let path = ownership_path(root, state.provider);
    let bytes = read_file(&path, Some(root))?.ok_or(CONFLICT)?;
    let ownership: Ownership = serde_json::from_slice(&bytes).map_err(|_| CONFLICT)?;
    let intent = InstallAction::File {
        path,
        sha256: digest(&bytes),
    };
    if action_status(journal, &intent).is_none()
        || (state.stage == Stage::Configured
            && action_status(journal, &intent) != Some(ActionStatus::Applied))
        || ownership.version != 1
        || ownership.invite_id != state.invite_id
        || ownership.enrollment_id != state.enrollment_id
        || ownership.provider != state.provider
        || ownership.server_id != state.server_id
        || ownership.profile != state.profile_name
        || ownership.workspace != state.workspace
        || ownership.manifest_sha256 != state.manifest_sha256
        || ownership.executable != state.executable
        || ownership.asr_version.is_empty()
        || hash_file(&ownership.executable, None)?.as_ref() != Some(&ownership.executable_sha256)
    {
        return Err(CONFLICT);
    }
    let expected_files = match state.provider {
        OnboardingProvider::ClaudeCode => 0,
        OnboardingProvider::CodexCli | OnboardingProvider::Omp => 1,
    };
    let expected_commands = if state.provider == OnboardingProvider::ClaudeCode {
        3
    } else {
        1
    };
    if ownership.files.len() != expected_files
        || ownership.commands.len() < expected_commands
        || ownership.commands.len()
            > expected_commands + usize::from(state.provider == OnboardingProvider::Omp)
    {
        return Err(CONFLICT);
    }
    for file in ownership.files {
        if action_status(journal, &file.intent()) != Some(ActionStatus::Applied)
            || hash_file(&file.path, file.private.then_some(root))?.as_ref() != Some(&file.sha256)
        {
            return Err(CONFLICT);
        }
    }
    for argv in ownership.commands {
        if action_status(journal, &InstallAction::Command { argv }) != Some(ActionStatus::Applied) {
            return Err(CONFLICT);
        }
    }
    Ok(())
}

#[must_use]
pub fn next_action(
    provider: OnboardingProvider,
    executable: &Path,
    profile: &str,
    workspace: &WorkspaceName,
) -> String {
    match provider {
        OnboardingProvider::ClaudeCode => format!(
            "Restart Claude Code with {} --profile {} claude --workspace {}. This existing launcher opts into the development Channel; organization policy must allow it. Then run /asr:workspace status and use the actual MCP workspace_list and workspace_members tools to confirm your identity. Installation alone does not activate this conversation.",
            shell_quote(&executable.to_string_lossy()), shell_quote(profile), shell_quote(workspace.as_str()),
        ),
        OnboardingProvider::CodexCli => "Start a new Codex CLI session with `codex`, then select $asr (or use /skills) and request `$asr workspace status`. Use the actual MCP workspace_list and workspace_members tools to confirm your identity; installation does not activate the current session.".to_owned(),
        OnboardingProvider::Omp => "Exit and restart the OMP process with `omp`, then run `/asr workspace status`. Confirm your identity with the actual MCP workspace_list and workspace_members tools. A command reload is not a plugin-registry restart.".to_owned(),
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn null_or_absent(value: &Value, key: &str) -> bool {
    value.get(key).is_none_or(Value::is_null)
}

fn empty_or_null(value: &Value) -> bool {
    value.is_null()
        || value.as_object().is_some_and(serde_json::Map::is_empty)
        || value.as_array().is_some_and(Vec::is_empty)
}

fn file_install(path: PathBuf, bytes: Vec<u8>, private: bool) -> FileInstall {
    FileInstall {
        record: RecordedFile {
            path,
            sha256: digest(&bytes),
            private,
        },
        bytes,
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn action_status(journal: &Journal, intent: &InstallAction) -> Option<ActionStatus> {
    journal
        .state()
        .actions
        .iter()
        .find(|record| &record.intent == intent)
        .map(|record| record.status)
}

fn action_for_file<'a>(journal: &'a Journal, path: &Path) -> Option<(&'a str, ActionStatus)> {
    journal
        .state()
        .actions
        .iter()
        .find_map(|record| match &record.intent {
            InstallAction::File {
                path: recorded,
                sha256,
            } if recorded == path => Some((sha256.as_str(), record.status)),
            _ => None,
        })
}

fn check_file(
    record: &RecordedFile,
    journal: &Journal,
    root: Option<&Path>,
) -> Result<(), ProviderInstallError> {
    let status = action_status(journal, &record.intent());
    match hash_file(&record.path, root)? {
        Some(hash) if hash == record.sha256 && status.is_some() => Ok(()),
        Some(_) => Err(CONFLICT),
        None if action_for_file(journal, &record.path).is_some_and(|(digest, applied)| {
            digest != record.sha256 || applied == ActionStatus::Applied
        }) =>
        {
            Err(CONFLICT)
        }
        None => Ok(()),
    }
}

fn publish_file(
    file: &FileInstall,
    journal: &mut Journal,
    root: &Path,
) -> Result<(), ProviderInstallError> {
    let private = file.record.private.then_some(root);
    // Check provenance before adding an intent: otherwise an identical foreign file
    // would become "ours" merely because planning happened before inspection.
    check_file(&file.record, journal, private)?;
    let index = journal.plan(file.record.intent()).map_err(journal_error)?;
    if hash_file(&file.record.path, private)?.is_none() {
        atomic_no_replace(&file.record.path, &file.bytes, private)?;
    }
    if hash_file(&file.record.path, private)?.as_ref() != Some(&file.record.sha256) {
        return Err(CONFLICT);
    }
    directory(
        file.record.path.parent().ok_or(PERMISSION)?,
        private,
        false,
        true,
    )?
    .ok_or(IO_ERROR)?
    .sync_all()
    .map_err(|_| IO_ERROR)?;
    journal.mark_applied(index).map_err(journal_error)
}

fn ownership_path(root: &Path, provider: OnboardingProvider) -> PathBuf {
    root.join("onboarding/installations")
        .join(format!("{}.json", provider.as_str()))
}

fn required_json(path: &Path) -> Result<Value, ProviderInstallError> {
    serde_json::from_slice(&required_bytes(path)?).map_err(|_| CONFLICT)
}

fn required_bytes(path: &Path) -> Result<Vec<u8>, ProviderInstallError> {
    read_file(path, None)?.ok_or(IO_ERROR)
}

fn absolute(path: &Path) -> Result<PathBuf, ProviderInstallError> {
    if path.as_os_str().is_empty()
        || path
            .to_str()
            .is_none_or(|value| value.chars().any(char::is_control))
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(PERMISSION);
    }
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(env::current_dir().map_err(|_| IO_ERROR)?.join(path))
    }
}

fn environment_path(name: &str, default: Option<PathBuf>) -> Result<PathBuf, ProviderInstallError> {
    match env::var_os(name) {
        Some(value) => {
            let path = PathBuf::from(value);
            let path = if let Ok(suffix) = path.strip_prefix("~") {
                PathBuf::from(env::var_os("HOME").ok_or(PERMISSION)?).join(suffix)
            } else {
                path
            };
            absolute(&path)
        }
        None => absolute(&default.ok_or(PERMISSION)?),
    }
}

fn omp_config_root(home: &Path) -> Result<PathBuf, ProviderInstallError> {
    let name = env::var_os("PI_CONFIG_DIR").unwrap_or_else(|| OsString::from(".omp"));
    if Path::new(&name).is_absolute() {
        return Err(PERMISSION);
    }
    let mut root = absolute(&home.join(name))?;
    let profile = env::var("OMP_PROFILE")
        .ok()
        .or_else(|| env::var("PI_PROFILE").ok());
    let profile = profile
        .as_deref()
        .filter(|value| !value.is_empty() && *value != "default");
    if let Some(profile) = profile {
        if !profile.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        }) || profile == "."
            || profile == ".."
        {
            return Err(PERMISSION);
        }
        root = root.join("profiles").join(profile);
    }
    if (env::var_os("PI_CODING_AGENT_DIR").is_none() || profile.is_some())
        && env::var_os("XDG_DATA_HOME").is_some()
    {
        let mut xdg = environment_path("XDG_DATA_HOME", None)?.join("omp");
        if let Some(profile) = profile {
            xdg = xdg.join("profiles").join(profile);
        }
        if directory(&xdg, None, false, true)?.is_some() {
            root = xdg;
        }
    }
    Ok(root)
}

fn resolve_program(provider: OnboardingProvider) -> Result<PathBuf, ProviderInstallError> {
    let name = match provider {
        OnboardingProvider::ClaudeCode => "claude",
        OnboardingProvider::CodexCli => "codex",
        OnboardingProvider::Omp => "omp",
    };
    let paths = env::var_os("PATH").ok_or(requirement(provider))?;
    for directory in env::split_paths(&paths) {
        let candidate = directory.join(name);
        let Ok(metadata) = std::fs::metadata(&candidate) else {
            continue;
        };
        if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
            continue;
        }
        // Provider launchers commonly are package-manager symlinks. Resolve only
        // this executable, never an installer-managed destination.
        return absolute(&std::fs::canonicalize(candidate).map_err(|_| PERMISSION)?);
    }
    Err(requirement(provider))
}

fn check_destination(path: &Path, root: Option<&Path>) -> Result<(), ProviderInstallError> {
    let parent = directory(path.parent().ok_or(PERMISSION)?, root, false, true)?;
    if let Some(file) = open_file(path, root)? {
        if file.metadata().map_err(|_| IO_ERROR)?.mode() & 0o200 == 0 {
            return Err(PERMISSION);
        }
        accessat(
            &parent.ok_or(PERMISSION)?,
            path.file_name().ok_or(PERMISSION)?,
            Access::WRITE_OK,
            AtFlags::EACCESS,
        )
        .map_err(|_| PERMISSION)?;
    }
    Ok(())
}

/// Traverse directory descriptors rather than following attacker-replaced parent
/// symlinks. Public user asset parents may be 0755; private descendants must be 0700.
fn directory(
    path: &Path,
    private_root: Option<&Path>,
    create: bool,
    writable: bool,
) -> Result<Option<File>, ProviderInstallError> {
    let path = absolute(path)?;
    let mut current =
        File::from(open("/", DIRECTORY_FLAGS, Mode::empty()).map_err(|_| PERMISSION)?);
    let mut traversed = PathBuf::from("/");
    for component in path.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        traversed.push(name);
        let child = match openat(&current, name, DIRECTORY_FLAGS, Mode::empty()) {
            Ok(child) => child,
            Err(rustix::io::Errno::NOENT) => {
                check_writable_directory(&current)?;
                if !create {
                    return Ok(None);
                }
                // Directory creation is part of the already-fsynced File intent.
                mkdirat(&current, name, Mode::RUSR | Mode::WUSR | Mode::XUSR).map_err(|error| {
                    if error == rustix::io::Errno::EXIST {
                        CONFLICT
                    } else {
                        PERMISSION
                    }
                })?;
                current.sync_all().map_err(|_| IO_ERROR)?;
                openat(&current, name, DIRECTORY_FLAGS, Mode::empty()).map_err(|_| PERMISSION)?
            }
            Err(_) => return Err(PERMISSION),
        };
        let child = File::from(child);
        let metadata = child.metadata().map_err(|_| IO_ERROR)?;
        let owned = metadata.uid() == rustix::process::getuid().as_raw();
        let private = private_root.is_some_and(|root| traversed.starts_with(root));
        let system_temporary_ancestor =
            metadata.uid() == 0 && metadata.mode() & 0o1000 != 0 && traversed != path;
        if (!owned && metadata.uid() != 0)
            || (metadata.mode() & 0o022 != 0 && !system_temporary_ancestor)
            || (private && (!owned || metadata.mode() & 0o777 != 0o700))
        {
            return Err(PERMISSION);
        }
        current = child;
    }
    if current.metadata().map_err(|_| IO_ERROR)?.uid() != rustix::process::getuid().as_raw() {
        return Err(PERMISSION);
    }
    if writable {
        check_writable_directory(&current)?;
    }
    Ok(Some(current))
}

fn check_writable_directory(directory: &File) -> Result<(), ProviderInstallError> {
    let metadata = directory.metadata().map_err(|_| IO_ERROR)?;
    if metadata.uid() != rustix::process::getuid().as_raw() || metadata.mode() & 0o300 != 0o300 {
        return Err(PERMISSION);
    }
    accessat(
        directory,
        ".",
        Access::WRITE_OK | Access::EXEC_OK,
        AtFlags::EACCESS,
    )
    .map_err(|_| PERMISSION)
}

fn valid_file(metadata: &Metadata, private: bool) -> Result<(), ProviderInstallError> {
    if !metadata.is_file()
        || metadata.uid() != rustix::process::getuid().as_raw()
        || metadata.nlink() != 1
        || metadata.mode() & 0o022 != 0
        || (private && metadata.mode() & 0o777 != 0o600)
    {
        return Err(PERMISSION);
    }
    Ok(())
}

fn open_file(path: &Path, root: Option<&Path>) -> Result<Option<File>, ProviderInstallError> {
    let path = absolute(path)?;
    let Some(parent) = directory(path.parent().ok_or(PERMISSION)?, root, false, false)? else {
        return Ok(None);
    };
    let descriptor = match openat(
        &parent,
        path.file_name().ok_or(PERMISSION)?,
        FILE_FLAGS,
        Mode::empty(),
    ) {
        Ok(file) => file,
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(_) => return Err(PERMISSION),
    };
    let file = File::from(descriptor);
    valid_file(&file.metadata().map_err(|_| IO_ERROR)?, root.is_some())?;
    Ok(Some(file))
}

fn read_file(path: &Path, root: Option<&Path>) -> Result<Option<Vec<u8>>, ProviderInstallError> {
    let Some(file) = open_file(path, root)? else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    file.take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| IO_ERROR)?;
    if bytes.len() > MAX_BYTES {
        return Err(CONFLICT);
    }
    Ok(Some(bytes))
}

fn hash_file(path: &Path, root: Option<&Path>) -> Result<Option<String>, ProviderInstallError> {
    let Some(mut file) = open_file(path, root)? else {
        return Ok(None);
    };
    let before = file.metadata().map_err(|_| IO_ERROR)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 32 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(|_| IO_ERROR)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    let after = file.metadata().map_err(|_| IO_ERROR)?;
    if before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
        || before.ctime() != after.ctime()
        || before.ctime_nsec() != after.ctime_nsec()
    {
        return Err(CONFLICT);
    }
    Ok(Some(format!("{:x}", digest.finalize())))
}

fn atomic_no_replace(
    path: &Path,
    bytes: &[u8],
    root: Option<&Path>,
) -> Result<(), ProviderInstallError> {
    let parent = directory(path.parent().ok_or(PERMISSION)?, root, true, true)?.ok_or(IO_ERROR)?;
    let temporary = format!(".onboarding-{}.tmp", Uuid::new_v4());
    let mut file = File::from(
        openat(
            &parent,
            temporary.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| PERMISSION)?,
    );
    let result = (|| {
        file.write_all(bytes).map_err(|_| IO_ERROR)?;
        file.sync_all().map_err(|_| IO_ERROR)?;
        renameat_with(
            &parent,
            temporary.as_str(),
            &parent,
            path.file_name().ok_or(PERMISSION)?,
            RenameFlags::NOREPLACE,
        )
        .map_err(|error| {
            if error == rustix::io::Errno::EXIST {
                CONFLICT
            } else {
                IO_ERROR
            }
        })?;
        parent.sync_all().map_err(|_| IO_ERROR)
    })();
    let _ = unlinkat(&parent, temporary.as_str(), AtFlags::empty());
    result
}

struct CliOutput {
    success: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

async fn run_cli<S: AsRef<std::ffi::OsStr>>(
    program: &Path,
    cwd: &Path,
    args: &[S],
    provider: OnboardingProvider,
) -> Result<CliOutput, ProviderInstallError> {
    let mut child = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .env("NO_COLOR", "1")
        .env("LC_ALL", "C")
        .env_remove("ASR_EXECUTABLE")
        .env_remove("ASR_PROFILE")
        .env_remove("ASR_CREDENTIAL_FILE")
        .env_remove("ASR_WORKSPACE")
        .env_remove("ROUTER_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::PermissionDenied => PERMISSION,
            std::io::ErrorKind::NotFound => requirement(provider),
            _ => COMMAND_FAILED,
        })?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or(COMMAND_FAILED)?
        .take((MAX_BYTES + 1) as u64);
    let mut stderr = child
        .stderr
        .take()
        .ok_or(COMMAND_FAILED)?
        .take((MAX_BYTES + 1) as u64);
    let mut out = Vec::new();
    let mut err = Vec::new();
    let result = tokio::time::timeout(CLI_TIMEOUT, async {
        let (status, stdout_result, stderr_result) = tokio::join!(
            child.wait(),
            stdout.read_to_end(&mut out),
            stderr.read_to_end(&mut err)
        );
        let status = status.map_err(|_| COMMAND_FAILED)?;
        stdout_result.map_err(|_| COMMAND_FAILED)?;
        stderr_result.map_err(|_| COMMAND_FAILED)?;
        if out.len() > MAX_BYTES || err.len() > MAX_BYTES {
            return Err(COMMAND_FAILED);
        }
        Ok(CliOutput {
            success: status.success(),
            stdout: out,
            stderr: err,
        })
    })
    .await;
    if let Ok(result) = result {
        result
    } else {
        let _ = child.kill().await;
        let _ = child.wait().await;
        Err(COMMAND_FAILED)
    }
}
