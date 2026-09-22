use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    future::Future,
    io::{ErrorKind, Read as _, Write as _},
    os::unix::fs::DirBuilderExt as _,
    path::{Path, PathBuf},
    time::Duration,
};

use rustix::fs::{Mode, OFlags, open};
use tokio::{process::Command, sync::broadcast, time::Instant};
use uuid::Uuid;

use crate::{
    config::DelegateLaunchContext,
    credentials::{self, CredentialError},
    install::resolve_integration_asset,
    protocol::{TaskDispatch, TaskFence, WorkspaceName},
};

pub mod claude;
pub mod codex;
pub mod inbox;
pub mod router;

pub const MAX_PROVIDER_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_EARLY_NOTIFICATIONS: usize = 256;
pub const MAX_DELEGATE_CONTEXT_BYTES: usize = 64 * 1024;
pub const PROVIDER_TERMINAL_GRACE: Duration = Duration::from_secs(5);
pub const STDERR_DRAIN_BUFFER_BYTES: usize = 8 * 1024;
pub const MCP_TOOL_TIMEOUT_SECONDS: u64 = 605;

pub struct SessionRequest {
    pub request_id: String,
    pub workspace: Option<WorkspaceName>,
    pub from: String,
    pub content: String,
    pub deadline: Instant,
    pub task: Option<TaskDispatch>,
}

impl Clone for SessionRequest {
    fn clone(&self) -> Self {
        Self {
            request_id: self.request_id.clone(),
            workspace: self.workspace.clone(),
            from: self.from.clone(),
            content: self.content.clone(),
            deadline: self.deadline,
            task: self.task.clone(),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum SessionResult {
    Success { content: String },
    Failure { error: ProviderError },
}

impl SessionResult {
    #[must_use]
    pub fn success(content: impl Into<String>) -> Self {
        Self::Success {
            content: content.into(),
        }
    }

    #[must_use]
    pub const fn failure(error: ProviderError) -> Self {
        Self::Failure { error }
    }

    #[must_use]
    pub const fn error(&self) -> Option<ProviderError> {
        match self {
            Self::Success { .. } => None,
            Self::Failure { error } => Some(*error),
        }
    }
}

impl std::fmt::Debug for SessionResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success { .. } => formatter.write_str("SessionResult::Success(<redacted>)"),
            Self::Failure { error } => formatter
                .debug_struct("SessionResult::Failure")
                .field("error", error)
                .finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderError {
    ProviderNotReady,
    SessionBusy,
    RequestTimeout,
    ProviderDisconnected,
    CodexInitializeFailed,
    CodexProtocolError,
    CodexTurnInterrupted,
    CodexTurnFailed,
    CodexNoFinalResponse,
    ClaudeInitializeFailed,
    BridgeProtocolError,
    ClaudeProtocolError,
    ClaudeMaxTurns,
    ClaudeMaxBudget,
    ClaudeStructuredOutputError,
    ClaudeExecutionError,
    ClaudeNoResult,
    ClaudeSdkError,
    InvalidLaunchContext,
    RouterLifecycleUnavailable,
    ExecutionFenceChanged,
    LaunchFailed,
}

impl ProviderError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ProviderNotReady => "provider_not_ready",
            Self::SessionBusy => "session_busy",
            Self::RequestTimeout => "request_timeout",
            Self::ProviderDisconnected => "provider_disconnected",
            Self::CodexInitializeFailed => "codex_initialize_failed",
            Self::CodexProtocolError => "codex_protocol_error",
            Self::CodexTurnInterrupted => "codex_turn_interrupted",
            Self::CodexTurnFailed => "codex_turn_failed",
            Self::CodexNoFinalResponse => "codex_no_final_response",
            Self::ClaudeInitializeFailed => "claude_initialize_failed",
            Self::BridgeProtocolError => "bridge_protocol_error",
            Self::ClaudeProtocolError => "claude_protocol_error",
            Self::ClaudeMaxTurns => "claude_max_turns",
            Self::ClaudeMaxBudget => "claude_max_budget",
            Self::ClaudeStructuredOutputError => "claude_structured_output_error",
            Self::ClaudeExecutionError => "claude_execution_error",
            Self::ClaudeNoResult => "claude_no_result",
            Self::ClaudeSdkError => "claude_sdk_error",
            Self::InvalidLaunchContext => "invalid_launch_context",
            Self::RouterLifecycleUnavailable => "router_lifecycle_unavailable",
            Self::ExecutionFenceChanged => "execution_fence_changed",
            Self::LaunchFailed => "provider_launch_failed",
        }
    }
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ProviderError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelReason {
    RequestTimeout,
    ProviderDisconnected,
    TaskInterrupted,
    RequestCancelled,
}

impl CancelReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestTimeout => "request_timeout",
            Self::ProviderDisconnected => "provider_disconnected",
            Self::TaskInterrupted => "task_interrupted",
            Self::RequestCancelled => "request_cancelled",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalReason {
    TurnEnded,
    SessionEnded,
    HostError,
    OperatorInterrupt,
    RequestTimeout,
    RequestCancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalEvidence {
    pub request_id: String,
    pub reason: TerminalReason,
    pub child_reaped: bool,
}

pub trait OwnedProvider: Send + Sync + 'static {
    fn ready(&self) -> bool;

    fn handle(&self, request: SessionRequest) -> impl Future<Output = SessionResult> + Send;

    fn cancel(
        &self,
        request_id: String,
        reason: CancelReason,
    ) -> impl Future<Output = Result<(), ProviderError>> + Send;

    fn close(&self) -> impl Future<Output = Result<(), ProviderError>> + Send;

    fn subscribe_terminal(&self) -> broadcast::Receiver<TerminalEvidence>;
}

pub trait RouterProviderLifecycle: Send + Sync + 'static {
    fn set_ready(&self, ready: bool) -> impl Future<Output = Result<(), ProviderError>> + Send;
    fn execution_barrier(
        &self,
    ) -> impl Future<Output = Result<Option<TaskFence>, ProviderError>> + Send;

    fn execution_stopped(
        &self,
        evidence: TerminalEvidence,
        fence: Option<TaskFence>,
    ) -> impl Future<Output = Result<(), ProviderError>> + Send;
}

pub struct DelegateContextFile {
    directory: PathBuf,
    path: PathBuf,
}

impl DelegateContextFile {
    pub fn create(context: &DelegateLaunchContext) -> Result<Self, ProviderError> {
        Self::create_in(&std::env::temp_dir(), context)
    }

    pub fn create_in(root: &Path, context: &DelegateLaunchContext) -> Result<Self, ProviderError> {
        context
            .validate()
            .map_err(|_| ProviderError::InvalidLaunchContext)?;
        if !root.is_absolute() {
            return Err(ProviderError::InvalidLaunchContext);
        }
        let root = fs::canonicalize(root).map_err(|_| ProviderError::InvalidLaunchContext)?;
        let root_metadata =
            fs::symlink_metadata(&root).map_err(|_| ProviderError::InvalidLaunchContext)?;
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            return Err(ProviderError::InvalidLaunchContext);
        }
        let directory = (0..8)
            .find_map(|_| {
                let candidate = root.join(format!("asr-provider-{}", Uuid::new_v4()));
                let mut builder = fs::DirBuilder::new();
                match builder.mode(0o700).create(&candidate) {
                    Ok(()) => Some(Ok(candidate)),
                    Err(error) if error.kind() == ErrorKind::AlreadyExists => None,
                    Err(_) => Some(Err(ProviderError::InvalidLaunchContext)),
                }
            })
            .transpose()?
            .ok_or(ProviderError::InvalidLaunchContext)?;
        if credentials::ensure_private_directory(&directory, false).is_err() {
            let _ = fs::remove_dir(&directory);
            return Err(ProviderError::InvalidLaunchContext);
        }
        let path = directory.join("context.json");
        let Ok(payload) = serde_json::to_vec(context) else {
            let _ = fs::remove_dir(&directory);
            return Err(ProviderError::InvalidLaunchContext);
        };
        if payload.len() > MAX_DELEGATE_CONTEXT_BYTES {
            let _ = fs::remove_dir(&directory);
            return Err(ProviderError::InvalidLaunchContext);
        }
        let Ok(descriptor) = open(
            &path,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        ) else {
            let _ = fs::remove_dir(&directory);
            return Err(ProviderError::InvalidLaunchContext);
        };
        let mut file = File::from(descriptor);
        let result = (|| {
            file.write_all(&payload)?;
            file.sync_all()?;
            File::open(&directory)?.sync_all()?;
            Ok::<(), std::io::Error>(())
        })();
        if result.is_err() || validate_delegate_context(&path).is_err() {
            let _ = fs::remove_file(&path);
            let _ = fs::remove_dir(&directory);
            return Err(ProviderError::InvalidLaunchContext);
        }
        Ok(Self { directory, path })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DelegateContextFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_dir(&self.directory);
    }
}

pub fn validate_delegate_context(path: &Path) -> Result<DelegateLaunchContext, ProviderError> {
    let file = credentials::open_private_file(path).map_err(map_context_error)?;
    let mut bytes = Vec::new();
    file.take((MAX_DELEGATE_CONTEXT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ProviderError::InvalidLaunchContext)?;
    if bytes.len() > MAX_DELEGATE_CONTEXT_BYTES {
        return Err(ProviderError::InvalidLaunchContext);
    }
    let context: DelegateLaunchContext =
        serde_json::from_slice(&bytes).map_err(|_| ProviderError::InvalidLaunchContext)?;
    context
        .validate()
        .map_err(|_| ProviderError::InvalidLaunchContext)?;
    Ok(context)
}

fn map_context_error(_error: CredentialError) -> ProviderError {
    ProviderError::InvalidLaunchContext
}

pub struct ManagedLaunch {
    context: DelegateContextFile,
    context_path_utf8: String,
    asr_executable: PathBuf,
    cwd: PathBuf,
    environment: Vec<(OsString, OsString)>,
    integration_environment: Vec<(OsString, OsString)>,
}

pub(crate) fn managed_path_utf8(path: &Path) -> Result<&str, ProviderError> {
    path.to_str().ok_or(ProviderError::InvalidLaunchContext)
}

impl ManagedLaunch {
    pub fn new(
        asr_executable: PathBuf,
        cwd: PathBuf,
        context: &DelegateLaunchContext,
        source_environment: impl IntoIterator<Item = (OsString, OsString)>,
    ) -> Result<Self, ProviderError> {
        Self::new_in(
            asr_executable,
            cwd,
            context,
            source_environment,
            &std::env::temp_dir(),
        )
    }

    pub fn new_in(
        asr_executable: PathBuf,
        cwd: PathBuf,
        context: &DelegateLaunchContext,
        source_environment: impl IntoIterator<Item = (OsString, OsString)>,
        context_root: &Path,
    ) -> Result<Self, ProviderError> {
        if !asr_executable.is_absolute() || !cwd.is_absolute() || !cwd.is_dir() {
            return Err(ProviderError::InvalidLaunchContext);
        }
        managed_path_utf8(&asr_executable)?;
        managed_path_utf8(&cwd)?;
        let (integration_environment, provider_environment): (Vec<_>, Vec<_>) = source_environment
            .into_iter()
            .partition(|(key, _)| key == OsStr::new(crate::install::INTEGRATIONS_ENV));
        let context = DelegateContextFile::create_in(context_root, context)?;
        let context_path_utf8 = managed_path_utf8(context.path())?.to_owned();
        Ok(Self {
            context,
            context_path_utf8,
            asr_executable,
            cwd,
            environment: filtered_provider_environment(provider_environment),
            integration_environment,
        })
    }

    #[must_use]
    pub fn context_path(&self) -> &Path {
        self.context.path()
    }

    #[must_use]
    pub fn cwd(&self) -> &Path {
        &self.cwd
    }

    #[must_use]
    pub fn delegate_arguments(&self) -> Vec<String> {
        vec![
            String::from("mcp"),
            String::from("delegate"),
            String::from("--context-file"),
            self.context_path_utf8.clone(),
        ]
    }

    pub fn configure_child(&self, command: &mut Command) {
        command
            .current_dir(&self.cwd)
            .env_clear()
            .envs(self.environment.iter().map(|(key, value)| (key, value)));
    }

    pub fn resolve_integration_asset(&self, relative: &Path) -> Result<PathBuf, ProviderError> {
        resolve_integration_asset(
            &self.integration_environment,
            &self.asr_executable,
            relative,
        )
        .map_err(|_| ProviderError::LaunchFailed)
    }

    pub fn codex_mcp_overrides(
        &self,
        enabled_tools: &[String],
    ) -> Result<Vec<OsString>, ProviderError> {
        if enabled_tools.is_empty()
            || enabled_tools.iter().any(|name| {
                name.is_empty()
                    || !name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            })
        {
            return Err(ProviderError::LaunchFailed);
        }
        let prefix = "mcp_servers.agent_session_router";
        let executable = managed_path_utf8(&self.asr_executable)?;
        let command =
            serde_json::to_string(executable).map_err(|_| ProviderError::InvalidLaunchContext)?;
        let args = serde_json::to_string(&self.delegate_arguments())
            .map_err(|_| ProviderError::InvalidLaunchContext)?;
        let enabled_tools =
            serde_json::to_string(enabled_tools).map_err(|_| ProviderError::LaunchFailed)?;
        Ok(vec![
            OsString::from("-c"),
            OsString::from(format!("{prefix}.command={command}")),
            OsString::from("-c"),
            OsString::from(format!("{prefix}.args={args}")),
            OsString::from("-c"),
            OsString::from(format!("{prefix}.required=true")),
            OsString::from("-c"),
            OsString::from(format!("{prefix}.enabled_tools={enabled_tools}")),
            OsString::from("-c"),
            OsString::from(format!("{prefix}.default_tools_approval_mode=\"approve\"")),
            OsString::from("-c"),
            OsString::from(format!(
                "{prefix}.tool_timeout_sec={MCP_TOOL_TIMEOUT_SECONDS}"
            )),
        ])
    }

    pub fn claude_mcp_config(&self) -> Result<serde_json::Value, ProviderError> {
        let executable = managed_path_utf8(&self.asr_executable)?;
        Ok(serde_json::json!({
            "command": executable,
            "args": self.delegate_arguments(),
            "env": {},
        }))
    }
}

pub fn filtered_provider_environment(
    source: impl IntoIterator<Item = (OsString, OsString)>,
) -> Vec<(OsString, OsString)> {
    source
        .into_iter()
        .filter(|(key, _)| {
            key.to_str()
                .is_some_and(|key| allowed_environment_key(key) && !forbidden_environment_key(key))
        })
        .collect()
}

fn allowed_environment_key(key: &str) -> bool {
    matches!(
        key,
        "HOME"
            | "PATH"
            | "TMPDIR"
            | "TMP"
            | "TEMP"
            | "USER"
            | "LOGNAME"
            | "SHELL"
            | "LANG"
            | "TERM"
            | "SSL_CERT_FILE"
            | "SSL_CERT_DIR"
            | "NODE_EXTRA_CA_CERTS"
            | "HTTP_PROXY"
            | "HTTPS_PROXY"
            | "ALL_PROXY"
            | "NO_PROXY"
            | "http_proxy"
            | "https_proxy"
            | "all_proxy"
            | "no_proxy"
            | "OPENAI_API_KEY"
            | "CODEX_API_KEY"
            | "ANTHROPIC_API_KEY"
            | "CLAUDE_CODE_OAUTH_TOKEN"
    ) || key.starts_with("XDG_")
        || key.starts_with("LC_")
        || key.starts_with("ANTHROPIC_")
        || key.starts_with("OPENAI_")
}

fn forbidden_environment_key(key: &str) -> bool {
    matches!(
        key,
        "ROUTER_TOKEN"
            | "ROUTER_URL"
            | "ROUTER_PUBLIC_URL"
            | "ASR_CREDENTIAL_FILE"
            | "ASR_WORKSPACE"
            | "ASR_DATA_DIR"
            | "ASR_INTEGRATIONS_FILE"
            | "ASR_INTEGRATIONS_DIR"
            | "GITHUB_TOKEN"
            | "GITHUB_ENTERPRISE_TOKEN"
            | "LINEAR_API_KEY"
            | "DEBUG"
            | "PAGER"
            | "CLICOLOR_FORCE"
            | "CODEX_CWD"
            | "CODEX_THREAD_ID"
            | "CLAUDE_RESUME_SESSION_ID"
    ) || key.starts_with("ROUTER_TLS_")
        || key.starts_with("GATEWAY_")
        || key.starts_with("AGENT_ROUTER_")
        || key.starts_with("GH_")
}

pub(crate) async fn drain_stderr(mut stderr: tokio::process::ChildStderr) {
    use tokio::io::AsyncReadExt as _;

    let mut buffer = [0_u8; STDERR_DRAIN_BUFFER_BYTES];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

pub(crate) fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

pub(crate) async fn terminate_and_reap(child: &mut tokio::process::Child) -> bool {
    if child.try_wait().ok().flatten().is_some() {
        return true;
    }
    let _ = child.start_kill();
    child.wait().await.is_ok()
}

pub(crate) async fn graceful_reap(child: &mut tokio::process::Child, grace: Duration) -> bool {
    if child.try_wait().ok().flatten().is_some() {
        return true;
    }
    match tokio::time::timeout(grace, child.wait()).await {
        Ok(result) => result.is_ok(),
        Err(_) => terminate_and_reap(child).await,
    }
}
