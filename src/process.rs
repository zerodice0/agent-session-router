use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    fmt,
    fs::{self, File},
    future::{Future, pending},
    io::{self, Read as _, Write as _},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    os::unix::fs::MetadataExt as _,
    path::{Path, PathBuf},
    pin::Pin,
    process::{Command, ExitStatus, Stdio},
    sync::mpsc as std_mpsc,
    thread,
    time::{Duration, Instant as StdInstant},
};

use rustix::{
    fs::{FlockOperation, Mode, OFlags, flock, open},
    process::{Pid, Signal, getuid, kill_process, kill_process_group},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    process::{Child, ChildStdin, ChildStdout, Command as TokioCommand},
    signal::unix::{Signal as SignalStream, SignalKind, signal},
    time::timeout,
};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LaunchMode {
    /// Replace the CLI process so the provider owns the existing foreground terminal.
    ReplaceForeground,
    /// Run a finite helper and map its exit status after it returns.
    Wait,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchPlan {
    pub program: OsString,
    pub arguments: Vec<OsString>,
    pub caller_cwd: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
    pub mode: LaunchMode,
}

impl LaunchPlan {
    #[must_use]
    pub fn passthrough(
        program: impl Into<OsString>,
        caller_cwd: impl Into<PathBuf>,
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Self {
        Self {
            program: program.into(),
            arguments: arguments.into_iter().collect(),
            caller_cwd: caller_cwd.into(),
            environment: Vec::new(),
            mode: LaunchMode::ReplaceForeground,
        }
    }

    /// Stock Codex must receive the caller's directory through its explicit `-C` flag.
    #[must_use]
    pub fn stock_codex(
        program: impl Into<OsString>,
        caller_cwd: impl Into<PathBuf>,
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Self {
        let caller_cwd = caller_cwd.into();
        let mut planned_arguments = Vec::new();
        planned_arguments.push(OsString::from("-C"));
        planned_arguments.push(caller_cwd.as_os_str().to_owned());
        planned_arguments.extend(arguments);
        Self {
            program: program.into(),
            arguments: planned_arguments,
            caller_cwd,
            environment: Vec::new(),
            mode: LaunchMode::ReplaceForeground,
        }
    }

    /// Owned Codex App Server receives `CODEX_CWD`; all launchers also retain caller cwd.
    #[must_use]
    pub fn owned_codex(
        program: impl Into<OsString>,
        caller_cwd: impl Into<PathBuf>,
        configured_codex_cwd: Option<OsString>,
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Self {
        let caller_cwd = caller_cwd.into();
        let codex_cwd = configured_codex_cwd.unwrap_or_else(|| caller_cwd.as_os_str().to_owned());
        Self {
            program: program.into(),
            arguments: arguments.into_iter().collect(),
            caller_cwd,
            environment: vec![(OsString::from("CODEX_CWD"), codex_cwd)],
            mode: LaunchMode::ReplaceForeground,
        }
    }

    /// Plan the project-local Claude MCP registration using an absolute current executable.
    pub fn setup_claude(
        claude_program: impl Into<OsString>,
        caller_cwd: impl Into<PathBuf>,
        current_exe: impl Into<PathBuf>,
    ) -> Result<Self, LaunchError> {
        let current_exe = current_exe.into();
        if !current_exe.is_absolute() {
            return Err(LaunchError::InvalidPlan(
                "setup-claude requires an absolute current executable".to_owned(),
            ));
        }
        Ok(Self {
            program: claude_program.into(),
            arguments: vec![
                OsString::from("mcp"),
                OsString::from("add"),
                OsString::from("--transport"),
                OsString::from("stdio"),
                OsString::from("--scope"),
                OsString::from("local"),
                OsString::from("agent-session-router-channel"),
                OsString::from("--"),
                current_exe.into_os_string(),
                OsString::from("mcp"),
                OsString::from("claude-channel"),
            ],
            caller_cwd: caller_cwd.into(),
            environment: Vec::new(),
            mode: LaunchMode::Wait,
        })
    }

    #[must_use]
    pub fn with_environment(
        mut self,
        key: impl Into<OsString>,
        value: impl Into<OsString>,
    ) -> Self {
        self.environment.push((key.into(), value.into()));
        self
    }

    #[must_use]
    pub const fn with_mode(mut self, mode: LaunchMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn validate(&self) -> Result<(), LaunchError> {
        if self.program.is_empty() {
            return Err(LaunchError::InvalidPlan(
                "process program must not be empty".to_owned(),
            ));
        }
        if !self.caller_cwd.is_absolute() {
            return Err(LaunchError::InvalidPlan(
                "caller cwd must be absolute".to_owned(),
            ));
        }
        if self.environment.iter().any(|(key, _)| key.is_empty()) {
            return Err(LaunchError::InvalidPlan(
                "environment variable name must not be empty".to_owned(),
            ));
        }
        Ok(())
    }

    fn command(&self) -> Result<Command, LaunchError> {
        self.validate()?;
        let mut command = Command::new(&self.program);
        command
            .args(&self.arguments)
            .current_dir(&self.caller_cwd)
            .envs(self.environment.iter().map(|(key, value)| (key, value)));
        Ok(command)
    }
}

#[derive(Debug)]
pub enum LaunchError {
    InvalidPlan(String),
    Spawn(io::Error),
}

impl fmt::Display for LaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan(message) => formatter.write_str(message),
            Self::Spawn(error) => write!(formatter, "process launch failed: {error}"),
        }
    }
}

impl std::error::Error for LaunchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidPlan(_) => None,
            Self::Spawn(error) => Some(error),
        }
    }
}

/// Execute a validated plan. Foreground providers replace the current Unix process rather
/// than being detached into a new process group.
pub fn execute(plan: &LaunchPlan) -> Result<i32, LaunchError> {
    let mut command = plan.command()?;
    match plan.mode {
        LaunchMode::Wait => command
            .status()
            .map(map_exit_status)
            .map_err(LaunchError::Spawn),
        LaunchMode::ReplaceForeground => execute_foreground(command),
    }
}

#[cfg(unix)]
fn execute_foreground(mut command: Command) -> Result<i32, LaunchError> {
    use std::os::unix::process::CommandExt as _;

    Err(LaunchError::Spawn(command.exec()))
}

#[cfg(not(unix))]
fn execute_foreground(mut command: Command) -> Result<i32, LaunchError> {
    command
        .status()
        .map(map_exit_status)
        .map_err(LaunchError::Spawn)
}

#[must_use]
pub fn map_exit_status(status: ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return if (0..=255).contains(&code) { code } else { 1 };
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if status.signal() == Some(2) {
            return 130;
        }
    }
    1
}

pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_STARTUP_FRAME_BYTES: usize = 4 * 1024;
pub const MAX_RUNTIME_RECORD_BYTES: usize = 64 * 1024;
pub const MAX_COMMAND_OUTPUT_BYTES: usize = 1024 * 1024;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const RUNTIME_FILE_NAME: &str = "router-runtime.json";
const LOCK_FILE_NAME: &str = "launcher.lock";

pub type ProcessFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeShareMode {
    Local,
    Tailscale,
    Lan,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OwnedServe {
    pub instance_id: Uuid,
    pub port: u16,
    pub target: String,
}

impl OwnedServe {
    #[must_use]
    pub fn loopback(instance_id: Uuid, port: u16) -> Self {
        Self {
            instance_id,
            port,
            target: format!("tcp://127.0.0.1:{port}"),
        }
    }

    fn validate(&self, record_instance: Uuid) -> Result<(), RouterLaunchError> {
        if self.instance_id != record_instance
            || self.port == 0
            || self.target != format!("tcp://127.0.0.1:{}", self.port)
        {
            return Err(RouterLaunchError::InvalidRuntimeRecord);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeRecord {
    pub instance_id: Uuid,
    pub control_url: String,
    pub share_mode: RuntimeShareMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertised_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned_serve: Option<OwnedServe>,
}

impl RuntimeRecord {
    pub fn validate(&self) -> Result<(), RouterLaunchError> {
        if self.instance_id.is_nil() {
            return Err(RouterLaunchError::InvalidRuntimeRecord);
        }
        validate_router_url(&self.control_url)?;
        if let Some(url) = &self.advertised_url {
            validate_router_url(url)?;
        }
        match (self.share_mode, &self.owned_serve) {
            (RuntimeShareMode::Tailscale, Some(serve)) => serve.validate(self.instance_id)?,
            (RuntimeShareMode::Tailscale, None)
            | (RuntimeShareMode::Local | RuntimeShareMode::Lan, Some(_)) => {
                return Err(RouterLaunchError::InvalidRuntimeRecord);
            }
            (RuntimeShareMode::Local | RuntimeShareMode::Lan, None) => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareRequest {
    Local,
    Auto,
    Tailscale,
    Lan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlsSettings {
    pub certificate_file: PathBuf,
    pub private_key_file: PathBuf,
    pub public_url: String,
    pub ca_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartOptions {
    pub bind: SocketAddr,
    pub share: ShareRequest,
    pub tls: Option<TlsSettings>,
    pub background: bool,
    pub instance_id: Option<Uuid>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartOutcome {
    Reused(RuntimeRecord),
    Started(RuntimeRecord),
    ForegroundExited { record: RuntimeRecord, code: i32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StopOutcome {
    NotRunning,
    StaleRecovered,
    Stopped(RuntimeRecord),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeFailure {
    ConnectionRefused,
    Tls,
    Authentication,
    MarkerMismatch,
    Transport,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HealthMarker {
    pub service: String,
    pub protocol_version: u8,
    pub status: String,
    pub instance_id: Uuid,
}

impl HealthMarker {
    pub(crate) fn verify(&self, expected_instance: Uuid) -> Result<(), RouterLaunchError> {
        if self.service != "agent-session-router"
            || self.protocol_version != 2
            || self.status != "ok"
            || self.instance_id != expected_instance
        {
            return Err(RouterLaunchError::Health(ProbeFailure::MarkerMismatch));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TailscaleSnapshot {
    pub backend_running: bool,
    pub self_ipv4: Vec<Ipv4Addr>,
    pub online_peer_ipv4: Vec<Ipv4Addr>,
    pub tcp_forwards: BTreeMap<u16, String>,
}

impl TailscaleSnapshot {
    #[must_use]
    pub fn all_online_ipv4(&self) -> HashSet<Ipv4Addr> {
        self.self_ipv4
            .iter()
            .chain(&self.online_peer_ipv4)
            .copied()
            .collect()
    }

    pub fn validate(&self) -> Result<(), RouterLaunchError> {
        if !self.backend_running
            || self.self_ipv4.is_empty()
            || self
                .self_ipv4
                .iter()
                .chain(&self.online_peer_ipv4)
                .any(|address| !is_tailscale_ipv4(*address))
        {
            return Err(RouterLaunchError::TailscaleUnavailable);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum RouterLaunchError {
    #[error("router_busy")]
    RouterBusy,
    #[error("router_instance_changed")]
    InstanceChanged,
    #[error("interrupted")]
    Interrupted,
    #[error("launcher_permissions")]
    Permissions,
    #[error("launcher_io")]
    Io,
    #[error("runtime_record_invalid")]
    InvalidRuntimeRecord,
    #[error("runtime_record_too_large")]
    RuntimeRecordTooLarge,
    #[error("startup_protocol_error")]
    StartupProtocol,
    #[error("startup_timeout")]
    StartupTimeout,
    #[error("shutdown_timeout")]
    ShutdownTimeout,
    #[error("child_launch_failed")]
    ChildLaunch,
    #[error("child_terminated")]
    ChildTerminated,
    #[error("health_{0:?}")]
    Health(ProbeFailure),
    #[error("admin_authentication_failed")]
    AdminAuthentication,
    #[error("admin_protocol_error")]
    AdminProtocol,
    #[error("tls_configuration_required")]
    TlsRequired,
    #[error("tls_configuration_invalid")]
    TlsInvalid,
    #[error("tailscale_unavailable")]
    TailscaleUnavailable,
    #[error("tailscale_status_invalid")]
    TailscaleStatus,
    #[error("tailscale_mapping_conflict")]
    TailscaleMappingConflict,
    #[error("tailscale_command_failed")]
    TailscaleCommand,
    #[error("profile_update_failed")]
    Profile,
}

pub struct LauncherLock {
    file: File,
}

impl LauncherLock {
    pub fn acquire(data_dir: &Path) -> Result<Self, RouterLaunchError> {
        validate_private_directory(data_dir)?;
        let path = data_dir.join(LOCK_FILE_NAME);
        let existed = fs::symlink_metadata(&path).is_ok();
        let descriptor = open(
            &path,
            OFlags::RDWR | OFlags::CREATE | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| RouterLaunchError::Permissions)?;
        let file = File::from(descriptor);
        validate_owned_file_0600(&file.metadata().map_err(|_| RouterLaunchError::Io)?)?;
        if !existed {
            file.sync_all().map_err(|_| RouterLaunchError::Io)?;
            sync_directory(data_dir)?;
        }
        match flock(&file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => Ok(Self { file }),
            Err(error)
                if error == rustix::io::Errno::WOULDBLOCK || error == rustix::io::Errno::AGAIN =>
            {
                Err(RouterLaunchError::RouterBusy)
            }
            Err(_) => Err(RouterLaunchError::Io),
        }
    }

    #[must_use]
    pub fn file(&self) -> &File {
        &self.file
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeStore {
    data_dir: PathBuf,
}

impl RuntimeStore {
    pub fn new(data_dir: PathBuf) -> Result<Self, RouterLaunchError> {
        validate_private_directory(&data_dir)?;
        Ok(Self { data_dir })
    }

    #[must_use]
    pub fn runtime_path(&self) -> PathBuf {
        self.data_dir.join(RUNTIME_FILE_NAME)
    }

    pub fn lock(&self) -> Result<LauncherLock, RouterLaunchError> {
        LauncherLock::acquire(&self.data_dir)
    }

    pub fn read(&self) -> Result<Option<RuntimeRecord>, RouterLaunchError> {
        let path = self.runtime_path();
        let descriptor = match open(
            &path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(descriptor) => descriptor,
            Err(error) if error == rustix::io::Errno::NOENT => return Ok(None),
            Err(_) => return Err(RouterLaunchError::Permissions),
        };
        let file = File::from(descriptor);
        validate_owned_file_0600(&file.metadata().map_err(|_| RouterLaunchError::Io)?)?;
        let mut bytes = Vec::new();
        file.take((MAX_RUNTIME_RECORD_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| RouterLaunchError::Io)?;
        if bytes.len() > MAX_RUNTIME_RECORD_BYTES {
            return Err(RouterLaunchError::RuntimeRecordTooLarge);
        }
        let record: RuntimeRecord =
            serde_json::from_slice(&bytes).map_err(|_| RouterLaunchError::InvalidRuntimeRecord)?;
        record.validate()?;
        Ok(Some(record))
    }

    pub fn write(&self, record: &RuntimeRecord) -> Result<(), RouterLaunchError> {
        record.validate()?;
        if fs::symlink_metadata(self.runtime_path()).is_ok() {
            self.read()?;
        }
        let payload =
            serde_json::to_vec(record).map_err(|_| RouterLaunchError::InvalidRuntimeRecord)?;
        if payload.len() > MAX_RUNTIME_RECORD_BYTES {
            return Err(RouterLaunchError::RuntimeRecordTooLarge);
        }
        let temporary = self
            .data_dir
            .join(format!(".asr-runtime-{}.tmp", Uuid::new_v4()));
        let descriptor = open(
            &temporary,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| RouterLaunchError::Io)?;
        let mut file = File::from(descriptor);
        let result = (|| {
            file.write_all(&payload)?;
            file.sync_all()?;
            fs::rename(&temporary, self.runtime_path())?;
            File::open(&self.data_dir)?.sync_all()
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
            return Err(RouterLaunchError::Io);
        }
        Ok(())
    }

    pub fn remove_if_instance(&self, instance_id: Uuid) -> Result<bool, RouterLaunchError> {
        let Some(record) = self.read()? else {
            return Ok(false);
        };
        if record.instance_id != instance_id {
            return Ok(false);
        }
        fs::remove_file(self.runtime_path()).map_err(|_| RouterLaunchError::Io)?;
        sync_directory(&self.data_dir)?;
        Ok(true)
    }
}

fn validate_private_directory(path: &Path) -> Result<(), RouterLaunchError> {
    if !path.is_absolute() {
        return Err(RouterLaunchError::Permissions);
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| RouterLaunchError::Permissions)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != getuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(RouterLaunchError::Permissions);
    }
    Ok(())
}

fn validate_private_file_metadata(metadata: &fs::Metadata) -> Result<(), RouterLaunchError> {
    if !metadata.is_file() || metadata.uid() != getuid().as_raw() || metadata.mode() & 0o077 != 0 {
        return Err(RouterLaunchError::Permissions);
    }
    Ok(())
}

fn validate_owned_file_0600(metadata: &fs::Metadata) -> Result<(), RouterLaunchError> {
    validate_private_file_metadata(metadata)?;
    if metadata.mode() & 0o777 != 0o600 {
        return Err(RouterLaunchError::Permissions);
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), RouterLaunchError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| RouterLaunchError::Io)
}

pub fn tls_settings_from_environment(
    bind: SocketAddr,
    environment: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<Option<TlsSettings>, RouterLaunchError> {
    let mut certificate = None;
    let mut private_key = None;
    let mut public_url = None;
    let mut ca_file = None;
    for (key, value) in environment {
        match key.to_str() {
            Some("ROUTER_TLS_CERT") => certificate = Some(PathBuf::from(value)),
            Some("ROUTER_TLS_KEY") => private_key = Some(PathBuf::from(value)),
            Some("ROUTER_PUBLIC_URL") => public_url = value.into_string().ok(),
            Some("ASR_CA_FILE") => ca_file = Some(PathBuf::from(value)),
            _ => {}
        }
    }
    let present = [
        certificate.is_some(),
        private_key.is_some(),
        public_url.is_some(),
    ];
    if present.iter().any(|value| *value) && !present.iter().all(|value| *value) {
        return Err(RouterLaunchError::TlsInvalid);
    }
    let tls = match (certificate, private_key, public_url) {
        (Some(certificate_file), Some(private_key_file), Some(public_url)) => {
            let cert_metadata =
                fs::metadata(&certificate_file).map_err(|_| RouterLaunchError::TlsInvalid)?;
            if !cert_metadata.is_file() {
                return Err(RouterLaunchError::TlsInvalid);
            }
            let key = open(
                &private_key_file,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(|_| RouterLaunchError::TlsInvalid)?;
            validate_private_file_metadata(
                &File::from(key)
                    .metadata()
                    .map_err(|_| RouterLaunchError::TlsInvalid)?,
            )
            .map_err(|_| RouterLaunchError::TlsInvalid)?;
            validate_public_router_url(&public_url, bind.port())?;
            Some(TlsSettings {
                certificate_file,
                private_key_file,
                public_url,
                ca_file,
            })
        }
        (None, None, None) => None,
        _ => return Err(RouterLaunchError::TlsInvalid),
    };
    if !bind.ip().is_loopback() && tls.is_none() {
        return Err(RouterLaunchError::TlsRequired);
    }
    Ok(tls)
}

pub fn resolve_share_mode(
    request: ShareRequest,
    bind: SocketAddr,
    tls: Option<&TlsSettings>,
    tailscale: Option<&TailscaleSnapshot>,
) -> Result<RuntimeShareMode, RouterLaunchError> {
    match request {
        ShareRequest::Local => {
            if !bind.ip().is_loopback() {
                return Err(RouterLaunchError::TlsRequired);
            }
            Ok(RuntimeShareMode::Local)
        }
        ShareRequest::Lan => {
            if bind.ip().is_loopback() || tls.is_none() {
                return Err(RouterLaunchError::TlsRequired);
            }
            Ok(RuntimeShareMode::Lan)
        }
        ShareRequest::Tailscale => {
            if tls.is_some() || !bind.ip().is_loopback() {
                return Err(RouterLaunchError::TlsInvalid);
            }
            tailscale
                .ok_or(RouterLaunchError::TailscaleUnavailable)?
                .validate()?;
            Ok(RuntimeShareMode::Tailscale)
        }
        ShareRequest::Auto => {
            if tls.is_some() {
                return if bind.ip().is_loopback() {
                    Ok(RuntimeShareMode::Local)
                } else {
                    Ok(RuntimeShareMode::Lan)
                };
            }
            if !bind.ip().is_loopback() {
                return Err(RouterLaunchError::TlsRequired);
            }
            if let Some(snapshot) = tailscale
                && snapshot.validate().is_ok()
            {
                return Ok(RuntimeShareMode::Tailscale);
            }
            Ok(RuntimeShareMode::Local)
        }
    }
}

pub fn validate_tailscale_router_url(
    router_url: &str,
    snapshot: &TailscaleSnapshot,
) -> Result<(), RouterLaunchError> {
    snapshot.validate()?;
    let url = Url::parse(router_url).map_err(|_| RouterLaunchError::TailscaleStatus)?;
    if url.scheme() != "ws"
        || url.path() != "/ws"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(RouterLaunchError::TailscaleStatus);
    }
    let address = url
        .host_str()
        .and_then(|host| host.parse::<Ipv4Addr>().ok())
        .filter(|address| snapshot.all_online_ipv4().contains(address))
        .ok_or(RouterLaunchError::TailscaleStatus)?;
    if !is_tailscale_ipv4(address) {
        return Err(RouterLaunchError::TailscaleStatus);
    }
    url.port().ok_or(RouterLaunchError::TailscaleStatus)?;
    Ok(())
}

fn validate_router_url(value: &str) -> Result<Url, RouterLaunchError> {
    let url = Url::parse(value).map_err(|_| RouterLaunchError::InvalidRuntimeRecord)?;
    if !matches!(url.scheme(), "ws" | "wss")
        || url.host_str().is_none()
        || url.path() != "/ws"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(RouterLaunchError::InvalidRuntimeRecord);
    }
    Ok(url)
}

fn validate_public_router_url(value: &str, expected_port: u16) -> Result<(), RouterLaunchError> {
    let url = validate_router_url(value).map_err(|_| RouterLaunchError::TlsInvalid)?;
    if url.scheme() != "wss" || url.port_or_known_default() != Some(expected_port) {
        return Err(RouterLaunchError::TlsInvalid);
    }
    Ok(())
}

fn is_tailscale_ipv4(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    octets[0] == 100 && (64..=127).contains(&octets[1])
}

pub trait HealthProbe: Send {
    fn probe<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<HealthMarker, ProbeFailure>>;
}

pub trait AdminControl: Send {
    fn verify_owner<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>>;

    fn shutdown<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>>;

    fn wait_stopped<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
        deadline: Duration,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>>;
}

pub trait TailscaleControl: Send {
    fn snapshot(&mut self) -> ProcessFuture<'_, Result<TailscaleSnapshot, RouterLaunchError>>;

    fn enable<'a>(
        &'a mut self,
        serve: &'a OwnedServe,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>>;

    fn disable<'a>(
        &'a mut self,
        serve: &'a OwnedServe,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>>;
}

pub trait ProfilePublisher: Send {
    fn publish(
        &mut self,
        router_url: &str,
        record: &RuntimeRecord,
    ) -> Result<(), RouterLaunchError>;
}

pub trait ChildSupervisor: Send {
    fn configure_share_mode(
        &mut self,
        _share_mode: RuntimeShareMode,
    ) -> Result<(), RouterLaunchError> {
        Ok(())
    }
    fn launch(
        &mut self,
        expected_instance: Uuid,
        background: bool,
    ) -> ProcessFuture<'_, Result<StartupReady, RouterLaunchError>>;

    fn acknowledge(
        &mut self,
        instance_id: Uuid,
    ) -> ProcessFuture<'_, Result<(), RouterLaunchError>>;

    fn terminate(&mut self) -> ProcessFuture<'_, Result<(), RouterLaunchError>>;

    fn wait(&mut self) -> ProcessFuture<'_, Result<i32, RouterLaunchError>>;

    fn detach(&mut self);
}

pub struct NativeLauncher<H, A, T, P, C> {
    store: RuntimeStore,
    pub health: H,
    pub admin: A,
    pub tailscale: T,
    pub profiles: P,
    pub child: C,
}

impl<H, A, T, P, C> NativeLauncher<H, A, T, P, C>
where
    H: HealthProbe,
    A: AdminControl,
    T: TailscaleControl,
    P: ProfilePublisher,
    C: ChildSupervisor,
{
    pub fn new(
        store: RuntimeStore,
        health: H,
        admin: A,
        tailscale: T,
        profiles: P,
        child: C,
    ) -> Self {
        Self {
            store,
            health,
            admin,
            tailscale,
            profiles,
            child,
        }
    }

    pub async fn start(
        &mut self,
        options: StartOptions,
    ) -> Result<StartOutcome, RouterLaunchError> {
        let launcher_lock = self.store.lock()?;
        if let Some(record) = self.store.read()? {
            match self.health.probe(&record).await {
                Ok(marker) => {
                    verify_health_marker(&marker, record.instance_id)?;
                    self.admin.verify_owner(&record).await?;
                    return Ok(StartOutcome::Reused(record));
                }
                Err(ProbeFailure::ConnectionRefused) => {
                    cleanup_owned_serve(&mut self.tailscale, &record).await?;
                    self.store.remove_if_instance(record.instance_id)?;
                }
                Err(error) => return Err(RouterLaunchError::Health(error)),
            }
        }

        let mut initial_snapshot = None;
        if matches!(options.share, ShareRequest::Auto | ShareRequest::Tailscale) {
            match self.tailscale.snapshot().await {
                Ok(snapshot) => initial_snapshot = Some(snapshot),
                Err(error) if options.share == ShareRequest::Auto => {
                    let _ = error;
                }
                Err(error) => return Err(error),
            }
        }
        let share_mode = resolve_share_mode(
            options.share,
            options.bind,
            options.tls.as_ref(),
            initial_snapshot.as_ref(),
        )?;
        let instance_id = options.instance_id.unwrap_or_else(Uuid::new_v4);
        if instance_id.is_nil() {
            return Err(RouterLaunchError::StartupProtocol);
        }
        self.child.configure_share_mode(share_mode)?;
        let ready = match self.child.launch(instance_id, options.background).await {
            Ok(ready) => ready,
            Err(error) => {
                let _ = self.child.terminate().await;
                return Err(error);
            }
        };
        if ready.instance_id != instance_id {
            let _ = self.child.terminate().await;
            return Err(RouterLaunchError::StartupProtocol);
        }
        let control = match validate_ready_control_url(&ready.control_url, &options) {
            Ok(control) => control,
            Err(error) => {
                let _ = self.child.terminate().await;
                return Err(error);
            }
        };
        let Some(port) = control.port_or_known_default() else {
            let _ = self.child.terminate().await;
            return Err(RouterLaunchError::StartupProtocol);
        };
        let owned_serve = if share_mode == RuntimeShareMode::Tailscale {
            let snapshot = match self.tailscale.snapshot().await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let _ = self.child.terminate().await;
                    return Err(error);
                }
            };
            if let Err(error) = snapshot.validate() {
                let _ = self.child.terminate().await;
                return Err(error);
            }
            if snapshot.tcp_forwards.contains_key(&port) {
                let _ = self.child.terminate().await;
                return Err(RouterLaunchError::TailscaleMappingConflict);
            }
            Some(OwnedServe::loopback(instance_id, port))
        } else {
            None
        };
        let mut record = RuntimeRecord {
            instance_id,
            control_url: ready.control_url,
            share_mode,
            advertised_url: None,
            owned_serve,
        };
        if let Err(error) = self.store.write(&record) {
            let _ = self.child.terminate().await;
            return Err(error);
        }
        if let Err(error) = self.child.acknowledge(instance_id).await {
            let _ = self.child.terminate().await;
            self.store.remove_if_instance(instance_id)?;
            return Err(error);
        }

        let setup = timeout(STARTUP_TIMEOUT, self.finish_start(&mut record)).await;
        let setup = match setup {
            Ok(result) => result,
            Err(_) => Err(RouterLaunchError::StartupTimeout),
        };
        if let Err(error) = setup {
            let cleanup = self.cleanup_failed_start(&record).await;
            return Err(cleanup.err().unwrap_or(error));
        }

        if options.background {
            self.child.detach();
            return Ok(StartOutcome::Started(record));
        }

        drop(launcher_lock);
        let wait_result = self.child.wait().await;
        let _cleanup_lock = acquire_lock_with_retry(&self.store, SHUTDOWN_TIMEOUT).await?;
        if self
            .store
            .read()?
            .is_some_and(|current| current.instance_id == record.instance_id)
        {
            cleanup_owned_serve(&mut self.tailscale, &record).await?;
            self.store.remove_if_instance(record.instance_id)?;
        }
        let code = wait_result?;
        Ok(StartOutcome::ForegroundExited { record, code })
    }

    async fn finish_start(&mut self, record: &mut RuntimeRecord) -> Result<(), RouterLaunchError> {
        let marker = self
            .health
            .probe(record)
            .await
            .map_err(RouterLaunchError::Health)?;
        verify_health_marker(&marker, record.instance_id)?;
        self.admin.verify_owner(record).await?;

        let published_url = match record.share_mode {
            RuntimeShareMode::Local => record.control_url.clone(),
            RuntimeShareMode::Lan => {
                let control = Url::parse(&record.control_url)
                    .map_err(|_| RouterLaunchError::InvalidRuntimeRecord)?;
                if control.scheme() != "wss" {
                    return Err(RouterLaunchError::TlsRequired);
                }
                record.control_url.clone()
            }
            RuntimeShareMode::Tailscale => {
                let serve = record
                    .owned_serve
                    .as_ref()
                    .ok_or(RouterLaunchError::InvalidRuntimeRecord)?;
                self.tailscale.enable(serve).await?;
                let snapshot = self.tailscale.snapshot().await?;
                snapshot.validate()?;
                if snapshot.tcp_forwards.get(&serve.port) != Some(&serve.target) {
                    return Err(RouterLaunchError::TailscaleMappingConflict);
                }
                let address = snapshot
                    .self_ipv4
                    .first()
                    .ok_or(RouterLaunchError::TailscaleUnavailable)?;
                format!("ws://{address}:{}/ws", serve.port)
            }
        };
        if record.share_mode != RuntimeShareMode::Local {
            record.advertised_url = Some(published_url.clone());
            self.store.write(record)?;
        }
        self.profiles.publish(&published_url, record)?;
        Ok(())
    }

    async fn cleanup_failed_start(
        &mut self,
        record: &RuntimeRecord,
    ) -> Result<(), RouterLaunchError> {
        let serve_result = cleanup_owned_serve(&mut self.tailscale, record).await;
        let child_result = self.child.terminate().await;
        if serve_result.is_ok() && child_result.is_ok() {
            self.store.remove_if_instance(record.instance_id)?;
        }
        serve_result?;
        child_result
    }

    pub async fn stop(&mut self) -> Result<StopOutcome, RouterLaunchError> {
        self.stop_locked(None).await
    }

    pub async fn stop_if_instance(
        &mut self,
        expected: Uuid,
    ) -> Result<StopOutcome, RouterLaunchError> {
        self.stop_locked(Some(expected)).await
    }

    async fn stop_locked(
        &mut self,
        expected: Option<Uuid>,
    ) -> Result<StopOutcome, RouterLaunchError> {
        let _launcher_lock = self.store.lock()?;
        let Some(record) = self.store.read()? else {
            return Ok(StopOutcome::NotRunning);
        };
        // Fence the selected console instance before even probing or cleaning stale state.
        if expected.is_some_and(|expected| expected != record.instance_id) {
            return Err(RouterLaunchError::InstanceChanged);
        }
        match self.health.probe(&record).await {
            Ok(marker) => verify_health_marker(&marker, record.instance_id)?,
            Err(ProbeFailure::ConnectionRefused) => {
                cleanup_owned_serve(&mut self.tailscale, &record).await?;
                self.store.remove_if_instance(record.instance_id)?;
                return Ok(StopOutcome::StaleRecovered);
            }
            Err(error) => return Err(RouterLaunchError::Health(error)),
        }
        self.admin.verify_owner(&record).await?;
        self.admin.shutdown(&record).await?;
        timeout(
            SHUTDOWN_TIMEOUT,
            self.admin.wait_stopped(&record, SHUTDOWN_TIMEOUT),
        )
        .await
        .map_err(|_| RouterLaunchError::ShutdownTimeout)??;
        cleanup_owned_serve(&mut self.tailscale, &record).await?;
        self.store.remove_if_instance(record.instance_id)?;
        Ok(StopOutcome::Stopped(record))
    }
}

fn verify_health_marker(marker: &HealthMarker, instance_id: Uuid) -> Result<(), RouterLaunchError> {
    marker
        .verify(instance_id)
        .map_err(|_| RouterLaunchError::Health(ProbeFailure::MarkerMismatch))
}

async fn acquire_lock_with_retry(
    store: &RuntimeStore,
    deadline: Duration,
) -> Result<LauncherLock, RouterLaunchError> {
    let expires = tokio::time::Instant::now() + deadline;
    loop {
        match store.lock() {
            Ok(lock) => return Ok(lock),
            Err(RouterLaunchError::RouterBusy) if tokio::time::Instant::now() < expires => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(RouterLaunchError::RouterBusy) => return Err(RouterLaunchError::RouterBusy),
            Err(error) => return Err(error),
        }
    }
}

async fn cleanup_owned_serve<T: TailscaleControl>(
    tailscale: &mut T,
    record: &RuntimeRecord,
) -> Result<(), RouterLaunchError> {
    let Some(serve) = &record.owned_serve else {
        return Ok(());
    };
    serve.validate(record.instance_id)?;
    let snapshot = tailscale.snapshot().await?;
    snapshot.validate()?;
    match snapshot.tcp_forwards.get(&serve.port) {
        None => Ok(()),
        Some(target) if target == &serve.target => {
            tailscale.disable(serve).await?;
            let after = tailscale.snapshot().await?;
            after.validate()?;
            if after.tcp_forwards.contains_key(&serve.port) {
                return Err(RouterLaunchError::TailscaleMappingConflict);
            }
            Ok(())
        }
        Some(_) => Err(RouterLaunchError::TailscaleMappingConflict),
    }
}

fn validate_ready_control_url(
    value: &str,
    options: &StartOptions,
) -> Result<Url, RouterLaunchError> {
    let url = validate_router_url(value).map_err(|_| RouterLaunchError::StartupProtocol)?;
    if let Some(tls) = &options.tls {
        if value != tls.public_url || url.scheme() != "wss" {
            return Err(RouterLaunchError::StartupProtocol);
        }
    } else {
        let host = url
            .host_str()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .ok_or(RouterLaunchError::StartupProtocol)?;
        if url.scheme() != "ws" || !host.is_loopback() {
            return Err(RouterLaunchError::StartupProtocol);
        }
        if options.bind.port() != 0 && url.port_or_known_default() != Some(options.bind.port()) {
            return Err(RouterLaunchError::StartupProtocol);
        }
    }
    Ok(url)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartupReady {
    pub instance_id: Uuid,
    pub control_url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StartupAck {
    instance_id: Uuid,
}

#[derive(Clone, Debug)]
pub struct NativeChildConfig {
    pub program: PathBuf,
    pub arguments: Vec<OsString>,
    pub cwd: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
    pub stderr_file: PathBuf,
    pub signal_process_group: bool,
}

/// Resolve bootstrap assets against the invoking process's working directory.
#[must_use]
pub fn bootstrap_assets_directory(
    data_dir: &Path,
    cwd: &Path,
    configured: Option<OsString>,
) -> PathBuf {
    let path = configured
        .filter(|value| !value.is_empty())
        .map_or_else(|| data_dir.join("bootstrap"), PathBuf::from);
    if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    }
}

pub struct NativeChildSupervisor {
    config: NativeChildConfig,
    startup_timeout: Duration,
    child: Option<Child>,
    startup_input: Option<ChildStdin>,
    startup_output: Option<ChildStdout>,
    foreground_interrupt: Option<SignalStream>,
}

impl NativeChildSupervisor {
    #[must_use]
    pub fn new(config: NativeChildConfig) -> Self {
        Self {
            config,
            startup_timeout: STARTUP_TIMEOUT,
            child: None,
            startup_input: None,
            startup_output: None,
            foreground_interrupt: None,
        }
    }

    /// Set the initial readiness budget without changing acknowledgement,
    /// health-probe, or shutdown deadlines.
    #[must_use]
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    async fn terminate_owned(&mut self) -> Result<(), RouterLaunchError> {
        let signal = if self.config.signal_process_group {
            Signal::TERM
        } else {
            Signal::KILL
        };
        self.terminate_owned_with_signal(signal, STARTUP_TIMEOUT)
            .await
    }

    async fn terminate_owned_with_signal(
        &mut self,
        signal: Signal,
        deadline: Duration,
    ) -> Result<(), RouterLaunchError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        if child
            .try_wait()
            .map_err(|_| RouterLaunchError::Io)?
            .is_none()
        {
            signal_owned_child(child, self.config.signal_process_group, signal);
            if let Ok(result) = timeout(deadline, child.wait()).await {
                result.map_err(|_| RouterLaunchError::Io)?;
            } else {
                signal_owned_child(child, self.config.signal_process_group, Signal::KILL);
                child.wait().await.map_err(|_| RouterLaunchError::Io)?;
            }
        }
        self.child.take();
        self.startup_input.take();
        self.startup_output.take();
        self.foreground_interrupt.take();
        Ok(())
    }
}

fn signal_owned_child(child: &Child, process_group: bool, signal: Signal) {
    let Some(pid) = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(Pid::from_raw)
    else {
        return;
    };
    // A retained, unreaped child owns this PID. Its group may not exist yet during startup.
    if !process_group || kill_process_group(pid, signal).is_err() {
        let _ = kill_process(pid, signal);
    }
}

async fn foreground_interrupt(stream: &mut Option<SignalStream>) {
    if let Some(stream) = stream
        && stream.recv().await.is_some()
    {
        return;
    }
    pending::<()>().await;
}

impl ChildSupervisor for NativeChildSupervisor {
    fn configure_share_mode(
        &mut self,
        share_mode: RuntimeShareMode,
    ) -> Result<(), RouterLaunchError> {
        self.config
            .environment
            .retain(|(key, _)| key != "ASR_RUNTIME_SHARE_MODE");
        self.config.environment.push((
            OsString::from("ASR_RUNTIME_SHARE_MODE"),
            OsString::from(match share_mode {
                RuntimeShareMode::Local => "local",
                RuntimeShareMode::Tailscale => "tailscale",
                RuntimeShareMode::Lan => "lan",
            }),
        ));
        Ok(())
    }
    fn launch(
        &mut self,
        expected_instance: Uuid,
        background: bool,
    ) -> ProcessFuture<'_, Result<StartupReady, RouterLaunchError>> {
        Box::pin(async move {
            if self.child.is_some()
                || !self.config.program.is_absolute()
                || !self.config.cwd.is_absolute()
            {
                return Err(RouterLaunchError::ChildLaunch);
            }
            // Install before spawning: readiness/acknowledgement can otherwise leave an orphan.
            self.foreground_interrupt = if background {
                None
            } else {
                Some(signal(SignalKind::interrupt()).map_err(|_| RouterLaunchError::Io)?)
            };
            let stderr = open_private_append(&self.config.stderr_file)?;
            let mut command = TokioCommand::new(&self.config.program);
            command
                .args(&self.config.arguments)
                .current_dir(&self.config.cwd)
                .env_clear()
                .envs(
                    self.config
                        .environment
                        .iter()
                        .map(|(key, value)| (key, value)),
                )
                .env("ASR_LAUNCH_INSTANCE_ID", expected_instance.to_string())
                .env("ASR_BACKGROUND_CHILD", if background { "1" } else { "0" })
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::from(stderr))
                .kill_on_drop(false);
            if self.config.signal_process_group && !background {
                command.process_group(0);
            }
            let mut child = command
                .spawn()
                .map_err(|_| RouterLaunchError::ChildLaunch)?;
            self.startup_input = child.stdin.take();
            self.startup_output = child.stdout.take();
            self.child = Some(child);
            let output = self
                .startup_output
                .as_mut()
                .ok_or(RouterLaunchError::ChildLaunch)?;
            let ready = tokio::select! {
                biased;
                () = foreground_interrupt(&mut self.foreground_interrupt) => {
                    Err(RouterLaunchError::Interrupted)
                }
                ready = read_startup_ready(output, self.startup_timeout) => ready,
            };
            let ready = match ready {
                Ok(ready) => ready,
                Err(error) => {
                    if matches!(error, RouterLaunchError::Interrupted) {
                        self.terminate_owned_with_signal(Signal::INT, SHUTDOWN_TIMEOUT)
                            .await?;
                    } else {
                        self.terminate_owned().await?;
                    }
                    return Err(error);
                }
            };
            Ok(ready)
        })
    }

    fn acknowledge(
        &mut self,
        instance_id: Uuid,
    ) -> ProcessFuture<'_, Result<(), RouterLaunchError>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(&StartupAck { instance_id })
                .map_err(|_| RouterLaunchError::StartupProtocol)?;
            let input = self
                .startup_input
                .as_mut()
                .ok_or(RouterLaunchError::StartupProtocol)?;
            timeout(STARTUP_TIMEOUT, async {
                input.write_all(&payload).await?;
                input.write_all(b"\n").await?;
                input.flush().await
            })
            .await
            .map_err(|_| RouterLaunchError::StartupTimeout)?
            .map_err(|_| RouterLaunchError::StartupProtocol)?;
            self.startup_input.take();
            if let Some(mut output) = self.startup_output.take() {
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 1024];
                    loop {
                        match output.read(&mut buffer).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {}
                        }
                    }
                });
            }
            Ok(())
        })
    }

    fn terminate(&mut self) -> ProcessFuture<'_, Result<(), RouterLaunchError>> {
        Box::pin(self.terminate_owned())
    }

    fn wait(&mut self) -> ProcessFuture<'_, Result<i32, RouterLaunchError>> {
        Box::pin(async move {
            self.startup_input.take();
            self.startup_output.take();
            let child = self
                .child
                .as_mut()
                .ok_or(RouterLaunchError::ChildTerminated)?;
            let status = tokio::select! {
                biased;
                () = foreground_interrupt(&mut self.foreground_interrupt) => None,
                status = child.wait() => Some(status),
            };
            if let Some(status) = status {
                self.child.take();
                self.foreground_interrupt.take();
                Ok(map_exit_status(status.map_err(|_| RouterLaunchError::Io)?))
            } else {
                self.terminate_owned_with_signal(Signal::INT, SHUTDOWN_TIMEOUT)
                    .await?;
                Ok(130)
            }
        })
    }

    fn detach(&mut self) {
        self.startup_input.take();
        self.startup_output.take();
        self.child.take();
        self.foreground_interrupt.take();
    }
}

pub fn enter_owned_process_group(background: bool) -> Result<(), RouterLaunchError> {
    if background {
        rustix::process::setsid().map_err(|_| RouterLaunchError::ChildLaunch)?;
    } else {
        rustix::process::setpgid(None, None).map_err(|_| RouterLaunchError::ChildLaunch)?;
    }
    Ok(())
}

pub async fn child_startup_handshake<W, R>(
    output: &mut W,
    input: &mut R,
    ready: &StartupReady,
) -> Result<(), RouterLaunchError>
where
    W: tokio::io::AsyncWrite + Unpin,
    R: tokio::io::AsyncRead + Unpin,
{
    ready.validate()?;
    let payload = serde_json::to_vec(ready).map_err(|_| RouterLaunchError::StartupProtocol)?;
    if payload.len() > MAX_STARTUP_FRAME_BYTES {
        return Err(RouterLaunchError::StartupProtocol);
    }
    timeout(STARTUP_TIMEOUT, async {
        output.write_all(&payload).await?;
        output.write_all(b"\n").await?;
        output.flush().await
    })
    .await
    .map_err(|_| RouterLaunchError::StartupTimeout)?
    .map_err(|_| RouterLaunchError::StartupProtocol)?;
    let ack = read_startup_ack(input).await?;
    if ack.instance_id != ready.instance_id {
        return Err(RouterLaunchError::StartupProtocol);
    }
    Ok(())
}

impl StartupReady {
    fn validate(&self) -> Result<(), RouterLaunchError> {
        if self.instance_id.is_nil() {
            return Err(RouterLaunchError::StartupProtocol);
        }
        validate_router_url(&self.control_url).map_err(|_| RouterLaunchError::StartupProtocol)?;
        Ok(())
    }
}

async fn read_startup_ready(
    output: &mut ChildStdout,
    startup_timeout: Duration,
) -> Result<StartupReady, RouterLaunchError> {
    let bytes = read_startup_frame(output, startup_timeout).await?;
    let ready: StartupReady =
        serde_json::from_slice(&bytes).map_err(|_| RouterLaunchError::StartupProtocol)?;
    ready.validate()?;
    Ok(ready)
}

async fn read_startup_ack<R>(input: &mut R) -> Result<StartupAck, RouterLaunchError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let bytes = read_startup_frame(input, STARTUP_TIMEOUT).await?;
    serde_json::from_slice(&bytes).map_err(|_| RouterLaunchError::StartupProtocol)
}

async fn read_startup_frame<R>(
    input: &mut R,
    frame_timeout: Duration,
) -> Result<Vec<u8>, RouterLaunchError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    timeout(frame_timeout, async {
        let mut frame = Vec::new();
        let mut buffer = [0_u8; 512];
        loop {
            let read = input
                .read(&mut buffer)
                .await
                .map_err(|_| RouterLaunchError::StartupProtocol)?;
            if read == 0 {
                if frame.is_empty() {
                    return Err(RouterLaunchError::StartupProtocol);
                }
                return Ok(frame);
            }
            if let Some(newline) = buffer[..read].iter().position(|byte| *byte == b'\n') {
                frame.extend_from_slice(&buffer[..newline]);
                if buffer[newline + 1..read]
                    .iter()
                    .any(|byte| !byte.is_ascii_whitespace())
                {
                    return Err(RouterLaunchError::StartupProtocol);
                }
                return (frame.len() <= MAX_STARTUP_FRAME_BYTES)
                    .then_some(frame)
                    .ok_or(RouterLaunchError::StartupProtocol);
            }
            frame.extend_from_slice(&buffer[..read]);
            if frame.len() > MAX_STARTUP_FRAME_BYTES {
                return Err(RouterLaunchError::StartupProtocol);
            }
        }
    })
    .await
    .map_err(|_| RouterLaunchError::StartupTimeout)?
}

fn open_private_append(path: &Path) -> Result<File, RouterLaunchError> {
    let parent = path.parent().ok_or(RouterLaunchError::Permissions)?;
    validate_private_directory(parent)?;
    let descriptor = open(
        path,
        OFlags::WRONLY | OFlags::CREATE | OFlags::APPEND | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|_| RouterLaunchError::Permissions)?;
    let file = File::from(descriptor);
    validate_owned_file_0600(&file.metadata().map_err(|_| RouterLaunchError::Io)?)?;
    Ok(file)
}

#[derive(Clone, Debug)]
pub struct ReqwestHealthProbe {
    ca_file: Option<PathBuf>,
}

impl ReqwestHealthProbe {
    #[must_use]
    pub fn new(ca_file: Option<PathBuf>) -> Self {
        Self { ca_file }
    }
}

impl HealthProbe for ReqwestHealthProbe {
    fn probe<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<HealthMarker, ProbeFailure>> {
        Box::pin(async move {
            let mut url =
                Url::parse(&record.control_url).map_err(|_| ProbeFailure::MarkerMismatch)?;
            let secure = url.scheme() == "wss";
            url.set_scheme(if secure { "https" } else { "http" })
                .map_err(|()| ProbeFailure::MarkerMismatch)?;
            url.set_path("/healthz");
            url.set_query(None);
            url.set_fragment(None);
            crate::tls::install_crypto_provider().map_err(|_| ProbeFailure::Tls)?;
            let mut builder = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .timeout(STARTUP_TIMEOUT);
            if secure {
                let tls = crate::tls::load_client_config(self.ca_file.as_deref())
                    .map_err(|_| ProbeFailure::Tls)?;
                builder = builder.tls_backend_preconfigured((*tls).clone());
            }
            let client = builder.build().map_err(|_| ProbeFailure::Transport)?;
            let mut response = match client.get(url).send().await {
                Ok(response) => response,
                Err(error) if error_has_connection_refused(&error) => {
                    return Err(ProbeFailure::ConnectionRefused);
                }
                Err(_) if secure => return Err(ProbeFailure::Tls),
                Err(_) => return Err(ProbeFailure::Transport),
            };
            if matches!(
                response.status(),
                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
            ) {
                return Err(ProbeFailure::Authentication);
            }
            if !response.status().is_success() {
                return Err(ProbeFailure::MarkerMismatch);
            }
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| ProbeFailure::Transport)?
            {
                if body.len().saturating_add(chunk.len()) > MAX_STARTUP_FRAME_BYTES {
                    return Err(ProbeFailure::MarkerMismatch);
                }
                body.extend_from_slice(&chunk);
            }
            let marker: HealthMarker =
                serde_json::from_slice(&body).map_err(|_| ProbeFailure::MarkerMismatch)?;
            marker
                .verify(record.instance_id)
                .map_err(|_| ProbeFailure::MarkerMismatch)?;
            Ok(marker)
        })
    }
}

fn error_has_connection_refused(error: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        if current
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.kind() == io::ErrorKind::ConnectionRefused)
        {
            return true;
        }
        source = current.source();
    }
    false
}

#[derive(Clone, Debug)]
pub struct SystemTailscale {
    executable: PathBuf,
}

impl SystemTailscale {
    pub fn new(executable: PathBuf) -> Result<Self, RouterLaunchError> {
        if !executable.is_absolute() {
            return Err(RouterLaunchError::TailscaleCommand);
        }
        Ok(Self { executable })
    }

    fn inspect(&self) -> Result<TailscaleSnapshot, RouterLaunchError> {
        let status = run_bounded_command(
            &self.executable,
            &[OsString::from("status"), OsString::from("--json")],
            COMMAND_TIMEOUT,
        )?;
        let serve = run_bounded_command(
            &self.executable,
            &[
                OsString::from("serve"),
                OsString::from("status"),
                OsString::from("--json"),
            ],
            COMMAND_TIMEOUT,
        )?;
        parse_tailscale_snapshot(&status, &serve)
    }
}

impl TailscaleControl for SystemTailscale {
    fn snapshot(&mut self) -> ProcessFuture<'_, Result<TailscaleSnapshot, RouterLaunchError>> {
        let this = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || this.inspect())
                .await
                .map_err(|_| RouterLaunchError::TailscaleCommand)?
        })
    }

    fn enable<'a>(
        &'a mut self,
        serve: &'a OwnedServe,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        let executable = self.executable.clone();
        let port = serve.port;
        let target = serve.target.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                run_bounded_command(
                    &executable,
                    &[
                        OsString::from("serve"),
                        OsString::from("--bg"),
                        OsString::from(format!("--tcp={port}")),
                        OsString::from(target),
                    ],
                    COMMAND_TIMEOUT,
                )
                .map(|_| ())
            })
            .await
            .map_err(|_| RouterLaunchError::TailscaleCommand)?
        })
    }

    fn disable<'a>(
        &'a mut self,
        serve: &'a OwnedServe,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        let executable = self.executable.clone();
        let port = serve.port;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                run_bounded_command(
                    &executable,
                    &[
                        OsString::from("serve"),
                        OsString::from(format!("--tcp={port}")),
                        OsString::from("off"),
                    ],
                    COMMAND_TIMEOUT,
                )
                .map(|_| ())
            })
            .await
            .map_err(|_| RouterLaunchError::TailscaleCommand)?
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TailscaleStatusDocument {
    backend_state: String,
    #[serde(rename = "Self")]
    self_node: TailscaleNode,
    #[serde(default)]
    peer: BTreeMap<String, TailscaleNode>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TailscaleNode {
    #[serde(default)]
    online: bool,
    #[serde(rename = "TailscaleIPs")]
    tailscale_ips: Vec<String>,
}

fn parse_tailscale_snapshot(
    status: &[u8],
    serve: &[u8],
) -> Result<TailscaleSnapshot, RouterLaunchError> {
    let status: TailscaleStatusDocument =
        serde_json::from_slice(status).map_err(|_| RouterLaunchError::TailscaleStatus)?;
    let self_ipv4 = parse_tailscale_addresses(&status.self_node.tailscale_ips)?;
    let mut online_peer_ipv4 = Vec::new();
    for peer in status.peer.values().filter(|peer| peer.online) {
        online_peer_ipv4.extend(parse_tailscale_addresses(&peer.tailscale_ips)?);
    }
    let tcp_forwards = parse_serve_status(serve)?;
    let snapshot = TailscaleSnapshot {
        backend_running: status.backend_state == "Running",
        self_ipv4,
        online_peer_ipv4,
        tcp_forwards,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn parse_tailscale_addresses(values: &[String]) -> Result<Vec<Ipv4Addr>, RouterLaunchError> {
    let mut addresses = Vec::new();
    for value in values {
        let address = value
            .parse::<IpAddr>()
            .map_err(|_| RouterLaunchError::TailscaleStatus)?;
        match address {
            IpAddr::V4(address) if is_tailscale_ipv4(address) => addresses.push(address),
            IpAddr::V6(_) => {}
            IpAddr::V4(_) => return Err(RouterLaunchError::TailscaleStatus),
        }
    }
    Ok(addresses)
}

fn parse_serve_status(bytes: &[u8]) -> Result<BTreeMap<u16, String>, RouterLaunchError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| RouterLaunchError::TailscaleStatus)?;
    let root = value
        .as_object()
        .ok_or(RouterLaunchError::TailscaleStatus)?;
    let Some(tcp) = root.get("TCP") else {
        return Ok(BTreeMap::new());
    };
    let tcp = tcp.as_object().ok_or(RouterLaunchError::TailscaleStatus)?;
    let mut mappings = BTreeMap::new();
    for (port, config) in tcp {
        let port = port
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or(RouterLaunchError::TailscaleStatus)?;
        let config = config
            .as_object()
            .ok_or(RouterLaunchError::TailscaleStatus)?;
        let target = config
            .get("TCPForward")
            .and_then(Value::as_str)
            .map(|target| format!("tcp://{target}"))
            .unwrap_or_default();
        if mappings.insert(port, target).is_some() {
            return Err(RouterLaunchError::TailscaleStatus);
        }
    }
    Ok(mappings)
}

fn run_bounded_command(
    program: &Path,
    arguments: &[OsString],
    deadline: Duration,
) -> Result<Vec<u8>, RouterLaunchError> {
    let mut child = Command::new(program)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| RouterLaunchError::TailscaleCommand)?;
    let stdout = child
        .stdout
        .take()
        .ok_or(RouterLaunchError::TailscaleCommand)?;
    let (sender, receiver) = std_mpsc::sync_channel(1);
    let reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stdout
            .take((MAX_COMMAND_OUTPUT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map(|_| bytes);
        let _ = sender.send(result);
    });
    let expires = StdInstant::now() + deadline;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if StdInstant::now() < expires => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(RouterLaunchError::TailscaleCommand);
            }
        }
    };
    let bytes = receiver
        .recv_timeout(expires.saturating_duration_since(StdInstant::now()))
        .map_err(|_| RouterLaunchError::TailscaleCommand)?
        .map_err(|_| RouterLaunchError::TailscaleCommand)?;
    reader
        .join()
        .map_err(|_| RouterLaunchError::TailscaleCommand)?;
    if !status.success() || bytes.len() > MAX_COMMAND_OUTPUT_BYTES {
        return Err(RouterLaunchError::TailscaleCommand);
    }
    Ok(bytes)
}
