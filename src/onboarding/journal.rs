use std::{
    fmt,
    fs::{File, Metadata},
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use rustix::fs::{
    AtFlags, Dir, FlockOperation, Mode, OFlags, RenameFlags, flock, open, openat, renameat,
    renameat_with, unlinkat,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    bootstrap::routes::{VerifiedRoute, onboarding_url},
    config::{self, ConfigError},
    credentials::{
        CredentialError, CredentialFile, CredentialRole, MAX_CREDENTIAL_BYTES,
        PublicCredentialClaims, ensure_private_directory, write_credential_atomic_no_replace,
    },
    onboarding::{
        EnrollmentRequest, EnrollmentResponse, MAX_ENROLLMENT_BYTES, OnboardingProvider,
        OnboardingTicket, VERSION, invite_subject, valid_sha256,
    },
    protocol::{PROTOCOL_VERSION, WorkspaceName},
};

const STATE_FILE: &str = "state.json";
const CREDENTIAL_FILE: &str = "credential.json";
const LOCK_FILE: &str = "onboarding.lock";
const MAX_STATE_BYTES: usize = 1024 * 1024;
const ENROLL_TIMEOUT: Duration = Duration::from_secs(10);
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOFOLLOW);
const PRIVATE_READ_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Stage {
    Prepared,
    Enrolled,
    Configured,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum InstallAction {
    File { path: PathBuf, sha256: String },
    Command { argv: Vec<String> },
}

impl fmt::Debug for InstallAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::File { .. } => "File { .. }",
            Self::Command { .. } => "Command { .. }",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ActionStatus {
    Planned,
    Applied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionRecord {
    pub intent: InstallAction,
    pub status: ActionStatus,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JournalState {
    pub version: u8,
    pub invite_id: Uuid,
    pub provider: OnboardingProvider,
    pub server_id: Uuid,
    pub profile_name: String,
    pub workspace: WorkspaceName,
    pub enrollment_id: Uuid,
    pub credential_file: PathBuf,
    pub executable: PathBuf,
    pub manifest_sha256: String,
    pub stage: Stage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket: Option<OnboardingTicket>,
    pub actions: Vec<ActionRecord>,
}

impl fmt::Debug for JournalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JournalState")
            .field("invite_id", &self.invite_id)
            .field("provider", &self.provider)
            .field("stage", &self.stage)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JournalError {
    #[error("configuration_busy")]
    ConfigurationBusy,
    #[error("journal_permissions")]
    Permissions,
    #[error("journal_io")]
    Io,
    #[error("journal_invalid")]
    Invalid,
    #[error("journal_not_found")]
    NotFound,
    #[error("journal_conflict")]
    Conflict,
    #[error("profile_conflict")]
    ProfileConflict,
    #[error("binding_conflict")]
    BindingConflict,
    #[error("invalid_configuration")]
    Configuration,
    #[error("server_identity_mismatch")]
    IdentityMismatch,
    #[error("route_invalid")]
    InvalidRoute,
    #[error("enrollment_claims_mismatch")]
    ClaimsMismatch,
    #[error("enrollment_response_invalid")]
    InvalidResponse,
    #[error("invite_unavailable")]
    InviteUnavailable,
    #[error("enrollment_unavailable")]
    Unavailable,
    #[error("enrollment_rate_limited")]
    RateLimited,
}

impl JournalError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ConfigurationBusy => "configuration_busy",
            Self::Permissions => "journal_permissions",
            Self::Io => "journal_io",
            Self::Invalid => "journal_invalid",
            Self::NotFound => "journal_not_found",
            Self::Conflict => "journal_conflict",
            Self::ProfileConflict => "profile_conflict",
            Self::BindingConflict => "binding_conflict",
            Self::Configuration => "invalid_configuration",
            Self::IdentityMismatch => "server_identity_mismatch",
            Self::InvalidRoute => "route_invalid",
            Self::ClaimsMismatch => "enrollment_claims_mismatch",
            Self::InvalidResponse => "enrollment_response_invalid",
            Self::InviteUnavailable => "invite_unavailable",
            Self::Unavailable => "enrollment_unavailable",
            Self::RateLimited => "enrollment_rate_limited",
        }
    }
}

impl From<ConfigError> for JournalError {
    fn from(error: ConfigError) -> Self {
        match error {
            ConfigError::ProfileConflict => Self::ProfileConflict,
            ConfigError::BindingConflict => Self::BindingConflict,
            ConfigError::Io(_) => Self::Io,
            _ => Self::Configuration,
        }
    }
}

/// Hold across preflight, enrollment, profile publication, and host configuration.
/// The file persists; only the OS advisory lock represents ownership.
pub struct ConfigurationLock {
    _file: File,
}

impl ConfigurationLock {
    pub fn acquire(config_path: &Path) -> Result<Self, JournalError> {
        let config_path = absolute_path(config_path)?;
        let directory = private_directory(config_parent(&config_path)?, true)?;
        let file = File::from(
            openat(
                &directory,
                LOCK_FILE,
                OFlags::RDWR
                    | OFlags::CREATE
                    | OFlags::CLOEXEC
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK,
                Mode::RUSR | Mode::WUSR,
            )
            .map_err(|_| JournalError::Permissions)?,
        );
        validate_private_file(&file.metadata().map_err(|_| JournalError::Io)?)?;
        match flock(&file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {}
            Err(error)
                if error == rustix::io::Errno::WOULDBLOCK || error == rustix::io::Errno::AGAIN =>
            {
                return Err(JournalError::ConfigurationBusy);
            }
            Err(_) => return Err(JournalError::Io),
        }
        file.sync_all().map_err(|_| JournalError::Io)?;
        directory.sync_all().map_err(|_| JournalError::Io)?;
        Ok(Self { _file: file })
    }
}

pub struct Journal {
    config_path: PathBuf,
    directory: File,
    state: JournalState,
    persisted: Vec<u8>,
    needs_reload: bool,
}

impl fmt::Debug for Journal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.state.fmt(f)
    }
}

impl Journal {
    /// The caller holds `ConfigurationLock` throughout this journal's mutations.
    pub fn prepare(
        config_path: &Path,
        ticket: &OnboardingTicket,
        provider: OnboardingProvider,
        executable: &Path,
    ) -> Result<Self, JournalError> {
        ticket.validate().map_err(|_| JournalError::Invalid)?;
        if ticket.provider.is_some_and(|bound| bound != provider) {
            return Err(JournalError::Conflict);
        }
        validate_executable(executable)?;
        let config_path = absolute_path(config_path)?;
        let (path, directory) = journal_directory(&config_path, ticket.invite_id, provider, true)?;
        match read_state(&directory) {
            Ok(_) => {
                let journal = Self::load(&config_path, ticket.invite_id, provider)?;
                let state = journal.state();
                if state.server_id != ticket.server_id
                    || state.profile_name != ticket.profile_name
                    || state.workspace != ticket.workspace
                    || state.manifest_sha256 != ticket.manifest_sha256
                    || state.executable != executable
                    || state
                        .ticket
                        .as_ref()
                        .is_some_and(|saved| !same_ticket(saved, ticket))
                {
                    return Err(JournalError::Conflict);
                }
                return Ok(journal);
            }
            Err(JournalError::NotFound) => {}
            Err(error) => return Err(error),
        }
        let credential_file = path.join(CREDENTIAL_FILE);
        config::check_onboarding_binding(&config_path, ticket, provider, &credential_file)?;
        let credential = match read_pending_credential(
            &directory,
            ticket.invite_id,
            provider,
            &ticket.workspace,
        ) {
            Ok(credential) => credential,
            Err(JournalError::NotFound) => {
                let (side, client) = provider.identity();
                let generated = CredentialFile::generate(
                    CredentialRole::Agent,
                    invite_subject(ticket.invite_id),
                    Some(side),
                    Some(client),
                    vec![ticket.workspace.clone()],
                )
                .map_err(|_| JournalError::Io)?;
                write_credential_atomic_no_replace(&path, CREDENTIAL_FILE, &generated)
                    .map_err(credential_error)?;
                generated
            }
            Err(error) => return Err(error),
        };
        // A crash between credential publication and state publication leaves an
        // orphan. Adopt only the exact fixed invitation claims, never replace it.
        validate_claims(&credential, ticket.invite_id, provider, &ticket.workspace)?;
        let state = JournalState {
            version: VERSION,
            invite_id: ticket.invite_id,
            provider,
            server_id: ticket.server_id,
            profile_name: ticket.profile_name.clone(),
            workspace: ticket.workspace.clone(),
            enrollment_id: Uuid::new_v4(),
            credential_file,
            executable: executable.to_owned(),
            manifest_sha256: ticket.manifest_sha256.clone(),
            stage: Stage::Prepared,
            ticket: Some(ticket.clone()),
            actions: Vec::new(),
        };
        let mut journal = Self {
            config_path,
            directory,
            state,
            persisted: Vec::new(),
            needs_reload: false,
        };
        journal.persist(journal.state.clone())?;
        Ok(journal)
    }

    pub fn load(
        config_path: &Path,
        invite_id: Uuid,
        provider: OnboardingProvider,
    ) -> Result<Self, JournalError> {
        let config_path = absolute_path(config_path)?;
        let (path, directory) = journal_directory(&config_path, invite_id, provider, false)?;
        let persisted = read_state(&directory)?;
        let state: JournalState =
            serde_json::from_slice(&persisted).map_err(|_| JournalError::Invalid)?;
        validate_state(&state, invite_id, provider, &path)?;
        let journal = Self {
            config_path,
            directory,
            state,
            persisted,
            needs_reload: false,
        };
        journal.credential()?;
        // A previous process may have published the file but failed its final
        // directory fsync. Adoption must make that publication durable too.
        journal.directory.sync_all().map_err(|_| JournalError::Io)?;
        Ok(journal)
    }

    #[must_use]
    pub fn state(&self) -> &JournalState {
        &self.state
    }

    pub fn credential(&self) -> Result<CredentialFile, JournalError> {
        read_pending_credential(
            &self.directory,
            self.state.invite_id,
            self.state.provider,
            &self.state.workspace,
        )
    }

    /// Never skips the exchange merely because an earlier response was saved:
    /// exact replay also checks server-side invite and credential revocation.
    pub async fn enroll(
        &mut self,
        route: &VerifiedRoute,
    ) -> Result<PublicCredentialClaims, JournalError> {
        let ticket = self.state.ticket.as_ref().ok_or(JournalError::Conflict)?;
        if route.info.version != VERSION
            || route.info.protocol_version != PROTOCOL_VERSION
            || route.info.server_id != self.state.server_id
        {
            return Err(JournalError::IdentityMismatch);
        }
        if !ticket.routes.contains(&route.route) {
            return Err(JournalError::InvalidRoute);
        }
        let url = onboarding_url(&route.route.router_url, "/onboarding/enroll")
            .map_err(|_| JournalError::InvalidRoute)?;
        self.assert_unchanged()?;
        let credential = self.credential()?;
        config::check_onboarding_binding(
            &self.config_path,
            ticket,
            self.state.provider,
            &self.state.credential_file,
        )?;
        // Persist public CA material before exchanging so an unwritable trust
        // cache cannot consume an invite. No profile is published at this point.
        let routes = config::store_onboarding_routes(
            &ticket.routes,
            &config_parent(&self.config_path)?.join("onboarding/ca"),
        )?;
        let request = EnrollmentRequest {
            version: VERSION,
            server_id: self.state.server_id,
            invite_id: self.state.invite_id,
            invite_token: ticket.invite_token.clone(),
            enrollment_id: self.state.enrollment_id,
            provider: self.state.provider,
            credential_id: credential.id,
            credential_token: credential.token.clone(),
        };
        let body = serde_json::to_vec(&request).map_err(|_| JournalError::Invalid)?;
        if body.len() > MAX_ENROLLMENT_BYTES {
            return Err(JournalError::Invalid);
        }
        let response = tokio::time::timeout(ENROLL_TIMEOUT, async {
            let mut response = route
                .client
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await
                .map_err(|_| JournalError::Unavailable)?;
            match response.status() {
                reqwest::StatusCode::OK => {}
                reqwest::StatusCode::UNAUTHORIZED => return Err(JournalError::InviteUnavailable),
                reqwest::StatusCode::TOO_MANY_REQUESTS => return Err(JournalError::RateLimited),
                reqwest::StatusCode::SERVICE_UNAVAILABLE => return Err(JournalError::Unavailable),
                _ => return Err(JournalError::InvalidResponse),
            }
            if response
                .content_length()
                .is_some_and(|size| size > MAX_ENROLLMENT_BYTES as u64)
                || !response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| {
                        value.split(';').next().is_some_and(|mime| {
                            mime.trim().eq_ignore_ascii_case("application/json")
                        })
                    })
            {
                return Err(JournalError::InvalidResponse);
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| JournalError::Unavailable)?
            {
                if bytes.len() + chunk.len() > MAX_ENROLLMENT_BYTES {
                    return Err(JournalError::InvalidResponse);
                }
                bytes.extend_from_slice(&chunk);
            }
            serde_json::from_slice::<EnrollmentResponse>(&bytes)
                .map_err(|_| JournalError::InvalidResponse)
        })
        .await
        .map_err(|_| JournalError::Unavailable)??;
        if response.version != VERSION
            || response.server_id != self.state.server_id
            || response.invite_id != self.state.invite_id
            || response.claims != credential.public_claims()
        {
            return Err(JournalError::ClaimsMismatch);
        }
        // The exchange is durable before profile publication. Publication failure
        // leaves an enrolled journal and the same replayable enrollment identity.
        let mut candidate = self.state.clone();
        candidate.stage = Stage::Enrolled;
        self.persist(candidate)?;
        config::publish_onboarding_binding(
            &self.config_path,
            self.state.ticket.as_ref().ok_or(JournalError::Invalid)?,
            self.state.provider,
            &self.state.credential_file,
            &route.route.router_url,
            routes,
        )?;
        Ok(response.claims)
    }

    pub fn mark_configured(&mut self) -> Result<(), JournalError> {
        if self.state.stage == Stage::Prepared
            || self
                .state
                .actions
                .iter()
                .any(|action| action.status != ActionStatus::Applied)
        {
            return Err(JournalError::Conflict);
        }
        let mut candidate = self.state.clone();
        candidate.stage = Stage::Configured;
        candidate.ticket = None;
        self.persist(candidate)
    }

    pub fn plan(&mut self, action: InstallAction) -> Result<usize, JournalError> {
        self.assert_unchanged()?;
        validate_action(
            &action,
            self.state.ticket.as_ref(),
            Some(&self.credential()?),
        )?;
        if let Some(index) = self
            .state
            .actions
            .iter()
            .position(|record| record.intent == action)
        {
            return Ok(index);
        }
        if self.state.stage == Stage::Configured || self.state.actions.iter().any(|record| {
            matches!((&record.intent, &action),
                (InstallAction::File { path: old, .. }, InstallAction::File { path: new, .. }) if old == new)
        }) {
            return Err(JournalError::Conflict);
        }
        let index = self.state.actions.len();
        let mut candidate = self.state.clone();
        candidate.actions.push(ActionRecord {
            intent: action,
            status: ActionStatus::Planned,
        });
        self.persist(candidate)?;
        Ok(index)
    }

    pub fn mark_applied(&mut self, index: usize) -> Result<(), JournalError> {
        let mut candidate = self.state.clone();
        let record = candidate
            .actions
            .get_mut(index)
            .ok_or(JournalError::Conflict)?;
        record.status = ActionStatus::Applied;
        self.persist(candidate)
    }

    fn assert_unchanged(&self) -> Result<(), JournalError> {
        if self.needs_reload {
            return Err(JournalError::Conflict);
        }
        match read_state(&self.directory) {
            Ok(bytes) if bytes == self.persisted => Ok(()),
            Err(JournalError::NotFound) if self.persisted.is_empty() => Ok(()),
            Ok(_) | Err(JournalError::NotFound) => Err(JournalError::Conflict),
            Err(error) => Err(error),
        }
    }

    fn persist(&mut self, candidate: JournalState) -> Result<(), JournalError> {
        self.assert_unchanged()?;
        let bytes = serde_json::to_vec(&candidate).map_err(|_| JournalError::Invalid)?;
        if bytes.len() > MAX_STATE_BYTES {
            return Err(JournalError::Invalid);
        }
        let temporary = format!(".state-{}.tmp", Uuid::new_v4());
        let mut file = File::from(
            openat(
                &self.directory,
                temporary.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::RUSR | Mode::WUSR,
            )
            .map_err(|_| JournalError::Io)?,
        );
        let result: Result<(), JournalError> = (|| {
            file.write_all(&bytes).map_err(|_| JournalError::Io)?;
            file.sync_all().map_err(|_| JournalError::Io)?;
            if self.persisted.is_empty() {
                renameat_with(
                    &self.directory,
                    temporary.as_str(),
                    &self.directory,
                    STATE_FILE,
                    RenameFlags::NOREPLACE,
                )
                .map_err(|error| {
                    if error == rustix::io::Errno::EXIST {
                        JournalError::Conflict
                    } else {
                        JournalError::Io
                    }
                })?;
            } else {
                renameat(
                    &self.directory,
                    temporary.as_str(),
                    &self.directory,
                    STATE_FILE,
                )
                .map_err(|_| JournalError::Io)?;
            }
            Ok(())
        })();
        let _ = unlinkat(&self.directory, temporary.as_str(), AtFlags::empty());
        result?;
        // Publication has occurred, but success is not durable until this sync.
        // On failure, keep the last committed visible state and forbid every
        // mutation/idempotent shortcut until an explicit load syncs the directory.
        self.needs_reload = true;
        self.directory.sync_all().map_err(|_| JournalError::Io)?;
        self.persisted = bytes;
        self.state = candidate;
        self.needs_reload = false;
        Ok(())
    }
}

fn same_ticket(left: &OnboardingTicket, right: &OnboardingTicket) -> bool {
    // Ticket has intentionally redacted Debug and no public equality contract.
    serde_json::to_vec(left)
        .ok()
        .zip(serde_json::to_vec(right).ok())
        .is_some_and(|(left, right)| left == right)
}

fn validate_state(
    state: &JournalState,
    invite_id: Uuid,
    provider: OnboardingProvider,
    path: &Path,
) -> Result<(), JournalError> {
    if state.version != VERSION
        || state.invite_id != invite_id
        || state.provider != provider
        || state.server_id.is_nil()
        || state.enrollment_id.is_nil()
        || state.credential_file != path.join(CREDENTIAL_FILE)
        || state.profile_name == "local"
        || config::validate_profile_name(&state.profile_name).is_err()
        || !valid_sha256(&state.manifest_sha256)
    {
        return Err(JournalError::Invalid);
    }
    validate_executable(&state.executable)?;
    match (&state.ticket, state.stage) {
        (Some(ticket), Stage::Prepared | Stage::Enrolled) => {
            ticket.validate().map_err(|_| JournalError::Invalid)?;
            if ticket.invite_id != state.invite_id
                || ticket.server_id != state.server_id
                || ticket.profile_name != state.profile_name
                || ticket.workspace != state.workspace
                || ticket.manifest_sha256 != state.manifest_sha256
                || ticket.provider.is_some_and(|bound| bound != provider)
            {
                return Err(JournalError::Invalid);
            }
        }
        (None, Stage::Configured) => {}
        _ => return Err(JournalError::Invalid),
    }
    for (index, record) in state.actions.iter().enumerate() {
        validate_action(&record.intent, state.ticket.as_ref(), None)?;
        if (state.stage == Stage::Configured && record.status != ActionStatus::Applied)
            || state.actions[..index].iter().any(|earlier| {
                earlier.intent == record.intent || matches!((&earlier.intent, &record.intent),
                    (InstallAction::File { path: left, .. }, InstallAction::File { path: right, .. }) if left == right)
            })
        {
            return Err(JournalError::Invalid);
        }
    }
    Ok(())
}

fn validate_claims(
    credential: &CredentialFile,
    invite_id: Uuid,
    provider: OnboardingProvider,
    workspace: &WorkspaceName,
) -> Result<(), JournalError> {
    let (side, client) = provider.identity();
    if credential.id.is_nil()
        || credential.role != CredentialRole::Agent
        || credential.subject != invite_subject(invite_id)
        || credential.agent_side != Some(side)
        || credential.agent_client != Some(client)
        || credential.workspaces.as_slice() != std::slice::from_ref(workspace)
    {
        return Err(JournalError::ClaimsMismatch);
    }
    Ok(())
}

fn validate_action(
    action: &InstallAction,
    ticket: Option<&OnboardingTicket>,
    credential: Option<&CredentialFile>,
) -> Result<(), JournalError> {
    match action {
        InstallAction::File { path, sha256 } => {
            if !path.is_absolute() || absolute_path(path).is_err() || !valid_sha256(sha256) {
                return Err(JournalError::Invalid);
            }
        }
        InstallAction::Command { argv } => {
            if argv.first().is_none_or(String::is_empty)
                || argv.iter().any(|arg| {
                    arg.contains('\0')
                        || ticket.is_some_and(|ticket| arg.contains(ticket.invite_token.expose()))
                        || credential
                            .is_some_and(|credential| arg.contains(credential.token.expose()))
                })
            {
                return Err(JournalError::Invalid);
            }
        }
    }
    Ok(())
}

fn read_pending_credential(
    directory: &File,
    invite_id: Uuid,
    provider: OnboardingProvider,
    workspace: &WorkspaceName,
) -> Result<CredentialFile, JournalError> {
    let mut file = File::from(
        openat(
            directory,
            CREDENTIAL_FILE,
            PRIVATE_READ_FLAGS,
            Mode::empty(),
        )
        .map_err(open_error)?,
    );
    let metadata = file.metadata().map_err(|_| JournalError::Io)?;
    validate_private_metadata(&metadata)?;
    if !matches!(metadata.nlink(), 1 | 2) {
        return Err(JournalError::Permissions);
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take((MAX_CREDENTIAL_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| JournalError::Io)?;
    if bytes.len() > MAX_CREDENTIAL_BYTES {
        return Err(JournalError::Invalid);
    }
    let credential: CredentialFile =
        serde_json::from_slice(&bytes).map_err(|_| JournalError::Invalid)?;
    credential.validate().map_err(credential_error)?;
    validate_claims(&credential, invite_id, provider, workspace)?;
    if metadata.nlink() == 2 {
        recover_credential_temporary_link(directory, &file, &metadata)?;
    }
    Ok(credential)
}

/// The existing no-replace credential writer links a fsynced `.credential-UUID.tmp`
/// to credential.json before unlinking the temporary name. Recover exactly that
/// crash window, never an arbitrary hard link or a credential with wrong claims.
fn recover_credential_temporary_link(
    directory: &File,
    credential: &File,
    metadata: &Metadata,
) -> Result<(), JournalError> {
    let entries = Dir::read_from(directory).map_err(|_| JournalError::Io)?;
    let mut temporary = None;
    for entry in entries {
        let entry = entry.map_err(|_| JournalError::Io)?;
        let Ok(name) = entry.file_name().to_str() else {
            continue;
        };
        let Some(id) = name
            .strip_prefix(".credential-")
            .and_then(|name| name.strip_suffix(".tmp"))
        else {
            continue;
        };
        let Ok(id) = Uuid::parse_str(id) else {
            continue;
        };
        if name != format!(".credential-{id}.tmp") || id.get_version_num() != 4 {
            continue;
        }
        let candidate = File::from(
            openat(
                directory,
                entry.file_name(),
                PRIVATE_READ_FLAGS,
                Mode::empty(),
            )
            .map_err(open_error)?,
        );
        let candidate_metadata = candidate.metadata().map_err(|_| JournalError::Io)?;
        if candidate_metadata.dev() == metadata.dev() && candidate_metadata.ino() == metadata.ino()
        {
            validate_private_metadata(&candidate_metadata)?;
            if candidate_metadata.nlink() != 2 || temporary.is_some() {
                return Err(JournalError::Permissions);
            }
            temporary = Some(entry.file_name().to_owned());
        }
    }
    let temporary = temporary.ok_or(JournalError::Permissions)?;
    unlinkat(directory, temporary.as_c_str(), AtFlags::empty()).map_err(|_| JournalError::Io)?;
    validate_private_file(&credential.metadata().map_err(|_| JournalError::Io)?)?;
    credential.sync_all().map_err(|_| JournalError::Io)?;
    directory.sync_all().map_err(|_| JournalError::Io)?;
    Ok(())
}

fn read_state(directory: &File) -> Result<Vec<u8>, JournalError> {
    let file = File::from(
        openat(directory, STATE_FILE, PRIVATE_READ_FLAGS, Mode::empty()).map_err(open_error)?,
    );
    let metadata = file.metadata().map_err(|_| JournalError::Io)?;
    validate_private_file(&metadata)?;
    if metadata.len() > MAX_STATE_BYTES as u64 {
        return Err(JournalError::Invalid);
    }
    let mut bytes = Vec::new();
    file.take((MAX_STATE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| JournalError::Io)?;
    if bytes.len() > MAX_STATE_BYTES {
        return Err(JournalError::Invalid);
    }
    Ok(bytes)
}

fn journal_directory(
    config_path: &Path,
    invite_id: Uuid,
    provider: OnboardingProvider,
    create: bool,
) -> Result<(PathBuf, File), JournalError> {
    if invite_id.is_nil() {
        return Err(JournalError::Invalid);
    }
    let parent = config_parent(config_path)?;
    private_directory(parent, create)?;
    let onboarding = parent.join("onboarding");
    private_directory(&onboarding, create)?;
    let invite = onboarding.join(invite_id.to_string());
    private_directory(&invite, create)?;
    let path = invite.join(provider.as_str());
    let directory = private_directory(&path, create)?;
    Ok((path, directory))
}

fn private_directory(path: &Path, create: bool) -> Result<File, JournalError> {
    ensure_private_directory(path, create).map_err(credential_error)?;
    let mut directory = File::from(
        open(
            if path.is_absolute() { "/" } else { "." },
            DIRECTORY_FLAGS,
            Mode::empty(),
        )
        .map_err(open_error)?,
    );
    for component in path.components() {
        match component {
            Component::Normal(name) => {
                let child = File::from(
                    openat(&directory, name, DIRECTORY_FLAGS, Mode::empty()).map_err(open_error)?,
                );
                if create
                    && directory.metadata().map_err(|_| JournalError::Io)?.uid()
                        == rustix::process::getuid().as_raw()
                {
                    directory.sync_all().map_err(|_| JournalError::Io)?;
                }
                directory = child;
            }
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) => return Err(JournalError::Permissions),
        }
    }
    let metadata = directory.metadata().map_err(|_| JournalError::Io)?;
    if metadata.uid() != rustix::process::getuid().as_raw() || metadata.mode() & 0o777 != 0o700 {
        return Err(JournalError::Permissions);
    }
    Ok(directory)
}

fn validate_private_file(metadata: &Metadata) -> Result<(), JournalError> {
    validate_private_metadata(metadata)?;
    if metadata.nlink() != 1 {
        return Err(JournalError::Permissions);
    }
    Ok(())
}

fn validate_private_metadata(metadata: &Metadata) -> Result<(), JournalError> {
    if !metadata.is_file()
        || metadata.uid() != rustix::process::getuid().as_raw()
        || metadata.mode() & 0o777 != 0o600
    {
        return Err(JournalError::Permissions);
    }
    Ok(())
}

fn validate_executable(path: &Path) -> Result<(), JournalError> {
    if !path.is_absolute() || absolute_path(path).is_err() {
        return Err(JournalError::Invalid);
    }
    let file = File::from(open(path, PRIVATE_READ_FLAGS, Mode::empty()).map_err(open_error)?);
    let metadata = file.metadata().map_err(|_| JournalError::Io)?;
    if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
        return Err(JournalError::Invalid);
    }
    Ok(())
}

fn absolute_path(path: &Path) -> Result<PathBuf, JournalError> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(JournalError::Invalid);
    }
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(std::env::current_dir()
            .map_err(|_| JournalError::Io)?
            .join(path))
    }
}

fn config_parent(path: &Path) -> Result<&Path, JournalError> {
    path.parent().ok_or(JournalError::Invalid)
}

fn open_error(error: rustix::io::Errno) -> JournalError {
    if error == rustix::io::Errno::NOENT {
        JournalError::NotFound
    } else {
        JournalError::Permissions
    }
}

fn credential_error(error: CredentialError) -> JournalError {
    match error {
        CredentialError::Permissions => JournalError::Permissions,
        CredentialError::Invalid => JournalError::Invalid,
        CredentialError::Exists => JournalError::Conflict,
        CredentialError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => {
            JournalError::NotFound
        }
        CredentialError::Io(_) | CredentialError::Random => JournalError::Io,
    }
}
