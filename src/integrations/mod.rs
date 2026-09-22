use std::{
    collections::{HashMap, HashSet},
    env, fmt,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures_util::StreamExt as _;
use reqwest::{RequestBuilder, StatusCode};
use rusqlite::{OptionalExtension as _, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{Semaphore, SemaphorePermit};
use url::Url;
use uuid::Uuid;

use crate::{
    credentials::{CredentialError, open_private_file},
    protocol::{RouterErrorCode, WorkspaceEvent, WorkspaceEventKind, WorkspaceName},
    store::{EventInsert, RouterStore, append_event_in_transaction, now_millis},
    tasks::{
        CallerContext, ExternalOperationKind, ExternalOperationStatus, ExternalProvider,
        ExternalPublishKind, ExternalResolution, ExternalResolutionOutcome, IntegrationChange,
        IntegrationEvent, MAX_SAFE_INTEGER, TaskChange, TaskEvent, get_task, validate_description,
        validate_title,
    },
    tls,
};

pub mod github;
pub mod linear;

const CONFIG_VERSION: u8 = 1;
const MAX_CONFIG_BYTES: usize = 256 * 1024;
const MAX_CONNECTIONS: usize = 128;
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_RAW_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_TYPED_RESULT_BYTES: usize = 256 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_REQUESTS: usize = 4;

static REQUEST_SLOTS: Semaphore = Semaphore::const_new(MAX_CONCURRENT_REQUESTS);

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationAccess {
    #[default]
    Read,
    Write,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationPublic {
    pub provider: ExternalProvider,
    pub workspace: WorkspaceName,
    pub target: String,
    pub access: IntegrationAccess,
    pub available: bool,
    pub error: Option<String>,
}

pub struct IntegrationCatalog {
    connections: Vec<IntegrationConnection>,
}

impl IntegrationCatalog {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            connections: Vec::new(),
        }
    }

    pub fn load(path: &Path) -> Result<Self, IntegrationConfigurationError> {
        Self::load_inner(path, true)
    }

    pub fn load_explicit(path: &Path) -> Result<Self, IntegrationConfigurationError> {
        Self::load_inner(path, false)
    }

    fn load_inner(
        path: &Path,
        missing_is_empty: bool,
    ) -> Result<Self, IntegrationConfigurationError> {
        let file = match open_private_file(path) {
            Ok(file) => file,
            Err(CredentialError::Io(error))
                if missing_is_empty && error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(Self::empty());
            }
            Err(_) => return Err(IntegrationConfigurationError),
        };
        let bytes = read_bounded(file, MAX_CONFIG_BYTES)?;
        let raw: RawConfiguration =
            serde_json::from_slice(&bytes).map_err(|_| IntegrationConfigurationError)?;
        if raw.version != CONFIG_VERSION || raw.connections.len() > MAX_CONNECTIONS {
            return Err(IntegrationConfigurationError);
        }

        let mut workspace_providers = HashSet::with_capacity(raw.connections.len());
        let mut targets = HashSet::with_capacity(raw.connections.len());
        let mut connections = Vec::with_capacity(raw.connections.len());
        for connection in raw.connections {
            let parsed = IntegrationConnection::from_raw(connection)?;
            if !workspace_providers.insert((parsed.workspace().clone(), parsed.provider().as_str()))
                || !targets.insert(parsed.target_key())
            {
                return Err(IntegrationConfigurationError);
            }
            connections.push(parsed);
        }
        Ok(Self { connections })
    }

    #[must_use]
    pub fn public(&self) -> Vec<IntegrationPublic> {
        self.connections
            .iter()
            .map(IntegrationConnection::public)
            .collect()
    }

    #[must_use]
    pub fn connection(
        &self,
        workspace: &WorkspaceName,
        provider: ExternalProvider,
    ) -> Option<&IntegrationConnection> {
        self.connections.iter().find(|connection| {
            connection.workspace() == workspace && connection.provider() == provider
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    pub(crate) fn into_connections(self) -> Vec<IntegrationConnection> {
        self.connections
    }
}

impl fmt::Debug for IntegrationCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntegrationCatalog")
            .field("connections", &self.public())
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct ActiveIntegration {
    pub connection: IntegrationConnection,
    pub revision: i64,
}

pub(crate) struct IntegrationRegistry {
    path: PathBuf,
    explicit: bool,
    configuration_invalid: bool,
    active: HashMap<(WorkspaceName, ExternalProvider), ActiveIntegration>,
}

impl IntegrationRegistry {
    pub fn load(store: &mut RouterStore, data_dir: &Path) -> Self {
        let explicit_path = env::var_os("ASR_INTEGRATIONS_FILE").map(PathBuf::from);
        let (path, explicit) = explicit_path.map_or_else(
            || (data_dir.join("integrations.json"), false),
            |path| (path, true),
        );
        let loaded = if explicit {
            IntegrationCatalog::load_explicit(&path)
        } else {
            IntegrationCatalog::load(&path)
        };
        if let Ok((active, _)) = loaded.and_then(|catalog| {
            apply_catalog(store, catalog).map_err(|_| IntegrationConfigurationError)
        }) {
            Self {
                path,
                explicit,
                configuration_invalid: false,
                active,
            }
        } else {
            let _ = store
                .connection_mut()
                .execute("UPDATE integration_bindings SET enabled=0", []);
            Self {
                path,
                explicit,
                configuration_invalid: true,
                active: HashMap::new(),
            }
        }
    }

    pub fn reload(
        &mut self,
        store: &mut RouterStore,
    ) -> Result<(Vec<IntegrationPublic>, Vec<WorkspaceEvent>), RouterErrorCode> {
        let catalog = if self.explicit {
            IntegrationCatalog::load_explicit(&self.path)
        } else {
            IntegrationCatalog::load(&self.path)
        }
        .map_err(|_| RouterErrorCode::IntegrationConfigurationInvalid)?;
        let (active, events) = apply_catalog(store, catalog)?;
        self.active = active;
        self.configuration_invalid = false;
        Ok((self.public_all(), events))
    }

    pub fn binding(
        &self,
        workspace: &WorkspaceName,
        provider: ExternalProvider,
    ) -> Result<ActiveIntegration, RouterErrorCode> {
        if self.configuration_invalid {
            return Err(RouterErrorCode::IntegrationConfigurationInvalid);
        }
        self.active
            .get(&(workspace.clone(), provider))
            .cloned()
            .ok_or(RouterErrorCode::IntegrationNotConfigured)
    }

    pub fn list(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Vec<IntegrationPublic>, RouterErrorCode> {
        if self.configuration_invalid {
            return Err(RouterErrorCode::IntegrationConfigurationInvalid);
        }
        let mut values = self
            .active
            .iter()
            .filter(|((candidate, _), _)| candidate == workspace)
            .map(|(_, active)| active.connection.public())
            .collect::<Vec<_>>();
        values.sort_by_key(|value| value.provider.as_str());
        Ok(values)
    }

    pub fn has_active(&self) -> bool {
        !self.active.is_empty()
    }

    fn public_all(&self) -> Vec<IntegrationPublic> {
        let mut values = self
            .active
            .values()
            .map(|active| active.connection.public())
            .collect::<Vec<_>>();
        values.sort_by(|left, right| {
            left.workspace
                .cmp(&right.workspace)
                .then_with(|| left.provider.as_str().cmp(right.provider.as_str()))
        });
        values
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredTarget {
    target: String,
}

struct StoredBinding {
    workspace: WorkspaceName,
    provider: ExternalProvider,
    target: String,
    access: IntegrationAccess,
    config_hash: [u8; 32],
    revision: i64,
    enabled: bool,
}

fn apply_catalog(
    store: &mut RouterStore,
    catalog: IntegrationCatalog,
) -> Result<
    (
        HashMap<(WorkspaceName, ExternalProvider), ActiveIntegration>,
        Vec<WorkspaceEvent>,
    ),
    RouterErrorCode,
> {
    let stored = load_stored_bindings(store)?;
    let connections = catalog.into_connections();
    for connection in &connections {
        if !store
            .workspace_exists(connection.workspace())
            .map_err(RouterErrorCode::from)?
        {
            return Err(RouterErrorCode::IntegrationConfigurationInvalid);
        }
    }
    for existing in stored.iter().filter(|binding| binding.enabled) {
        let replacement = connections.iter().find(|connection| {
            connection.workspace() == &existing.workspace
                && connection.provider() == existing.provider
        });
        let configuration_changed =
            replacement.is_none_or(|connection| connection.config_hash() != existing.config_hash);
        if configuration_changed && binding_has_blocking_operation(store, existing)? {
            return Err(RouterErrorCode::IntegrationBusy);
        }
        let target_changed =
            replacement.is_none_or(|connection| connection.namespace() != existing.target);
        if target_changed && binding_has_links(store, existing)? {
            return Err(RouterErrorCode::ExternalConflict);
        }
    }

    let now = now_millis().map_err(RouterErrorCode::from)?;
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    transaction
        .execute(
            "UPDATE integration_bindings SET enabled=0,updated_at=?1",
            [now],
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let mut active = HashMap::with_capacity(connections.len());
    let mut events = Vec::new();
    for connection in connections {
        let previous = stored.iter().find(|binding| {
            binding.workspace == *connection.workspace()
                && binding.provider == connection.provider()
        });
        let config_hash = connection.config_hash();
        let changed =
            previous.is_none_or(|binding| !binding.enabled || binding.config_hash != config_hash);
        let revision = previous.map_or(1, |binding| {
            if changed {
                binding.revision + 1
            } else {
                binding.revision
            }
        });
        let namespace = connection.namespace();
        let target_json = serde_json::to_string(&StoredTarget {
            target: namespace.clone(),
        })
        .map_err(|_| RouterErrorCode::StorageError)?;
        transaction
            .execute(
                "INSERT INTO integration_bindings(workspace,provider,target_json,access,enabled,config_hash,revision,updated_at) VALUES(?1,?2,?3,?4,1,?5,?6,?7) ON CONFLICT(workspace,provider) DO UPDATE SET target_json=excluded.target_json,access=excluded.access,enabled=1,config_hash=excluded.config_hash,revision=excluded.revision,updated_at=excluded.updated_at",
                params![
                    connection.workspace().as_str(),
                    connection.provider().as_str(),
                    target_json,
                    match connection.access() {
                        IntegrationAccess::Read => "read",
                        IntegrationAccess::Write => "write",
                    },
                    config_hash.as_slice(),
                    revision,
                    now,
                ],
            )
            .map_err(crate::store::StoreError::Sqlite)
            .map_err(RouterErrorCode::from)?;
        if changed {
            let public = connection.public();
            let content = serde_json::to_string(&IntegrationEvent {
                change: IntegrationChange::Configured,
                integration: Some(public),
                operation: None,
                resolution: None,
            })
            .map_err(|_| RouterErrorCode::StorageError)?;
            events.push(
                append_event_in_transaction(
                    &transaction,
                    &EventInsert {
                        workspace: connection.workspace(),
                        kind: WorkspaceEventKind::Integration,
                        actor_id: "system:router",
                        request_id: None,
                        target_id: None,
                        task_id: None,
                        content: Some(&content),
                        ok: None,
                        error: None,
                    },
                )
                .map_err(RouterErrorCode::from)?,
            );
        }
        active.insert(
            (connection.workspace().clone(), connection.provider()),
            ActiveIntegration {
                connection,
                revision,
            },
        );
    }
    for existing in stored.iter().filter(|binding| binding.enabled) {
        if active.contains_key(&(existing.workspace.clone(), existing.provider)) {
            continue;
        }
        let content = serde_json::to_string(&IntegrationEvent {
            change: IntegrationChange::Disabled,
            integration: Some(IntegrationPublic {
                provider: existing.provider,
                workspace: existing.workspace.clone(),
                target: existing.target.clone(),
                access: existing.access,
                available: false,
                error: None,
            }),
            operation: None,
            resolution: None,
        })
        .map_err(|_| RouterErrorCode::StorageError)?;
        events.push(
            append_event_in_transaction(
                &transaction,
                &EventInsert {
                    workspace: &existing.workspace,
                    kind: WorkspaceEventKind::Integration,
                    actor_id: "system:router",
                    request_id: None,
                    target_id: None,
                    task_id: None,
                    content: Some(&content),
                    ok: None,
                    error: None,
                },
            )
            .map_err(RouterErrorCode::from)?,
        );
    }
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok((active, events))
}

fn load_stored_bindings(store: &RouterStore) -> Result<Vec<StoredBinding>, RouterErrorCode> {
    let mut statement = store
        .connection()
        .prepare(
            "SELECT workspace,provider,target_json,access,config_hash,revision,enabled FROM integration_bindings ORDER BY workspace,provider",
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    statement
        .query_map([], |row| {
            let workspace = WorkspaceName::parse(row.get::<_, String>(0)?)
                .map_err(|_| rusqlite::Error::InvalidQuery)?;
            let provider = match row.get::<_, String>(1)?.as_str() {
                "github" => ExternalProvider::Github,
                "linear" => ExternalProvider::Linear,
                _ => return Err(rusqlite::Error::InvalidQuery),
            };
            let target: StoredTarget = serde_json::from_str(&row.get::<_, String>(2)?)
                .map_err(|_| rusqlite::Error::InvalidQuery)?;
            let access = match row.get::<_, String>(3)?.as_str() {
                "read" => IntegrationAccess::Read,
                "write" => IntegrationAccess::Write,
                _ => return Err(rusqlite::Error::InvalidQuery),
            };
            let hash = row.get::<_, Vec<u8>>(4)?;
            let config_hash =
                <[u8; 32]>::try_from(hash).map_err(|_| rusqlite::Error::InvalidQuery)?;
            Ok(StoredBinding {
                workspace,
                provider,
                target: target.target,
                access,
                config_hash,
                revision: row.get(5)?,
                enabled: row.get::<_, i64>(6)? != 0,
            })
        })
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)
}

fn binding_has_blocking_operation(
    store: &RouterStore,
    binding: &StoredBinding,
) -> Result<bool, RouterErrorCode> {
    store
        .connection()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM external_operations WHERE workspace=?1 AND provider=?2 AND namespace=?3 AND status IN('running','unconfirmed'))",
            params![
                binding.workspace.as_str(),
                binding.provider.as_str(),
                binding.target
            ],
            |row| row.get(0),
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)
}

fn binding_has_links(
    store: &RouterStore,
    binding: &StoredBinding,
) -> Result<bool, RouterErrorCode> {
    store
        .connection()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM external_links WHERE workspace=?1 AND provider=?2 AND namespace=?3)",
            params![
                binding.workspace.as_str(),
                binding.provider.as_str(),
                binding.target
            ],
            |row| row.get(0),
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)
}

#[derive(Clone)]
pub struct IntegrationConnection {
    workspace: WorkspaceName,
    access: IntegrationAccess,
    target: IntegrationTarget,
    token: SecretToken,
}

impl IntegrationConnection {
    fn from_raw(raw: RawConnection) -> Result<Self, IntegrationConfigurationError> {
        let (workspace, access, target, token_file) = match raw {
            RawConnection::Github {
                workspace,
                repository,
                access,
                token_file,
            } => (
                workspace,
                access,
                IntegrationTarget::Github(github::Repository::parse(&repository)?),
                token_file,
            ),
            RawConnection::Linear {
                workspace,
                team_id,
                project_id,
                access,
                token_file,
            } => (
                workspace,
                access,
                IntegrationTarget::Linear(linear::Target::parse(&team_id, &project_id)?),
                token_file,
            ),
        };
        if !token_file.is_absolute() {
            return Err(IntegrationConfigurationError);
        }
        let token = SecretToken::load(&token_file)?;
        Ok(Self {
            workspace,
            access,
            target,
            token,
        })
    }

    #[must_use]
    pub fn workspace(&self) -> &WorkspaceName {
        &self.workspace
    }

    #[must_use]
    pub fn provider(&self) -> ExternalProvider {
        match self.target {
            IntegrationTarget::Github(_) => ExternalProvider::Github,
            IntegrationTarget::Linear(_) => ExternalProvider::Linear,
        }
    }

    #[must_use]
    pub const fn access(&self) -> IntegrationAccess {
        self.access
    }

    #[must_use]
    pub fn public(&self) -> IntegrationPublic {
        IntegrationPublic {
            provider: self.provider(),
            workspace: self.workspace.clone(),
            target: self.target.public_name(),
            access: self.access,
            available: true,
            error: None,
        }
    }

    fn target_key(&self) -> String {
        format!("{}:{}", self.provider().as_str(), self.namespace())
    }

    pub(crate) fn namespace(&self) -> String {
        self.target.public_name()
    }

    pub(crate) fn config_hash(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        for value in [
            self.workspace.as_str(),
            self.provider().as_str(),
            self.namespace().as_str(),
            match self.access {
                IntegrationAccess::Read => "read",
                IntegrationAccess::Write => "write",
            },
            self.token.expose(),
        ] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        hash.finalize().into()
    }

    const fn token(&self) -> &SecretToken {
        &self.token
    }
}

impl fmt::Debug for IntegrationConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.public().fmt(formatter)
    }
}

#[derive(Clone)]
enum IntegrationTarget {
    Github(github::Repository),
    Linear(linear::Target),
}

impl IntegrationTarget {
    fn public_name(&self) -> String {
        match self {
            Self::Github(repository) => repository.as_str().to_owned(),
            Self::Linear(target) => target.public_name(),
        }
    }
}

#[derive(Clone)]
struct SecretToken(Arc<str>);

impl SecretToken {
    fn load(path: &Path) -> Result<Self, IntegrationConfigurationError> {
        let file = open_private_file(path).map_err(|_| IntegrationConfigurationError)?;
        let mut bytes = read_bounded(file, MAX_TOKEN_BYTES + 2)?;
        if bytes.ends_with(b"\r\n") {
            bytes.truncate(bytes.len() - 2);
        } else if bytes.ends_with(b"\n") {
            bytes.truncate(bytes.len() - 1);
        }
        if bytes.is_empty()
            || bytes.len() > MAX_TOKEN_BYTES
            || !bytes.iter().all(|value| matches!(value, 0x21..=0x7e))
        {
            return Err(IntegrationConfigurationError);
        }
        let value = String::from_utf8(bytes).map_err(|_| IntegrationConfigurationError)?;
        Ok(Self(Arc::from(value)))
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("integration_configuration_invalid")]
pub struct IntegrationConfigurationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalErrorCode {
    AuthRequired,
    PermissionDenied,
    NotFound,
    RateLimited,
    InvalidRequest,
    ScopeMismatch,
    RebindRequired,
    ResponseTooLarge,
    ApiError,
    Timeout,
    ConfigurationInvalid,
    NotDispatched,
    NotApplied,
    OperatorConfirmedNotApplied,
}

impl ExternalErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthRequired => "auth_required",
            Self::PermissionDenied => "permission_denied",
            Self::NotFound => "not_found",
            Self::RateLimited => "rate_limited",
            Self::InvalidRequest => "invalid_request",
            Self::ScopeMismatch => "scope_mismatch",
            Self::RebindRequired => "rebind_required",
            Self::ResponseTooLarge => "response_too_large",
            Self::ApiError => "api_error",
            Self::Timeout => "timeout",
            Self::ConfigurationInvalid => "configuration_invalid",
            Self::NotDispatched => "not_dispatched",
            Self::NotApplied => "not_applied",
            Self::OperatorConfirmedNotApplied => "operator_confirmed_not_applied",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationClientError {
    Busy,
    Failed(ExternalErrorCode),
    Unconfirmed(ExternalErrorCode),
}

impl IntegrationClientError {
    #[must_use]
    pub const fn detail_code(self) -> Option<ExternalErrorCode> {
        match self {
            Self::Busy => None,
            Self::Failed(code) | Self::Unconfirmed(code) => Some(code),
        }
    }

    #[must_use]
    pub const fn is_unconfirmed(self) -> bool {
        matches!(self, Self::Unconfirmed(_))
    }
}

impl fmt::Display for IntegrationClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Busy => "integration_busy",
            Self::Failed(code) => code.as_str(),
            Self::Unconfirmed(_) => "external_unconfirmed",
        })
    }
}

impl std::error::Error for IntegrationClientError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntegrationCheck {
    pub provider: ExternalProvider,
    pub target: String,
    pub url: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalIssue {
    pub external_id: String,
    pub url: String,
    pub title: String,
    pub description: String,
    pub state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalMutation {
    pub external_id: String,
    pub url: String,
}

#[derive(Clone)]
pub struct IntegrationClient {
    http: reqwest::Client,
}

impl IntegrationClient {
    pub fn new(explicit_ca_file: Option<&Path>) -> Result<Self, IntegrationClientError> {
        Self::build(explicit_ca_file, &[], REQUEST_TIMEOUT)
    }

    fn build(
        explicit_ca_file: Option<&Path>,
        resolutions: &[(&str, SocketAddr)],
        request_timeout: Duration,
    ) -> Result<Self, IntegrationClientError> {
        let tls_config = tls::load_client_config(explicit_ca_file)
            .map_err(|_| IntegrationClientError::Failed(ExternalErrorCode::ConfigurationInvalid))?;
        let mut builder = reqwest::Client::builder()
            .tls_backend_preconfigured((*tls_config).clone())
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .tls_sslkeylogfile(false)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(request_timeout);
        for (host, address) in resolutions {
            builder = builder.resolve(host, *address);
        }
        let http = builder
            .build()
            .map_err(|_| IntegrationClientError::Failed(ExternalErrorCode::ConfigurationInvalid))?;
        Ok(Self { http })
    }

    #[doc(hidden)]
    pub fn new_for_test(
        explicit_ca_file: &Path,
        github_address: SocketAddr,
        linear_address: SocketAddr,
        request_timeout: Duration,
    ) -> Result<Self, IntegrationClientError> {
        Self::build(
            Some(explicit_ca_file),
            &[
                (github::HOST, github_address),
                (linear::HOST, linear_address),
            ],
            request_timeout,
        )
    }

    pub async fn check(
        &self,
        connection: &IntegrationConnection,
    ) -> Result<IntegrationCheck, IntegrationClientError> {
        let _permit = admit()?;
        match &connection.target {
            IntegrationTarget::Github(repository) => {
                github::check(self, connection.token(), repository).await
            }
            IntegrationTarget::Linear(target) => {
                linear::check(self, connection.token(), target).await
            }
        }
    }

    pub async fn get_issue(
        &self,
        connection: &IntegrationConnection,
        external_id: &str,
    ) -> Result<ExternalIssue, IntegrationClientError> {
        let _permit = admit()?;
        match &connection.target {
            IntegrationTarget::Github(repository) => {
                github::get_issue(self, connection.token(), repository, external_id).await
            }
            IntegrationTarget::Linear(target) => {
                linear::get_issue(self, connection.token(), target, external_id).await
            }
        }
    }

    pub async fn create_issue(
        &self,
        connection: &IntegrationConnection,
        creation_id: Uuid,
        title: &str,
        description: &str,
    ) -> Result<ExternalMutation, IntegrationClientError> {
        let _permit = admit()?;
        require_write(connection)?;
        ensure_outbound_size(&[title, description])?;
        match &connection.target {
            IntegrationTarget::Github(repository) => {
                github::create_issue(self, connection.token(), repository, title, description).await
            }
            IntegrationTarget::Linear(target) => {
                linear::create_issue(
                    self,
                    connection.token(),
                    target,
                    creation_id,
                    title,
                    description,
                )
                .await
            }
        }
    }

    pub async fn create_comment(
        &self,
        connection: &IntegrationConnection,
        issue_id: &str,
        creation_id: Uuid,
        body: &str,
    ) -> Result<ExternalMutation, IntegrationClientError> {
        let _permit = admit()?;
        require_write(connection)?;
        ensure_outbound_size(&[body])?;
        match &connection.target {
            IntegrationTarget::Github(repository) => {
                github::create_comment(self, connection.token(), repository, issue_id, body).await
            }
            IntegrationTarget::Linear(target) => {
                linear::create_comment(
                    self,
                    connection.token(),
                    target,
                    issue_id,
                    creation_id,
                    body,
                )
                .await
            }
        }
    }

    pub async fn verify_issue_marker(
        &self,
        connection: &IntegrationConnection,
        external_id: &str,
        marker: &str,
    ) -> Result<ExternalMutation, IntegrationClientError> {
        let issue = self.get_issue(connection, external_id).await?;
        if !issue.description.contains(marker) {
            return Err(IntegrationClientError::Failed(
                ExternalErrorCode::ScopeMismatch,
            ));
        }
        Ok(ExternalMutation {
            external_id: issue.external_id,
            url: issue.url,
        })
    }

    pub async fn verify_comment_marker(
        &self,
        connection: &IntegrationConnection,
        issue_id: &str,
        comment_id: &str,
        marker: &str,
    ) -> Result<ExternalMutation, IntegrationClientError> {
        let _permit = admit()?;
        match &connection.target {
            IntegrationTarget::Github(repository) => {
                github::verify_comment_marker(
                    self,
                    connection.token(),
                    repository,
                    issue_id,
                    comment_id,
                    marker,
                )
                .await
            }
            IntegrationTarget::Linear(target) => {
                linear::verify_comment_marker(
                    self,
                    connection.token(),
                    target,
                    issue_id,
                    comment_id,
                    marker,
                )
                .await
            }
        }
    }

    async fn send(
        &self,
        request: RequestBuilder,
        expected_status: StatusCode,
        mutation: bool,
    ) -> Result<Vec<u8>, IntegrationClientError> {
        let response = request
            .send()
            .await
            .map_err(|error| uncertain(mutation, transport_code(&error)))?;
        let status = response.status();
        if status.is_redirection() {
            return Err(IntegrationClientError::Failed(
                ExternalErrorCode::RebindRequired,
            ));
        }
        if status.is_client_error() {
            return Err(IntegrationClientError::Failed(status_code(status)));
        }
        if status != expected_status {
            return Err(uncertain(mutation, ExternalErrorCode::ApiError));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RAW_RESPONSE_BYTES as u64)
        {
            return Err(uncertain(mutation, ExternalErrorCode::ResponseTooLarge));
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| uncertain(mutation, transport_code(&error)))?;
            let next_length = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or_else(|| uncertain(mutation, ExternalErrorCode::ResponseTooLarge))?;
            if next_length > MAX_RAW_RESPONSE_BYTES {
                return Err(uncertain(mutation, ExternalErrorCode::ResponseTooLarge));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

impl fmt::Debug for IntegrationClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntegrationClient")
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfiguration {
    version: u8,
    connections: Vec<RawConnection>,
}

#[derive(Deserialize)]
#[serde(tag = "provider", rename_all = "lowercase", deny_unknown_fields)]
enum RawConnection {
    Github {
        workspace: WorkspaceName,
        repository: String,
        #[serde(default)]
        access: IntegrationAccess,
        #[serde(rename = "tokenFile")]
        token_file: PathBuf,
    },
    Linear {
        workspace: WorkspaceName,
        #[serde(rename = "teamId")]
        team_id: String,
        #[serde(rename = "projectId")]
        project_id: String,
        #[serde(default)]
        access: IntegrationAccess,
        #[serde(rename = "tokenFile")]
        token_file: PathBuf,
    },
}

fn read_bounded(
    file: std::fs::File,
    maximum: usize,
) -> Result<Vec<u8>, IntegrationConfigurationError> {
    let mut bytes = Vec::new();
    file.take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| IntegrationConfigurationError)?;
    if bytes.len() > maximum {
        return Err(IntegrationConfigurationError);
    }
    Ok(bytes)
}

fn admit() -> Result<SemaphorePermit<'static>, IntegrationClientError> {
    REQUEST_SLOTS
        .try_acquire()
        .map_err(|_| IntegrationClientError::Busy)
}

const fn require_write(connection: &IntegrationConnection) -> Result<(), IntegrationClientError> {
    match connection.access {
        IntegrationAccess::Write => Ok(()),
        IntegrationAccess::Read => Err(IntegrationClientError::Failed(
            ExternalErrorCode::PermissionDenied,
        )),
    }
}

fn ensure_outbound_size(values: &[&str]) -> Result<(), IntegrationClientError> {
    ensure_typed_size(values.iter().map(|value| value.len()))
        .map_err(IntegrationClientError::Failed)
}

pub(super) fn ensure_typed_size(
    lengths: impl IntoIterator<Item = usize>,
) -> Result<(), ExternalErrorCode> {
    let total = lengths.into_iter().try_fold(0_usize, usize::checked_add);
    if total.is_some_and(|length| length <= MAX_TYPED_RESULT_BYTES) {
        Ok(())
    } else {
        Err(ExternalErrorCode::ResponseTooLarge)
    }
}

pub(super) fn invalid_response(mutation: bool, code: ExternalErrorCode) -> IntegrationClientError {
    uncertain(mutation, code)
}

const fn uncertain(mutation: bool, code: ExternalErrorCode) -> IntegrationClientError {
    if mutation {
        IntegrationClientError::Unconfirmed(code)
    } else {
        IntegrationClientError::Failed(code)
    }
}

fn transport_code(error: &reqwest::Error) -> ExternalErrorCode {
    if error.is_timeout() {
        ExternalErrorCode::Timeout
    } else {
        ExternalErrorCode::ApiError
    }
}

const fn status_code(status: StatusCode) -> ExternalErrorCode {
    match status.as_u16() {
        400 | 422 => ExternalErrorCode::InvalidRequest,
        401 => ExternalErrorCode::AuthRequired,
        403 => ExternalErrorCode::PermissionDenied,
        404 | 410 => ExternalErrorCode::NotFound,
        429 => ExternalErrorCode::RateLimited,
        _ => ExternalErrorCode::ApiError,
    }
}

pub(super) fn parse_url(value: &str) -> Result<Url, ExternalErrorCode> {
    Url::parse(value).map_err(|_| ExternalErrorCode::ApiError)
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub(crate) enum ExternalCommand {
    Import {
        operation_id: Uuid,
        provider: ExternalProvider,
        external_id: String,
    },
    Link {
        operation_id: Uuid,
        task_id: i64,
        expected_version: i64,
        provider: ExternalProvider,
        external_id: String,
        replace: bool,
    },
    Publish {
        operation_id: Uuid,
        task_id: i64,
        expected_version: i64,
        provider: ExternalProvider,
        kind: ExternalPublishKind,
        report_id: Option<Uuid>,
    },
}

impl ExternalCommand {
    pub const fn operation_id(&self) -> Uuid {
        match self {
            Self::Import { operation_id, .. }
            | Self::Link { operation_id, .. }
            | Self::Publish { operation_id, .. } => *operation_id,
        }
    }

    pub const fn provider(&self) -> ExternalProvider {
        match self {
            Self::Import { provider, .. }
            | Self::Link { provider, .. }
            | Self::Publish { provider, .. } => *provider,
        }
    }

    pub const fn task_id(&self) -> Option<i64> {
        match self {
            Self::Import { .. } => None,
            Self::Link { task_id, .. } | Self::Publish { task_id, .. } => Some(*task_id),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum OperationPayload {
    Import {
        external_id: String,
    },
    Link {
        external_id: String,
        expected_version: i64,
        replace: bool,
    },
    PublishIssue {
        creation_id: Uuid,
        title: String,
        body: String,
        marker: String,
    },
    PublishReport {
        creation_id: Uuid,
        issue_id: String,
        report_id: Uuid,
        body: String,
        marker: String,
    },
}

#[derive(Clone)]
pub(crate) struct PreparedExternalOperation {
    pub workspace: WorkspaceName,
    pub actor_id: String,
    pub namespace: String,
    pub summary: crate::tasks::ExternalOperationSummary,
    pub payload: Option<OperationPayload>,
    pub event: Option<WorkspaceEvent>,
    pub credential_id: Uuid,
    pub binding_revision: i64,
    pub deadline: tokio::time::Instant,
}

pub(crate) fn replay_external_operation(
    store: &RouterStore,
    caller: &CallerContext,
    command: &ExternalCommand,
) -> Result<Option<crate::tasks::ExternalOperationSummary>, RouterErrorCode> {
    let request = serde_json::to_vec(command).map_err(|_| RouterErrorCode::StorageError)?;
    let request_hash: [u8; 32] = Sha256::digest(&request).into();
    let Some((summary, actor_id, stored_hash)) =
        operation_receipt(store, &caller.workspace, command.operation_id())?
    else {
        return Ok(None);
    };
    if actor_id != caller.actor_id || stored_hash != request_hash {
        return Err(RouterErrorCode::RequestConflict);
    }
    Ok(Some(summary))
}

pub(crate) fn prepare_external_operation(
    store: &mut RouterStore,
    caller: &CallerContext,
    active: &ActiveIntegration,
    command: &ExternalCommand,
) -> Result<PreparedExternalOperation, RouterErrorCode> {
    let operation_id = command.operation_id();
    let request = serde_json::to_vec(command).map_err(|_| RouterErrorCode::StorageError)?;
    let request_hash: [u8; 32] = Sha256::digest(&request).into();
    if let Some((summary, actor_id, stored_hash)) =
        operation_receipt(store, &caller.workspace, operation_id)?
    {
        if actor_id != caller.actor_id || stored_hash != request_hash {
            return Err(RouterErrorCode::RequestConflict);
        }
        return Ok(PreparedExternalOperation {
            workspace: caller.workspace.clone(),
            actor_id: caller.actor_id.clone(),
            namespace: active.connection.namespace(),
            summary,
            payload: None,
            event: None,
            credential_id: caller.credential_id,
            deadline: tokio::time::Instant::now() + REQUEST_TIMEOUT,
            binding_revision: active.revision,
        });
    }
    if active.connection.provider() != command.provider()
        || active.connection.workspace() != &caller.workspace
    {
        return Err(RouterErrorCode::IntegrationNotConfigured);
    }

    let (kind, task_id, source_version, payload) = match command {
        ExternalCommand::Import { external_id, .. } => (
            ExternalOperationKind::Import,
            None,
            None,
            OperationPayload::Import {
                external_id: external_id.clone(),
            },
        ),
        ExternalCommand::Link {
            task_id,
            expected_version,
            external_id,
            replace,
            ..
        } => {
            let task = require_task_version(store, &caller.workspace, *task_id, *expected_version)?;
            require_no_blocking_operation(store, &caller.workspace, *task_id, command.provider())?;
            (
                ExternalOperationKind::Link,
                Some(*task_id),
                Some(task.summary.version),
                OperationPayload::Link {
                    external_id: external_id.clone(),
                    expected_version: *expected_version,
                    replace: *replace,
                },
            )
        }
        ExternalCommand::Publish {
            task_id,
            expected_version,
            kind,
            report_id,
            ..
        } => {
            if active.connection.access() != IntegrationAccess::Write {
                return Err(RouterErrorCode::PermissionDenied);
            }
            let task = require_task_version(store, &caller.workspace, *task_id, *expected_version)?;
            require_no_blocking_operation(store, &caller.workspace, *task_id, command.provider())?;
            let marker = format!(
                "[asr workspace:{} task:{} version:{} actor:{} operation:{}]",
                caller.workspace, task_id, expected_version, caller.actor_id, operation_id
            );
            let creation_id = Uuid::new_v4();
            match kind {
                ExternalPublishKind::Issue => {
                    if task
                        .links
                        .iter()
                        .any(|link| link.provider == command.provider())
                    {
                        return Err(RouterErrorCode::ExternalConflict);
                    }
                    let body = format!(
                        "{marker}\n\nNative state: {}\n\n{}",
                        task.summary.state.as_str(),
                        task.description
                    );
                    validate_external_body(&body)?;
                    (
                        ExternalOperationKind::PublishIssue,
                        Some(*task_id),
                        Some(task.summary.version),
                        OperationPayload::PublishIssue {
                            creation_id,
                            title: task.summary.title,
                            body,
                            marker,
                        },
                    )
                }
                ExternalPublishKind::Report => {
                    let report_id = report_id.ok_or(RouterErrorCode::InvalidMessage)?;
                    let link = task
                        .links
                        .iter()
                        .find(|link| link.provider == command.provider())
                        .ok_or(RouterErrorCode::ExternalConflict)?;
                    let report =
                        load_report_snapshot(store, &caller.workspace, *task_id, report_id)?;
                    let body = format!(
                        "{marker}\n\nNative state: {}\n\nSelected report:\n\n{}",
                        task.summary.state.as_str(),
                        report
                    );
                    validate_external_body(&body)?;
                    (
                        ExternalOperationKind::PublishReport,
                        Some(*task_id),
                        Some(task.summary.version),
                        OperationPayload::PublishReport {
                            creation_id,
                            issue_id: link.external_id.clone(),
                            report_id,
                            body,
                            marker,
                        },
                    )
                }
            }
        }
    };
    let payload_json =
        serde_json::to_string(&payload).map_err(|_| RouterErrorCode::StorageError)?;
    if payload_json.len() > 128 * 1024 {
        return Err(RouterErrorCode::MessageTooLarge);
    }
    let now = now_millis().map_err(RouterErrorCode::from)?;
    let summary = crate::tasks::ExternalOperationSummary {
        id: operation_id,
        task_id,
        provider: command.provider(),
        kind,
        status: ExternalOperationStatus::Running,
        source_version,
        external_id: None,
        url: None,
        error: None,
        created_at: now,
        updated_at: now,
    };
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    transaction
        .execute(
            "INSERT INTO external_operations(workspace,operation_id,actor_id,provider,namespace,kind,task_id,source_version,request_hash,payload_json,phase,status,external_id,url,error,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'prepared','running',NULL,NULL,NULL,?11,?11)",
            params![
                caller.workspace.as_str(),
                operation_id.to_string(),
                caller.actor_id,
                command.provider().as_str(),
                active.connection.namespace(),
                external_kind_str(kind),
                task_id,
                source_version,
                request_hash.as_slice(),
                payload_json,
                now,
            ],
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let event =
        append_operation_event(&transaction, &caller.workspace, &caller.actor_id, &summary)?;
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(PreparedExternalOperation {
        workspace: caller.workspace.clone(),
        actor_id: caller.actor_id.clone(),
        namespace: active.connection.namespace(),
        summary,
        payload: Some(payload),
        event: Some(event),
        credential_id: caller.credential_id,
        deadline: tokio::time::Instant::now() + REQUEST_TIMEOUT,
        binding_revision: active.revision,
    })
}

pub(crate) fn external_status(
    store: &RouterStore,
    workspace: &WorkspaceName,
    operation_id: Uuid,
) -> Result<
    Option<(
        crate::tasks::ExternalOperationSummary,
        Option<ExternalResolution>,
    )>,
    RouterErrorCode,
> {
    let Some(summary) = operation_summary(store.connection(), workspace, operation_id)? else {
        return Ok(None);
    };
    let resolution = load_resolution(store.connection(), workspace, operation_id)?;
    Ok(Some((summary, resolution)))
}

#[derive(Clone)]
pub(crate) struct PreparedExternalResolution {
    pub workspace: WorkspaceName,
    pub operation_id: Uuid,
    pub resolution_id: Uuid,
    pub actor_id: String,
    pub credential_id: Uuid,
    pub provider: ExternalProvider,
    pub namespace: String,
    pub binding_revision: i64,
    pub task_id: Option<i64>,
    pub payload: OperationPayload,
    pub external_id: String,
    pub note: String,
    pub request_hash: [u8; 32],
    pub deadline: tokio::time::Instant,
}

pub(crate) enum ResolutionPreparation {
    Replay {
        operation: crate::tasks::ExternalOperationSummary,
        resolution: ExternalResolution,
    },
    Completed(Box<ExternalResolutionCompletion>),
    Verify(PreparedExternalResolution),
}

pub(crate) struct ExternalResolutionCompletion {
    pub operation: crate::tasks::ExternalOperationSummary,
    pub resolution: ExternalResolution,
    pub event: WorkspaceEvent,
    pub task_event: Option<WorkspaceEvent>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResolutionRequest<'a> {
    resolution_id: Uuid,
    outcome: ExternalResolutionOutcome,
    external_id: Option<&'a str>,
    note: &'a str,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_external_resolution(
    store: &mut RouterStore,
    caller: &CallerContext,
    active: &ActiveIntegration,
    operation_id: Uuid,
    resolution_id: Uuid,
    outcome: ExternalResolutionOutcome,
    external_id: Option<&str>,
    note: &str,
) -> Result<ResolutionPreparation, RouterErrorCode> {
    if note.is_empty() || note.len() > 4 * 1024 {
        return Err(RouterErrorCode::InvalidMessage);
    }
    let credential = store
        .credential_by_id(caller.credential_id)
        .map_err(RouterErrorCode::from)?
        .ok_or(RouterErrorCode::PermissionDenied)?;
    let claims = credential.claims;
    let admin = claims.role == crate::credentials::CredentialRole::Operator
        && claims.subject == "admin"
        && claims.workspaces.is_empty();
    if credential.revoked_at.is_some()
        || claims.role != crate::credentials::CredentialRole::Operator
        || (!admin
            && !claims
                .workspaces
                .iter()
                .any(|workspace| workspace == &caller.workspace))
    {
        return Err(RouterErrorCode::PermissionDenied);
    }
    let encoded = serde_json::to_vec(&ResolutionRequest {
        resolution_id,
        outcome,
        external_id,
        note,
    })
    .map_err(|_| RouterErrorCode::StorageError)?;
    let request_hash: [u8; 32] = Sha256::digest(encoded).into();
    if let Some((resolution, resolver, stored_hash)) =
        resolution_receipt(store.connection(), &caller.workspace, operation_id)?
    {
        if resolution.id != resolution_id
            || resolver != caller.actor_id
            || stored_hash != request_hash
        {
            return Err(RouterErrorCode::RequestConflict);
        }
        let operation = operation_summary(store.connection(), &caller.workspace, operation_id)?
            .ok_or(RouterErrorCode::StorageError)?;
        return Ok(ResolutionPreparation::Replay {
            operation,
            resolution,
        });
    }
    let resolution_id_used: bool = store
        .connection()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM external_resolutions WHERE workspace=?1 AND resolution_id=?2)",
            params![caller.workspace.as_str(), resolution_id.to_string()],
            |row| row.get(0),
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    if resolution_id_used {
        return Err(RouterErrorCode::RequestConflict);
    }
    let operation = operation_summary(store.connection(), &caller.workspace, operation_id)?
        .ok_or(RouterErrorCode::IntegrationError)?;
    if operation.status != ExternalOperationStatus::Unconfirmed
        || !matches!(
            operation.kind,
            ExternalOperationKind::PublishIssue | ExternalOperationKind::PublishReport
        )
    {
        return Err(RouterErrorCode::ExternalConflict);
    }
    let (namespace, payload_json, phase) = store
        .connection()
        .query_row(
            "SELECT namespace,payload_json,phase FROM external_operations WHERE workspace=?1 AND operation_id=?2",
            params![caller.workspace.as_str(), operation_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    if phase != "terminal"
        || active.connection.workspace() != &caller.workspace
        || active.connection.provider() != operation.provider
        || active.connection.namespace() != namespace
    {
        return Err(RouterErrorCode::ExternalConflict);
    }
    let payload: OperationPayload =
        serde_json::from_str(&payload_json).map_err(|_| RouterErrorCode::StorageError)?;
    match outcome {
        ExternalResolutionOutcome::NotApplied => {
            if external_id.is_some() {
                return Err(RouterErrorCode::InvalidMessage);
            }
            let completion = complete_not_applied_resolution(
                store,
                caller,
                operation,
                resolution_id,
                note,
                request_hash,
            )?;
            Ok(ResolutionPreparation::Completed(Box::new(completion)))
        }
        ExternalResolutionOutcome::Applied => {
            let external_id = external_id
                .filter(|value| !value.is_empty() && value.len() <= 4 * 1024)
                .ok_or(RouterErrorCode::InvalidMessage)?;
            let marker = match &payload {
                OperationPayload::PublishIssue { marker, .. }
                | OperationPayload::PublishReport { marker, .. } => marker,
                OperationPayload::Import { .. } | OperationPayload::Link { .. } => {
                    return Err(RouterErrorCode::ExternalConflict);
                }
            };
            if marker.is_empty() {
                return Err(RouterErrorCode::StorageError);
            }
            Ok(ResolutionPreparation::Verify(PreparedExternalResolution {
                workspace: caller.workspace.clone(),
                operation_id,
                resolution_id,
                actor_id: caller.actor_id.clone(),
                credential_id: caller.credential_id,
                provider: operation.provider,
                namespace,
                binding_revision: active.revision,
                task_id: operation.task_id,
                payload,
                external_id: external_id.to_owned(),
                note: note.to_owned(),
                request_hash,
                deadline: tokio::time::Instant::now() + REQUEST_TIMEOUT,
            }))
        }
    }
}

fn resolution_receipt(
    connection: &rusqlite::Connection,
    workspace: &WorkspaceName,
    operation_id: Uuid,
) -> Result<Option<(ExternalResolution, String, [u8; 32])>, RouterErrorCode> {
    let row = connection
        .query_row(
            "SELECT resolution_id,resolver_actor_id,request_hash FROM external_resolutions WHERE workspace=?1 AND operation_id=?2",
            params![workspace.as_str(), operation_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let Some((resolution_id, resolver, hash)) = row else {
        return Ok(None);
    };
    if hash.len() != 32 {
        return Err(RouterErrorCode::StorageError);
    }
    let mut request_hash = [0; 32];
    request_hash.copy_from_slice(&hash);
    let resolution = load_resolution(connection, workspace, operation_id)?
        .filter(|value| value.id.to_string() == resolution_id)
        .ok_or(RouterErrorCode::StorageError)?;
    Ok(Some((resolution, resolver, request_hash)))
}

fn complete_not_applied_resolution(
    store: &mut RouterStore,
    caller: &CallerContext,
    mut operation: crate::tasks::ExternalOperationSummary,
    resolution_id: Uuid,
    note: &str,
    request_hash: [u8; 32],
) -> Result<ExternalResolutionCompletion, RouterErrorCode> {
    let now = now_millis().map_err(RouterErrorCode::from)?;
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    insert_resolution(
        &transaction,
        &caller.workspace,
        resolution_id,
        operation.id,
        &caller.actor_id,
        request_hash,
        ExternalResolutionOutcome::NotApplied,
        None,
        note,
        now,
    )?;
    let changed = transaction
        .execute(
            "UPDATE external_operations SET status='failed',error='operator_confirmed_not_applied',updated_at=?3 WHERE workspace=?1 AND operation_id=?2 AND status='unconfirmed'",
            params![caller.workspace.as_str(), operation.id.to_string(), now],
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    if changed != 1 {
        return Err(RouterErrorCode::ExternalConflict);
    }
    operation.status = ExternalOperationStatus::Failed;
    operation.error = Some(
        ExternalErrorCode::OperatorConfirmedNotApplied
            .as_str()
            .to_owned(),
    );
    operation.updated_at = now;
    let resolution = ExternalResolution {
        id: resolution_id,
        operation_id: operation.id,
        actor_id: caller.actor_id.clone(),
        outcome: ExternalResolutionOutcome::NotApplied,
        external_id: None,
        note: note.to_owned(),
        created_at: now,
    };
    let event = append_resolution_event(
        &transaction,
        &caller.workspace,
        &caller.actor_id,
        &operation,
        &resolution,
    )?;
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(ExternalResolutionCompletion {
        operation,
        resolution,
        event,
        task_event: None,
    })
}

fn operation_receipt(
    store: &RouterStore,
    workspace: &WorkspaceName,
    operation_id: Uuid,
) -> Result<Option<(crate::tasks::ExternalOperationSummary, String, [u8; 32])>, RouterErrorCode> {
    let row = store
        .connection()
        .query_row(
            "SELECT actor_id,request_hash FROM external_operations WHERE workspace=?1 AND operation_id=?2",
            params![workspace.as_str(), operation_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let Some((actor_id, hash)) = row else {
        return Ok(None);
    };
    if hash.len() != 32 {
        return Err(RouterErrorCode::StorageError);
    }
    let mut request_hash = [0_u8; 32];
    request_hash.copy_from_slice(&hash);
    let summary = operation_summary(store.connection(), workspace, operation_id)?
        .ok_or(RouterErrorCode::StorageError)?;
    Ok(Some((summary, actor_id, request_hash)))
}

fn operation_summary(
    connection: &rusqlite::Connection,
    workspace: &WorkspaceName,
    operation_id: Uuid,
) -> Result<Option<crate::tasks::ExternalOperationSummary>, RouterErrorCode> {
    connection
        .query_row(
            "SELECT operation_id,task_id,provider,kind,status,source_version,external_id,url,error,created_at,updated_at FROM external_operations WHERE workspace=?1 AND operation_id=?2",
            params![workspace.as_str(), operation_id.to_string()],
            |row| {
                let provider = parse_provider_sql(&row.get::<_, String>(2)?)?;
                let kind = parse_kind_sql(&row.get::<_, String>(3)?)?;
                let status = parse_status_sql(&row.get::<_, String>(4)?)?;
                Ok(crate::tasks::ExternalOperationSummary {
                    id: Uuid::parse_str(&row.get::<_, String>(0)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    task_id: row.get(1)?,
                    provider,
                    kind,
                    status,
                    source_version: row.get(5)?,
                    external_id: row.get(6)?,
                    url: row.get(7)?,
                    error: row.get(8)?,
                    created_at: row.get(9)?,
                    updated_at: row.get(10)?,
                })
            },
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)
}

fn load_resolution(
    connection: &rusqlite::Connection,
    workspace: &WorkspaceName,
    operation_id: Uuid,
) -> Result<Option<ExternalResolution>, RouterErrorCode> {
    connection
        .query_row(
            "SELECT resolution_id,operation_id,resolver_actor_id,outcome,external_id,note,created_at FROM external_resolutions WHERE workspace=?1 AND operation_id=?2",
            params![workspace.as_str(), operation_id.to_string()],
            |row| {
                Ok(ExternalResolution {
                    id: Uuid::parse_str(&row.get::<_, String>(0)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    operation_id: Uuid::parse_str(&row.get::<_, String>(1)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    actor_id: row.get(2)?,
                    outcome: match row.get::<_, String>(3)?.as_str() {
                        "applied" => ExternalResolutionOutcome::Applied,
                        "not_applied" => ExternalResolutionOutcome::NotApplied,
                        _ => return Err(rusqlite::Error::InvalidQuery),
                    },
                    external_id: row.get(4)?,
                    note: row.get(5)?,
                    created_at: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)
}

fn require_task_version(
    store: &RouterStore,
    workspace: &WorkspaceName,
    task_id: i64,
    expected_version: i64,
) -> Result<crate::tasks::TaskDetail, RouterErrorCode> {
    let task = get_task(store, workspace, task_id)
        .map_err(RouterErrorCode::from)?
        .ok_or(RouterErrorCode::TaskNotFound)?;
    if task.summary.version != expected_version {
        return Err(RouterErrorCode::TaskConflict);
    }
    Ok(task)
}

fn require_no_blocking_operation(
    store: &RouterStore,
    workspace: &WorkspaceName,
    task_id: i64,
    provider: ExternalProvider,
) -> Result<(), RouterErrorCode> {
    let status = store
        .connection()
        .query_row(
            "SELECT status FROM external_operations WHERE workspace=?1 AND task_id=?2 AND provider=?3 AND status IN('running','unconfirmed') ORDER BY updated_at DESC LIMIT 1",
            params![workspace.as_str(), task_id, provider.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    match status.as_deref() {
        Some("running") => Err(RouterErrorCode::IntegrationBusy),
        Some("unconfirmed") => Err(RouterErrorCode::ExternalUnconfirmed),
        Some(_) => Err(RouterErrorCode::StorageError),
        None => Ok(()),
    }
}

fn load_report_snapshot(
    store: &RouterStore,
    workspace: &WorkspaceName,
    task_id: i64,
    report_id: Uuid,
) -> Result<String, RouterErrorCode> {
    store
        .connection()
        .query_row(
            "SELECT kind,body_json FROM task_reports WHERE workspace=?1 AND task_id=?2 AND id=?3",
            params![workspace.as_str(), task_id, report_id.to_string()],
            |row| {
                let kind: String = row.get(0)?;
                let body: String = row.get(1)?;
                if !matches!(kind.as_str(), "note" | "checkpoint" | "result") {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                Ok(body)
            },
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?
        .ok_or(RouterErrorCode::TaskNotFound)
}

fn validate_external_body(body: &str) -> Result<(), RouterErrorCode> {
    if body.len() > 64 * 1024 {
        Err(RouterErrorCode::MessageTooLarge)
    } else {
        Ok(())
    }
}

fn append_operation_event(
    transaction: &rusqlite::Transaction<'_>,
    workspace: &WorkspaceName,
    actor_id: &str,
    operation: &crate::tasks::ExternalOperationSummary,
) -> Result<WorkspaceEvent, RouterErrorCode> {
    let content = serde_json::to_string(&IntegrationEvent {
        change: IntegrationChange::Operation,
        integration: None,
        operation: Some(operation.clone()),
        resolution: None,
    })
    .map_err(|_| RouterErrorCode::StorageError)?;
    append_event_in_transaction(
        transaction,
        &EventInsert {
            workspace,
            kind: WorkspaceEventKind::Integration,
            actor_id,
            request_id: None,
            target_id: None,
            task_id: operation.task_id,
            content: Some(&content),
            ok: None,
            error: None,
        },
    )
    .map_err(RouterErrorCode::from)
}

fn append_resolution_event(
    transaction: &rusqlite::Transaction<'_>,
    workspace: &WorkspaceName,
    actor_id: &str,
    operation: &crate::tasks::ExternalOperationSummary,
    resolution: &ExternalResolution,
) -> Result<WorkspaceEvent, RouterErrorCode> {
    let content = serde_json::to_string(&IntegrationEvent {
        change: IntegrationChange::Resolved,
        integration: None,
        operation: Some(operation.clone()),
        resolution: Some(resolution.clone()),
    })
    .map_err(|_| RouterErrorCode::StorageError)?;
    append_event_in_transaction(
        transaction,
        &EventInsert {
            workspace,
            kind: WorkspaceEventKind::Integration,
            actor_id,
            request_id: None,
            target_id: None,
            task_id: operation.task_id,
            content: Some(&content),
            ok: None,
            error: None,
        },
    )
    .map_err(RouterErrorCode::from)
}

#[allow(clippy::too_many_arguments)]
fn insert_resolution(
    transaction: &rusqlite::Transaction<'_>,
    workspace: &WorkspaceName,
    resolution_id: Uuid,
    operation_id: Uuid,
    actor_id: &str,
    request_hash: [u8; 32],
    outcome: ExternalResolutionOutcome,
    external_id: Option<&str>,
    note: &str,
    now: i64,
) -> Result<(), RouterErrorCode> {
    let outcome = match outcome {
        ExternalResolutionOutcome::Applied => "applied",
        ExternalResolutionOutcome::NotApplied => "not_applied",
    };
    transaction
        .execute(
            "INSERT INTO external_resolutions(workspace,resolution_id,operation_id,resolver_actor_id,request_hash,outcome,external_id,note,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                workspace.as_str(),
                resolution_id.to_string(),
                operation_id.to_string(),
                actor_id,
                request_hash.as_slice(),
                outcome,
                external_id,
                note,
                now,
            ],
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(())
}

const fn external_kind_str(kind: ExternalOperationKind) -> &'static str {
    match kind {
        ExternalOperationKind::Import => "import",
        ExternalOperationKind::Link => "link",
        ExternalOperationKind::PublishIssue => "publish_issue",
        ExternalOperationKind::PublishReport => "publish_report",
    }
}

fn parse_provider_sql(value: &str) -> rusqlite::Result<ExternalProvider> {
    match value {
        "github" => Ok(ExternalProvider::Github),
        "linear" => Ok(ExternalProvider::Linear),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn parse_kind_sql(value: &str) -> rusqlite::Result<ExternalOperationKind> {
    match value {
        "import" => Ok(ExternalOperationKind::Import),
        "link" => Ok(ExternalOperationKind::Link),
        "publish_issue" => Ok(ExternalOperationKind::PublishIssue),
        "publish_report" => Ok(ExternalOperationKind::PublishReport),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn parse_status_sql(value: &str) -> rusqlite::Result<ExternalOperationStatus> {
    match value {
        "running" => Ok(ExternalOperationStatus::Running),
        "succeeded" => Ok(ExternalOperationStatus::Succeeded),
        "failed" => Ok(ExternalOperationStatus::Failed),
        "unconfirmed" => Ok(ExternalOperationStatus::Unconfirmed),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

pub(crate) struct ExternalCompletion {
    pub event: WorkspaceEvent,
    pub task_event: Option<WorkspaceEvent>,
}

pub(crate) fn mark_external_dispatched(
    store: &mut RouterStore,
    prepared: &PreparedExternalOperation,
) -> Result<WorkspaceEvent, RouterErrorCode> {
    let now = now_millis().map_err(RouterErrorCode::from)?;
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let changed = transaction
        .execute(
            "UPDATE external_operations SET phase='dispatched',updated_at=?3 WHERE workspace=?1 AND operation_id=?2 AND phase='prepared' AND status='running'",
            params![
                prepared.workspace.as_str(),
                prepared.summary.id.to_string(),
                now
            ],
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    if changed != 1 {
        return Err(RouterErrorCode::ExternalConflict);
    }
    let operation = operation_summary(&transaction, &prepared.workspace, prepared.summary.id)?
        .ok_or(RouterErrorCode::StorageError)?;
    let event = append_operation_event(
        &transaction,
        &prepared.workspace,
        &prepared.actor_id,
        &operation,
    )?;
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(event)
}

pub(crate) fn complete_external_read(
    store: &mut RouterStore,
    prepared: &PreparedExternalOperation,
    issue: &ExternalIssue,
) -> Result<ExternalCompletion, RouterErrorCode> {
    match prepared.payload.as_ref() {
        Some(OperationPayload::Import { .. }) => complete_import(store, prepared, issue),
        Some(OperationPayload::Link { .. }) => complete_link(store, prepared, issue),
        _ => Err(RouterErrorCode::StorageError),
    }
}

pub(crate) fn complete_external_mutation(
    store: &mut RouterStore,
    prepared: &PreparedExternalOperation,
    mutation: &ExternalMutation,
) -> Result<ExternalCompletion, RouterErrorCode> {
    let now = now_millis().map_err(RouterErrorCode::from)?;
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let mut task_event = None;
    match prepared.payload.as_ref() {
        Some(OperationPayload::PublishIssue { .. }) => {
            let task_id = prepared
                .summary
                .task_id
                .ok_or(RouterErrorCode::StorageError)?;
            transaction
                .execute(
                    "INSERT INTO external_links(workspace,task_id,provider,namespace,external_id,url,linked_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        prepared.workspace.as_str(),
                        task_id,
                        prepared.summary.provider.as_str(),
                        prepared.namespace,
                        mutation.external_id,
                        mutation.url,
                        now,
                    ],
                )
                .map_err(crate::store::StoreError::Sqlite)
                .map_err(RouterErrorCode::from)?;
            transaction
                .execute(
                    "UPDATE tasks SET version=version+1,updated_by=?3,updated_at=?4 WHERE workspace=?1 AND id=?2",
                    params![
                        prepared.workspace.as_str(),
                        task_id,
                        prepared.actor_id,
                        now
                    ],
                )
                .map_err(crate::store::StoreError::Sqlite)
                .map_err(RouterErrorCode::from)?;
            task_event = Some(append_external_task_event(
                &transaction,
                &prepared.workspace,
                &prepared.actor_id,
                task_id,
                prepared.summary.id,
            )?);
        }
        Some(OperationPayload::PublishReport { .. }) => {}
        _ => return Err(RouterErrorCode::StorageError),
    }
    let operation = update_terminal_operation(
        &transaction,
        prepared,
        ExternalOperationStatus::Succeeded,
        Some(&mutation.external_id),
        Some(&mutation.url),
        None,
        now,
    )?;
    let event = append_operation_event(
        &transaction,
        &prepared.workspace,
        &prepared.actor_id,
        &operation,
    )?;
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(ExternalCompletion { event, task_event })
}

pub(crate) fn complete_external_error(
    store: &mut RouterStore,
    prepared: &PreparedExternalOperation,
    error: IntegrationClientError,
) -> Result<ExternalCompletion, RouterErrorCode> {
    let (status, code) = match error {
        IntegrationClientError::Busy => (ExternalOperationStatus::Failed, "integration_busy"),
        IntegrationClientError::Failed(code) => (ExternalOperationStatus::Failed, code.as_str()),
        IntegrationClientError::Unconfirmed(_) => {
            (ExternalOperationStatus::Unconfirmed, "external_unconfirmed")
        }
    };
    terminal_external_error(store, prepared, status, code)
}

pub(crate) fn terminal_external_error(
    store: &mut RouterStore,
    prepared: &PreparedExternalOperation,
    status: ExternalOperationStatus,
    error: &str,
) -> Result<ExternalCompletion, RouterErrorCode> {
    let now = now_millis().map_err(RouterErrorCode::from)?;
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let operation =
        update_terminal_operation(&transaction, prepared, status, None, None, Some(error), now)?;
    let event = append_operation_event(
        &transaction,
        &prepared.workspace,
        &prepared.actor_id,
        &operation,
    )?;
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(ExternalCompletion {
        event,
        task_event: None,
    })
}

fn complete_import(
    store: &mut RouterStore,
    prepared: &PreparedExternalOperation,
    issue: &ExternalIssue,
) -> Result<ExternalCompletion, RouterErrorCode> {
    validate_title(&issue.title).map_err(|_| RouterErrorCode::InvalidMessage)?;
    validate_description(&issue.description).map_err(|_| RouterErrorCode::InvalidMessage)?;
    let now = now_millis().map_err(RouterErrorCode::from)?;
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let existing = transaction
        .query_row(
            "SELECT workspace,task_id FROM external_links WHERE provider=?1 AND namespace=?2 AND external_id=?3",
            params![
                prepared.summary.provider.as_str(),
                prepared.namespace,
                issue.external_id
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let (task_id, task_event) = if let Some((workspace, task_id)) = existing {
        if workspace != prepared.workspace.as_str() {
            return terminal_semantic_conflict(
                transaction,
                prepared,
                RouterErrorCode::ExternalConflict,
                now,
            );
        }
        (task_id, None)
    } else {
        let task_id: i64 = transaction
            .query_row(
                "UPDATE workspaces SET next_task_id=next_task_id+1 WHERE name=?1 AND next_task_id<=?2 RETURNING next_task_id-1",
                params![prepared.workspace.as_str(), MAX_SAFE_INTEGER],
                |row| row.get(0),
            )
            .optional()
            .map_err(crate::store::StoreError::Sqlite)
            .map_err(RouterErrorCode::from)?
            .ok_or(RouterErrorCode::StorageError)?;
        transaction
            .execute(
                "INSERT INTO tasks(workspace,id,version,title,description,state,assigned_agent_id,current_attempt_id,last_attempt_id,last_checkpoint_id,result_report_id,pause_reason,created_by,updated_by,created_at,updated_at) VALUES(?1,?2,1,?3,?4,'todo',NULL,NULL,NULL,NULL,NULL,NULL,?5,?5,?6,?6)",
                params![
                    prepared.workspace.as_str(),
                    task_id,
                    issue.title,
                    issue.description,
                    prepared.actor_id,
                    now,
                ],
            )
            .map_err(crate::store::StoreError::Sqlite)
            .map_err(RouterErrorCode::from)?;
        let note_id = Uuid::new_v4();
        let note = format!(
            "Imported from {} {} in external state {}.",
            prepared.summary.provider.as_str(),
            issue.external_id,
            issue.state
        );
        let note_json = serde_json::to_string(&note).map_err(|_| RouterErrorCode::StorageError)?;
        transaction
            .execute(
                "INSERT INTO task_reports(id,workspace,task_id,attempt_id,actor_id,kind,body_json,created_at) VALUES(?1,?2,?3,NULL,?4,'note',?5,?6)",
                params![
                    note_id.to_string(),
                    prepared.workspace.as_str(),
                    task_id,
                    prepared.actor_id,
                    note_json,
                    now,
                ],
            )
            .map_err(crate::store::StoreError::Sqlite)
            .map_err(RouterErrorCode::from)?;
        transaction
            .execute(
                "INSERT INTO external_links(workspace,task_id,provider,namespace,external_id,url,linked_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    prepared.workspace.as_str(),
                    task_id,
                    prepared.summary.provider.as_str(),
                    prepared.namespace,
                    issue.external_id,
                    issue.url,
                    now,
                ],
            )
            .map_err(crate::store::StoreError::Sqlite)
            .map_err(RouterErrorCode::from)?;
        (
            task_id,
            Some(append_external_task_event(
                &transaction,
                &prepared.workspace,
                &prepared.actor_id,
                task_id,
                prepared.summary.id,
            )?),
        )
    };
    let mut adjusted = prepared.clone();
    adjusted.summary.task_id = Some(task_id);
    let operation = update_terminal_operation(
        &transaction,
        &adjusted,
        ExternalOperationStatus::Succeeded,
        Some(&issue.external_id),
        Some(&issue.url),
        None,
        now,
    )?;
    let event = append_operation_event(
        &transaction,
        &prepared.workspace,
        &prepared.actor_id,
        &operation,
    )?;
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(ExternalCompletion { event, task_event })
}

fn complete_link(
    store: &mut RouterStore,
    prepared: &PreparedExternalOperation,
    issue: &ExternalIssue,
) -> Result<ExternalCompletion, RouterErrorCode> {
    let Some(OperationPayload::Link {
        expected_version,
        replace,
        ..
    }) = prepared.payload.as_ref()
    else {
        return Err(RouterErrorCode::StorageError);
    };
    let task_id = prepared
        .summary
        .task_id
        .ok_or(RouterErrorCode::StorageError)?;
    let now = now_millis().map_err(RouterErrorCode::from)?;
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let version = transaction
        .query_row(
            "SELECT version FROM tasks WHERE workspace=?1 AND id=?2",
            params![prepared.workspace.as_str(), task_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?
        .ok_or(RouterErrorCode::TaskNotFound)?;
    if version != *expected_version {
        return terminal_semantic_conflict(
            transaction,
            prepared,
            RouterErrorCode::TaskConflict,
            now,
        );
    }
    let collision = transaction
        .query_row(
            "SELECT workspace,task_id FROM external_links WHERE provider=?1 AND namespace=?2 AND external_id=?3",
            params![
                prepared.summary.provider.as_str(),
                prepared.namespace,
                issue.external_id
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    if collision.as_ref().is_some_and(|(workspace, linked_task)| {
        workspace != prepared.workspace.as_str() || *linked_task != task_id
    }) {
        return terminal_semantic_conflict(
            transaction,
            prepared,
            RouterErrorCode::ExternalConflict,
            now,
        );
    }
    let existing = transaction
        .query_row(
            "SELECT namespace,external_id,url FROM external_links WHERE workspace=?1 AND task_id=?2 AND provider=?3",
            params![
                prepared.workspace.as_str(),
                task_id,
                prepared.summary.provider.as_str()
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let unchanged = existing.as_ref().is_some_and(|(namespace, id, _)| {
        namespace == &prepared.namespace && id == &issue.external_id
    });
    if existing.is_some() && !unchanged && !replace {
        return terminal_semantic_conflict(
            transaction,
            prepared,
            RouterErrorCode::ExternalConflict,
            now,
        );
    }
    let task_event = if unchanged {
        None
    } else {
        transaction
            .execute(
                "INSERT INTO external_links(workspace,task_id,provider,namespace,external_id,url,linked_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(workspace,task_id,provider) DO UPDATE SET namespace=excluded.namespace,external_id=excluded.external_id,url=excluded.url,linked_at=excluded.linked_at",
                params![
                    prepared.workspace.as_str(),
                    task_id,
                    prepared.summary.provider.as_str(),
                    prepared.namespace,
                    issue.external_id,
                    issue.url,
                    now,
                ],
            )
            .map_err(crate::store::StoreError::Sqlite)
            .map_err(RouterErrorCode::from)?;
        transaction
            .execute(
                "UPDATE tasks SET version=version+1,updated_by=?3,updated_at=?4 WHERE workspace=?1 AND id=?2",
                params![
                    prepared.workspace.as_str(),
                    task_id,
                    prepared.actor_id,
                    now
                ],
            )
            .map_err(crate::store::StoreError::Sqlite)
            .map_err(RouterErrorCode::from)?;
        Some(append_external_task_event(
            &transaction,
            &prepared.workspace,
            &prepared.actor_id,
            task_id,
            prepared.summary.id,
        )?)
    };
    let operation = update_terminal_operation(
        &transaction,
        prepared,
        ExternalOperationStatus::Succeeded,
        Some(&issue.external_id),
        Some(&issue.url),
        None,
        now,
    )?;
    let event = append_operation_event(
        &transaction,
        &prepared.workspace,
        &prepared.actor_id,
        &operation,
    )?;
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(ExternalCompletion { event, task_event })
}

fn update_terminal_operation(
    transaction: &rusqlite::Transaction<'_>,
    prepared: &PreparedExternalOperation,
    status: ExternalOperationStatus,
    external_id: Option<&str>,
    url: Option<&str>,
    error: Option<&str>,
    now: i64,
) -> Result<crate::tasks::ExternalOperationSummary, RouterErrorCode> {
    let phase = "terminal";
    let changed = transaction
        .execute(
            "UPDATE external_operations SET task_id=?3,phase=?4,status=?5,external_id=?6,url=?7,error=?8,updated_at=?9 WHERE workspace=?1 AND operation_id=?2 AND status='running'",
            params![
                prepared.workspace.as_str(),
                prepared.summary.id.to_string(),
                prepared.summary.task_id,
                phase,
                external_status_str(status),
                external_id,
                url,
                error,
                now,
            ],
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    if changed != 1 {
        return Err(RouterErrorCode::ExternalConflict);
    }
    Ok(crate::tasks::ExternalOperationSummary {
        status,
        external_id: external_id.map(str::to_owned),
        url: url.map(str::to_owned),
        error: error.map(str::to_owned),
        updated_at: now,
        ..prepared.summary.clone()
    })
}

fn terminal_semantic_conflict(
    transaction: rusqlite::Transaction<'_>,
    prepared: &PreparedExternalOperation,
    code: RouterErrorCode,
    now: i64,
) -> Result<ExternalCompletion, RouterErrorCode> {
    let operation = update_terminal_operation(
        &transaction,
        prepared,
        ExternalOperationStatus::Failed,
        None,
        None,
        Some(code.as_str()),
        now,
    )?;
    let event = append_operation_event(
        &transaction,
        &prepared.workspace,
        &prepared.actor_id,
        &operation,
    )?;
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(ExternalCompletion {
        event,
        task_event: None,
    })
}

fn append_external_task_event(
    transaction: &rusqlite::Transaction<'_>,
    workspace: &WorkspaceName,
    actor_id: &str,
    task_id: i64,
    operation_id: Uuid,
) -> Result<WorkspaceEvent, RouterErrorCode> {
    let task = crate::tasks::get_task_detail_connection(transaction, workspace, task_id)
        .map_err(RouterErrorCode::from)?
        .ok_or(RouterErrorCode::TaskNotFound)?;
    let content = serde_json::to_string(&TaskEvent {
        change: TaskChange::External,
        task: task.summary,
        attempt_id: None,
        report_id: None,
        external_operation_id: Some(operation_id),
    })
    .map_err(|_| RouterErrorCode::StorageError)?;
    append_event_in_transaction(
        transaction,
        &EventInsert {
            workspace,
            kind: WorkspaceEventKind::Task,
            actor_id,
            request_id: None,
            target_id: None,
            task_id: Some(task_id),
            content: Some(&content),
            ok: None,
            error: None,
        },
    )
    .map_err(RouterErrorCode::from)
}

const fn external_status_str(status: ExternalOperationStatus) -> &'static str {
    match status {
        ExternalOperationStatus::Running => "running",
        ExternalOperationStatus::Succeeded => "succeeded",
        ExternalOperationStatus::Failed => "failed",
        ExternalOperationStatus::Unconfirmed => "unconfirmed",
    }
}

pub(crate) fn validate_external_snapshot(
    store: &mut RouterStore,
    active: &ActiveIntegration,
    prepared: &PreparedExternalOperation,
    dispatch_mutation: bool,
) -> Result<(), RouterErrorCode> {
    if active.revision != prepared.binding_revision
        || active.connection.namespace() != prepared.namespace
        || active.connection.provider() != prepared.summary.provider
        || active.connection.workspace() != &prepared.workspace
    {
        return Err(RouterErrorCode::IntegrationNotConfigured);
    }
    let credential = store
        .credential_by_id(prepared.credential_id)
        .map_err(RouterErrorCode::from)?
        .ok_or(RouterErrorCode::PermissionDenied)?;
    let claims = credential.claims;
    let admin = claims.role == crate::credentials::CredentialRole::Operator
        && claims.subject == "admin"
        && claims.workspaces.is_empty();
    if credential.revoked_at.is_some()
        || (!admin
            && !claims
                .workspaces
                .iter()
                .any(|workspace| workspace == &prepared.workspace))
    {
        return Err(RouterErrorCode::PermissionDenied);
    }
    match prepared.payload.as_ref() {
        Some(OperationPayload::PublishIssue { .. }) => {
            let task_id = prepared
                .summary
                .task_id
                .ok_or(RouterErrorCode::StorageError)?;
            let linked: bool = store
                .connection()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM external_links WHERE workspace=?1 AND task_id=?2 AND provider=?3)",
                    params![
                        prepared.workspace.as_str(),
                        task_id,
                        prepared.summary.provider.as_str()
                    ],
                    |row| row.get(0),
                )
                .map_err(crate::store::StoreError::Sqlite)
                .map_err(RouterErrorCode::from)?;
            if linked {
                return Err(RouterErrorCode::ExternalConflict);
            }
        }
        Some(OperationPayload::PublishReport { issue_id, .. }) => {
            let task_id = prepared
                .summary
                .task_id
                .ok_or(RouterErrorCode::StorageError)?;
            let linked = store
                .connection()
                .query_row(
                    "SELECT namespace,external_id FROM external_links WHERE workspace=?1 AND task_id=?2 AND provider=?3",
                    params![
                        prepared.workspace.as_str(),
                        task_id,
                        prepared.summary.provider.as_str()
                    ],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(crate::store::StoreError::Sqlite)
                .map_err(RouterErrorCode::from)?;
            if linked.as_ref() != Some(&(prepared.namespace.clone(), issue_id.clone())) {
                return Err(RouterErrorCode::ExternalConflict);
            }
        }
        Some(OperationPayload::Import { .. } | OperationPayload::Link { .. }) => {}
        None => return Err(RouterErrorCode::ExternalConflict),
    }
    if dispatch_mutation {
        mark_external_dispatched(store, prepared)?;
    }
    Ok(())
}

pub(crate) fn validate_resolution_snapshot(
    store: &RouterStore,
    active: &ActiveIntegration,
    prepared: &PreparedExternalResolution,
) -> Result<(), RouterErrorCode> {
    if tokio::time::Instant::now() >= prepared.deadline {
        return Err(RouterErrorCode::RequestTimeout);
    }
    if active.revision != prepared.binding_revision
        || active.connection.namespace() != prepared.namespace
        || active.connection.provider() != prepared.provider
        || active.connection.workspace() != &prepared.workspace
    {
        return Err(RouterErrorCode::IntegrationNotConfigured);
    }
    let credential = store
        .credential_by_id(prepared.credential_id)
        .map_err(RouterErrorCode::from)?
        .ok_or(RouterErrorCode::PermissionDenied)?;
    let claims = credential.claims;
    let admin = claims.role == crate::credentials::CredentialRole::Operator
        && claims.subject == "admin"
        && claims.workspaces.is_empty();
    if credential.revoked_at.is_some()
        || claims.role != crate::credentials::CredentialRole::Operator
        || (!admin
            && !claims
                .workspaces
                .iter()
                .any(|workspace| workspace == &prepared.workspace))
    {
        return Err(RouterErrorCode::PermissionDenied);
    }
    let unresolved: bool = store
        .connection()
        .query_row(
            "SELECT status='unconfirmed' AND NOT EXISTS(SELECT 1 FROM external_resolutions WHERE workspace=?1 AND operation_id=?2) FROM external_operations WHERE workspace=?1 AND operation_id=?2",
            params![prepared.workspace.as_str(), prepared.operation_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?
        .unwrap_or(false);
    if !unresolved {
        return Err(RouterErrorCode::ExternalConflict);
    }
    match &prepared.payload {
        OperationPayload::PublishIssue { .. } => {
            let task_id = prepared.task_id.ok_or(RouterErrorCode::StorageError)?;
            let linked: bool = store
                .connection()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM external_links WHERE workspace=?1 AND task_id=?2 AND provider=?3)",
                    params![prepared.workspace.as_str(), task_id, prepared.provider.as_str()],
                    |row| row.get(0),
                )
                .map_err(crate::store::StoreError::Sqlite)
                .map_err(RouterErrorCode::from)?;
            if linked {
                return Err(RouterErrorCode::ExternalConflict);
            }
        }
        OperationPayload::PublishReport { issue_id, .. } => {
            let task_id = prepared.task_id.ok_or(RouterErrorCode::StorageError)?;
            let linked = store
                .connection()
                .query_row(
                    "SELECT namespace,external_id FROM external_links WHERE workspace=?1 AND task_id=?2 AND provider=?3",
                    params![prepared.workspace.as_str(), task_id, prepared.provider.as_str()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(crate::store::StoreError::Sqlite)
                .map_err(RouterErrorCode::from)?;
            if linked.as_ref() != Some(&(prepared.namespace.clone(), issue_id.clone())) {
                return Err(RouterErrorCode::ExternalConflict);
            }
        }
        OperationPayload::Import { .. } | OperationPayload::Link { .. } => {
            return Err(RouterErrorCode::ExternalConflict);
        }
    }
    Ok(())
}

pub(crate) fn complete_applied_resolution(
    store: &mut RouterStore,
    prepared: &PreparedExternalResolution,
    mutation: &ExternalMutation,
) -> Result<ExternalResolutionCompletion, RouterErrorCode> {
    if mutation.external_id != prepared.external_id {
        return Err(RouterErrorCode::ExternalConflict);
    }
    let now = now_millis().map_err(RouterErrorCode::from)?;
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    let mut task_event = None;
    match &prepared.payload {
        OperationPayload::PublishIssue { .. } => {
            let task_id = prepared.task_id.ok_or(RouterErrorCode::StorageError)?;
            transaction
                .execute(
                    "INSERT INTO external_links(workspace,task_id,provider,namespace,external_id,url,linked_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        prepared.workspace.as_str(),
                        task_id,
                        prepared.provider.as_str(),
                        prepared.namespace,
                        mutation.external_id,
                        mutation.url,
                        now,
                    ],
                )
                .map_err(crate::store::StoreError::Sqlite)
                .map_err(RouterErrorCode::from)?;
            let changed = transaction
                .execute(
                    "UPDATE tasks SET version=version+1,updated_by=?3,updated_at=?4 WHERE workspace=?1 AND id=?2",
                    params![
                        prepared.workspace.as_str(),
                        task_id,
                        prepared.actor_id,
                        now
                    ],
                )
                .map_err(crate::store::StoreError::Sqlite)
                .map_err(RouterErrorCode::from)?;
            if changed != 1 {
                return Err(RouterErrorCode::TaskNotFound);
            }
            task_event = Some(append_external_task_event(
                &transaction,
                &prepared.workspace,
                &prepared.actor_id,
                task_id,
                prepared.operation_id,
            )?);
        }
        OperationPayload::PublishReport { .. } => {}
        OperationPayload::Import { .. } | OperationPayload::Link { .. } => {
            return Err(RouterErrorCode::ExternalConflict);
        }
    }
    let changed = transaction
        .execute(
            "UPDATE external_operations SET status='succeeded',external_id=?3,url=?4,error=NULL,updated_at=?5 WHERE workspace=?1 AND operation_id=?2 AND status='unconfirmed'",
            params![
                prepared.workspace.as_str(),
                prepared.operation_id.to_string(),
                mutation.external_id,
                mutation.url,
                now
            ],
        )
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    if changed != 1 {
        return Err(RouterErrorCode::ExternalConflict);
    }
    insert_resolution(
        &transaction,
        &prepared.workspace,
        prepared.resolution_id,
        prepared.operation_id,
        &prepared.actor_id,
        prepared.request_hash,
        ExternalResolutionOutcome::Applied,
        Some(&mutation.external_id),
        &prepared.note,
        now,
    )?;
    let operation = operation_summary(&transaction, &prepared.workspace, prepared.operation_id)?
        .ok_or(RouterErrorCode::StorageError)?;
    let resolution = ExternalResolution {
        id: prepared.resolution_id,
        operation_id: prepared.operation_id,
        actor_id: prepared.actor_id.clone(),
        outcome: ExternalResolutionOutcome::Applied,
        external_id: Some(mutation.external_id.clone()),
        note: prepared.note.clone(),
        created_at: now,
    };
    let event = append_resolution_event(
        &transaction,
        &prepared.workspace,
        &prepared.actor_id,
        &operation,
        &resolution,
    )?;
    transaction
        .commit()
        .map_err(crate::store::StoreError::Sqlite)
        .map_err(RouterErrorCode::from)?;
    Ok(ExternalResolutionCompletion {
        operation,
        resolution,
        event,
        task_event,
    })
}
