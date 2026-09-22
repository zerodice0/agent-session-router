use std::{
    env,
    ffi::{OsStr, OsString},
    fs::File,
    io::Read as _,
    os::unix::fs::MetadataExt as _,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use futures_util::StreamExt as _;
use serde::Deserialize;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    process::Command,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    sync::CancellationToken,
};

use crate::{
    bootstrap::routes::{RouteError, probe_routes},
    cli::{McpArgs, McpRoleArg, escape_terminal},
    client::{
        AgentSendResult, ClientConfig, ClientError, ClientEvent, ClientEvents, ClientRole,
        RouterClient,
    },
    config::{
        self, ConfigError, DELEGATE_CONTEXT_VERSION, DelegateLaunchContext, ProviderSelection,
    },
    credentials::{self, CredentialError, CredentialRole, SecretToken},
    install::{AssetError, resolve_integration_asset},
    mcp::{
        BackendError, McpRole, McpRuntimeError, McpServer, RouterMcpBackend, catalog, serve_stdio,
    },
    onboarding::{MAX_CA_BYTES, OnboardingProvider, OnboardingRoute, validate_ca_pem},
    process::{LaunchError, LaunchMode, LaunchPlan, MAX_COMMAND_OUTPUT_BYTES},
    protocol::{
        AgentClient, AgentRegistration, AgentSide, ClientMessage, DeliveryMode, RouterErrorCode,
        ServerMessage, TaskDispatch, TaskFence, WorkspaceName, is_agent_id, normalize_timeout_ms,
    },
    providers::{
        CancelReason, ManagedLaunch, OwnedProvider, ProviderError, SessionRequest, SessionResult,
        TerminalEvidence,
        claude::{ClaudeConfig, ClaudeProvider},
        codex::{CodexConfig, CodexProvider, ThreadSelection},
        router::{RouterClientLifecycle, RouterManagedProvider},
        validate_delegate_context,
    },
};

const HOST_CLOSE_TIMEOUT: Duration = Duration::from_secs(7);
const OMP_PACKAGE: &str = "omp/package.json";
const CODEX_MCP_PREFIX: &str = "mcp_servers.agent_session_router";
const OMP_PACKAGE_NAME: &str = "@agent-session-router/omp-integration";
const OMP_PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(5);
const INTERACTIVE_LINE_BYTES: usize = 64 * 1024;
const INTERACTIVE_TURN_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Debug, Error)]
pub enum HostError {
    #[error("{0}")]
    Backend(#[from] BackendError),
    #[error("{0}")]
    Mcp(#[from] McpRuntimeError),
    #[error("{0}")]
    Client(#[from] ClientError),
    #[error("{0}")]
    Provider(#[from] ProviderError),
    #[error("{0}")]
    Asset(#[from] AssetError),
    #[error("{0}")]
    Launch(#[from] LaunchError),
    #[error("{0}")]
    Config(#[from] ConfigError),
    #[error("{0}")]
    Credential(#[from] CredentialError),
    #[error("{0}")]
    Route(#[from] RouteError),
    #[error("MCP dry-run commands must be planned before credential resolution")]
    McpDryRun,
    #[error("delegate MCP does not accept profile or credential selection")]
    McpDelegateSelection,
    #[error("MCP role does not match the selected credential claims")]
    McpCredentialClaims,
    #[error("interactive Codex requires matching primary codex-app-server claims")]
    InteractiveCodexClaims,
    #[error("interactive terminal I/O failed")]
    InteractiveIo,
    #[error("managed provider requires a primary client with a delegation token")]
    DelegationRequired,
    #[error("managed provider CA file must be an absolute UTF-8 path")]
    InvalidCaFile,
    #[error("current executable must be an absolute UTF-8 path")]
    InvalidCurrentExecutable,
    #[error("OMP plugin inspection failed")]
    OmpPreflightFailed,
    #[error("OMP integration is not linked; run `asr setup-omp`")]
    OmpSetupRequired,
    #[error(
        "OMP integration is disabled; run `omp plugin enable @agent-session-router/omp-integration`"
    )]
    OmpEnableRequired,
    #[error("a different agent-session-router OMP integration is already installed")]
    OmpSetupConflict,
    #[error("provider host closed: {0}")]
    RouterClosed(RouterErrorCode),
    #[error("provider host close timed out")]
    CloseTimeout,
    #[error("provider host task failed")]
    ProviderTask,
}

pub struct McpInvocation {
    pub role: McpRole,
    pub agent_id: String,
    pub config: ClientConfig,
    pub initial_workspace: Option<WorkspaceName>,
    pub provider_selection: Option<ProviderSelection>,
}

pub fn mcp_invocation(
    args: &McpArgs,
    profile: Option<&str>,
    credential_file: Option<&Path>,
    dry_run: bool,
) -> Result<McpInvocation, HostError> {
    if dry_run {
        return Err(HostError::McpDryRun);
    }
    match &args.role {
        McpRoleArg::Delegate { context_file } => {
            delegate_mcp_invocation(context_file, profile, credential_file)
        }
        role => primary_mcp_invocation(role, profile, credential_file),
    }
}

fn delegate_mcp_invocation(
    context_file: &Path,
    profile: Option<&str>,
    credential_file: Option<&Path>,
) -> Result<McpInvocation, HostError> {
    if profile.is_some() || credential_file.is_some() {
        return Err(HostError::McpDelegateSelection);
    }
    if !context_file.is_absolute() {
        return Err(ProviderError::InvalidLaunchContext.into());
    }
    let context = validate_delegate_context(context_file)?;
    if !is_agent_id(&context.owner_id) {
        return Err(ProviderError::InvalidLaunchContext.into());
    }
    let router_url = config::normalize_router_url(&context.router_url)?;
    let delegation_token = SecretToken::parse(context.delegation_token)?;
    let ca_file = context.ca_file.map(PathBuf::from);
    if ca_file.as_ref().is_some_and(|path| !path.is_absolute()) {
        return Err(HostError::InvalidCaFile);
    }
    Ok(McpInvocation {
        role: McpRole::Delegate,
        agent_id: context.owner_id.clone(),
        config: ClientConfig {
            router_url,
            role: ClientRole::Delegate {
                owner_id: context.owner_id,
                delegation_token,
            },
            ca_file,
        },
        initial_workspace: None,
        provider_selection: None,
    })
}

fn primary_mcp_invocation(
    role: &McpRoleArg,
    profile: Option<&str>,
    credential_file: Option<&Path>,
) -> Result<McpInvocation, HostError> {
    let (provider, expected_side, expected_client, delivery_mode) = match role {
        McpRoleArg::CodexCli => (
            OnboardingProvider::CodexCli,
            AgentSide::Codex,
            AgentClient::CodexCli,
            DeliveryMode::Pull,
        ),
        McpRoleArg::ClaudeChannel => (
            OnboardingProvider::ClaudeCode,
            AgentSide::Claude,
            AgentClient::ClaudeCode,
            DeliveryMode::Push,
        ),
        McpRoleArg::Omp => (
            OnboardingProvider::Omp,
            AgentSide::Generic,
            AgentClient::Omp,
            DeliveryMode::Push,
        ),
        McpRoleArg::Delegate { .. } => return Err(HostError::McpCredentialClaims),
    };
    let environment_profile = env::var("ASR_PROFILE")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let provider_selection = config::select_provider(
        profile.or(environment_profile.as_deref()),
        credential_file,
        provider,
    )?;
    let selection = &provider_selection.selection;
    let credential_path = selection
        .credential_file
        .as_deref()
        .ok_or(ConfigError::Required)?;
    let credential = credentials::read_credential(credential_path)?;
    let (Some(side), Some(client)) = (credential.agent_side, credential.agent_client) else {
        return Err(HostError::McpCredentialClaims);
    };
    if credential.role != CredentialRole::Agent
        || side != expected_side
        || client != expected_client
    {
        return Err(HostError::McpCredentialClaims);
    }
    let agent_id = credential.subject.clone();
    let initial_workspace = if matches!(role, McpRoleArg::CodexCli | McpRoleArg::ClaudeChannel) {
        env::var_os("ASR_WORKSPACE")
            .filter(|value| !value.is_empty())
            .map(|value| {
                value
                    .into_string()
                    .map_err(|_| ConfigError::Invalid)
                    .and_then(|value| WorkspaceName::parse(value).map_err(|_| ConfigError::Invalid))
            })
            .transpose()?
            .or_else(|| provider_selection.initial_workspace.clone())
    } else {
        None
    };
    Ok(McpInvocation {
        role: role.mcp_role(),
        agent_id: agent_id.clone(),
        config: ClientConfig {
            router_url: selection.router_url.clone(),
            role: ClientRole::Primary {
                agent: AgentRegistration {
                    agent_id,
                    side,
                    client,
                    activity: None,
                    delivery_mode,
                },
                credential,
                delegation_token: None,
            },
            ca_file: provider_selection.ca_file.clone(),
        },
        initial_workspace,
        provider_selection: Some(provider_selection),
    })
}

/// Selects a verified route only before opening a new provider MCP connection.
/// Reconnects of a live client retain their existing endpoint.
pub async fn resolve_mcp_route(invocation: &mut McpInvocation) -> Result<(), HostError> {
    let Some(selection) = invocation.provider_selection.as_ref() else {
        return Ok(());
    };
    let Some(server_id) = selection.expected_server_id else {
        return Ok(());
    };
    let explicit_pem = if env::var_os("ASR_CA_FILE").is_some_and(|value| !value.is_empty()) {
        selection
            .ca_file
            .as_deref()
            .map(|path| read_provider_ca(path, false))
            .transpose()?
    } else {
        None
    };
    let routes = selection
        .routes
        .iter()
        .map(|route| {
            let ca_pem = if route.router_url.starts_with("wss://") {
                match &explicit_pem {
                    Some(pem) => Some(pem.clone()),
                    None => route
                        .ca_file
                        .as_deref()
                        .map(|path| read_provider_ca(path, true))
                        .transpose()?,
                }
            } else {
                None
            };
            Ok(OnboardingRoute {
                kind: route.kind,
                router_url: route.router_url.clone(),
                ca_pem,
            })
        })
        .collect::<Result<Vec<_>, RouteError>>()?;
    let config_path = config::config_path()?;
    let ca_directory = config_path
        .parent()
        .ok_or(ConfigError::Invalid)?
        .join("onboarding/ca");
    let verified = probe_routes(&routes, server_id, None, &ca_directory).await?;
    invocation.config.router_url = config::normalize_router_url(&verified.route.router_url)?;
    // Use the exact validated CA bytes persisted by the probe, including an
    // explicit override, rather than reopening a possibly changed input file.
    invocation.config.ca_file = verified.ca_file;
    Ok(())
}

fn read_provider_ca(path: &Path, private: bool) -> Result<String, RouteError> {
    use rustix::fs::{Mode, OFlags, open, openat};

    let uid = rustix::process::getuid().as_raw();
    let file_flags = OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let file = if private {
        let parent = path.parent().ok_or(RouteError::InvalidCa)?;
        let name = path.file_name().ok_or(RouteError::InvalidCa)?;
        let directory_flags =
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let mut directory = File::from(
            open(
                if path.is_absolute() { "/" } else { "." },
                directory_flags,
                Mode::empty(),
            )
            .map_err(|_| RouteError::InvalidCa)?,
        );
        for component in parent.components() {
            match component {
                Component::Normal(name) => {
                    directory = File::from(
                        openat(&directory, name, directory_flags, Mode::empty())
                            .map_err(|_| RouteError::InvalidCa)?,
                    );
                }
                Component::RootDir | Component::CurDir => {}
                Component::ParentDir | Component::Prefix(_) => return Err(RouteError::InvalidCa),
            }
        }
        let metadata = directory.metadata().map_err(|_| RouteError::InvalidCa)?;
        if metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
            return Err(RouteError::InvalidCa);
        }
        File::from(
            openat(
                &directory,
                name,
                file_flags | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(|_| RouteError::InvalidCa)?,
        )
    } else {
        // An explicit CA may be a system-owned file or user-selected symlink.
        File::from(open(path, file_flags, Mode::empty()).map_err(|_| RouteError::InvalidCa)?)
    };
    let metadata = file.metadata().map_err(|_| RouteError::InvalidCa)?;
    if !metadata.is_file()
        || metadata.len() > MAX_CA_BYTES as u64
        || (private
            && (metadata.uid() != uid || metadata.mode() & 0o077 != 0 || metadata.nlink() != 1))
    {
        return Err(RouteError::InvalidCa);
    }
    let mut pem = String::new();
    file.take((MAX_CA_BYTES + 1) as u64)
        .read_to_string(&mut pem)
        .map_err(|_| RouteError::InvalidCa)?;
    validate_ca_pem(&pem).map_err(|_| RouteError::InvalidCa)?;
    Ok(pem)
}

/// Runs the production router MCP server over the bounded stdio transport.
pub async fn run_mcp_stdio(
    role: McpRole,
    agent_id: impl Into<Arc<str>>,
    config: ClientConfig,
    initial_workspace: Option<WorkspaceName>,
) -> Result<(), HostError> {
    let agent_id = agent_id.into();
    let backend = Arc::new(RouterMcpBackend::new_with_initial_workspace(
        role,
        Arc::clone(&agent_id),
        config,
        initial_workspace,
    )?);
    let server = McpServer::new(backend, role, agent_id);
    serve_stdio(server).await.map_err(HostError::from)
}

struct SharedProvider<P>(Arc<P>);

impl<P> Clone for SharedProvider<P> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<P: OwnedProvider> OwnedProvider for SharedProvider<P> {
    fn ready(&self) -> bool {
        self.0.ready()
    }

    async fn handle(&self, request: SessionRequest) -> SessionResult {
        self.0.handle(request).await
    }

    async fn cancel(&self, request_id: String, reason: CancelReason) -> Result<(), ProviderError> {
        self.0.cancel(request_id, reason).await
    }

    async fn close(&self) -> Result<(), ProviderError> {
        self.0.close().await
    }

    fn subscribe_terminal(&self) -> broadcast::Receiver<TerminalEvidence> {
        self.0.subscribe_terminal()
    }
}

type Managed<P> = Arc<RouterManagedProvider<SharedProvider<P>>>;

struct ActiveTurn {
    request_id: String,
    workspace: WorkspaceName,
    task_id: Option<i64>,
    cancelled: bool,
    task: JoinHandle<()>,
}

struct TurnOutcome {
    request_id: String,
    result: SessionResult,
}

fn uses_explicit_readiness(config: &ClientConfig) -> bool {
    matches!(
        &config.role,
        ClientRole::Primary { agent, .. } if agent.delivery_mode == DeliveryMode::Pull
    )
}

/// Connects one owned provider to the router until shutdown or a terminal router error.
///
/// A configured workspace is joined exactly once. Without one, no delivery can start a provider
/// turn. Only `Delivery` starts work; durable workspace/task/
/// integration events are acknowledged or ignored without being copied into a model prompt.
pub async fn run_owned_provider<P: OwnedProvider>(
    provider: P,
    config: ClientConfig,
    workspace: Option<WorkspaceName>,
    shutdown: CancellationToken,
) -> Result<(), HostError> {
    let explicit_readiness = uses_explicit_readiness(&config);
    let provider = Arc::new(provider);
    let (client, events) = match RouterClient::connect(config).await {
        Ok(connected) => connected,
        Err(error) => {
            let _ = provider.close().await;
            return Err(error.into());
        }
    };
    let managed = match prepare_managed(
        &client,
        workspace.as_ref(),
        SharedProvider(Arc::clone(&provider)),
        explicit_readiness,
    )
    .await
    {
        Ok(managed) => managed,
        Err(error) => {
            let _ = provider.close().await;
            let _ = client.close().await;
            return Err(error);
        }
    };
    let (turns_tx, turns_rx) = mpsc::channel(1);
    let (run_result, active) = run_host_loop(
        &client,
        events,
        managed.as_ref(),
        workspace.as_ref(),
        shutdown,
        turns_tx,
        turns_rx,
    )
    .await;
    let close_result = close_host(&client, &provider, active, explicit_readiness).await;
    match run_result {
        Err(error) => Err(error),
        Ok(()) => close_result,
    }
}

async fn prepare_managed<P: OwnedProvider>(
    client: &RouterClient,
    workspace: Option<&WorkspaceName>,
    provider: SharedProvider<P>,
    explicit_readiness: bool,
) -> Result<Option<Managed<P>>, HostError> {
    let Some(workspace) = workspace else {
        if explicit_readiness {
            client.set_ready(false).await?;
        }
        return Ok(None);
    };
    let (joined, _) = client.workspace_join(workspace.clone()).await?;
    if &joined != workspace {
        return Err(ClientError::Router(RouterErrorCode::WorkspaceMismatch).into());
    }
    let lifecycle = RouterClientLifecycle::new_with_explicit_readiness(
        client.clone(),
        joined,
        explicit_readiness,
    );
    let managed = Arc::new(RouterManagedProvider::new(provider, lifecycle));
    managed.activate().await?;
    Ok(Some(managed))
}

#[allow(clippy::too_many_arguments)]
async fn run_host_loop<P: OwnedProvider>(
    client: &RouterClient,
    mut events: ClientEvents,
    managed: Option<&Managed<P>>,
    workspace: Option<&WorkspaceName>,
    shutdown: CancellationToken,
    turns_tx: mpsc::Sender<TurnOutcome>,
    mut turns_rx: mpsc::Receiver<TurnOutcome>,
) -> (Result<(), HostError>, Option<ActiveTurn>) {
    let mut active = None;
    let result = loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break Ok(()),
            event = events.recv() => {
                let Some(event) = event else {
                    break Err(HostError::RouterClosed(RouterErrorCode::ProviderDisconnected));
                };
                if let Some(result) = handle_client_event(
                    client,
                    managed,
                    workspace,
                    &mut active,
                    &turns_tx,
                    event.event,
                ).await {
                    break result;
                }
            }
            outcome = turns_rx.recv(), if active.is_some() => {
                let Some(outcome) = outcome else {
                    break Err(HostError::ProviderTask);
                };
                if let Err(error) = finish_turn(client, &mut active, outcome).await {
                    break Err(error);
                }
            }
        }
    };
    (result, active)
}

async fn handle_client_event<P: OwnedProvider>(
    client: &RouterClient,
    managed: Option<&Managed<P>>,
    workspace: Option<&WorkspaceName>,
    active: &mut Option<ActiveTurn>,
    turns: &mpsc::Sender<TurnOutcome>,
    event: ClientEvent,
) -> Option<Result<(), HostError>> {
    let result = match event {
        ClientEvent::Delivery {
            workspace: delivered_workspace,
            request_id,
            from,
            content,
            timeout_ms,
            task,
        } => {
            receive_delivery(
                client,
                managed,
                workspace,
                active,
                turns,
                Delivery {
                    workspace: delivered_workspace,
                    request_id,
                    from,
                    content,
                    timeout_ms,
                    task,
                },
            )
            .await
        }
        ClientEvent::WorkCancelled {
            workspace,
            request_id,
            reason,
            task,
        } => {
            cancel_delivery(
                managed,
                active,
                &workspace,
                &request_id,
                task.as_ref(),
                reason,
            )
            .await
        }
        ClientEvent::WorkspaceEvent(event) => {
            if workspace == Some(&event.workspace) {
                client
                    .ack_event(event.workspace, event.seq)
                    .map_err(HostError::from)
            } else {
                Ok(())
            }
        }
        ClientEvent::Closed(code) => return Some(Err(HostError::RouterClosed(code))),
        ClientEvent::MembershipChanged { .. }
        | ClientEvent::TaskAttemptChanged { .. }
        | ClientEvent::SendResult(_) => Ok(()),
    };
    result.err().map(Err)
}

struct Delivery {
    workspace: WorkspaceName,
    request_id: String,
    from: String,
    content: String,
    timeout_ms: u64,
    task: Option<TaskDispatch>,
}

async fn receive_delivery<P: OwnedProvider>(
    client: &RouterClient,
    managed: Option<&Managed<P>>,
    workspace: Option<&WorkspaceName>,
    active: &mut Option<ActiveTurn>,
    turns: &mpsc::Sender<TurnOutcome>,
    delivery: Delivery,
) -> Result<(), HostError> {
    let Some(managed) = managed else {
        return reply_error(
            client,
            delivery.request_id,
            RouterErrorCode::WorkspaceRequired,
        )
        .await;
    };
    if workspace != Some(&delivery.workspace) {
        return reply_error(
            client,
            delivery.request_id,
            RouterErrorCode::WorkspaceMismatch,
        )
        .await;
    }
    if active.is_some() {
        return reply_error(client, delivery.request_id, RouterErrorCode::SessionBusy).await;
    }
    let request_id = delivery.request_id.clone();
    let active_workspace = delivery.workspace.clone();
    let task_id = delivery.task.as_ref().map(|task| task.id);
    let request = SessionRequest {
        request_id: delivery.request_id,
        workspace: Some(delivery.workspace),
        from: delivery.from,
        content: delivery.content,
        deadline: Instant::now()
            + Duration::from_millis(normalize_timeout_ms(Some(delivery.timeout_ms))),
        task: delivery.task,
    };
    let managed = Arc::clone(managed);
    let outcomes = turns.clone();
    let outcome_id = request_id.clone();
    let task = tokio::spawn(async move {
        let result = managed.handle(request).await;
        let _ = outcomes
            .send(TurnOutcome {
                request_id: outcome_id,
                result,
            })
            .await;
    });
    *active = Some(ActiveTurn {
        request_id,
        workspace: active_workspace,
        task_id,
        cancelled: false,
        task,
    });
    Ok(())
}

async fn cancel_delivery<P: OwnedProvider>(
    managed: Option<&Managed<P>>,
    active: &mut Option<ActiveTurn>,
    workspace: &WorkspaceName,
    request_id: &str,
    task: Option<&TaskFence>,
    reason: RouterErrorCode,
) -> Result<(), HostError> {
    let (Some(managed), Some(active)) = (managed, active.as_mut()) else {
        return Ok(());
    };
    if active.workspace != *workspace
        || active.request_id != request_id
        || !matching_cancel_task(active.task_id, task)
    {
        return Ok(());
    }
    active.cancelled = true;
    match managed
        .cancel(workspace, request_id, task, cancel_reason(reason))
        .await
    {
        Ok(()) | Err(ProviderError::ExecutionFenceChanged) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

const fn matching_cancel_task(task_id: Option<i64>, fence: Option<&TaskFence>) -> bool {
    match (task_id, fence) {
        (None, None) => true,
        (Some(task_id), Some(fence)) => task_id == fence.task_id,
        (None, Some(_)) | (Some(_), None) => false,
    }
}

const fn cancel_reason(reason: RouterErrorCode) -> CancelReason {
    match reason {
        RouterErrorCode::RequestTimeout => CancelReason::RequestTimeout,
        RouterErrorCode::TaskInterrupted => CancelReason::TaskInterrupted,
        RouterErrorCode::ProviderDisconnected
        | RouterErrorCode::TargetDisconnected
        | RouterErrorCode::RequesterDisconnected
        | RouterErrorCode::RouterRestarted => CancelReason::ProviderDisconnected,
        _ => CancelReason::RequestCancelled,
    }
}

async fn finish_turn(
    client: &RouterClient,
    active: &mut Option<ActiveTurn>,
    outcome: TurnOutcome,
) -> Result<(), HostError> {
    let Some(current) = active.as_ref() else {
        return Ok(());
    };
    if current.request_id != outcome.request_id {
        return Ok(());
    }
    let current = active.take().expect("active turn checked above");
    current.task.await.map_err(|_| HostError::ProviderTask)?;
    if current.cancelled {
        return Ok(());
    }
    match outcome.result {
        SessionResult::Success { content } => client
            .reply(outcome.request_id, true, Some(content), None)
            .await
            .map_err(HostError::from),
        SessionResult::Failure { error } => {
            reply_error(client, outcome.request_id, provider_router_error(error)).await
        }
    }
}

async fn reply_error(
    client: &RouterClient,
    request_id: String,
    error: RouterErrorCode,
) -> Result<(), HostError> {
    client
        .reply(request_id, false, None, Some(error))
        .await
        .map_err(HostError::from)
}

const fn provider_router_error(error: ProviderError) -> RouterErrorCode {
    match error {
        ProviderError::ProviderNotReady => RouterErrorCode::ProviderNotReady,
        ProviderError::SessionBusy => RouterErrorCode::SessionBusy,
        ProviderError::RequestTimeout => RouterErrorCode::RequestTimeout,
        ProviderError::ProviderDisconnected => RouterErrorCode::ProviderDisconnected,
        ProviderError::CodexInitializeFailed | ProviderError::CodexProtocolError => {
            RouterErrorCode::CodexProtocolError
        }
        ProviderError::CodexTurnInterrupted => RouterErrorCode::CodexTurnInterrupted,
        ProviderError::CodexTurnFailed => RouterErrorCode::CodexTurnFailed,
        ProviderError::CodexNoFinalResponse => RouterErrorCode::CodexNoFinalResponse,
        ProviderError::ClaudeInitializeFailed | ProviderError::ClaudeSdkError => {
            RouterErrorCode::ClaudeSdkError
        }
        ProviderError::BridgeProtocolError | ProviderError::ClaudeProtocolError => {
            RouterErrorCode::ClaudeProtocolError
        }
        ProviderError::ClaudeMaxTurns => RouterErrorCode::ClaudeMaxTurns,
        ProviderError::ClaudeMaxBudget => RouterErrorCode::ClaudeMaxBudget,
        ProviderError::ClaudeStructuredOutputError => RouterErrorCode::ClaudeStructuredOutputError,
        ProviderError::ClaudeExecutionError => RouterErrorCode::ClaudeExecutionError,
        ProviderError::ClaudeNoResult => RouterErrorCode::ClaudeNoResult,
        ProviderError::ExecutionFenceChanged => RouterErrorCode::TaskStaleAttempt,
        ProviderError::InvalidLaunchContext
        | ProviderError::RouterLifecycleUnavailable
        | ProviderError::LaunchFailed => RouterErrorCode::ProviderError,
    }
}

async fn close_host<P: OwnedProvider>(
    client: &RouterClient,
    provider: &Arc<P>,
    mut active: Option<ActiveTurn>,
    explicit_readiness: bool,
) -> Result<(), HostError> {
    let readiness = if explicit_readiness {
        client.set_ready(false).await
    } else {
        Ok(())
    };
    if let Some(turn) = active.as_mut() {
        turn.cancelled = true;
        let _ = provider
            .cancel(turn.request_id.clone(), CancelReason::ProviderDisconnected)
            .await;
    }
    let turn_close = if let Some(turn) = active {
        match tokio::time::timeout(HOST_CLOSE_TIMEOUT, turn.task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(HostError::ProviderTask),
            Err(_) => Err(HostError::CloseTimeout),
        }
    } else {
        Ok(())
    };
    let provider_close = provider.close().await;
    let client_close = client.close().await;
    provider_close?;
    turn_close?;
    if !matches!(
        readiness,
        Ok(()) | Err(ClientError::Closed | ClientError::Disconnected)
    ) {
        readiness?;
    }
    if !matches!(
        client_close,
        Ok(()) | Err(ClientError::Closed | ClientError::Disconnected)
    ) {
        client_close?;
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct McpChildSelection {
    pub profile: Option<String>,
    pub credential_file: Option<PathBuf>,
}

impl McpChildSelection {
    fn command_arguments(&self, role: &str) -> Vec<OsString> {
        let mut arguments = Vec::new();
        if let Some(profile) = &self.profile {
            arguments.push(OsString::from("--profile"));
            arguments.push(OsString::from(profile));
        }
        if let Some(credential) = &self.credential_file {
            arguments.push(OsString::from("--credential"));
            arguments.push(credential.as_os_str().to_owned());
        }
        arguments.push(OsString::from("mcp"));
        arguments.push(OsString::from(role));
        arguments
    }
}

#[derive(Clone, Debug)]
pub struct ManagedProviderOptions {
    pub asr_executable: PathBuf,
    pub caller_cwd: PathBuf,
    pub source_environment: Vec<(OsString, OsString)>,
}

pub struct ManagedCodexOptions {
    pub common: ManagedProviderOptions,
    pub executable: PathBuf,
    pub executable_arguments: Vec<OsString>,
    pub thread: ThreadSelection,
}

pub struct ManagedClaudeOptions {
    pub common: ManagedProviderOptions,
    pub node_executable: PathBuf,
    pub node_arguments: Vec<OsString>,
    pub claude_executable: Option<PathBuf>,
    pub resume_session_id: Option<String>,
}

pub fn managed_codex_config(
    client: &ClientConfig,
    options: ManagedCodexOptions,
) -> Result<CodexConfig, HostError> {
    let launch = managed_launch(client, options.common)?;
    let enabled_tools = catalog(McpRole::Delegate)
        .into_iter()
        .map(|tool| tool.name.into_owned())
        .collect();
    let mut config =
        CodexConfig::managed(options.executable, launch, options.thread, enabled_tools);
    config.executable_arguments = options.executable_arguments;
    Ok(config)
}

pub fn managed_claude_config(
    client: &ClientConfig,
    options: ManagedClaudeOptions,
) -> Result<ClaudeConfig, HostError> {
    let launch = managed_launch(client, options.common)?;
    let mut config = ClaudeConfig::managed(options.node_executable, launch)?;
    config.node_arguments = options.node_arguments;
    config.claude_executable = options.claude_executable;
    config.resume_session_id = options.resume_session_id;
    Ok(config)
}

pub async fn launch_managed_codex(
    client: &ClientConfig,
    options: ManagedCodexOptions,
) -> Result<CodexProvider, HostError> {
    CodexProvider::launch(managed_codex_config(client, options)?)
        .await
        .map_err(HostError::from)
}

pub async fn launch_managed_claude(
    client: &ClientConfig,
    options: ManagedClaudeOptions,
) -> Result<ClaudeProvider, HostError> {
    ClaudeProvider::launch(managed_claude_config(client, options)?)
        .await
        .map_err(HostError::from)
}

pub async fn run_managed_codex(
    client: ClientConfig,
    workspace: Option<WorkspaceName>,
    options: ManagedCodexOptions,
    shutdown: CancellationToken,
) -> Result<(), HostError> {
    let provider = launch_managed_codex(&client, options).await?;
    run_owned_provider(provider, client, workspace, shutdown).await
}

pub async fn run_managed_claude(
    client: ClientConfig,
    workspace: Option<WorkspaceName>,
    options: ManagedClaudeOptions,
    shutdown: CancellationToken,
) -> Result<(), HostError> {
    let provider = launch_managed_claude(&client, options).await?;
    run_owned_provider(provider, client, workspace, shutdown).await
}

struct InteractiveLocalTurn {
    request_id: String,
    task: JoinHandle<()>,
}

struct InteractiveLocalOutcome {
    request_id: String,
    result: Result<SessionResult, ProviderError>,
}

enum SlashEffect {
    Output(String),
    PendingSend(String),
}

struct InteractiveSession<P> {
    provider: Arc<P>,
    client: RouterClient,
    managed: Option<Managed<P>>,
    workspace: Option<WorkspaceName>,
    agent_id: String,
    explicit_readiness: bool,
    pending_send: Option<String>,
}

/// Launches the owned Codex app-server and runs its caller-terminal line interface.
pub async fn run_interactive_codex(
    client: ClientConfig,
    workspace: Option<WorkspaceName>,
    options: ManagedCodexOptions,
    shutdown: CancellationToken,
) -> Result<(), HostError> {
    run_interactive_codex_io(
        client,
        workspace,
        options,
        tokio::io::stdin(),
        tokio::io::stdout(),
        shutdown,
    )
    .await
}

/// The I/O-parametric production path used by terminal adapters and focused integration tests.
pub async fn run_interactive_codex_io<R, W>(
    client: ClientConfig,
    workspace: Option<WorkspaceName>,
    options: ManagedCodexOptions,
    input: R,
    output: W,
    shutdown: CancellationToken,
) -> Result<(), HostError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let agent_id = interactive_codex_agent_id(&client)?;
    let provider = launch_managed_codex(&client, options).await?;
    run_interactive_provider(
        provider, client, workspace, agent_id, input, output, shutdown,
    )
    .await
}

fn interactive_codex_agent_id(client: &ClientConfig) -> Result<String, HostError> {
    let ClientRole::Primary {
        agent,
        credential,
        delegation_token: Some(_),
    } = &client.role
    else {
        return Err(HostError::InteractiveCodexClaims);
    };
    if agent.side != AgentSide::Codex
        || agent.client != AgentClient::CodexAppServer
        || agent.delivery_mode != DeliveryMode::Push
        || credential.role != CredentialRole::Agent
        || credential.subject != agent.agent_id
        || credential.agent_side != Some(agent.side)
        || credential.agent_client != Some(agent.client)
    {
        return Err(HostError::InteractiveCodexClaims);
    }
    Ok(agent.agent_id.clone())
}

async fn run_interactive_provider<P, R, W>(
    provider: P,
    config: ClientConfig,
    workspace: Option<WorkspaceName>,
    agent_id: String,
    input: R,
    output: W,
    shutdown: CancellationToken,
) -> Result<(), HostError>
where
    P: OwnedProvider,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let explicit_readiness = uses_explicit_readiness(&config);
    let provider = Arc::new(provider);
    let (client, events) = match RouterClient::connect(config).await {
        Ok(connected) => connected,
        Err(error) => {
            let _ = provider.close().await;
            return Err(error.into());
        }
    };
    let managed = match prepare_managed(
        &client,
        workspace.as_ref(),
        SharedProvider(Arc::clone(&provider)),
        explicit_readiness,
    )
    .await
    {
        Ok(managed) => managed,
        Err(error) => {
            let _ = provider.close().await;
            let _ = client.close().await;
            return Err(error);
        }
    };
    let mut session = InteractiveSession {
        provider,
        client,
        managed,
        workspace,
        agent_id,
        explicit_readiness,
        pending_send: None,
    };
    let (run_result, remote, local) =
        run_interactive_loop(&mut session, events, input, output, shutdown).await;
    let close_result = close_interactive_session(&session, remote, local).await;
    match run_result {
        Err(error) => Err(error),
        Ok(()) => close_result,
    }
}

async fn run_interactive_loop<P, R, W>(
    session: &mut InteractiveSession<P>,
    mut events: ClientEvents,
    input: R,
    mut output: W,
    shutdown: CancellationToken,
) -> (
    Result<(), HostError>,
    Option<ActiveTurn>,
    Option<InteractiveLocalTurn>,
)
where
    P: OwnedProvider,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = FramedRead::new(
        input,
        LinesCodec::new_with_max_length(INTERACTIVE_LINE_BYTES),
    );
    let (remote_tx, mut remote_rx) = mpsc::channel(1);
    let (local_tx, mut local_rx) = mpsc::channel(1);
    let mut remote = None;
    let mut local = None;
    let result = loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break Ok(()),
            event = events.recv() => {
                let Some(event) = event else {
                    break Err(HostError::RouterClosed(RouterErrorCode::ProviderDisconnected));
                };
                let event = event.event;
                if let ClientEvent::SendResult(result) = &event
                    && session.pending_send.as_deref() == Some(&result.request_id)
                {
                    session.pending_send = None;
                    if let Err(error) = write_terminal_line(&mut output, &render_send_result(result)).await {
                        break Err(error);
                    }
                    continue;
                }
                if local.is_some()
                    && let ClientEvent::Delivery { request_id, .. } = event
                {
                    if let Err(error) =
                        reply_error(&session.client, request_id, RouterErrorCode::SessionBusy).await
                    {
                        break Err(error);
                    }
                    continue;
                }
                if let Some(result) = handle_client_event(
                    &session.client,
                    session.managed.as_ref(),
                    session.workspace.as_ref(),
                    &mut remote,
                    &remote_tx,
                    event,
                ).await {
                    break result;
                }
            }
            outcome = remote_rx.recv(), if remote.is_some() => {
                let Some(outcome) = outcome else {
                    break Err(HostError::ProviderTask);
                };
                if let Err(error) = finish_turn(&session.client, &mut remote, outcome).await {
                    break Err(error);
                }
            }
            outcome = local_rx.recv(), if local.is_some() => {
                let Some(outcome) = outcome else {
                    break Err(HostError::ProviderTask);
                };
                if let Err(error) =
                    finish_local_turn(session, &mut local, outcome, &mut output).await
                {
                    break Err(error);
                }
            }
            line = lines.next() => {
                let line = match line {
                    Some(Ok(line)) => line,
                    Some(Err(_)) => break Err(HostError::InteractiveIo),
                    None => break Ok(()),
                };
                if is_quit_line(&line) {
                    break Ok(());
                }
                let busy = remote.is_some() || local.is_some() || session.pending_send.is_some();
                if line.starts_with('/') {
                    if busy {
                        if let Err(error) = write_terminal_error(&mut output, "session_busy").await {
                            break Err(error);
                        }
                        continue;
                    }
                    match session.execute_slash(&line).await {
                        Ok(SlashEffect::Output(value)) => {
                            if let Err(error) = write_terminal_line(&mut output, &value).await {
                                break Err(error);
                            }
                        }
                        Ok(SlashEffect::PendingSend(request_id)) => {
                            session.pending_send = Some(request_id);
                        }
                        Err(error) => {
                            if let Err(error) = write_terminal_error(&mut output, &error).await {
                                break Err(error);
                            }
                        }
                    }
                    continue;
                }
                if line.trim().is_empty() {
                    continue;
                }
                if busy {
                    if let Err(error) = write_terminal_error(&mut output, "session_busy").await {
                        break Err(error);
                    }
                    continue;
                }
                if let Err(error) = session.set_readiness(false).await {
                    break Err(error.into());
                }
                local = Some(start_local_turn(session, line, local_tx.clone()));
            }
        }
    };
    (result, remote, local)
}

fn is_quit_line(line: &str) -> bool {
    matches!(line.trim(), "quit" | "/quit" | "/exit")
}

fn start_local_turn<P: OwnedProvider>(
    session: &InteractiveSession<P>,
    content: String,
    outcomes: mpsc::Sender<InteractiveLocalOutcome>,
) -> InteractiveLocalTurn {
    let request_id = format!("local:{}", uuid::Uuid::new_v4());
    let outcome_id = request_id.clone();
    let provider = Arc::clone(&session.provider);
    let request = SessionRequest {
        request_id: request_id.clone(),
        workspace: None,
        from: session.agent_id.clone(),
        content,
        deadline: Instant::now() + INTERACTIVE_TURN_TIMEOUT,
        task: None,
    };
    let task = tokio::spawn(async move {
        let mut terminal = provider.subscribe_terminal();
        let result = provider.handle(request).await;
        let result = wait_for_local_terminal(&mut terminal, &outcome_id)
            .await
            .map(|_| result);
        let _ = outcomes
            .send(InteractiveLocalOutcome {
                request_id: outcome_id,
                result,
            })
            .await;
    });
    InteractiveLocalTurn { request_id, task }
}

async fn wait_for_local_terminal(
    terminal: &mut broadcast::Receiver<TerminalEvidence>,
    request_id: &str,
) -> Result<TerminalEvidence, ProviderError> {
    tokio::time::timeout(HOST_CLOSE_TIMEOUT, async {
        loop {
            match terminal.recv().await {
                Ok(evidence) if evidence.request_id == request_id => return Ok(evidence),
                Ok(_) => {}
                Err(
                    broadcast::error::RecvError::Lagged(_) | broadcast::error::RecvError::Closed,
                ) => return Err(ProviderError::RouterLifecycleUnavailable),
            }
        }
    })
    .await
    .map_err(|_| ProviderError::RouterLifecycleUnavailable)?
}

async fn finish_local_turn<P, W>(
    session: &InteractiveSession<P>,
    active: &mut Option<InteractiveLocalTurn>,
    outcome: InteractiveLocalOutcome,
    output: &mut W,
) -> Result<(), HostError>
where
    P: OwnedProvider,
    W: AsyncWrite + Unpin,
{
    let current = active.take().ok_or(HostError::ProviderTask)?;
    if current.request_id != outcome.request_id {
        return Err(HostError::ProviderTask);
    }
    current.task.await.map_err(|_| HostError::ProviderTask)?;
    let result = outcome.result?;
    session.restore_readiness().await?;
    match result {
        SessionResult::Success { content } => write_terminal_line(output, &content).await,
        SessionResult::Failure { error } => write_terminal_error(output, &error.to_string()).await,
    }
}

impl<P: OwnedProvider> InteractiveSession<P> {
    async fn restore_readiness(&self) -> Result<(), HostError> {
        let ready = self.managed.as_ref().is_some_and(|managed| managed.ready());
        self.set_readiness(ready).await.map_err(HostError::from)
    }

    async fn set_readiness(&self, ready: bool) -> Result<(), ClientError> {
        if self.explicit_readiness {
            self.client.set_ready(ready).await
        } else {
            Ok(())
        }
    }

    async fn execute_slash(&mut self, line: &str) -> Result<SlashEffect, String> {
        let (command, rest) = take_word(line).ok_or_else(|| "unknown_command".to_owned())?;
        match command {
            "/agents" => {
                require_no_arguments(rest)?;
                self.agent_list().await
            }
            "/send" => self.agent_send(rest).await,
            "/workspace" => self.workspace_command(rest).await,
            _ => Err("unknown_command".to_owned()),
        }
    }

    async fn agent_list(&self) -> Result<SlashEffect, String> {
        let response = self
            .client
            .call(ClientMessage::List {
                request_id: new_host_request_id(),
            })
            .await
            .map_err(|error| slash_client_error(&error))?;
        let ServerMessage::Agents { mut agents, .. } = response else {
            return Err(slash_server_error(&response));
        };
        agents.retain(|agent| agent.agent_id != self.agent_id);
        slash_json(serde_json::json!({ "agents": agents }))
    }

    async fn agent_send(&self, rest: &str) -> Result<SlashEffect, String> {
        let (target, content) =
            take_word(rest).ok_or_else(|| "usage: /send TARGET TEXT".to_owned())?;
        if !is_agent_id(target) || content.trim().is_empty() {
            return Err("usage: /send TARGET TEXT".to_owned());
        }
        let request_id = new_host_request_id();
        let response = self
            .client
            .call(ClientMessage::Send {
                request_id: request_id.clone(),
                to: target.to_owned(),
                content: content.to_owned(),
                timeout_ms: None,
            })
            .await
            .map_err(|error| slash_client_error(&error))?;
        if !matches!(response, ServerMessage::Accepted { .. }) {
            return Err(slash_server_error(&response));
        }
        Ok(SlashEffect::PendingSend(request_id))
    }

    async fn workspace_command(&mut self, rest: &str) -> Result<SlashEffect, String> {
        let (command, arguments) =
            take_word(rest).ok_or_else(|| "usage: /workspace COMMAND".to_owned())?;
        match command {
            "join" => self.workspace_join(arguments).await,
            "leave" => {
                require_no_arguments(arguments)?;
                self.workspace_leave().await
            }
            "list" => {
                require_no_arguments(arguments)?;
                self.workspace_list().await
            }
            "members" => {
                require_no_arguments(arguments)?;
                self.workspace_members().await
            }
            "history" => {
                require_no_arguments(arguments)?;
                self.workspace_history().await
            }
            "post" => self.workspace_post(arguments).await,
            _ => Err("usage: /workspace join|leave|list|members|history|post".to_owned()),
        }
    }

    async fn workspace_join(&mut self, arguments: &str) -> Result<SlashEffect, String> {
        let (name, extra) =
            take_word(arguments).ok_or_else(|| "usage: /workspace join NAME".to_owned())?;
        require_no_arguments(extra)?;
        let requested = WorkspaceName::parse(name.to_owned()).map_err(|error| error.to_string())?;
        self.set_readiness(false)
            .await
            .map_err(|error| slash_client_error(&error))?;
        let joined = self.client.workspace_join(requested.clone()).await;
        let (joined, cursor) = match joined {
            Ok(joined) => joined,
            Err(error) => {
                let _ = self.restore_readiness().await;
                return Err(slash_client_error(&error));
            }
        };
        if joined != requested {
            return Err(RouterErrorCode::WorkspaceMismatch.to_string());
        }
        let managed = Arc::new(RouterManagedProvider::new(
            SharedProvider(Arc::clone(&self.provider)),
            RouterClientLifecycle::new_with_explicit_readiness(
                self.client.clone(),
                joined.clone(),
                self.explicit_readiness,
            ),
        ));
        managed
            .activate()
            .await
            .map_err(|error| error.to_string())?;
        self.workspace = Some(joined.clone());
        self.managed = Some(managed);
        slash_json(serde_json::json!({ "workspace": joined, "cursor": cursor }))
    }

    async fn workspace_leave(&mut self) -> Result<SlashEffect, String> {
        self.set_readiness(false)
            .await
            .map_err(|error| slash_client_error(&error))?;
        let left = match self.client.workspace_leave().await {
            Ok(left) => left,
            Err(error) => {
                let _ = self.restore_readiness().await;
                return Err(slash_client_error(&error));
            }
        };
        self.managed = None;
        self.workspace = None;
        slash_json(serde_json::json!({ "workspace": left }))
    }

    async fn workspace_list(&self) -> Result<SlashEffect, String> {
        let response = self
            .client
            .call(ClientMessage::WorkspaceList {
                request_id: new_host_request_id(),
                after: None,
                limit: None,
            })
            .await
            .map_err(|error| slash_client_error(&error))?;
        let ServerMessage::Workspaces {
            workspaces,
            next_cursor,
            has_more,
            ..
        } = response
        else {
            return Err(slash_server_error(&response));
        };
        slash_json(serde_json::json!({
            "workspaces": workspaces,
            "nextCursor": next_cursor,
            "hasMore": has_more,
        }))
    }

    async fn workspace_members(&self) -> Result<SlashEffect, String> {
        let response = self
            .client
            .call(ClientMessage::WorkspaceMembers {
                request_id: new_host_request_id(),
            })
            .await
            .map_err(|error| slash_client_error(&error))?;
        let ServerMessage::Agents { agents, .. } = response else {
            return Err(slash_server_error(&response));
        };
        slash_json(serde_json::json!({ "agents": agents }))
    }

    async fn workspace_history(&self) -> Result<SlashEffect, String> {
        let page = self
            .client
            .workspace_history(None, None)
            .await
            .map_err(|error| slash_client_error(&error))?;
        slash_json(page)
    }

    async fn workspace_post(&self, content: &str) -> Result<SlashEffect, String> {
        if content.trim().is_empty() {
            return Err("usage: /workspace post TEXT".to_owned());
        }
        let response = self
            .client
            .call(ClientMessage::WorkspacePost {
                request_id: new_host_request_id(),
                content: content.to_owned(),
            })
            .await
            .map_err(|error| slash_client_error(&error))?;
        let ServerMessage::WorkspacePosted { workspace, seq, .. } = response else {
            return Err(slash_server_error(&response));
        };
        slash_json(serde_json::json!({ "workspace": workspace, "seq": seq }))
    }
}

fn take_word(value: &str) -> Option<(&str, &str)> {
    let value = value.trim_start();
    if value.is_empty() {
        return None;
    }
    let end = value.find(char::is_whitespace).unwrap_or(value.len());
    Some((&value[..end], value[end..].trim_start()))
}

fn require_no_arguments(value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Ok(())
    } else {
        Err("unexpected_arguments".to_owned())
    }
}

fn slash_client_error(error: &ClientError) -> String {
    match error {
        ClientError::Router(code) => code.to_string(),
        ClientError::Disconnected => "gateway_disconnected".to_owned(),
        ClientError::MessageTooLarge => "message_too_large".to_owned(),
        ClientError::QueueFull => "client_queue_full".to_owned(),
        ClientError::Closed => "client_closed".to_owned(),
        ClientError::Transport => "transport_error".to_owned(),
    }
}

fn slash_server_error(message: &ServerMessage) -> String {
    match message {
        ServerMessage::Error { code, .. } => code.to_string(),
        _ => "router_protocol_error".to_owned(),
    }
}

fn slash_json(value: impl serde::Serialize) -> Result<SlashEffect, String> {
    serde_json::to_string(&value)
        .map(SlashEffect::Output)
        .map_err(|_| "output_error".to_owned())
}

fn new_host_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn render_send_result(result: &AgentSendResult) -> String {
    serde_json::json!({
        "requestId": result.request_id,
        "workspace": result.workspace,
        "from": result.from,
        "ok": result.ok,
        "content": result.content,
        "error": result.error,
    })
    .to_string()
}

async fn write_terminal_error<W: AsyncWrite + Unpin>(
    output: &mut W,
    error: &str,
) -> Result<(), HostError> {
    write_terminal_line(output, &format!("error: {error}")).await
}

async fn write_terminal_line<W: AsyncWrite + Unpin>(
    output: &mut W,
    value: &str,
) -> Result<(), HostError> {
    output
        .write_all(escape_terminal(value).as_bytes())
        .await
        .map_err(|_| HostError::InteractiveIo)?;
    output
        .write_all(b"\n")
        .await
        .map_err(|_| HostError::InteractiveIo)?;
    output.flush().await.map_err(|_| HostError::InteractiveIo)
}

async fn close_interactive_session<P: OwnedProvider>(
    session: &InteractiveSession<P>,
    remote: Option<ActiveTurn>,
    local: Option<InteractiveLocalTurn>,
) -> Result<(), HostError> {
    let readiness = session.set_readiness(false).await;
    if let Some(turn) = remote.as_ref() {
        let _ = session
            .provider
            .cancel(turn.request_id.clone(), CancelReason::ProviderDisconnected)
            .await;
    }
    if let Some(turn) = local.as_ref() {
        let _ = session
            .provider
            .cancel(turn.request_id.clone(), CancelReason::ProviderDisconnected)
            .await;
    }
    let remote_close = wait_interactive_task(remote.map(|turn| turn.task)).await;
    let local_close = wait_interactive_task(local.map(|turn| turn.task)).await;
    let provider_close = session.provider.close().await;
    let client_close = session.client.close().await;
    provider_close?;
    remote_close?;
    local_close?;
    if !matches!(
        readiness,
        Ok(()) | Err(ClientError::Closed | ClientError::Disconnected)
    ) {
        readiness?;
    }
    if !matches!(
        client_close,
        Ok(()) | Err(ClientError::Closed | ClientError::Disconnected)
    ) {
        client_close?;
    }
    Ok(())
}

async fn wait_interactive_task(task: Option<JoinHandle<()>>) -> Result<(), HostError> {
    let Some(task) = task else {
        return Ok(());
    };
    match tokio::time::timeout(HOST_CLOSE_TIMEOUT, task).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(HostError::ProviderTask),
        Err(_) => Err(HostError::CloseTimeout),
    }
}

fn managed_launch(
    client: &ClientConfig,
    options: ManagedProviderOptions,
) -> Result<ManagedLaunch, HostError> {
    let context = delegate_context(client)?;
    ManagedLaunch::new(
        options.asr_executable,
        options.caller_cwd,
        &context,
        options.source_environment,
    )
    .map_err(HostError::from)
}

fn delegate_context(client: &ClientConfig) -> Result<DelegateLaunchContext, HostError> {
    let ClientRole::Primary {
        agent,
        delegation_token: Some(delegation_token),
        ..
    } = &client.role
    else {
        return Err(HostError::DelegationRequired);
    };
    let ca_file = client
        .ca_file
        .as_ref()
        .map(|path| {
            if !path.is_absolute() {
                return Err(HostError::InvalidCaFile);
            }
            path.to_str()
                .map(str::to_owned)
                .ok_or(HostError::InvalidCaFile)
        })
        .transpose()?;
    Ok(DelegateLaunchContext {
        version: DELEGATE_CONTEXT_VERSION,
        router_url: client.router_url.to_string(),
        owner_id: agent.agent_id.clone(),
        delegation_token: delegation_token.expose().to_owned(),
        ca_file,
    })
}

pub fn stock_codex_plan(
    program: OsString,
    caller_cwd: PathBuf,
    current_executable: &Path,
    selection: &McpChildSelection,
    workspace: Option<&WorkspaceName>,
    forwarded_arguments: Vec<OsString>,
) -> Result<LaunchPlan, HostError> {
    let executable = absolute_utf8_executable(current_executable)?;
    let command =
        serde_json::to_string(executable).map_err(|_| HostError::InvalidCurrentExecutable)?;
    let child_arguments = selection.command_arguments(McpRoleArg::CodexCli.command_name());
    let arguments = child_arguments
        .iter()
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .ok_or(HostError::InvalidCurrentExecutable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let arguments =
        serde_json::to_string(&arguments).map_err(|_| HostError::InvalidCurrentExecutable)?;
    let enabled_tools = serde_json::to_string(
        &catalog(McpRole::CodexCli)
            .into_iter()
            .map(|tool| tool.name.into_owned())
            .collect::<Vec<_>>(),
    )
    .map_err(|_| HostError::InvalidCurrentExecutable)?;
    let overrides = [
        config_override("command", command),
        config_override("args", arguments),
        config_override("required", "true"),
        config_override("enabled_tools", enabled_tools),
        config_override("default_tools_approval_mode", "\"approve\""),
        config_override("startup_timeout_sec", "10"),
        config_override("tool_timeout_sec", "605"),
    ];
    let mut arguments = Vec::with_capacity(overrides.len() * 2 + forwarded_arguments.len());
    for override_value in overrides {
        arguments.push(OsString::from("-c"));
        arguments.push(override_value);
    }
    arguments.extend(forwarded_arguments);
    let mut plan = LaunchPlan::stock_codex(program, caller_cwd, arguments);
    if let Some(workspace) = workspace {
        plan = plan.with_environment("ASR_WORKSPACE", workspace.as_str());
    }
    Ok(plan)
}

fn config_override(key: &str, value: impl AsRef<str>) -> OsString {
    OsString::from(format!("{CODEX_MCP_PREFIX}.{key}={}", value.as_ref()))
}

#[derive(Deserialize)]
struct OmpPluginList {
    #[serde(default)]
    npm: Vec<OmpPlugin>,
}

#[derive(Deserialize)]
struct OmpPlugin {
    name: String,
    path: PathBuf,
    enabled: bool,
}

/// Verifies that the packaged OMP plugin is linked and enabled before launching OMP.
pub async fn preflight_omp_plugin(
    program: &OsStr,
    caller_cwd: &Path,
    current_executable: &Path,
    source_environment: &[(OsString, OsString)],
) -> Result<(), HostError> {
    absolute_utf8_executable(current_executable)?;
    let manifest = resolve_integration_asset(
        source_environment,
        current_executable,
        Path::new(OMP_PACKAGE),
    )?;
    let expected = std::fs::canonicalize(manifest.parent().ok_or(AssetError::InvalidPath)?)
        .map_err(AssetError::from)?;
    let bytes = run_omp_plugin_list(program, caller_cwd).await?;
    let listed: OmpPluginList =
        serde_json::from_slice(&bytes).map_err(|_| HostError::OmpPreflightFailed)?;
    let Some(plugin) = listed
        .npm
        .into_iter()
        .find(|plugin| plugin.name == OMP_PACKAGE_NAME)
    else {
        return Err(HostError::OmpSetupRequired);
    };
    let installed = std::fs::canonicalize(plugin.path).map_err(|_| HostError::OmpSetupRequired)?;
    if installed != expected {
        return Err(HostError::OmpSetupRequired);
    }
    if !plugin.enabled {
        return Err(HostError::OmpEnableRequired);
    }
    Ok(())
}

/// Inspects the official OMP plugin registry before planning a setup link.
///
/// `None` means the exact packaged integration is already linked and enabled. A returned plan is
/// safe to execute once. Conflicting or disabled installs fail without producing a link plan.
pub async fn setup_omp_checked_plan(
    program: OsString,
    caller_cwd: PathBuf,
    current_executable: &Path,
    source_environment: &[(OsString, OsString)],
) -> Result<Option<LaunchPlan>, HostError> {
    absolute_utf8_executable(current_executable)?;
    let manifest = resolve_integration_asset(
        source_environment,
        current_executable,
        Path::new(OMP_PACKAGE),
    )?;
    let expected = std::fs::canonicalize(manifest.parent().ok_or(AssetError::InvalidPath)?)
        .map_err(AssetError::from)?;
    let bytes = run_omp_plugin_list(&program, &caller_cwd).await?;
    let listed: OmpPluginList =
        serde_json::from_slice(&bytes).map_err(|_| HostError::OmpPreflightFailed)?;
    let mut exact = None;
    for plugin in listed.npm.into_iter().filter(|plugin| {
        matches!(
            plugin.name.as_str(),
            OMP_PACKAGE_NAME | "agent-session-router"
        )
    }) {
        let installed =
            std::fs::canonicalize(&plugin.path).map_err(|_| HostError::OmpSetupConflict)?;
        if installed != expected {
            return Err(HostError::OmpSetupConflict);
        }
        exact = Some(plugin);
    }
    match exact {
        Some(plugin) if !plugin.enabled => Err(HostError::OmpEnableRequired),
        Some(_) => Ok(None),
        None => {
            setup_omp_plan(program, caller_cwd, current_executable, source_environment).map(Some)
        }
    }
}

async fn run_omp_plugin_list(program: &OsStr, caller_cwd: &Path) -> Result<Vec<u8>, HostError> {
    let mut child = Command::new(program)
        .args(["plugin", "list", "--json"])
        .current_dir(caller_cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| HostError::OmpPreflightFailed)?;
    let stdout = child.stdout.take().ok_or(HostError::OmpPreflightFailed)?;
    let output = async {
        let mut bytes = Vec::new();
        let mut bounded =
            stdout.take(u64::try_from(MAX_COMMAND_OUTPUT_BYTES + 1).unwrap_or(u64::MAX));
        let (status, read) = tokio::join!(child.wait(), bounded.read_to_end(&mut bytes));
        status
            .map_err(|_| HostError::OmpPreflightFailed)?
            .success()
            .then_some(())
            .ok_or(HostError::OmpPreflightFailed)?;
        read.map_err(|_| HostError::OmpPreflightFailed)?;
        if bytes.len() > MAX_COMMAND_OUTPUT_BYTES {
            return Err(HostError::OmpPreflightFailed);
        }
        Ok(bytes)
    };
    tokio::time::timeout(OMP_PREFLIGHT_TIMEOUT, output)
        .await
        .map_err(|_| HostError::OmpPreflightFailed)?
}

pub fn stock_omp_plan(
    program: OsString,
    caller_cwd: PathBuf,
    current_executable: PathBuf,
    selection: &McpChildSelection,
    workspace: Option<&WorkspaceName>,
    forwarded_arguments: Vec<OsString>,
) -> Result<LaunchPlan, HostError> {
    absolute_utf8_executable(&current_executable)?;
    let mut plan = LaunchPlan::passthrough(program, caller_cwd, forwarded_arguments)
        .with_environment("ASR_EXECUTABLE", current_executable.into_os_string());
    if let Some(profile) = &selection.profile {
        plan = plan.with_environment("ASR_PROFILE", profile);
    }
    if let Some(credential) = &selection.credential_file {
        plan = plan.with_environment("ASR_CREDENTIAL_FILE", credential.as_os_str());
    }
    if let Some(workspace) = workspace {
        plan = plan.with_environment("ASR_WORKSPACE", workspace.as_str());
    }
    Ok(plan)
}

pub fn setup_omp_plan(
    program: OsString,
    caller_cwd: PathBuf,
    current_executable: &Path,
    source_environment: &[(OsString, OsString)],
) -> Result<LaunchPlan, HostError> {
    absolute_utf8_executable(current_executable)?;
    let manifest = resolve_integration_asset(
        source_environment,
        current_executable,
        Path::new(OMP_PACKAGE),
    )?;
    let package = manifest.parent().ok_or(AssetError::InvalidPath)?;
    Ok(LaunchPlan::passthrough(
        program,
        caller_cwd,
        [
            OsString::from("plugin"),
            OsString::from("link"),
            package.as_os_str().to_owned(),
        ],
    )
    .with_mode(LaunchMode::Wait))
}

#[must_use]
pub fn stock_claude_plan(
    program: OsString,
    caller_cwd: PathBuf,
    selection: &McpChildSelection,
    workspace: Option<&WorkspaceName>,
    auto: bool,
    resume_session_id: Option<String>,
) -> LaunchPlan {
    let mut arguments = Vec::new();
    if auto {
        arguments.push(OsString::from("--permission-mode"));
        arguments.push(OsString::from("auto"));
    }
    if let Some(session_id) = resume_session_id {
        arguments.push(OsString::from("--resume"));
        arguments.push(OsString::from(session_id));
    }
    arguments.push(OsString::from("--dangerously-load-development-channels"));
    arguments.push(OsString::from("server:agent-session-router-channel"));
    let mut plan = LaunchPlan::passthrough(program, caller_cwd, arguments);
    if let Some(profile) = &selection.profile {
        plan = plan.with_environment("ASR_PROFILE", profile);
    }
    if let Some(credential) = &selection.credential_file {
        plan = plan.with_environment("ASR_CREDENTIAL_FILE", credential.as_os_str());
    }
    if let Some(workspace) = workspace {
        plan = plan.with_environment("ASR_WORKSPACE", workspace.as_str());
    }
    plan
}

pub fn setup_claude_plan(
    program: OsString,
    caller_cwd: PathBuf,
    current_executable: PathBuf,
) -> Result<LaunchPlan, HostError> {
    absolute_utf8_executable(&current_executable)?;
    LaunchPlan::setup_claude(program, caller_cwd, current_executable).map_err(HostError::from)
}

fn absolute_utf8_executable(path: &Path) -> Result<&str, HostError> {
    if !path.is_absolute() {
        return Err(HostError::InvalidCurrentExecutable);
    }
    path.to_str().ok_or(HostError::InvalidCurrentExecutable)
}
