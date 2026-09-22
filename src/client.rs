use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio_tungstenite::{
    Connector, connect_async_tls_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use url::Url;
use uuid::Uuid;

use crate::{
    credentials::{CredentialFile, SecretToken},
    integrations::IntegrationPublic,
    protocol::{
        AgentRegistration, ClientMessage, DEFAULT_REQUEST_TIMEOUT_MS, HistoryPage,
        PROTOCOL_VERSION, RouterErrorCode, ServerMessage, TaskExecutionEvidence, TaskFence,
        TaskHistoryPage, WorkspaceEvent, WorkspaceName, parse_server_message,
    },
    tasks::{
        ExternalOperationSummary, ExternalProvider, ExternalPublishKind, ExternalResolution,
        ExternalResolutionOutcome, PauseReason, TaskAttempt, TaskDetail, TaskMutationResult,
        TaskState, TaskSummary,
    },
    tls::load_client_config,
};

pub const CLIENT_COMMAND_CAPACITY: usize = 64;
pub const CLIENT_EVENT_CAPACITY: usize = 64;
pub const CLIENT_BYTE_CAPACITY: usize = 1024 * 1024;
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
pub const RECONNECT_INITIAL: Duration = Duration::from_millis(250);
pub const RECONNECT_MAX: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub enum ClientRole {
    Primary {
        agent: AgentRegistration,
        credential: CredentialFile,
        delegation_token: Option<SecretToken>,
    },
    Delegate {
        owner_id: String,
        delegation_token: SecretToken,
    },
    Operator {
        credential: CredentialFile,
    },
}

#[derive(Clone)]
pub struct ClientConfig {
    pub router_url: Url,
    pub role: ClientRole,
    pub ca_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientConnectionState {
    Connecting,
    Connected { epoch: u64 },
    Reconnecting,
    Closed { reason: Option<RouterErrorCode> },
}

#[derive(Clone)]
pub struct RouterClient {
    sender: mpsc::Sender<ClientCommand>,
    bytes: Arc<Semaphore>,
    membership_operation: Arc<AtomicBool>,
    connection: Arc<RwLock<ConnectionSnapshot>>,
    connection_state: watch::Receiver<ClientConnectionState>,
}

pub struct ClientEvents {
    receiver: mpsc::Receiver<ClientEventItem>,
}

impl ClientEvents {
    pub async fn recv(&mut self) -> Option<ClientEventItem> {
        self.receiver.recv().await
    }
}

pub struct ClientEventItem {
    pub event: ClientEvent,
    _bytes: OwnedSemaphorePermit,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionBarrier {
    pub session_id: Uuid,
    pub current: Option<TaskFence>,
    pub stop_pending: Option<TaskFence>,
}

#[derive(Clone, Debug)]
pub enum ClientEvent {
    MembershipChanged {
        workspace: Option<WorkspaceName>,
        cursor: i64,
    },
    WorkspaceEvent(WorkspaceEvent),
    Delivery {
        workspace: WorkspaceName,
        request_id: String,
        from: String,
        content: String,
        timeout_ms: u64,
        task: Option<crate::protocol::TaskDispatch>,
    },
    WorkCancelled {
        workspace: WorkspaceName,
        request_id: String,
        reason: RouterErrorCode,
        task: Option<TaskFence>,
    },
    Closed(RouterErrorCode),
    TaskAttemptChanged {
        workspace: WorkspaceName,
        task_id: i64,
        attempt: Option<TaskAttempt>,
        closed_attempt_id: Option<Uuid>,
        current: Option<TaskFence>,
        stop_pending: Option<TaskFence>,
    },
    SendResult(AgentSendResult),
}

#[derive(Clone, Debug)]
pub struct AgentSendResult {
    pub request_id: String,
    pub workspace: Option<WorkspaceName>,
    pub from: Option<String>,
    pub ok: bool,
    pub content: Option<String>,
    pub error: Option<RouterErrorCode>,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("{0}")]
    Router(RouterErrorCode),
    #[error("gateway_disconnected")]
    Disconnected,
    #[error("message_too_large")]
    MessageTooLarge,
    #[error("client_queue_full")]
    QueueFull,
    #[error("client_closed")]
    Closed,
    #[error("transport_error")]
    Transport,
}

struct MembershipCallGuard(Arc<AtomicBool>);

impl Drop for MembershipCallGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl RouterClient {
    pub async fn connect(config: ClientConfig) -> Result<(Self, ClientEvents), ClientError> {
        let tls_config = match config.router_url.scheme() {
            "ws" => None,
            "wss" => Some(
                load_client_config(config.ca_file.as_deref())
                    .map_err(|_| ClientError::Router(RouterErrorCode::ConfigurationRequired))?,
            ),
            _ => return Err(ClientError::Router(RouterErrorCode::ConfigurationRequired)),
        };
        let (command_tx, command_rx) = mpsc::channel(CLIENT_COMMAND_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel(CLIENT_EVENT_CAPACITY);
        let bytes = Arc::new(Semaphore::new(CLIENT_BYTE_CAPACITY));
        let event_bytes = Arc::new(Semaphore::new(CLIENT_BYTE_CAPACITY));
        let connection = Arc::new(RwLock::new(ConnectionSnapshot::default()));
        let (state_tx, state_rx) = watch::channel(ClientConnectionState::Connecting);
        let (initial_tx, initial_rx) = oneshot::channel();
        tokio::spawn(connection_task(
            config,
            tls_config,
            command_rx,
            event_tx,
            event_bytes,
            ConnectionMemory::new(connection.clone(), state_tx),
            initial_tx,
        ));
        initial_rx.await.map_err(|_| ClientError::Disconnected)??;
        Ok((
            Self {
                sender: command_tx,
                bytes,
                membership_operation: Arc::new(AtomicBool::new(false)),
                connection,
                connection_state: state_rx,
            },
            ClientEvents { receiver: event_rx },
        ))
    }

    pub async fn call(&self, message: ClientMessage) -> Result<ServerMessage, ClientError> {
        self.call_with_deadline(
            message,
            tokio::time::Instant::now() + Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
        )
        .await
    }

    pub async fn call_with_deadline(
        &self,
        message: ClientMessage,
        deadline: tokio::time::Instant,
    ) -> Result<ServerMessage, ClientError> {
        let request_id = message
            .request_id()
            .ok_or(ClientError::Router(RouterErrorCode::InvalidMessage))?
            .to_owned();
        let _membership_guard = if matches!(
            message,
            ClientMessage::WorkspaceJoin { .. } | ClientMessage::WorkspaceLeave { .. }
        ) {
            self.membership_operation
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| ClientError::Router(RouterErrorCode::WorkspaceBusy))?;
            Some(MembershipCallGuard(self.membership_operation.clone()))
        } else {
            None
        };
        let effect = CallEffect::from_message(&message);
        let encoded = serde_json::to_vec(&message).map_err(|_| ClientError::Transport)?;
        if encoded.len() > crate::protocol::MAX_WEBSOCKET_MESSAGE_BYTES {
            return Err(ClientError::MessageTooLarge);
        }
        let permits =
            u32::try_from(encoded.len().max(1)).map_err(|_| ClientError::MessageTooLarge)?;
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| ClientError::QueueFull)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = ClientCommand::Rpc {
            request_id: request_id.clone(),
            encoded,
            deadline,
            effect,
            reply: reply_tx,
            _bytes: permit,
        };
        match self.sender.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => return Err(ClientError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(ClientError::Closed),
        }
        let mut cancellation = CancelOnDrop {
            sender: self.sender.clone(),
            request_id,
            armed: true,
        };
        let result = reply_rx.await.map_err(|_| ClientError::Disconnected)?;
        cancellation.armed = false;
        result
    }
    #[must_use]
    pub fn session_id(&self) -> Option<Uuid> {
        self.connection
            .read()
            .ok()
            .and_then(|connection| connection.session_id)
    }

    #[must_use]
    pub fn operator_is_admin(&self) -> Option<bool> {
        self.connection
            .read()
            .ok()
            .and_then(|connection| connection.operator_is_admin)
    }

    #[must_use]
    pub fn connection_state(&self) -> watch::Receiver<ClientConnectionState> {
        self.connection_state.clone()
    }

    pub async fn set_ready(&self, ready: bool) -> Result<(), ClientError> {
        self.write_only(ClientMessage::Readiness { ready }).await
    }

    pub async fn reply(
        &self,
        request_id: String,
        ok: bool,
        content: Option<String>,
        error: Option<RouterErrorCode>,
    ) -> Result<(), ClientError> {
        self.write_only(ClientMessage::Reply {
            request_id,
            ok,
            content,
            error,
        })
        .await
    }

    async fn write_only(&self, message: ClientMessage) -> Result<(), ClientError> {
        let encoded = serde_json::to_vec(&message).map_err(|_| ClientError::Transport)?;
        if encoded.len() > crate::protocol::MAX_WEBSOCKET_MESSAGE_BYTES {
            return Err(ClientError::MessageTooLarge);
        }
        let permits =
            u32::try_from(encoded.len().max(1)).map_err(|_| ClientError::MessageTooLarge)?;
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| ClientError::QueueFull)?;
        let (reply, received) = oneshot::channel();
        self.sender
            .try_send(ClientCommand::Write {
                encoded,
                reply,
                _bytes: permit,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ClientError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => ClientError::Closed,
            })?;
        received.await.map_err(|_| ClientError::Disconnected)?
    }

    pub async fn execution_barrier(&self) -> Result<ExecutionBarrier, ClientError> {
        let request_id = format!("barrier:{}", Uuid::new_v4());
        let encoded = serde_json::to_vec(&ClientMessage::Ping {
            request_id: request_id.clone(),
        })
        .map_err(|_| ClientError::Transport)?;
        let permits =
            u32::try_from(encoded.len().max(1)).map_err(|_| ClientError::MessageTooLarge)?;
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| ClientError::QueueFull)?;
        let (reply, received) = oneshot::channel();
        self.sender
            .try_send(ClientCommand::Barrier {
                request_id: request_id.clone(),
                encoded,
                deadline: tokio::time::Instant::now()
                    + Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
                reply,
                _bytes: permit,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ClientError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => ClientError::Closed,
            })?;
        let mut cancellation = CancelOnDrop {
            sender: self.sender.clone(),
            request_id,
            armed: true,
        };
        let result = received.await.map_err(|_| ClientError::Disconnected)?;
        cancellation.armed = false;
        result
    }

    pub async fn close(&self) -> Result<(), ClientError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(ClientCommand::Close { reply: reply_tx })
            .await
            .map_err(|_| ClientError::Closed)?;
        reply_rx.await.map_err(|_| ClientError::Disconnected)
    }

    pub fn ack_event(&self, workspace: WorkspaceName, seq: i64) -> Result<(), ClientError> {
        self.sender
            .try_send(ClientCommand::AckEvent { workspace, seq })
            .map_err(|_| ClientError::QueueFull)
    }

    pub async fn workspace_join(
        &self,
        name: WorkspaceName,
    ) -> Result<(WorkspaceName, i64), ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::WorkspaceJoin { request_id, name })
            .await?
        {
            ServerMessage::WorkspaceJoined {
                workspace, cursor, ..
            } => Ok((workspace, cursor)),
            message => Err(server_error(message)),
        }
    }

    pub async fn workspace_leave(&self) -> Result<Option<WorkspaceName>, ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::WorkspaceLeave { request_id })
            .await?
        {
            ServerMessage::WorkspaceLeft { workspace, .. } => Ok(workspace),
            message => Err(server_error(message)),
        }
    }

    pub async fn workspace_history(
        &self,
        after: Option<i64>,
        limit: Option<u16>,
    ) -> Result<HistoryPage, ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::WorkspaceHistory {
                request_id,
                after,
                limit,
            })
            .await?
        {
            ServerMessage::WorkspaceHistory { page, .. } => Ok(page),
            message => Err(server_error(message)),
        }
    }

    pub async fn workspace_subscribe(
        &self,
        after: i64,
    ) -> Result<(Vec<WorkspaceEvent>, i64, bool), ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::WorkspaceSubscribe { request_id, after })
            .await?
        {
            ServerMessage::WorkspaceSubscription {
                events,
                next_cursor,
                live,
                ..
            } => Ok((events, next_cursor, live)),
            message => Err(server_error(message)),
        }
    }

    pub async fn work_idle(
        &self,
        workspace: WorkspaceName,
        work_request_id: String,
        task: Option<TaskFence>,
        ready: bool,
    ) -> Result<(), ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::WorkIdle {
                request_id,
                workspace,
                work_request_id,
                task,
                ready,
            })
            .await?
        {
            ServerMessage::WorkIdleAck { .. } => Ok(()),
            message => Err(server_error(message)),
        }
    }

    pub async fn task_execution_stopped(
        &self,
        workspace: WorkspaceName,
        task_id: i64,
        attempt_id: Uuid,
        ended_session_id: Uuid,
        evidence: TaskExecutionEvidence,
        reason: PauseReason,
    ) -> Result<(), ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::TaskExecutionStopped {
                request_id,
                workspace,
                task_id,
                attempt_id,
                ended_session_id,
                evidence,
                reason,
            })
            .await?
        {
            ServerMessage::TaskExecutionStoppedAck { .. } => Ok(()),
            message => Err(server_error(message)),
        }
    }

    pub async fn task_list(
        &self,
        workspace: WorkspaceName,
        states: Option<Vec<TaskState>>,
        assigned_agent_id: Option<String>,
        after: Option<i64>,
        limit: Option<u16>,
    ) -> Result<(Vec<TaskSummary>, i64, bool), ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::TaskList {
                request_id,
                workspace,
                states,
                assigned_agent_id,
                after,
                limit,
            })
            .await?
        {
            ServerMessage::Tasks {
                tasks,
                next_cursor,
                has_more,
                ..
            } => Ok((tasks, next_cursor, has_more)),
            message => Err(server_error(message)),
        }
    }

    pub async fn task_get(
        &self,
        workspace: WorkspaceName,
        task_id: i64,
    ) -> Result<TaskDetail, ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::TaskGet {
                request_id,
                workspace,
                task_id,
            })
            .await?
        {
            ServerMessage::Task { task, .. } => Ok(task),
            message => Err(server_error(message)),
        }
    }

    pub async fn task_history(
        &self,
        workspace: WorkspaceName,
        task_id: i64,
        after: Option<i64>,
        limit: Option<u16>,
    ) -> Result<TaskHistoryPage, ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::TaskHistory {
                request_id,
                workspace,
                task_id,
                after,
                limit,
            })
            .await?
        {
            ServerMessage::TaskHistory { page, .. } => Ok(page),
            message => Err(server_error(message)),
        }
    }

    pub async fn integration_list(
        &self,
        workspace: WorkspaceName,
    ) -> Result<Vec<IntegrationPublic>, ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::IntegrationList {
                request_id,
                workspace,
            })
            .await?
        {
            ServerMessage::Integrations { integrations, .. } => Ok(integrations),
            message => Err(server_error(message)),
        }
    }

    pub async fn integration_check(
        &self,
        workspace: WorkspaceName,
        provider: ExternalProvider,
    ) -> Result<IntegrationPublic, ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::IntegrationCheck {
                request_id,
                workspace,
                provider,
            })
            .await?
        {
            ServerMessage::IntegrationChecked { integration, .. } => Ok(integration),
            message => Err(server_error(message)),
        }
    }

    pub async fn integration_reload(&self) -> Result<Vec<IntegrationPublic>, ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::IntegrationReload { request_id })
            .await?
        {
            ServerMessage::IntegrationsReloaded { integrations, .. } => Ok(integrations),
            message => Err(server_error(message)),
        }
    }

    pub async fn task_import(
        &self,
        workspace: WorkspaceName,
        provider: ExternalProvider,
        external_id: String,
        operation_id: Uuid,
    ) -> Result<ExternalOperationSummary, ClientError> {
        self.external_operation(ClientMessage::TaskImport {
            request_id: new_request_id(),
            workspace,
            provider,
            external_id,
            operation_id,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn task_link(
        &self,
        workspace: WorkspaceName,
        provider: ExternalProvider,
        external_id: String,
        task_id: i64,
        expected_version: i64,
        operation_id: Uuid,
        replace: bool,
    ) -> Result<ExternalOperationSummary, ClientError> {
        self.external_operation(ClientMessage::TaskLink {
            request_id: new_request_id(),
            workspace,
            operation_id,
            task_id,
            expected_version,
            provider,
            external_id,
            replace,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn task_publish(
        &self,
        workspace: WorkspaceName,
        provider: ExternalProvider,
        task_id: i64,
        expected_version: i64,
        operation_id: Uuid,
        kind: ExternalPublishKind,
        report_id: Option<Uuid>,
    ) -> Result<ExternalOperationSummary, ClientError> {
        self.external_operation(ClientMessage::TaskPublish {
            request_id: new_request_id(),
            workspace,
            operation_id,
            task_id,
            expected_version,
            provider,
            kind,
            report_id,
        })
        .await
    }

    pub async fn task_external_status(
        &self,
        workspace: WorkspaceName,
        operation_id: Uuid,
    ) -> Result<(ExternalOperationSummary, Option<ExternalResolution>), ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::TaskExternalStatus {
                request_id,
                workspace,
                operation_id,
            })
            .await?
        {
            ServerMessage::ExternalStatus {
                operation,
                resolution,
                ..
            } => Ok((operation, resolution)),
            message => Err(server_error(message)),
        }
    }

    pub async fn task_external_resolve(
        &self,
        workspace: WorkspaceName,
        operation_id: Uuid,
        resolution_id: Uuid,
        outcome: ExternalResolutionOutcome,
        external_id: Option<String>,
        note: String,
    ) -> Result<(ExternalOperationSummary, ExternalResolution), ClientError> {
        let request_id = new_request_id();
        match self
            .call(ClientMessage::TaskExternalResolve {
                request_id,
                workspace,
                operation_id,
                resolution_id,
                outcome,
                external_id,
                note,
            })
            .await?
        {
            ServerMessage::ExternalResolved {
                operation,
                resolution,
                ..
            } => Ok((operation, resolution)),
            message => Err(server_error(message)),
        }
    }

    async fn external_operation(
        &self,
        message: ClientMessage,
    ) -> Result<ExternalOperationSummary, ClientError> {
        match self.call(message).await? {
            ServerMessage::ExternalOperation { operation, .. } => Ok(operation),
            message => Err(server_error(message)),
        }
    }

    pub async fn task_mutation(
        &self,
        message: ClientMessage,
    ) -> Result<TaskMutationResult, ClientError> {
        match self.call(message).await? {
            ServerMessage::TaskMutated { result, .. } => Ok(result),
            message => Err(server_error(message)),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CallEffect {
    None,
    WorkspaceLeave,
    WorkspaceSubscribe(i64),
    WorkspaceUnsubscribe,
}

impl CallEffect {
    fn from_message(message: &ClientMessage) -> Self {
        match message {
            ClientMessage::WorkspaceLeave { .. } => Self::WorkspaceLeave,
            ClientMessage::WorkspaceSubscribe { after, .. } => Self::WorkspaceSubscribe(*after),
            ClientMessage::WorkspaceUnsubscribe { .. } => Self::WorkspaceUnsubscribe,
            _ => Self::None,
        }
    }
}

enum PendingReply {
    Rpc(oneshot::Sender<Result<ServerMessage, ClientError>>),
    Barrier(oneshot::Sender<Result<ExecutionBarrier, ClientError>>),
}

struct PendingCall {
    reply: PendingReply,
    deadline: tokio::time::Instant,
    effect: CallEffect,
    workspace: Option<WorkspaceName>,
    subscription_revision: u64,
}

impl PendingCall {
    fn fail(self, error: ClientError) {
        match self.reply {
            PendingReply::Rpc(reply) => {
                let _ = reply.send(Err(error));
            }
            PendingReply::Barrier(reply) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    fn finish(self, response: ServerMessage, memory: &ConnectionMemory) {
        match self.reply {
            PendingReply::Rpc(reply) => {
                let _ = reply.send(Ok(response));
            }
            PendingReply::Barrier(reply) => {
                let result = if matches!(response, ServerMessage::Pong { .. }) {
                    memory.execution_barrier()
                } else {
                    Err(ClientError::Router(RouterErrorCode::InvalidMessage))
                };
                let _ = reply.send(result);
            }
        }
    }
}

enum ClientCommand {
    Rpc {
        request_id: String,
        encoded: Vec<u8>,
        deadline: tokio::time::Instant,
        effect: CallEffect,
        reply: oneshot::Sender<Result<ServerMessage, ClientError>>,
        _bytes: OwnedSemaphorePermit,
    },
    Barrier {
        request_id: String,
        encoded: Vec<u8>,
        deadline: tokio::time::Instant,
        reply: oneshot::Sender<Result<ExecutionBarrier, ClientError>>,
        _bytes: OwnedSemaphorePermit,
    },
    Write {
        encoded: Vec<u8>,
        reply: oneshot::Sender<Result<(), ClientError>>,
        _bytes: OwnedSemaphorePermit,
    },
    Cancel {
        request_id: String,
    },
    AckEvent {
        workspace: WorkspaceName,
        seq: i64,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
}

struct CancelOnDrop {
    sender: mpsc::Sender<ClientCommand>,
    request_id: String,
    armed: bool,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.sender.try_send(ClientCommand::Cancel {
                request_id: self.request_id.clone(),
            });
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ConnectionSnapshot {
    session_id: Option<Uuid>,
    operator_is_admin: Option<bool>,
    current: Option<TaskFence>,
    stop_pending: Option<TaskFence>,
}

struct ConnectionMemory {
    desired_workspace: Option<WorkspaceName>,
    acked_cursors: HashMap<WorkspaceName, i64>,
    subscribed_workspace: Option<WorkspaceName>,
    highest_enqueued: HashMap<WorkspaceName, i64>,
    subscription_revision: u64,
    connected_once: bool,
    session_id: Option<Uuid>,
    operator_is_admin: Option<bool>,
    current: Option<TaskFence>,
    stop_pending: Option<TaskFence>,
    shared: Arc<RwLock<ConnectionSnapshot>>,
    state: watch::Sender<ClientConnectionState>,
    epoch: u64,
}

impl ConnectionMemory {
    fn new(
        shared: Arc<RwLock<ConnectionSnapshot>>,
        state: watch::Sender<ClientConnectionState>,
    ) -> Self {
        Self {
            desired_workspace: None,
            acked_cursors: HashMap::new(),
            subscribed_workspace: None,
            highest_enqueued: HashMap::new(),
            subscription_revision: 0,
            connected_once: false,
            session_id: None,
            operator_is_admin: None,
            current: None,
            stop_pending: None,
            shared,
            state,
            epoch: 0,
        }
    }

    fn closed(&mut self, reason: Option<RouterErrorCode>) {
        self.clear_registration();
        self.state
            .send_replace(ClientConnectionState::Closed { reason });
    }

    fn publish(&self) {
        if let Ok(mut shared) = self.shared.write() {
            shared.session_id = self.session_id;
            shared.operator_is_admin = self.operator_is_admin;
            shared.current.clone_from(&self.current);
            shared.stop_pending.clone_from(&self.stop_pending);
        }
    }

    fn set_registration(&mut self, session_id: Option<Uuid>, operator_is_admin: Option<bool>) {
        self.session_id = session_id;
        self.operator_is_admin = operator_is_admin;
        self.current = None;
        self.stop_pending = None;
        self.publish();
    }

    fn clear_registration(&mut self) {
        self.set_registration(None, None);
    }

    fn set_attempts(&mut self, current: Option<TaskFence>, stop_pending: Option<TaskFence>) {
        self.current = current;
        self.stop_pending = stop_pending;
        self.publish();
    }

    fn execution_barrier(&self) -> Result<ExecutionBarrier, ClientError> {
        let session_id = self.session_id.ok_or(ClientError::Disconnected)?;
        Ok(ExecutionBarrier {
            session_id,
            current: self.current.clone(),
            stop_pending: self.stop_pending.clone(),
        })
    }
}

async fn connection_task(
    config: ClientConfig,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    mut commands: mpsc::Receiver<ClientCommand>,
    events: mpsc::Sender<ClientEventItem>,
    event_bytes: Arc<Semaphore>,
    mut memory: ConnectionMemory,
    initial: oneshot::Sender<Result<(), ClientError>>,
) {
    let mut initial = Some(initial);
    let mut reconnect_attempt = 0_u32;
    loop {
        match establish(
            &config,
            tls_config.as_ref(),
            &mut memory,
            &events,
            &event_bytes,
            &mut commands,
        )
        .await
        {
            Ok(socket) => {
                reconnect_attempt = 0;
                memory.connected_once = true;
                memory.epoch += 1;
                memory.state.send_replace(ClientConnectionState::Connected {
                    epoch: memory.epoch,
                });
                if let Some(initial) = initial.take() {
                    let _ = initial.send(Ok(()));
                }
                match pump(
                    socket,
                    &mut commands,
                    &events,
                    &event_bytes,
                    &mut memory,
                    !matches!(&config.role, ClientRole::Delegate { .. }),
                )
                .await
                {
                    PumpResult::Closed => {
                        memory.closed(None);
                        return;
                    }
                    PumpResult::Disconnected(mut pending) => {
                        memory.clear_registration();
                        memory
                            .state
                            .send_replace(ClientConnectionState::Reconnecting);
                        for (_, pending) in pending.drain() {
                            pending.fail(ClientError::Disconnected);
                        }
                    }
                    PumpResult::Terminal(code, mut pending) => {
                        memory.closed(Some(code));
                        for (_, pending) in pending.drain() {
                            pending.fail(ClientError::Router(code));
                        }
                        let _ = enqueue_event(
                            &events,
                            &event_bytes,
                            &mut memory,
                            ClientEvent::Closed(code),
                        );
                        return;
                    }
                }
            }
            Err(ClientError::Closed) => {
                memory.closed(None);
                if let Some(initial) = initial.take() {
                    let _ = initial.send(Err(ClientError::Closed));
                }
                return;
            }
            Err(error) if !memory.connected_once => {
                let reason = match &error {
                    ClientError::Router(code) => Some(*code),
                    _ => None,
                };
                memory.closed(reason);
                if let Some(initial) = initial.take() {
                    let _ = initial.send(Err(error));
                }
                return;
            }
            Err(error) if terminal_connection_error(&error) => {
                let reason = match &error {
                    ClientError::Router(code) => Some(*code),
                    _ => None,
                };
                memory.closed(reason);
                if let ClientError::Router(code) = error {
                    let _ = enqueue_event(
                        &events,
                        &event_bytes,
                        &mut memory,
                        ClientEvent::Closed(code),
                    );
                }
                return;
            }
            Err(_) => {
                memory.clear_registration();
            }
        }
        let delay = reconnect_delay(reconnect_attempt);
        reconnect_attempt = reconnect_attempt.saturating_add(1);
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => break,
                command = commands.recv() => {
                    if handle_disconnected_command(command, &mut memory).is_err() {
                        return;
                    }
                }
            }
        }
    }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn handle_disconnected_command(
    command: Option<ClientCommand>,
    memory: &mut ConnectionMemory,
) -> Result<(), ClientError> {
    match command {
        Some(ClientCommand::AckEvent { workspace, seq }) => {
            advance_cursor(memory, workspace, seq);
        }
        Some(ClientCommand::Rpc { reply, .. }) => {
            let _ = reply.send(Err(ClientError::Disconnected));
        }
        Some(ClientCommand::Barrier { reply, .. }) => {
            let _ = reply.send(Err(ClientError::Disconnected));
        }
        Some(ClientCommand::Write { reply, .. }) => {
            let _ = reply.send(Err(ClientError::Disconnected));
        }
        Some(ClientCommand::Cancel { .. }) => {}
        Some(ClientCommand::Close { reply }) => {
            memory.closed(None);
            let _ = reply.send(());
            return Err(ClientError::Closed);
        }
        None => {
            memory.closed(None);
            return Err(ClientError::Closed);
        }
    }
    Ok(())
}

async fn wait_during_restore<T>(
    future: impl Future<Output = Result<T, ClientError>>,
    deadline: tokio::time::Instant,
    commands: &mut mpsc::Receiver<ClientCommand>,
    memory: &mut ConnectionMemory,
) -> Result<T, ClientError> {
    tokio::pin!(future);
    let timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            biased;
            () = &mut timeout => return Err(ClientError::Transport),
            command = commands.recv() => handle_disconnected_command(command, memory)?,
            result = &mut future => return result,
        }
    }
}

fn membership_changed(
    memory: &mut ConnectionMemory,
    workspace: Option<WorkspaceName>,
    cursor: i64,
) -> ClientEvent {
    if memory.subscribed_workspace.as_ref() != workspace.as_ref() {
        memory.subscribed_workspace = None;
    }
    memory.desired_workspace.clone_from(&workspace);
    if let Some(workspace) = &workspace {
        memory
            .acked_cursors
            .entry(workspace.clone())
            .or_insert(cursor);
    }
    ClientEvent::MembershipChanged { workspace, cursor }
}

fn restore_notification(
    message: ServerMessage,
    memory: &mut ConnectionMemory,
) -> Result<ClientEvent, ClientError> {
    match message {
        ServerMessage::WorkspaceChanged { workspace, cursor } => {
            Ok(membership_changed(memory, workspace, cursor))
        }
        ServerMessage::TaskAttemptChanged {
            workspace,
            task_id,
            attempt,
            closed_attempt_id,
            current,
            stop_pending,
        } => {
            memory.set_attempts(current.clone(), stop_pending.clone());
            Ok(ClientEvent::TaskAttemptChanged {
                workspace,
                task_id,
                attempt,
                closed_attempt_id,
                current,
                stop_pending,
            })
        }
        message => Err(server_error(message)),
    }
}

async fn establish(
    config: &ClientConfig,
    tls_config: Option<&Arc<rustls::ClientConfig>>,
    memory: &mut ConnectionMemory,
    events: &mpsc::Sender<ClientEventItem>,
    event_bytes: &Arc<Semaphore>,
    commands: &mut mpsc::Receiver<ClientCommand>,
) -> Result<WsStream, ClientError> {
    let connector = match tls_config {
        Some(config) => Connector::Rustls(config.clone()),
        None => Connector::Plain,
    };
    let websocket_config = WebSocketConfig::default()
        .max_message_size(Some(crate::protocol::MAX_WEBSOCKET_MESSAGE_BYTES))
        .max_frame_size(Some(crate::protocol::MAX_WEBSOCKET_MESSAGE_BYTES))
        .max_write_buffer_size(CLIENT_BYTE_CAPACITY);
    let (mut socket, _) = wait_during_restore(
        Box::pin(async {
            connect_async_tls_with_config(
                config.router_url.as_str(),
                Some(websocket_config),
                false,
                Some(connector),
            )
            .await
            .map_err(|_| ClientError::Transport)
        }),
        tokio::time::Instant::now() + Duration::from_secs(5),
        commands,
        memory,
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let registration = registration_message(&config.role);
    wait_during_restore(
        send_json(&mut socket, &registration),
        deadline,
        commands,
        memory,
    )
    .await?;
    let registered =
        wait_during_restore(recv_server(&mut socket), deadline, commands, memory).await?;
    let (workspace, cursor, session_id, operator_is_admin) = match (&config.role, registered) {
        (
            ClientRole::Primary { .. } | ClientRole::Delegate { .. },
            ServerMessage::Registered {
                protocol_version,
                agent,
                workspace,
                cursor,
                ..
            },
        ) if protocol_version == PROTOCOL_VERSION => {
            (workspace, cursor, Some(agent.session_id), None)
        }
        (
            ClientRole::Operator { .. },
            ServerMessage::RegisteredOperator {
                protocol_version,
                admin,
                workspace,
                cursor,
                ..
            },
        ) if protocol_version == PROTOCOL_VERSION => (workspace, cursor, None, Some(admin)),
        (_, ServerMessage::Registered { .. } | ServerMessage::RegisteredOperator { .. }) => {
            return Err(ClientError::Router(RouterErrorCode::ProtocolMismatch));
        }
        (_, message) => return Err(server_error(message)),
    };
    memory.set_registration(session_id, operator_is_admin);
    if !memory.connected_once {
        memory.desired_workspace.clone_from(&workspace);
        if let Some(workspace) = &workspace {
            memory.acked_cursors.insert(workspace.clone(), cursor);
        }
    } else if matches!(config.role, ClientRole::Delegate { .. }) {
        if memory.subscribed_workspace.as_ref() != workspace.as_ref() {
            memory.subscribed_workspace = None;
        }
        memory.desired_workspace.clone_from(&workspace);
        if let Some(workspace) = &workspace {
            memory
                .acked_cursors
                .entry(workspace.clone())
                .or_insert(cursor);
        }
    } else if workspace.is_some() {
        return Err(ClientError::Router(RouterErrorCode::ProtocolMismatch));
    }
    if memory.connected_once
        && !matches!(config.role, ClientRole::Delegate { .. })
        && let Some(workspace) = memory.desired_workspace.clone()
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let request_id = new_request_id();
        let join = ClientMessage::WorkspaceJoin {
            request_id: request_id.clone(),
            name: workspace,
        };
        wait_during_restore(send_json(&mut socket, &join), deadline, commands, memory).await?;
        loop {
            match wait_during_restore(recv_server(&mut socket), deadline, commands, memory).await? {
                ServerMessage::WorkspaceJoined { request_id: id, .. } if id == request_id => break,
                message => {
                    let event = restore_notification(message, memory)?;
                    enqueue_restored_event(events, event_bytes, memory, commands, event, deadline)
                        .await?;
                }
            }
        }
    }
    if memory.connected_once
        && let Some(workspace) = memory.desired_workspace.clone()
        && memory.subscribed_workspace.as_ref() == Some(&workspace)
    {
        let after = memory.acked_cursors.get(&workspace).copied().unwrap_or(0);
        restore_subscription(
            &mut socket,
            workspace,
            after,
            memory,
            events,
            event_bytes,
            commands,
        )
        .await?;
    }
    Ok(socket)
}

async fn restore_subscription(
    socket: &mut WsStream,
    workspace: WorkspaceName,
    mut after: i64,
    memory: &mut ConnectionMemory,
    events: &mpsc::Sender<ClientEventItem>,
    event_bytes: &Arc<Semaphore>,
    commands: &mut mpsc::Receiver<ClientCommand>,
) -> Result<(), ClientError> {
    loop {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let request_id = new_request_id();
        let subscribe = ClientMessage::WorkspaceSubscribe {
            request_id: request_id.clone(),
            after,
        };
        wait_during_restore(send_json(socket, &subscribe), deadline, commands, memory).await?;
        loop {
            match wait_during_restore(recv_server(socket), deadline, commands, memory).await? {
                ServerMessage::WorkspaceSubscription {
                    request_id: id,
                    workspace: response_workspace,
                    events: page,
                    next_cursor,
                    live,
                } if id == request_id => {
                    if response_workspace != workspace {
                        return Err(ClientError::Router(RouterErrorCode::InvalidMessage));
                    }
                    if memory.desired_workspace.as_ref() != Some(&workspace)
                        || memory.subscribed_workspace.as_ref() != Some(&workspace)
                    {
                        return Ok(());
                    }
                    for event in page {
                        enqueue_restored_event(
                            events,
                            event_bytes,
                            memory,
                            commands,
                            ClientEvent::WorkspaceEvent(event),
                            deadline,
                        )
                        .await?;
                    }
                    after = next_cursor;
                    if live {
                        return Ok(());
                    }
                    break;
                }
                message => {
                    let event = restore_notification(message, memory)?;
                    enqueue_restored_event(events, event_bytes, memory, commands, event, deadline)
                        .await?;
                }
            }
        }
    }
}

async fn pump(
    socket: WsStream,
    commands: &mut mpsc::Receiver<ClientCommand>,
    events: &mpsc::Sender<ClientEventItem>,
    event_bytes: &Arc<Semaphore>,
    memory: &mut ConnectionMemory,
    disconnect_on_uncertain_leave: bool,
) -> PumpResult {
    let (mut sink, mut stream) = socket.split();
    let mut pending = HashMap::<String, PendingCall>::new();
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut pending_pong: Option<(String, tokio::time::Instant)> = None;
    loop {
        let next_deadline = pending.values().map(|call| call.deadline).min();
        tokio::select! {
            biased;
            () = async {
                if let Some(deadline) = next_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                let now = tokio::time::Instant::now();
                let expired = pending
                    .iter()
                    .filter(|(_, call)| call.deadline <= now)
                    .map(|(request_id, _)| request_id.clone())
                    .collect::<Vec<_>>();
                let mut disconnect = false;
                for request_id in expired {
                    let Some(call) = pending.remove(&request_id) else {
                        continue;
                    };
                    let code = if call.effect == CallEffect::WorkspaceLeave
                        && !disconnect_on_uncertain_leave
                    {
                        RouterErrorCode::LeaveUnconfirmed
                    } else {
                        RouterErrorCode::RequestTimeout
                    };
                    let effect = call.effect;
                    call.fail(ClientError::Router(code));
                    if effect == CallEffect::WorkspaceLeave && disconnect_on_uncertain_leave {
                        memory.desired_workspace = None;
                        memory.subscribed_workspace = None;
                        disconnect = true;
                    }
                }
                if disconnect {
                    let _ = sink.close().await;
                    return PumpResult::Disconnected(pending);
                }
            }
            command = commands.recv() => {
                match command {
                    Some(ClientCommand::Rpc {
                        request_id,
                        encoded,
                        deadline,
                        effect,
                        reply,
                        ..
                    }) => {
                        if pending.contains_key(&request_id) {
                            let _ = reply.send(Err(ClientError::Router(RouterErrorCode::RequestConflict)));
                            continue;
                        }
                        if sink.send(Message::Text(String::from_utf8_lossy(&encoded).into_owned().into())).await.is_err() {
                            let _ = reply.send(Err(ClientError::Disconnected));
                            return PumpResult::Disconnected(pending);
                        }
                        let subscription_call = matches!(
                            effect,
                            CallEffect::WorkspaceSubscribe(_) | CallEffect::WorkspaceUnsubscribe
                        );
                        if subscription_call {
                            memory.subscription_revision += 1;
                        }
                        pending.insert(request_id, PendingCall {
                            reply: PendingReply::Rpc(reply),
                            deadline,
                            effect,
                            workspace: subscription_call
                                .then(|| memory.desired_workspace.clone())
                                .flatten(),
                            subscription_revision: memory.subscription_revision,
                        });
                    }
                    Some(ClientCommand::Barrier {
                        request_id,
                        encoded,
                        deadline,
                        reply,
                        ..
                    }) => {
                        if pending.contains_key(&request_id) {
                            let _ = reply.send(Err(ClientError::Router(
                                RouterErrorCode::RequestConflict,
                            )));
                            continue;
                        }
                        if sink
                            .send(Message::Text(
                                String::from_utf8_lossy(&encoded).into_owned().into(),
                            ))
                            .await
                            .is_err()
                        {
                            let _ = reply.send(Err(ClientError::Disconnected));
                            return PumpResult::Disconnected(pending);
                        }
                        pending.insert(
                            request_id,
                            PendingCall {
                                reply: PendingReply::Barrier(reply),
                                deadline,
                                effect: CallEffect::None,
                                workspace: None,
                                subscription_revision: 0,
                            },
                        );
                    }
                    Some(ClientCommand::Write { encoded, reply, .. }) => {
                        let result = sink
                            .send(Message::Text(
                                String::from_utf8_lossy(&encoded).into_owned().into(),
                            ))
                            .await
                            .map_err(|_| ClientError::Disconnected);
                        let disconnected = result.is_err();
                        let _ = reply.send(result);
                        if disconnected {
                            return PumpResult::Disconnected(pending);
                        }
                    }
                    Some(ClientCommand::Cancel { request_id }) => {
                        let disconnect = pending
                            .remove(&request_id)
                            .is_some_and(|call| call.effect == CallEffect::WorkspaceLeave)
                            && disconnect_on_uncertain_leave;
                        if disconnect {
                            memory.desired_workspace = None;
                            memory.subscribed_workspace = None;
                            let _ = sink.close().await;
                            return PumpResult::Disconnected(pending);
                        }
                    }
                    Some(ClientCommand::AckEvent { workspace, seq }) => {
                        advance_cursor(memory, workspace, seq);
                    }
                    Some(ClientCommand::Close { reply }) => {
                        let _ = sink.close().await;
                        memory.closed(None);
                        let _ = reply.send(());
                        for (_, pending_call) in pending.drain() {
                            pending_call.fail(ClientError::Closed);
                        }
                        return PumpResult::Closed;
                    }
                    None => return PumpResult::Closed,
                }
            }
            incoming = stream.next() => {
                let Some(Ok(message)) = incoming else {
                    return PumpResult::Disconnected(pending);
                };
                match message {
                    Message::Text(text) => {
                        let parsed = match parse_server_message(text.as_str()) {
                            Ok(parsed) => parsed,
                            Err(code) => return PumpResult::Terminal(code, pending),
                        };
                        if let ServerMessage::Pong { request_id } = &parsed
                            && pending_pong.as_ref().is_some_and(|(id, _)| id == request_id)
                        {
                            pending_pong = None;
                            continue;
                        }
                        match parsed {
                            ServerMessage::WorkspaceChanged { workspace, cursor } => {
                                let event = membership_changed(memory, workspace, cursor);
                                if enqueue_event(events, event_bytes, memory, event).is_err() {
                                    return PumpResult::Disconnected(pending);
                                }
                            }
                            ServerMessage::WorkspaceEvent { event } => {
                                if enqueue_event(events, event_bytes, memory, ClientEvent::WorkspaceEvent(event)).is_err() {
                                    return PumpResult::Disconnected(pending);
                                }
                            }
                            ServerMessage::Deliver { workspace, request_id, from, content, timeout_ms, task } => {
                                if enqueue_event(events, event_bytes, memory, ClientEvent::Delivery { workspace, request_id, from, content, timeout_ms, task }).is_err() {
                                    return PumpResult::Disconnected(pending);
                                }
                            }
                            ServerMessage::CancelWork { workspace, request_id, reason, task } => {
                                if let Some(fence) = &task {
                                    memory.set_attempts(None, Some(fence.clone()));
                                }
                                if enqueue_event(
                                    events,
                                    event_bytes,
                                    memory,
                                    ClientEvent::WorkCancelled {
                                        workspace,
                                        request_id,
                                        reason,
                                        task,
                                    },
                                )
                                .is_err()
                                {
                                    return PumpResult::Disconnected(pending);
                                }
                            }
                            ServerMessage::TaskAttemptChanged {
                                workspace,
                                task_id,
                                attempt,
                                closed_attempt_id,
                                current,
                                stop_pending,
                            } => {
                                memory.set_attempts(current.clone(), stop_pending.clone());
                                if enqueue_event(
                                    events,
                                    event_bytes,
                                    memory,
                                    ClientEvent::TaskAttemptChanged {
                                        workspace,
                                        task_id,
                                        attempt,
                                        closed_attempt_id,
                                        current,
                                        stop_pending,
                                    },
                                )
                                .is_err()
                                {
                                    return PumpResult::Disconnected(pending);
                                }
                            }
                            response => {
                                match &response {
                                    ServerMessage::WorkspaceJoined { workspace, cursor, .. } => {
                                        memory.desired_workspace = Some(workspace.clone());
                                        memory.acked_cursors.entry(workspace.clone()).or_insert(*cursor);
                                    }
                                    ServerMessage::WorkspaceLeft { .. } => {
                                        memory.desired_workspace = None;
                                        memory.subscribed_workspace = None;
                                    }
                                    ServerMessage::Error { request_id: None, code, .. }
                                        if terminal_router_code(*code) =>
                                    {
                                        return PumpResult::Terminal(*code, pending);
                                    }
                                    _ => {}
                                }
                                if let Some(request_id) = response.request_id().map(str::to_owned) {
                                    if let Some(call) = pending.remove(&request_id) {
                                        if call.subscription_revision == memory.subscription_revision
                                            && call.workspace == memory.desired_workspace
                                        {
                                            match (call.effect, &response) {
                                                (
                                                    CallEffect::WorkspaceSubscribe(after),
                                                    ServerMessage::WorkspaceSubscription {
                                                        workspace,
                                                        live,
                                                        ..
                                                    },
                                                ) if call.workspace.as_ref() == Some(workspace) => {
                                                    memory.acked_cursors.insert(workspace.clone(), after);
                                                    memory.highest_enqueued.remove(workspace);
                                                    if *live {
                                                        memory.subscribed_workspace = Some(workspace.clone());
                                                    }
                                                }
                                                (
                                                    CallEffect::WorkspaceUnsubscribe,
                                                    ServerMessage::WorkspaceUnsubscribed { workspace, .. },
                                                ) if call.workspace.as_ref() == Some(workspace)
                                                    && memory.subscribed_workspace.as_ref() == Some(workspace) =>
                                                {
                                                    memory.subscribed_workspace = None;
                                                }
                                                _ => {}
                                            }
                                        }
                                        call.finish(response, memory);
                                    } else if let ServerMessage::Result {
                                        workspace,
                                        request_id,
                                        from,
                                        ok,
                                        content,
                                        error,
                                        ..
                                    } = response
                                        && enqueue_event(
                                            events,
                                            event_bytes,
                                            memory,
                                            ClientEvent::SendResult(AgentSendResult {
                                                request_id,
                                                workspace: Some(workspace),
                                                from: Some(from),
                                                ok,
                                                content,
                                                error,
                                            }),
                                        )
                                        .is_err()
                                    {
                                        return PumpResult::Disconnected(pending);
                                    }
                                }
                            }
                        }
                    }
                    Message::Ping(payload) => {
                        if sink.send(Message::Pong(payload)).await.is_err() {
                            return PumpResult::Disconnected(pending);
                        }
                    }
                    Message::Close(_) => return PumpResult::Disconnected(pending),
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
            _ = heartbeat.tick() => {
                if pending_pong.as_ref().is_some_and(|(_, deadline)| *deadline <= tokio::time::Instant::now()) {
                    return PumpResult::Disconnected(pending);
                }
                if pending_pong.is_none() {
                    let request_id = format!("heartbeat:{}", Uuid::new_v4());
                    let message = ClientMessage::Ping { request_id: request_id.clone() };
                    let Ok(encoded) = serde_json::to_string(&message) else {
                        return PumpResult::Disconnected(pending);
                    };
                    if sink.send(Message::Text(encoded.into())).await.is_err() {
                        return PumpResult::Disconnected(pending);
                    }
                    pending_pong = Some((request_id, tokio::time::Instant::now() + HEARTBEAT_TIMEOUT));
                }
            }
        }
    }
}

enum PumpResult {
    Closed,
    Disconnected(HashMap<String, PendingCall>),
    Terminal(RouterErrorCode, HashMap<String, PendingCall>),
}

fn enqueue_event(
    sender: &mpsc::Sender<ClientEventItem>,
    bytes: &Arc<Semaphore>,
    memory: &mut ConnectionMemory,
    event: ClientEvent,
) -> Result<(), ClientError> {
    if event_already_enqueued(memory, &event) {
        return Ok(());
    }
    let permit = bytes
        .clone()
        .try_acquire_many_owned(event_permits(&event)?)
        .map_err(|_| ClientError::QueueFull)?;
    let slot = sender.try_reserve().map_err(|_| ClientError::QueueFull)?;
    remember_enqueued_event(memory, &event);
    slot.send(ClientEventItem {
        event,
        _bytes: permit,
    });
    Ok(())
}

async fn enqueue_restored_event(
    sender: &mpsc::Sender<ClientEventItem>,
    bytes: &Arc<Semaphore>,
    memory: &mut ConnectionMemory,
    commands: &mut mpsc::Receiver<ClientCommand>,
    event: ClientEvent,
    deadline: tokio::time::Instant,
) -> Result<(), ClientError> {
    if event_already_enqueued(memory, &event) {
        return Ok(());
    }
    let permits = event_permits(&event)?;
    let (permit, slot) = wait_during_restore(
        async {
            let permit = bytes
                .clone()
                .acquire_many_owned(permits)
                .await
                .map_err(|_| ClientError::Closed)?;
            let slot = sender.reserve().await.map_err(|_| ClientError::Closed)?;
            Ok((permit, slot))
        },
        deadline,
        commands,
        memory,
    )
    .await?;
    remember_enqueued_event(memory, &event);
    slot.send(ClientEventItem {
        event,
        _bytes: permit,
    });
    Ok(())
}

fn event_already_enqueued(memory: &ConnectionMemory, event: &ClientEvent) -> bool {
    matches!(
        event,
        ClientEvent::WorkspaceEvent(event)
            if memory.highest_enqueued.get(&event.workspace).is_some_and(|highest| *highest >= event.seq)
    )
}

fn remember_enqueued_event(memory: &mut ConnectionMemory, event: &ClientEvent) {
    if let ClientEvent::WorkspaceEvent(event) = event {
        memory
            .highest_enqueued
            .insert(event.workspace.clone(), event.seq);
    }
}

fn event_permits(event: &ClientEvent) -> Result<u32, ClientError> {
    let encoded_len = serde_json::to_vec(&event_size_value(event))
        .map_err(|_| ClientError::Transport)?
        .len()
        .max(1);
    if encoded_len > CLIENT_BYTE_CAPACITY {
        return Err(ClientError::MessageTooLarge);
    }
    u32::try_from(encoded_len).map_err(|_| ClientError::MessageTooLarge)
}

fn event_size_value(event: &ClientEvent) -> serde_json::Value {
    match event {
        ClientEvent::WorkspaceEvent(event) => {
            serde_json::to_value(event).unwrap_or(serde_json::Value::Null)
        }
        ClientEvent::Delivery { content, .. } => serde_json::Value::String(content.clone()),
        ClientEvent::SendResult(result) => result
            .content
            .clone()
            .map_or(serde_json::Value::Null, serde_json::Value::String),
        _ => serde_json::Value::Null,
    }
}

fn advance_cursor(memory: &mut ConnectionMemory, workspace: WorkspaceName, seq: i64) {
    let cursor = memory.acked_cursors.entry(workspace).or_insert(0);
    if seq > *cursor {
        *cursor = seq;
    }
}

fn registration_message(role: &ClientRole) -> ClientMessage {
    match role {
        ClientRole::Primary {
            agent,
            credential,
            delegation_token,
        } => ClientMessage::Register {
            protocol_version: PROTOCOL_VERSION,
            agent: agent.clone(),
            token: credential.token().expose().to_owned(),
            delegation_token: delegation_token
                .as_ref()
                .map(|value| value.expose().to_owned()),
        },
        ClientRole::Delegate {
            owner_id,
            delegation_token,
        } => ClientMessage::RegisterDelegate {
            protocol_version: PROTOCOL_VERSION,
            owner_id: owner_id.clone(),
            delegation_token: delegation_token.expose().to_owned(),
        },
        ClientRole::Operator { credential } => ClientMessage::RegisterOperator {
            protocol_version: PROTOCOL_VERSION,
            token: credential.token().expose().to_owned(),
        },
    }
}

async fn send_json(socket: &mut WsStream, message: &ClientMessage) -> Result<(), ClientError> {
    let encoded = serde_json::to_string(message).map_err(|_| ClientError::Transport)?;
    if encoded.len() > crate::protocol::MAX_WEBSOCKET_MESSAGE_BYTES {
        return Err(ClientError::MessageTooLarge);
    }
    socket
        .send(Message::Text(encoded.into()))
        .await
        .map_err(|_| ClientError::Transport)
}

async fn recv_server(socket: &mut WsStream) -> Result<ServerMessage, ClientError> {
    loop {
        let message = socket
            .next()
            .await
            .ok_or(ClientError::Disconnected)?
            .map_err(|_| ClientError::Transport)?;
        match message {
            Message::Text(text) => {
                return parse_server_message(text.as_str()).map_err(ClientError::Router);
            }
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .map_err(|_| ClientError::Transport)?,
            Message::Close(_) => return Err(ClientError::Disconnected),
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn server_error(message: ServerMessage) -> ClientError {
    match message {
        ServerMessage::Error { code, .. } => ClientError::Router(code),
        _ => ClientError::Router(RouterErrorCode::InvalidMessage),
    }
}

fn reconnect_delay(attempt: u32) -> Duration {
    let exponent = attempt.min(6);
    let base = RECONNECT_INITIAL
        .saturating_mul(2_u32.saturating_pow(exponent))
        .min(RECONNECT_MAX);
    let mut random = [0_u8; 2];
    let factor = if getrandom::fill(&mut random).is_ok() {
        800_u128 + u128::from(u16::from_le_bytes(random) % 401)
    } else {
        1000
    };
    let millis = base.as_millis().saturating_mul(factor) / 1000;
    Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
}

fn terminal_router_code(code: RouterErrorCode) -> bool {
    matches!(
        code,
        RouterErrorCode::ProtocolMismatch
            | RouterErrorCode::InvalidMessage
            | RouterErrorCode::Unauthorized
            | RouterErrorCode::NotRegistered
    )
}

fn terminal_connection_error(error: &ClientError) -> bool {
    matches!(error, ClientError::Router(code) if terminal_router_code(*code))
}

fn new_request_id() -> String {
    format!("rpc:{}", Uuid::new_v4())
}
