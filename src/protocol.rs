use std::{collections::HashSet, fmt, str::FromStr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    credentials::{CredentialFile, CredentialRole, MAX_GRANTS, PublicCredentialClaims},
    integrations::IntegrationPublic,
    tasks::{
        ExternalOperationSummary, ExternalProvider, ExternalPublishKind, ExternalResolution,
        ExternalResolutionOutcome, MAX_SAFE_INTEGER, PauseReason, TaskAttempt, TaskCheckpoint,
        TaskDetail, TaskEvent, TaskMutationResult, TaskReportRecord, TaskState, TaskSummary,
        validate_description, validate_handoff_note, validate_note, validate_positive_safe_integer,
        validate_title,
    },
};

pub const PROTOCOL_VERSION: u8 = 2;
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 60_000;
pub const MAX_REQUEST_TIMEOUT_MS: u64 = 600_000;
pub const MAX_SHARED_CONTENT_BYTES: usize = 64 * 1024;
pub const MAX_JSON_STRING_BYTES: usize = 128 * 1024;
pub const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 512 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 256 * 1024;
pub const MAX_ERROR_BYTES: usize = 256;
pub const MAX_MCP_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_WORKSPACE_NAME_BYTES: usize = 64;
pub const MAX_AGENT_ID_BYTES: usize = 128;
pub const DEFAULT_PAGE_LIMIT: u16 = 50;
pub const MAX_PAGE_LIMIT: u16 = 100;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct WorkspaceName(String);

impl WorkspaceName {
    pub fn parse(value: impl Into<String>) -> Result<Self, RouterErrorCode> {
        let value = value.into();
        if is_workspace_name(&value) {
            Ok(Self(value))
        } else {
            Err(RouterErrorCode::InvalidMessage)
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for WorkspaceName {
    type Err = RouterErrorCode;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}
impl<'de> Deserialize<'de> for WorkspaceName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentSide {
    Claude,
    Codex,
    Generic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum AgentClient {
    Omp,
    ClaudeCode,
    ClaudeSdk,
    CodexCli,
    CodexAppServer,
    Generic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Idle,
    Busy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    Push,
    Pull,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRegistration {
    pub agent_id: String,
    pub side: AgentSide,
    pub client: AgentClient,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<String>,
    #[serde(default = "default_delivery_mode")]
    pub delivery_mode: DeliveryMode,
}

const fn default_delivery_mode() -> DeliveryMode {
    DeliveryMode::Push
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentDescriptor {
    pub agent_id: String,
    pub side: AgentSide,
    pub client: AgentClient,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<String>,
    pub status: AgentStatus,
    pub delivery_mode: DeliveryMode,
    pub ready: bool,
    pub session_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskDispatch {
    pub id: i64,
    pub expected_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskFence {
    pub task_id: i64,
    pub attempt_id: Uuid,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CredentialSummary {
    #[serde(flatten)]
    pub claims: PublicCredentialClaims,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceSummary {
    pub name: WorkspaceName,
    pub created_at: i64,
    pub connected_agents: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceMembership {
    pub workspace: WorkspaceName,
    pub cursor: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceEventKind {
    Chat,
    Request,
    Result,
    MemberJoined,
    MemberLeft,
    Task,
    Integration,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceEvent {
    pub workspace: WorkspaceName,
    pub seq: i64,
    pub kind: WorkspaceEventKind,
    pub actor_id: String,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RouterErrorCode>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoryPage {
    pub workspace: WorkspaceName,
    pub events: Vec<WorkspaceEvent>,
    pub next_cursor: i64,
    pub has_more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskHistoryEvent {
    pub seq: i64,
    pub actor_id: String,
    pub created_at: i64,
    #[serde(flatten)]
    pub event: TaskEvent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<TaskAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<TaskReportRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskHistoryPage {
    pub workspace: WorkspaceName,
    pub task_id: i64,
    pub events: Vec<TaskHistoryEvent>,
    pub next_cursor: i64,
    pub has_more: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RouterErrorCode {
    InvalidMessage,
    ProtocolMismatch,
    Unauthorized,
    NotRegistered,
    AgentConflict,
    TargetOffline,
    TargetNotReady,
    RequestConflict,
    RequestNotFound,
    ReplyForbidden,
    TargetDisconnected,
    RequesterDisconnected,
    RequestTimeout,
    RequestCancelled,
    WorkspaceRequired,
    WorkspaceNotFound,
    WorkspaceConflict,
    WorkspaceAlreadyJoined,
    WorkspaceBusy,
    WorkspaceMismatch,
    PermissionDenied,
    MessageTooLarge,
    RateLimited,
    StorageError,
    StoreInUse,
    ConfigurationRequired,
    RouterRestarted,
    SessionBusy,
    ProviderError,
    ProviderNotReady,
    ProviderDisconnected,
    ReplyMissing,
    TaskNotFound,
    TaskConflict,
    TaskNotAssigned,
    TaskActive,
    TaskStaleAttempt,
    TaskStopUnconfirmed,
    TaskInvalidTransition,
    TaskInterrupted,
    IntegrationNotConfigured,
    IntegrationBusy,
    IntegrationError,
    ExternalUnconfirmed,
    ExternalConflict,
    IntegrationConfigurationInvalid,
    SessionAdapterFailed,
    ClaudeProtocolError,
    ClaudeNoResult,
    ClaudeSdkError,
    ClaudeMaxTurns,
    ClaudeMaxBudget,
    ClaudeStructuredOutputError,
    ClaudeExecutionError,
    CodexProtocolError,
    CodexNoFinalResponse,
    CodexTurnInterrupted,
    CodexTurnFailed,
    LeaveUnconfirmed,
}

impl RouterErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidMessage => "invalid_message",
            Self::ProtocolMismatch => "protocol_mismatch",
            Self::Unauthorized => "unauthorized",
            Self::NotRegistered => "not_registered",
            Self::AgentConflict => "agent_conflict",
            Self::TargetOffline => "target_offline",
            Self::TargetNotReady => "target_not_ready",
            Self::RequestConflict => "request_conflict",
            Self::RequestNotFound => "request_not_found",
            Self::ReplyForbidden => "reply_forbidden",
            Self::TargetDisconnected => "target_disconnected",
            Self::RequesterDisconnected => "requester_disconnected",
            Self::RequestTimeout => "request_timeout",
            Self::RequestCancelled => "request_cancelled",
            Self::WorkspaceRequired => "workspace_required",
            Self::WorkspaceNotFound => "workspace_not_found",
            Self::WorkspaceConflict => "workspace_conflict",
            Self::WorkspaceAlreadyJoined => "workspace_already_joined",
            Self::WorkspaceBusy => "workspace_busy",
            Self::WorkspaceMismatch => "workspace_mismatch",
            Self::PermissionDenied => "permission_denied",
            Self::MessageTooLarge => "message_too_large",
            Self::RateLimited => "rate_limited",
            Self::StorageError => "storage_error",
            Self::StoreInUse => "store_in_use",
            Self::ConfigurationRequired => "configuration_required",
            Self::RouterRestarted => "router_restarted",
            Self::SessionBusy => "session_busy",
            Self::ProviderError => "provider_error",
            Self::ProviderNotReady => "provider_not_ready",
            Self::ProviderDisconnected => "provider_disconnected",
            Self::ReplyMissing => "reply_missing",
            Self::TaskNotFound => "task_not_found",
            Self::TaskConflict => "task_conflict",
            Self::TaskNotAssigned => "task_not_assigned",
            Self::TaskActive => "task_active",
            Self::TaskStaleAttempt => "task_stale_attempt",
            Self::TaskStopUnconfirmed => "task_stop_unconfirmed",
            Self::TaskInvalidTransition => "task_invalid_transition",
            Self::TaskInterrupted => "task_interrupted",
            Self::IntegrationNotConfigured => "integration_not_configured",
            Self::IntegrationBusy => "integration_busy",
            Self::IntegrationError => "integration_error",
            Self::ExternalUnconfirmed => "external_unconfirmed",
            Self::ExternalConflict => "external_conflict",
            Self::IntegrationConfigurationInvalid => "integration_configuration_invalid",
            Self::SessionAdapterFailed => "session_adapter_failed",
            Self::ClaudeProtocolError => "claude_protocol_error",
            Self::ClaudeNoResult => "claude_no_result",
            Self::ClaudeSdkError => "claude_sdk_error",
            Self::ClaudeMaxTurns => "claude_max_turns",
            Self::ClaudeMaxBudget => "claude_max_budget",
            Self::ClaudeStructuredOutputError => "claude_structured_output_error",
            Self::ClaudeExecutionError => "claude_execution_error",
            Self::CodexProtocolError => "codex_protocol_error",
            Self::CodexNoFinalResponse => "codex_no_final_response",
            Self::CodexTurnInterrupted => "codex_turn_interrupted",
            Self::CodexTurnFailed => "codex_turn_failed",
            Self::LeaveUnconfirmed => "leave_unconfirmed",
        }
    }
}

impl fmt::Display for RouterErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for RouterErrorCode {}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Register {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        agent: AgentRegistration,
        token: String,
        #[serde(rename = "delegationToken")]
        delegation_token: Option<String>,
    },
    RegisterDelegate {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        #[serde(rename = "ownerId")]
        owner_id: String,
        #[serde(rename = "delegationToken")]
        delegation_token: String,
    },
    RegisterOperator {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        token: String,
    },
    Ping {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    Readiness {
        ready: bool,
    },
    List {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    Send {
        #[serde(rename = "requestId")]
        request_id: String,
        to: String,
        content: String,
        #[serde(rename = "timeoutMs")]
        timeout_ms: Option<u64>,
    },
    Reply {
        #[serde(rename = "requestId")]
        request_id: String,
        ok: bool,
        content: Option<String>,
        error: Option<RouterErrorCode>,
    },
    WorkIdle {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "workRequestId")]
        work_request_id: String,
        task: Option<TaskFence>,
        ready: bool,
    },
    TaskExecutionStopped {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "attemptId")]
        attempt_id: Uuid,
        #[serde(rename = "endedSessionId")]
        ended_session_id: Uuid,
        evidence: TaskExecutionEvidence,
        reason: PauseReason,
    },
    CredentialIssue {
        #[serde(rename = "requestId")]
        request_id: String,
        role: CredentialRole,
        subject: String,
        #[serde(rename = "agentSide")]
        agent_side: Option<AgentSide>,
        #[serde(rename = "agentClient")]
        agent_client: Option<AgentClient>,
        workspaces: Vec<WorkspaceName>,
    },
    CredentialList {
        #[serde(rename = "requestId")]
        request_id: String,
        after: Option<Uuid>,
        limit: Option<u16>,
    },
    CredentialRevoke {
        #[serde(rename = "requestId")]
        request_id: String,
        id: Uuid,
    },
    WorkspaceCreate {
        #[serde(rename = "requestId")]
        request_id: String,
        name: WorkspaceName,
    },
    WorkspaceList {
        #[serde(rename = "requestId")]
        request_id: String,
        after: Option<String>,
        limit: Option<u16>,
    },
    WorkspaceJoin {
        #[serde(rename = "requestId")]
        request_id: String,
        name: WorkspaceName,
    },
    WorkspaceLeave {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    WorkspaceMembers {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    WorkspacePost {
        #[serde(rename = "requestId")]
        request_id: String,
        content: String,
    },
    WorkspaceHistory {
        #[serde(rename = "requestId")]
        request_id: String,
        after: Option<i64>,
        limit: Option<u16>,
    },
    WorkspaceSubscribe {
        #[serde(rename = "requestId")]
        request_id: String,
        after: i64,
    },
    WorkspaceUnsubscribe {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    TaskList {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        states: Option<Vec<TaskState>>,
        #[serde(rename = "assignedAgentId")]
        assigned_agent_id: Option<String>,
        after: Option<i64>,
        limit: Option<u16>,
    },
    TaskGet {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "taskId")]
        task_id: i64,
    },
    TaskHistory {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "taskId")]
        task_id: i64,
        after: Option<i64>,
        limit: Option<u16>,
    },
    TaskCreate {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        title: String,
        description: String,
    },
    TaskEdit {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        title: Option<String>,
        description: Option<String>,
    },
    TaskAssign {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        #[serde(rename = "agentId")]
        agent_id: Option<String>,
    },
    TaskNote {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        text: String,
    },
    TaskBegin {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "workRequestId")]
        work_request_id: String,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        #[serde(rename = "lastCheckpointId")]
        last_checkpoint_id: Option<Uuid>,
        #[serde(rename = "resumeNote")]
        resume_note: String,
    },
    TaskCheckpoint {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "attemptId")]
        attempt_id: Uuid,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        checkpoint: TaskCheckpoint,
    },
    TaskPause {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "attemptId")]
        attempt_id: Uuid,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        checkpoint: TaskCheckpoint,
        reason: TaskPauseKind,
    },
    TaskComplete {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "attemptId")]
        attempt_id: Uuid,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        result: TaskCheckpoint,
    },
    TaskCancel {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        note: String,
    },
    TaskReopen {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        note: String,
    },
    TaskRequest {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        message: Option<String>,
        #[serde(rename = "timeoutMs")]
        timeout_ms: Option<u64>,
    },
    TaskInterrupt {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        note: String,
    },
    TaskConfirmStopped {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "attemptId")]
        attempt_id: Uuid,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        note: String,
    },
    TaskImport {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        provider: ExternalProvider,
        #[serde(rename = "externalId")]
        external_id: String,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
    },
    TaskLink {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        provider: ExternalProvider,
        #[serde(rename = "externalId")]
        external_id: String,
        #[serde(default)]
        replace: bool,
    },
    TaskPublish {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "expectedVersion")]
        expected_version: i64,
        provider: ExternalProvider,
        kind: ExternalPublishKind,
        #[serde(rename = "reportId")]
        report_id: Option<Uuid>,
    },
    TaskExternalStatus {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
    },
    TaskExternalResolve {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "operationId")]
        operation_id: Uuid,
        #[serde(rename = "resolutionId")]
        resolution_id: Uuid,
        outcome: ExternalResolutionOutcome,
        #[serde(rename = "externalId")]
        external_id: Option<String>,
        note: String,
    },
    IntegrationList {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
    },
    IntegrationCheck {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        provider: ExternalProvider,
    },
    IntegrationReload {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    RouterShutdown {
        #[serde(rename = "requestId")]
        request_id: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutionEvidence {
    ProviderTerminal,
    ProviderClosed,
    HostIdle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPauseKind {
    Paused,
    Blocked,
}

impl ClientMessage {
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Register { .. }
            | Self::RegisterDelegate { .. }
            | Self::RegisterOperator { .. }
            | Self::Readiness { .. } => None,
            Self::Ping { request_id }
            | Self::List { request_id }
            | Self::Send { request_id, .. }
            | Self::Reply { request_id, .. }
            | Self::WorkIdle { request_id, .. }
            | Self::TaskExecutionStopped { request_id, .. }
            | Self::CredentialIssue { request_id, .. }
            | Self::CredentialList { request_id, .. }
            | Self::CredentialRevoke { request_id, .. }
            | Self::WorkspaceCreate { request_id, .. }
            | Self::WorkspaceList { request_id, .. }
            | Self::WorkspaceJoin { request_id, .. }
            | Self::WorkspaceLeave { request_id }
            | Self::WorkspaceMembers { request_id }
            | Self::WorkspacePost { request_id, .. }
            | Self::WorkspaceHistory { request_id, .. }
            | Self::WorkspaceSubscribe { request_id, .. }
            | Self::WorkspaceUnsubscribe { request_id }
            | Self::TaskList { request_id, .. }
            | Self::TaskGet { request_id, .. }
            | Self::TaskHistory { request_id, .. }
            | Self::TaskCreate { request_id, .. }
            | Self::TaskEdit { request_id, .. }
            | Self::TaskAssign { request_id, .. }
            | Self::TaskNote { request_id, .. }
            | Self::TaskBegin { request_id, .. }
            | Self::TaskCheckpoint { request_id, .. }
            | Self::TaskPause { request_id, .. }
            | Self::TaskComplete { request_id, .. }
            | Self::TaskCancel { request_id, .. }
            | Self::TaskReopen { request_id, .. }
            | Self::TaskRequest { request_id, .. }
            | Self::TaskInterrupt { request_id, .. }
            | Self::TaskConfirmStopped { request_id, .. }
            | Self::TaskImport { request_id, .. }
            | Self::TaskLink { request_id, .. }
            | Self::TaskPublish { request_id, .. }
            | Self::TaskExternalStatus { request_id, .. }
            | Self::TaskExternalResolve { request_id, .. }
            | Self::IntegrationList { request_id, .. }
            | Self::IntegrationCheck { request_id, .. }
            | Self::IntegrationReload { request_id }
            | Self::RouterShutdown { request_id } => Some(request_id),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Registered {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        agent: AgentDescriptor,
        role: RegistrationRole,
        workspace: Option<WorkspaceName>,
        cursor: i64,
    },
    RegisteredOperator {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        subject: String,
        admin: bool,
        workspace: Option<WorkspaceName>,
        cursor: i64,
    },
    Pong {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    Error {
        #[serde(rename = "requestId", skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        code: RouterErrorCode,
        #[serde(skip_serializing_if = "Option::is_none")]
        workspace: Option<WorkspaceName>,
        #[serde(rename = "operationId", skip_serializing_if = "Option::is_none")]
        operation_id: Option<Uuid>,
        #[serde(rename = "currentVersion", skip_serializing_if = "Option::is_none")]
        current_version: Option<i64>,
    },
    Agents {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        agents: Vec<AgentDescriptor>,
    },
    Accepted {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        to: String,
    },
    Deliver {
        workspace: WorkspaceName,
        #[serde(rename = "requestId")]
        request_id: String,
        from: String,
        content: String,
        #[serde(rename = "timeoutMs")]
        timeout_ms: u64,
        task: Option<TaskDispatch>,
    },
    Result {
        workspace: WorkspaceName,
        #[serde(rename = "requestId")]
        request_id: String,
        from: String,
        ok: bool,
        content: Option<String>,
        error: Option<RouterErrorCode>,
        #[serde(rename = "taskId")]
        task_id: Option<i64>,
    },
    CancelWork {
        workspace: WorkspaceName,
        #[serde(rename = "requestId")]
        request_id: String,
        reason: RouterErrorCode,
        task: Option<TaskFence>,
    },
    WorkIdleAck {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "workRequestId")]
        work_request_id: String,
    },
    TaskExecutionStoppedAck {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(rename = "taskId")]
        task_id: i64,
        #[serde(rename = "attemptId")]
        attempt_id: Uuid,
    },
    TaskAttemptChanged {
        workspace: WorkspaceName,
        #[serde(rename = "taskId")]
        task_id: i64,
        attempt: Option<TaskAttempt>,
        #[serde(rename = "closedAttemptId")]
        closed_attempt_id: Option<Uuid>,
        current: Option<TaskFence>,
        #[serde(rename = "stopPending")]
        stop_pending: Option<TaskFence>,
    },
    WorkspaceCreated {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceSummary,
    },
    Workspaces {
        #[serde(rename = "requestId")]
        request_id: String,
        workspaces: Vec<WorkspaceSummary>,
        #[serde(rename = "nextCursor")]
        next_cursor: Option<String>,
        #[serde(rename = "hasMore")]
        has_more: bool,
    },
    CredentialIssued {
        #[serde(rename = "requestId")]
        request_id: String,
        credential: CredentialFile,
    },
    Credentials {
        #[serde(rename = "requestId")]
        request_id: String,
        credentials: Vec<CredentialSummary>,
        #[serde(rename = "nextCursor")]
        next_cursor: Option<Uuid>,
        #[serde(rename = "hasMore")]
        has_more: bool,
    },
    CredentialRevoked {
        #[serde(rename = "requestId")]
        request_id: String,
        id: Uuid,
    },
    Integrations {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        integrations: Vec<IntegrationPublic>,
    },
    IntegrationChecked {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        integration: IntegrationPublic,
    },
    IntegrationsReloaded {
        #[serde(rename = "requestId")]
        request_id: String,
        integrations: Vec<IntegrationPublic>,
    },
    ExternalOperation {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        operation: ExternalOperationSummary,
    },
    ExternalStatus {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        operation: ExternalOperationSummary,
        resolution: Option<ExternalResolution>,
    },
    ExternalResolved {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        operation: ExternalOperationSummary,
        resolution: ExternalResolution,
    },
    WorkspaceJoined {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        cursor: i64,
    },
    WorkspaceLeft {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: Option<WorkspaceName>,
    },
    WorkspaceChanged {
        workspace: Option<WorkspaceName>,
        cursor: i64,
    },
    WorkspacePosted {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        seq: i64,
    },
    WorkspaceHistory {
        #[serde(rename = "requestId")]
        request_id: String,
        #[serde(flatten)]
        page: HistoryPage,
    },
    WorkspaceSubscription {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        events: Vec<WorkspaceEvent>,
        #[serde(rename = "nextCursor")]
        next_cursor: i64,
        live: bool,
    },
    WorkspaceEvent {
        event: WorkspaceEvent,
    },
    WorkspaceUnsubscribed {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
    },
    Tasks {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        tasks: Vec<TaskSummary>,
        #[serde(rename = "nextCursor")]
        next_cursor: i64,
        #[serde(rename = "hasMore")]
        has_more: bool,
    },
    Task {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        task: TaskDetail,
    },
    TaskHistory {
        #[serde(rename = "requestId")]
        request_id: String,
        #[serde(flatten)]
        page: TaskHistoryPage,
    },
    TaskMutated {
        #[serde(rename = "requestId")]
        request_id: String,
        workspace: WorkspaceName,
        #[serde(flatten)]
        result: TaskMutationResult,
    },
    RouterStopping {
        #[serde(rename = "requestId")]
        request_id: String,
    },
}

impl ServerMessage {
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Pong { request_id }
            | Self::Agents { request_id, .. }
            | Self::Accepted { request_id, .. }
            | Self::Result { request_id, .. }
            | Self::WorkIdleAck { request_id, .. }
            | Self::TaskExecutionStoppedAck { request_id, .. }
            | Self::WorkspaceCreated { request_id, .. }
            | Self::Workspaces { request_id, .. }
            | Self::WorkspaceJoined { request_id, .. }
            | Self::WorkspaceLeft { request_id, .. }
            | Self::CredentialIssued { request_id, .. }
            | Self::Credentials { request_id, .. }
            | Self::CredentialRevoked { request_id, .. }
            | Self::WorkspacePosted { request_id, .. }
            | Self::WorkspaceHistory { request_id, .. }
            | Self::WorkspaceSubscription { request_id, .. }
            | Self::WorkspaceUnsubscribed { request_id, .. }
            | Self::Tasks { request_id, .. }
            | Self::Task { request_id, .. }
            | Self::TaskMutated { request_id, .. }
            | Self::TaskHistory { request_id, .. }
            | Self::Integrations { request_id, .. }
            | Self::IntegrationChecked { request_id, .. }
            | Self::IntegrationsReloaded { request_id, .. }
            | Self::ExternalOperation { request_id, .. }
            | Self::ExternalStatus { request_id, .. }
            | Self::ExternalResolved { request_id, .. }
            | Self::RouterStopping { request_id } => Some(request_id),
            Self::Error { request_id, .. } => request_id.as_deref(),
            Self::Registered { .. }
            | Self::RegisteredOperator { .. }
            | Self::Deliver { .. }
            | Self::CancelWork { .. }
            | Self::TaskAttemptChanged { .. }
            | Self::WorkspaceChanged { .. }
            | Self::WorkspaceEvent { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistrationRole {
    Agent,
    Delegate,
}

pub fn parse_client_message(raw: &str) -> Result<ClientMessage, RouterErrorCode> {
    if raw.len() > MAX_WEBSOCKET_MESSAGE_BYTES {
        return Err(RouterErrorCode::MessageTooLarge);
    }
    let value: Value = serde_json::from_str(raw).map_err(|_| RouterErrorCode::InvalidMessage)?;
    let object = value.as_object().ok_or(RouterErrorCode::InvalidMessage)?;
    let message_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or(RouterErrorCode::InvalidMessage)?;
    if matches!(
        message_type,
        "register" | "register_delegate" | "register_operator"
    ) && object.get("protocolVersion").and_then(Value::as_u64)
        != Some(u64::from(PROTOCOL_VERSION))
    {
        return Err(RouterErrorCode::ProtocolMismatch);
    }
    validate_unknown_fields(message_type, object)?;
    let message: ClientMessage =
        serde_json::from_value(value).map_err(|_| RouterErrorCode::InvalidMessage)?;
    validate_client_message(&message)?;
    Ok(message)
}

pub fn parse_server_message(raw: &str) -> Result<ServerMessage, RouterErrorCode> {
    if raw.len() > MAX_WEBSOCKET_MESSAGE_BYTES {
        return Err(RouterErrorCode::MessageTooLarge);
    }
    let value: Value = serde_json::from_str(raw).map_err(|_| RouterErrorCode::InvalidMessage)?;
    let object = value.as_object().ok_or(RouterErrorCode::InvalidMessage)?;
    let message_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or(RouterErrorCode::InvalidMessage)?;
    if matches!(message_type, "registered" | "registered_operator")
        && object.get("protocolVersion").and_then(Value::as_u64)
            != Some(u64::from(PROTOCOL_VERSION))
    {
        return Err(RouterErrorCode::ProtocolMismatch);
    }
    validate_server_unknown_fields(message_type, object)?;
    let message = serde_json::from_value(value).map_err(|_| RouterErrorCode::InvalidMessage)?;
    validate_server_message(&message)?;
    Ok(message)
}

#[must_use]
pub fn normalize_timeout_ms(value: Option<u64>) -> u64 {
    value
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS)
        .clamp(1, MAX_REQUEST_TIMEOUT_MS)
}

pub fn validate_page(
    after: Option<i64>,
    limit: Option<u16>,
) -> Result<(i64, u16), RouterErrorCode> {
    let after = after.unwrap_or(0);
    let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if after < 0 || !(1..=MAX_PAGE_LIMIT).contains(&limit) {
        return Err(RouterErrorCode::InvalidMessage);
    }
    Ok((after, limit))
}

#[must_use]
pub fn is_workspace_name(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_WORKSPACE_NAME_BYTES || !value.is_ascii() {
        return false;
    }
    value.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_alphanumeric() || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
    })
}

#[must_use]
pub fn is_agent_id(value: &str) -> bool {
    if value.is_empty()
        || value.len() > MAX_AGENT_ID_BYTES
        || !value.is_ascii()
        || value.starts_with("operator:")
        || value.starts_with("system:")
    {
        return false;
    }
    value.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_alphanumeric() || (index > 0 && matches!(byte, b'.' | b'_' | b'-' | b':'))
    })
}

#[must_use]
pub fn is_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_AGENT_ID_BYTES
        && value.is_ascii()
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphanumeric() || (index > 0 && matches!(byte, b'.' | b'_' | b'-' | b':'))
        })
}

fn validate_client_message(message: &ClientMessage) -> Result<(), RouterErrorCode> {
    if let Some(request_id) = message.request_id()
        && !is_request_id(request_id)
    {
        return Err(RouterErrorCode::InvalidMessage);
    }
    match message {
        ClientMessage::Register {
            agent,
            token,
            delegation_token,
            ..
        } => {
            if !is_agent_id(&agent.agent_id)
                || !valid_bearer(token)
                || delegation_token
                    .as_deref()
                    .is_some_and(|value| !valid_bearer(value))
                || !valid_agent_client(agent.side, agent.client)
                || agent.activity.as_deref().is_some_and(|value| {
                    value.len() > MAX_ERROR_BYTES || !value.chars().all(valid_text_scalar)
                })
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::RegisterDelegate {
            owner_id,
            delegation_token,
            ..
        } => {
            if !is_agent_id(owner_id) || !valid_bearer(delegation_token) {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::RegisterOperator { token, .. } if !valid_bearer(token) => {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ClientMessage::Send {
            to,
            content,
            timeout_ms,
            ..
        } => {
            if !is_agent_id(to) || !valid_shared_content(content) || !valid_timeout(*timeout_ms) {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::Reply {
            ok, content, error, ..
        } => {
            let valid = if *ok {
                content.as_deref().is_some_and(valid_shared_content) && error.is_none()
            } else {
                content.is_none() && error.is_some()
            };
            if !valid {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::WorkIdle {
            work_request_id,
            task,
            ..
        } => {
            if !is_request_id(work_request_id)
                || task
                    .as_ref()
                    .is_some_and(|fence| validate_positive_safe_integer(fence.task_id).is_err())
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::TaskExecutionStopped {
            task_id, reason, ..
        } => {
            if validate_positive_safe_integer(*task_id).is_err()
                || !matches!(
                    reason,
                    PauseReason::SessionEnded
                        | PauseReason::TurnEnded
                        | PauseReason::HostError
                        | PauseReason::OperatorInterrupt
                        | PauseReason::RequestTimeout
                        | PauseReason::RequestCancelled
                )
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::CredentialIssue {
            role,
            subject,
            agent_side,
            agent_client,
            workspaces,
            ..
        } => {
            let distinct = workspaces
                .iter()
                .map(WorkspaceName::as_str)
                .collect::<HashSet<_>>()
                .len();
            let valid_subject = match role {
                CredentialRole::Agent => {
                    is_agent_id(subject)
                        && agent_side
                            .zip(*agent_client)
                            .is_some_and(|(side, client)| valid_agent_client(side, client))
                        && !workspaces.is_empty()
                }
                CredentialRole::Operator => {
                    !subject.is_empty()
                        && subject.len() <= 119
                        && subject.is_ascii()
                        && !subject.starts_with("system:")
                        && agent_side.is_none()
                        && agent_client.is_none()
                }
            };
            if !valid_subject || workspaces.len() > MAX_GRANTS || distinct != workspaces.len() {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::CredentialList { limit, .. } if !valid_limit(*limit) => {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ClientMessage::WorkspaceList { after, limit, .. } => {
            if after
                .as_deref()
                .is_some_and(|value| !is_workspace_name(value))
                || !valid_limit(*limit)
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::WorkspaceHistory { after, limit, .. } => {
            if !valid_cursor(*after) || !valid_limit(*limit) {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::WorkspaceSubscribe { after, .. }
            if !(0..=MAX_SAFE_INTEGER).contains(after) =>
        {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ClientMessage::WorkspacePost { content, .. } if !valid_shared_content(content) => {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ClientMessage::TaskList {
            states,
            assigned_agent_id,
            after,
            limit,
            ..
        } => {
            if states.as_ref().is_some_and(|values| {
                values.is_empty()
                    || values.len() > 6
                    || values.iter().copied().collect::<HashSet<_>>().len() != values.len()
            }) || assigned_agent_id
                .as_deref()
                .is_some_and(|value| !is_agent_id(value))
                || !valid_cursor(*after)
                || !valid_limit(*limit)
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::TaskGet { task_id, .. } => {
            validate_positive_safe_integer(*task_id)
                .map_err(|_| RouterErrorCode::InvalidMessage)?;
        }
        ClientMessage::TaskHistory {
            task_id,
            after,
            limit,
            ..
        } => {
            validate_positive_safe_integer(*task_id)
                .map_err(|_| RouterErrorCode::InvalidMessage)?;
            if !valid_cursor(*after) || !valid_limit(*limit) {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::TaskCreate {
            title, description, ..
        } => {
            validate_title(title).map_err(|_| RouterErrorCode::InvalidMessage)?;
            validate_description(description).map_err(|_| RouterErrorCode::InvalidMessage)?;
        }
        ClientMessage::TaskEdit {
            task_id,
            expected_version,
            title,
            description,
            ..
        } => {
            validate_task_version(*task_id, *expected_version)?;
            if title.is_none() && description.is_none() {
                return Err(RouterErrorCode::InvalidMessage);
            }
            if let Some(title) = title {
                validate_title(title).map_err(|_| RouterErrorCode::InvalidMessage)?;
            }
            if let Some(description) = description {
                validate_description(description).map_err(|_| RouterErrorCode::InvalidMessage)?;
            }
        }
        ClientMessage::TaskAssign {
            task_id,
            expected_version,
            agent_id,
            ..
        } => {
            validate_task_version(*task_id, *expected_version)?;
            if agent_id.as_deref().is_some_and(|value| !is_agent_id(value)) {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::TaskNote { task_id, text, .. } => {
            validate_positive_safe_integer(*task_id)
                .map_err(|_| RouterErrorCode::InvalidMessage)?;
            validate_note(text).map_err(|_| RouterErrorCode::InvalidMessage)?;
        }
        ClientMessage::TaskBegin {
            task_id,
            work_request_id,
            expected_version,
            resume_note,
            ..
        } => {
            validate_task_version(*task_id, *expected_version)?;
            if !is_request_id(work_request_id) {
                return Err(RouterErrorCode::InvalidMessage);
            }
            validate_handoff_note(resume_note).map_err(|_| RouterErrorCode::InvalidMessage)?;
        }
        ClientMessage::TaskCheckpoint {
            task_id,
            expected_version,
            checkpoint,
            ..
        }
        | ClientMessage::TaskPause {
            task_id,
            expected_version,
            checkpoint,
            ..
        }
        | ClientMessage::TaskComplete {
            task_id,
            expected_version,
            result: checkpoint,
            ..
        } => {
            validate_task_version(*task_id, *expected_version)?;
            checkpoint
                .validate()
                .map_err(|_| RouterErrorCode::InvalidMessage)?;
        }
        ClientMessage::TaskCancel {
            task_id,
            expected_version,
            note,
            ..
        }
        | ClientMessage::TaskReopen {
            task_id,
            expected_version,
            note,
            ..
        }
        | ClientMessage::TaskInterrupt {
            task_id,
            expected_version,
            note,
            ..
        }
        | ClientMessage::TaskConfirmStopped {
            task_id,
            expected_version,
            note,
            ..
        } => {
            validate_task_version(*task_id, *expected_version)?;
            validate_handoff_note(note).map_err(|_| RouterErrorCode::InvalidMessage)?;
        }
        ClientMessage::TaskRequest {
            task_id,
            expected_version,
            message,
            timeout_ms,
            ..
        } => {
            validate_task_version(*task_id, *expected_version)?;
            if message
                .as_deref()
                .is_some_and(|value| !valid_shared_content(value))
                || !valid_timeout(*timeout_ms)
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::TaskImport { external_id, .. }
            if external_id.is_empty() || external_id.len() > MAX_ERROR_BYTES =>
        {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ClientMessage::TaskLink {
            task_id,
            expected_version,
            external_id,
            ..
        } => {
            validate_task_version(*task_id, *expected_version)?;
            if external_id.is_empty() || external_id.len() > MAX_ERROR_BYTES {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::TaskPublish {
            task_id,
            expected_version,
            kind,
            report_id,
            ..
        } => {
            validate_task_version(*task_id, *expected_version)?;
            if matches!(
                (kind, report_id),
                (ExternalPublishKind::Issue, Some(_)) | (ExternalPublishKind::Report, None)
            ) {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::TaskExternalResolve {
            outcome,
            external_id,
            note,
            ..
        } => {
            validate_handoff_note(note).map_err(|_| RouterErrorCode::InvalidMessage)?;
            let valid_id = external_id
                .as_deref()
                .is_some_and(|value| !value.is_empty() && value.len() <= MAX_ERROR_BYTES);
            if matches!(outcome, ExternalResolutionOutcome::Applied) != valid_id {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ClientMessage::Ping { .. }
        | ClientMessage::Readiness { .. }
        | ClientMessage::List { .. }
        | ClientMessage::WorkspaceCreate { .. }
        | ClientMessage::WorkspaceJoin { .. }
        | ClientMessage::WorkspaceLeave { .. }
        | ClientMessage::WorkspaceMembers { .. }
        | ClientMessage::WorkspaceUnsubscribe { .. }
        | ClientMessage::IntegrationList { .. }
        | ClientMessage::IntegrationCheck { .. }
        | ClientMessage::IntegrationReload { .. }
        | ClientMessage::TaskExternalStatus { .. }
        | ClientMessage::RouterShutdown { .. }
        | ClientMessage::CredentialRevoke { .. }
        | ClientMessage::CredentialList { .. }
        | ClientMessage::RegisterOperator { .. }
        | ClientMessage::WorkspacePost { .. }
        | ClientMessage::WorkspaceSubscribe { .. }
        | ClientMessage::TaskImport { .. } => {}
    }
    Ok(())
}

fn valid_bearer(value: &str) -> bool {
    value.len() == 43
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

const fn valid_agent_client(side: AgentSide, client: AgentClient) -> bool {
    matches!(
        (side, client),
        (AgentSide::Generic, AgentClient::Omp | AgentClient::Generic)
            | (
                AgentSide::Claude,
                AgentClient::ClaudeCode | AgentClient::ClaudeSdk
            )
            | (
                AgentSide::Codex,
                AgentClient::CodexCli | AgentClient::CodexAppServer
            )
    )
}

fn valid_text_scalar(value: char) -> bool {
    !value.is_control()
}

fn valid_shared_content(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SHARED_CONTENT_BYTES
        && serde_json::to_vec(value).is_ok_and(|encoded| encoded.len() <= MAX_JSON_STRING_BYTES)
}

fn valid_timeout(value: Option<u64>) -> bool {
    value.is_none_or(|timeout| (1..=MAX_REQUEST_TIMEOUT_MS).contains(&timeout))
}

fn valid_limit(value: Option<u16>) -> bool {
    value.is_none_or(|limit| (1..=MAX_PAGE_LIMIT).contains(&limit))
}

fn valid_cursor(value: Option<i64>) -> bool {
    value.is_none_or(|cursor| (0..=MAX_SAFE_INTEGER).contains(&cursor))
}

fn validate_task_version(task_id: i64, expected_version: i64) -> Result<(), RouterErrorCode> {
    validate_positive_safe_integer(task_id).map_err(|_| RouterErrorCode::InvalidMessage)?;
    validate_positive_safe_integer(expected_version).map_err(|_| RouterErrorCode::InvalidMessage)
}

fn validate_server_message(message: &ServerMessage) -> Result<(), RouterErrorCode> {
    if let Some(request_id) = message.request_id()
        && !is_request_id(request_id)
    {
        return Err(RouterErrorCode::InvalidMessage);
    }
    match message {
        ServerMessage::Registered {
            agent,
            cursor,
            protocol_version,
            ..
        } => {
            if *protocol_version != PROTOCOL_VERSION
                || !valid_descriptor(agent)
                || !(0..=MAX_SAFE_INTEGER).contains(cursor)
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ServerMessage::RegisteredOperator {
            protocol_version,
            subject,
            cursor,
            ..
        } => {
            if *protocol_version != PROTOCOL_VERSION
                || subject.is_empty()
                || subject.len() > 119
                || !subject.is_ascii()
                || !(0..=MAX_SAFE_INTEGER).contains(cursor)
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ServerMessage::Agents { agents, .. } => {
            if agents.iter().any(|agent| !valid_descriptor(agent)) {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ServerMessage::Deliver {
            request_id,
            from,
            content,
            timeout_ms,
            task,
            ..
        } => {
            if !is_request_id(request_id)
                || !valid_actor_id(from)
                || !valid_shared_content(content)
                || !(1..=MAX_REQUEST_TIMEOUT_MS).contains(timeout_ms)
                || task.as_ref().is_some_and(|dispatch| {
                    validate_positive_safe_integer(dispatch.id).is_err()
                        || validate_positive_safe_integer(dispatch.expected_version).is_err()
                })
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ServerMessage::CancelWork {
            request_id, task, ..
        } => {
            if !is_request_id(request_id)
                || task
                    .as_ref()
                    .is_some_and(|fence| validate_positive_safe_integer(fence.task_id).is_err())
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ServerMessage::WorkIdleAck {
            work_request_id, ..
        } if !is_request_id(work_request_id) => {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ServerMessage::TaskExecutionStoppedAck { task_id, .. }
            if validate_positive_safe_integer(*task_id).is_err() =>
        {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ServerMessage::TaskAttemptChanged {
            task_id,
            attempt,
            current,
            stop_pending,
            ..
        } if validate_positive_safe_integer(*task_id).is_err()
            || attempt.as_ref().is_some_and(|attempt| {
                attempt.task_id != *task_id
                    || !valid_actor_id(&attempt.agent_id)
                    || validate_positive_safe_integer(attempt.task_id).is_err()
            })
            || current
                .as_ref()
                .into_iter()
                .chain(stop_pending)
                .any(|fence| validate_positive_safe_integer(fence.task_id).is_err()) =>
        {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ServerMessage::CredentialIssued { credential, .. } => {
            credential
                .validate()
                .map_err(|_| RouterErrorCode::InvalidMessage)?;
        }
        ServerMessage::Credentials { credentials, .. }
            if credentials.iter().any(|credential| {
                credential.created_at < 0
                    || credential
                        .revoked_at
                        .is_some_and(|value| value < credential.created_at)
            }) =>
        {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ServerMessage::Result {
            ok,
            content,
            error,
            task_id,
            from,
            ..
        } => {
            let valid_shape = if *ok {
                content.as_deref().is_some_and(valid_shared_content) && error.is_none()
            } else {
                content.is_none() && error.is_some()
            };
            if !valid_shape
                || !valid_actor_id(from)
                || task_id.is_some_and(|value| validate_positive_safe_integer(value).is_err())
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ServerMessage::WorkspaceHistory { page, .. } => {
            validate_event_page(&page.events, page.next_cursor)?;
        }
        ServerMessage::WorkspaceSubscription {
            events,
            next_cursor,
            ..
        } => validate_event_page(events, *next_cursor)?,
        ServerMessage::WorkspaceEvent { event } => validate_workspace_event(event)?,
        ServerMessage::WorkspaceJoined { cursor, .. }
        | ServerMessage::WorkspaceChanged { cursor, .. }
            if !(0..=MAX_SAFE_INTEGER).contains(cursor) =>
        {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ServerMessage::WorkspacePosted { seq, .. }
            if validate_positive_safe_integer(*seq).is_err() =>
        {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ServerMessage::Tasks {
            tasks, next_cursor, ..
        } => {
            if !(0..=MAX_SAFE_INTEGER).contains(next_cursor)
                || tasks.iter().any(|task| !valid_task_summary(task))
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
        }
        ServerMessage::Task { task, .. }
        | ServerMessage::TaskMutated {
            result: TaskMutationResult { task, .. },
            ..
        } if !valid_task_summary(&task.summary) => {
            return Err(RouterErrorCode::InvalidMessage);
        }
        ServerMessage::TaskHistory { page, .. } => {
            if validate_positive_safe_integer(page.task_id).is_err()
                || !(0..=MAX_SAFE_INTEGER).contains(&page.next_cursor)
            {
                return Err(RouterErrorCode::InvalidMessage);
            }
            let mut previous = 0;
            for event in &page.events {
                if validate_positive_safe_integer(event.seq).is_err()
                    || event.seq <= previous
                    || event.seq > page.next_cursor
                    || event.created_at < 0
                    || !valid_actor_id(&event.actor_id)
                    || event.event.task.id != page.task_id
                    || event.event.task.workspace != page.workspace.as_str()
                    || !valid_task_summary(&event.event.task)
                {
                    return Err(RouterErrorCode::InvalidMessage);
                }
                previous = event.seq;
            }
        }
        ServerMessage::Error {
            request_id,
            current_version,
            ..
        } if request_id
            .as_deref()
            .is_some_and(|value| !is_request_id(value))
            || current_version
                .is_some_and(|value| validate_positive_safe_integer(value).is_err()) =>
        {
            return Err(RouterErrorCode::InvalidMessage);
        }
        _ => {}
    }
    Ok(())
}

fn valid_descriptor(agent: &AgentDescriptor) -> bool {
    is_agent_id(&agent.agent_id)
        && valid_agent_client(agent.side, agent.client)
        && agent.activity.as_deref().is_none_or(|value| {
            value.len() <= MAX_ERROR_BYTES && value.chars().all(valid_text_scalar)
        })
}

fn valid_actor_id(value: &str) -> bool {
    (value.starts_with("operator:") || value.starts_with("system:") || is_agent_id(value))
        && !value.is_empty()
        && value.len() <= MAX_AGENT_ID_BYTES
        && value.is_ascii()
}

fn valid_task_summary(task: &TaskSummary) -> bool {
    validate_positive_safe_integer(task.id).is_ok()
        && is_workspace_name(&task.workspace)
        && validate_title(&task.title).is_ok()
        && validate_positive_safe_integer(task.version).is_ok()
        && task.assigned_agent_id.as_deref().is_none_or(is_agent_id)
        && task.created_at >= 0
        && task.updated_at >= 0
}

fn validate_event_page(events: &[WorkspaceEvent], next_cursor: i64) -> Result<(), RouterErrorCode> {
    if !(0..=MAX_SAFE_INTEGER).contains(&next_cursor) {
        return Err(RouterErrorCode::InvalidMessage);
    }
    for event in events {
        validate_workspace_event(event)?;
    }
    Ok(())
}

fn validate_workspace_event(event: &WorkspaceEvent) -> Result<(), RouterErrorCode> {
    if validate_positive_safe_integer(event.seq).is_err()
        || event.created_at < 0
        || event.created_at > MAX_SAFE_INTEGER
        || !valid_actor_id(&event.actor_id)
        || event
            .request_id
            .as_deref()
            .is_some_and(|value| !is_request_id(value))
        || event
            .task_id
            .is_some_and(|value| validate_positive_safe_integer(value).is_err())
        || event.content.as_ref().is_some_and(|value| {
            value.len() > MAX_SHARED_CONTENT_BYTES
                || serde_json::to_vec(value)
                    .is_ok_and(|encoded| encoded.len() > MAX_JSON_STRING_BYTES)
        })
    {
        return Err(RouterErrorCode::InvalidMessage);
    }
    Ok(())
}

fn validate_server_unknown_fields(
    message_type: &str,
    object: &Map<String, Value>,
) -> Result<(), RouterErrorCode> {
    let allowed: &[&str] = match message_type {
        "registered" => &[
            "type",
            "protocolVersion",
            "agent",
            "role",
            "workspace",
            "cursor",
        ],
        "registered_operator" => &[
            "type",
            "protocolVersion",
            "subject",
            "admin",
            "workspace",
            "cursor",
        ],
        "pong" | "router_stopping" => &["type", "requestId"],
        "error" => &[
            "type",
            "requestId",
            "code",
            "workspace",
            "operationId",
            "currentVersion",
        ],
        "agents" => &["type", "requestId", "workspace", "agents"],
        "accepted" => &["type", "requestId", "workspace", "to"],
        "deliver" => &[
            "type",
            "workspace",
            "requestId",
            "from",
            "content",
            "timeoutMs",
            "task",
        ],
        "cancel_work" => &["type", "workspace", "requestId", "reason", "task"],
        "work_idle_ack" => &["type", "requestId", "workspace", "workRequestId"],
        "task_execution_stopped_ack" => &["type", "requestId", "workspace", "taskId", "attemptId"],
        "task_attempt_changed" => &[
            "type",
            "workspace",
            "taskId",
            "attempt",
            "closedAttemptId",
            "current",
            "stopPending",
        ],
        "credential_issued" => &["type", "requestId", "credential"],
        "credentials" => &["type", "requestId", "credentials", "nextCursor", "hasMore"],
        "credential_revoked" => &["type", "requestId", "id"],
        "result" => &[
            "type",
            "workspace",
            "requestId",
            "from",
            "ok",
            "content",
            "error",
            "taskId",
        ],
        "workspace_created" | "workspace_left" | "workspace_unsubscribed" => {
            &["type", "requestId", "workspace"]
        }
        "workspaces" => &["type", "requestId", "workspaces", "nextCursor", "hasMore"],
        "integrations" => &["type", "requestId", "workspace", "integrations"],
        "integration_checked" => &["type", "requestId", "workspace", "integration"],
        "integrations_reloaded" => &["type", "requestId", "integrations"],
        "external_operation" => &["type", "requestId", "workspace", "operation"],
        "external_status" | "external_resolved" => {
            &["type", "requestId", "workspace", "operation", "resolution"]
        }
        "workspace_joined" => &["type", "requestId", "workspace", "cursor"],
        "workspace_changed" => &["type", "workspace", "cursor"],
        "workspace_posted" => &["type", "requestId", "workspace", "seq"],
        "workspace_history" => &[
            "type",
            "requestId",
            "workspace",
            "events",
            "nextCursor",
            "hasMore",
        ],
        "workspace_subscription" => &[
            "type",
            "requestId",
            "workspace",
            "events",
            "nextCursor",
            "live",
        ],
        "workspace_event" => &["type", "event"],
        "tasks" => &[
            "type",
            "requestId",
            "workspace",
            "tasks",
            "nextCursor",
            "hasMore",
        ],
        "task" => &["type", "requestId", "workspace", "task"],
        "task_history" => &[
            "type",
            "requestId",
            "workspace",
            "taskId",
            "events",
            "nextCursor",
            "hasMore",
        ],
        "task_mutated" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "appliedVersion",
            "reportId",
            "task",
        ],
        _ => return Err(RouterErrorCode::InvalidMessage),
    };
    if object.keys().all(|key| allowed.contains(&key.as_str())) {
        Ok(())
    } else {
        Err(RouterErrorCode::InvalidMessage)
    }
}

fn validate_unknown_fields(
    message_type: &str,
    object: &Map<String, Value>,
) -> Result<(), RouterErrorCode> {
    let common: &[&str] = match message_type {
        "register" => &[
            "type",
            "protocolVersion",
            "agent",
            "token",
            "delegationToken",
        ],
        "register_delegate" => &["type", "protocolVersion", "ownerId", "delegationToken"],
        "register_operator" => &["type", "protocolVersion", "token"],
        "ping"
        | "list"
        | "workspace_leave"
        | "workspace_members"
        | "workspace_unsubscribe"
        | "router_shutdown"
        | "integration_reload" => &["type", "requestId"],
        "readiness" => &["type", "ready"],
        "send" => &["type", "requestId", "to", "content", "timeoutMs"],
        "reply" => &["type", "requestId", "ok", "content", "error"],
        "work_idle" => &[
            "type",
            "requestId",
            "workspace",
            "workRequestId",
            "task",
            "ready",
        ],
        "task_execution_stopped" => &[
            "type",
            "requestId",
            "workspace",
            "taskId",
            "attemptId",
            "endedSessionId",
            "evidence",
            "reason",
        ],
        "credential_issue" => &[
            "type",
            "requestId",
            "role",
            "subject",
            "agentSide",
            "agentClient",
            "workspaces",
        ],
        "credential_revoke" => &["type", "requestId", "id"],
        "workspace_create" | "workspace_join" => &["type", "requestId", "name"],
        "workspace_list" | "workspace_history" | "credential_list" => {
            &["type", "requestId", "after", "limit"]
        }
        "workspace_post" => &["type", "requestId", "content"],
        "workspace_subscribe" => &["type", "requestId", "after"],
        "task_list" => &[
            "type",
            "requestId",
            "workspace",
            "states",
            "assignedAgentId",
            "after",
            "limit",
        ],
        "task_get" => &["type", "requestId", "workspace", "taskId"],
        "task_history" => &["type", "requestId", "workspace", "taskId", "after", "limit"],
        "task_create" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "title",
            "description",
        ],
        "task_edit" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "expectedVersion",
            "title",
            "description",
        ],
        "task_assign" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "expectedVersion",
            "agentId",
        ],
        "task_note" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "text",
        ],
        "task_begin" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "workRequestId",
            "expectedVersion",
            "lastCheckpointId",
            "resumeNote",
        ],
        "task_checkpoint" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "attemptId",
            "expectedVersion",
            "checkpoint",
        ],
        "task_pause" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "attemptId",
            "expectedVersion",
            "checkpoint",
            "reason",
        ],
        "task_complete" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "attemptId",
            "expectedVersion",
            "result",
        ],
        "task_cancel" | "task_reopen" | "task_interrupt" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "expectedVersion",
            "note",
        ],
        "task_confirm_stopped" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "attemptId",
            "expectedVersion",
            "note",
        ],
        "task_request" => &[
            "type",
            "requestId",
            "workspace",
            "taskId",
            "expectedVersion",
            "message",
            "timeoutMs",
        ],
        "task_import" => &[
            "type",
            "requestId",
            "workspace",
            "provider",
            "externalId",
            "operationId",
        ],
        "task_link" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "expectedVersion",
            "provider",
            "externalId",
            "replace",
        ],
        "task_publish" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "taskId",
            "expectedVersion",
            "provider",
            "kind",
            "reportId",
        ],
        "task_external_status" => &["type", "requestId", "workspace", "operationId"],
        "task_external_resolve" => &[
            "type",
            "requestId",
            "workspace",
            "operationId",
            "resolutionId",
            "outcome",
            "externalId",
            "note",
        ],
        "integration_check" => &["type", "requestId", "workspace", "provider"],
        "integration_list" => &["type", "requestId", "workspace"],
        _ => return Err(RouterErrorCode::InvalidMessage),
    };
    let allowed: HashSet<&str> = common.iter().copied().collect();
    if object.keys().all(|key| allowed.contains(key.as_str())) {
        Ok(())
    } else {
        Err(RouterErrorCode::InvalidMessage)
    }
}
