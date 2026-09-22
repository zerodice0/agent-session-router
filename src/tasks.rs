use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
pub const MAX_TITLE_BYTES: usize = 1024;
pub const MAX_TITLE_SCALARS: usize = 256;
pub const MAX_DESCRIPTION_BYTES: usize = 64 * 1024;
pub const MAX_NOTE_BYTES: usize = 16 * 1024;
pub const MAX_HANDOFF_NOTE_BYTES: usize = 4 * 1024;
pub const MAX_CHECKPOINT_TEXT_BYTES: usize = 8 * 1024;
pub const MAX_CHECKPOINT_BYTES: usize = 32 * 1024;
pub const MAX_ARTIFACTS: usize = 32;
pub const MAX_ARTIFACT_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Todo,
    InProgress,
    Blocked,
    Paused,
    Done,
    Cancelled,
}

impl TaskState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Cancelled)
    }

    #[must_use]
    pub const fn is_beginable(self) -> bool {
        matches!(self, Self::Todo | Self::Paused | Self::Blocked)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Todo => "todo",
            Self::InProgress => "in_progress",
            Self::Blocked => "blocked",
            Self::Paused => "paused",
            Self::Done => "done",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Running,
    Released,
    Completed,
    Interrupted,
}

impl AttemptStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Released => "released",
            Self::Completed => "completed",
            Self::Interrupted => "interrupted",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StopEvidence {
    Released,
    Confirmed,
    Unknown,
}

impl StopEvidence {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Released => "released",
            Self::Confirmed => "confirmed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    Paused,
    Blocked,
    TransportLost,
    CredentialRevoked,
    SessionEnded,
    RouterRestarted,
    RequestTimeout,
    RequestCancelled,
    TurnEnded,
    HostError,
    OperatorInterrupt,
    ReplyWithoutRelease,
}

impl PauseReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Paused => "paused",
            Self::Blocked => "blocked",
            Self::TransportLost => "transport_lost",
            Self::CredentialRevoked => "credential_revoked",
            Self::SessionEnded => "session_ended",
            Self::RouterRestarted => "router_restarted",
            Self::RequestTimeout => "request_timeout",
            Self::RequestCancelled => "request_cancelled",
            Self::TurnEnded => "turn_ended",
            Self::HostError => "host_error",
            Self::OperatorInterrupt => "operator_interrupt",
            Self::ReplyWithoutRelease => "reply_without_release",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportKind {
    Note,
    Checkpoint,
    Result,
}

impl ReportKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Note => "note",
            Self::Checkpoint => "checkpoint",
            Self::Result => "result",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskCheckpoint {
    pub summary: String,
    pub next_steps: String,
    pub artifacts: Vec<String>,
    pub risks: String,
}

impl TaskCheckpoint {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_nonempty_text(&self.summary, MAX_CHECKPOINT_TEXT_BYTES)?;
        validate_text(&self.next_steps, MAX_CHECKPOINT_TEXT_BYTES)?;
        validate_text(&self.risks, MAX_CHECKPOINT_TEXT_BYTES)?;
        if self.artifacts.len() > MAX_ARTIFACTS {
            return Err(ValidationError::InvalidCheckpoint);
        }
        for artifact in &self.artifacts {
            validate_nonempty_text(artifact, MAX_ARTIFACT_BYTES)?;
        }
        let encoded = serde_json::to_vec(self).map_err(|_| ValidationError::InvalidCheckpoint)?;
        if encoded.len() > MAX_CHECKPOINT_BYTES {
            return Err(ValidationError::InvalidCheckpoint);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskSummary {
    pub id: i64,
    pub workspace: String,
    pub title: String,
    pub state: TaskState,
    pub version: i64,
    pub assigned_agent_id: Option<String>,
    pub current_attempt_id: Option<Uuid>,
    pub last_executor_id: Option<String>,
    pub execution_session_id: Option<Uuid>,
    pub last_checkpoint_at: Option<i64>,
    pub pause_reason: Option<PauseReason>,
    pub stop_evidence: Option<StopEvidence>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskAttempt {
    pub id: Uuid,
    pub task_id: i64,
    pub agent_id: String,
    pub session_id: Uuid,
    pub work_request_id: String,
    pub resumed_from_checkpoint_id: Option<Uuid>,
    pub status: AttemptStatus,
    pub stop_evidence: StopEvidence,
    pub reason: Option<PauseReason>,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub stopped_at: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ReportBody {
    Checkpoint(TaskCheckpoint),
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskReportRecord {
    pub id: Uuid,
    pub task_id: i64,
    pub attempt_id: Option<Uuid>,
    pub actor_id: String,
    pub kind: ReportKind,
    pub body: ReportBody,
    pub created_at: i64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExternalProvider {
    Github,
    Linear,
}

impl ExternalProvider {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Github => "github",
            Self::Linear => "linear",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalLink {
    pub provider: ExternalProvider,
    pub namespace: String,
    pub external_id: String,
    pub url: String,
    pub linked_at: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExternalOperationKind {
    Import,
    Link,
    PublishIssue,
    PublishReport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExternalOperationStatus {
    Running,
    Succeeded,
    Failed,
    Unconfirmed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalOperationSummary {
    pub id: Uuid,
    pub task_id: Option<i64>,
    pub provider: ExternalProvider,
    pub kind: ExternalOperationKind,
    pub status: ExternalOperationStatus,
    pub source_version: Option<i64>,
    pub external_id: Option<String>,
    pub url: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExternalPublishKind {
    Issue,
    Report,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExternalResolutionOutcome {
    Applied,
    NotApplied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalResolution {
    pub id: Uuid,
    pub operation_id: Uuid,
    pub actor_id: String,
    pub outcome: ExternalResolutionOutcome,
    pub external_id: Option<String>,
    pub note: String,
    pub created_at: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationChange {
    Configured,
    Disabled,
    Operation,
    Resolved,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IntegrationEvent {
    pub change: IntegrationChange,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration: Option<crate::integrations::IntegrationPublic>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<ExternalOperationSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution: Option<ExternalResolution>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskDetail {
    #[serde(flatten)]
    pub summary: TaskSummary,
    pub description: String,
    pub created_by: String,
    pub updated_by: String,
    pub current_attempt: Option<TaskAttempt>,
    pub last_attempt: Option<TaskAttempt>,
    pub checkpoint: Option<TaskReportRecord>,
    pub result: Option<TaskReportRecord>,
    pub links: Vec<ExternalLink>,
    pub external_operations: Vec<ExternalOperationSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskMutationResult {
    pub operation_id: Uuid,
    pub applied_version: i64,
    pub report_id: Option<Uuid>,
    pub task: TaskDetail,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskEvent {
    pub change: TaskChange,
    pub task: TaskSummary,
    pub attempt_id: Option<Uuid>,
    pub report_id: Option<Uuid>,
    pub external_operation_id: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskChange {
    Created,
    Edited,
    Assigned,
    Begun,
    Checkpoint,
    Noted,
    Paused,
    Completed,
    Interrupted,
    Stopped,
    Cancelled,
    Reopened,
    External,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationError {
    Empty,
    TooLarge,
    InvalidCheckpoint,
    InvalidIdentifier,
}

pub fn validate_title(title: &str) -> Result<(), ValidationError> {
    validate_nonempty_text(title, MAX_TITLE_BYTES)?;
    if title.chars().count() > MAX_TITLE_SCALARS {
        return Err(ValidationError::TooLarge);
    }
    Ok(())
}

pub fn validate_description(description: &str) -> Result<(), ValidationError> {
    validate_text(description, MAX_DESCRIPTION_BYTES)
}

pub fn validate_note(note: &str) -> Result<(), ValidationError> {
    validate_nonempty_text(note, MAX_NOTE_BYTES)
}

pub fn validate_handoff_note(note: &str) -> Result<(), ValidationError> {
    validate_nonempty_text(note, MAX_HANDOFF_NOTE_BYTES)
}

pub fn validate_positive_safe_integer(value: i64) -> Result<(), ValidationError> {
    if (1..=MAX_SAFE_INTEGER).contains(&value) {
        Ok(())
    } else {
        Err(ValidationError::InvalidIdentifier)
    }
}

fn validate_nonempty_text(value: &str, max_bytes: usize) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::Empty);
    }
    validate_text(value, max_bytes)
}

fn validate_text(value: &str, max_bytes: usize) -> Result<(), ValidationError> {
    if value.len() > max_bytes {
        Err(ValidationError::TooLarge)
    } else {
        Ok(())
    }
}

use crate::{
    protocol::{RouterErrorCode, TaskFence, WorkspaceEvent, WorkspaceEventKind, WorkspaceName},
    store::{EventInsert, RouterStore, StoreError, append_event_in_transaction, now_millis},
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallerRole {
    Agent,
    Delegate,
    Operator,
    Admin,
}

#[derive(Clone, Debug)]
pub struct CallerContext {
    pub actor_id: String,
    pub role: CallerRole,
    pub workspace: WorkspaceName,
    pub agent_id: Option<String>,
    pub credential_id: Uuid,
    pub session_id: Option<Uuid>,
    pub connection_generation: i64,
    pub reservation: Option<ReservationFence>,
}

#[derive(Clone, Debug)]
pub struct ReservationFence {
    pub work_request_id: String,
    pub task_id: i64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum TaskCommand {
    Create {
        operation_id: Uuid,
        title: String,
        description: String,
    },
    Edit {
        operation_id: Uuid,
        task_id: i64,
        expected_version: i64,
        title: Option<String>,
        description: Option<String>,
    },
    Assign {
        operation_id: Uuid,
        task_id: i64,
        expected_version: i64,
        agent_id: Option<String>,
    },
    Note {
        operation_id: Uuid,
        task_id: i64,
        text: String,
    },
    Begin {
        operation_id: Uuid,
        task_id: i64,
        work_request_id: String,
        expected_version: i64,
        last_checkpoint_id: Option<Uuid>,
        resume_note: String,
    },
    Checkpoint {
        operation_id: Uuid,
        task_id: i64,
        attempt_id: Uuid,
        expected_version: i64,
        checkpoint: TaskCheckpoint,
    },
    Pause {
        operation_id: Uuid,
        task_id: i64,
        attempt_id: Uuid,
        expected_version: i64,
        checkpoint: TaskCheckpoint,
        blocked: bool,
    },
    Complete {
        operation_id: Uuid,
        task_id: i64,
        attempt_id: Uuid,
        expected_version: i64,
        result: TaskCheckpoint,
    },
    Cancel {
        operation_id: Uuid,
        task_id: i64,
        expected_version: i64,
        note: String,
    },
    Reopen {
        operation_id: Uuid,
        task_id: i64,
        expected_version: i64,
        note: String,
    },
    Interrupt {
        operation_id: Uuid,
        task_id: i64,
        expected_version: i64,
        note: String,
    },
    ConfirmStopped {
        operation_id: Uuid,
        task_id: i64,
        attempt_id: Uuid,
        expected_version: i64,
        note: String,
    },
}

impl TaskCommand {
    #[must_use]
    pub const fn operation_id(&self) -> Uuid {
        match self {
            Self::Create { operation_id, .. }
            | Self::Edit { operation_id, .. }
            | Self::Assign { operation_id, .. }
            | Self::Note { operation_id, .. }
            | Self::Begin { operation_id, .. }
            | Self::Checkpoint { operation_id, .. }
            | Self::Pause { operation_id, .. }
            | Self::Complete { operation_id, .. }
            | Self::Cancel { operation_id, .. }
            | Self::Reopen { operation_id, .. }
            | Self::Interrupt { operation_id, .. }
            | Self::ConfirmStopped { operation_id, .. } => *operation_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TaskApplyResult {
    pub mutation: TaskMutationResult,
    pub event: WorkspaceEvent,
    pub closed_attempt_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub struct TaskError {
    pub code: RouterErrorCode,
    pub current_version: Option<i64>,
    pub operation_id: Option<Uuid>,
}

impl TaskError {
    fn code(code: RouterErrorCode, operation_id: Option<Uuid>) -> Self {
        Self {
            code,
            current_version: None,
            operation_id,
        }
    }

    fn conflict(version: i64, operation_id: Uuid) -> Self {
        Self {
            code: RouterErrorCode::TaskConflict,
            current_version: Some(version),
            operation_id: Some(operation_id),
        }
    }
}

impl From<StoreError> for TaskError {
    fn from(value: StoreError) -> Self {
        Self::code(value.into(), None)
    }
}

pub fn apply_task(
    store: &mut RouterStore,
    caller: &CallerContext,
    command: &TaskCommand,
) -> Result<TaskApplyResult, TaskError> {
    validate_command(caller, command)?;
    let operation_id = command.operation_id();
    let request_hash: [u8; 32] = Sha256::digest(
        serde_json::to_vec(command)
            .map_err(|_| TaskError::code(RouterErrorCode::InvalidMessage, Some(operation_id)))?,
    )
    .into();
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    if let Some(receipt) =
        mutation_receipt(&transaction, &caller.workspace, operation_id).map_err(TaskError::from)?
    {
        if receipt.actor_id != caller.actor_id || receipt.request_hash != request_hash {
            return Err(TaskError::code(
                RouterErrorCode::RequestConflict,
                Some(operation_id),
            ));
        }
        if let TaskCommand::Begin { .. } = command {
            let active = receipt.attempt_id.is_some_and(|attempt_id| {
                exact_active_owner_attempt(&transaction, caller, attempt_id).unwrap_or(false)
            });
            if !active {
                return Err(TaskError::code(
                    RouterErrorCode::TaskStaleAttempt,
                    Some(operation_id),
                ));
            }
        }
        let task = get_task_detail_tx(&transaction, &caller.workspace, receipt.task_id)
            .map_err(TaskError::from)?
            .ok_or_else(|| TaskError::code(RouterErrorCode::TaskNotFound, Some(operation_id)))?;
        let event = last_task_event(&transaction, &caller.workspace, receipt.task_id)
            .map_err(TaskError::from)?
            .ok_or_else(|| TaskError::code(RouterErrorCode::StorageError, Some(operation_id)))?;
        transaction
            .commit()
            .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
        return Ok(TaskApplyResult {
            mutation: TaskMutationResult {
                operation_id,
                applied_version: receipt.applied_version,
                report_id: receipt.report_id,
                task,
            },
            event,
            closed_attempt_id: None,
        });
    }

    let applied = match command {
        TaskCommand::Create {
            title, description, ..
        } => apply_create(
            &transaction,
            caller,
            operation_id,
            title,
            description,
            request_hash,
        ),
        TaskCommand::Edit {
            task_id,
            expected_version,
            title,
            description,
            ..
        } => apply_edit(
            &transaction,
            caller,
            operation_id,
            *task_id,
            *expected_version,
            title.as_deref(),
            description.as_deref(),
            request_hash,
        ),
        TaskCommand::Assign {
            task_id,
            expected_version,
            agent_id,
            ..
        } => apply_assign(
            &transaction,
            caller,
            operation_id,
            *task_id,
            *expected_version,
            agent_id.as_deref(),
            request_hash,
        ),
        TaskCommand::Note { task_id, text, .. } => apply_note(
            &transaction,
            caller,
            operation_id,
            *task_id,
            text,
            request_hash,
        ),
        TaskCommand::Begin {
            task_id,
            work_request_id,
            expected_version,
            last_checkpoint_id,
            resume_note,
            ..
        } => apply_begin(
            &transaction,
            caller,
            operation_id,
            *task_id,
            work_request_id,
            *expected_version,
            *last_checkpoint_id,
            resume_note,
            request_hash,
        ),
        TaskCommand::Checkpoint {
            task_id,
            attempt_id,
            expected_version,
            checkpoint,
            ..
        } => apply_checkpoint(
            &transaction,
            caller,
            operation_id,
            *task_id,
            *attempt_id,
            *expected_version,
            checkpoint,
            request_hash,
        ),
        TaskCommand::Pause {
            task_id,
            attempt_id,
            expected_version,
            checkpoint,
            blocked,
            ..
        } => apply_release(
            &transaction,
            caller,
            operation_id,
            *task_id,
            *attempt_id,
            *expected_version,
            checkpoint,
            if *blocked {
                TaskState::Blocked
            } else {
                TaskState::Paused
            },
            false,
            request_hash,
        ),
        TaskCommand::Complete {
            task_id,
            attempt_id,
            expected_version,
            result,
            ..
        } => apply_release(
            &transaction,
            caller,
            operation_id,
            *task_id,
            *attempt_id,
            *expected_version,
            result,
            TaskState::Done,
            true,
            request_hash,
        ),
        TaskCommand::Cancel {
            task_id,
            expected_version,
            note,
            ..
        } => apply_terminal(
            &transaction,
            caller,
            operation_id,
            *task_id,
            *expected_version,
            note,
            TaskState::Cancelled,
            request_hash,
        ),
        TaskCommand::Reopen {
            task_id,
            expected_version,
            note,
            ..
        } => apply_terminal(
            &transaction,
            caller,
            operation_id,
            *task_id,
            *expected_version,
            note,
            TaskState::Todo,
            request_hash,
        ),
        TaskCommand::Interrupt {
            task_id,
            expected_version,
            note,
            ..
        } => apply_interrupt(
            &transaction,
            caller,
            operation_id,
            *task_id,
            *expected_version,
            note,
            request_hash,
        ),
        TaskCommand::ConfirmStopped {
            task_id,
            attempt_id,
            expected_version,
            note,
            ..
        } => apply_confirm_stopped(
            &transaction,
            caller,
            operation_id,
            *task_id,
            *attempt_id,
            *expected_version,
            note,
            request_hash,
        ),
    }?;
    transaction
        .commit()
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    Ok(applied)
}

pub fn interrupt_attempt(
    store: &mut RouterStore,
    attempt_id: Uuid,
    reason: PauseReason,
) -> Result<Option<WorkspaceEvent>, StoreError> {
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(StoreError::Sqlite)?;
    let row = transaction
        .query_row(
            "SELECT workspace,task_id,status FROM task_attempts WHERE id=?1",
            [attempt_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let Some((workspace, task_id, status)) = row else {
        return Ok(None);
    };
    if status != "running" {
        transaction.commit().map_err(StoreError::Sqlite)?;
        return Ok(None);
    }
    let workspace = WorkspaceName::parse(workspace).map_err(|_| StoreError::InvalidData)?;
    let now = now_millis()?;
    transaction
        .execute(
            "UPDATE task_attempts SET status='interrupted',stop_evidence='unknown',reason=?2,ended_at=?3 WHERE id=?1 AND status='running'",
            params![attempt_id.to_string(), reason.as_str(), now],
        )
        .map_err(StoreError::Sqlite)?;
    let changed = transaction
        .execute(
            "UPDATE tasks SET state='paused',version=version+1,current_attempt_id=NULL,last_attempt_id=?3,pause_reason=?4,updated_by='system:router',updated_at=?5 WHERE workspace=?1 AND id=?2 AND current_attempt_id=?3",
            params![
                workspace.as_str(),
                task_id,
                attempt_id.to_string(),
                reason.as_str(),
                now,
            ],
        )
        .map_err(StoreError::Sqlite)?;
    if changed != 1 {
        return Err(StoreError::InvalidData);
    }
    let task = get_task_detail_connection(&transaction, &workspace, task_id)?
        .ok_or(StoreError::InvalidData)?;
    let content = serde_json::to_string(&TaskEvent {
        change: TaskChange::Interrupted,
        task: task.summary,
        attempt_id: Some(attempt_id),
        report_id: None,
        external_operation_id: None,
    })
    .map_err(|_| StoreError::InvalidData)?;
    let event = append_event_in_transaction(
        &transaction,
        &EventInsert {
            workspace: &workspace,
            kind: WorkspaceEventKind::Task,
            actor_id: "system:router",
            request_id: None,
            target_id: None,
            task_id: Some(task_id),
            content: Some(&content),
            ok: None,
            error: None,
        },
    )?;
    transaction.commit().map_err(StoreError::Sqlite)?;
    Ok(Some(event))
}

pub fn interrupt_session_attempts(
    store: &mut RouterStore,
    agent_id: &str,
    credential_id: Uuid,
    session_id: Uuid,
    connection_generation: i64,
    reason: PauseReason,
) -> Result<Vec<WorkspaceEvent>, StoreError> {
    let attempts = {
        let mut statement = store
            .connection()
            .prepare_cached(
                "SELECT id FROM task_attempts WHERE agent_id=?1 AND credential_id=?2 AND session_id=?3 AND connection_generation=?4 AND status='running'",
            )
            .map_err(StoreError::Sqlite)?;
        statement
            .query_map(
                params![
                    agent_id,
                    credential_id.to_string(),
                    session_id.to_string(),
                    connection_generation,
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(StoreError::Sqlite)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::Sqlite)?
    };
    let mut events = Vec::with_capacity(attempts.len());
    for attempt_id in attempts {
        let attempt_id = Uuid::parse_str(&attempt_id).map_err(|_| StoreError::InvalidData)?;
        if let Some(event) = interrupt_attempt(store, attempt_id, reason)? {
            events.push(event);
        }
    }
    Ok(events)
}

#[allow(clippy::too_many_arguments)]
pub fn record_execution_stopped(
    store: &mut RouterStore,
    workspace: &WorkspaceName,
    task_id: i64,
    attempt_id: Uuid,
    ended_session_id: Uuid,
    agent_id: &str,
    credential_id: Uuid,
    reason: PauseReason,
) -> Result<Option<WorkspaceEvent>, TaskError> {
    let transaction = store
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    let attempt = transaction
        .query_row(
            "SELECT agent_id,credential_id,session_id,status,stop_evidence FROM task_attempts WHERE id=?1 AND workspace=?2 AND task_id=?3",
            params![attempt_id.to_string(), workspace.as_str(), task_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    let Some((stored_agent, stored_credential, stored_session, status, stop_evidence)) = attempt
    else {
        return Err(TaskError::code(RouterErrorCode::TaskStaleAttempt, None));
    };
    if stored_agent != agent_id
        || stored_credential != credential_id.to_string()
        || stored_session != ended_session_id.to_string()
    {
        return Err(TaskError::code(RouterErrorCode::TaskStaleAttempt, None));
    }
    if matches!(status.as_str(), "released" | "completed") || stop_evidence == "confirmed" {
        transaction
            .commit()
            .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
        return Ok(None);
    }
    if status != "running" && !(status == "interrupted" && stop_evidence == "unknown") {
        return Err(TaskError::code(RouterErrorCode::TaskStaleAttempt, None));
    }

    let now = now_millis().map_err(TaskError::from)?;
    let changed = if status == "running" {
        transaction
            .execute(
                "UPDATE task_attempts SET status='interrupted',stop_evidence='confirmed',reason=?2,ended_at=?3,stopped_at=?3 WHERE id=?1 AND status='running'",
                params![attempt_id.to_string(), reason.as_str(), now],
            )
            .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
        transaction
            .execute(
                "UPDATE tasks SET state='paused',version=version+1,current_attempt_id=NULL,last_attempt_id=?3,pause_reason=?4,updated_by=?5,updated_at=?6 WHERE workspace=?1 AND id=?2 AND current_attempt_id=?3",
                params![
                    workspace.as_str(),
                    task_id,
                    attempt_id.to_string(),
                    reason.as_str(),
                    agent_id,
                    now,
                ],
            )
            .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?
    } else {
        transaction
            .execute(
                "UPDATE task_attempts SET stop_evidence='confirmed',stopped_at=?2 WHERE id=?1 AND status='interrupted' AND stop_evidence='unknown'",
                params![attempt_id.to_string(), now],
            )
            .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
        transaction
            .execute(
                "UPDATE tasks SET version=version+1,updated_by=?4,updated_at=?5 WHERE workspace=?1 AND id=?2 AND current_attempt_id IS NULL AND last_attempt_id=?3",
                params![
                    workspace.as_str(),
                    task_id,
                    attempt_id.to_string(),
                    agent_id,
                    now,
                ],
            )
            .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?
    };
    if changed != 1 {
        return Err(TaskError::from(StoreError::InvalidData));
    }
    let task = get_task_detail_connection(&transaction, workspace, task_id)
        .map_err(TaskError::from)?
        .ok_or_else(|| TaskError::code(RouterErrorCode::TaskNotFound, None))?;
    let content = serde_json::to_string(&TaskEvent {
        change: TaskChange::Stopped,
        task: task.summary,
        attempt_id: Some(attempt_id),
        report_id: None,
        external_operation_id: None,
    })
    .map_err(|_| TaskError::code(RouterErrorCode::StorageError, None))?;
    let event = append_event_in_transaction(
        &transaction,
        &EventInsert {
            workspace,
            kind: WorkspaceEventKind::Task,
            actor_id: agent_id,
            request_id: None,
            target_id: None,
            task_id: Some(task_id),
            content: Some(&content),
            ok: None,
            error: None,
        },
    )
    .map_err(TaskError::from)?;
    transaction
        .commit()
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    Ok(Some(event))
}

pub fn attempt_agent_id(
    store: &RouterStore,
    attempt_id: Uuid,
) -> Result<Option<String>, StoreError> {
    store
        .connection()
        .query_row(
            "SELECT agent_id FROM task_attempts WHERE id=?1",
            [attempt_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)
}
pub fn execution_attempt(
    store: &RouterStore,
    attempt_id: Uuid,
) -> Result<Option<(WorkspaceName, TaskAttempt)>, StoreError> {
    let workspace = store
        .connection()
        .query_row(
            "SELECT workspace FROM task_attempts WHERE id=?1",
            [attempt_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let Some(workspace) = workspace else {
        return Ok(None);
    };
    let workspace = WorkspaceName::parse(workspace).map_err(|_| StoreError::InvalidData)?;
    let attempt = load_attempt(store.connection(), &attempt_id.to_string())?;
    Ok(Some((workspace, attempt)))
}

pub fn agent_execution_fences(
    store: &RouterStore,
    agent_id: &str,
) -> Result<(Option<TaskFence>, Option<TaskFence>), StoreError> {
    let mut statement = store
        .connection()
        .prepare_cached(
            "SELECT task_id,id,status FROM task_attempts WHERE agent_id=?1 AND (status='running' OR (status='interrupted' AND stop_evidence='unknown')) ORDER BY started_at DESC",
        )
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map([agent_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(StoreError::Sqlite)?;
    let mut current = None;
    let mut stop_pending = None;
    for row in rows {
        let (task_id, attempt_id, status) = row.map_err(StoreError::Sqlite)?;
        let fence = TaskFence {
            task_id,
            attempt_id: Uuid::parse_str(&attempt_id).map_err(|_| StoreError::InvalidData)?,
        };
        let slot = if status == "running" {
            &mut current
        } else {
            &mut stop_pending
        };
        if slot.replace(fence).is_some() {
            return Err(StoreError::InvalidData);
        }
    }
    Ok((current, stop_pending))
}

pub fn agent_execution_barriers(
    store: &RouterStore,
    agent_id: &str,
) -> Result<(bool, bool), StoreError> {
    let (current, stop_pending) = agent_execution_fences(store, agent_id)?;
    Ok((current.is_some(), stop_pending.is_some()))
}

pub fn agent_has_execution_barrier(
    store: &RouterStore,
    agent_id: &str,
) -> Result<bool, StoreError> {
    let (running, unconfirmed) = agent_execution_barriers(store, agent_id)?;
    Ok(running || unconfirmed)
}

pub fn get_task(
    store: &RouterStore,
    workspace: &WorkspaceName,
    task_id: i64,
) -> Result<Option<TaskDetail>, StoreError> {
    get_task_detail_connection(store.connection(), workspace, task_id)
}

pub fn list_tasks(
    store: &RouterStore,
    workspace: &WorkspaceName,
    states: &[TaskState],
    assigned_agent_id: Option<&str>,
    after: i64,
    limit: u16,
) -> Result<(Vec<TaskSummary>, i64, bool), StoreError> {
    if states.is_empty() {
        return Err(StoreError::InvalidData);
    }
    let placeholders = std::iter::repeat_n("?", states.len())
        .collect::<Vec<_>>()
        .join(",");
    let mut sql = format!(
        "SELECT workspace,id,version,title,state,assigned_agent_id,current_attempt_id,last_attempt_id,last_checkpoint_id,pause_reason,created_at,updated_at FROM tasks WHERE workspace=? AND id>? AND state IN({placeholders})"
    );
    if assigned_agent_id.is_some() {
        sql.push_str(" AND assigned_agent_id=?");
    }
    sql.push_str(" ORDER BY id LIMIT ?");
    let mut values = Vec::<rusqlite::types::Value>::new();
    values.push(workspace.as_str().to_owned().into());
    values.push(after.into());
    for state in states {
        values.push(state.as_str().to_owned().into());
    }
    if let Some(agent_id) = assigned_agent_id {
        values.push(agent_id.to_owned().into());
    }
    values.push((i64::from(limit) + 1).into());
    let mut statement = store
        .connection()
        .prepare(&sql)
        .map_err(StoreError::Sqlite)?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(values), |row| {
            task_summary_from_row(store.connection(), row)
        })
        .map_err(StoreError::Sqlite)?;
    let mut tasks = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Sqlite)?;
    let has_more = tasks.len() > usize::from(limit);
    if has_more {
        tasks.pop();
    }
    let cursor = tasks.last().map_or(after, |task| task.id);
    Ok((tasks, cursor, has_more))
}

fn validate_command(caller: &CallerContext, command: &TaskCommand) -> Result<(), TaskError> {
    let operation_id = Some(command.operation_id());
    match command {
        TaskCommand::Create {
            title, description, ..
        } => {
            validate_title(title)
                .and_then(|()| validate_description(description))
                .map_err(|_| TaskError::code(RouterErrorCode::InvalidMessage, operation_id))?;
        }
        TaskCommand::Edit {
            title, description, ..
        } => {
            if title.is_none() && description.is_none() {
                return Err(TaskError::code(
                    RouterErrorCode::InvalidMessage,
                    operation_id,
                ));
            }
            if let Some(title) = title {
                validate_title(title)
                    .map_err(|_| TaskError::code(RouterErrorCode::InvalidMessage, operation_id))?;
            }
            if let Some(description) = description {
                validate_description(description)
                    .map_err(|_| TaskError::code(RouterErrorCode::InvalidMessage, operation_id))?;
            }
        }
        TaskCommand::Assign { agent_id, .. } => {
            if agent_id
                .as_deref()
                .is_some_and(|value| !crate::protocol::is_agent_id(value))
            {
                return Err(TaskError::code(
                    RouterErrorCode::InvalidMessage,
                    operation_id,
                ));
            }
        }
        TaskCommand::Note { text, .. } => {
            validate_note(text)
                .map_err(|_| TaskError::code(RouterErrorCode::InvalidMessage, operation_id))?;
        }
        TaskCommand::Begin {
            resume_note,
            work_request_id,
            ..
        } => {
            validate_handoff_note(resume_note)
                .map_err(|_| TaskError::code(RouterErrorCode::InvalidMessage, operation_id))?;
            if !crate::protocol::is_request_id(work_request_id) {
                return Err(TaskError::code(
                    RouterErrorCode::InvalidMessage,
                    operation_id,
                ));
            }
            require_executor(caller, operation_id)?;
        }
        TaskCommand::Checkpoint { checkpoint, .. }
        | TaskCommand::Pause { checkpoint, .. }
        | TaskCommand::Complete {
            result: checkpoint, ..
        } => {
            checkpoint
                .validate()
                .map_err(|_| TaskError::code(RouterErrorCode::InvalidMessage, operation_id))?;
            require_executor(caller, operation_id)?;
        }
        TaskCommand::Cancel { note, .. }
        | TaskCommand::Reopen { note, .. }
        | TaskCommand::Interrupt { note, .. }
        | TaskCommand::ConfirmStopped { note, .. } => {
            validate_handoff_note(note)
                .map_err(|_| TaskError::code(RouterErrorCode::InvalidMessage, operation_id))?;
        }
    }
    if matches!(
        command,
        TaskCommand::Interrupt { .. } | TaskCommand::ConfirmStopped { .. }
    ) && !matches!(caller.role, CallerRole::Operator | CallerRole::Admin)
    {
        return Err(TaskError::code(
            RouterErrorCode::PermissionDenied,
            operation_id,
        ));
    }
    Ok(())
}

fn require_executor(caller: &CallerContext, operation_id: Option<Uuid>) -> Result<(), TaskError> {
    if matches!(caller.role, CallerRole::Agent | CallerRole::Delegate) && caller.agent_id.is_some()
    {
        Ok(())
    } else {
        Err(TaskError::code(
            RouterErrorCode::PermissionDenied,
            operation_id,
        ))
    }
}

#[derive(Clone)]
struct MutationReceipt {
    actor_id: String,
    request_hash: [u8; 32],
    task_id: i64,
    applied_version: i64,
    attempt_id: Option<Uuid>,
    report_id: Option<Uuid>,
}

fn mutation_receipt(
    transaction: &Transaction<'_>,
    workspace: &WorkspaceName,
    operation_id: Uuid,
) -> Result<Option<MutationReceipt>, StoreError> {
    transaction
        .query_row(
            "SELECT actor_id,request_hash,task_id,applied_version,attempt_id,report_id FROM task_mutations WHERE workspace=?1 AND operation_id=?2",
            params![workspace.as_str(), operation_id.to_string()],
            |row| {
                let hash: Vec<u8> = row.get(1)?;
                if hash.len() != 32 {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                let mut request_hash = [0_u8; 32];
                request_hash.copy_from_slice(&hash);
                let attempt_id = row
                    .get::<_, Option<String>>(4)?
                    .map(|value| Uuid::parse_str(&value))
                    .transpose()
                    .map_err(|_| rusqlite::Error::InvalidQuery)?;
                let report_id = row
                    .get::<_, Option<String>>(5)?
                    .map(|value| Uuid::parse_str(&value))
                    .transpose()
                    .map_err(|_| rusqlite::Error::InvalidQuery)?;
                Ok(MutationReceipt {
                    actor_id: row.get(0)?,
                    request_hash,
                    task_id: row.get(2)?,
                    applied_version: row.get(3)?,
                    attempt_id,
                    report_id,
                })
            },
        )
        .optional()
        .map_err(StoreError::Sqlite)
}

fn insert_receipt(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    request_hash: [u8; 32],
    task_id: i64,
    applied_version: i64,
    attempt_id: Option<Uuid>,
    report_id: Option<Uuid>,
    created_at: i64,
) -> Result<(), TaskError> {
    transaction
        .execute(
            "INSERT INTO task_mutations(workspace,operation_id,actor_id,request_hash,task_id,applied_version,attempt_id,report_id,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                caller.workspace.as_str(),
                operation_id.to_string(),
                caller.actor_id,
                request_hash.as_slice(),
                task_id,
                applied_version,
                attempt_id.map(|value| value.to_string()),
                report_id.map(|value| value.to_string()),
                created_at,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    Ok(())
}

fn apply_create(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    title: &str,
    description: &str,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    let task_id: i64 = transaction
        .query_row(
            "UPDATE workspaces SET next_task_id=next_task_id+1 WHERE name=?1 AND next_task_id<=?2 RETURNING next_task_id-1",
            params![caller.workspace.as_str(), MAX_SAFE_INTEGER],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?
        .ok_or_else(|| TaskError::code(RouterErrorCode::StorageError, Some(operation_id)))?;
    let now = now_millis().map_err(TaskError::from)?;
    transaction
        .execute(
            "INSERT INTO tasks(workspace,id,version,title,description,state,assigned_agent_id,current_attempt_id,last_attempt_id,last_checkpoint_id,result_report_id,pause_reason,created_by,updated_by,created_at,updated_at) VALUES(?1,?2,1,?3,?4,'todo',NULL,NULL,NULL,NULL,NULL,NULL,?5,?5,?6,?6)",
            params![
                caller.workspace.as_str(),
                task_id,
                title,
                description,
                caller.actor_id,
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        1,
        TaskChange::Created,
        None,
        None,
        None,
        now,
    )
}

fn apply_edit(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    task_id: i64,
    expected_version: i64,
    title: Option<&str>,
    description: Option<&str>,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    let row = task_state_version(transaction, &caller.workspace, task_id, operation_id)?;
    check_version(row.1, expected_version, operation_id)?;
    if row.0.is_terminal() || row.0 == TaskState::InProgress {
        return Err(TaskError::code(
            RouterErrorCode::TaskInvalidTransition,
            Some(operation_id),
        ));
    }
    let version = expected_version + 1;
    let now = now_millis().map_err(TaskError::from)?;
    transaction
        .execute(
            "UPDATE tasks SET title=COALESCE(?3,title),description=COALESCE(?4,description),version=?5,updated_by=?6,updated_at=?7 WHERE workspace=?1 AND id=?2",
            params![
                caller.workspace.as_str(),
                task_id,
                title,
                description,
                version,
                caller.actor_id,
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        version,
        TaskChange::Edited,
        None,
        None,
        None,
        now,
    )
}

fn apply_assign(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    task_id: i64,
    expected_version: i64,
    agent_id: Option<&str>,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    let (state, version) =
        task_state_version(transaction, &caller.workspace, task_id, operation_id)?;
    check_version(version, expected_version, operation_id)?;
    if state.is_terminal() {
        return Err(TaskError::code(
            RouterErrorCode::TaskInvalidTransition,
            Some(operation_id),
        ));
    }
    if let Some(agent_id) = agent_id {
        let exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM credentials WHERE role='agent' AND subject=?1 AND revoked_at IS NULL AND EXISTS(SELECT 1 FROM json_each(workspaces_json) WHERE value=?2))",
                params![agent_id, caller.workspace.as_str()],
                |row| row.get(0),
            )
            .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
        if !exists {
            return Err(TaskError::code(
                RouterErrorCode::PermissionDenied,
                Some(operation_id),
            ));
        }
    }
    let new_version = expected_version + 1;
    let now = now_millis().map_err(TaskError::from)?;
    transaction
        .execute(
            "UPDATE tasks SET assigned_agent_id=?3,version=?4,updated_by=?5,updated_at=?6 WHERE workspace=?1 AND id=?2",
            params![
                caller.workspace.as_str(),
                task_id,
                agent_id,
                new_version,
                caller.actor_id,
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        new_version,
        TaskChange::Assigned,
        None,
        None,
        None,
        now,
    )
}

fn apply_note(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    task_id: i64,
    text: &str,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    let (_, version) = task_state_version(transaction, &caller.workspace, task_id, operation_id)?;
    let attempt_id = task_last_attempt_id(transaction, &caller.workspace, task_id)?;
    let now = now_millis().map_err(TaskError::from)?;
    let report_id = insert_text_report(
        transaction,
        caller,
        task_id,
        attempt_id,
        ReportKind::Note,
        text,
        now,
    )?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        version,
        TaskChange::Noted,
        attempt_id,
        Some(report_id),
        None,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_begin(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    task_id: i64,
    work_request_id: &str,
    expected_version: i64,
    last_checkpoint_id: Option<Uuid>,
    resume_note: &str,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    let agent_id = caller
        .agent_id
        .as_deref()
        .ok_or_else(|| TaskError::code(RouterErrorCode::PermissionDenied, Some(operation_id)))?;
    let session_id = caller
        .session_id
        .ok_or_else(|| TaskError::code(RouterErrorCode::PermissionDenied, Some(operation_id)))?;
    if !caller.reservation.as_ref().is_some_and(|reservation| {
        reservation.task_id == task_id && reservation.work_request_id == work_request_id
    }) {
        return Err(TaskError::code(
            RouterErrorCode::TaskStaleAttempt,
            Some(operation_id),
        ));
    }
    let row: (String, i64, Option<String>, Option<String>, Option<String>) = transaction
        .query_row(
            "SELECT state,version,assigned_agent_id,current_attempt_id,last_checkpoint_id FROM tasks WHERE workspace=?1 AND id=?2",
            params![caller.workspace.as_str(), task_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?
        .ok_or_else(|| TaskError::code(RouterErrorCode::TaskNotFound, Some(operation_id)))?;
    check_version(row.1, expected_version, operation_id)?;
    let state = parse_task_state(&row.0)
        .ok_or_else(|| TaskError::code(RouterErrorCode::StorageError, Some(operation_id)))?;
    if !state.is_beginable() || row.3.is_some() {
        return Err(TaskError::code(
            RouterErrorCode::TaskActive,
            Some(operation_id),
        ));
    }
    if row.2.as_deref() != Some(agent_id) {
        return Err(TaskError::code(
            RouterErrorCode::TaskNotAssigned,
            Some(operation_id),
        ));
    }
    let stored_checkpoint = row
        .4
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| TaskError::code(RouterErrorCode::StorageError, Some(operation_id)))?;
    if stored_checkpoint != last_checkpoint_id {
        return Err(TaskError::code(
            RouterErrorCode::TaskConflict,
            Some(operation_id),
        ));
    }
    let attempt_id = Uuid::new_v4();
    let now = now_millis().map_err(TaskError::from)?;
    transaction
        .execute(
            "INSERT INTO task_attempts(id,workspace,task_id,agent_id,credential_id,session_id,connection_generation,work_request_id,resumed_from_checkpoint_id,status,stop_evidence,reason,started_at,ended_at,stopped_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'running','unknown',NULL,?10,NULL,NULL)",
            params![
                attempt_id.to_string(),
                caller.workspace.as_str(),
                task_id,
                agent_id,
                caller.credential_id.to_string(),
                session_id.to_string(),
                caller.connection_generation,
                work_request_id,
                last_checkpoint_id.map(|value| value.to_string()),
                now,
            ],
        )
        .map_err(|error| {
            if is_constraint(&error) {
                TaskError::code(RouterErrorCode::TaskStopUnconfirmed, Some(operation_id))
            } else {
                TaskError::from(StoreError::Sqlite(error))
            }
        })?;
    let report_id = insert_text_report(
        transaction,
        caller,
        task_id,
        Some(attempt_id),
        ReportKind::Note,
        resume_note,
        now,
    )?;
    let new_version = expected_version + 1;
    transaction
        .execute(
            "UPDATE tasks SET state='in_progress',version=?3,current_attempt_id=?4,last_attempt_id=?4,pause_reason=NULL,updated_by=?5,updated_at=?6 WHERE workspace=?1 AND id=?2",
            params![
                caller.workspace.as_str(),
                task_id,
                new_version,
                attempt_id.to_string(),
                caller.actor_id,
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        new_version,
        TaskChange::Begun,
        Some(attempt_id),
        Some(report_id),
        None,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_checkpoint(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    task_id: i64,
    attempt_id: Uuid,
    expected_version: i64,
    checkpoint: &TaskCheckpoint,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    require_attempt_owner(
        transaction,
        caller,
        task_id,
        attempt_id,
        expected_version,
        operation_id,
    )?;
    let now = now_millis().map_err(TaskError::from)?;
    let report_id = insert_checkpoint_report(
        transaction,
        caller,
        task_id,
        attempt_id,
        ReportKind::Checkpoint,
        checkpoint,
        now,
    )?;
    let version = expected_version + 1;
    transaction
        .execute(
            "UPDATE tasks SET version=?3,last_checkpoint_id=?4,updated_by=?5,updated_at=?6 WHERE workspace=?1 AND id=?2",
            params![
                caller.workspace.as_str(),
                task_id,
                version,
                report_id.to_string(),
                caller.actor_id,
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        version,
        TaskChange::Checkpoint,
        Some(attempt_id),
        Some(report_id),
        None,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_release(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    task_id: i64,
    attempt_id: Uuid,
    expected_version: i64,
    checkpoint: &TaskCheckpoint,
    next_state: TaskState,
    completed: bool,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    require_attempt_owner(
        transaction,
        caller,
        task_id,
        attempt_id,
        expected_version,
        operation_id,
    )?;
    let now = now_millis().map_err(TaskError::from)?;
    let kind = if completed {
        ReportKind::Result
    } else {
        ReportKind::Checkpoint
    };
    let report_id = insert_checkpoint_report(
        transaction,
        caller,
        task_id,
        attempt_id,
        kind,
        checkpoint,
        now,
    )?;
    transaction
        .execute(
            "UPDATE task_attempts SET status=?2,stop_evidence='released',reason=?3,ended_at=?4,stopped_at=?4 WHERE id=?1 AND status='running'",
            params![
                attempt_id.to_string(),
                if completed { "completed" } else { "released" },
                if completed { None::<&str> } else if next_state == TaskState::Blocked { Some("blocked") } else { Some("paused") },
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    let version = expected_version + 1;
    transaction
        .execute(
            "UPDATE tasks SET state=?3,version=?4,assigned_agent_id=CASE WHEN ?3='done' THEN NULL ELSE assigned_agent_id END,current_attempt_id=NULL,last_attempt_id=?5,last_checkpoint_id=?6,result_report_id=CASE WHEN ?3='done' THEN ?6 ELSE result_report_id END,pause_reason=CASE WHEN ?3='blocked' THEN 'blocked' WHEN ?3='paused' THEN 'paused' ELSE NULL END,updated_by=?7,updated_at=?8 WHERE workspace=?1 AND id=?2",
            params![
                caller.workspace.as_str(),
                task_id,
                next_state.as_str(),
                version,
                attempt_id.to_string(),
                report_id.to_string(),
                caller.actor_id,
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        version,
        if completed {
            TaskChange::Completed
        } else {
            TaskChange::Paused
        },
        Some(attempt_id),
        Some(report_id),
        Some(attempt_id),
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_terminal(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    task_id: i64,
    expected_version: i64,
    note: &str,
    next_state: TaskState,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    let (state, version) =
        task_state_version(transaction, &caller.workspace, task_id, operation_id)?;
    check_version(version, expected_version, operation_id)?;
    let valid = match next_state {
        TaskState::Cancelled => !state.is_terminal() && state != TaskState::InProgress,
        TaskState::Todo => state.is_terminal(),
        _ => false,
    };
    if !valid {
        return Err(TaskError::code(
            RouterErrorCode::TaskInvalidTransition,
            Some(operation_id),
        ));
    }
    let blocked: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM task_attempts WHERE workspace=?1 AND task_id=?2 AND status='interrupted' AND stop_evidence='unknown')",
            params![caller.workspace.as_str(), task_id],
            |row| row.get(0),
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    if blocked {
        return Err(TaskError::code(
            RouterErrorCode::TaskStopUnconfirmed,
            Some(operation_id),
        ));
    }
    let now = now_millis().map_err(TaskError::from)?;
    let attempt_id = task_last_attempt_id(transaction, &caller.workspace, task_id)?;
    let report_id = insert_text_report(
        transaction,
        caller,
        task_id,
        attempt_id,
        ReportKind::Note,
        note,
        now,
    )?;
    let new_version = expected_version + 1;
    transaction
        .execute(
            "UPDATE tasks SET state=?3,version=?4,assigned_agent_id=NULL,current_attempt_id=NULL,result_report_id=CASE WHEN ?3='todo' THEN NULL ELSE result_report_id END,pause_reason=NULL,updated_by=?5,updated_at=?6 WHERE workspace=?1 AND id=?2",
            params![
                caller.workspace.as_str(),
                task_id,
                next_state.as_str(),
                new_version,
                caller.actor_id,
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        new_version,
        if next_state == TaskState::Todo {
            TaskChange::Reopened
        } else {
            TaskChange::Cancelled
        },
        attempt_id,
        Some(report_id),
        None,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_interrupt(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    task_id: i64,
    expected_version: i64,
    note: &str,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    let (state, version) =
        task_state_version(transaction, &caller.workspace, task_id, operation_id)?;
    check_version(version, expected_version, operation_id)?;
    if state != TaskState::InProgress {
        return Err(TaskError::code(
            RouterErrorCode::TaskInvalidTransition,
            Some(operation_id),
        ));
    }
    let attempt_id = task_current_attempt_id(transaction, &caller.workspace, task_id)?
        .ok_or_else(|| TaskError::code(RouterErrorCode::TaskStaleAttempt, Some(operation_id)))?;
    let now = now_millis().map_err(TaskError::from)?;
    let report_id = insert_text_report(
        transaction,
        caller,
        task_id,
        Some(attempt_id),
        ReportKind::Note,
        note,
        now,
    )?;
    transaction
        .execute(
            "UPDATE task_attempts SET status='interrupted',stop_evidence='unknown',reason='operator_interrupt',ended_at=?2 WHERE id=?1 AND status='running'",
            params![attempt_id.to_string(), now],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    let new_version = expected_version + 1;
    transaction
        .execute(
            "UPDATE tasks SET state='paused',version=?3,current_attempt_id=NULL,last_attempt_id=?4,pause_reason='operator_interrupt',updated_by=?5,updated_at=?6 WHERE workspace=?1 AND id=?2",
            params![
                caller.workspace.as_str(),
                task_id,
                new_version,
                attempt_id.to_string(),
                caller.actor_id,
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        new_version,
        TaskChange::Interrupted,
        Some(attempt_id),
        Some(report_id),
        Some(attempt_id),
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_confirm_stopped(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    task_id: i64,
    attempt_id: Uuid,
    expected_version: i64,
    note: &str,
    request_hash: [u8; 32],
) -> Result<TaskApplyResult, TaskError> {
    let (_, version) = task_state_version(transaction, &caller.workspace, task_id, operation_id)?;
    check_version(version, expected_version, operation_id)?;
    let pending: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM task_attempts WHERE id=?1 AND workspace=?2 AND task_id=?3 AND status='interrupted' AND stop_evidence='unknown')",
            params![attempt_id.to_string(), caller.workspace.as_str(), task_id],
            |row| row.get(0),
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    if !pending {
        return Err(TaskError::code(
            RouterErrorCode::TaskStaleAttempt,
            Some(operation_id),
        ));
    }
    let now = now_millis().map_err(TaskError::from)?;
    let report_id = insert_text_report(
        transaction,
        caller,
        task_id,
        Some(attempt_id),
        ReportKind::Note,
        note,
        now,
    )?;
    transaction
        .execute(
            "UPDATE task_attempts SET stop_evidence='confirmed',stopped_at=?2 WHERE id=?1",
            params![attempt_id.to_string(), now],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    let new_version = expected_version + 1;
    transaction
        .execute(
            "UPDATE tasks SET version=?3,updated_by=?4,updated_at=?5 WHERE workspace=?1 AND id=?2",
            params![
                caller.workspace.as_str(),
                task_id,
                new_version,
                caller.actor_id,
                now,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    finish_mutation(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        new_version,
        TaskChange::Stopped,
        Some(attempt_id),
        Some(report_id),
        None,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn finish_mutation(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    operation_id: Uuid,
    request_hash: [u8; 32],
    task_id: i64,
    applied_version: i64,
    change: TaskChange,
    attempt_id: Option<Uuid>,
    report_id: Option<Uuid>,
    closed_attempt_id: Option<Uuid>,
    created_at: i64,
) -> Result<TaskApplyResult, TaskError> {
    let task = get_task_detail_tx(transaction, &caller.workspace, task_id)
        .map_err(TaskError::from)?
        .ok_or_else(|| TaskError::code(RouterErrorCode::TaskNotFound, Some(operation_id)))?;
    let content = serde_json::to_string(&TaskEvent {
        change,
        task: task.summary.clone(),
        attempt_id,
        report_id,
        external_operation_id: None,
    })
    .map_err(|_| TaskError::code(RouterErrorCode::StorageError, Some(operation_id)))?;
    let event = append_event_in_transaction(
        transaction,
        &EventInsert {
            workspace: &caller.workspace,
            kind: WorkspaceEventKind::Task,
            actor_id: &caller.actor_id,
            request_id: None,
            target_id: None,
            task_id: Some(task_id),
            content: Some(&content),
            ok: None,
            error: None,
        },
    )
    .map_err(TaskError::from)?;
    insert_receipt(
        transaction,
        caller,
        operation_id,
        request_hash,
        task_id,
        applied_version,
        attempt_id,
        report_id,
        created_at,
    )?;
    Ok(TaskApplyResult {
        mutation: TaskMutationResult {
            operation_id,
            applied_version,
            report_id,
            task,
        },
        event,
        closed_attempt_id,
    })
}

fn task_state_version(
    transaction: &Transaction<'_>,
    workspace: &WorkspaceName,
    task_id: i64,
    operation_id: Uuid,
) -> Result<(TaskState, i64), TaskError> {
    let row: Option<(String, i64)> = transaction
        .query_row(
            "SELECT state,version FROM tasks WHERE workspace=?1 AND id=?2",
            params![workspace.as_str(), task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    let (state, version) =
        row.ok_or_else(|| TaskError::code(RouterErrorCode::TaskNotFound, Some(operation_id)))?;
    Ok((
        parse_task_state(&state)
            .ok_or_else(|| TaskError::code(RouterErrorCode::StorageError, Some(operation_id)))?,
        version,
    ))
}

fn check_version(current: i64, expected: i64, operation_id: Uuid) -> Result<(), TaskError> {
    if current == expected {
        Ok(())
    } else {
        Err(TaskError::conflict(current, operation_id))
    }
}

fn require_attempt_owner(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    task_id: i64,
    attempt_id: Uuid,
    expected_version: i64,
    operation_id: Uuid,
) -> Result<(), TaskError> {
    let (_, version) = task_state_version(transaction, &caller.workspace, task_id, operation_id)?;
    check_version(version, expected_version, operation_id)?;
    let agent_id = caller
        .agent_id
        .as_deref()
        .ok_or_else(|| TaskError::code(RouterErrorCode::PermissionDenied, Some(operation_id)))?;
    let session_id = caller
        .session_id
        .ok_or_else(|| TaskError::code(RouterErrorCode::PermissionDenied, Some(operation_id)))?;
    let matches: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM task_attempts a JOIN tasks t ON t.workspace=a.workspace AND t.id=a.task_id WHERE a.id=?1 AND a.workspace=?2 AND a.task_id=?3 AND a.agent_id=?4 AND a.credential_id=?5 AND a.session_id=?6 AND a.connection_generation=?7 AND a.status='running' AND t.current_attempt_id=a.id)",
            params![
                attempt_id.to_string(),
                caller.workspace.as_str(),
                task_id,
                agent_id,
                caller.credential_id.to_string(),
                session_id.to_string(),
                caller.connection_generation,
            ],
            |row| row.get(0),
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    if matches {
        Ok(())
    } else {
        Err(TaskError::code(
            RouterErrorCode::TaskStaleAttempt,
            Some(operation_id),
        ))
    }
}

fn exact_active_owner_attempt(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    attempt_id: Uuid,
) -> Result<bool, StoreError> {
    let Some(agent_id) = caller.agent_id.as_deref() else {
        return Ok(false);
    };
    let Some(session_id) = caller.session_id else {
        return Ok(false);
    };
    transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM task_attempts WHERE id=?1 AND agent_id=?2 AND credential_id=?3 AND session_id=?4 AND connection_generation=?5 AND status='running')",
            params![
                attempt_id.to_string(),
                agent_id,
                caller.credential_id.to_string(),
                session_id.to_string(),
                caller.connection_generation,
            ],
            |row| row.get(0),
        )
        .map_err(StoreError::Sqlite)
}

fn insert_text_report(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    task_id: i64,
    attempt_id: Option<Uuid>,
    kind: ReportKind,
    text: &str,
    created_at: i64,
) -> Result<Uuid, TaskError> {
    let report_id = Uuid::new_v4();
    let body = serde_json::to_string(text)
        .map_err(|_| TaskError::code(RouterErrorCode::StorageError, None))?;
    transaction
        .execute(
            "INSERT INTO task_reports(id,workspace,task_id,attempt_id,actor_id,kind,body_json,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                report_id.to_string(),
                caller.workspace.as_str(),
                task_id,
                attempt_id.map(|value| value.to_string()),
                caller.actor_id,
                kind.as_str(),
                body,
                created_at,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    Ok(report_id)
}

fn insert_checkpoint_report(
    transaction: &Transaction<'_>,
    caller: &CallerContext,
    task_id: i64,
    attempt_id: Uuid,
    kind: ReportKind,
    checkpoint: &TaskCheckpoint,
    created_at: i64,
) -> Result<Uuid, TaskError> {
    let report_id = Uuid::new_v4();
    let body = serde_json::to_string(checkpoint)
        .map_err(|_| TaskError::code(RouterErrorCode::StorageError, None))?;
    transaction
        .execute(
            "INSERT INTO task_reports(id,workspace,task_id,attempt_id,actor_id,kind,body_json,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                report_id.to_string(),
                caller.workspace.as_str(),
                task_id,
                attempt_id.to_string(),
                caller.actor_id,
                kind.as_str(),
                body,
                created_at,
            ],
        )
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?;
    Ok(report_id)
}

fn task_current_attempt_id(
    transaction: &Transaction<'_>,
    workspace: &WorkspaceName,
    task_id: i64,
) -> Result<Option<Uuid>, TaskError> {
    query_task_uuid(transaction, workspace, task_id, "current_attempt_id")
}

fn task_last_attempt_id(
    transaction: &Transaction<'_>,
    workspace: &WorkspaceName,
    task_id: i64,
) -> Result<Option<Uuid>, TaskError> {
    query_task_uuid(transaction, workspace, task_id, "last_attempt_id")
}

fn query_task_uuid(
    transaction: &Transaction<'_>,
    workspace: &WorkspaceName,
    task_id: i64,
    column: &str,
) -> Result<Option<Uuid>, TaskError> {
    let sql = format!("SELECT {column} FROM tasks WHERE workspace=?1 AND id=?2");
    transaction
        .query_row(&sql, params![workspace.as_str(), task_id], |row| {
            row.get::<_, Option<String>>(0)
        })
        .optional()
        .map_err(|error| TaskError::from(StoreError::Sqlite(error)))?
        .flatten()
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| TaskError::code(RouterErrorCode::StorageError, None))
}

fn last_task_event(
    transaction: &Transaction<'_>,
    workspace: &WorkspaceName,
    task_id: i64,
) -> Result<Option<WorkspaceEvent>, StoreError> {
    transaction
        .query_row(
            "SELECT workspace,seq,kind,actor_id,created_at,request_id,target_id,task_id,content,ok,error_code FROM events WHERE workspace=?1 AND task_id=?2 AND kind='task' ORDER BY seq DESC LIMIT 1",
            params![workspace.as_str(), task_id],
            task_event_row,
        )
        .optional()
        .map_err(StoreError::Sqlite)
}

fn task_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkspaceEvent> {
    Ok(WorkspaceEvent {
        workspace: WorkspaceName::parse(row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        seq: row.get(1)?,
        kind: WorkspaceEventKind::Task,
        actor_id: row.get(3)?,
        created_at: row.get(4)?,
        request_id: row.get(5)?,
        target_id: row.get(6)?,
        task_id: row.get(7)?,
        content: row.get(8)?,
        ok: row.get::<_, Option<i64>>(9)?.map(|value| value != 0),
        error: row
            .get::<_, Option<String>>(10)?
            .map(|value| serde_json::from_value(serde_json::Value::String(value)))
            .transpose()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
    })
}

fn get_task_detail_tx(
    transaction: &Transaction<'_>,
    workspace: &WorkspaceName,
    task_id: i64,
) -> Result<Option<TaskDetail>, StoreError> {
    get_task_detail_connection(transaction, workspace, task_id)
}

pub(crate) fn get_task_detail_connection(
    connection: &rusqlite::Connection,
    workspace: &WorkspaceName,
    task_id: i64,
) -> Result<Option<TaskDetail>, StoreError> {
    let base: Option<(
        String,
        i64,
        i64,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        String,
        i64,
        i64,
    )> = connection
        .query_row(
            "SELECT workspace,id,version,title,description,state,assigned_agent_id,current_attempt_id,last_attempt_id,last_checkpoint_id,result_report_id,pause_reason,created_by,updated_by,created_at,updated_at FROM tasks WHERE workspace=?1 AND id=?2",
            params![workspace.as_str(), task_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                    row.get(15)?,
                ))
            },
        )
        .optional()
        .map_err(StoreError::Sqlite)?;
    let Some(base) = base else {
        return Ok(None);
    };
    let current_attempt = base
        .7
        .as_deref()
        .map(|id| load_attempt(connection, id))
        .transpose()?;
    let last_attempt = base
        .8
        .as_deref()
        .map(|id| load_attempt(connection, id))
        .transpose()?;
    let checkpoint = base
        .9
        .as_deref()
        .map(|id| load_report(connection, id))
        .transpose()?;
    let result = base
        .10
        .as_deref()
        .map(|id| load_report(connection, id))
        .transpose()?;
    let last_executor_id = current_attempt
        .as_ref()
        .or(last_attempt.as_ref())
        .map(|attempt| attempt.agent_id.clone());
    let execution_session_id = current_attempt
        .as_ref()
        .or(last_attempt.as_ref())
        .map(|attempt| attempt.session_id);
    let stop_evidence = current_attempt
        .as_ref()
        .or(last_attempt.as_ref())
        .map(|attempt| attempt.stop_evidence);
    let last_checkpoint_at = checkpoint.as_ref().map(|report| report.created_at);
    let summary = TaskSummary {
        id: base.1,
        workspace: base.0,
        title: base.3,
        state: parse_task_state(&base.5).ok_or(StoreError::InvalidData)?,
        version: base.2,
        assigned_agent_id: base.6,
        current_attempt_id: parse_optional_uuid(base.7)?,
        last_executor_id,
        execution_session_id,
        last_checkpoint_at,
        pause_reason: base.11.as_deref().map(parse_pause_reason).transpose()?,
        stop_evidence,
        created_at: base.14,
        updated_at: base.15,
    };
    Ok(Some(TaskDetail {
        summary,
        description: base.4,
        created_by: base.12,
        updated_by: base.13,
        current_attempt,
        last_attempt,
        checkpoint,
        result,
        links: load_links(connection, workspace, task_id)?,
        external_operations: load_external_operations(connection, workspace, task_id)?,
    }))
}

fn task_summary_from_row(
    connection: &rusqlite::Connection,
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<TaskSummary> {
    let workspace: String = row.get(0)?;
    let current_id: Option<String> = row.get(6)?;
    let last_id: Option<String> = row.get(7)?;
    let checkpoint_id: Option<String> = row.get(8)?;
    let selected_attempt = current_id.as_deref().or(last_id.as_deref());
    let attempt = selected_attempt
        .map(|id| load_attempt(connection, id))
        .transpose()
        .map_err(store_to_sql)?;
    let checkpoint = checkpoint_id
        .as_deref()
        .map(|id| load_report(connection, id))
        .transpose()
        .map_err(store_to_sql)?;
    Ok(TaskSummary {
        workspace,
        id: row.get(1)?,
        version: row.get(2)?,
        title: row.get(3)?,
        state: parse_task_state(&row.get::<_, String>(4)?).ok_or(rusqlite::Error::InvalidQuery)?,
        assigned_agent_id: row.get(5)?,
        current_attempt_id: current_id
            .map(|value| Uuid::parse_str(&value))
            .transpose()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        last_executor_id: attempt.as_ref().map(|value| value.agent_id.clone()),
        execution_session_id: attempt.as_ref().map(|value| value.session_id),
        last_checkpoint_at: checkpoint.as_ref().map(|value| value.created_at),
        pause_reason: row
            .get::<_, Option<String>>(9)?
            .as_deref()
            .map(parse_pause_reason)
            .transpose()
            .map_err(store_to_sql)?,
        stop_evidence: attempt.as_ref().map(|value| value.stop_evidence),
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

fn load_attempt(connection: &rusqlite::Connection, id: &str) -> Result<TaskAttempt, StoreError> {
    connection
        .query_row(
            "SELECT id,task_id,agent_id,session_id,work_request_id,resumed_from_checkpoint_id,status,stop_evidence,reason,started_at,ended_at,stopped_at FROM task_attempts WHERE id=?1",
            [id],
            |row| {
                let status: String = row.get(6)?;
                let evidence: String = row.get(7)?;
                let reason: Option<String> = row.get(8)?;
                Ok(TaskAttempt {
                    id: parse_uuid_sql(&row.get::<_, String>(0)?)?,
                    task_id: row.get(1)?,
                    agent_id: row.get(2)?,
                    session_id: parse_uuid_sql(&row.get::<_, String>(3)?)?,
                    work_request_id: row.get(4)?,
                    resumed_from_checkpoint_id: row
                        .get::<_, Option<String>>(5)?
                        .as_deref()
                        .map(parse_uuid_sql)
                        .transpose()?,
                    status: parse_attempt_status(&status).ok_or(rusqlite::Error::InvalidQuery)?,
                    stop_evidence: parse_stop_evidence(&evidence)
                        .ok_or(rusqlite::Error::InvalidQuery)?,
                    reason: reason
                        .as_deref()
                        .map(parse_pause_reason)
                        .transpose()
                        .map_err(store_to_sql)?,
                    started_at: row.get(9)?,
                    ended_at: row.get(10)?,
                    stopped_at: row.get(11)?,
                })
            },
        )
        .map_err(StoreError::Sqlite)
}

fn load_report(
    connection: &rusqlite::Connection,
    id: &str,
) -> Result<TaskReportRecord, StoreError> {
    connection
        .query_row(
            "SELECT id,task_id,attempt_id,actor_id,kind,body_json,created_at FROM task_reports WHERE id=?1",
            [id],
            |row| {
                let kind_value: String = row.get(4)?;
                let kind = parse_report_kind(&kind_value).ok_or(rusqlite::Error::InvalidQuery)?;
                let body_json: String = row.get(5)?;
                let body = match kind {
                    ReportKind::Note => ReportBody::Text(
                        serde_json::from_str(&body_json)
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    ),
                    ReportKind::Checkpoint | ReportKind::Result => ReportBody::Checkpoint(
                        serde_json::from_str(&body_json)
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    ),
                };
                Ok(TaskReportRecord {
                    id: parse_uuid_sql(&row.get::<_, String>(0)?)?,
                    task_id: row.get(1)?,
                    attempt_id: row
                        .get::<_, Option<String>>(2)?
                        .as_deref()
                        .map(parse_uuid_sql)
                        .transpose()?,
                    actor_id: row.get(3)?,
                    kind,
                    body,
                    created_at: row.get(6)?,
                })
            },
        )
        .map_err(StoreError::Sqlite)
}

fn load_links(
    connection: &rusqlite::Connection,
    workspace: &WorkspaceName,
    task_id: i64,
) -> Result<Vec<ExternalLink>, StoreError> {
    let mut statement = connection
        .prepare_cached(
            "SELECT provider,namespace,external_id,url,linked_at FROM external_links WHERE workspace=?1 AND task_id=?2 ORDER BY provider",
        )
        .map_err(StoreError::Sqlite)?;
    statement
        .query_map(params![workspace.as_str(), task_id], |row| {
            let provider: String = row.get(0)?;
            Ok(ExternalLink {
                provider: parse_external_provider(&provider)
                    .ok_or(rusqlite::Error::InvalidQuery)?,
                namespace: row.get(1)?,
                external_id: row.get(2)?,
                url: row.get(3)?,
                linked_at: row.get(4)?,
            })
        })
        .map_err(StoreError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Sqlite)
}

fn load_external_operations(
    connection: &rusqlite::Connection,
    workspace: &WorkspaceName,
    task_id: i64,
) -> Result<Vec<ExternalOperationSummary>, StoreError> {
    let mut result = Vec::new();
    for provider in ["github", "linear"] {
        let row = connection
            .query_row(
                "SELECT operation_id,provider,kind,status,source_version,external_id,url,error,created_at,updated_at FROM external_operations WHERE workspace=?1 AND task_id=?2 AND provider=?3 ORDER BY updated_at DESC LIMIT 1",
                params![workspace.as_str(), task_id, provider],
                |row| {
                    let provider: String = row.get(1)?;
                    let kind: String = row.get(2)?;
                    let status: String = row.get(3)?;
                    Ok(ExternalOperationSummary {
                        id: parse_uuid_sql(&row.get::<_, String>(0)?)?,
                        task_id: Some(task_id),
                        provider: parse_external_provider(&provider).ok_or(rusqlite::Error::InvalidQuery)?,
                        kind: parse_external_kind(&kind).ok_or(rusqlite::Error::InvalidQuery)?,
                        status: parse_external_status(&status).ok_or(rusqlite::Error::InvalidQuery)?,
                        source_version: row.get(4)?,
                        external_id: row.get(5)?,
                        url: row.get(6)?,
                        error: row.get(7)?,
                        created_at: row.get(8)?,
                        updated_at: row.get(9)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::Sqlite)?;
        if let Some(row) = row {
            result.push(row);
        }
    }
    Ok(result)
}

fn parse_optional_uuid(value: Option<String>) -> Result<Option<Uuid>, StoreError> {
    value
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| StoreError::InvalidData)
}

fn parse_uuid_sql(value: &str) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| rusqlite::Error::InvalidQuery)
}

fn store_to_sql(error: StoreError) -> rusqlite::Error {
    match error {
        StoreError::Sqlite(error) => error,
        _ => rusqlite::Error::InvalidQuery,
    }
}

fn is_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

fn parse_task_state(value: &str) -> Option<TaskState> {
    match value {
        "todo" => Some(TaskState::Todo),
        "in_progress" => Some(TaskState::InProgress),
        "blocked" => Some(TaskState::Blocked),
        "paused" => Some(TaskState::Paused),
        "done" => Some(TaskState::Done),
        "cancelled" => Some(TaskState::Cancelled),
        _ => None,
    }
}

fn parse_attempt_status(value: &str) -> Option<AttemptStatus> {
    match value {
        "running" => Some(AttemptStatus::Running),
        "released" => Some(AttemptStatus::Released),
        "completed" => Some(AttemptStatus::Completed),
        "interrupted" => Some(AttemptStatus::Interrupted),
        _ => None,
    }
}

fn parse_stop_evidence(value: &str) -> Option<StopEvidence> {
    match value {
        "released" => Some(StopEvidence::Released),
        "confirmed" => Some(StopEvidence::Confirmed),
        "unknown" => Some(StopEvidence::Unknown),
        _ => None,
    }
}

fn parse_pause_reason(value: &str) -> Result<PauseReason, StoreError> {
    match value {
        "paused" => Ok(PauseReason::Paused),
        "blocked" => Ok(PauseReason::Blocked),
        "transport_lost" => Ok(PauseReason::TransportLost),
        "credential_revoked" => Ok(PauseReason::CredentialRevoked),
        "session_ended" => Ok(PauseReason::SessionEnded),
        "router_restarted" => Ok(PauseReason::RouterRestarted),
        "request_timeout" => Ok(PauseReason::RequestTimeout),
        "request_cancelled" => Ok(PauseReason::RequestCancelled),
        "turn_ended" => Ok(PauseReason::TurnEnded),
        "host_error" => Ok(PauseReason::HostError),
        "operator_interrupt" => Ok(PauseReason::OperatorInterrupt),
        "reply_without_release" => Ok(PauseReason::ReplyWithoutRelease),
        _ => Err(StoreError::InvalidData),
    }
}

fn parse_report_kind(value: &str) -> Option<ReportKind> {
    match value {
        "note" => Some(ReportKind::Note),
        "checkpoint" => Some(ReportKind::Checkpoint),
        "result" => Some(ReportKind::Result),
        _ => None,
    }
}

fn parse_external_provider(value: &str) -> Option<ExternalProvider> {
    match value {
        "github" => Some(ExternalProvider::Github),
        "linear" => Some(ExternalProvider::Linear),
        _ => None,
    }
}

fn parse_external_kind(value: &str) -> Option<ExternalOperationKind> {
    match value {
        "import" => Some(ExternalOperationKind::Import),
        "link" => Some(ExternalOperationKind::Link),
        "publish_issue" => Some(ExternalOperationKind::PublishIssue),
        "publish_report" => Some(ExternalOperationKind::PublishReport),
        _ => None,
    }
}

fn parse_external_status(value: &str) -> Option<ExternalOperationStatus> {
    match value {
        "running" => Some(ExternalOperationStatus::Running),
        "succeeded" => Some(ExternalOperationStatus::Succeeded),
        "failed" => Some(ExternalOperationStatus::Failed),
        "unconfirmed" => Some(ExternalOperationStatus::Unconfirmed),
        _ => None,
    }
}
