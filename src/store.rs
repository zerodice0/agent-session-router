use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    credentials::{
        CredentialError, CredentialFile, CredentialRole, PublicCredentialClaims,
        ensure_private_directory,
    },
    protocol::{
        AgentClient, AgentSide, RouterErrorCode, WorkspaceEvent, WorkspaceEventKind, WorkspaceName,
    },
    tasks::{
        ExternalOperationKind, ExternalOperationStatus, ExternalOperationSummary, ExternalProvider,
        IntegrationChange, IntegrationEvent, MAX_SAFE_INTEGER, TaskAttempt, TaskChange, TaskEvent,
        TaskReportRecord, get_task_detail_connection,
    },
};

pub const SCHEMA_VERSION: i64 = 1;
pub const DATABASE_FILE_NAME: &str = "router.sqlite";

pub struct RouterStore {
    connection: Connection,
    path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub(crate) struct StoredTaskHistoryEvent {
    pub event: WorkspaceEvent,
    pub attempt: Option<TaskAttempt>,
    pub report: Option<TaskReportRecord>,
}

#[derive(Clone, Debug)]
pub struct StoredCredential {
    pub claims: PublicCredentialClaims,
    pub token_hash: [u8; 32],
    pub created_at: i64,
    pub revoked_at: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct EventInsert<'a> {
    pub workspace: &'a WorkspaceName,
    pub kind: WorkspaceEventKind,
    pub actor_id: &'a str,
    pub request_id: Option<&'a str>,
    pub target_id: Option<&'a str>,
    pub task_id: Option<i64>,
    pub content: Option<&'a str>,
    pub ok: Option<bool>,
    pub error: Option<RouterErrorCode>,
}

#[derive(Clone, Debug)]
pub struct ChatAppend {
    pub event: WorkspaceEvent,
    pub inserted: bool,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("store_in_use")]
    InUse,
    #[error("request_conflict")]
    Conflict,
    #[error("unsupported_schema")]
    UnsupportedSchema,
    #[error("storage_error")]
    Sqlite(#[source] rusqlite::Error),
    #[error("storage_io")]
    Io(#[from] io::Error),
    #[error("storage_invalid_data")]
    InvalidData,
    #[error("storage_overflow")]
    Overflow,
}

impl From<StoreError> for RouterErrorCode {
    fn from(value: StoreError) -> Self {
        match value {
            StoreError::InUse => Self::StoreInUse,
            StoreError::Conflict => Self::RequestConflict,
            StoreError::UnsupportedSchema | StoreError::InvalidData => Self::ConfigurationRequired,
            StoreError::Sqlite(_) | StoreError::Io(_) | StoreError::Overflow => Self::StorageError,
        }
    }
}

impl RouterStore {
    pub fn open(data_dir: &Path) -> Result<Self, StoreError> {
        ensure_private_directory(data_dir, true).map_err(map_credential_error)?;
        let path = data_dir.join(DATABASE_FILE_NAME);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if !metadata.is_file()
                    || metadata.file_type().is_symlink()
                    || metadata.uid() != rustix::process::getuid().as_raw()
                    || metadata.mode() & 0o077 != 0
                {
                    return Err(StoreError::InvalidData);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&path)?;
                file.sync_all()?;
                File::open(data_dir)?.sync_all()?;
            }
            Err(error) => return Err(StoreError::Io(error)),
        }
        let connection = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(classify_open_error)?;
        let mut store = Self {
            connection,
            path: Some(path),
        };
        store.configure_and_migrate()?;
        store.recover()?;
        Ok(store)
    }

    pub fn open_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory().map_err(StoreError::Sqlite)?;
        let mut store = Self {
            connection,
            path: None,
        };
        store.configure_and_migrate()?;
        store.recover()?;
        Ok(store)
    }

    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn close(self) -> Result<(), StoreError> {
        self.connection
            .close()
            .map_err(|(_, error)| StoreError::Sqlite(error))
    }

    pub fn credential_count(&self) -> Result<i64, StoreError> {
        self.connection
            .query_row("SELECT count(*) FROM credentials", [], |row| row.get(0))
            .map_err(StoreError::Sqlite)
    }

    pub fn insert_credential(&mut self, credential: &CredentialFile) -> Result<(), StoreError> {
        let now = now_millis()?;
        let workspaces = serde_json::to_string(&credential.public_claims().workspaces)
            .map_err(|_| StoreError::InvalidData)?;
        self.connection
            .execute(
                "INSERT INTO credentials(id,token_hash,role,subject,agent_side,agent_client,workspaces_json,created_at,revoked_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,NULL)",
                params![
                    credential.id.to_string(),
                    credential.token_hash().as_slice(),
                    credential.role.as_str(),
                    credential.subject,
                    credential.agent_side.map(agent_side_str),
                    credential.agent_client.map(agent_client_str),
                    workspaces,
                    now,
                ],
            )
            .map_err(StoreError::Sqlite)?;
        Ok(())
    }

    pub fn credential_by_hash(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<StoredCredential>, StoreError> {
        self.connection
            .query_row(
                "SELECT id,token_hash,role,subject,agent_side,agent_client,workspaces_json,created_at,revoked_at FROM credentials WHERE token_hash=?1",
                [token_hash.as_slice()],
                credential_from_row,
            )
            .optional()
            .map_err(StoreError::Sqlite)
    }

    pub fn credential_by_id(&self, id: Uuid) -> Result<Option<StoredCredential>, StoreError> {
        self.connection
            .query_row(
                "SELECT id,token_hash,role,subject,agent_side,agent_client,workspaces_json,created_at,revoked_at FROM credentials WHERE id=?1",
                [id.to_string()],
                credential_from_row,
            )
            .optional()
            .map_err(StoreError::Sqlite)
    }

    pub fn revoke_credential(&mut self, id: Uuid) -> Result<bool, StoreError> {
        let changed = self
            .connection
            .execute(
                "UPDATE credentials SET revoked_at=?2 WHERE id=?1 AND revoked_at IS NULL AND NOT(role='operator' AND subject='admin')",
                params![id.to_string(), now_millis()?],
            )
            .map_err(StoreError::Sqlite)?;
        Ok(changed == 1)
    }

    pub fn list_credentials(
        &self,
        after: Option<Uuid>,
        limit: u16,
    ) -> Result<(Vec<StoredCredential>, Option<Uuid>, bool), StoreError> {
        let after = after.map(|id| id.to_string()).unwrap_or_default();
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT id,token_hash,role,subject,agent_side,agent_client,workspaces_json,created_at,revoked_at FROM credentials WHERE id>?1 ORDER BY id LIMIT ?2",
            )
            .map_err(StoreError::Sqlite)?;
        let rows = statement
            .query_map(params![after, i64::from(limit) + 1], credential_from_row)
            .map_err(StoreError::Sqlite)?;
        let mut credentials = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::Sqlite)?;
        let has_more = credentials.len() > usize::from(limit);
        if has_more {
            credentials.pop();
        }
        let cursor = credentials.last().map(|credential| credential.claims.id);
        Ok((credentials, cursor, has_more))
    }

    pub fn create_workspace(&mut self, name: &WorkspaceName) -> Result<(i64, bool), StoreError> {
        let now = now_millis()?;
        let changed = self
            .connection
            .execute(
                "INSERT INTO workspaces(name,created_at,next_seq,next_task_id) VALUES(?1,?2,1,1) ON CONFLICT(name) DO NOTHING",
                params![name.as_str(), now],
            )
            .map_err(StoreError::Sqlite)?;
        Ok((now, changed == 1))
    }

    pub fn workspace_exists(&self, name: &WorkspaceName) -> Result<bool, StoreError> {
        self.connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspaces WHERE name=?1)",
                [name.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::Sqlite)
    }

    pub fn workspace_created_at(&self, name: &WorkspaceName) -> Result<Option<i64>, StoreError> {
        self.connection
            .query_row(
                "SELECT created_at FROM workspaces WHERE name=?1",
                [name.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Sqlite)
    }

    pub fn list_workspace_rows(
        &self,
        allowed: Option<&[WorkspaceName]>,
        after: Option<&str>,
        limit: u16,
    ) -> Result<(Vec<(WorkspaceName, i64)>, Option<String>, bool), StoreError> {
        let after = after.unwrap_or("");
        let mut statement = self
            .connection
            .prepare_cached("SELECT name,created_at FROM workspaces WHERE name>?1 ORDER BY name")
            .map_err(StoreError::Sqlite)?;
        let rows = statement
            .query_map([after], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(StoreError::Sqlite)?;
        let allowed =
            allowed.map(|values| values.iter().map(WorkspaceName::as_str).collect::<Vec<_>>());
        let mut result = Vec::with_capacity(usize::from(limit) + 1);
        for row in rows {
            let (name, created_at) = row.map_err(StoreError::Sqlite)?;
            if allowed
                .as_ref()
                .is_some_and(|grants| !grants.contains(&name.as_str()))
            {
                continue;
            }
            result.push((
                WorkspaceName::parse(name).map_err(|_| StoreError::InvalidData)?,
                created_at,
            ));
            if result.len() > usize::from(limit) {
                break;
            }
        }
        let has_more = result.len() > usize::from(limit);
        if has_more {
            result.pop();
        }
        let cursor = result
            .last()
            .map(|(name, _)| name.as_str().to_owned())
            .or_else(|| (!after.is_empty()).then(|| after.to_owned()));
        Ok((result, cursor, has_more))
    }

    pub fn latest_seq(&self, workspace: &WorkspaceName) -> Result<Option<i64>, StoreError> {
        self.connection
            .query_row(
                "SELECT next_seq-1 FROM workspaces WHERE name=?1",
                [workspace.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::Sqlite)
    }

    pub fn append_event(&mut self, insert: &EventInsert<'_>) -> Result<WorkspaceEvent, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_open_error)?;
        let event = append_event_in_transaction(&transaction, insert)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(event)
    }

    pub fn append_chat_idempotent(
        &mut self,
        workspace: &WorkspaceName,
        actor_id: &str,
        request_id: &str,
        content: &str,
    ) -> Result<ChatAppend, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_open_error)?;
        let existing = transaction
            .query_row(
                "SELECT workspace,seq,kind,actor_id,created_at,request_id,target_id,task_id,content,ok,error_code FROM events WHERE workspace=?1 AND actor_id=?2 AND request_id=?3 AND kind='chat'",
                params![workspace.as_str(), actor_id, request_id],
                event_from_row,
            )
            .optional()
            .map_err(StoreError::Sqlite)?;
        if let Some(event) = existing {
            if event.content.as_deref() != Some(content) {
                return Err(StoreError::Conflict);
            }
            transaction.commit().map_err(StoreError::Sqlite)?;
            return Ok(ChatAppend {
                event,
                inserted: false,
            });
        }
        let event = append_event_in_transaction(
            &transaction,
            &EventInsert {
                workspace,
                kind: WorkspaceEventKind::Chat,
                actor_id,
                request_id: Some(request_id),
                target_id: None,
                task_id: None,
                content: Some(content),
                ok: None,
                error: None,
            },
        )?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(ChatAppend {
            event,
            inserted: true,
        })
    }

    pub fn append_request(
        &mut self,
        insert: &EventInsert<'_>,
    ) -> Result<WorkspaceEvent, StoreError> {
        if insert.kind != WorkspaceEventKind::Request {
            return Err(StoreError::InvalidData);
        }
        let request_id = insert.request_id.ok_or(StoreError::InvalidData)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_open_error)?;
        let exists = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE workspace=?1 AND request_id=?2 AND kind='request')",
                params![insert.workspace.as_str(), request_id],
                |row| row.get::<_, bool>(0),
            )
            .map_err(StoreError::Sqlite)?;
        if exists {
            return Err(StoreError::Conflict);
        }
        let event = append_event_in_transaction(&transaction, insert)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(event)
    }

    pub fn recent_history(
        &self,
        workspace: &WorkspaceName,
        limit: u16,
    ) -> Result<(Vec<WorkspaceEvent>, i64, bool), StoreError> {
        let latest = self.latest_seq(workspace)?.ok_or(StoreError::InvalidData)?;
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT workspace,seq,kind,actor_id,created_at,request_id,target_id,task_id,content,ok,error_code FROM events WHERE workspace=?1 ORDER BY seq DESC LIMIT ?2",
            )
            .map_err(StoreError::Sqlite)?;
        let rows = statement
            .query_map(
                params![workspace.as_str(), i64::from(limit)],
                event_from_row,
            )
            .map_err(StoreError::Sqlite)?;
        let mut events = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::Sqlite)?;
        events.reverse();
        let cursor = events.last().map_or(0, |event| event.seq);
        debug_assert_eq!(cursor, latest);
        Ok((events, cursor, false))
    }

    pub fn history(
        &self,
        workspace: &WorkspaceName,
        after: i64,
        limit: u16,
    ) -> Result<(Vec<WorkspaceEvent>, i64, bool), StoreError> {
        let latest = self.latest_seq(workspace)?.ok_or(StoreError::InvalidData)?;
        if after > latest {
            return Err(StoreError::InvalidData);
        }
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT workspace,seq,kind,actor_id,created_at,request_id,target_id,task_id,content,ok,error_code FROM events WHERE workspace=?1 AND seq>?2 ORDER BY seq LIMIT ?3",
            )
            .map_err(StoreError::Sqlite)?;
        let rows = statement
            .query_map(
                params![workspace.as_str(), after, i64::from(limit) + 1],
                event_from_row,
            )
            .map_err(StoreError::Sqlite)?;
        let mut events = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::Sqlite)?;
        let has_more = events.len() > usize::from(limit);
        if has_more {
            events.pop();
        }
        let cursor = events.last().map_or(after, |event| event.seq);
        Ok((events, cursor, has_more))
    }

    pub(crate) fn task_history(
        &self,
        workspace: &WorkspaceName,
        task_id: i64,
        after: i64,
        limit: u16,
    ) -> Result<(Vec<StoredTaskHistoryEvent>, i64, bool), StoreError> {
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT e.workspace,e.seq,e.kind,e.actor_id,e.created_at,e.request_id,e.target_id,e.task_id,e.content,e.ok,e.error_code,
                 CASE WHEN a.id IS NULL THEN NULL ELSE json_object(
                   'id',a.id,'taskId',a.task_id,'agentId',a.agent_id,'sessionId',a.session_id,
                   'workRequestId',a.work_request_id,'resumedFromCheckpointId',a.resumed_from_checkpoint_id,
                   'status',a.status,'stopEvidence',a.stop_evidence,'reason',a.reason,
                   'startedAt',a.started_at,'endedAt',a.ended_at,'stoppedAt',a.stopped_at) END,
                 CASE WHEN r.id IS NULL THEN NULL ELSE json_object(
                   'id',r.id,'taskId',r.task_id,'attemptId',r.attempt_id,'actorId',r.actor_id,
                   'kind',r.kind,'body',json(r.body_json),'createdAt',r.created_at) END
                 FROM events e
                 LEFT JOIN task_attempts a ON a.id=json_extract(e.content,'$.attemptId')
                   AND a.workspace=e.workspace AND a.task_id=e.task_id
                 LEFT JOIN task_reports r ON r.id=json_extract(e.content,'$.reportId')
                   AND r.workspace=e.workspace AND r.task_id=e.task_id
                 WHERE e.workspace=?1 AND e.task_id=?2 AND e.kind='task' AND e.seq>?3
                 ORDER BY e.seq LIMIT ?4",
            )
            .map_err(StoreError::Sqlite)?;
        let rows = statement
            .query_map(
                params![workspace.as_str(), task_id, after, i64::from(limit) + 1],
                |row| {
                    Ok((
                        event_from_row(row)?,
                        row.get::<_, Option<String>>(11)?,
                        row.get::<_, Option<String>>(12)?,
                    ))
                },
            )
            .map_err(StoreError::Sqlite)?;
        let mut events = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::Sqlite)?
            .into_iter()
            .map(|(event, attempt, report)| {
                Ok(StoredTaskHistoryEvent {
                    event,
                    attempt: attempt
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(|_| StoreError::InvalidData)?,
                    report: report
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(|_| StoreError::InvalidData)?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let has_more = events.len() > usize::from(limit);
        if has_more {
            events.pop();
        }
        let cursor = events.last().map_or(after, |event| event.event.seq);
        Ok((events, cursor, has_more))
    }

    pub(crate) fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(crate) fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }

    fn configure_and_migrate(&mut self) -> Result<(), StoreError> {
        self.connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(StoreError::Sqlite)?;
        self.connection
            .pragma_update(None, "locking_mode", "EXCLUSIVE")
            .map_err(classify_open_error)?;
        self.connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(StoreError::Sqlite)?;
        self.connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(classify_open_error)?;
        self.connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(StoreError::Sqlite)?;
        self.connection
            .pragma_update(None, "busy_timeout", 5000_i64)
            .map_err(StoreError::Sqlite)?;

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_open_error)?;
        let version: i64 = transaction
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(StoreError::Sqlite)?;
        if version > SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema);
        }
        if version == 0 {
            transaction
                .execute_batch(SCHEMA)
                .map_err(StoreError::Sqlite)?;
            transaction
                .pragma_update(None, "user_version", SCHEMA_VERSION)
                .map_err(StoreError::Sqlite)?;
        }
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(())
    }

    pub(crate) fn recover_external_inflight(&mut self) -> Result<(), StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::Sqlite)?;
        recover_external_operations(&transaction)?;
        transaction.commit().map_err(StoreError::Sqlite)
    }

    fn recover(&mut self) -> Result<(), StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify_open_error)?;
        recover_unfinished_requests(&transaction)?;
        recover_running_attempts(&transaction)?;
        recover_external_operations(&transaction)?;
        transaction.commit().map_err(StoreError::Sqlite)?;
        Ok(())
    }
}

pub(crate) fn append_event_in_transaction(
    transaction: &Transaction<'_>,
    insert: &EventInsert<'_>,
) -> Result<WorkspaceEvent, StoreError> {
    let seq: i64 = transaction
        .query_row(
            "UPDATE workspaces SET next_seq=next_seq+1 WHERE name=?1 AND next_seq<=?2 RETURNING next_seq-1",
            params![insert.workspace.as_str(), MAX_SAFE_INTEGER],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::Sqlite)?
        .ok_or(StoreError::Overflow)?;
    let created_at = now_millis()?;
    transaction
        .execute(
            "INSERT INTO events(workspace,seq,kind,actor_id,created_at,request_id,target_id,task_id,content,ok,error_code) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                insert.workspace.as_str(),
                seq,
                event_kind_str(insert.kind),
                insert.actor_id,
                created_at,
                insert.request_id,
                insert.target_id,
                insert.task_id,
                insert.content,
                insert.ok.map(i64::from),
                insert.error.map(RouterErrorCode::as_str),
            ],
        )
        .map_err(StoreError::Sqlite)?;
    Ok(WorkspaceEvent {
        workspace: insert.workspace.clone(),
        seq,
        kind: insert.kind,
        actor_id: insert.actor_id.to_owned(),
        created_at,
        request_id: insert.request_id.map(str::to_owned),
        target_id: insert.target_id.map(str::to_owned),
        task_id: insert.task_id,
        content: insert.content.map(str::to_owned),
        ok: insert.ok,
        error: insert.error,
    })
}

pub fn now_millis() -> Result<i64, StoreError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreError::InvalidData)?;
    i64::try_from(duration.as_millis()).map_err(|_| StoreError::Overflow)
}

fn recover_unfinished_requests(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let mut statement = transaction
        .prepare_cached(
            "SELECT r.workspace,r.request_id,r.actor_id,r.task_id FROM events r LEFT JOIN events x ON x.workspace=r.workspace AND x.kind='result' AND x.request_id=r.request_id WHERE r.kind='request' AND x.seq IS NULL ORDER BY r.workspace,r.seq",
        )
        .map_err(StoreError::Sqlite)?;
    let requests = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })
        .map_err(StoreError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Sqlite)?;
    drop(statement);
    for (workspace, request_id, requester, task_id) in requests {
        let workspace = WorkspaceName::parse(workspace).map_err(|_| StoreError::InvalidData)?;
        append_event_in_transaction(
            transaction,
            &EventInsert {
                workspace: &workspace,
                kind: WorkspaceEventKind::Result,
                actor_id: "system:router",
                request_id: Some(&request_id),
                target_id: Some(&requester),
                task_id,
                content: None,
                ok: Some(false),
                error: Some(RouterErrorCode::RouterRestarted),
            },
        )?;
    }
    Ok(())
}

fn recover_running_attempts(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let now = now_millis()?;
    let attempts = {
        let mut statement = transaction
            .prepare_cached("SELECT id,workspace,task_id FROM task_attempts WHERE status='running'")
            .map_err(StoreError::Sqlite)?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(StoreError::Sqlite)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::Sqlite)?
    };
    for (attempt_id, workspace, task_id) in attempts {
        transaction
            .execute(
                "UPDATE task_attempts SET status='interrupted',stop_evidence='unknown',reason='router_restarted',ended_at=?2 WHERE id=?1 AND status='running'",
                params![attempt_id, now],
            )
            .map_err(StoreError::Sqlite)?;
        transaction
            .execute(
                "UPDATE tasks SET state='paused',current_attempt_id=NULL,pause_reason='router_restarted',version=version+1,updated_by='system:router',updated_at=?3 WHERE workspace=?1 AND id=?2 AND current_attempt_id=?4",
                params![workspace, task_id, now, attempt_id],
            )
            .map_err(StoreError::Sqlite)?;
        let workspace = WorkspaceName::parse(workspace).map_err(|_| StoreError::InvalidData)?;
        let task = get_task_detail_connection(transaction, &workspace, task_id)?
            .ok_or(StoreError::InvalidData)?;
        let attempt_id = Uuid::parse_str(&attempt_id).map_err(|_| StoreError::InvalidData)?;
        let content = serde_json::to_string(&TaskEvent {
            change: TaskChange::Interrupted,
            task: task.summary,
            attempt_id: Some(attempt_id),
            report_id: None,
            external_operation_id: None,
        })
        .map_err(|_| StoreError::InvalidData)?;
        append_event_in_transaction(
            transaction,
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
    }
    Ok(())
}

fn recover_external_operations(transaction: &Transaction<'_>) -> Result<(), StoreError> {
    let now = now_millis()?;
    let operations = {
        let mut statement = transaction
            .prepare(
                "SELECT workspace,operation_id,task_id,provider,kind,source_version,external_id,url,created_at,phase FROM external_operations WHERE status='running' AND phase IN('prepared','dispatched') ORDER BY workspace,created_at,operation_id",
            )
            .map_err(StoreError::Sqlite)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, String>(9)?,
                ))
            })
            .map_err(StoreError::Sqlite)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::Sqlite)?
    };
    for (
        workspace,
        operation_id,
        task_id,
        provider,
        kind,
        source_version,
        external_id,
        url,
        created_at,
        phase,
    ) in operations
    {
        let workspace = WorkspaceName::parse(workspace).map_err(|_| StoreError::InvalidData)?;
        let id = Uuid::parse_str(&operation_id).map_err(|_| StoreError::InvalidData)?;
        let provider = match provider.as_str() {
            "github" => ExternalProvider::Github,
            "linear" => ExternalProvider::Linear,
            _ => return Err(StoreError::InvalidData),
        };
        let kind = match kind.as_str() {
            "import" => ExternalOperationKind::Import,
            "link" => ExternalOperationKind::Link,
            "publish_issue" => ExternalOperationKind::PublishIssue,
            "publish_report" => ExternalOperationKind::PublishReport,
            _ => return Err(StoreError::InvalidData),
        };
        let (status, error) = if phase == "dispatched" {
            (ExternalOperationStatus::Unconfirmed, "external_unconfirmed")
        } else {
            (
                ExternalOperationStatus::Failed,
                if matches!(
                    kind,
                    ExternalOperationKind::PublishIssue | ExternalOperationKind::PublishReport
                ) {
                    "not_dispatched"
                } else {
                    "not_applied"
                },
            )
        };
        transaction
            .execute(
                "UPDATE external_operations SET phase='terminal',status=?3,error=?4,updated_at=?5 WHERE workspace=?1 AND operation_id=?2 AND status='running'",
                params![
                    workspace.as_str(),
                    operation_id,
                    match status {
                        ExternalOperationStatus::Unconfirmed => "unconfirmed",
                        ExternalOperationStatus::Failed => "failed",
                        ExternalOperationStatus::Running
                        | ExternalOperationStatus::Succeeded => {
                            return Err(StoreError::InvalidData);
                        }
                    },
                    error,
                    now
                ],
            )
            .map_err(StoreError::Sqlite)?;
        let operation = ExternalOperationSummary {
            id,
            task_id,
            provider,
            kind,
            status,
            source_version,
            external_id,
            url,
            error: Some(error.to_owned()),
            created_at,
            updated_at: now,
        };
        let content = serde_json::to_string(&IntegrationEvent {
            change: IntegrationChange::Operation,
            integration: None,
            operation: Some(operation),
            resolution: None,
        })
        .map_err(|_| StoreError::InvalidData)?;
        append_event_in_transaction(
            transaction,
            &EventInsert {
                workspace: &workspace,
                kind: WorkspaceEventKind::Integration,
                actor_id: "system:router",
                request_id: None,
                target_id: None,
                task_id,
                content: Some(&content),
                ok: None,
                error: None,
            },
        )?;
    }
    Ok(())
}

fn credential_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredCredential> {
    let id: String = row.get(0)?;
    let hash: Vec<u8> = row.get(1)?;
    let role: String = row.get(2)?;
    let subject: String = row.get(3)?;
    let side: Option<String> = row.get(4)?;
    let client: Option<String> = row.get(5)?;
    let grants: String = row.get(6)?;
    let mut token_hash = [0_u8; 32];
    if hash.len() != token_hash.len() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    token_hash.copy_from_slice(&hash);
    let claims = PublicCredentialClaims {
        version: 1,
        id: Uuid::parse_str(&id).map_err(|_| rusqlite::Error::InvalidQuery)?,
        role: parse_role(&role).ok_or(rusqlite::Error::InvalidQuery)?,
        subject,
        agent_side: side
            .as_deref()
            .map(parse_agent_side)
            .transpose()
            .map_err(|()| rusqlite::Error::InvalidQuery)?,
        agent_client: client
            .as_deref()
            .map(parse_agent_client)
            .transpose()
            .map_err(|()| rusqlite::Error::InvalidQuery)?,
        workspaces: serde_json::from_str(&grants).map_err(|_| rusqlite::Error::InvalidQuery)?,
    };
    Ok(StoredCredential {
        claims,
        token_hash,
        created_at: row.get(7)?,
        revoked_at: row.get(8)?,
    })
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkspaceEvent> {
    let workspace: String = row.get(0)?;
    let kind: String = row.get(2)?;
    let error: Option<String> = row.get(10)?;
    Ok(WorkspaceEvent {
        workspace: WorkspaceName::parse(workspace).map_err(|_| rusqlite::Error::InvalidQuery)?,
        seq: row.get(1)?,
        kind: parse_event_kind(&kind).ok_or(rusqlite::Error::InvalidQuery)?,
        actor_id: row.get(3)?,
        created_at: row.get(4)?,
        request_id: row.get(5)?,
        target_id: row.get(6)?,
        task_id: row.get(7)?,
        content: row.get(8)?,
        ok: row.get::<_, Option<i64>>(9)?.map(|value| value != 0),
        error: error
            .map(|value| serde_json::from_value(serde_json::Value::String(value)))
            .transpose()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
    })
}

fn map_credential_error(error: CredentialError) -> StoreError {
    match error {
        CredentialError::Io(error) => StoreError::Io(error),
        CredentialError::Invalid
        | CredentialError::Permissions
        | CredentialError::Random
        | CredentialError::Exists => StoreError::InvalidData,
    }
}

fn classify_open_error(error: rusqlite::Error) -> StoreError {
    match &error {
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(
                code.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) =>
        {
            StoreError::InUse
        }
        _ => StoreError::Sqlite(error),
    }
}

const fn event_kind_str(kind: WorkspaceEventKind) -> &'static str {
    match kind {
        WorkspaceEventKind::Chat => "chat",
        WorkspaceEventKind::Request => "request",
        WorkspaceEventKind::Result => "result",
        WorkspaceEventKind::MemberJoined => "member_joined",
        WorkspaceEventKind::MemberLeft => "member_left",
        WorkspaceEventKind::Task => "task",
        WorkspaceEventKind::Integration => "integration",
    }
}

fn parse_event_kind(value: &str) -> Option<WorkspaceEventKind> {
    match value {
        "chat" => Some(WorkspaceEventKind::Chat),
        "request" => Some(WorkspaceEventKind::Request),
        "result" => Some(WorkspaceEventKind::Result),
        "member_joined" => Some(WorkspaceEventKind::MemberJoined),
        "member_left" => Some(WorkspaceEventKind::MemberLeft),
        "task" => Some(WorkspaceEventKind::Task),
        "integration" => Some(WorkspaceEventKind::Integration),
        _ => None,
    }
}

const fn agent_side_str(value: AgentSide) -> &'static str {
    match value {
        AgentSide::Claude => "claude",
        AgentSide::Codex => "codex",
        AgentSide::Generic => "generic",
    }
}

const fn agent_client_str(value: AgentClient) -> &'static str {
    match value {
        AgentClient::Omp => "omp",
        AgentClient::ClaudeCode => "claude-code",
        AgentClient::ClaudeSdk => "claude-sdk",
        AgentClient::CodexCli => "codex-cli",
        AgentClient::CodexAppServer => "codex-app-server",
        AgentClient::Generic => "generic",
    }
}

fn parse_agent_side(value: &str) -> Result<AgentSide, ()> {
    match value {
        "claude" => Ok(AgentSide::Claude),
        "codex" => Ok(AgentSide::Codex),
        "generic" => Ok(AgentSide::Generic),
        _ => Err(()),
    }
}

fn parse_agent_client(value: &str) -> Result<AgentClient, ()> {
    match value {
        "omp" => Ok(AgentClient::Omp),
        "claude-code" => Ok(AgentClient::ClaudeCode),
        "claude-sdk" => Ok(AgentClient::ClaudeSdk),
        "codex-cli" => Ok(AgentClient::CodexCli),
        "codex-app-server" => Ok(AgentClient::CodexAppServer),
        "generic" => Ok(AgentClient::Generic),
        _ => Err(()),
    }
}

fn parse_role(value: &str) -> Option<CredentialRole> {
    match value {
        "agent" => Some(CredentialRole::Agent),
        "operator" => Some(CredentialRole::Operator),
        _ => None,
    }
}

const SCHEMA: &str = r"
CREATE TABLE workspaces(
    name TEXT PRIMARY KEY NOT NULL,
    created_at INTEGER NOT NULL CHECK(created_at>=0 AND created_at<=9007199254740991),
    next_seq INTEGER NOT NULL DEFAULT 1 CHECK(next_seq>=1 AND next_seq<=9007199254740991),
    next_task_id INTEGER NOT NULL DEFAULT 1 CHECK(next_task_id>=1 AND next_task_id<=9007199254740991)
) STRICT;
CREATE TABLE events(
    workspace TEXT NOT NULL REFERENCES workspaces(name),
    seq INTEGER NOT NULL CHECK(seq>=1 AND seq<=9007199254740991),
    kind TEXT NOT NULL CHECK(kind IN('chat','request','result','member_joined','member_left','task','integration')),
    actor_id TEXT NOT NULL CHECK(length(actor_id) BETWEEN 1 AND 128),
    created_at INTEGER NOT NULL CHECK(created_at>=0 AND created_at<=9007199254740991),
    request_id TEXT NULL,
    target_id TEXT NULL,
    task_id INTEGER NULL CHECK(task_id IS NULL OR (task_id>=1 AND task_id<=9007199254740991)),
    content TEXT NULL,
    ok INTEGER NULL CHECK(ok IS NULL OR ok IN(0,1)),
    error_code TEXT NULL,
    PRIMARY KEY(workspace,seq),
    CHECK(
      (kind='chat' AND content IS NOT NULL AND request_id IS NOT NULL AND target_id IS NULL AND task_id IS NULL AND ok IS NULL AND error_code IS NULL) OR
      (kind='request' AND content IS NOT NULL AND request_id IS NOT NULL AND target_id IS NOT NULL AND ok IS NULL AND error_code IS NULL) OR
      (kind='result' AND request_id IS NOT NULL AND target_id IS NOT NULL AND ok IS NOT NULL AND ((ok=1 AND content IS NOT NULL AND error_code IS NULL) OR (ok=0 AND content IS NULL AND error_code IS NOT NULL))) OR
      (kind IN('member_joined','member_left') AND request_id IS NULL AND target_id IS NULL AND task_id IS NULL AND content IS NULL AND ok IS NULL AND error_code IS NULL) OR
      (kind IN('task','integration') AND request_id IS NULL AND target_id IS NULL AND content IS NOT NULL AND ok IS NULL AND error_code IS NULL)
    )
) STRICT;
CREATE UNIQUE INDEX event_request_unique ON events(workspace,request_id) WHERE kind='request';
CREATE UNIQUE INDEX event_result_unique ON events(workspace,request_id) WHERE kind='result';
CREATE UNIQUE INDEX event_chat_idempotency ON events(workspace,actor_id,request_id) WHERE kind='chat';
CREATE INDEX events_task_seq ON events(workspace,task_id,seq);
CREATE TABLE credentials(
    id TEXT PRIMARY KEY NOT NULL,
    token_hash BLOB UNIQUE NOT NULL CHECK(length(token_hash)=32),
    role TEXT NOT NULL CHECK(role IN('agent','operator')),
    subject TEXT NOT NULL CHECK(length(subject) BETWEEN 1 AND 128),
    agent_side TEXT NULL CHECK(agent_side IS NULL OR agent_side IN('claude','codex','generic')),
    agent_client TEXT NULL CHECK(agent_client IS NULL OR agent_client IN('omp','claude-code','claude-sdk','codex-cli','codex-app-server','generic')),
    workspaces_json TEXT NOT NULL CHECK(json_valid(workspaces_json) AND json_type(workspaces_json)='array' AND json_array_length(workspaces_json)<=64),
    created_at INTEGER NOT NULL CHECK(created_at>=0 AND created_at<=9007199254740991),
    revoked_at INTEGER NULL CHECK(revoked_at IS NULL OR (revoked_at>=0 AND revoked_at<=9007199254740991)),
    CHECK((role='agent' AND agent_side IS NOT NULL AND agent_client IS NOT NULL) OR (role='operator' AND agent_side IS NULL AND agent_client IS NULL))
) STRICT;
CREATE TABLE tasks(
    workspace TEXT NOT NULL,
    id INTEGER NOT NULL CHECK(id>=1 AND id<=9007199254740991),
    version INTEGER NOT NULL CHECK(version>=1 AND version<=9007199254740991),
    title TEXT NOT NULL CHECK(length(title)>0 AND length(CAST(title AS BLOB))<=1024),
    description TEXT NOT NULL CHECK(length(CAST(description AS BLOB))<=65536),
    state TEXT NOT NULL CHECK(state IN('todo','in_progress','blocked','paused','done','cancelled')),
    assigned_agent_id TEXT NULL,
    current_attempt_id TEXT NULL,
    last_attempt_id TEXT NULL,
    last_checkpoint_id TEXT NULL,
    result_report_id TEXT NULL,
    pause_reason TEXT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    created_at INTEGER NOT NULL CHECK(created_at>=0 AND created_at<=9007199254740991),
    updated_at INTEGER NOT NULL CHECK(updated_at>=0 AND updated_at<=9007199254740991),
    PRIMARY KEY(workspace,id),
    FOREIGN KEY(workspace) REFERENCES workspaces(name),
    FOREIGN KEY(current_attempt_id) REFERENCES task_attempts(id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(last_attempt_id) REFERENCES task_attempts(id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(last_checkpoint_id) REFERENCES task_reports(id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(result_report_id) REFERENCES task_reports(id) DEFERRABLE INITIALLY DEFERRED,
    CHECK((state='in_progress' AND current_attempt_id IS NOT NULL) OR (state<>'in_progress' AND current_attempt_id IS NULL)),
    CHECK(state NOT IN('done','cancelled') OR assigned_agent_id IS NULL)
) STRICT;
CREATE TABLE task_attempts(
    id TEXT PRIMARY KEY NOT NULL,
    workspace TEXT NOT NULL,
    task_id INTEGER NOT NULL,
    agent_id TEXT NOT NULL,
    credential_id TEXT NOT NULL REFERENCES credentials(id),
    session_id TEXT NOT NULL,
    connection_generation INTEGER NOT NULL CHECK(connection_generation>=1 AND connection_generation<=9007199254740991),
    work_request_id TEXT NOT NULL,
    resumed_from_checkpoint_id TEXT NULL REFERENCES task_reports(id) DEFERRABLE INITIALLY DEFERRED,
    status TEXT NOT NULL CHECK(status IN('running','released','completed','interrupted')),
    stop_evidence TEXT NOT NULL CHECK(stop_evidence IN('released','confirmed','unknown')),
    reason TEXT NULL CHECK(reason IS NULL OR reason IN('paused','blocked','transport_lost','credential_revoked','session_ended','router_restarted','request_timeout','request_cancelled','turn_ended','host_error','operator_interrupt','reply_without_release')),
    started_at INTEGER NOT NULL CHECK(started_at>=0 AND started_at<=9007199254740991),
    ended_at INTEGER NULL CHECK(ended_at IS NULL OR (ended_at>=0 AND ended_at<=9007199254740991)),
    stopped_at INTEGER NULL CHECK(stopped_at IS NULL OR (stopped_at>=0 AND stopped_at<=9007199254740991)),
    FOREIGN KEY(workspace,task_id) REFERENCES tasks(workspace,id),
    CHECK((status='running' AND ended_at IS NULL AND stop_evidence='unknown') OR (status<>'running' AND ended_at IS NOT NULL))
) STRICT;
CREATE UNIQUE INDEX task_running_unique ON task_attempts(workspace,task_id) WHERE status='running';
CREATE UNIQUE INDEX agent_execution_barrier_unique ON task_attempts(agent_id) WHERE status='running' OR (status='interrupted' AND stop_evidence='unknown');
CREATE TABLE task_reports(
    id TEXT PRIMARY KEY NOT NULL,
    workspace TEXT NOT NULL,
    task_id INTEGER NOT NULL,
    attempt_id TEXT NULL REFERENCES task_attempts(id),
    actor_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN('note','checkpoint','result')),
    body_json TEXT NOT NULL CHECK(json_valid(body_json) AND length(CAST(body_json AS BLOB))<=131072),
    created_at INTEGER NOT NULL CHECK(created_at>=0 AND created_at<=9007199254740991),
    FOREIGN KEY(workspace,task_id) REFERENCES tasks(workspace,id)
) STRICT;
CREATE TABLE task_mutations(
    workspace TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_hash BLOB NOT NULL CHECK(length(request_hash)=32),
    task_id INTEGER NOT NULL,
    applied_version INTEGER NOT NULL,
    attempt_id TEXT NULL,
    report_id TEXT NULL REFERENCES task_reports(id),
    created_at INTEGER NOT NULL,
    PRIMARY KEY(workspace,operation_id),
    FOREIGN KEY(workspace,task_id) REFERENCES tasks(workspace,id)
) STRICT;
CREATE INDEX tasks_state_id ON tasks(workspace,state,id);
CREATE INDEX tasks_assignee_id ON tasks(workspace,assigned_agent_id,id);
CREATE INDEX reports_task_time ON task_reports(workspace,task_id,created_at,id);
CREATE TABLE integration_bindings(
    workspace TEXT NOT NULL REFERENCES workspaces(name),
    provider TEXT NOT NULL CHECK(provider IN('github','linear')),
    target_json TEXT NOT NULL CHECK(json_valid(target_json)),
    access TEXT NOT NULL CHECK(access IN('read','write')),
    enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),
    config_hash BLOB NOT NULL CHECK(length(config_hash)=32),
    revision INTEGER NOT NULL CHECK(revision>=1),
    updated_at INTEGER NOT NULL CHECK(updated_at>=0 AND updated_at<=9007199254740991),
    PRIMARY KEY(workspace,provider)
) STRICT;
CREATE TABLE external_links(
    workspace TEXT NOT NULL,
    task_id INTEGER NOT NULL,
    provider TEXT NOT NULL CHECK(provider IN('github','linear')),
    namespace TEXT NOT NULL,
    external_id TEXT NOT NULL,
    url TEXT NOT NULL,
    linked_at INTEGER NOT NULL CHECK(linked_at>=0 AND linked_at<=9007199254740991),
    PRIMARY KEY(workspace,task_id,provider),
    UNIQUE(provider,namespace,external_id),
    FOREIGN KEY(workspace,task_id) REFERENCES tasks(workspace,id)
) STRICT;
CREATE TABLE external_operations(
    workspace TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    provider TEXT NOT NULL CHECK(provider IN('github','linear')),
    namespace TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN('import','link','publish_issue','publish_report')),
    task_id INTEGER NULL,
    source_version INTEGER NULL,
    request_hash BLOB NOT NULL CHECK(length(request_hash)=32),
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json) AND length(CAST(payload_json AS BLOB))<=131072),
    phase TEXT NOT NULL CHECK(phase IN('prepared','dispatched','terminal')),
    status TEXT NOT NULL CHECK(status IN('running','succeeded','failed','unconfirmed')),
    external_id TEXT NULL,
    url TEXT NULL,
    error TEXT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(workspace,operation_id),
    FOREIGN KEY(workspace,task_id) REFERENCES tasks(workspace,id)
) STRICT;
CREATE TABLE external_resolutions(
    workspace TEXT NOT NULL,
    resolution_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    resolver_actor_id TEXT NOT NULL,
    request_hash BLOB NOT NULL CHECK(length(request_hash)=32),
    outcome TEXT NOT NULL CHECK(outcome IN('applied','not_applied')),
    external_id TEXT NULL,
    note TEXT NOT NULL CHECK(length(CAST(note AS BLOB)) BETWEEN 1 AND 4096),
    created_at INTEGER NOT NULL,
    PRIMARY KEY(workspace,resolution_id),
    UNIQUE(workspace,operation_id),
    FOREIGN KEY(workspace,operation_id) REFERENCES external_operations(workspace,operation_id)
) STRICT;
";
