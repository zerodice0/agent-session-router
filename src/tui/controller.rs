use std::{
    collections::{HashSet, VecDeque},
    path::PathBuf,
    sync::MutexGuard,
    task::Poll,
    time::Duration,
};

use tokio::{sync::mpsc, task::JoinSet};
use uuid::Uuid;

use crate::{
    client::{
        ClientConfig, ClientConnectionState, ClientError, ClientEvent, ClientEvents, RouterClient,
    },
    onboarding::{
        OnboardingProvider,
        issue::{IssuedPrompt, PromptOptions, issue_prompt},
    },
    process::{HealthProbe, ReqwestHealthProbe, RuntimeStore},
    protocol::{
        ClientMessage, HistoryPage, RouterErrorCode, ServerMessage, WorkspaceEvent, WorkspaceName,
    },
    tasks::TaskDetail,
};

use super::{
    SharedState, UiCommand, UiError, UiExit, UiNotice, UiOptions,
    state::{
        Connection, EventApplied, EventDisposition, FieldId, FormKind, MutationStage, Notice,
        PreparedMutation, PreviousHistoryLoad, ReadTicket, StateError, SwitchPhase, UiState,
        WorkspaceStamp,
    },
};

const PAGE_SIZE: u16 = 100;
// Reserve the fourth read slot for the action lane's fresh task/stop check.
const BACKGROUND_READS: usize = 3;

fn lock(state: &SharedState) -> MutexGuard<'_, UiState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn request_id() -> String {
    Uuid::new_v4().to_string()
}

fn client_code(error: &ClientError) -> Option<RouterErrorCode> {
    match error {
        ClientError::Router(code) => Some(*code),
        _ => None,
    }
}

fn uncertain(error: &ClientError) -> bool {
    !matches!(error, ClientError::Router(code) if !matches!(code,
        RouterErrorCode::RequestTimeout | RouterErrorCode::LeaveUnconfirmed))
}

fn notice(state: &mut UiState, message: &'static str, error: Option<RouterErrorCode>) {
    state.notice = Some(Notice { message, error });
    state.mark_dirty();
}

fn form_error(state: &mut UiState, error: StateError) {
    state.form_busy = false;
    if let Some(form) = state.form_mut() {
        form.error = Some(error);
    }
    notice(state, super::view::state_error(error), None);
}

/// Poll dispatch while holding the same short lock used to accept detach.
/// No guard survives a Pending result. This closes the prepare/spawn/send race:
/// detach-first cannot enqueue a new RPC, while tracked-request-first is visible
/// to the input layer's pending-request confirmation.
async fn attached<F: Future>(state: &SharedState, future: F) -> Option<F::Output> {
    tokio::pin!(future);
    std::future::poll_fn(|context| {
        let state = lock(state);
        if state.detaching {
            return Poll::Ready(None);
        }
        future.as_mut().poll(context).map(Some)
    })
    .await
}

// Dropping a UI future must end the real requester connection, not merely drop a
// clone of its command sender. Normal shutdown awaits close; this is the abort fence.
struct ClientLifetime(Option<RouterClient>);
impl Drop for ClientLifetime {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            tokio::spawn(async move {
                let _ = client.close().await;
            });
        }
    }
}

#[derive(Clone)]
struct Fence {
    client: u64,
    epoch: u64,
    stamp: Option<WorkspaceStamp>,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
enum ReadKind {
    Workspaces,
    Members,
    Tasks,
    Detail,
    TaskHistory,
    Recent,
    Previous,
    Next,
    Sync,
    Pending(i64),
}

struct ReadDone {
    generation: u64,
    fence: Fence,
    kind: ReadKind,
    ticket: Option<ReadTicket>,
    revision: u64,
    result: Result<ReadValue, ClientError>,
}
enum ReadValue {
    Wire(ServerMessage),
    History(HistoryPage),
    Previous(PreviousHistoryLoad),
    Subscription {
        events: Vec<WorkspaceEvent>,
        next: i64,
        live: bool,
    },
    Pending {
        task: TaskDetail,
        request_id: String,
        execution_observed: bool,
    },
}

enum ActionValue {
    Preflight(Result<TaskDetail, ClientError>),
    Wire {
        request_id: String,
        chat: bool,
        result: Result<ServerMessage, ClientError>,
    },
    Switch(SwitchResult),
    Replacement(Result<(ClientLifetime, ClientEvents), ClientError>),
    Invite(Result<IssuedPrompt, &'static str>),
    Stop(Result<(), &'static str>),
}
struct ActionDone {
    fence: Fence,
    value: ActionValue,
}
enum SwitchResult {
    Joined(WorkspaceName),
    Restored(ClientError),
    RecoveryFailed(ClientError),
    LeaveUnconfirmed,
}

struct Controller {
    config: ClientConfig,
    options: UiOptions,
    state: SharedState,
    client: RouterClient,
    lifetime: ClientLifetime,
    client_generation: u64,
    epoch: u64,
    readers: JoinSet<ReadDone>,
    read_generation: u64,
    actions: JoinSet<ActionDone>,
    active: HashSet<ReadKind>,
    queued: VecDeque<ReadKind>,
    reset_subscription: bool,
    gap_revision: u64,
    initial_selection: bool,
    requested: Option<WorkspaceName>,
    replacing: bool,
    restore_pending: Option<WorkspaceName>,
    pending_ack: Option<(WorkspaceStamp, i64)>,
}

/// The sole UI network owner. Workers never hold a `SharedState` lock across await.
pub async fn run(
    config: ClientConfig,
    options: UiOptions,
    state: SharedState,
    mut commands: mpsc::Receiver<UiCommand>,
    notices: mpsc::Sender<UiNotice>,
) -> Result<(), UiError> {
    let connecting = RouterClient::connect(config.clone());
    tokio::pin!(connecting);
    let (client, mut events) = loop {
        tokio::select! {
            result = &mut connecting => break result.map_err(|error| UiError::new(
                client_code(&error).map_or("connection_failed", RouterErrorCode::as_str),
                "Unable to connect to the router; the router was not stopped.",
            ))?,
            () = notices.closed() => return Ok(()),
            command = commands.recv() => match command {
                None => return Ok(()),
                Some(_) => form_error(&mut lock(&state), StateError::NotConnected),
            },
        }
    };
    let mut connection = client.connection_state();
    let mut controller = Controller {
        requested: options.workspace.clone(),
        config,
        options,
        state,
        lifetime: ClientLifetime(Some(client.clone())),
        client,
        client_generation: 0,
        epoch: 0,
        readers: JoinSet::new(),
        read_generation: 0,
        actions: JoinSet::new(),
        active: HashSet::new(),
        queued: VecDeque::new(),
        reset_subscription: false,
        gap_revision: 0,
        initial_selection: true,
        replacing: false,
        restore_pending: None,
        pending_ack: None,
    };
    controller.connection(&connection.borrow_and_update());
    let mut poll = tokio::time::interval(Duration::from_secs(5));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ack_retry = tokio::time::interval(Duration::from_millis(20));
    ack_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let outcome = loop {
        if commands.is_closed() || notices.is_closed() || lock(&controller.state).detaching {
            lock(&controller.state).detaching = true;
            break Ok(());
        }
        controller.flush_ack();
        controller.schedule();
        tokio::select! {
            () = notices.closed() => break Ok(()),
            command = commands.recv() => {
                let Some(command) = command else { break Ok(()); };
                controller.command(command);
            },
            changed = connection.changed(), if !controller.replacing => {
                if changed.is_err() { break Err(UiError::new("connection_closed", "Router connection closed.")); }
                controller.connection(&connection.borrow_and_update());
            },
            item = events.recv(), if !controller.replacing => {
                let Some(item) = item else { break Ok(()); };
                controller.event(item.event);
            },
            done = controller.readers.join_next(), if !controller.readers.is_empty() => {
                match done {
                    Some(Ok(done)) => controller.read_done(done),
                    Some(Err(error)) if error.is_cancelled() => {},
                    Some(Err(_)) => break Err(UiError::new("worker_failed", "A router read worker stopped.")),
                    None => {},
                }
            },
            done = controller.actions.join_next(), if !controller.actions.is_empty() => {
                match done {
                    Some(Ok(_)) if commands.is_closed() || notices.is_closed() => {
                        lock(&controller.state).detaching = true;
                        break Ok(());
                    },
                    Some(Ok(done)) => {
                        match controller.action_done(done, &notices) {
                            ActionTransition::Continue => {},
                            ActionTransition::Replace(new_events) => {
                                events = new_events;
                                connection = controller.client.connection_state();
                                controller.connection(&connection.borrow_and_update());
                            },
                            ActionTransition::Exit => break Ok(()),
                        }
                    },
                    Some(Err(_)) => break Err(UiError::new("worker_failed", "A router action worker stopped; inspect server state before retrying.")),
                    None => {},
                }
            },
            _ = ack_retry.tick(), if controller.pending_ack.is_some() => {},
            _ = poll.tick() => {
                controller.enqueue_poll(ReadKind::Workspaces);
                controller.enqueue_poll(ReadKind::Members);
            },
        }
    };
    lock(&controller.state).detaching = true;
    controller.cancel_reads();
    controller.actions.abort_all();
    let _ = controller.client.close().await;
    while controller.readers.join_next().await.is_some() {}
    while controller.actions.join_next().await.is_some() {}
    controller.lifetime.0 = None;
    outcome
}

enum ActionTransition {
    Continue,
    Replace(ClientEvents),
    Exit,
}

impl Controller {
    fn fence(&self) -> Fence {
        Fence {
            client: self.client_generation,
            epoch: self.epoch,
            stamp: lock(&self.state).stamp(),
        }
    }

    fn accepts(&self, fence: &Fence) -> bool {
        let state = lock(&self.state);
        fence.client == self.client_generation
            && fence.epoch == self.epoch
            && matches!(state.connection, Connection::Connected { epoch } if epoch == self.epoch)
            && fence
                .stamp
                .as_ref()
                .is_none_or(|stamp| state.accepts(stamp))
    }

    fn enqueue(&mut self, kind: ReadKind) {
        // At most the nine fixed read kinds plus one per bounded pending task.
        // Remove resolved request refreshes before accepting another task key.
        let state = lock(&self.state);
        self.queued.retain(|queued| match queued {
            ReadKind::Pending(id) => state
                .pending_requests
                .values()
                .any(|request| request.task_id == *id),
            _ => true,
        });
        if let ReadKind::Pending(id) = kind
            && !state
                .pending_requests
                .values()
                .any(|request| request.task_id == id)
        {
            return;
        }
        drop(state);
        if display_history(kind) {
            self.queued.retain(|queued| !display_history(*queued));
        }
        if !self.queued.contains(&kind) {
            self.queued.push_back(kind);
        }
    }

    fn enqueue_poll(&mut self, kind: ReadKind) {
        if !self.active.contains(&kind) {
            self.enqueue(kind);
        }
    }

    fn connection(&mut self, connection: &ClientConnectionState) {
        let connection = match connection {
            ClientConnectionState::Connecting => Connection::Connecting,
            ClientConnectionState::Connected { epoch } => {
                self.epoch = *epoch;
                Connection::Connected { epoch: *epoch }
            }
            ClientConnectionState::Reconnecting => Connection::Reconnecting,
            ClientConnectionState::Closed { reason } => Connection::Closed { reason: *reason },
        };
        let refresh = {
            let mut state = lock(&self.state);
            state.header.is_admin = matches!(connection, Connection::Connected { .. })
                && self.client.operator_is_admin() == Some(true);
            state.connection_changed(connection)
        };
        if refresh {
            if let Some(workspace) = self.restore_pending.take() {
                self.restore_previous(Some(workspace));
            }
            self.enqueue(ReadKind::Workspaces);
            self.enqueue(ReadKind::Members);
            self.enqueue(ReadKind::Tasks);
            let pending: Vec<_> = lock(&self.state)
                .pending_requests
                .values()
                .map(|request| request.task_id)
                .collect();
            for task_id in pending {
                self.enqueue(ReadKind::Pending(task_id));
            }
            self.enqueue(ReadKind::Detail);
            self.enqueue(ReadKind::TaskHistory);
            let history = {
                let state = lock(&self.state);
                state.workspace.as_ref().map(|_| {
                    if state.chat.applied_cursor.is_some() {
                        ReadKind::Sync
                    } else {
                        ReadKind::Recent
                    }
                })
            };
            if let Some(kind) = history {
                self.enqueue(kind);
            }
        }
    }

    fn event(&mut self, event: ClientEvent) {
        match event {
            ClientEvent::WorkspaceEvent(event) => {
                let stamp = lock(&self.state).stamp();
                if let Some(stamp) = stamp {
                    let effect = lock(&self.state).apply_event(&stamp, event);
                    self.effect(&stamp, &effect);
                }
            }
            ClientEvent::SendResult(result) => {
                let mut state = lock(&self.state);
                if let Some(stamp) = state.stamp()
                    && result
                        .workspace
                        .as_ref()
                        .is_none_or(|workspace| workspace == &stamp.workspace)
                    && state.resolve_request(&stamp, &result.request_id)
                {
                    notice(
                        &mut state,
                        "Request resolved; its result does not imply task completion.",
                        result.error,
                    );
                    drop(state);
                    self.enqueue(ReadKind::Detail);
                    self.enqueue(ReadKind::TaskHistory);
                }
            }
            ClientEvent::Closed(code) => {
                self.connection(&ClientConnectionState::Closed { reason: Some(code) });
            }
            // Membership is committed only by the confirmed switch operation. Operator
            // clients do not execute deliveries or synthesize native stop evidence.
            ClientEvent::MembershipChanged { .. }
            | ClientEvent::Delivery { .. }
            | ClientEvent::WorkCancelled { .. }
            | ClientEvent::TaskAttemptChanged { .. } => {}
        }
    }

    fn effect(&mut self, stamp: &WorkspaceStamp, effect: &EventApplied) {
        if let Some(seq) = effect.ack {
            // ACKs are cumulative applied cursors, not one queue entry per body.
            // A 100-event RPC page must not fill the client's 64-command queue.
            match self.pending_ack.as_mut() {
                Some((pending_stamp, cursor)) if pending_stamp == stamp => {
                    *cursor = (*cursor).max(seq);
                }
                _ => self.pending_ack = Some((stamp.clone(), seq)),
            }
        }
        if matches!(
            effect.disposition,
            EventDisposition::Gap { .. } | EventDisposition::Invalid
        ) {
            self.gap_revision = self.gap_revision.wrapping_add(1);
            self.reset_subscription = true;
            self.enqueue(ReadKind::Sync);
        }
        if effect.refresh_task_detail {
            self.enqueue(ReadKind::Detail);
        }
        if effect.request_resolved {
            self.enqueue(ReadKind::Detail);
            self.enqueue(ReadKind::TaskHistory);
        }
        let refresh_tasks = lock(&self.state).tasks.refresh_required;
        if refresh_tasks {
            self.enqueue(ReadKind::Tasks);
        }
    }

    fn flush_ack(&mut self) {
        let Some((stamp, seq)) = self.pending_ack.take() else {
            return;
        };
        if !lock(&self.state).accepts(&stamp) {
            return;
        }
        if self.client.ack_event(stamp.workspace.clone(), seq).is_err() {
            // Queue pressure delays the client's replay watermark; it does not
            // invalidate already-applied live history or require resubscription.
            // Retain one bounded watermark and retry without blocking event drain.
            self.pending_ack = Some((stamp, seq));
        }
    }

    fn schedule(&mut self) {
        if !matches!(lock(&self.state).connection, Connection::Connected { .. })
            || lock(&self.state).switching.is_some()
        {
            return;
        }
        let attempts = self.queued.len();
        for _ in 0..attempts {
            if self.readers.len() >= BACKGROUND_READS {
                break;
            }
            let Some(kind) = self.queued.pop_front() else {
                break;
            };
            if self.active.contains(&kind) {
                self.queued.push_back(kind);
                continue;
            }
            if display_history(kind) && self.active.iter().any(|active| display_history(*active)) {
                self.queued.push_back(kind);
                continue;
            }
            if !lock(&self.state).chat.synchronized
                && !matches!(
                    kind,
                    ReadKind::Workspaces | ReadKind::Recent | ReadKind::Sync
                )
            {
                self.queued.push_back(kind);
                continue;
            }
            if let Some(job) = self.read_job(kind) {
                self.active.insert(kind);
                self.readers.spawn(job);
            }
        }
    }

    fn read_job(
        &mut self,
        kind: ReadKind,
    ) -> Option<impl Future<Output = ReadDone> + Send + 'static + use<>> {
        let generation = self.read_generation;
        let fence = self.fence();
        let mut state = lock(&self.state);
        let stamp = state.stamp();
        if kind != ReadKind::Workspaces && stamp.is_none() {
            return None;
        }
        let mut ticket = None;
        let mut revision = 0;
        let pending_request = if let ReadKind::Pending(task_id) = kind {
            Some(
                state
                    .pending_requests
                    .values()
                    .find(|request| request.task_id == task_id)?
                    .request_id
                    .clone(),
            )
        } else {
            None
        };
        let mut previous = None;
        let message = match kind {
            ReadKind::Workspaces => {
                revision = state.workspaces.begin_load();
                Some(ClientMessage::WorkspaceList {
                    request_id: request_id(),
                    after: state.workspaces.pagination.after.clone(),
                    limit: Some(PAGE_SIZE),
                })
            }
            ReadKind::Members => {
                ticket = state.begin_members_load();
                ticket.as_ref()?;
                Some(ClientMessage::WorkspaceMembers {
                    request_id: request_id(),
                })
            }
            ReadKind::Tasks => {
                ticket = Some(state.tasks.begin_load(stamp.clone()?));
                Some(ClientMessage::TaskList {
                    request_id: request_id(),
                    workspace: stamp.as_ref()?.workspace.clone(),
                    states: Some(state.tasks.filter.states.clone()),
                    assigned_agent_id: state.tasks.filter.assigned_agent_id.clone(),
                    after: state.tasks.pagination.after,
                    limit: Some(PAGE_SIZE),
                })
            }
            ReadKind::Detail => {
                let task_id = state.tasks.selected_id?;
                ticket = Some(state.tasks.begin_detail_load(stamp.clone()?));
                Some(ClientMessage::TaskGet {
                    request_id: request_id(),
                    workspace: stamp.as_ref()?.workspace.clone(),
                    task_id,
                })
            }
            ReadKind::TaskHistory => {
                let task_id = state.tasks.selected_id?;
                ticket = Some(state.tasks.begin_history_load(stamp.clone()?));
                Some(ClientMessage::TaskHistory {
                    request_id: request_id(),
                    workspace: stamp.as_ref()?.workspace.clone(),
                    task_id,
                    after: state.tasks.history_pagination.after,
                    limit: Some(PAGE_SIZE),
                })
            }
            ReadKind::Pending(_) => None,
            ReadKind::Recent => {
                ticket = Some(state.chat.begin_history_load(stamp.clone()?));
                None
            }
            ReadKind::Previous => {
                let oldest = state.chat.buffer.events().front()?.seq;
                if oldest <= 1 {
                    notice(&mut state, "Beginning of workspace history.", None);
                    return None;
                }
                previous = Some(PreviousHistoryLoad::new(oldest));
                state.chat.enter_past();
                ticket = Some(state.chat.begin_history_load(stamp.clone()?));
                None
            }
            ReadKind::Next => {
                let after = state.chat.buffer.events().back()?.seq;
                state.chat.enter_past();
                ticket = Some(state.chat.begin_history_load(stamp.clone()?));
                Some(ClientMessage::WorkspaceHistory {
                    request_id: request_id(),
                    after: Some(after),
                    limit: Some(PAGE_SIZE),
                })
            }
            ReadKind::Sync => {
                state.chat.applied_cursor?;
                revision = self.gap_revision;
                None
            }
        };
        let after = state.chat.applied_cursor.unwrap_or(0);
        let reset = kind == ReadKind::Sync && std::mem::take(&mut self.reset_subscription);
        state.mark_dirty();
        drop(state);
        let client = self.client.clone();
        Some(async move {
            let result = async {
                if let Some(message) = message {
                    return client.call(message).await.map(ReadValue::Wire);
                }
                match kind {
                    ReadKind::Pending(task_id) => {
                        let workspace = fence
                            .stamp
                            .as_ref()
                            .expect("pending workspace")
                            .workspace
                            .clone();
                        let request_id = pending_request.expect("pending request captured");
                        let task = client.task_get(workspace.clone(), task_id).await?;
                        let mut execution_observed = task
                            .current_attempt
                            .as_ref()
                            .or(task.last_attempt.as_ref())
                            .is_some_and(|attempt| attempt.work_request_id == request_id);
                        let mut after = None;
                        loop {
                            let page = client
                                .task_history(workspace.clone(), task_id, after, Some(PAGE_SIZE))
                                .await?;
                            execution_observed |= page.events.iter().any(|event| {
                                event
                                    .attempt
                                    .as_ref()
                                    .is_some_and(|attempt| attempt.work_request_id == request_id)
                            });
                            if !page.has_more {
                                break;
                            }
                            if after.is_some_and(|after| page.next_cursor <= after) {
                                return Err(ClientError::Router(RouterErrorCode::InvalidMessage));
                            }
                            after = Some(page.next_cursor);
                        }
                        Ok(ReadValue::Pending {
                            task,
                            request_id,
                            execution_observed,
                        })
                    }
                    ReadKind::Recent => client
                        .workspace_history(None, Some(PAGE_SIZE))
                        .await
                        .map(ReadValue::History),
                    ReadKind::Previous => {
                        let mut load = previous.expect("previous history boundary captured");
                        while !load.done {
                            let page = client
                                .workspace_history(Some(load.after), Some(PAGE_SIZE))
                                .await?;
                            if fence
                                .stamp
                                .as_ref()
                                .is_none_or(|stamp| stamp.workspace != page.workspace)
                            {
                                return Err(ClientError::Router(RouterErrorCode::InvalidMessage));
                            }
                            load.absorb(page).map_err(|_| {
                                ClientError::Router(RouterErrorCode::InvalidMessage)
                            })?;
                        }
                        Ok(ReadValue::Previous(load))
                    }
                    ReadKind::Sync => {
                        if reset {
                            unsubscribe(&client).await?;
                        }
                        let (events, next, live) = client.workspace_subscribe(after).await?;
                        Ok(ReadValue::Subscription { events, next, live })
                    }
                    _ => unreachable!("wire read has a message"),
                }
            }
            .await;
            ReadDone {
                generation,
                fence,
                kind,
                ticket,
                revision,
                result,
            }
        })
    }

    fn read_done(&mut self, done: ReadDone) {
        if done.generation != self.read_generation {
            return;
        }
        self.active.remove(&done.kind);
        if !self.accepts(&done.fence) {
            return;
        }
        let result = match done.result {
            Ok(ReadValue::Wire(ServerMessage::Error { code, .. })) => {
                Err(ClientError::Router(code))
            }
            value => value,
        };
        let mut state = lock(&self.state);
        if display_history(done.kind)
            && done
                .ticket
                .as_ref()
                .is_none_or(|ticket| !state.accepts_history(ticket))
        {
            return;
        }
        let mut effects = Vec::new();
        let mut select = None;
        let mut sync = false;
        let mut detail = false;
        match result {
            Err(error) => {
                match done.kind {
                    ReadKind::Workspaces => state.workspaces.fail_load(done.revision),
                    ReadKind::Members => {
                        if let Some(ticket) = &done.ticket {
                            state.fail_members_load(ticket);
                        }
                    }
                    ReadKind::Tasks => {
                        if let Some(ticket) = &done.ticket {
                            state.tasks.fail_load(ticket.revision);
                        }
                    }
                    _ => {}
                }
                notice(
                    &mut state,
                    if display_history(done.kind) {
                        "History display read failed; live updates are unchanged. Use End to reload."
                    } else {
                        "Router read failed; refresh to try again."
                    },
                    client_code(&error),
                );
            }
            Ok(ReadValue::Wire(ServerMessage::Workspaces {
                workspaces,
                next_cursor,
                has_more,
                ..
            })) => {
                if state
                    .workspaces
                    .apply(done.revision, workspaces, next_cursor, has_more)
                    && self.initial_selection
                {
                    self.initial_selection = false;
                    select = self
                        .requested
                        .take()
                        .or_else(|| state.workspaces.rows.first().map(|row| row.name.clone()));
                }
                let created = state
                    .mutation
                    .as_ref()
                    .filter(|mutation| {
                        mutation.kind == FormKind::WorkspaceCreate
                            && mutation.stage == MutationStage::Uncertain
                    })
                    .map(|mutation| mutation.stamp.workspace.clone());
                if created
                    .is_some_and(|name| state.workspaces.rows.iter().any(|row| row.name == name))
                {
                    state.close_form();
                    state.mutation = None;
                    notice(
                        &mut state,
                        "Workspace exists; creation was not replayed. Select it to switch.",
                        None,
                    );
                }
                state.mark_dirty();
            }
            Ok(ReadValue::Wire(ServerMessage::Agents {
                workspace, agents, ..
            })) => {
                if let Some(ticket) = &done.ticket
                    && ticket.stamp.workspace == workspace
                {
                    state.apply_members(ticket, agents);
                }
            }
            Ok(ReadValue::Wire(ServerMessage::Tasks {
                tasks,
                next_cursor,
                has_more,
                ..
            })) => {
                if let Some(ticket) = &done.ticket {
                    detail = state.apply_task_page(ticket, tasks, next_cursor, has_more);
                }
            }
            Ok(ReadValue::Pending {
                task,
                request_id,
                execution_observed,
            }) => {
                if execution_observed
                    && let Some(request) = state.pending_requests.get_mut(&request_id)
                {
                    request.stage = super::state::RequestStage::ExecutionObserved;
                }
                detail = state.tasks.observe(task.summary);
                state.mark_dirty();
            }
            Ok(ReadValue::Wire(ServerMessage::Task { task, .. })) => {
                if let Some(ticket) = &done.ticket {
                    if state
                        .mutation
                        .as_ref()
                        .is_some_and(|mutation| mutation.stage == MutationStage::Conflict)
                        && let Some(form) = state.form_mut()
                    {
                        let _ = form.check_latest(&task);
                    }
                    state.apply_task_detail(ticket, task);
                }
            }
            Ok(ReadValue::Wire(ServerMessage::TaskHistory { page, .. })) => {
                if let Some(ticket) = &done.ticket {
                    state.apply_task_history(ticket, page);
                }
            }
            Ok(ReadValue::Wire(ServerMessage::WorkspaceHistory { page, .. })) => {
                if let Some(ticket) = &done.ticket
                    && state.apply_past_page(ticket, page).is_err()
                {
                    notice(&mut state, "Invalid workspace history page.", None);
                }
            }
            Ok(ReadValue::History(page)) => {
                if let Some(ticket) = &done.ticket {
                    match state.apply_recent_page(ticket, page) {
                        Ok(applied) => {
                            effects = applied;
                            sync = state.chat.applied_cursor.is_some();
                        }
                        Err(_) => notice(&mut state, "Invalid workspace history page.", None),
                    }
                }
            }
            Ok(ReadValue::Previous(load)) => {
                if let Some(ticket) = &done.ticket
                    && state.apply_previous_page(ticket, load).is_err()
                {
                    notice(&mut state, "Invalid workspace history page.", None);
                }
            }
            Ok(ReadValue::Subscription { events, next, live }) => {
                if let Some(stamp) = &done.fence.stamp {
                    for event in events {
                        effects.push(state.apply_event(stamp, event));
                    }
                    let valid = !effects.iter().any(|effect| {
                        matches!(
                            effect.disposition,
                            EventDisposition::Gap { .. }
                                | EventDisposition::Invalid
                                | EventDisposition::StaleWorkspace
                        )
                    });
                    if !live
                        || !valid
                        || done.revision != self.gap_revision
                        || !state.subscription_live(stamp, next)
                    {
                        sync = true;
                    }
                }
            }
            Ok(_) => notice(
                &mut state,
                "Unexpected router response.",
                Some(RouterErrorCode::InvalidMessage),
            ),
        }
        drop(state);
        if let Some(stamp) = &done.fence.stamp {
            for effect in effects {
                self.effect(stamp, &effect);
            }
        }
        if sync {
            self.enqueue(ReadKind::Sync);
        }
        if detail {
            self.enqueue(ReadKind::Detail);
        }
        if let Some(workspace) = select {
            self.switch(workspace);
        }
    }

    fn command(&mut self, command: UiCommand) {
        match command {
            UiCommand::LoadWorkspaces => self.enqueue(ReadKind::Workspaces),
            UiCommand::LoadMembers => self.enqueue(ReadKind::Members),
            UiCommand::LoadTasks => self.enqueue(ReadKind::Tasks),
            UiCommand::LoadTaskDetail => self.enqueue(ReadKind::Detail),
            UiCommand::LoadTaskHistory => self.enqueue(ReadKind::TaskHistory),
            UiCommand::RecentHistory => self.enqueue(ReadKind::Recent),
            UiCommand::PreviousHistory => self.enqueue(ReadKind::Previous),
            UiCommand::NextHistory => self.enqueue(ReadKind::Next),
            UiCommand::SelectWorkspace(workspace) => self.switch(workspace),
            UiCommand::SubmitForm => self.submit_form(),
            UiCommand::SubmitChat => self.chat(false),
            UiCommand::RetryChat => self.chat(true),
            UiCommand::RetryMutation => self.retry(),
            UiCommand::ConfirmStop => self.stop(),
        }
    }

    fn cancel_reads(&mut self) {
        self.read_generation = self.read_generation.wrapping_add(1);
        self.gap_revision = self.gap_revision.wrapping_add(1);
        self.readers.abort_all();
        self.active.clear();
        self.queued.clear();
        lock(&self.state).invalidate_reads();
    }

    fn switch(&mut self, target: WorkspaceName) {
        let mut state = lock(&self.state);
        if !self.actions.is_empty() {
            form_error(&mut state, StateError::Busy);
            return;
        }
        if state.workspace.as_ref() == Some(&target) {
            return;
        }
        if let Err(error) = state.begin_switch(target.clone()) {
            form_error(&mut state, error);
            return;
        }
        state.chat.synchronized = false;
        let old = state.workspace.clone();
        drop(state);
        // Membership-relative reads must finish/cancel before the membership changes.
        self.cancel_reads();
        let fence = self.fence();
        let client = self.client.clone();
        let state = self.state.clone();
        self.actions.spawn(async move {
            let value = switch_workspace(&client, &state, old, target).await;
            ActionDone {
                fence,
                value: ActionValue::Switch(value),
            }
        });
    }

    fn submit_form(&mut self) {
        if !self.actions.is_empty() {
            form_error(&mut lock(&self.state), StateError::Busy);
            return;
        }
        let fence = self.fence();
        let mut state = lock(&self.state);
        if state.detaching {
            form_error(&mut state, StateError::Busy);
            return;
        }
        let Some(form) = state.form() else {
            state.form_busy = false;
            return;
        };
        let kind = form.kind;
        if kind == FormKind::TaskFilter {
            match form.task_filter() {
                Ok(filter) => {
                    state.tasks.set_filter(filter);
                    state.close_form();
                    state.mark_dirty();
                    drop(state);
                    self.enqueue(ReadKind::Tasks);
                }
                Err(error) => form_error(&mut state, error),
            }
            return;
        }
        if kind == FormKind::Invite {
            if !state.can_invite() {
                form_error(&mut state, StateError::PermissionDenied);
                return;
            }
            if let Err(error) = state.writable() {
                form_error(&mut state, error);
                return;
            }
            let Some(data_dir) = self.options.owned_data_dir.clone() else {
                form_error(&mut state, StateError::PermissionDenied);
                return;
            };
            let form = state.form().expect("active invite form");
            let provider = match form.value(FieldId::Provider).trim() {
                "" => None,
                "claude-code" => Some(OnboardingProvider::ClaudeCode),
                "codex-cli" => Some(OnboardingProvider::CodexCli),
                "omp" => Some(OnboardingProvider::Omp),
                _ => {
                    form_error(&mut state, StateError::InvalidInput);
                    return;
                }
            };
            let nonempty =
                |value: &str| (!value.trim().is_empty()).then(|| value.trim().to_owned());
            let options = PromptOptions {
                workspace: state.workspace.clone().expect("invite workspace"),
                create_workspace: false,
                name: nonempty(form.value(FieldId::Profile)),
                provider,
                endpoints: form
                    .value(FieldId::Endpoints)
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(str::to_owned)
                    .collect(),
                ca_file: nonempty(form.value(FieldId::CaFile))
                    .map(PathBuf::from)
                    .or_else(|| self.config.ca_file.clone()),
            };
            state.form_busy = true;
            let shared = self.state.clone();
            drop(state);
            self.actions.spawn(async move {
                ActionDone {
                    fence,
                    value: ActionValue::Invite(
                        attached(&shared, issue_prompt(&data_dir, options))
                            .await
                            .map_or(Err("invitation_cancelled"), |result| {
                                result.map_err(|error| error.0)
                            }),
                    ),
                }
            });
            return;
        }
        if kind != FormKind::WorkspaceCreate
            && let Err(error) = state.writable()
        {
            form_error(&mut state, error);
            return;
        }
        let baseline = state.form().and_then(|form| form.baseline.clone());
        state.form_busy = true;
        if kind != FormKind::TaskNote
            && let Some(baseline) = baseline
        {
            let client = self.client.clone();
            let Some(stamp) = &fence.stamp else {
                form_error(&mut state, StateError::NoWorkspace);
                return;
            };
            let workspace = stamp.workspace.clone();
            drop(state);
            self.actions.spawn(async move {
                ActionDone {
                    fence,
                    value: ActionValue::Preflight(
                        client.task_get(workspace, baseline.task_id).await,
                    ),
                }
            });
        } else {
            drop(state);
            self.send_form();
        }
    }

    fn send_form(&mut self) {
        let fence = self.fence();
        let shared = self.state.clone();
        let mut state = lock(&shared);
        if state.detaching {
            form_error(&mut state, StateError::Busy);
            return;
        }
        let result = state
            .form()
            .ok_or(StateError::InvalidInput)
            .and_then(|form| state.prepare_form(form));
        let mut mutation = match result {
            Ok(value) => value,
            Err(error) => {
                form_error(&mut state, error);
                return;
            }
        };
        if mutation.kind == FormKind::TaskRequest
            && let Err(error) = state.track_request(&mutation)
        {
            form_error(&mut state, error);
            return;
        }
        let message = match mutation.submit() {
            Ok(message) => message,
            Err(error) => {
                form_error(&mut state, error);
                return;
            }
        };
        state.mutation = Some(mutation);
        state.form_busy = true;
        state.mark_dirty();
        self.call_action(fence, message, false);
        drop(state);
    }

    fn call_action(&mut self, fence: Fence, message: ClientMessage, chat: bool) {
        let client = self.client.clone();
        let request_id = message.request_id().expect("UI call request ID").to_owned();
        let shared = self.state.clone();
        self.actions.spawn(async move {
            ActionDone {
                fence,
                value: ActionValue::Wire {
                    request_id,
                    chat,
                    result: attached(&shared, client.call(message))
                        .await
                        .unwrap_or(Err(ClientError::Closed)),
                },
            }
        });
    }

    fn retry(&mut self) {
        let fence = self.fence();
        let mut state = lock(&self.state);
        if !self.actions.is_empty() || !retry_ready(&state) || state.composer.is_locked() {
            form_error(&mut state, StateError::Busy);
            return;
        }
        let result = state
            .mutation
            .as_mut()
            .ok_or(StateError::NotUncertain)
            .and_then(PreparedMutation::retry);
        match result {
            Ok(message) => {
                state.form_busy = true;
                state.mark_dirty();
                drop(state);
                self.call_action(fence, message, false);
            }
            Err(error) => form_error(&mut state, error),
        }
    }

    fn chat(&mut self, retry: bool) {
        let fence = self.fence();
        let mut state = lock(&self.state);
        if !self.actions.is_empty() {
            form_error(&mut state, StateError::Busy);
            return;
        }
        let result = if retry {
            if !retry_ready(&state)
                || state.mutation.as_ref().is_some_and(|mutation| {
                    matches!(
                        mutation.stage,
                        MutationStage::InFlight | MutationStage::Uncertain
                    )
                })
            {
                Err(StateError::Synchronizing)
            } else {
                state.composer.retry()
            }
        } else {
            state.writable().and_then(|()| state.composer.prepare())
        };
        match result {
            Ok(message) => {
                state.form_busy = true;
                state.mark_dirty();
                drop(state);
                self.call_action(fence, message, true);
            }
            Err(error) => form_error(&mut state, error),
        }
    }

    fn stop(&mut self) {
        let state = lock(&self.state);
        if state.detaching || !state.stop_confirmed() || !self.actions.is_empty() {
            drop(state);
            form_error(&mut lock(&self.state), StateError::Busy);
            return;
        }
        let fence = Fence {
            client: self.client_generation,
            epoch: self.epoch,
            stamp: state.stamp(),
        };
        drop(state);
        let options = self.options.clone();
        let config = self.config.clone();
        let client = self.client.clone();
        let shared = self.state.clone();
        self.actions.spawn(async move {
            ActionDone {
                fence,
                value: ActionValue::Stop(
                    attached(&shared, verify_stop(&config, &options, &client))
                        .await
                        .unwrap_or(Err("stop_cancelled")),
                ),
            }
        });
    }

    fn action_done(
        &mut self,
        done: ActionDone,
        notices: &mpsc::Sender<UiNotice>,
    ) -> ActionTransition {
        if lock(&self.state).detaching {
            return ActionTransition::Exit;
        }
        // A replacement has its own client generation; its new epoch starts at one.
        if let ActionValue::Replacement(result) = done.value {
            if done.fence.client != self.client_generation {
                return ActionTransition::Continue;
            }
            match result {
                Ok((mut lifetime, events)) => {
                    self.client = lifetime.0.as_ref().expect("replacement client").clone();
                    self.lifetime.0 = lifetime.0.take();
                    self.replacing = false;
                    self.epoch = 0;
                    let mut state = lock(&self.state);
                    state.switching = None;
                    notice(
                        &mut state,
                        "Previous workspace restored. Select a workspace again to switch.",
                        None,
                    );
                    drop(state);
                    self.reset_subscription = true;
                    return ActionTransition::Replace(events);
                }
                Err(error) => {
                    let mut state = lock(&self.state);
                    state.switching = None;
                    state.header.is_admin = false;
                    state.connection_changed(Connection::Closed {
                        reason: client_code(&error),
                    });
                    notice(
                        &mut state,
                        "Unable to restore membership. Detach and reattach; no target workspace was joined.",
                        client_code(&error),
                    );
                    return ActionTransition::Continue;
                }
            }
        }
        if matches!(
            &done.value,
            ActionValue::Switch(SwitchResult::LeaveUnconfirmed)
        ) {
            self.replace_client();
            return ActionTransition::Continue;
        }
        if !self.accepts(&done.fence) {
            let mut state = lock(&self.state);
            state.form_busy = false;
            if let Some(mutation) = state.mutation.as_mut() {
                mutation.mark_uncertain();
            }
            state.composer.mark_uncertain();
            if state
                .mutation
                .as_ref()
                .is_some_and(|mutation| mutation.kind == FormKind::TaskRequest)
            {
                state.close_form();
                state.mutation = None;
            }
            notice(
                &mut state,
                "Connection changed; inspect the authoritative state before retrying.",
                None,
            );
            let switching = matches!(&done.value, ActionValue::Switch(_));
            let old = state.workspace.clone();
            drop(state);
            if switching {
                self.restore_previous(old);
            } else {
                self.enqueue(ReadKind::Workspaces);
                self.enqueue(ReadKind::Tasks);
                self.enqueue(ReadKind::Detail);
                self.enqueue(ReadKind::TaskHistory);
            }
            return ActionTransition::Continue;
        }
        match done.value {
            ActionValue::Preflight(result) => {
                let mut state = lock(&self.state);
                match result {
                    Ok(task) => {
                        let result = state
                            .form_mut()
                            .ok_or(StateError::InvalidInput)
                            .and_then(|form| form.check_latest(&task));
                        let ticket = state
                            .stamp()
                            .map(|stamp| state.tasks.begin_detail_load(stamp));
                        if let Some(ticket) = ticket {
                            state.apply_task_detail(&ticket, task);
                        }
                        match result {
                            Ok(()) => {
                                drop(state);
                                self.send_form();
                            }
                            Err(error) => form_error(&mut state, error),
                        }
                    }
                    Err(error) => {
                        state.form_busy = false;
                        notice(
                            &mut state,
                            "Fresh task check failed; nothing was submitted.",
                            client_code(&error),
                        );
                    }
                }
            }
            ActionValue::Wire {
                request_id,
                chat,
                result,
            } => self.wire_done(&done.fence, &request_id, chat, result),
            ActionValue::Switch(result) => match result {
                SwitchResult::Joined(workspace) => {
                    lock(&self.state).commit_workspace(workspace);
                    self.reset_subscription = false;
                    self.enqueue(ReadKind::Recent);
                    self.enqueue(ReadKind::Members);
                    self.enqueue(ReadKind::Tasks);
                }
                SwitchResult::Restored(error) => {
                    let mut state = lock(&self.state);
                    state.switching = None;
                    notice(
                        &mut state,
                        "Workspace switch refused; previous workspace retained.",
                        client_code(&error),
                    );
                    drop(state);
                    self.reset_subscription = true;
                    self.enqueue(ReadKind::Sync);
                    self.enqueue(ReadKind::Workspaces);
                    self.enqueue(ReadKind::Members);
                    self.enqueue(ReadKind::Tasks);
                    self.enqueue(ReadKind::Detail);
                    self.enqueue(ReadKind::TaskHistory);
                }
                SwitchResult::RecoveryFailed(error) => {
                    let mut state = lock(&self.state);
                    state.switching = None;
                    state.header.is_admin = false;
                    state.connection_changed(Connection::Closed {
                        reason: client_code(&error),
                    });
                    notice(
                        &mut state,
                        "Membership recovery failed. Detach and reattach.",
                        client_code(&error),
                    );
                }
                SwitchResult::LeaveUnconfirmed => self.replace_client(),
            },
            ActionValue::Invite(result) => {
                let mut state = lock(&self.state);
                state.form_busy = false;
                match result {
                    Ok(prompt) => {
                        state.close_form();
                        if notices.try_send(UiNotice::Invite(prompt)).is_err() {
                            notice(
                                &mut state,
                                "Invitation issued but display unavailable; it was not issued again.",
                                None,
                            );
                        }
                    }
                    Err(code) => notice(&mut state, code, None),
                }
            }
            ActionValue::Stop(result) => {
                let mut state = lock(&self.state);
                state.form_busy = false;
                match result {
                    Ok(()) if state.stop_confirmed() => {
                        if notices
                            .try_send(UiNotice::Exit(UiExit::StopOwnedRouter))
                            .is_ok()
                        {
                            return ActionTransition::Exit;
                        }
                        notice(
                            &mut state,
                            "Stop confirmation could not be delivered; router remains running.",
                            None,
                        );
                    }
                    Ok(()) => {}
                    Err(code) => notice(&mut state, code, None),
                }
            }
            ActionValue::Replacement(_) => unreachable!(),
        }
        ActionTransition::Continue
    }

    fn wire_done(
        &mut self,
        fence: &Fence,
        request_id: &str,
        chat: bool,
        result: Result<ServerMessage, ClientError>,
    ) {
        let mut state = lock(&self.state);
        state.form_busy = false;
        let kind = state.mutation.as_ref().map(|mutation| mutation.kind);
        let mut select = None;
        let mut refresh = false;
        match result {
            Ok(ServerMessage::WorkspacePosted {
                request_id: id,
                workspace,
                ..
            }) if chat && id == request_id && state.workspace.as_ref() == Some(&workspace) => {
                state.composer.acknowledge(&id);
                notice(
                    &mut state,
                    "Message posted; chat does not start model execution.",
                    None,
                );
            }
            Ok(ServerMessage::TaskMutated {
                request_id: id,
                result,
                ..
            }) if !chat && id == request_id => {
                if let Some(stamp) = &fence.stamp
                    && state.apply_receipt(stamp, result).unwrap_or(false)
                {
                    state.close_form();
                    state.mutation = None;
                    notice(
                        &mut state,
                        if kind == Some(FormKind::TaskAssign) {
                            "Assignee changed; execution was not started."
                        } else {
                            "Task change confirmed by router."
                        },
                        None,
                    );
                    refresh = true;
                } else {
                    notice(
                        &mut state,
                        "Mutation receipt did not match the submitted operation.",
                        None,
                    );
                }
            }
            Ok(ServerMessage::Accepted {
                request_id: id,
                workspace,
                ..
            }) if kind == Some(FormKind::TaskRequest)
                && id == request_id
                && state.workspace.as_ref() == Some(&workspace) =>
            {
                state.request_accepted(&id);
                state.close_form();
                state.mutation = None;
                notice(
                    &mut state,
                    "Request accepted; waiting for observed execution. Accepted is not begun.",
                    None,
                );
            }
            Ok(ServerMessage::WorkspaceCreated {
                request_id: id,
                workspace,
            }) if kind == Some(FormKind::WorkspaceCreate) && id == request_id => {
                state.close_form();
                state.mutation = None;
                select = Some(workspace.name);
                notice(
                    &mut state,
                    "Workspace created. Switching may still be refused; creation is retained.",
                    None,
                );
            }
            Ok(ServerMessage::Error {
                code,
                current_version,
                ..
            }) => {
                if chat {
                    state.composer.reject(request_id);
                } else if code == RouterErrorCode::TaskConflict {
                    if kind == Some(FormKind::TaskRequest) {
                        state.pending_requests.remove(request_id);
                    }
                    if let Some(mutation) = state.mutation.as_mut() {
                        mutation.conflict(current_version);
                    }
                    if let Some(form) = state.form_mut() {
                        form.error = Some(StateError::ReconfirmationRequired);
                        form.router_error = Some(code);
                    }
                    refresh = true;
                } else {
                    if let Some(mutation) = state.mutation.as_mut() {
                        mutation.stage = MutationStage::Rejected;
                    }
                    if kind == Some(FormKind::TaskRequest) {
                        state.pending_requests.remove(request_id);
                    }
                    if let Some(form) = state.form_mut() {
                        form.router_error = Some(code);
                    }
                    state.mutation = None;
                }
                notice(
                    &mut state,
                    "Router rejected the operation; draft retained.",
                    Some(code),
                );
            }
            Err(error) if !uncertain(&error) => {
                if chat {
                    state.composer.reject(request_id);
                } else {
                    if kind == Some(FormKind::TaskRequest) {
                        state.pending_requests.remove(request_id);
                    }
                    state.mutation = None;
                    if let Some(form) = state.form_mut() {
                        form.router_error = client_code(&error);
                    }
                }
                notice(
                    &mut state,
                    "Operation rejected; draft retained.",
                    client_code(&error),
                );
            }
            Err(_) | Ok(_) => {
                if chat {
                    state.composer.mark_uncertain();
                } else if let Some(mutation) = state.mutation.as_mut() {
                    mutation.mark_uncertain();
                }
                if kind == Some(FormKind::WorkspaceCreate) {
                    // Workspace creation has no idempotency key. Keep its frozen form
                    // until the user dismisses it; never replay the create operation.
                    notice(
                        &mut state,
                        "Workspace creation is unconfirmed. Refresh the list; do not recreate automatically.",
                        None,
                    );
                } else if kind == Some(FormKind::TaskRequest) {
                    state.close_form();
                    state.mutation = None;
                    if let Some(pending) = state.pending_requests.get_mut(request_id) {
                        pending.stage = super::state::RequestStage::Uncertain;
                    }
                    notice(
                        &mut state,
                        "Request outcome uncertain; inspect task/history. This request cannot be retried.",
                        None,
                    );
                } else {
                    notice(
                        &mut state,
                        "Outcome uncertain; explicit retry uses the identical operation and payload.",
                        None,
                    );
                }
                refresh = true;
            }
        }
        state.mark_dirty();
        drop(state);
        if refresh {
            self.enqueue(ReadKind::Tasks);
            self.enqueue(ReadKind::Detail);
            self.enqueue(ReadKind::TaskHistory);
        }
        let pending: Vec<_> = lock(&self.state)
            .pending_requests
            .values()
            .filter(|request| request.stage == super::state::RequestStage::Uncertain)
            .map(|request| request.task_id)
            .collect();
        for task_id in pending {
            self.enqueue(ReadKind::Pending(task_id));
        }
        if let Some(workspace) = select {
            self.switch(workspace);
        }
        if kind == Some(FormKind::WorkspaceCreate) {
            self.enqueue(ReadKind::Workspaces);
        }
    }

    fn restore_previous(&mut self, old: Option<WorkspaceName>) {
        if !matches!(lock(&self.state).connection, Connection::Connected { .. }) {
            if old.is_some() {
                self.restore_pending = old;
            } else {
                lock(&self.state).switching = None;
            }
            return;
        }
        let fence = self.fence();
        let client = self.client.clone();
        let shared = self.state.clone();
        self.actions.spawn(async move {
            let result = match old {
                Some(workspace) => match attached(&shared, client.workspace_join(workspace))
                    .await
                    .unwrap_or(Err(ClientError::Closed))
                {
                    Ok(_) => SwitchResult::Restored(ClientError::Disconnected),
                    Err(error) => SwitchResult::RecoveryFailed(error),
                },
                None => SwitchResult::RecoveryFailed(ClientError::Disconnected),
            };
            ActionDone {
                fence,
                value: ActionValue::Switch(result),
            }
        });
    }

    fn replace_client(&mut self) {
        self.replacing = true;
        self.pending_ack = None;
        self.client_generation = self.client_generation.wrapping_add(1);
        self.cancel_reads();
        let mut state = lock(&self.state);
        let old = state.workspace.clone();
        if let Some(switch) = state.switching.as_mut() {
            switch.phase = SwitchPhase::RecoveringPreviousMembership;
        }
        state.header.is_admin = false;
        state.connection_changed(Connection::Connecting);
        drop(state);
        let fence = self.fence();
        let client = self.client.clone();
        let config = self.config.clone();
        let shared = self.state.clone();
        self.actions.spawn(async move {
            let _ = client.close().await;
            let result = async {
                let (client, events) = attached(&shared, RouterClient::connect(config))
                    .await
                    .unwrap_or(Err(ClientError::Closed))?;
                let lifetime = ClientLifetime(Some(client.clone()));
                if let Some(old) = old
                    && let Err(error) = attached(&shared, client.workspace_join(old))
                        .await
                        .unwrap_or(Err(ClientError::Closed))
                {
                    let _ = client.close().await;
                    return Err(error);
                }
                Ok((lifetime, events))
            }
            .await;
            ActionDone {
                fence,
                value: ActionValue::Replacement(result),
            }
        });
    }
}

fn display_history(kind: ReadKind) -> bool {
    matches!(kind, ReadKind::Recent | ReadKind::Previous | ReadKind::Next)
}

fn retry_ready(state: &UiState) -> bool {
    !state.detaching
        && state.terminal_large_enough()
        && matches!(state.connection, Connection::Connected { .. })
        && state.workspace.is_some()
        && state.chat.synchronized
        && state.switching.is_none()
}

async fn unsubscribe(client: &RouterClient) -> Result<(), ClientError> {
    match client
        .call(ClientMessage::WorkspaceUnsubscribe {
            request_id: request_id(),
        })
        .await?
    {
        ServerMessage::WorkspaceUnsubscribed { .. } => Ok(()),
        ServerMessage::Error { code, .. } => Err(ClientError::Router(code)),
        _ => Err(ClientError::Router(RouterErrorCode::InvalidMessage)),
    }
}

async fn switch_workspace(
    client: &RouterClient,
    state: &SharedState,
    old: Option<WorkspaceName>,
    target: WorkspaceName,
) -> SwitchResult {
    if old.is_some() {
        if let Err(error) = attached(state, unsubscribe(client))
            .await
            .unwrap_or(Err(ClientError::Closed))
        {
            return SwitchResult::Restored(error);
        }
        if let Some(switch) = lock(state).switching.as_mut() {
            switch.phase = SwitchPhase::Leaving;
        }
        match attached(state, client.workspace_leave())
            .await
            .unwrap_or(Err(ClientError::Closed))
        {
            Ok(_) => {}
            Err(ClientError::Router(RouterErrorCode::LeaveUnconfirmed)) => {
                return SwitchResult::LeaveUnconfirmed;
            }
            Err(error) => return SwitchResult::Restored(error),
        }
    }
    if let Some(switch) = lock(state).switching.as_mut() {
        switch.phase = SwitchPhase::Joining;
    }
    match attached(state, client.workspace_join(target))
        .await
        .unwrap_or(Err(ClientError::Closed))
    {
        Ok((workspace, _)) => SwitchResult::Joined(workspace),
        Err(error) => {
            if let Some(old) = old
                && let Err(recovery) = attached(state, client.workspace_join(old))
                    .await
                    .unwrap_or(Err(ClientError::Closed))
            {
                return SwitchResult::RecoveryFailed(recovery);
            }
            SwitchResult::Restored(error)
        }
    }
}

async fn verify_stop(
    config: &ClientConfig,
    options: &UiOptions,
    client: &RouterClient,
) -> Result<(), &'static str> {
    let expected = options
        .owned_runtime
        .as_ref()
        .ok_or("owned_runtime_required")?;
    let data_dir = options
        .owned_data_dir
        .as_ref()
        .ok_or("owned_runtime_required")?;
    if client.operator_is_admin() != Some(true) {
        return Err("permission_denied");
    }
    let store = RuntimeStore::new(data_dir.clone()).map_err(|_| "owned_runtime_invalid")?;
    let current = store
        .read()
        .map_err(|_| "owned_runtime_invalid")?
        .ok_or("router_not_running")?;
    if current.instance_id != expected.instance_id
        || current.control_url != expected.control_url
        || config.router_url.as_str() != current.control_url
    {
        return Err("runtime_instance_changed");
    }
    let marker = ReqwestHealthProbe::new(config.ca_file.clone())
        .probe(&current)
        .await
        .map_err(|_| "owned_router_unreachable")?;
    if marker.instance_id != expected.instance_id
        || marker.service != "agent-session-router"
        || marker.protocol_version != crate::protocol::PROTOCOL_VERSION
        || marker.status != "ok"
    {
        return Err("runtime_instance_changed");
    }
    match client
        .call(ClientMessage::Ping {
            request_id: request_id(),
        })
        .await
    {
        Ok(ServerMessage::Pong { .. }) => Ok(()),
        _ => Err("admin_connection_failed"),
    }
}
