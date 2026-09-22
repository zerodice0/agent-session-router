use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_session_router::{
    client::{ClientConfig, ClientEvent, ClientEvents, ClientRole, RouterClient},
    credentials::{CredentialFile, CredentialRole, read_credential},
    protocol::{
        AgentClient, AgentRegistration, AgentSide, ClientMessage, DeliveryMode, RouterErrorCode,
        ServerMessage, WorkspaceEventKind, WorkspaceName, parse_client_message,
        parse_server_message,
    },
    router::{RouterConfig, RouterExposure, RouterRuntime},
    store::RouterStore,
    tasks::{StopEvidence, TaskCheckpoint, TaskState},
    tui::{
        SharedState, UiCommand, UiNotice, UiOptions, controller,
        state::{
            FieldId, FormKind, FormState, Header, HistoryMode, Modal, MutationStage, Ownership,
            RequestStage, StateError, UiState,
        },
    },
};
use futures_util::{SinkExt, StreamExt};
use tempfile::{TempDir, tempdir};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc},
    task::JoinHandle,
};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use url::Url;
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(10);

fn shared_lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn config(data_dir: &std::path::Path) -> RouterConfig {
    RouterConfig {
        bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        data_dir: data_dir.to_owned(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    }
}

struct Fixture {
    _directory: TempDir,
    runtime: RouterRuntime,
    url: Url,
    workspace: WorkspaceName,
    admin: CredentialFile,
    workers: Vec<CredentialFile>,
}

impl Fixture {
    // Same private real RouterStore/bootstrap pattern as router_lifecycle.rs.
    async fn start() -> Self {
        Self::start_with_history(0).await
    }

    async fn start_with_history(history_events: usize) -> Self {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().canonicalize().unwrap().join("data");
        let bootstrap = RouterRuntime::start(config(&data_dir)).await.unwrap();
        let admin = read_credential(&data_dir.join("credentials/admin.json")).unwrap();
        bootstrap.shutdown().await.unwrap();
        bootstrap.wait().await.unwrap();
        let workspace = WorkspaceName::parse("console-room").unwrap();
        let mut store = RouterStore::open(&data_dir).unwrap();
        store.create_workspace(&workspace).unwrap();
        let actor_id = format!("operator:{}", admin.subject);
        for index in 0..history_events {
            store
                .append_chat_idempotent(
                    &workspace,
                    &actor_id,
                    &format!("history-seed:{index}"),
                    &format!("existing-history-{index:03}"),
                )
                .unwrap();
        }
        let mut workers = Vec::new();
        for id in ["worker-one", "worker-two"] {
            let credential = CredentialFile::generate(
                CredentialRole::Agent,
                id.into(),
                Some(AgentSide::Generic),
                Some(AgentClient::Omp),
                vec![workspace.clone()],
            )
            .unwrap();
            store.insert_credential(&credential).unwrap();
            workers.push(credential);
        }
        store.close().unwrap();
        let runtime = RouterRuntime::start(config(&data_dir)).await.unwrap();
        let url = Url::parse(&format!("ws://{}/ws", runtime.address)).unwrap();
        Self {
            _directory: directory,
            runtime,
            url,
            workspace,
            admin,
            workers,
        }
    }

    fn operator_config(&self, url: &Url) -> ClientConfig {
        ClientConfig {
            router_url: url.clone(),
            role: ClientRole::Operator {
                credential: self.admin.clone(),
            },
            ca_file: None,
        }
    }

    async fn operator(&self) -> (RouterClient, ClientEvents) {
        let (client, events) = RouterClient::connect(self.operator_config(&self.url))
            .await
            .unwrap();
        client.workspace_join(self.workspace.clone()).await.unwrap();
        (client, events)
    }

    async fn worker(&self, index: usize) -> (RouterClient, ClientEvents) {
        let credential = self.workers[index].clone();
        let (client, events) = RouterClient::connect(ClientConfig {
            router_url: self.url.clone(),
            role: ClientRole::Primary {
                agent: AgentRegistration {
                    agent_id: credential.subject.clone(),
                    side: AgentSide::Generic,
                    client: AgentClient::Omp,
                    activity: None,
                    delivery_mode: DeliveryMode::Push,
                },
                credential,
                delegation_token: None,
            },
            ca_file: None,
        })
        .await
        .unwrap();
        client.workspace_join(self.workspace.clone()).await.unwrap();
        // Push readiness is established by join; Readiness is a Pull-only frame.
        // Verify this exact live session is ready before exercising UI dispatch.
        let barrier = client
            .execution_barrier()
            .await
            .expect("push worker registration barrier");
        match client
            .call(ClientMessage::WorkspaceMembers {
                request_id: Uuid::new_v4().to_string(),
            })
            .await
            .expect("push worker membership query")
        {
            ServerMessage::Agents {
                workspace, agents, ..
            } => {
                assert_eq!(workspace, self.workspace);
                let member = agents
                    .iter()
                    .find(|member| member.agent_id == self.workers[index].subject)
                    .expect("joined push worker must remain present");
                assert_eq!(member.session_id, barrier.session_id);
                assert_eq!(member.delivery_mode, DeliveryMode::Push);
                assert_eq!(
                    member.status,
                    agent_session_router::protocol::AgentStatus::Idle
                );
                assert!(
                    member.ready,
                    "joined push worker must be ready for an explicit request"
                );
            }
            ServerMessage::Error { code, .. } => {
                panic!("worker membership rejected: {}", code.as_str())
            }
            _ => panic!("unexpected worker membership response"),
        }
        (client, events)
    }

    async fn ui(&self, url: &Url) -> Console {
        self.ui_with_credential(url, self.admin.clone()).await
    }

    async fn ui_with_credential(&self, url: &Url, credential: CredentialFile) -> Console {
        let state = Arc::new(Mutex::new(UiState::new(
            Header {
                profile: None,
                endpoint: url.to_string(),
                transport: "loopback".into(),
                ownership: Ownership::Remote,
                instance_id: None,
                is_admin: false,
            },
            true,
        )));
        state.lock().unwrap().resize(120, 40);
        let (commands, receiver) = mpsc::channel(32);
        let (notices, notice_rx) = mpsc::channel(8);
        let options = UiOptions {
            profile_name: None,
            workspace: Some(self.workspace.clone()),
            owned_runtime: None,
            ownership: Ownership::Remote,
            owned_data_dir: None,
        };
        let job = tokio::spawn(controller::run(
            ClientConfig {
                router_url: url.clone(),
                role: ClientRole::Operator { credential },
                ca_file: None,
            },
            options,
            state.clone(),
            receiver,
            notices,
        ));
        let console = Console {
            state,
            commands,
            notices: notice_rx,
            job,
        };
        console
            .wait(|state| {
                state.chat.synchronized && state.workspace.as_ref() == Some(&self.workspace)
            })
            .await;
        console
    }

    async fn finish(self) {
        self.runtime.shutdown().await.unwrap();
        self.runtime.wait().await.unwrap();
    }
}

struct Console {
    state: SharedState,
    commands: mpsc::Sender<UiCommand>,
    notices: mpsc::Receiver<UiNotice>,
    job: JoinHandle<Result<(), agent_session_router::tui::UiError>>,
}

impl Console {
    async fn wait(&self, predicate: impl Fn(&UiState) -> bool) {
        tokio::time::timeout(DEADLINE, async {
            loop {
                if predicate(&self.state.lock().unwrap()) {
                    break;
                }
                assert!(
                    !self.job.is_finished(),
                    "controller exited before observable state"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("controller state deadline");
    }

    async fn submit(&self, kind: FormKind, fields: &[(FieldId, &str)]) {
        {
            let mut state = self.state.lock().unwrap();
            assert!(!state.form_busy);
            let mut form = FormState::new(kind, state.tasks.detail.as_ref()).unwrap();
            for (id, value) in fields {
                let field = form
                    .fields
                    .iter_mut()
                    .find(|field| field.id == *id)
                    .unwrap();
                field.input.clear();
                field.input.insert(value).unwrap();
            }
            state.modal = Some(Modal::Form(form));
            state.form_busy = true;
        }
        self.commands.send(UiCommand::SubmitForm).await.unwrap();
    }

    async fn mutation_finished(&self) {
        self.wait(|state| !state.form_busy && state.mutation.is_none() && state.form().is_none())
            .await;
    }

    async fn detail(&self, id: i64, version: i64) {
        {
            self.state.lock().unwrap().tasks.select(Some(id));
        }
        self.commands.send(UiCommand::LoadTaskDetail).await.unwrap();
        self.wait(|state| {
            state
                .tasks
                .detail
                .as_ref()
                .is_some_and(|task| task.summary.id == id && task.summary.version >= version)
        })
        .await;
    }

    async fn show_previous_history(&self, oldest: i64, live_cursor: i64) {
        self.commands
            .send(UiCommand::PreviousHistory)
            .await
            .unwrap();
        self.wait(|state| {
            state.chat.mode == HistoryMode::Past
                && state
                    .chat
                    .buffer
                    .events()
                    .back()
                    .is_some_and(|event| event.seq == oldest - 1)
        })
        .await;
        let state = shared_lock(&self.state);
        assert_eq!(state.chat.buffer.events().front().unwrap().seq, 1);
        assert_eq!(state.chat.applied_cursor, Some(live_cursor));
        assert!(state.chat.synchronized);
    }

    async fn close(self) {
        drop(self.commands);
        drop(self.notices);
        tokio::time::timeout(DEADLINE, self.job)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

async fn next_worker_event(events: &mut ClientEvents) -> ClientEvent {
    let item = events
        .recv()
        .await
        .expect("worker event stream closed before scenario completed");
    match item.event {
        ClientEvent::Closed(code) => panic!("worker connection closed: {}", code.as_str()),
        event => event,
    }
}

async fn next_delivery(events: &mut ClientEvents) -> (String, i64, i64) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            if let ClientEvent::Delivery {
                request_id,
                task: Some(task),
                ..
            } = next_worker_event(events).await
            {
                break (request_id, task.id, task.expected_version);
            }
        }
    })
    .await
    .unwrap()
}

async fn no_delivery(events: &mut ClientEvents) {
    assert!(
        tokio::time::timeout(Duration::from_millis(150), async {
            loop {
                if matches!(
                    next_worker_event(events).await,
                    ClientEvent::Delivery { .. }
                ) {
                    break;
                }
            }
        })
        .await
        .is_err(),
        "create/assign must not execute work"
    );
}

async fn create(client: &RouterClient, workspace: &WorkspaceName, title: &str) -> i64 {
    client
        .task_mutation(ClientMessage::TaskCreate {
            request_id: Uuid::new_v4().to_string(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            title: title.into(),
            description: String::new(),
        })
        .await
        .unwrap()
        .task
        .summary
        .id
}

async fn post(client: &RouterClient, content: &str) -> i64 {
    match client
        .call(ClientMessage::WorkspacePost {
            request_id: Uuid::new_v4().to_string(),
            content: content.into(),
        })
        .await
        .unwrap()
    {
        ServerMessage::WorkspacePosted { seq, .. } => seq,
        ServerMessage::Error { code, .. } => panic!("post rejected: {}", code.as_str()),
        _ => panic!("unexpected post response"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn console_create_assign_request_begin_checkpoint_interrupt_and_confirm_stopped() {
    let fixture = Fixture::start().await;
    let (inspector, _events) = fixture.operator().await;
    let (worker, mut first_events) = fixture.worker(0).await;
    let (second, mut second_events) = fixture.worker(1).await;
    let console = fixture.ui(&fixture.url).await;
    console
        .submit(
            FormKind::TaskCreate,
            &[
                (FieldId::Title, "Inspect the shared task"),
                (FieldId::Description, "No implicit execution"),
            ],
        )
        .await;
    console.mutation_finished().await;
    let id = console.state.lock().unwrap().tasks.selected_id.unwrap();
    console.detail(id, 1).await;
    no_delivery(&mut first_events).await;
    no_delivery(&mut second_events).await;
    console
        .submit(FormKind::TaskAssign, &[(FieldId::Agent, "worker-one")])
        .await;
    console.mutation_finished().await;
    no_delivery(&mut first_events).await;
    no_delivery(&mut second_events).await;
    let assigned = inspector
        .task_get(fixture.workspace.clone(), id)
        .await
        .unwrap();
    assert_eq!(assigned.summary.state, TaskState::Todo);
    assert_eq!(
        assigned.summary.assigned_agent_id.as_deref(),
        Some("worker-one")
    );
    console.detail(id, assigned.summary.version).await;
    request_begin_and_checkpoint(
        &console,
        &inspector,
        &worker,
        &mut first_events,
        &fixture.workspace,
        id,
    )
    .await;
    console
        .submit(
            FormKind::TaskInterrupt,
            &[(FieldId::Note, "Operator asks execution to stop")],
        )
        .await;
    console.mutation_finished().await;
    let interrupted = inspector
        .task_get(fixture.workspace.clone(), id)
        .await
        .unwrap();
    assert_eq!(
        interrupted.summary.stop_evidence,
        Some(StopEvidence::Unknown)
    );
    assert_ne!(interrupted.summary.state, TaskState::Done);
    console.detail(id, interrupted.summary.version).await;
    assert!(!console.state.lock().unwrap().tasks.can_request());
    confirm_stop_requires_observation(&console, &inspector, &fixture.workspace, id).await;
    console.close().await;
    worker.close().await.unwrap();
    second.close().await.unwrap();
    inspector.close().await.unwrap();
    fixture.finish().await;
}

async fn request_begin_and_checkpoint(
    console: &Console,
    inspector: &RouterClient,
    worker: &RouterClient,
    events: &mut ClientEvents,
    workspace: &WorkspaceName,
    id: i64,
) {
    console
        .submit(
            FormKind::TaskRequest,
            &[(FieldId::Message, "Begin explicitly")],
        )
        .await;
    console.mutation_finished().await;
    let (request, delivered, version) = next_delivery(events).await;
    assert_eq!(delivered, id);
    assert_eq!(
        inspector
            .task_get(workspace.clone(), id)
            .await
            .unwrap()
            .summary
            .state,
        TaskState::Todo
    );
    assert_eq!(
        console
            .state
            .lock()
            .unwrap()
            .pending_requests
            .get(&request)
            .unwrap()
            .stage,
        RequestStage::Accepted
    );
    let begun = worker
        .task_mutation(ClientMessage::TaskBegin {
            request_id: "begin-console".into(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            task_id: id,
            work_request_id: request.clone(),
            expected_version: version,
            last_checkpoint_id: None,
            resume_note: "Observed explicit request".into(),
        })
        .await
        .unwrap();
    let attempt = begun.task.current_attempt.as_ref().unwrap().id;
    let checkpoint = worker
        .task_mutation(ClientMessage::TaskCheckpoint {
            request_id: "checkpoint-console".into(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            task_id: id,
            attempt_id: attempt,
            expected_version: begun.applied_version,
            checkpoint: TaskCheckpoint {
                summary: "Reached a safe boundary".into(),
                next_steps: "Wait for operator".into(),
                artifacts: vec![],
                risks: String::new(),
            },
        })
        .await
        .unwrap();
    console.detail(id, checkpoint.applied_version).await;
    console
        .wait(|state| {
            state
                .pending_requests
                .get(&request)
                .is_some_and(|pending| pending.stage == RequestStage::ExecutionObserved)
        })
        .await;
}

async fn confirm_stop_requires_observation(
    console: &Console,
    inspector: &RouterClient,
    workspace: &WorkspaceName,
    id: i64,
) {
    console
        .submit(
            FormKind::TaskConfirmStopped,
            &[(FieldId::Note, "Observed the worker process stopped")],
        )
        .await;
    console
        .wait(|state| {
            !state.form_busy
                && state
                    .form()
                    .is_some_and(|form| form.error == Some(StateError::MustObserveStopped))
        })
        .await;
    assert_eq!(
        inspector
            .task_get(workspace.clone(), id)
            .await
            .unwrap()
            .summary
            .stop_evidence,
        Some(StopEvidence::Unknown)
    );
    {
        let mut state = console.state.lock().unwrap();
        state.form_mut().unwrap().stopped_observed = true;
        state.form_busy = true;
    }
    console.commands.send(UiCommand::SubmitForm).await.unwrap();
    console.mutation_finished().await;
    let stopped = inspector.task_get(workspace.clone(), id).await.unwrap();
    assert_eq!(stopped.summary.stop_evidence, Some(StopEvidence::Confirmed));
    assert_ne!(stopped.summary.state, TaskState::Done);
}

#[tokio::test(flavor = "multi_thread")]
async fn console_stale_form_preserves_draft_and_requires_explicit_reconfirmation() {
    let fixture = Fixture::start().await;
    let (inspector, _events) = fixture.operator().await;
    let id = create(&inspector, &fixture.workspace, "Original title").await;
    let console = fixture.ui(&fixture.url).await;
    console.detail(id, 1).await;
    {
        let mut state = console.state.lock().unwrap();
        let mut form = FormState::new(FormKind::TaskEdit, state.tasks.detail.as_ref()).unwrap();
        let title = form
            .fields
            .iter_mut()
            .find(|field| field.id == FieldId::Title)
            .unwrap();
        title.input.clear();
        title.input.insert("Retained human draft").unwrap();
        state.modal = Some(Modal::Form(form));
    }
    let latest = inspector
        .task_get(fixture.workspace.clone(), id)
        .await
        .unwrap();
    let other = inspector
        .task_mutation(ClientMessage::TaskEdit {
            request_id: "concurrent-edit".into(),
            workspace: fixture.workspace.clone(),
            operation_id: Uuid::new_v4(),
            task_id: id,
            expected_version: latest.summary.version,
            title: Some("Concurrent authoritative title".into()),
            description: None,
        })
        .await
        .unwrap();
    console.commands.send(UiCommand::SubmitForm).await.unwrap();
    console
        .wait(|state| {
            !state.form_busy
                && state
                    .form()
                    .is_some_and(|form| form.error == Some(StateError::ReconfirmationRequired))
        })
        .await;
    assert_eq!(
        inspector
            .task_get(fixture.workspace.clone(), id)
            .await
            .unwrap()
            .summary
            .version,
        other.applied_version
    );
    {
        let mut state = console.state.lock().unwrap();
        let form = state.form_mut().unwrap();
        assert_eq!(form.value(FieldId::Title), "Retained human draft");
        form.reconfirm_latest().unwrap();
        state.mutation = None;
        state.form_busy = true;
    }
    console.commands.send(UiCommand::SubmitForm).await.unwrap();
    console.mutation_finished().await;
    let applied = inspector
        .task_get(fixture.workspace.clone(), id)
        .await
        .unwrap();
    assert_eq!(applied.summary.title, "Retained human draft");
    assert_eq!(applied.summary.version, other.applied_version + 1);
    console.close().await;
    inspector.close().await.unwrap();
    fixture.finish().await;
}

#[derive(Default)]
struct Faults {
    tasks: TaskFaults,
    workspace: WorkspaceFaults,
    requests: RequestFaults,
}

#[derive(Default)]
struct TaskFaults {
    lose_mutation: bool,
    mutation_ids: Vec<Uuid>,
    hold_detail: bool,
    held_detail: Option<Message>,
    hold_task_gets: bool,
    held_details: Vec<Message>,
}

#[derive(Default)]
struct WorkspaceFaults {
    drop_chat: bool,
    subscriptions: Vec<i64>,
    hold_members: bool,
    held_members: Option<Message>,
    member_reads: usize,
    reject_forward_history: bool,
}

#[derive(Default)]
struct RequestFaults {
    hold_request: bool,
    held_request: Option<Message>,
    request_ids: Vec<String>,
}
struct Proxy {
    url: Url,
    faults: Arc<Mutex<Faults>>,
    release: Arc<Notify>,
    job: JoinHandle<()>,
}
impl Proxy {
    async fn start(upstream: Url) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("ws://{}/ws", listener.local_addr().unwrap())).unwrap();
        let faults = Arc::new(Mutex::new(Faults::default()));
        let release = Arc::new(Notify::new());
        let shared = faults.clone();
        let released = release.clone();
        let job = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut downstream = accept_async(socket).await.unwrap();
                let (mut upstream, _) = connect_async(upstream.as_str()).await.unwrap();
                loop {
                    tokio::select! {
                        () = released.notified() => {
                            let (held, members, request, details) = {
                                let mut faults = shared_lock(&shared);
                                (faults.tasks.held_detail.take(), faults.workspace.held_members.take(), faults.requests.held_request.take(), std::mem::take(&mut faults.tasks.held_details))
                            };
                            if let Some(message) = held && downstream.send(message).await.is_err() { break; }
                            if let Some(message) = members && downstream.send(message).await.is_err() { break; }
                            for message in details { if downstream.send(message).await.is_err() { break; } }
                            if let Some(message) = request && upstream.send(message).await.is_err() { break; }
                        },
                        message = downstream.next() => {
                            let Some(Ok(message)) = message else { break; };
                            let mut suppress = false;
                            let mut replacement = None;
                            if let Message::Text(text) = &message && let Ok(parsed) = parse_client_message(text.as_str()) {
                                let mut faults = shared.lock().unwrap();
                                match parsed {
                                    ClientMessage::TaskCreate { operation_id, .. } => faults.tasks.mutation_ids.push(operation_id),
                                    ClientMessage::WorkspaceSubscribe { after, .. } => faults.workspace.subscriptions.push(after),
                                    ClientMessage::WorkspaceMembers { .. } => faults.workspace.member_reads += 1,
                                    ClientMessage::WorkspaceHistory { request_id, after: Some(_), limit } if faults.workspace.reject_forward_history => {
                                        faults.workspace.reject_forward_history = false;
                                        // Ask the real router for a protocol-valid but nonexistent
                                        // future cursor. Its rejection must not affect subscription.
                                        replacement = Some(Message::Text(serde_json::to_string(&ClientMessage::WorkspaceHistory {
                                            request_id, after: Some(9_007_199_254_740_991), limit,
                                        }).unwrap().into()));
                                    },
                                    ClientMessage::TaskRequest { request_id, .. } => {
                                        faults.requests.request_ids.push(request_id);
                                        if faults.requests.hold_request {
                                            faults.requests.hold_request = false;
                                            faults.requests.held_request = Some(message.clone());
                                            suppress = true;
                                        }
                                    },
                                    _ => {},
                                }
                            }
                            let message = replacement.unwrap_or(message);
                            if !suppress && upstream.send(message).await.is_err() { break; }
                        },
                        message = upstream.next() => {
                            let Some(Ok(message)) = message else { break; };
                            let mut lose = false; let mut suppress = false;
                            if let Message::Text(text) = &message && let Ok(parsed) = parse_server_message(text.as_str()) {
                                let mut faults = shared.lock().unwrap();
                                match parsed {
                                    ServerMessage::TaskMutated { .. } if faults.tasks.lose_mutation => { faults.tasks.lose_mutation = false; lose = true; },
                                    ServerMessage::WorkspaceEvent { event } if faults.workspace.drop_chat && event.kind == WorkspaceEventKind::Chat => { faults.workspace.drop_chat = false; suppress = true; },
                                    ServerMessage::Agents { .. } if faults.workspace.hold_members => { faults.workspace.hold_members = false; faults.workspace.held_members = Some(message.clone()); suppress = true; },
                                    ServerMessage::Task { .. } if faults.tasks.hold_task_gets => { faults.tasks.held_details.push(message.clone()); suppress = true; },
                                    ServerMessage::Task { .. } if faults.tasks.hold_detail => { faults.tasks.hold_detail = false; faults.tasks.held_detail = Some(message.clone()); suppress = true; },
                                    _ => {},
                                }
                            }
                            if lose { let _ = downstream.close(None).await; let _ = upstream.close(None).await; break; }
                            if !suppress && downstream.send(message).await.is_err() { break; }
                        },
                    }
                }
            }
        });
        Self {
            url,
            faults,
            release,
            job,
        }
    }

    async fn wait(&self, predicate: impl Fn(&Faults) -> bool) {
        tokio::time::timeout(DEADLINE, async {
            while !predicate(&self.faults.lock().unwrap()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.job.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn console_uncertain_retry_keeps_operation_id_and_applies_once_on_real_router() {
    let fixture = Fixture::start().await;
    let proxy = Proxy::start(fixture.url.clone()).await;
    let (inspector, _events) = fixture.operator().await;
    let console = fixture.ui(&proxy.url).await;
    proxy.faults.lock().unwrap().tasks.lose_mutation = true;
    console
        .submit(FormKind::TaskCreate, &[(FieldId::Title, "Exactly once")])
        .await;
    console
        .wait(|state| {
            state.chat.synchronized
                && state
                    .mutation
                    .as_ref()
                    .is_some_and(|mutation| mutation.stage == MutationStage::Uncertain)
        })
        .await;
    assert_eq!(proxy.faults.lock().unwrap().tasks.mutation_ids.len(), 1);
    let (before, _, _) = inspector
        .task_list(fixture.workspace.clone(), None, None, None, Some(100))
        .await
        .unwrap();
    assert_eq!(
        before
            .iter()
            .filter(|task| task.title == "Exactly once")
            .count(),
        1
    );
    console
        .commands
        .send(UiCommand::RetryMutation)
        .await
        .unwrap();
    console.mutation_finished().await;
    let ids = proxy.faults.lock().unwrap().tasks.mutation_ids.clone();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1]);
    let (after, _, _) = inspector
        .task_list(fixture.workspace.clone(), None, None, None, Some(100))
        .await
        .unwrap();
    assert_eq!(
        after
            .iter()
            .filter(|task| task.title == "Exactly once")
            .count(),
        1
    );
    assert_eq!(before[0].version, after[0].version);
    console.close().await;
    inspector.close().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn console_gap_recovers_from_applied_cursor_and_stale_detail_cannot_change_selection() {
    let fixture = Fixture::start().await;
    let proxy = Proxy::start(fixture.url.clone()).await;
    let (inspector, _events) = fixture.operator().await;
    let first = create(&inspector, &fixture.workspace, "First").await;
    let second = create(&inspector, &fixture.workspace, "Second").await;
    let console = fixture.ui(&proxy.url).await;
    console.detail(first, 1).await;
    proxy.faults.lock().unwrap().tasks.hold_detail = true;
    console
        .commands
        .send(UiCommand::LoadTaskDetail)
        .await
        .unwrap();
    proxy
        .wait(|faults| faults.tasks.held_detail.is_some())
        .await;
    console.state.lock().unwrap().tasks.select(Some(second));
    console
        .commands
        .send(UiCommand::LoadTaskDetail)
        .await
        .unwrap();
    proxy.release.notify_one();
    console
        .wait(|state| {
            state
                .tasks
                .detail
                .as_ref()
                .is_some_and(|task| task.summary.id == second)
        })
        .await;
    assert_eq!(
        console.state.lock().unwrap().tasks.selected_id,
        Some(second)
    );
    let cursor = console.state.lock().unwrap().chat.applied_cursor.unwrap();
    proxy.faults.lock().unwrap().workspace.drop_chat = true;
    let lost = post(&inspector, "durable but not forwarded").await;
    proxy.wait(|faults| !faults.workspace.drop_chat).await;
    let next = post(&inspector, "exposes sequence gap").await;
    console
        .wait(|state| state.chat.synchronized && state.chat.applied_cursor == Some(next))
        .await;
    {
        let state = console.state.lock().unwrap();
        assert!(
            state
                .chat
                .buffer
                .events()
                .iter()
                .any(|event| event.seq == lost)
        );
        assert!(
            state
                .chat
                .buffer
                .events()
                .iter()
                .any(|event| event.seq == next)
        );
    }
    assert!(
        proxy
            .faults
            .lock()
            .unwrap()
            .workspace
            .subscriptions
            .contains(&cursor)
    );
    console.close().await;
    inspector.close().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn console_pending_request_blocks_workspace_switch_and_close_cancels_requester() {
    let fixture = Fixture::start().await;
    let (inspector, _events) = fixture.operator().await;
    let (worker, mut events) = fixture.worker(0).await;
    let console = fixture.ui(&fixture.url).await;
    let target = WorkspaceName::parse("other-room").unwrap();
    assert!(matches!(
        inspector
            .call(ClientMessage::WorkspaceCreate {
                request_id: "other-room".into(),
                name: target.clone()
            })
            .await
            .unwrap(),
        ServerMessage::WorkspaceCreated { .. }
    ));
    console
        .submit(
            FormKind::TaskCreate,
            &[(FieldId::Title, "Pending lifetime")],
        )
        .await;
    console.mutation_finished().await;
    let id = console.state.lock().unwrap().tasks.selected_id.unwrap();
    console.detail(id, 1).await;
    console
        .submit(FormKind::TaskAssign, &[(FieldId::Agent, "worker-one")])
        .await;
    console.mutation_finished().await;
    console.submit(FormKind::TaskRequest, &[]).await;
    console.mutation_finished().await;
    let (request, _, _) = next_delivery(&mut events).await;
    console
        .commands
        .send(UiCommand::SelectWorkspace(target))
        .await
        .unwrap();
    console
        .wait(|state| {
            state.notice.as_ref().is_some_and(|notice| {
                notice.message == agent_session_router::tui::view::state_error(StateError::Busy)
            })
        })
        .await;
    assert_eq!(
        console.state.lock().unwrap().workspace.as_ref(),
        Some(&fixture.workspace)
    );
    console.close().await;
    let reason = tokio::time::timeout(DEADLINE, async {
        loop {
            if let ClientEvent::WorkCancelled {
                request_id, reason, ..
            } = next_worker_event(&mut events).await
                && request_id == request
            {
                break reason;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(reason, RouterErrorCode::RequesterDisconnected);
    assert_eq!(
        inspector
            .task_get(fixture.workspace.clone(), id)
            .await
            .unwrap()
            .summary
            .state,
        TaskState::Todo
    );
    worker.close().await.unwrap();
    inspector.close().await.unwrap();
    fixture.finish().await;
}

async fn assign(client: &RouterClient, workspace: &WorkspaceName, task_id: i64) -> i64 {
    let version = client
        .task_get(workspace.clone(), task_id)
        .await
        .unwrap()
        .summary
        .version;
    client
        .task_mutation(ClientMessage::TaskAssign {
            request_id: Uuid::new_v4().to_string(),
            workspace: workspace.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            expected_version: version,
            agent_id: Some("worker-one".into()),
        })
        .await
        .unwrap()
        .applied_version
}

#[tokio::test(flavor = "multi_thread")]
async fn console_definite_request_conflict_releases_pending_and_preserves_reconfirmation() {
    let fixture = Fixture::start().await;
    let proxy = Proxy::start(fixture.url.clone()).await;
    let (inspector, _events) = fixture.operator().await;
    let (worker, mut events) = fixture.worker(0).await;
    let id = create(&inspector, &fixture.workspace, "Competing request").await;
    let assigned = assign(&inspector, &fixture.workspace, id).await;
    let console = fixture.ui(&proxy.url).await;
    console.detail(id, assigned).await;
    shared_lock(&proxy.faults).requests.hold_request = true;
    console
        .submit(
            FormKind::TaskRequest,
            &[(FieldId::Message, "Retain this exact request draft")],
        )
        .await;
    proxy
        .wait(|faults| faults.requests.held_request.is_some())
        .await;
    let rejected_id = shared_lock(&proxy.faults).requests.request_ids[0].clone();
    assert!(
        shared_lock(&console.state)
            .pending_requests
            .contains_key(&rejected_id)
    );
    let changed = inspector
        .task_mutation(ClientMessage::TaskEdit {
            request_id: "win-request-race".into(),
            workspace: fixture.workspace.clone(),
            operation_id: Uuid::new_v4(),
            task_id: id,
            expected_version: assigned,
            title: Some("Updated before request dispatch".into()),
            description: None,
        })
        .await
        .unwrap();
    proxy.release.notify_one();
    console
        .wait(|state| {
            state.mutation.as_ref().is_some_and(|mutation| {
                mutation.stage == MutationStage::Conflict
                    && mutation.current_version == Some(changed.applied_version)
            }) && state.form().is_some_and(|form| form.changed.is_some())
        })
        .await;
    {
        let mut state = shared_lock(&console.state);
        assert!(
            state.pending_requests.is_empty(),
            "definitively rejected request must not retain requester lifetime"
        );
        assert_eq!(
            state.form().unwrap().value(FieldId::Message),
            "Retain this exact request draft"
        );
        state.form_mut().unwrap().reconfirm_latest().unwrap();
        state.mutation = None;
    }
    no_delivery(&mut events).await;
    console.commands.send(UiCommand::SubmitForm).await.unwrap();
    console.mutation_finished().await;
    let (accepted_id, delivered_task, _) = next_delivery(&mut events).await;
    assert_eq!(delivered_task, id);
    assert_ne!(accepted_id, rejected_id);
    {
        let state = shared_lock(&console.state);
        assert_eq!(state.pending_requests.len(), 1);
        assert!(state.pending_requests.contains_key(&accepted_id));
    }
    console.close().await;
    worker.close().await.unwrap();
    inspector.close().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn console_denied_target_join_cancels_held_members_and_refreshes_restored_room() {
    let fixture = Fixture::start().await;
    let proxy = Proxy::start(fixture.url.clone()).await;
    let (inspector, _events) = fixture.operator().await;
    let target = WorkspaceName::parse("unauthorized-target").unwrap();
    assert!(matches!(
        inspector
            .call(ClientMessage::WorkspaceCreate {
                request_id: "create-denied-target".into(),
                name: target.clone(),
            })
            .await
            .unwrap(),
        ServerMessage::WorkspaceCreated { .. }
    ));
    let scoped = match inspector
        .call(ClientMessage::CredentialIssue {
            request_id: "issue-scoped-console".into(),
            role: CredentialRole::Operator,
            subject: "scoped-console".into(),
            agent_side: None,
            agent_client: None,
            workspaces: vec![fixture.workspace.clone()],
        })
        .await
        .unwrap()
    {
        ServerMessage::CredentialIssued { credential, .. } => credential,
        ServerMessage::Error { code, .. } => {
            panic!("scoped credential rejected: {}", code.as_str())
        }
        _ => panic!("unexpected credential response"),
    };
    let (first, _first_events) = fixture.worker(0).await;
    let console = fixture.ui_with_credential(&proxy.url, scoped).await;
    console
        .wait(|state| {
            !state.members_loading
                && state
                    .members
                    .iter()
                    .any(|member| member.agent_id == "worker-one")
        })
        .await;
    shared_lock(&proxy.faults).workspace.hold_members = true;
    console.commands.send(UiCommand::LoadMembers).await.unwrap();
    proxy
        .wait(|faults| faults.workspace.held_members.is_some())
        .await;
    let before = shared_lock(&proxy.faults).workspace.member_reads;
    let (second, _second_events) = fixture.worker(1).await;
    console
        .commands
        .send(UiCommand::SelectWorkspace(target))
        .await
        .unwrap();
    console
        .wait(|state| {
            state.switching.is_none()
                && state.chat.synchronized
                && state.workspace.as_ref() == Some(&fixture.workspace)
                && !state.members_loading
                && state
                    .members
                    .iter()
                    .any(|member| member.agent_id == "worker-two")
        })
        .await;
    assert!(shared_lock(&proxy.faults).workspace.member_reads > before);
    assert_eq!(
        shared_lock(&console.state)
            .notice
            .as_ref()
            .and_then(|notice| notice.error),
        Some(RouterErrorCode::WorkspaceNotFound)
    );
    proxy.release.notify_one();
    console.close().await;
    first.close().await.unwrap();
    second.close().await.unwrap();
    inspector.close().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn console_ctrl_c_during_held_request_preflight_never_dispatches_unseen_work() {
    let fixture = Fixture::start().await;
    let proxy = Proxy::start(fixture.url.clone()).await;
    let (inspector, _events) = fixture.operator().await;
    let (worker, mut events) = fixture.worker(0).await;
    let id = create(&inspector, &fixture.workspace, "Detach before request").await;
    let version = assign(&inspector, &fixture.workspace, id).await;
    let console = fixture.ui(&proxy.url).await;
    console.detail(id, version).await;
    shared_lock(&proxy.faults).tasks.hold_task_gets = true;
    console
        .submit(
            FormKind::TaskRequest,
            &[(FieldId::Message, "Never dispatch after detach")],
        )
        .await;
    proxy
        .wait(|faults| !faults.tasks.held_details.is_empty())
        .await;
    {
        let mut state = shared_lock(&console.state);
        assert!(state.form_busy);
        assert!(state.pending_requests.is_empty());
        assert!(matches!(
            agent_session_router::tui::input::handle(
                &mut state,
                crossterm::event::Event::Key(crossterm::event::KeyEvent::new(
                    crossterm::event::KeyCode::Char('c'),
                    crossterm::event::KeyModifiers::CONTROL
                ))
            ),
            agent_session_router::tui::UiInput::Detach
        ));
        assert!(state.detaching);
    }
    // Make a completed latest-get compete with lost command ownership. The
    // shared detach claim already won, so neither scheduling order may send.
    drop(console.commands);
    proxy.release.notify_one();
    tokio::time::timeout(DEADLINE, console.job)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(shared_lock(&proxy.faults).requests.request_ids.is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(150), async {
            loop {
                if matches!(
                    next_worker_event(&mut events).await,
                    ClientEvent::Delivery { .. } | ClientEvent::WorkCancelled { .. }
                ) {
                    break;
                }
            }
        })
        .await
        .is_err(),
        "detach before tracking must neither deliver nor cancel unseen work"
    );
    assert_eq!(
        inspector
            .task_get(fixture.workspace.clone(), id)
            .await
            .unwrap()
            .summary
            .version,
        version
    );
    worker.close().await.unwrap();
    inspector.close().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn console_recent_hundred_then_previous_history_keeps_live_cursor_and_writes() {
    let fixture = Fixture::start_with_history(153).await;
    let (inspector, _events) = fixture.operator().await;
    let last = inspector
        .workspace_history(None, Some(1))
        .await
        .unwrap()
        .next_cursor;
    let console = fixture.ui(&fixture.url).await;
    let oldest = {
        let state = shared_lock(&console.state);
        assert_eq!(state.chat.applied_cursor, Some(last));
        assert_eq!(state.chat.buffer.events().len(), 100);
        let oldest = state.chat.buffer.events().front().unwrap().seq;
        assert_eq!(oldest, last - 99);
        oldest
    };
    console.show_previous_history(oldest, last).await;
    let new_seq = post(&inspector, "Live continues while old bodies are displayed").await;
    console
        .wait(|state| state.chat.applied_cursor == Some(new_seq))
        .await;
    {
        let mut state = shared_lock(&console.state);
        assert_eq!(state.chat.buffer.events().back().unwrap().seq, oldest - 1);
        assert!(state.chat.synchronized);
        assert!(state.writable().is_ok());
        state
            .composer
            .input
            .insert("Posting from historical view")
            .unwrap();
    }
    console.commands.send(UiCommand::SubmitChat).await.unwrap();
    console
        .wait(|state| {
            state.composer.input.text().is_empty() && state.chat.applied_cursor == Some(new_seq + 1)
        })
        .await;
    assert_eq!(
        shared_lock(&console.state)
            .chat
            .buffer
            .events()
            .back()
            .unwrap()
            .seq,
        oldest - 1
    );
    console.commands.send(UiCommand::NextHistory).await.unwrap();
    console
        .wait(|state| {
            state
                .chat
                .buffer
                .events()
                .front()
                .is_some_and(|event| event.seq == oldest)
        })
        .await;
    {
        let mut state = shared_lock(&console.state);
        assert_eq!(state.chat.applied_cursor, Some(new_seq + 1));
        assert!(state.chat.synchronized);
        assert!(state.chat.display.has_more);
        state.chat.begin_return_to_live();
    }
    console
        .commands
        .send(UiCommand::RecentHistory)
        .await
        .unwrap();
    console
        .wait(|state| {
            state.chat.mode == HistoryMode::Live
                && state.chat.synchronized
                && state
                    .chat
                    .buffer
                    .events()
                    .back()
                    .is_some_and(|event| event.seq == new_seq + 1)
        })
        .await;
    console.close().await;
    inspector.close().await.unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn console_rejected_past_history_keeps_live_subscription_and_chat_writable() {
    let fixture = Fixture::start_with_history(153).await;
    let proxy = Proxy::start(fixture.url.clone()).await;
    let (inspector, _events) = fixture.operator().await;
    let last = inspector
        .workspace_history(None, Some(1))
        .await
        .unwrap()
        .next_cursor;
    let console = fixture.ui(&proxy.url).await;
    let subscriptions = shared_lock(&proxy.faults).workspace.subscriptions.len();
    shared_lock(&proxy.faults).workspace.reject_forward_history = true;
    console
        .commands
        .send(UiCommand::PreviousHistory)
        .await
        .unwrap();
    console
        .wait(|state| {
            state
                .notice
                .as_ref()
                .is_some_and(|notice| notice.error == Some(RouterErrorCode::InvalidMessage))
        })
        .await;
    {
        let mut state = shared_lock(&console.state);
        assert!(
            state.chat.synchronized,
            "display-only error must not revoke proven live synchronization"
        );
        assert_eq!(state.chat.applied_cursor, Some(last));
        assert!(state.writable().is_ok());
        state
            .composer
            .input
            .insert("Post after rejected historical read")
            .unwrap();
    }
    assert_eq!(
        shared_lock(&proxy.faults).workspace.subscriptions.len(),
        subscriptions,
        "historical rejection must not force resubscription"
    );
    console.commands.send(UiCommand::SubmitChat).await.unwrap();
    console
        .wait(|state| {
            state.composer.input.text().is_empty() && state.chat.applied_cursor == Some(last + 1)
        })
        .await;
    let actual = inspector.workspace_history(None, Some(100)).await.unwrap();
    assert!(actual.events.iter().any(|event| event.seq == last + 1
        && event.content.as_deref() == Some("Post after rejected historical read")));
    shared_lock(&console.state).chat.begin_return_to_live();
    console
        .commands
        .send(UiCommand::RecentHistory)
        .await
        .unwrap();
    console
        .wait(|state| {
            state.chat.mode == HistoryMode::Live
                && state.chat.synchronized
                && state
                    .chat
                    .buffer
                    .events()
                    .back()
                    .is_some_and(|event| event.seq == last + 1)
        })
        .await;
    console.close().await;
    inspector.close().await.unwrap();
    fixture.finish().await;
}
