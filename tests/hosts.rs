use std::{
    ffi::{OsStr, OsString},
    os::unix::fs::PermissionsExt as _,
    path::PathBuf,
    process::Command as StdCommand,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use agent_session_router::{
    cli::{Cli, Command as CliCommand, McpArgs, McpRoleArg},
    client::{ClientConfig, ClientRole},
    config::{DELEGATE_CONTEXT_VERSION, DelegateLaunchContext},
    credentials::{CredentialFile, CredentialRole, write_credential_exclusive},
    hosts::{
        HostError, ManagedClaudeOptions, ManagedCodexOptions, ManagedProviderOptions,
        McpChildSelection, managed_claude_config, managed_codex_config, mcp_invocation,
        preflight_omp_plugin, run_interactive_codex_io, run_owned_provider, setup_claude_plan,
        setup_omp_checked_plan, setup_omp_plan, stock_claude_plan, stock_codex_plan,
        stock_omp_plan,
    },
    install::{AssetError, INTEGRATIONS_ENV},
    mcp::{McpRole, catalog},
    process::LaunchMode,
    protocol::{
        AgentClient, AgentDescriptor, AgentRegistration, AgentSide, AgentStatus, ClientMessage,
        DeliveryMode, HistoryPage, PROTOCOL_VERSION, RegistrationRole, RouterErrorCode,
        ServerMessage, TaskDispatch, TaskExecutionEvidence, TaskFence, WorkspaceEvent,
        WorkspaceEventKind, WorkspaceName, WorkspaceSummary, parse_client_message,
    },
    providers::{
        CancelReason, DelegateContextFile, OwnedProvider, ProviderError, SessionRequest,
        SessionResult, TerminalEvidence, TerminalReason, codex::ThreadSelection,
        filtered_provider_environment,
    },
    tasks::{AttemptStatus, PauseReason, StopEvidence, TaskAttempt},
};
use clap::Parser as _;
use futures_util::{SinkExt as _, StreamExt as _};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader},
    sync::{Mutex, Notify, broadcast, oneshot},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message as WebSocketMessage};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

const INTERACTIVE_CODEX_FAKE: &str = r"
const fs = require('fs');
const readline = require('readline');
const record = process.argv[2];
const delay = Number(process.argv[3] || '0');
let pending = null;
function send(value) { process.stdout.write(JSON.stringify(value) + '\n'); }
function append(value) { fs.appendFileSync(record, JSON.stringify(value) + '\n'); }
function terminal(turnId, status) {
  send({method:'turn/completed', params:{threadId:'thread-1', turn:{id:turnId,status}}});
}
function complete(turnId, text) {
  send({method:'item/completed',params:{threadId:'thread-1',turnId,item:{type:'agentMessage',text,phase:'final_answer'}}});
  terminal(turnId, 'completed');
}
const rl = readline.createInterface({input: process.stdin, crlfDelay: Infinity});
rl.on('line', raw => {
  const message = JSON.parse(raw);
  if (message.method === 'initialize') {
    fs.writeFileSync(record, JSON.stringify({
      cwd: process.cwd(),
      argv: process.argv.slice(2),
      routerToken: process.env.ROUTER_TOKEN || null,
      delegationToken: process.env.AGENT_ROUTER_DELEGATION_TOKEN || null,
    }) + '\n');
    return send({id:message.id,result:{}});
  }
  if (message.method === 'initialized') return;
  if (message.method === 'thread/start') {
    return send({id:message.id,result:{thread:{id:'thread-1'}}});
  }
  if (message.method === 'turn/start') {
    const turnId = 'turn-' + message.id;
    const text = message.params.input[0].text;
    append({turn:text});
    send({id:message.id,result:{turn:{id:turnId}}});
    const timer = setTimeout(() => { pending = null; complete(turnId, 'answer:' + text + '\u001b[31m'); }, delay);
    pending = {turnId,timer};
    return;
  }
  if (message.method === 'turn/interrupt') {
    append({interrupt:message.params.turnId});
    if (pending) {
      clearTimeout(pending.timer);
      const turnId = pending.turnId;
      pending = null;
      send({id:message.id,result:{}});
      return terminal(turnId, 'interrupted');
    }
    return send({id:message.id,result:{}});
  }
});
rl.on('close', () => process.exit(0));
";

#[derive(Clone, Copy)]
enum FakeMode {
    Success,
    WaitForCancel,
}

struct FakeState {
    requests: Mutex<Vec<SessionRequest>>,
    cancellations: Mutex<Vec<(String, CancelReason)>>,
    closed: AtomicBool,
    started: Notify,
    cancelled: Notify,
    order: Mutex<Vec<&'static str>>,
}

impl FakeState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            cancellations: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            started: Notify::new(),
            cancelled: Notify::new(),
            order: Mutex::new(Vec::new()),
        })
    }
}

struct FakeProvider {
    mode: FakeMode,
    state: Arc<FakeState>,
    terminal: broadcast::Sender<TerminalEvidence>,
}

impl FakeProvider {
    fn new(mode: FakeMode, state: Arc<FakeState>) -> Self {
        let (terminal, _) = broadcast::channel(8);
        Self {
            mode,
            state,
            terminal,
        }
    }
}

impl OwnedProvider for FakeProvider {
    fn ready(&self) -> bool {
        !self.state.closed.load(Ordering::Acquire)
    }

    async fn handle(&self, request: SessionRequest) -> SessionResult {
        self.state.requests.lock().await.push(request.clone());
        self.state.started.notify_one();
        match self.mode {
            FakeMode::Success => {
                let _ = self.terminal.send(TerminalEvidence {
                    request_id: request.request_id,
                    reason: TerminalReason::TurnEnded,
                    child_reaped: false,
                });
                SessionResult::success("provider result")
            }
            FakeMode::WaitForCancel => {
                self.state.cancelled.notified().await;
                SessionResult::failure(ProviderError::ProviderDisconnected)
            }
        }
    }

    async fn cancel(&self, request_id: String, reason: CancelReason) -> Result<(), ProviderError> {
        self.state
            .cancellations
            .lock()
            .await
            .push((request_id.clone(), reason));
        self.state.cancelled.notify_one();
        let terminal = self.terminal.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = terminal.send(TerminalEvidence {
                request_id,
                reason: TerminalReason::RequestCancelled,
                child_reaped: false,
            });
        });
        Ok(())
    }

    async fn close(&self) -> Result<(), ProviderError> {
        self.state.closed.store(true, Ordering::Release);
        self.state.order.lock().await.push("provider_close");
        self.state.cancelled.notify_waiters();
        Ok(())
    }

    fn subscribe_terminal(&self) -> broadcast::Receiver<TerminalEvidence> {
        self.terminal.subscribe()
    }
}

async fn recv_client<S>(socket: &mut WebSocketStream<S>) -> Option<ClientMessage>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let message = socket.next().await?.expect("valid websocket frame");
    let WebSocketMessage::Text(text) = message else {
        return None;
    };
    Some(parse_client_message(text.as_str()).expect("valid client message"))
}

async fn send_server<S>(socket: &mut WebSocketStream<S>, message: &ServerMessage)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    socket
        .send(WebSocketMessage::Text(
            serde_json::to_string(message).unwrap().into(),
        ))
        .await
        .unwrap();
}

async fn accept_router(
    listener: tokio::net::TcpListener,
) -> WebSocketStream<tokio::net::TcpStream> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut socket = accept_async(stream).await.unwrap();
    assert!(matches!(
        recv_client(&mut socket).await,
        Some(ClientMessage::Register { .. })
    ));
    send_server(
        &mut socket,
        &ServerMessage::Registered {
            protocol_version: PROTOCOL_VERSION,
            agent: AgentDescriptor {
                agent_id: "local:host".to_owned(),
                side: AgentSide::Codex,
                client: AgentClient::CodexAppServer,
                activity: None,
                status: AgentStatus::Idle,
                delivery_mode: DeliveryMode::Push,
                ready: false,
                session_id: Uuid::new_v4(),
            },
            role: RegistrationRole::Agent,
            workspace: None,
            cursor: 0,
        },
    )
    .await;
    socket
}

fn client_config(address: std::net::SocketAddr, delegation: bool) -> ClientConfig {
    let workspace = WorkspaceName::parse("host-room").unwrap();
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        "local:host".to_owned(),
        Some(AgentSide::Codex),
        Some(AgentClient::CodexAppServer),
        vec![workspace],
    )
    .unwrap();
    let delegation_token = delegation.then(|| {
        CredentialFile::generate(
            CredentialRole::Agent,
            "local:delegate".to_owned(),
            Some(AgentSide::Codex),
            Some(AgentClient::CodexAppServer),
            Vec::new(),
        )
        .unwrap()
        .token()
        .clone()
    });
    ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).unwrap(),
        role: ClientRole::Primary {
            agent: AgentRegistration {
                agent_id: "local:host".to_owned(),
                side: AgentSide::Codex,
                client: AgentClient::CodexAppServer,
                activity: None,
                delivery_mode: DeliveryMode::Push,
            },
            credential,
            delegation_token,
        },
        ca_file: None,
    }
}

fn node_executable() -> PathBuf {
    let output = StdCommand::new("which").arg("node").output().unwrap();
    assert!(output.status.success());
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
}

fn interactive_codex_options(
    files: &TempDir,
    caller_cwd: &TempDir,
    delay_ms: u64,
) -> (ManagedCodexOptions, PathBuf) {
    let script = files.path().join("interactive-codex.js");
    let record = files.path().join("interactive-record.jsonl");
    std::fs::write(&script, INTERACTIVE_CODEX_FAKE).unwrap();
    let path = std::env::var_os("PATH").unwrap_or_default();
    (
        ManagedCodexOptions {
            common: ManagedProviderOptions {
                asr_executable: std::env::current_exe().unwrap(),
                caller_cwd: caller_cwd.path().to_path_buf(),
                source_environment: vec![
                    ("PATH".into(), path),
                    ("HOME".into(), "/safe-home".into()),
                    ("OPENAI_API_KEY".into(), "provider-secret".into()),
                    ("ROUTER_TOKEN".into(), "router-secret".into()),
                    (
                        "AGENT_ROUTER_DELEGATION_TOKEN".into(),
                        "delegate-secret".into(),
                    ),
                ],
            },
            executable: node_executable(),
            executable_arguments: vec![
                script.into_os_string(),
                record.as_os_str().to_owned(),
                delay_ms.to_string().into(),
            ],
            thread: ThreadSelection::start(),
        },
        record,
    )
}

async fn join_response(
    socket: &mut WebSocketStream<tokio::net::TcpStream>,
    request_id: String,
    workspace: &WorkspaceName,
) {
    send_server(
        socket,
        &ServerMessage::WorkspaceJoined {
            request_id,
            workspace: workspace.clone(),
            cursor: 0,
        },
    )
    .await;
}

#[derive(Default)]
struct InteractiveRouterState {
    readiness: Mutex<Vec<bool>>,
    replies: Mutex<Vec<(String, bool, Option<RouterErrorCode>)>>,
    workspace_posts: AtomicBool,
    deliver_competing: Notify,
}

async fn run_no_room_interactive_router(
    listener: tokio::net::TcpListener,
    state: Arc<InteractiveRouterState>,
) {
    let mut socket = accept_router(listener).await;
    while let Some(message) = recv_client(&mut socket).await {
        match message {
            ClientMessage::Readiness { ready } => {
                state.readiness.lock().await.push(ready);
            }
            ClientMessage::Ping { request_id } => {
                send_server(&mut socket, &ServerMessage::Pong { request_id }).await;
            }
            ClientMessage::WorkspacePost { .. } => {
                state.workspace_posts.store(true, Ordering::Release);
            }
            _ => panic!("unexpected no-room interactive message"),
        }
    }
}

async fn run_targeted_interactive_router(
    listener: tokio::net::TcpListener,
    workspace: WorkspaceName,
    state: Arc<InteractiveRouterState>,
) {
    let mut socket = accept_router(listener).await;
    let mut delivered = false;
    loop {
        tokio::select! {
            () = state.deliver_competing.notified(), if !delivered => {
                delivered = true;
                send_server(
                    &mut socket,
                    &ServerMessage::Deliver {
                        workspace: workspace.clone(),
                        request_id: "competing".to_owned(),
                        from: "local:sender".to_owned(),
                        content: "must not reach provider".to_owned(),
                        timeout_ms: 1_000,
                        task: None,
                    },
                )
                .await;
            }
            message = recv_client(&mut socket) => {
                let Some(message) = message else {
                    break;
                };
                match message {
                    ClientMessage::WorkspaceJoin { request_id, name } => {
                        assert_eq!(name, workspace);
                        join_response(&mut socket, request_id, &workspace).await;
                    }
                    ClientMessage::Readiness { ready } => {
                        state.readiness.lock().await.push(ready);
                    }
                    ClientMessage::Reply {
                        request_id,
                        ok,
                        error,
                        ..
                    } => {
                        state.replies.lock().await.push((request_id, ok, error));
                    }
                    ClientMessage::Ping { request_id } => {
                        send_server(&mut socket, &ServerMessage::Pong { request_id }).await;
                    }
                    _ => panic!("unexpected targeted interactive message"),
                }
            }
        }
    }
}

fn peer_agent() -> AgentDescriptor {
    AgentDescriptor {
        agent_id: "local:peer".to_owned(),
        side: AgentSide::Claude,
        client: AgentClient::ClaudeCode,
        activity: None,
        status: AgentStatus::Idle,
        delivery_mode: DeliveryMode::Push,
        ready: true,
        session_id: Uuid::new_v4(),
    }
}

#[allow(clippy::too_many_lines)]
async fn run_slash_interactive_router(listener: tokio::net::TcpListener, workspace: WorkspaceName) {
    let mut socket = accept_router(listener).await;
    while let Some(message) = recv_client(&mut socket).await {
        match message {
            ClientMessage::Readiness { .. } => panic!("Push client sent explicit readiness"),
            ClientMessage::WorkspaceJoin { request_id, name } => {
                assert_eq!(name, workspace);
                join_response(&mut socket, request_id, &workspace).await;
            }
            ClientMessage::List { request_id } | ClientMessage::WorkspaceMembers { request_id } => {
                send_server(
                    &mut socket,
                    &ServerMessage::Agents {
                        request_id,
                        workspace: workspace.clone(),
                        agents: vec![peer_agent()],
                    },
                )
                .await;
            }
            ClientMessage::WorkspaceList {
                request_id,
                after,
                limit,
            } => {
                assert_eq!(after, None);
                assert_eq!(limit, None);
                send_server(
                    &mut socket,
                    &ServerMessage::Workspaces {
                        request_id,
                        workspaces: vec![WorkspaceSummary {
                            name: workspace.clone(),
                            created_at: 1,
                            connected_agents: 2,
                        }],
                        next_cursor: None,
                        has_more: false,
                    },
                )
                .await;
            }
            ClientMessage::WorkspaceHistory {
                request_id,
                after,
                limit,
            } => {
                assert_eq!(after, None);
                assert_eq!(limit, None);
                send_server(
                    &mut socket,
                    &ServerMessage::WorkspaceHistory {
                        request_id,
                        page: HistoryPage {
                            workspace: workspace.clone(),
                            events: Vec::new(),
                            next_cursor: 0,
                            has_more: false,
                        },
                    },
                )
                .await;
            }
            ClientMessage::WorkspacePost {
                request_id,
                content,
            } => {
                assert_eq!(content, "hello room");
                send_server(
                    &mut socket,
                    &ServerMessage::WorkspacePosted {
                        request_id,
                        workspace: workspace.clone(),
                        seq: 1,
                    },
                )
                .await;
            }
            ClientMessage::Send {
                request_id,
                to,
                content,
                ..
            } => {
                assert_eq!(to, "local:peer");
                assert_eq!(content, "hello peer");
                send_server(
                    &mut socket,
                    &ServerMessage::Accepted {
                        request_id: request_id.clone(),
                        workspace: workspace.clone(),
                        to: to.clone(),
                    },
                )
                .await;
                send_server(
                    &mut socket,
                    &ServerMessage::Result {
                        workspace: workspace.clone(),
                        request_id,
                        from: to,
                        ok: true,
                        content: Some("peer reply".to_owned()),
                        error: None,
                        task_id: None,
                    },
                )
                .await;
            }
            ClientMessage::WorkspaceLeave { request_id } => {
                send_server(
                    &mut socket,
                    &ServerMessage::WorkspaceLeft {
                        request_id,
                        workspace: Some(workspace.clone()),
                    },
                )
                .await;
            }
            ClientMessage::Ping { request_id } => {
                send_server(&mut socket, &ServerMessage::Pong { request_id }).await;
            }
            _ => panic!("unexpected slash interactive message"),
        }
    }
}

async fn read_terminal_line(reader: &mut BufReader<tokio::io::DuplexStream>) -> String {
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    assert!(!line.is_empty());
    line
}

async fn run_success_router(
    listener: tokio::net::TcpListener,
    workspace: WorkspaceName,
    replied: oneshot::Sender<()>,
    order: Arc<FakeState>,
) {
    let mut socket = accept_router(listener).await;
    let mut reply = Some(replied);
    while let Some(message) = recv_client(&mut socket).await {
        match message {
            ClientMessage::WorkspaceJoin { request_id, name } => {
                assert_eq!(name, workspace);
                join_response(&mut socket, request_id, &workspace).await;
                send_server(
                    &mut socket,
                    &ServerMessage::WorkspaceEvent {
                        event: WorkspaceEvent {
                            workspace: workspace.clone(),
                            seq: 1,
                            kind: WorkspaceEventKind::Chat,
                            actor_id: "local:sender".to_owned(),
                            created_at: 1,
                            request_id: None,
                            target_id: None,
                            task_id: None,
                            content: Some("not a provider turn".to_owned()),
                            ok: None,
                            error: None,
                        },
                    },
                )
                .await;
                send_server(
                    &mut socket,
                    &ServerMessage::Deliver {
                        workspace: workspace.clone(),
                        request_id: "work-one".to_owned(),
                        from: "local:sender".to_owned(),
                        content: "provider prompt".to_owned(),
                        timeout_ms: 5_000,
                        task: None,
                    },
                )
                .await;
            }
            ClientMessage::Ping { request_id } => {
                send_server(&mut socket, &ServerMessage::Pong { request_id }).await;
            }
            ClientMessage::Reply {
                request_id,
                ok,
                content,
                error,
            } => {
                assert_eq!(request_id, "work-one");
                assert!(ok);
                assert_eq!(content.as_deref(), Some("provider result"));
                assert_eq!(error, None);
                reply.take().unwrap().send(()).unwrap();
            }
            other => panic!("unexpected success host message: {:?}", other.request_id()),
        }
    }
    order.order.lock().await.push("router_close");
}

#[tokio::test]
async fn delivery_drives_one_turn_one_reply_and_closes_provider_before_router() {
    let workspace = WorkspaceName::parse("host-room").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = FakeState::new();
    let (replied_tx, replied_rx) = oneshot::channel();
    let router = tokio::spawn(run_success_router(
        listener,
        workspace.clone(),
        replied_tx,
        Arc::clone(&state),
    ));
    let shutdown = CancellationToken::new();
    let host = tokio::spawn(run_owned_provider(
        FakeProvider::new(FakeMode::Success, Arc::clone(&state)),
        client_config(address, false),
        Some(workspace),
        shutdown.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(2), replied_rx)
        .await
        .unwrap()
        .unwrap();
    shutdown.cancel();
    assert!(host.await.unwrap().is_ok());
    router.await.unwrap();
    let requests = state.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].request_id, "work-one");
    assert_eq!(requests[0].content, "provider prompt");
    drop(requests);
    assert_eq!(
        state.order.lock().await.as_slice(),
        ["provider_close", "router_close"]
    );
}

async fn run_local_router(
    listener: tokio::net::TcpListener,
    idle: oneshot::Sender<()>,
    state: Arc<FakeState>,
) {
    let mut socket = accept_router(listener).await;
    idle.send(()).unwrap();
    while let Some(message) = recv_client(&mut socket).await {
        match message {
            ClientMessage::Readiness { .. } => panic!("Push client sent explicit readiness"),
            ClientMessage::Ping { request_id } => {
                send_server(&mut socket, &ServerMessage::Pong { request_id }).await;
            }
            ClientMessage::WorkspaceJoin { .. } | ClientMessage::Reply { .. } => {
                panic!("workspace-free host emitted work protocol")
            }
            _ => panic!("unexpected local host message"),
        }
    }
    state.order.lock().await.push("router_close");
}

#[tokio::test]
async fn workspace_none_stays_unready_and_never_starts_a_turn() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = FakeState::new();
    let (idle_tx, idle_rx) = oneshot::channel();
    let router = tokio::spawn(run_local_router(listener, idle_tx, Arc::clone(&state)));
    let shutdown = CancellationToken::new();
    let host = tokio::spawn(run_owned_provider(
        FakeProvider::new(FakeMode::Success, Arc::clone(&state)),
        client_config(address, false),
        None,
        shutdown.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(2), idle_rx)
        .await
        .unwrap()
        .unwrap();
    shutdown.cancel();
    assert!(host.await.unwrap().is_ok());
    router.await.unwrap();
    assert!(state.requests.lock().await.is_empty());
    assert!(state.closed.load(Ordering::Acquire));
}

fn running_attempt(
    workspace_request: &str,
    task_id: i64,
    attempt_id: Uuid,
    session_id: Uuid,
) -> TaskAttempt {
    TaskAttempt {
        id: attempt_id,
        task_id,
        agent_id: "local:host".to_owned(),
        session_id,
        work_request_id: workspace_request.to_owned(),
        resumed_from_checkpoint_id: None,
        status: AttemptStatus::Running,
        stop_evidence: StopEvidence::Unknown,
        reason: None,
        started_at: 1,
        ended_at: None,
        stopped_at: None,
    }
}

async fn finish_cancel_protocol(
    socket: &mut WebSocketStream<tokio::net::TcpStream>,
    workspace: &WorkspaceName,
    fence: &TaskFence,
) {
    loop {
        match recv_client(socket).await.unwrap() {
            ClientMessage::Ping { request_id } => {
                send_server(socket, &ServerMessage::Pong { request_id }).await;
            }
            ClientMessage::TaskExecutionStopped {
                request_id,
                workspace: stopped_workspace,
                task_id,
                attempt_id,
                evidence,
                reason,
                ..
            } => {
                assert_eq!(&stopped_workspace, workspace);
                assert_eq!(task_id, fence.task_id);
                assert_eq!(attempt_id, fence.attempt_id);
                assert_eq!(evidence, TaskExecutionEvidence::ProviderTerminal);
                assert_eq!(reason, PauseReason::RequestCancelled);
                send_server(
                    socket,
                    &ServerMessage::TaskExecutionStoppedAck {
                        request_id,
                        workspace: workspace.clone(),
                        task_id,
                        attempt_id,
                    },
                )
                .await;
            }
            ClientMessage::WorkIdle {
                request_id,
                workspace: idle_workspace,
                work_request_id,
                task,
                ..
            } => {
                assert_eq!(&idle_workspace, workspace);
                assert_eq!(task.as_ref(), Some(fence));
                send_server(
                    socket,
                    &ServerMessage::WorkIdleAck {
                        request_id,
                        workspace: workspace.clone(),
                        work_request_id,
                    },
                )
                .await;
                return;
            }
            ClientMessage::Reply { .. } => panic!("cancelled work replied late"),
            ClientMessage::Readiness { .. } => panic!("Push client sent explicit readiness"),
            _ => panic!("unexpected cancellation terminal protocol"),
        }
    }
}

async fn run_cancel_router(
    listener: tokio::net::TcpListener,
    workspace: WorkspaceName,
    state: Arc<FakeState>,
    fenced: oneshot::Sender<()>,
) {
    let mut fenced = Some(fenced);
    let mut socket = accept_router(listener).await;
    let attempt_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let fence = TaskFence {
        task_id: 7,
        attempt_id,
    };
    while let Some(message) = recv_client(&mut socket).await {
        match message {
            ClientMessage::WorkspaceJoin { request_id, name } => {
                assert_eq!(name, workspace);
                join_response(&mut socket, request_id, &workspace).await;
                send_server(
                    &mut socket,
                    &ServerMessage::TaskAttemptChanged {
                        workspace: workspace.clone(),
                        task_id: 7,
                        attempt: Some(running_attempt("work-cancel", 7, attempt_id, session_id)),
                        closed_attempt_id: None,
                        current: Some(fence.clone()),
                        stop_pending: None,
                    },
                )
                .await;
                send_server(
                    &mut socket,
                    &ServerMessage::Deliver {
                        workspace: workspace.clone(),
                        request_id: "work-cancel".to_owned(),
                        from: "local:sender".to_owned(),
                        content: "cancel me".to_owned(),
                        timeout_ms: 5_000,
                        task: Some(TaskDispatch {
                            id: 7,
                            expected_version: 1,
                        }),
                    },
                )
                .await;
            }
            ClientMessage::Ping { request_id } if fenced.is_some() => {
                send_server(&mut socket, &ServerMessage::Pong { request_id }).await;
                state.started.notified().await;
                send_server(
                    &mut socket,
                    &ServerMessage::CancelWork {
                        workspace: workspace.clone(),
                        request_id: "other-request".to_owned(),
                        reason: RouterErrorCode::RequestCancelled,
                        task: None,
                    },
                )
                .await;
                send_server(
                    &mut socket,
                    &ServerMessage::CancelWork {
                        workspace: workspace.clone(),
                        request_id: "work-cancel".to_owned(),
                        reason: RouterErrorCode::RequestCancelled,
                        task: Some(fence.clone()),
                    },
                )
                .await;
                finish_cancel_protocol(&mut socket, &workspace, &fence).await;
                assert!(
                    tokio::time::timeout(Duration::from_millis(50), recv_client(&mut socket))
                        .await
                        .is_err()
                );
                fenced.take().unwrap().send(()).unwrap();
            }
            ClientMessage::Readiness { .. } => panic!("Push client sent explicit readiness"),
            ClientMessage::Ping { request_id } => {
                send_server(&mut socket, &ServerMessage::Pong { request_id }).await;
            }
            ClientMessage::Reply { .. } => panic!("cancelled work replied"),
            _ => panic!("unexpected cancel host message"),
        }
    }
    state.order.lock().await.push("router_close");
}

#[tokio::test]
async fn exact_cancellation_fences_late_terminal_and_suppresses_reply() {
    let workspace = WorkspaceName::parse("host-room").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = FakeState::new();
    let (fenced_tx, fenced_rx) = oneshot::channel();
    let router = tokio::spawn(run_cancel_router(
        listener,
        workspace.clone(),
        Arc::clone(&state),
        fenced_tx,
    ));
    let shutdown = CancellationToken::new();
    let host = tokio::spawn(run_owned_provider(
        FakeProvider::new(FakeMode::WaitForCancel, Arc::clone(&state)),
        client_config(address, false),
        Some(workspace),
        shutdown.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(2), fenced_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        state.cancellations.lock().await.as_slice(),
        [("work-cancel".to_owned(), CancelReason::RequestCancelled)]
    );
    shutdown.cancel();
    assert!(host.await.unwrap().is_ok());
    router.await.unwrap();
}

fn private_tempdir() -> TempDir {
    let temporary = TempDir::new().unwrap();
    std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    temporary
}

#[tokio::test]
async fn interactive_codex_local_prompt_is_ephemeral_escaped_and_uses_caller_cwd() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = Arc::new(InteractiveRouterState::default());
    let router = tokio::spawn(run_no_room_interactive_router(listener, Arc::clone(&state)));
    let files = TempDir::new().unwrap();
    let cwd = TempDir::new().unwrap();
    let (options, record) = interactive_codex_options(&files, &cwd, 0);
    let (mut input, host_input) = tokio::io::duplex(64 * 1024);
    let (host_output, output) = tokio::io::duplex(64 * 1024);
    let shutdown = CancellationToken::new();
    let host = tokio::spawn(run_interactive_codex_io(
        client_config(address, true),
        None,
        options,
        host_input,
        host_output,
        shutdown,
    ));
    let mut output = BufReader::new(output);

    input.write_all(b"hello local\n").await.unwrap();
    let rendered = read_terminal_line(&mut output).await;
    assert!(rendered.contains("answer:hello local"));
    assert!(rendered.contains("\\u{001B}"));
    assert!(!rendered.contains('\u{1b}'));
    input.write_all(b"quit\n").await.unwrap();

    assert!(host.await.unwrap().is_ok());
    router.await.unwrap();
    let records = std::fs::read_to_string(record).unwrap();
    let mut lines = records.lines();
    let initialized: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!(
        std::fs::canonicalize(initialized["cwd"].as_str().unwrap()).unwrap(),
        std::fs::canonicalize(cwd.path()).unwrap()
    );
    assert!(initialized["routerToken"].is_null());
    assert!(initialized["delegationToken"].is_null());
    assert_eq!(lines.count(), 1);
    assert!(!state.workspace_posts.load(Ordering::Acquire));
    assert!(state.readiness.lock().await.is_empty());
}

#[tokio::test]
async fn interactive_codex_push_rejects_competing_delivery_without_readiness_messages() {
    let workspace = WorkspaceName::parse("host-room").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = Arc::new(InteractiveRouterState::default());
    let router = tokio::spawn(run_targeted_interactive_router(
        listener,
        workspace.clone(),
        Arc::clone(&state),
    ));
    let files = TempDir::new().unwrap();
    let cwd = TempDir::new().unwrap();
    let (options, record) = interactive_codex_options(&files, &cwd, 500);
    let (mut input, host_input) = tokio::io::duplex(64 * 1024);
    let (host_output, output) = tokio::io::duplex(64 * 1024);
    let host = tokio::spawn(run_interactive_codex_io(
        client_config(address, true),
        Some(workspace),
        options,
        host_input,
        host_output,
        CancellationToken::new(),
    ));
    let mut output = BufReader::new(output);

    input.write_all(b"one local turn\n").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if std::fs::read_to_string(&record)
                .unwrap_or_default()
                .contains("\"turn\":\"one local turn\"")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    state.deliver_competing.notify_one();
    assert!(
        read_terminal_line(&mut output)
            .await
            .contains("answer:one local turn")
    );
    input.write_all(b"/quit\n").await.unwrap();
    assert!(host.await.unwrap().is_ok());
    router.await.unwrap();

    assert!(state.readiness.lock().await.is_empty());
    assert_eq!(
        state.replies.lock().await.as_slice(),
        [(
            "competing".to_owned(),
            false,
            Some(RouterErrorCode::SessionBusy)
        )]
    );
    let records = std::fs::read_to_string(record).unwrap();
    assert_eq!(
        records
            .lines()
            .filter(|line| line.contains("\"turn\""))
            .count(),
        1
    );
    assert!(!records.contains("must not reach provider"));
}

#[tokio::test]
async fn interactive_codex_slash_commands_use_router_and_errors_never_become_prompts() {
    let workspace = WorkspaceName::parse("host-room").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = tokio::spawn(run_slash_interactive_router(listener, workspace));
    let files = TempDir::new().unwrap();
    let cwd = TempDir::new().unwrap();
    let (options, record) = interactive_codex_options(&files, &cwd, 0);
    let (mut input, host_input) = tokio::io::duplex(64 * 1024);
    let (host_output, output) = tokio::io::duplex(64 * 1024);
    let host = tokio::spawn(run_interactive_codex_io(
        client_config(address, true),
        None,
        options,
        host_input,
        host_output,
        CancellationToken::new(),
    ));
    let mut output = BufReader::new(output);

    for (command, expected) in [
        ("/workspace join host-room\n", "\"workspace\":\"host-room\""),
        ("/agents\n", "local:peer"),
        ("/workspace list\n", "\"workspaces\""),
        ("/workspace members\n", "local:peer"),
        ("/workspace history\n", "\"events\":[]"),
        ("/workspace post hello room\n", "\"seq\":1"),
        ("/send local:peer hello peer\n", "peer reply"),
        ("/workspace leave\n", "\"workspace\":\"host-room\""),
        ("/send local:peer\n", "error: usage: /send TARGET TEXT"),
    ] {
        input.write_all(command.as_bytes()).await.unwrap();
        let line = read_terminal_line(&mut output).await;
        assert!(line.contains(expected), "{command:?}: {line:?}");
    }
    input.write_all(b"quit\n").await.unwrap();
    assert!(host.await.unwrap().is_ok());
    router.await.unwrap();
    let records = std::fs::read_to_string(record).unwrap();
    assert!(!records.contains("\"turn\""));
}

#[tokio::test]
async fn interactive_codex_shutdown_interrupts_turn_and_closes_provider_and_router() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = Arc::new(InteractiveRouterState::default());
    let router = tokio::spawn(run_no_room_interactive_router(listener, Arc::clone(&state)));
    let files = TempDir::new().unwrap();
    let cwd = TempDir::new().unwrap();
    let (options, record) = interactive_codex_options(&files, &cwd, 10_000);
    let (mut input, host_input) = tokio::io::duplex(64 * 1024);
    let (host_output, _output) = tokio::io::duplex(64 * 1024);
    let shutdown = CancellationToken::new();
    let host = tokio::spawn(run_interactive_codex_io(
        client_config(address, true),
        None,
        options,
        host_input,
        host_output,
        shutdown.clone(),
    ));

    input.write_all(b"block until signal\n").await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if std::fs::read_to_string(&record)
                .is_ok_and(|value| value.contains("\"turn\":\"block until signal\""))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.cancel();
    assert!(host.await.unwrap().is_ok());
    router.await.unwrap();
    assert!(
        std::fs::read_to_string(record)
            .unwrap()
            .contains("\"interrupt\"")
    );
}

#[test]
fn primary_mcp_invocation_derives_identity_and_transport_from_private_claims() {
    let temporary = private_tempdir();
    let root = std::fs::canonicalize(temporary.path()).unwrap();
    for (index, role, expected_role, side, client, delivery_mode) in [
        (
            0,
            McpRoleArg::CodexCli,
            McpRole::CodexCli,
            AgentSide::Codex,
            AgentClient::CodexCli,
            DeliveryMode::Pull,
        ),
        (
            1,
            McpRoleArg::ClaudeChannel,
            McpRole::ClaudeChannel,
            AgentSide::Claude,
            AgentClient::ClaudeCode,
            DeliveryMode::Push,
        ),
        (
            2,
            McpRoleArg::Omp,
            McpRole::Omp,
            AgentSide::Generic,
            AgentClient::Omp,
            DeliveryMode::Push,
        ),
    ] {
        let agent_id = format!("local:mcp-{index}");
        let credential = CredentialFile::generate(
            CredentialRole::Agent,
            agent_id.clone(),
            Some(side),
            Some(client),
            Vec::new(),
        )
        .unwrap();
        let path = root.join(format!("credential-{index}.json"));
        write_credential_exclusive(&path, &credential).unwrap();
        let invocation =
            mcp_invocation(&McpArgs { role }, Some("local"), Some(&path), false).unwrap();

        assert_eq!(invocation.role, expected_role);
        assert_eq!(invocation.agent_id, agent_id);
        assert_eq!(
            invocation.config.router_url.as_str(),
            "ws://127.0.0.1:8787/ws"
        );
        let ClientRole::Primary {
            agent,
            credential,
            delegation_token,
        } = invocation.config.role
        else {
            panic!("expected primary MCP client");
        };
        assert_eq!(agent.agent_id, invocation.agent_id);
        assert_eq!(agent.side, side);
        assert_eq!(agent.client, client);
        assert_eq!(agent.activity, None);
        assert_eq!(agent.delivery_mode, delivery_mode);
        assert_eq!(credential.subject, invocation.agent_id);
        assert!(delegation_token.is_none());
    }
}

#[test]
fn mcp_invocation_rejects_claim_mismatch_and_delegate_selection_overrides() {
    let temporary = private_tempdir();
    let root = std::fs::canonicalize(temporary.path()).unwrap();
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        "local:codex".to_owned(),
        Some(AgentSide::Codex),
        Some(AgentClient::CodexCli),
        Vec::new(),
    )
    .unwrap();
    let credential_path = root.join("credential.json");
    write_credential_exclusive(&credential_path, &credential).unwrap();
    assert!(matches!(
        mcp_invocation(
            &McpArgs {
                role: McpRoleArg::Omp,
            },
            Some("local"),
            Some(&credential_path),
            false,
        ),
        Err(HostError::McpCredentialClaims)
    ));

    let context = DelegateLaunchContext {
        version: DELEGATE_CONTEXT_VERSION,
        router_url: "wss://router.example/ws".to_owned(),
        owner_id: "local:owner".to_owned(),
        delegation_token: credential.token().expose().to_owned(),
        ca_file: None,
    };
    let context_file = DelegateContextFile::create_in(&root, &context).unwrap();
    let args = McpArgs {
        role: McpRoleArg::Delegate {
            context_file: context_file.path().to_path_buf(),
        },
    };
    assert!(matches!(
        mcp_invocation(&args, Some("local"), None, false),
        Err(HostError::McpDelegateSelection)
    ));
    let invocation = mcp_invocation(&args, None, None, false).unwrap();
    assert_eq!(invocation.role, McpRole::Delegate);
    assert_eq!(invocation.agent_id, "local:owner");
    assert_eq!(
        invocation.config.router_url.as_str(),
        "wss://router.example/ws"
    );
    let ClientRole::Delegate {
        owner_id,
        delegation_token,
    } = invocation.config.role
    else {
        panic!("expected delegate MCP client");
    };
    assert_eq!(owner_id, invocation.agent_id);
    assert_eq!(delegation_token.expose(), credential.token().expose());
}

#[test]
fn mcp_dry_run_guard_precedes_context_and_credential_io() {
    let args = McpArgs {
        role: McpRoleArg::Delegate {
            context_file: PathBuf::from("/definitely/missing/context.json"),
        },
    };
    assert!(matches!(
        mcp_invocation(
            &args,
            Some("missing-profile"),
            Some(std::path::Path::new("/definitely/missing/credential.json")),
            true,
        ),
        Err(HostError::McpDryRun)
    ));
}

fn integration_layout() -> (TempDir, PathBuf, PathBuf, Vec<(OsString, OsString)>) {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("integrations");
    std::fs::create_dir_all(root.join("omp")).unwrap();
    std::fs::create_dir_all(root.join("claude-sdk")).unwrap();
    std::fs::write(root.join("omp/index.js"), "export default () => {};").unwrap();
    std::fs::write(root.join("omp/package.json"), "{}").unwrap();
    std::fs::write(root.join("claude-sdk/bridge.js"), "").unwrap();
    let executable = temporary.path().join("bin/asr");
    std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
    std::fs::write(&executable, "").unwrap();
    let environment = vec![
        (
            OsString::from(INTEGRATIONS_ENV),
            root.as_os_str().to_owned(),
        ),
        (
            OsString::from("ROUTER_TOKEN"),
            OsString::from("central-secret"),
        ),
        (
            OsString::from("GITHUB_TOKEN"),
            OsString::from("external-secret"),
        ),
        (OsString::from("PATH"), OsString::from("/bin")),
    ];
    (temporary, root, executable, environment)
}

fn flattened_plan(plan: &agent_session_router::process::LaunchPlan) -> String {
    plan.arguments
        .iter()
        .chain(
            plan.environment
                .iter()
                .flat_map(|(key, value)| [key, value]),
        )
        .map(|value| value.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
}

fn assert_codex_hidden_command(
    plan: &agent_session_router::process::LaunchPlan,
    selection: &McpChildSelection,
) {
    let (overrides, remainder) = plan.arguments[2..plan.arguments.len() - 1].as_chunks::<2>();
    assert!(remainder.is_empty());
    let encoded_arguments = overrides
        .iter()
        .find_map(|[_, value]| {
            value
                .to_str()?
                .strip_prefix("mcp_servers.agent_session_router.args=")
                .map(str::to_owned)
        })
        .unwrap();
    let arguments: Vec<String> = serde_json::from_str(&encoded_arguments).unwrap();
    assert_eq!(
        arguments,
        vec![
            "--profile",
            "device",
            "--credential",
            selection
                .credential_file
                .as_ref()
                .unwrap()
                .to_str()
                .unwrap(),
            "mcp",
            "codex-cli",
        ]
    );
    let cli =
        Cli::try_parse_from(std::iter::once("asr").chain(arguments.iter().map(String::as_str)))
            .unwrap();
    assert!(matches!(
        cli.command,
        Some(CliCommand::Mcp(McpArgs {
            role: McpRoleArg::CodexCli
        }))
    ));
}

fn assert_claude_hidden_command(plan: &agent_session_router::process::LaunchPlan) {
    let separator = plan
        .arguments
        .iter()
        .position(|argument| argument == OsStr::new("--"))
        .unwrap();
    let hidden_arguments = &plan.arguments[separator + 2..];
    assert_eq!(
        hidden_arguments,
        [OsString::from("mcp"), OsString::from("claude-channel"),]
    );
    let cli = Cli::try_parse_from(
        std::iter::once(OsString::from("asr")).chain(hidden_arguments.iter().cloned()),
    )
    .unwrap();
    assert!(matches!(
        cli.command,
        Some(CliCommand::Mcp(McpArgs {
            role: McpRoleArg::ClaudeChannel
        }))
    ));
}

#[test]
fn stock_setup_and_exec_plans_preserve_paths_without_secret_values() {
    let (_temporary, root, executable, environment) = integration_layout();
    let cwd = root.parent().unwrap().to_path_buf();
    let selection = McpChildSelection {
        profile: Some("device".to_owned()),
        credential_file: Some(cwd.join("credential.json")),
    };
    let workspace = WorkspaceName::parse("host-room").unwrap();
    let codex = stock_codex_plan(
        OsString::from("codex"),
        cwd.clone(),
        &executable,
        &selection,
        Some(&workspace),
        vec![OsString::from("--search")],
    )
    .unwrap();
    assert_eq!(codex.caller_cwd, cwd);
    assert_eq!(codex.arguments[0], OsStr::new("-C"));
    assert_eq!(codex.arguments.last().unwrap(), OsStr::new("--search"));
    assert!(flattened_plan(&codex).contains("agent_wait"));
    let (overrides, remainder) = codex.arguments[2..codex.arguments.len() - 1].as_chunks::<2>();
    assert!(remainder.is_empty());
    for [flag, value] in overrides {
        assert_eq!(flag, OsStr::new("-c"));
        assert!(value.to_string_lossy().starts_with("mcp_servers."));
    }
    assert_codex_hidden_command(&codex, &selection);

    let omp = stock_omp_plan(
        OsString::from("omp"),
        cwd.clone(),
        executable.clone(),
        &selection,
        None,
        vec![OsString::from("--resume")],
    )
    .unwrap();
    assert_eq!(omp.arguments, vec![OsString::from("--resume")]);
    assert!(
        omp.arguments
            .iter()
            .all(|argument| argument != OsStr::new("--extension") && argument != OsStr::new("-e"))
    );
    assert!(
        omp.environment
            .iter()
            .all(|(key, _)| key != OsStr::new("ASR_WORKSPACE"))
    );

    let setup_omp = setup_omp_plan(
        OsString::from("omp"),
        cwd.clone(),
        &executable,
        &environment,
    )
    .unwrap();
    assert_eq!(setup_omp.mode, LaunchMode::Wait);
    assert_eq!(
        setup_omp.arguments,
        vec![
            OsString::from("plugin"),
            OsString::from("link"),
            root.join("omp").into_os_string()
        ]
    );

    let setup_claude =
        setup_claude_plan(OsString::from("claude"), cwd.clone(), executable.clone()).unwrap();
    assert_eq!(setup_claude.mode, LaunchMode::Wait);
    assert!(setup_claude.environment.is_empty());
    assert_claude_hidden_command(&setup_claude);

    let claude = stock_claude_plan(
        OsString::from("claude"),
        cwd,
        &selection,
        Some(&workspace),
        true,
        Some("session-id".to_owned()),
    );
    assert!(claude.arguments.contains(&OsString::from("--resume")));
    for plan in [&codex, &claude] {
        assert!(plan.environment.iter().any(|(key, value)| {
            key == OsStr::new("ASR_WORKSPACE") && value == OsStr::new("host-room")
        }));
    }
    for plan in [&codex, &omp, &setup_omp, &setup_claude, &claude] {
        let flattened = flattened_plan(plan);
        assert!(!flattened.contains("central-secret"));
        assert!(!flattened.contains("external-secret"));
    }
}

#[cfg(unix)]
#[test]
fn stock_provider_plans_preserve_opaque_forwarded_arguments() {
    use std::os::unix::ffi::OsStringExt as _;

    let (temporary, _root, executable, _environment) = integration_layout();
    let cwd = temporary.path().to_path_buf();
    let selection = McpChildSelection::default();
    let opaque = OsString::from_vec(vec![b'-', b'-', b'x', 0xff, b'y']);
    let codex = stock_codex_plan(
        OsString::from("codex"),
        cwd.clone(),
        &executable,
        &selection,
        None,
        vec![opaque.clone()],
    )
    .unwrap();
    assert_eq!(codex.arguments.last(), Some(&opaque));
    let omp = stock_omp_plan(
        OsString::from("omp"),
        cwd,
        executable,
        &selection,
        None,
        vec![opaque.clone()],
    )
    .unwrap();
    assert_eq!(omp.arguments, vec![opaque]);
}

#[cfg(unix)]
fn write_omp_list_program(path: &std::path::Path, value: &serde_json::Value) {
    use std::os::unix::fs::PermissionsExt as _;

    let json = serde_json::to_string(value).unwrap();
    std::fs::write(path, format!("#!/bin/sh\nprintf '%s\\n' '{json}'\n")).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(unix)]
fn write_logged_omp_list_program(
    path: &std::path::Path,
    log: &std::path::Path,
    value: &serde_json::Value,
) {
    use std::os::unix::fs::PermissionsExt as _;

    let json = serde_json::to_string(value).unwrap();
    std::fs::write(
        path,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nprintf '%s\\n' '{json}'\n",
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn omp_preflight_requires_the_linked_enabled_packaged_plugin() {
    let (temporary, root, executable, environment) = integration_layout();
    let program = temporary.path().join("omp");
    let plugin = |enabled| {
        serde_json::json!({
            "npm": [{
                "name": "@agent-session-router/omp-integration",
                "version": "0.1.0",
                "path": root.join("omp"),
                "manifest": {},
                "enabledFeatures": null,
                "enabled": enabled
            }],
            "marketplace": []
        })
    };
    write_omp_list_program(&program, &plugin(true));
    preflight_omp_plugin(
        program.as_os_str(),
        temporary.path(),
        &executable,
        &environment,
    )
    .await
    .unwrap();

    write_omp_list_program(&program, &plugin(false));
    let disabled = preflight_omp_plugin(
        program.as_os_str(),
        temporary.path(),
        &executable,
        &environment,
    )
    .await
    .unwrap_err();
    assert!(matches!(disabled, HostError::OmpEnableRequired));
    assert_eq!(
        disabled.to_string(),
        "OMP integration is disabled; run `omp plugin enable @agent-session-router/omp-integration`"
    );

    write_omp_list_program(
        &program,
        &serde_json::json!({ "npm": [], "marketplace": [] }),
    );
    let missing = preflight_omp_plugin(
        program.as_os_str(),
        temporary.path(),
        &executable,
        &environment,
    )
    .await
    .unwrap_err();
    assert!(matches!(missing, HostError::OmpSetupRequired));
    assert_eq!(
        missing.to_string(),
        "OMP integration is not linked; run `asr setup-omp`"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn setup_omp_preflight_is_idempotent_and_preserves_foreign_installs() {
    let (temporary, root, executable, environment) = integration_layout();
    let program = temporary.path().join("omp");
    let log = temporary.path().join("commands.log");
    let plugin = |path: &std::path::Path, enabled| {
        serde_json::json!({
            "npm": [{
                "name": "@agent-session-router/omp-integration",
                "path": path,
                "enabled": enabled
            }]
        })
    };

    write_logged_omp_list_program(&program, &log, &plugin(&root.join("omp"), true));
    let exact = setup_omp_checked_plan(
        program.as_os_str().to_owned(),
        temporary.path().to_path_buf(),
        &executable,
        &environment,
    )
    .await
    .unwrap();
    assert!(exact.is_none());
    assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 1);

    let foreign = temporary.path().join("foreign-plugin");
    std::fs::create_dir_all(&foreign).unwrap();
    let marker = foreign.join("keep");
    std::fs::write(&marker, "foreign").unwrap();
    write_logged_omp_list_program(&program, &log, &plugin(&foreign, true));
    let conflict = setup_omp_checked_plan(
        program.as_os_str().to_owned(),
        temporary.path().to_path_buf(),
        &executable,
        &environment,
    )
    .await
    .unwrap_err();
    assert!(matches!(conflict, HostError::OmpSetupConflict));
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "foreign");
    assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 2);

    write_logged_omp_list_program(
        &program,
        &log,
        &serde_json::json!({ "npm": [], "marketplace": [] }),
    );
    let link = setup_omp_checked_plan(
        program.into_os_string(),
        temporary.path().to_path_buf(),
        &executable,
        &environment,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(link.arguments[0], OsStr::new("plugin"));
    assert_eq!(link.arguments[1], OsStr::new("link"));
    assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 3);
}

#[test]
fn plans_report_exact_missing_prerequisites() {
    let temporary = TempDir::new().unwrap();
    let cwd = temporary.path().to_path_buf();
    let selection = McpChildSelection::default();
    assert!(matches!(
        stock_codex_plan(
            OsString::from("codex"),
            cwd.clone(),
            PathBuf::from("relative/asr").as_path(),
            &selection,
            None,
            Vec::new(),
        ),
        Err(HostError::InvalidCurrentExecutable)
    ));
    let executable = temporary.path().join("bin/asr");
    std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
    std::fs::write(&executable, "").unwrap();
    assert!(matches!(
        setup_omp_plan(OsString::from("omp"), cwd, executable.as_path(), &[],),
        Err(HostError::Asset(AssetError::Unavailable))
    ));
}

#[test]
fn managed_configs_use_private_delegate_context_and_existing_tool_catalog() {
    let (temporary, _root, executable, environment) = integration_layout();
    let cwd = temporary.path().to_path_buf();
    let client = client_config("127.0.0.1:9".parse().unwrap(), true);
    let delegation = match &client.role {
        ClientRole::Primary {
            delegation_token: Some(token),
            ..
        } => token.expose().to_owned(),
        _ => unreachable!(),
    };
    let codex = managed_codex_config(
        &client,
        ManagedCodexOptions {
            common: ManagedProviderOptions {
                asr_executable: executable.clone(),
                caller_cwd: cwd.clone(),
                source_environment: environment.clone(),
            },
            executable: PathBuf::from("/bin/codex"),
            executable_arguments: vec![OsString::from("opaque")],
            thread: ThreadSelection::start(),
        },
    )
    .unwrap();
    assert_eq!(codex.executable_arguments, vec![OsString::from("opaque")]);
    assert_eq!(codex.enabled_tools.len(), catalog(McpRole::Delegate).len());
    let codex_arguments = codex
        .launch
        .codex_mcp_overrides(&codex.enabled_tools)
        .unwrap();
    assert!(
        !codex_arguments
            .iter()
            .any(|value| value.to_string_lossy().contains(&delegation))
    );

    let claude = managed_claude_config(
        &client,
        ManagedClaudeOptions {
            common: ManagedProviderOptions {
                asr_executable: executable,
                caller_cwd: cwd,
                source_environment: environment.clone(),
            },
            node_executable: PathBuf::from("/bin/node"),
            node_arguments: vec![OsString::from("--no-warnings")],
            claude_executable: None,
            resume_session_id: Some(Uuid::new_v4().to_string()),
        },
    )
    .unwrap();
    assert_eq!(
        claude.bridge_asset,
        temporary.path().join("integrations/claude-sdk/bridge.js")
    );
    let mcp = claude.launch.claude_mcp_config().unwrap().to_string();
    assert!(!mcp.contains(&delegation));
    let filtered = filtered_provider_environment(environment);
    assert_eq!(
        filtered,
        vec![(OsString::from("PATH"), OsString::from("/bin"))]
    );
}
