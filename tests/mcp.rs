#[allow(dead_code)]
#[path = "../src/mcp.rs"]
mod mcp;

use agent_session_router::{cli::McpRoleArg, client, credentials, protocol, providers, tasks};

use std::{
    collections::HashSet,
    future::Future,
    io::Write as _,
    pin::Pin,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use client::{ClientConfig, ClientRole};
use credentials::{CredentialFile, CredentialRole};
use futures_util::{Sink, SinkExt as _, StreamExt as _};
use protocol::{
    AgentClient, AgentDescriptor, AgentRegistration, AgentSide, AgentStatus, ClientMessage,
    DeliveryMode, HistoryPage, PROTOCOL_VERSION, RegistrationRole, ServerMessage, WorkspaceEvent,
    WorkspaceEventKind, WorkspaceName, WorkspaceSummary, parse_client_message,
};
use rmcp::{
    ClientHandler, RoleClient, RoleServer, ServiceExt as _,
    model::{
        CallToolRequest, CallToolRequestParams, ClientJsonRpcMessage, ClientNotification,
        ClientRequest, CustomNotification, JsonObject, ProtocolVersion,
    },
    service::{NotificationContext, PeerRequestOptions, RunningService},
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    sync::{Mutex, Notify, mpsc, oneshot},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message as WebSocketMessage};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

fn names(role: mcp::McpRole) -> HashSet<String> {
    mcp::catalog(role)
        .into_iter()
        .map(|tool| tool.name.into_owned())
        .collect()
}

#[test]
fn hidden_cli_roles_map_one_to_one_to_production_mcp_roles() {
    use agent_session_router::mcp::McpRole as ProductionRole;

    for (argument, expected) in [
        (McpRoleArg::CodexCli, ProductionRole::CodexCli),
        (McpRoleArg::ClaudeChannel, ProductionRole::ClaudeChannel),
        (McpRoleArg::Omp, ProductionRole::Omp),
    ] {
        assert_eq!(argument.mcp_role(), expected);
    }
    assert_eq!(
        McpRoleArg::Delegate {
            context_file: "/private/context.json".into(),
        }
        .mcp_role(),
        ProductionRole::Delegate
    );
}

#[test]
fn catalog_is_role_aware_and_model_exclusions_never_appear() {
    let delegate = names(mcp::McpRole::Delegate);
    let codex = names(mcp::McpRole::CodexCli);
    let claude = names(mcp::McpRole::ClaudeChannel);
    let omp = names(mcp::McpRole::Omp);

    let expected_delegate = [
        "agent_list",
        "agent_send",
        "workspace_list",
        "workspace_join",
        "workspace_leave",
        "workspace_members",
        "workspace_post",
        "workspace_history",
        "task_list",
        "task_get",
        "task_history",
        "task_create",
        "task_edit",
        "task_assign",
        "task_note",
        "task_begin",
        "task_checkpoint",
        "task_pause",
        "task_complete",
        "task_cancel",
        "task_reopen",
        "task_request",
        "integration_list",
        "task_import",
        "task_link",
        "task_publish",
        "task_external_status",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<HashSet<_>>();
    assert_eq!(delegate, expected_delegate);
    assert_eq!(codex.len(), 29);
    assert_eq!(claude.len(), 28);
    assert_eq!(omp.len(), 28);
    assert!(!delegate.contains("agent_wait"));
    assert!(!delegate.contains("agent_reply"));
    assert!(codex.contains("agent_wait"));
    assert!(codex.contains("agent_reply"));
    assert!(!claude.contains("agent_wait"));
    assert!(claude.contains("agent_reply"));
    assert!(!omp.contains("agent_wait"));
    assert!(omp.contains("agent_reply"));

    for forbidden in [
        "task_interrupt",
        "task_confirm_stopped",
        "task_execution_stopped",
        "task_external_resolve",
        "integration_reload",
        "integration_check",
        "work_idle",
        "workspace_create",
        "credential_issue",
        "router_shutdown",
    ] {
        assert!(
            !codex.contains(forbidden),
            "internal/admin tool leaked into model catalog: {forbidden}"
        );
    }

    assert_eq!(mcp::MCP_STDOUT_TIMEOUT, Duration::from_secs(5));
}

#[test]
fn hints_match_read_destructive_and_open_world_contracts() {
    let tools = mcp::catalog(mcp::McpRole::CodexCli)
        .into_iter()
        .map(|tool| (tool.name.to_string(), tool))
        .collect::<std::collections::HashMap<_, _>>();

    let read = tools["task_get"].annotations.as_ref().unwrap();
    assert_eq!(read.read_only_hint, Some(true));
    assert_eq!(read.destructive_hint, Some(false));
    assert_eq!(read.idempotent_hint, Some(true));
    assert_eq!(read.open_world_hint, Some(false));

    let checkpoint = tools["task_checkpoint"].annotations.as_ref().unwrap();
    assert_eq!(checkpoint.read_only_hint, Some(false));
    assert_eq!(checkpoint.destructive_hint, Some(false));
    assert_eq!(checkpoint.idempotent_hint, Some(false));
    assert_eq!(checkpoint.open_world_hint, Some(false));

    let edit = tools["task_edit"].annotations.as_ref().unwrap();
    assert_eq!(edit.destructive_hint, Some(true));
    assert_eq!(edit.open_world_hint, Some(false));

    let request = tools["task_request"].annotations.as_ref().unwrap();
    assert_eq!(request.destructive_hint, Some(false));
    assert_eq!(request.open_world_hint, Some(true));

    let link = tools["task_link"].annotations.as_ref().unwrap();
    assert_eq!(link.destructive_hint, Some(true));
    assert_eq!(link.open_world_hint, Some(true));
}

#[test]
fn claude_capability_and_allowlist_prefix_are_role_specific() {
    let claude = mcp::server_config(mcp::McpRole::ClaudeChannel, "reviewer");
    let encoded = serde_json::to_value(&claude).unwrap();
    assert_eq!(
        encoded["capabilities"]["experimental"]["claude/channel"],
        json!({})
    );
    assert!(encoded["capabilities"]["tools"].is_object());
    assert!(encoded["capabilities"].get("permissions").is_none());
    assert!(
        claude
            .instructions
            .as_deref()
            .unwrap()
            .contains("Agent: reviewer")
    );

    let claude_names = mcp::enabled_tool_names(mcp::McpRole::ClaudeChannel);
    assert!(
        claude_names
            .iter()
            .all(|name| name.starts_with("mcp__agent_session_router__"))
    );
    let codex_names = mcp::enabled_tool_names(mcp::McpRole::CodexCli);
    assert!(codex_names.iter().all(|name| !name.starts_with("mcp__")));
}

#[test]
fn every_catalog_entry_has_an_object_schema_and_strict_known_fields() {
    for role in [
        mcp::McpRole::Delegate,
        mcp::McpRole::CodexCli,
        mcp::McpRole::ClaudeChannel,
        mcp::McpRole::Omp,
    ] {
        for tool in mcp::catalog(role) {
            let schema = tool.schema_as_json_value();
            assert_eq!(schema["type"], "object", "{} schema", tool.name);
            assert_eq!(
                schema["additionalProperties"], false,
                "{} must reject unknown fields",
                tool.name
            );
        }
    }
}

fn object(value: Value) -> JsonObject {
    match value {
        Value::Object(object) => object,
        _ => panic!("test fixture must be an object"),
    }
}

#[test]
fn typed_catalog_validation_accepts_contract_and_rejects_shape_drift() {
    let valid = object(json!({
        "workspace": "room",
        "title": "Review boundary",
        "description": "Check validation and failure handling"
    }));
    let call = mcp::validate_tool_call(mcp::McpRole::Delegate, "task_create", Some(valid)).unwrap();
    assert_eq!(call.name(), "task_create");

    let unknown = object(json!({
        "workspace": "room",
        "title": "Review",
        "description": "Details",
        "token": "secret-sentinel"
    }));
    assert_eq!(
        mcp::validate_tool_call(mcp::McpRole::Delegate, "task_create", Some(unknown)).unwrap_err(),
        mcp::CatalogError::InvalidArguments
    );

    let wrong_type = object(json!({"workspace": "room", "taskId": "1"}));
    assert_eq!(
        mcp::validate_tool_call(mcp::McpRole::Delegate, "task_get", Some(wrong_type)).unwrap_err(),
        mcp::CatalogError::InvalidArguments
    );

    let request_with_receipt = object(json!({
        "workspace": "room",
        "taskId": 1,
        "expectedVersion": 2,
        "operationId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    }));
    assert_eq!(
        mcp::validate_tool_call(
            mcp::McpRole::CodexCli,
            "task_request",
            Some(request_with_receipt)
        )
        .unwrap_err(),
        mcp::CatalogError::InvalidArguments
    );
}

#[test]
fn wait_schema_is_primary_codex_only_and_bounded() {
    assert_eq!(
        mcp::validate_tool_call(
            mcp::McpRole::Delegate,
            "agent_wait",
            Some(object(json!({})))
        )
        .unwrap_err(),
        mcp::CatalogError::ToolNotAvailable
    );
    assert!(
        mcp::validate_tool_call(
            mcp::McpRole::CodexCli,
            "agent_wait",
            Some(object(json!({})))
        )
        .is_ok()
    );
    for wait_ms in [0, 60_001] {
        assert_eq!(
            mcp::validate_tool_call(
                mcp::McpRole::CodexCli,
                "agent_wait",
                Some(object(json!({"waitMs": wait_ms})))
            )
            .unwrap_err(),
            mcp::CatalogError::InvalidWait
        );
    }
}

#[test]
fn admission_is_fail_fast_at_exactly_32_calls() {
    let admission = mcp::CallAdmission::new();
    let permits = (0..mcp::MCP_MAX_CONCURRENT_CALLS)
        .map(|_| admission.try_enter().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(admission.try_enter().unwrap_err(), mcp::AdmissionError);
    drop(permits.into_iter().next());
    assert!(admission.try_enter().is_ok());
}

#[test]
fn structured_output_is_single_copy_bounded_and_has_short_text() {
    let payload = json!({"items": [1, 2, 3], "body": "secret-sentinel"});
    let result = mcp::bounded_tool_result("task_get", payload.clone()).unwrap();
    assert_eq!(result.structured_content, Some(payload));
    let text = result.content[0].as_text().unwrap();
    assert_eq!(text.text, "task_get succeeded");
    assert!(!text.text.contains("secret-sentinel"));

    let sentinel = "OVERSIZE_SECRET_SENTINEL_687e";
    let oversized = Value::String(format!(
        "{sentinel}{}",
        "x".repeat(mcp::MCP_MAX_FRAME_BYTES)
    ));
    let error = mcp::bounded_tool_result("task_get", oversized).unwrap_err();
    assert_eq!(error, mcp::OutputError::TooLarge);
    assert!(!format!("{error:?} {error}").contains(sentinel));
}

#[tokio::test]
async fn bounded_transport_closes_on_malformed_input_without_protocol_output() {
    let sentinel = "RAW_SECRET_SENTINEL_48c6";
    let (server_io, mut client_io) = tokio::io::duplex(4096);
    let (reader, writer) = tokio::io::split(server_io);
    let (mut transport, health) = mcp::bounded_framed_transport(reader, writer);

    client_io
        .write_all(format!("{{not-json:{sentinel}}}\n").as_bytes())
        .await
        .unwrap();
    client_io.shutdown().await.unwrap();
    let received: Option<ClientJsonRpcMessage> =
        <_ as rmcp::transport::Transport<RoleServer>>::receive(&mut transport).await;
    assert!(received.is_none());
    assert!(health.input_failed());

    let mut output = [0_u8; 64];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), client_io.read(&mut output))
            .await
            .is_err(),
        "framing primitive must not echo raw malformed input"
    );
}

#[tokio::test]
async fn bounded_transport_rejects_a_newline_free_frame_over_one_mib() {
    let capacity = mcp::MCP_MAX_FRAME_BYTES + 2;
    let (server_io, mut client_io) = tokio::io::duplex(capacity);
    let (reader, writer) = tokio::io::split(server_io);
    let (mut transport, health) =
        mcp::bounded_framed_transport_with_timeout(reader, writer, Duration::from_secs(1));

    client_io
        .write_all(&vec![b'x'; mcp::MCP_MAX_FRAME_BYTES + 1])
        .await
        .unwrap();
    client_io.shutdown().await.unwrap();
    let received: Option<ClientJsonRpcMessage> =
        <_ as rmcp::transport::Transport<RoleServer>>::receive(&mut transport).await;
    assert!(received.is_none());
    assert!(health.input_failed());
}

struct StalledSink;

impl Sink<Value> for StalledSink {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _item: Value) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn guarded_stdout_fails_after_deadline_instead_of_stalling() {
    let timed_out = Arc::new(AtomicBool::new(false));
    let mut sink = mcp::GuardedSink::new(
        StalledSink,
        Duration::from_millis(20),
        Arc::clone(&timed_out),
    );
    let error = sink.send(json!({"ok": true})).await.unwrap_err();
    assert_eq!(error, mcp::GuardedSinkError::Timeout);
    assert!(timed_out.load(Ordering::Acquire));
}

struct ReadySink;

impl Sink<Value> for ReadySink {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _item: Value) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn guarded_stdout_rejects_an_overlong_encoded_message() {
    let timed_out = Arc::new(AtomicBool::new(false));
    let mut sink = mcp::GuardedSink::new(ReadySink, Duration::from_secs(1), timed_out);
    let error = sink
        .send(Value::String("x".repeat(mcp::MCP_MAX_FRAME_BYTES)))
        .await
        .unwrap_err();
    assert_eq!(error, mcp::GuardedSinkError::OutputTooLarge);
}

#[derive(Default)]
struct StatefulBackend {
    messages: Mutex<Vec<String>>,
    notification_sink: Mutex<Option<mcp::McpNotificationSink>>,
    omp_notifications: Mutex<Vec<mcp::OmpHostNotification>>,
    waits_started: AtomicUsize,
    waits_cancelled: AtomicUsize,
    wait_release: Notify,
    closes: AtomicUsize,
}

impl StatefulBackend {
    async fn wait_for_counter(&self, counter: &AtomicUsize, target: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while counter.load(Ordering::Acquire) < target {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("backend counter did not reach target");
    }

    async fn wait_for_omp_count(&self, target: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.omp_notifications.lock().await.len() < target {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("OMP hook did not reach target");
    }

    async fn sink(&self) -> mcp::McpNotificationSink {
        self.notification_sink
            .lock()
            .await
            .clone()
            .expect("initialize must install notification sink")
    }
}

impl mcp::McpBackend for StatefulBackend {
    async fn dispatch(
        &self,
        call: mcp::McpCall,
        cancellation: CancellationToken,
    ) -> Result<Value, mcp::BackendError> {
        match call {
            mcp::McpCall::AgentList(_) => Ok(json!({
                "agents": [{
                    "id": "local:reviewer",
                    "ready": true,
                    "deliveryMode": "push"
                }]
            })),
            mcp::McpCall::WorkspacePost(args) => {
                let mut messages = self.messages.lock().await;
                messages.push(args.content);
                Ok(json!({ "sequence": messages.len() }))
            }
            mcp::McpCall::WorkspaceHistory(args) => {
                let messages = self.messages.lock().await;
                let start = usize::try_from(args.after.unwrap_or(0))
                    .unwrap_or(usize::MAX)
                    .min(messages.len());
                let limit = usize::from(args.limit.unwrap_or(50));
                let items = messages
                    .iter()
                    .skip(start)
                    .take(limit)
                    .cloned()
                    .collect::<Vec<_>>();
                Ok(json!({ "items": items, "nextCursor": start + items.len() }))
            }
            mcp::McpCall::AgentWait(_) => {
                self.waits_started.fetch_add(1, Ordering::AcqRel);
                tokio::select! {
                    () = cancellation.cancelled() => {
                        self.waits_cancelled.fetch_add(1, Ordering::AcqRel);
                        Err(mcp::BackendError::Cancelled)
                    }
                    () = self.wait_release.notified() => Ok(json!({ "request": null })),
                }
            }
            mcp::McpCall::TaskGet(_) => Err(mcp::BackendError::NotFound),
            _ => Err(mcp::BackendError::InvalidState),
        }
    }

    async fn connected(
        &self,
        notifications: mcp::McpNotificationSink,
    ) -> Result<(), mcp::BackendError> {
        *self.notification_sink.lock().await = Some(notifications);
        Ok(())
    }

    async fn omp_notification(
        &self,
        notification: mcp::OmpHostNotification,
    ) -> Result<(), mcp::BackendError> {
        self.omp_notifications.lock().await.push(notification);
        Ok(())
    }

    fn close(&self) -> impl Future<Output = Result<(), mcp::BackendError>> + Send {
        self.closes.fetch_add(1, Ordering::AcqRel);
        std::future::ready(Ok(()))
    }
}

type OfficialClient = RunningService<RoleClient, ()>;

async fn start_official_client(
    role: mcp::McpRole,
    backend: Arc<StatefulBackend>,
) -> (
    OfficialClient,
    tokio::task::JoinHandle<Result<(), mcp::McpRuntimeError>>,
) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (client_reader, client_writer) = tokio::io::split(client_io);
    let (server_reader, server_writer) = tokio::io::split(server_io);
    let server = mcp::McpServer::new(backend, role, "local:test");
    let server_task = tokio::spawn(mcp::serve_io(server, server_reader, server_writer));
    let client: OfficialClient =
        ().serve((client_reader, client_writer))
            .await
            .expect("official client initialize");
    (client, server_task)
}

fn tool_params(name: &'static str, arguments: Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name).with_arguments(object(arguments))
}

async fn stop_official_client(
    client: OfficialClient,
    server_task: tokio::task::JoinHandle<Result<(), mcp::McpRuntimeError>>,
) {
    client.cancel().await.expect("close official client");
    assert_eq!(server_task.await.expect("server task"), Ok(()));
}

async fn recv_router_client<S>(socket: &mut WebSocketStream<S>) -> Option<ClientMessage>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let message = socket
        .next()
        .await?
        .expect("valid router websocket message");
    let WebSocketMessage::Text(text) = message else {
        return None;
    };
    Some(parse_client_message(text.as_str()).expect("valid router client protocol"))
}

async fn send_router_server<S>(socket: &mut WebSocketStream<S>, message: &ServerMessage)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    socket
        .send(WebSocketMessage::Text(
            serde_json::to_string(message)
                .expect("serialize router message")
                .into(),
        ))
        .await
        .expect("send router message");
}

async fn accept_primary_router(
    listener: tokio::net::TcpListener,
    workspace: Option<WorkspaceName>,
    side: AgentSide,
    agent_client: AgentClient,
    delivery_mode: DeliveryMode,
) -> WebSocketStream<tokio::net::TcpStream> {
    let (stream, _) = listener.accept().await.expect("router connection");
    let mut socket = accept_async(stream).await.expect("router websocket");
    assert!(matches!(
        recv_router_client(&mut socket).await,
        Some(ClientMessage::Register { .. })
    ));
    send_router_server(
        &mut socket,
        &ServerMessage::Registered {
            protocol_version: PROTOCOL_VERSION,
            agent: AgentDescriptor {
                agent_id: "local:mcp".to_owned(),
                side,
                client: agent_client,
                activity: None,
                status: AgentStatus::Idle,
                delivery_mode,
                ready: false,
                session_id: Uuid::new_v4(),
            },
            role: RegistrationRole::Agent,
            workspace,
            cursor: 0,
        },
    )
    .await;
    socket
}

fn router_client_config(
    address: std::net::SocketAddr,
    side: AgentSide,
    agent_client: AgentClient,
    delivery_mode: DeliveryMode,
) -> ClientConfig {
    let workspace = WorkspaceName::parse("mcp-room").unwrap();
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        "local:mcp".to_owned(),
        Some(side),
        Some(agent_client),
        vec![workspace],
    )
    .unwrap();
    ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).unwrap(),
        role: ClientRole::Primary {
            agent: AgentRegistration {
                agent_id: "local:mcp".to_owned(),
                side,
                client: agent_client,
                activity: None,
                delivery_mode,
            },
            credential,
            delegation_token: None,
        },
        ca_file: None,
    }
}

async fn start_router_official_client(
    role: mcp::McpRole,
    config: ClientConfig,
) -> (
    OfficialClient,
    tokio::task::JoinHandle<Result<(), mcp::McpRuntimeError>>,
) {
    let backend = Arc::new(mcp::RouterMcpBackend::new(role, "local:mcp", config).unwrap());
    let (client_io, server_io) = tokio::io::duplex(128 * 1024);
    let (client_reader, client_writer) = tokio::io::split(client_io);
    let (server_reader, server_writer) = tokio::io::split(server_io);
    let server_task = tokio::spawn(mcp::serve_io(
        mcp::McpServer::new(backend, role, "local:mcp"),
        server_reader,
        server_writer,
    ));
    let client = ().serve((client_reader, client_writer)).await.unwrap();
    (client, server_task)
}

async fn start_router_official_client_with_workspace(
    role: mcp::McpRole,
    config: ClientConfig,
    workspace: WorkspaceName,
) -> (
    OfficialClient,
    tokio::task::JoinHandle<Result<(), mcp::McpRuntimeError>>,
) {
    let backend = Arc::new(
        mcp::RouterMcpBackend::new_with_initial_workspace(
            role,
            "local:mcp",
            config,
            Some(workspace),
        )
        .unwrap(),
    );
    let (client_io, server_io) = tokio::io::duplex(128 * 1024);
    let (client_reader, client_writer) = tokio::io::split(client_io);
    let (server_reader, server_writer) = tokio::io::split(server_io);
    let server_task = tokio::spawn(mcp::serve_io(
        mcp::McpServer::new(backend, role, "local:mcp"),
        server_reader,
        server_writer,
    ));
    let client = ().serve((client_reader, client_writer)).await.unwrap();
    (client, server_task)
}

struct PullRouterSignals {
    first_ready: oneshot::Sender<()>,
    first_cancelled: oneshot::Sender<()>,
    replied: oneshot::Sender<()>,
}

async fn handle_pull_router_request(
    socket: &mut WebSocketStream<tokio::net::TcpStream>,
    workspace: &WorkspaceName,
    event: &WorkspaceEvent,
    message: ClientMessage,
) {
    match message {
        ClientMessage::WorkspaceList { request_id, .. } => {
            send_router_server(
                socket,
                &ServerMessage::Workspaces {
                    request_id,
                    workspaces: vec![WorkspaceSummary {
                        name: workspace.clone(),
                        created_at: 1,
                        connected_agents: 1,
                    }],
                    next_cursor: None,
                    has_more: false,
                },
            )
            .await;
        }
        ClientMessage::WorkspaceJoin { request_id, name } => {
            assert_eq!(&name, workspace);
            send_router_server(
                socket,
                &ServerMessage::WorkspaceJoined {
                    request_id,
                    workspace: workspace.clone(),
                    cursor: 0,
                },
            )
            .await;
        }
        ClientMessage::WorkspacePost {
            request_id,
            content,
        } => {
            assert_eq!(content, "durable message");
            send_router_server(
                socket,
                &ServerMessage::WorkspacePosted {
                    request_id,
                    workspace: workspace.clone(),
                    seq: 1,
                },
            )
            .await;
        }
        ClientMessage::WorkspaceHistory { request_id, .. } => {
            send_router_server(
                socket,
                &ServerMessage::WorkspaceHistory {
                    request_id,
                    page: HistoryPage {
                        workspace: workspace.clone(),
                        events: vec![event.clone()],
                        next_cursor: 1,
                        has_more: false,
                    },
                },
            )
            .await;
        }
        ClientMessage::WorkspaceLeave { request_id } => {
            send_router_server(
                socket,
                &ServerMessage::WorkspaceLeft {
                    request_id,
                    workspace: Some(workspace.clone()),
                },
            )
            .await;
        }
        ClientMessage::Ping { request_id } => {
            send_router_server(socket, &ServerMessage::Pong { request_id }).await;
        }
        _ => panic!("unexpected loopback router message"),
    }
}

async fn run_pull_router(
    listener: tokio::net::TcpListener,
    workspace: WorkspaceName,
    signals: PullRouterSignals,
) {
    let mut socket = accept_primary_router(
        listener,
        None,
        AgentSide::Codex,
        AgentClient::CodexCli,
        DeliveryMode::Pull,
    )
    .await;
    let event = WorkspaceEvent {
        workspace: workspace.clone(),
        seq: 1,
        kind: WorkspaceEventKind::Chat,
        actor_id: "local:sender".to_owned(),
        created_at: 7,
        request_id: None,
        target_id: None,
        task_id: None,
        content: Some("durable message".to_owned()),
        ok: None,
        error: None,
    };
    let mut ready_count = 0_u8;
    let mut first_ready = Some(signals.first_ready);
    let mut first_cancelled = Some(signals.first_cancelled);
    let mut replied = Some(signals.replied);
    while let Some(message) = recv_router_client(&mut socket).await {
        match message {
            ClientMessage::Readiness { ready: true } => {
                ready_count += 1;
                if ready_count == 1 {
                    first_ready.take().unwrap().send(()).unwrap();
                } else if ready_count == 2 {
                    send_router_server(
                        &mut socket,
                        &ServerMessage::Deliver {
                            workspace: workspace.clone(),
                            request_id: "pull-request".to_owned(),
                            from: "local:sender".to_owned(),
                            content: "review".to_owned(),
                            timeout_ms: 5_000,
                            task: None,
                        },
                    )
                    .await;
                }
            }
            ClientMessage::Readiness { ready: false } if ready_count == 1 => {
                first_cancelled.take().unwrap().send(()).unwrap();
            }
            ClientMessage::Readiness { ready: false } => {}
            ClientMessage::Reply {
                request_id,
                ok,
                content,
                error,
            } => {
                assert_eq!(request_id, "pull-request");
                assert!(ok);
                assert_eq!(content.as_deref(), Some("done"));
                assert_eq!(error, None);
                replied.take().unwrap().send(()).unwrap();
            }
            request => {
                handle_pull_router_request(&mut socket, &workspace, &event, request).await;
            }
        }
    }
}

async fn exercise_workspace_roundtrip(client: &OfficialClient) {
    let listed = client.list_tools(None).await.unwrap();
    assert!(listed.tools.iter().any(|tool| tool.name == "agent_wait"));
    assert_eq!(listed.tools.len(), 29);
    let workspaces = client
        .call_tool(tool_params(
            "workspace_list",
            json!({ "after": null, "limit": 20 }),
        ))
        .await
        .unwrap();
    assert_eq!(
        workspaces.structured_content.unwrap()["workspaces"][0]["name"],
        "mcp-room"
    );
    let joined = client
        .call_tool(tool_params("workspace_join", json!({ "name": "mcp-room" })))
        .await
        .unwrap();
    assert_eq!(joined.structured_content.unwrap()["cursor"], 0);
    let posted = client
        .call_tool(tool_params(
            "workspace_post",
            json!({ "content": "durable message" }),
        ))
        .await
        .unwrap();
    assert_eq!(posted.structured_content.unwrap()["seq"], 1);
    let history = client
        .call_tool(tool_params(
            "workspace_history",
            json!({ "after": 0, "limit": 20 }),
        ))
        .await
        .unwrap();
    assert_eq!(
        history.structured_content.unwrap()["events"][0]["content"],
        "durable message"
    );
}

async fn exercise_wait_cancel_reply(
    client: &OfficialClient,
    first_ready: oneshot::Receiver<()>,
    first_cancelled: oneshot::Receiver<()>,
    replied: oneshot::Receiver<()>,
) {
    let wait = client
        .peer()
        .send_request_with_option(wait_request(), PeerRequestOptions::no_options())
        .await
        .unwrap();
    first_ready.await.unwrap();
    wait.cancel(Some("stop waiting".to_owned())).await.unwrap();
    first_cancelled.await.unwrap();
    let work = client
        .call_tool(tool_params("agent_wait", json!({ "waitMs": 5_000 })))
        .await
        .unwrap();
    let request = &work.structured_content.as_ref().unwrap()["request"];
    assert_eq!(request["requestId"], "pull-request");
    assert_eq!(request["content"], "review");
    let reply = client
        .call_tool(tool_params(
            "agent_reply",
            json!({ "requestId": "pull-request", "text": "done" }),
        ))
        .await
        .unwrap();
    assert_eq!(reply.structured_content.unwrap()["ok"], true);
    replied.await.unwrap();
}

async fn run_initial_workspace_router(
    listener: tokio::net::TcpListener,
    workspace: WorkspaceName,
    joined: oneshot::Sender<()>,
) {
    let mut socket = accept_primary_router(
        listener,
        None,
        AgentSide::Codex,
        AgentClient::CodexCli,
        DeliveryMode::Pull,
    )
    .await;
    let Some(ClientMessage::WorkspaceJoin { request_id, name }) =
        recv_router_client(&mut socket).await
    else {
        panic!("initial workspace was not joined before MCP work");
    };
    assert_eq!(name, workspace);
    send_router_server(
        &mut socket,
        &ServerMessage::WorkspaceJoined {
            request_id,
            workspace,
            cursor: 0,
        },
    )
    .await;
    joined.send(()).unwrap();
    while let Some(message) = recv_router_client(&mut socket).await {
        match message {
            ClientMessage::Readiness { ready: false } => {}
            ClientMessage::Ping { request_id } => {
                send_router_server(&mut socket, &ServerMessage::Pong { request_id }).await;
            }
            _ => panic!("unexpected initial-workspace client message"),
        }
    }
}

#[tokio::test]
async fn primary_backend_joins_initial_workspace_before_mcp_tools() {
    let workspace = WorkspaceName::parse("mcp-room").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (joined_tx, joined_rx) = oneshot::channel();
    let router = tokio::spawn(run_initial_workspace_router(
        listener,
        workspace.clone(),
        joined_tx,
    ));
    let config = router_client_config(
        address,
        AgentSide::Codex,
        AgentClient::CodexCli,
        DeliveryMode::Pull,
    );
    let (client, server_task) =
        start_router_official_client_with_workspace(mcp::McpRole::CodexCli, config, workspace)
            .await;
    tokio::time::timeout(Duration::from_secs(2), joined_rx)
        .await
        .unwrap()
        .unwrap();
    stop_official_client(client, server_task).await;
    router.await.unwrap();
}

#[tokio::test]
async fn production_backend_official_client_routes_workspace_and_pull_lifecycle() {
    let workspace = WorkspaceName::parse("mcp-room").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (first_ready_tx, first_ready_rx) = oneshot::channel();
    let (first_cancelled_tx, first_cancelled_rx) = oneshot::channel();
    let (replied_tx, replied_rx) = oneshot::channel();
    let router = tokio::spawn(run_pull_router(
        listener,
        workspace,
        PullRouterSignals {
            first_ready: first_ready_tx,
            first_cancelled: first_cancelled_tx,
            replied: replied_tx,
        },
    ));
    let config = router_client_config(
        address,
        AgentSide::Codex,
        AgentClient::CodexCli,
        DeliveryMode::Pull,
    );
    let (client, server_task) = start_router_official_client(mcp::McpRole::CodexCli, config).await;
    exercise_workspace_roundtrip(&client).await;
    exercise_wait_cancel_reply(&client, first_ready_rx, first_cancelled_rx, replied_rx).await;
    let left = client
        .call_tool(tool_params("workspace_leave", json!({})))
        .await
        .unwrap();
    assert_eq!(left.structured_content.unwrap()["workspace"], "mcp-room");
    stop_official_client(client, server_task).await;
    router.await.unwrap();
}

async fn run_delegate_router(listener: tokio::net::TcpListener, connected: oneshot::Sender<()>) {
    let (stream, _) = listener.accept().await.unwrap();
    connected.send(()).unwrap();
    let mut socket = accept_async(stream).await.unwrap();
    assert!(matches!(
        recv_router_client(&mut socket).await,
        Some(ClientMessage::RegisterDelegate { .. })
    ));
    send_router_server(
        &mut socket,
        &ServerMessage::Registered {
            protocol_version: PROTOCOL_VERSION,
            agent: AgentDescriptor {
                agent_id: "local:mcp".to_owned(),
                side: AgentSide::Codex,
                client: AgentClient::CodexCli,
                activity: None,
                status: AgentStatus::Idle,
                delivery_mode: DeliveryMode::Pull,
                ready: false,
                session_id: Uuid::new_v4(),
            },
            role: RegistrationRole::Delegate,
            workspace: None,
            cursor: 0,
        },
    )
    .await;
    while let Some(message) = recv_router_client(&mut socket).await {
        match message {
            ClientMessage::WorkspaceList { request_id, .. } => {
                send_router_server(
                    &mut socket,
                    &ServerMessage::Workspaces {
                        request_id,
                        workspaces: Vec::new(),
                        next_cursor: None,
                        has_more: false,
                    },
                )
                .await;
            }
            ClientMessage::Ping { request_id } => {
                send_router_server(&mut socket, &ServerMessage::Pong { request_id }).await;
            }
            _ => panic!("unexpected delegate router message"),
        }
    }
}

#[tokio::test]
async fn production_delegate_connects_lazily_on_first_tool_call() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        "local:mcp".to_owned(),
        Some(AgentSide::Codex),
        Some(AgentClient::CodexCli),
        Vec::new(),
    )
    .unwrap();
    let config = ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).unwrap(),
        role: ClientRole::Delegate {
            owner_id: "local:mcp".to_owned(),
            delegation_token: credential.token().clone(),
        },
        ca_file: None,
    };
    let (connected_tx, mut connected_rx) = oneshot::channel();
    let router = tokio::spawn(run_delegate_router(listener, connected_tx));
    let (client, server_task) = start_router_official_client(mcp::McpRole::Delegate, config).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut connected_rx)
            .await
            .is_err()
    );
    let result = client
        .call_tool(tool_params(
            "workspace_list",
            json!({ "after": null, "limit": 20 }),
        ))
        .await
        .unwrap();
    assert_eq!(result.structured_content.unwrap()["workspaces"], json!([]));
    connected_rx.await.unwrap();
    stop_official_client(client, server_task).await;
    router.await.unwrap();
}

async fn start_router_recording_client(
    config: ClientConfig,
) -> (
    RunningService<RoleClient, RecordingClient>,
    mpsc::UnboundedReceiver<CustomNotification>,
    tokio::task::JoinHandle<Result<(), mcp::McpRuntimeError>>,
) {
    let backend = Arc::new(
        mcp::RouterMcpBackend::new(mcp::McpRole::ClaudeChannel, "local:mcp", config).unwrap(),
    );
    let (client_io, server_io) = tokio::io::duplex(128 * 1024);
    let (client_reader, client_writer) = tokio::io::split(client_io);
    let (server_reader, server_writer) = tokio::io::split(server_io);
    let server_task = tokio::spawn(mcp::serve_io(
        mcp::McpServer::new(backend, mcp::McpRole::ClaudeChannel, "local:mcp"),
        server_reader,
        server_writer,
    ));
    let (sender, receiver) = mpsc::unbounded_channel();
    let client = RecordingClient {
        notifications: sender,
    }
    .serve((client_reader, client_writer))
    .await
    .unwrap();
    client.list_tools(None).await.unwrap();
    (client, receiver, server_task)
}

async fn send_nontargeted_chat_event(
    socket: &mut WebSocketStream<tokio::net::TcpStream>,
    workspace: &WorkspaceName,
) {
    send_router_server(
        socket,
        &ServerMessage::WorkspaceEvent {
            event: WorkspaceEvent {
                workspace: workspace.clone(),
                seq: 1,
                kind: WorkspaceEventKind::Chat,
                actor_id: "local:sender".to_owned(),
                created_at: 7,
                request_id: None,
                target_id: None,
                task_id: None,
                content: Some("must not notify the model".to_owned()),
                ok: None,
                error: None,
            },
        },
    )
    .await;
}

async fn run_claude_router(
    listener: tokio::net::TcpListener,
    workspace: WorkspaceName,
    replied: oneshot::Sender<()>,
) {
    let mut socket = accept_primary_router(
        listener,
        None,
        AgentSide::Claude,
        AgentClient::ClaudeCode,
        DeliveryMode::Push,
    )
    .await;
    let mut delivered = false;
    let mut replied = Some(replied);
    while let Some(message) = recv_router_client(&mut socket).await {
        match message {
            ClientMessage::Readiness { .. } => panic!("Push MCP client sent explicit readiness"),
            ClientMessage::WorkspaceJoin { request_id, name } => {
                assert_eq!(name, workspace);
                send_router_server(
                    &mut socket,
                    &ServerMessage::WorkspaceJoined {
                        request_id,
                        workspace: workspace.clone(),
                        cursor: 0,
                    },
                )
                .await;
            }
            ClientMessage::List { request_id } => {
                send_router_server(
                    &mut socket,
                    &ServerMessage::Agents {
                        request_id,
                        workspace: workspace.clone(),
                        agents: Vec::new(),
                    },
                )
                .await;
                send_nontargeted_chat_event(&mut socket, &workspace).await;
                if !delivered {
                    delivered = true;
                    send_router_server(
                        &mut socket,
                        &ServerMessage::Deliver {
                            workspace: workspace.clone(),
                            request_id: "claude-request".to_owned(),
                            from: "local:sender".to_owned(),
                            content: "inspect".to_owned(),
                            timeout_ms: 4_000,
                            task: None,
                        },
                    )
                    .await;
                }
            }
            ClientMessage::Reply {
                request_id,
                ok,
                content,
                error,
            } => {
                assert_eq!(request_id, "claude-request");
                assert!(ok);
                assert_eq!(content.as_deref(), Some("complete"));
                assert_eq!(error, None);
                replied.take().unwrap().send(()).unwrap();
            }
            ClientMessage::WorkspaceLeave { request_id } => {
                send_router_server(
                    &mut socket,
                    &ServerMessage::WorkspaceLeft {
                        request_id,
                        workspace: Some(workspace.clone()),
                    },
                )
                .await;
            }
            ClientMessage::Ping { request_id } => {
                send_router_server(&mut socket, &ServerMessage::Pong { request_id }).await;
            }
            _ => panic!("unexpected Claude loopback message"),
        }
    }
}

#[tokio::test]
async fn production_claude_channel_correlates_only_targeted_delivery_and_reply() {
    let workspace = WorkspaceName::parse("mcp-room").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (replied_tx, replied_rx) = oneshot::channel();
    let router = tokio::spawn(run_claude_router(listener, workspace, replied_tx));
    let config = router_client_config(
        address,
        AgentSide::Claude,
        AgentClient::ClaudeCode,
        DeliveryMode::Push,
    );
    let (client, mut notifications, server_task) = start_router_recording_client(config).await;
    client
        .call_tool(tool_params("workspace_join", json!({ "name": "mcp-room" })))
        .await
        .unwrap();
    client
        .call_tool(tool_params("agent_list", json!({})))
        .await
        .unwrap();
    let notification = tokio::time::timeout(Duration::from_secs(1), notifications.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(notification.method, "notifications/claude/channel");
    let params = notification.params.unwrap();
    assert_eq!(params["content"], "inspect");
    assert_eq!(params["meta"]["request_id"], "claude-request");
    assert_eq!(params["meta"]["from"], "local:sender");
    let timeout_ms = params["meta"]["timeout_ms"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!((1..=4_000).contains(&timeout_ms));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), notifications.recv())
            .await
            .is_err()
    );
    client
        .call_tool(tool_params(
            "agent_reply",
            json!({ "requestId": "claude-request", "text": "complete" }),
        ))
        .await
        .unwrap();
    replied_rx.await.unwrap();
    client
        .call_tool(tool_params("workspace_leave", json!({})))
        .await
        .unwrap();
    client.cancel().await.unwrap();
    assert_eq!(server_task.await.unwrap(), Ok(()));
    router.await.unwrap();
}

#[tokio::test]
async fn official_legacy_client_lists_tools_and_runs_stateful_roundtrip() {
    let backend = Arc::new(StatefulBackend::default());
    let (client, server_task) =
        start_official_client(mcp::McpRole::Delegate, Arc::clone(&backend)).await;

    let peer_info = client.peer_info().expect("negotiated server info");
    assert_eq!(peer_info.protocol_version, ProtocolVersion::V_2025_11_25);
    let listed = client.list_tools(None).await.expect("list tools");
    assert_eq!(listed.tools.len(), 27);

    let posted = client
        .call_tool(tool_params(
            "workspace_post",
            json!({ "content": "first durable message" }),
        ))
        .await
        .expect("post message");
    assert_eq!(posted.structured_content, Some(json!({ "sequence": 1 })));

    let history = client
        .call_tool(tool_params(
            "workspace_history",
            json!({ "after": 0, "limit": 20 }),
        ))
        .await
        .expect("read message history");
    assert_eq!(
        history.structured_content,
        Some(json!({
            "items": ["first durable message"],
            "nextCursor": 1
        }))
    );

    let missing = client
        .call_tool(tool_params(
            "task_get",
            json!({ "workspace": "room", "taskId": 404 }),
        ))
        .await
        .expect("tool-level not-found result");
    assert_eq!(missing.is_error, Some(true));
    assert_eq!(
        missing.structured_content,
        Some(json!({ "ok": false, "error": "not_found" }))
    );

    stop_official_client(client, server_task).await;
    assert_eq!(backend.closes.load(Ordering::Acquire), 1);
}

fn wait_request() -> ClientRequest {
    ClientRequest::CallToolRequest(CallToolRequest::new(tool_params(
        "agent_wait",
        json!({ "waitMs": 60_000 }),
    )))
}

#[tokio::test]
async fn official_cancellation_reaches_backend_and_releases_admission() {
    let backend = Arc::new(StatefulBackend::default());
    let (client, server_task) =
        start_official_client(mcp::McpRole::CodexCli, Arc::clone(&backend)).await;
    let handle = client
        .peer()
        .send_request_with_option(wait_request(), PeerRequestOptions::no_options())
        .await
        .expect("start cancellable request");
    backend.wait_for_counter(&backend.waits_started, 1).await;

    handle
        .cancel(Some("caller stopped waiting".to_owned()))
        .await
        .expect("send cancellation");
    backend.wait_for_counter(&backend.waits_cancelled, 1).await;

    let post = client
        .call_tool(tool_params(
            "workspace_post",
            json!({ "content": "permit released" }),
        ))
        .await
        .expect("call after cancellation");
    assert_eq!(post.structured_content, Some(json!({ "sequence": 1 })));

    stop_official_client(client, server_task).await;
}

#[tokio::test]
async fn official_client_observes_fail_fast_saturation_at_32_calls() {
    let backend = Arc::new(StatefulBackend::default());
    let (client, server_task) =
        start_official_client(mcp::McpRole::CodexCli, Arc::clone(&backend)).await;
    let mut handles = Vec::with_capacity(mcp::MCP_MAX_CONCURRENT_CALLS);
    for _ in 0..mcp::MCP_MAX_CONCURRENT_CALLS {
        handles.push(
            client
                .peer()
                .send_request_with_option(wait_request(), PeerRequestOptions::no_options())
                .await
                .expect("start admitted wait"),
        );
    }
    backend
        .wait_for_counter(&backend.waits_started, mcp::MCP_MAX_CONCURRENT_CALLS)
        .await;

    let busy = tokio::time::timeout(
        Duration::from_millis(200),
        client.call_tool(tool_params("agent_wait", json!({ "waitMs": 60_000 }))),
    )
    .await
    .expect("saturated call must fail immediately")
    .expect("busy is a tool result");
    assert_eq!(
        busy.structured_content,
        Some(json!({ "ok": false, "error": "server_busy" }))
    );

    for handle in handles {
        handle.cancel(None).await.expect("cancel admitted wait");
    }
    backend
        .wait_for_counter(&backend.waits_cancelled, mcp::MCP_MAX_CONCURRENT_CALLS)
        .await;
    stop_official_client(client, server_task).await;
}

#[derive(Clone)]
struct RecordingClient {
    notifications: mpsc::UnboundedSender<CustomNotification>,
}

impl ClientHandler for RecordingClient {
    fn on_custom_notification(
        &self,
        notification: CustomNotification,
        _context: NotificationContext<RoleClient>,
    ) -> impl Future<Output = ()> + Send {
        let _ = self.notifications.send(notification);
        std::future::ready(())
    }
}

async fn start_recording_client(
    role: mcp::McpRole,
    backend: Arc<StatefulBackend>,
) -> (
    RunningService<RoleClient, RecordingClient>,
    mpsc::UnboundedReceiver<CustomNotification>,
    tokio::task::JoinHandle<Result<(), mcp::McpRuntimeError>>,
) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (client_reader, client_writer) = tokio::io::split(client_io);
    let (server_reader, server_writer) = tokio::io::split(server_io);
    let server_task = tokio::spawn(mcp::serve_io(
        mcp::McpServer::new(backend, role, "local:test"),
        server_reader,
        server_writer,
    ));
    let (sender, receiver) = mpsc::unbounded_channel();
    let client = RecordingClient {
        notifications: sender,
    }
    .serve((client_reader, client_writer))
    .await
    .expect("initialize recording client");
    client
        .list_tools(None)
        .await
        .expect("activate backend connection hook");
    (client, receiver, server_task)
}

#[tokio::test]
async fn omp_notifications_are_validated_hooked_and_role_scoped() {
    let backend = Arc::new(StatefulBackend::default());
    let (client, mut outbound, server_task) =
        start_recording_client(mcp::McpRole::Omp, Arc::clone(&backend)).await;

    client
        .send_notification(ClientNotification::CustomNotification(
            CustomNotification::new(
                "notifications/agent_session_router/host_state",
                Some(json!({ "v": 1, "ready": true })),
            ),
        ))
        .await
        .expect("send valid host state");
    backend.wait_for_omp_count(1).await;
    assert_eq!(
        backend.omp_notifications.lock().await.as_slice(),
        &[mcp::OmpHostNotification::HostState(mcp::OmpHostState {
            v: 1,
            ready: true
        })]
    );

    client
        .send_notification(ClientNotification::CustomNotification(
            CustomNotification::new(
                "notifications/agent_session_router/host_state",
                Some(json!({ "v": 2, "ready": false })),
            ),
        ))
        .await
        .expect("send invalid host state");
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(backend.omp_notifications.lock().await.len(), 1);

    let sink = backend.sink().await;
    assert_eq!(
        sink.send_claude(mcp::ClaudeChannelNotification {
            content: "wrong role".to_owned(),
            meta: mcp::ClaudeChannelMeta {
                request_id: "request".to_owned(),
                from: "local:sender".to_owned(),
                timeout_ms: "1000".to_owned(),
            },
        })
        .await,
        Err(mcp::NotificationError::WrongRole)
    );
    sink.send_omp(mcp::OmpServerNotification::Unread {
        workspace: "room".to_owned(),
        cursor: 8,
        count: 2,
    })
    .await
    .expect("send valid OMP notification");
    let notification = tokio::time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .expect("OMP notification deadline")
        .expect("OMP notification channel");
    assert_eq!(
        notification.method,
        "notifications/agent_session_router/unread"
    );
    assert_eq!(
        notification.params,
        Some(json!({ "v": 1, "workspace": "room", "cursor": 8, "count": 2 }))
    );

    client.cancel().await.expect("close OMP client");
    assert_eq!(server_task.await.expect("OMP server task"), Ok(()));
}

#[tokio::test]
async fn claude_channel_sink_emits_only_valid_role_specific_wire_shape() {
    let backend = Arc::new(StatefulBackend::default());
    let (client, mut outbound, server_task) =
        start_recording_client(mcp::McpRole::ClaudeChannel, Arc::clone(&backend)).await;
    let sink = backend.sink().await;
    let channel = mcp::ClaudeChannelNotification {
        content: "review the change".to_owned(),
        meta: mcp::ClaudeChannelMeta {
            request_id: "request-7".to_owned(),
            from: "local:coordinator".to_owned(),
            timeout_ms: "1000".to_owned(),
        },
    };
    sink.send_claude(channel.clone())
        .await
        .expect("send Claude channel event");
    let notification = tokio::time::timeout(Duration::from_secs(1), outbound.recv())
        .await
        .expect("Claude notification deadline")
        .expect("Claude notification channel");
    assert_eq!(notification.method, "notifications/claude/channel");
    assert_eq!(
        notification.params,
        Some(serde_json::to_value(channel).unwrap())
    );
    assert_eq!(
        sink.send_omp(mcp::OmpServerNotification::Unread {
            workspace: "room".to_owned(),
            cursor: 0,
            count: 0,
        })
        .await,
        Err(mcp::NotificationError::WrongRole)
    );

    client.cancel().await.expect("close Claude client");
    assert_eq!(server_task.await.expect("Claude server task"), Ok(()));
}

async fn rejected_frame(frame: Vec<u8>) -> (mcp::McpRuntimeError, Vec<u8>, usize) {
    let backend = Arc::new(StatefulBackend::default());
    let (server_io, mut client_io) = tokio::io::duplex(frame.len().max(4096) + 1);
    let (reader, writer) = tokio::io::split(server_io);
    let server_task = tokio::spawn(mcp::serve_io(
        mcp::McpServer::new(Arc::clone(&backend), mcp::McpRole::Delegate, "local:test"),
        reader,
        writer,
    ));
    client_io.write_all(&frame).await.unwrap();
    client_io.shutdown().await.unwrap();
    let mut output = Vec::new();
    client_io.read_to_end(&mut output).await.unwrap();
    let error = server_task.await.unwrap().unwrap_err();
    (error, output, backend.closes.load(Ordering::Acquire))
}

#[tokio::test]
async fn full_server_rejects_malformed_secret_without_output_and_closes_backend() {
    let sentinel = "MALFORMED_SERVER_SECRET_83ef";
    let (error, output, closes) =
        rejected_frame(format!("{{not-json:{sentinel}}}\n").into_bytes()).await;
    assert_eq!(error, mcp::McpRuntimeError::InvalidInput);
    assert!(output.is_empty());
    assert_eq!(closes, 1);
    assert!(!format!("{error:?} {error}").contains(sentinel));
}

#[tokio::test]
async fn full_server_rejects_overlong_input_and_closes_backend() {
    let frame = vec![b'x'; mcp::MCP_MAX_FRAME_BYTES + 1];
    let (error, output, closes) = rejected_frame(frame).await;
    assert_eq!(error, mcp::McpRuntimeError::InvalidInput);
    assert!(output.is_empty());
    assert_eq!(closes, 1);
}

struct StalledWriter;

impl AsyncWrite for StalledWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Poll::Pending
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Pending
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn full_server_times_out_stalled_stdout_and_closes_backend() {
    let backend = Arc::new(StatefulBackend::default());
    let (reader, mut feeder) = tokio::io::duplex(4096);
    let initialize = concat!(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{",
        "\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},",
        "\"clientInfo\":{\"name\":\"stall-test\",\"version\":\"1\"}}}\n"
    );
    let feed_task = tokio::spawn(async move {
        feeder.write_all(initialize.as_bytes()).await.unwrap();
        feeder.shutdown().await.unwrap();
    });
    let result = mcp::serve_io_with_timeout(
        mcp::McpServer::new(Arc::clone(&backend), mcp::McpRole::Delegate, "local:test"),
        reader,
        StalledWriter,
        Duration::from_millis(20),
    )
    .await;
    feed_task.await.unwrap();
    assert_eq!(result, Err(mcp::McpRuntimeError::OutputTimeout));
    assert_eq!(backend.closes.load(Ordering::Acquire), 1);
}

#[test]
#[ignore = "child-process endpoint for malformed frame privacy test"]
fn malformed_frame_child_process_endpoint() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let backend = Arc::new(StatefulBackend::default());
        assert_eq!(
            mcp::serve_stdio(mcp::McpServer::new(
                backend,
                mcp::McpRole::Delegate,
                "local:child"
            ))
            .await,
            Err(mcp::McpRuntimeError::InvalidInput)
        );
    });
}

#[test]
fn rust_log_trace_child_does_not_emit_malformed_secret_or_protocol_output() {
    let sentinel = "TRACE_CHILD_SECRET_7d35";
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "malformed_frame_child_process_endpoint",
            "--nocapture",
        ])
        .env("RUST_LOG", "trace")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(format!("{{not-json:{sentinel}}}\n").as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stdout.contains(sentinel));
    assert!(!stderr.contains(sentinel));
    assert!(!stdout.contains("\"jsonrpc\""));
    assert!(!stderr.contains("\"jsonrpc\""));
}
