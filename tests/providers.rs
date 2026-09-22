use std::{
    ffi::{OsString, OsString as StdOsString},
    fs,
    os::unix::{
        ffi::OsStringExt as _,
        fs::{PermissionsExt as _, symlink},
    },
    path::{Path, PathBuf},
    process::Command as StdCommand,
    sync::Arc,
    time::Duration,
};

use agent_session_router::{client, config, credentials, protocol, tasks};
use client::{ClientConfig, ClientRole, RouterClient};
use config::{DELEGATE_CONTEXT_VERSION, DelegateLaunchContext};
use credentials::{CredentialFile, CredentialRole};
use futures_util::{SinkExt as _, StreamExt as _};
use protocol::{
    AgentClient, AgentDescriptor, AgentRegistration, AgentSide, AgentStatus, ClientMessage,
    DeliveryMode, PROTOCOL_VERSION, RegistrationRole, ServerMessage, TaskDispatch,
    TaskExecutionEvidence, TaskFence, WorkspaceName, parse_client_message,
};
use serde_json::Value;
use tasks::PauseReason;
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use url::Url;
use uuid::Uuid;

#[allow(dead_code)]
#[path = "../src/install.rs"]
mod install;

#[allow(dead_code, clippy::too_many_arguments)]
#[path = "../src/providers/mod.rs"]
mod providers;

use install::{AssetError, resolve_integration_asset};
use providers::claude::{ClaudeConfig, ClaudeProvider};
use providers::codex::{CodexConfig, CodexProvider, ThreadSelection};
use providers::inbox::{InboxPhase, ProviderInbox};
use providers::router::{RouterClientLifecycle, RouterManagedProvider};
use providers::{
    CancelReason, DelegateContextFile, ManagedLaunch, ProviderError, SessionRequest, SessionResult,
    TerminalEvidence, TerminalReason, filtered_provider_environment, managed_path_utf8,
    validate_delegate_context,
};

const CODEX_FAKE: &str = r"
const fs = require('fs');
const readline = require('readline');
const mode = process.argv[2];
const record = process.argv[3];
let approvals = [];
let pendingTurn = null;
function line(value) { return Buffer.from(JSON.stringify(value) + '\n'); }
function send(value, fragmented = false, newline = true) {
  const payload = Buffer.from(JSON.stringify(value) + (newline ? '\n' : ''));
  if (!fragmented) return process.stdout.write(payload);
  process.stdout.write(payload.subarray(0, 2));
  setTimeout(() => process.stdout.write(payload.subarray(2)), 2);
}
function terminal(turnId, status = 'completed') {
  return {method:'turn/completed', params:{threadId:'thread-1', turn:{id:turnId,status}}};
}
function complete(turnId) {
  const values = [
    {method:'item/completed',params:{threadId:'old-thread',turnId,item:{type:'agentMessage',text:'wrong',phase:'final_answer'}}},
    {method:'item/completed',params:{threadId:'thread-1',turnId:'old-turn',item:{type:'agentMessage',text:'wrong',phase:'final_answer'}}},
    {method:'item/completed',params:{threadId:'thread-1',turnId,item:{type:'agentMessage',text:'fallback'}}},
    {method:'item/completed',params:{threadId:'thread-1',turnId,item:{type:'agentMessage',text:'commentary',phase:'commentary'}}},
    {method:'item/completed',params:{threadId:'thread-1',turnId,item:{type:'agentMessage',text:'one',phase:'final_answer'}}},
    {method:'item/completed',params:{threadId:'thread-1',turnId,item:{type:'agentMessage',text:'two',phase:'final_answer'}}},
    terminal(turnId),
  ];
  process.stdout.write(Buffer.concat(values.map(line)));
}
function recordInit(message) {
  if (!record) return;
  fs.writeFileSync(record, JSON.stringify({
    argv: process.argv.slice(2),
    env: {
      HOME: process.env.HOME,
      OPENAI_API_KEY: process.env.OPENAI_API_KEY,
      ROUTER_TOKEN: process.env.ROUTER_TOKEN,
      ASR_INTEGRATIONS_DIR: process.env.ASR_INTEGRATIONS_DIR,
    },
    initialize: message,
  }));
}
const rl = readline.createInterface({input: process.stdin, crlfDelay: Infinity});
rl.on('line', (raw) => {
  const message = JSON.parse(raw);
  if (message.method === 'initialize') {
    recordInit(message);
    if (mode === 'bad-utf8') return process.stdout.write(Buffer.from([0xff, 0x0a]));
    if (mode === 'oversize') return process.stdout.write(Buffer.alloc(1024 * 1024 + 1, 0x61));
    if (mode === 'final-eof') {
      send({id:message.id,result:{}}, false, false);
      return setTimeout(() => process.exit(0), 5);
    }
    return send({id:message.id,result:{}}, mode === 'fragment');
  }
  if (message.method === 'initialized') return;
  if (message.method === 'thread/start' || message.method === 'thread/resume') {
    if (record) fs.appendFileSync(record, '\n' + JSON.stringify({thread: message}));
    return send({id:message.id,result:{thread:{id:'thread-1'}}}, mode === 'fragment');
  }
  if (message.method === 'turn/start') {
    const turnId = 'turn-' + message.id;
    if (mode === 'early') {
      send({method:'item/completed',params:{threadId:'thread-1',turnId,item:{type:'agentMessage',text:'early',phase:'final_answer'}}});
      send(terminal(turnId));
      return send({id:message.id,result:{turn:{id:turnId}}});
    }
    if (mode === 'early-overflow') {
      for (let i = 0; i < 257; i += 1) {
        send({method:'item/completed',params:{threadId:'thread-1',turnId,item:{type:'agentMessage',text:'x'}}});
      }
      return;
    }
    if (mode === 'approvals') {
      pendingTurn = {rpcId:message.id, turnId};
      send({id:'command',method:'item/commandExecution/requestApproval',params:{}});
      send({id:'file',method:'item/fileChange/requestApproval',params:{}});
      send({id:'permissions',method:'item/permissions/requestApproval',params:{}});
      send({id:'elicitation',method:'mcpServer/elicitation/request',params:{}});
      return send({id:'unknown',method:'new/request',params:{}});
    }
    if (mode === 'timeout') {
      return setTimeout(() => send({id:message.id,result:{turn:{id:turnId}}}), 140);
    }
    send({id:message.id,result:{turn:{id:turnId}}});
    if (mode === 'cancel-late') {
      pendingTurn = {rpcId:message.id, turnId};
      return;
    }
    if (mode === 'fallback') {
      send({method:'item/completed',params:{threadId:'thread-1',turnId,item:{type:'agentMessage',text:'fallback-only'}}});
      return send(terminal(turnId));
    }
    if (mode === 'no-final') return send(terminal(turnId));
    if (mode === 'interrupted') return send(terminal(turnId, 'interrupted'));
    if (mode === 'failed') return send(terminal(turnId, 'failed'));
    if (mode === 'unknown-status') return send(terminal(turnId, 'mystery'));
    return complete(turnId);
  }
  if (message.method === 'turn/interrupt') {
    send({id:message.id,result:{}});
    return setTimeout(() => send(terminal(message.params.turnId, 'interrupted')), 120);
  }
  if (!message.method && mode === 'approvals') {
    approvals.push(message);
    if (approvals.length === 5) {
      fs.appendFileSync(record, '\n' + JSON.stringify({approvals}));
      send({id:pendingTurn.rpcId,result:{turn:{id:pendingTurn.turnId}}});
      complete(pendingTurn.turnId);
    }
  }
});
";

const CLAUDE_FAKE: &str = r"
const fs = require('fs');
const mode = process.argv[2];
const record = process.argv[3];
const SESSION_A = '11111111-1111-4111-8111-111111111111';
const SESSION_B = '22222222-2222-4222-8222-222222222222';
let data = Buffer.alloc(0);
let turns = 0;
let hold = null;
function frame(value) {
  const payload = Buffer.from(JSON.stringify(value));
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length);
  return Buffer.concat([header, payload]);
}
function send(value, fragmented = false) {
  const payload = frame(value);
  if (!fragmented) return process.stdout.write(payload);
  process.stdout.write(payload.subarray(0, 3));
  setTimeout(() => process.stdout.write(payload.subarray(3)), 2);
}
function response(id, result) { return {v:1,id,ok:true,result}; }
function failure(id, code, sessionId) {
  return {v:1,id,ok:false,error:{code,...(sessionId ? {sessionId} : {})}};
}
function recordInit(message) {
  if (!record) return;
  fs.writeFileSync(record, JSON.stringify({
    argv: process.argv.slice(2),
    env: {
      HOME: process.env.HOME,
      ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY,
      ROUTER_TOKEN: process.env.ROUTER_TOKEN,
      ASR_INTEGRATIONS_DIR: process.env.ASR_INTEGRATIONS_DIR,
    },
    initialize: message,
  }));
}
function dispatch(message) {
  if (message.method === 'initialize') {
    recordInit(message);
    if (mode === 'bad-length') {
      const header = Buffer.alloc(4); header.writeUInt32BE(1024 * 1024 + 1);
      return process.stdout.write(header);
    }
    if (mode === 'bad-json') {
      const header = Buffer.alloc(4); header.writeUInt32BE(1);
      return process.stdout.write(Buffer.concat([header, Buffer.from([0xff])]));
    }
    if (mode === 'invalid-init-id') return send(response(message.id + 1, {state:'ready'}));
    return send(response(message.id, {
      state:'ready',
      ...(message.params.resumeSessionId ? {sessionId:message.params.resumeSessionId} : {}),
    }), true);
  }
  if (message.method === 'turn') {
    turns += 1;
    if (mode === 'timeout-ignore') {
      hold = setInterval(() => {}, 1000);
      return;
    }
    if (mode === 'eof') return process.exit(0);
    if (mode === 'invalid-id') return send(response(message.id + 99, {content:'bad',sessionId:SESSION_A}));
    if (mode === 'duplicate') {
      const payload = frame(response(message.id, {content:'first',sessionId:SESSION_A}));
      return process.stdout.write(Buffer.concat([payload, payload]));
    }
    if (mode === 'error-once' && turns === 1) {
      return send(failure(message.id, 'error_max_turns', SESSION_A));
    }
    if (mode === 'max-budget') return send(failure(message.id, 'error_max_budget_usd', SESSION_A));
    if (mode === 'structured') return send(failure(message.id, 'error_max_structured_output_retries', SESSION_A));
    if (mode === 'no-result') return send(failure(message.id, 'claude_no_result', SESSION_A));
    if (mode === 'sdk-error') return send(failure(message.id, 'claude_sdk_error', SESSION_A));
    if (mode === 'throw') return send(failure(message.id, 'bridge_protocol_error', SESSION_A));
    if (mode === 'missing-content') return send(response(message.id, {sessionId:SESSION_A}));
    if (mode === 'invalid-session') return send(response(message.id, {content:'bad',sessionId:'not-a-uuid'}));
    return send(response(message.id, {content:'answer-' + turns,sessionId:turns === 1 ? SESSION_A : SESSION_B}));
  }
  if (message.method === 'cancel') {
    return send(response(message.id, {cancelled:true}));
  }
  if (message.method === 'shutdown') {
    send(response(message.id, {state:'closed'}));
    if (mode !== 'timeout-ignore') setTimeout(() => process.exit(0), 5);
  }
}
process.stdin.on('data', (chunk) => {
  data = Buffer.concat([data, chunk]);
  while (data.length >= 4) {
    const length = data.readUInt32BE(0);
    if (data.length < 4 + length) return;
    const payload = data.subarray(4, 4 + length);
    data = data.subarray(4 + length);
    dispatch(JSON.parse(payload.toString('utf8')));
  }
});
";

fn context() -> DelegateLaunchContext {
    DelegateLaunchContext {
        version: DELEGATE_CONTEXT_VERSION,
        router_url: "ws://127.0.0.1:8787/ws".to_owned(),
        owner_id: "owner".to_owned(),
        delegation_token: "x".repeat(32),
        ca_file: None,
    }
}

fn provider_environment() -> Vec<(OsString, OsString)> {
    vec![
        ("HOME".into(), "/safe-home".into()),
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("OPENAI_API_KEY".into(), "openai-secret".into()),
        ("ANTHROPIC_API_KEY".into(), "anthropic-secret".into()),
        ("ROUTER_TOKEN".into(), "router-secret".into()),
        (
            "AGENT_ROUTER_DELEGATION_TOKEN".into(),
            "delegate-secret".into(),
        ),
        (
            "ASR_INTEGRATIONS_DIR".into(),
            "/checkout/integrations".into(),
        ),
        ("DEBUG".into(), "*".into()),
    ]
}

fn managed_launch(root: &TempDir, cwd: &TempDir) -> ManagedLaunch {
    ManagedLaunch::new_in(
        std::env::current_exe().unwrap(),
        cwd.path().to_owned(),
        &context(),
        provider_environment(),
        root.path(),
    )
    .unwrap()
}

fn node_executable() -> PathBuf {
    let output = StdCommand::new("which").arg("node").output().unwrap();
    assert!(output.status.success());
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
}

fn write_script(directory: &TempDir, name: &str, source: &str) -> PathBuf {
    let path = directory.path().join(name);
    fs::write(&path, source).unwrap();
    path
}

fn request(id: &str, timeout_after: Duration) -> SessionRequest {
    SessionRequest {
        request_id: id.to_owned(),
        workspace: None,
        from: "router".to_owned(),
        content: "prompt".to_owned(),
        deadline: Instant::now() + timeout_after,
        task: None,
    }
}

fn codex_config(
    mode: &str,
    record: &Path,
    root: &TempDir,
    cwd: &TempDir,
    script: &Path,
    thread: ThreadSelection,
) -> CodexConfig {
    let mut config = CodexConfig::managed(
        node_executable(),
        managed_launch(root, cwd),
        thread,
        vec!["agent_list".to_owned(), "agent_send".to_owned()],
    );
    config.executable_arguments = vec![
        script.as_os_str().to_owned(),
        mode.into(),
        record.as_os_str().to_owned(),
    ];
    config.rpc_timeout = Duration::from_secs(3);
    config
}

fn claude_config(
    mode: &str,
    record: &Path,
    root: &TempDir,
    cwd: &TempDir,
    script: &Path,
) -> ClaudeConfig {
    let mut config = ClaudeConfig::with_bridge_asset(
        node_executable(),
        script.to_owned(),
        managed_launch(root, cwd),
    );
    config.bridge_arguments = vec![mode.into(), record.as_os_str().to_owned()];
    config.initialize_timeout = Duration::from_secs(3);
    config.max_turns = 8;
    config
}

async fn recv_router_client<S>(socket: &mut WebSocketStream<S>) -> ClientMessage
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let message = socket
        .next()
        .await
        .expect("router client websocket message")
        .expect("valid router client websocket message");
    let Message::Text(text) = message else {
        panic!("expected router client text message");
    };
    parse_client_message(text.as_str()).expect("valid router client protocol message")
}

async fn send_router_server<S>(socket: &mut WebSocketStream<S>, message: &ServerMessage)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    socket
        .send(Message::Text(
            serde_json::to_string(message)
                .expect("serialize router server message")
                .into(),
        ))
        .await
        .expect("send router server message");
}

async fn register_lifecycle_client<S>(
    socket: &mut WebSocketStream<S>,
    workspace: &WorkspaceName,
    fence: &TaskFence,
    session_id: Uuid,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    assert!(matches!(
        recv_router_client(socket).await,
        ClientMessage::Register { .. }
    ));
    send_router_server(
        socket,
        &ServerMessage::Registered {
            protocol_version: PROTOCOL_VERSION,
            agent: AgentDescriptor {
                agent_id: "provider-worker".to_owned(),
                side: AgentSide::Codex,
                client: AgentClient::CodexAppServer,
                activity: None,
                status: AgentStatus::Idle,
                delivery_mode: DeliveryMode::Pull,
                ready: false,
                session_id,
            },
            role: RegistrationRole::Agent,
            workspace: Some(workspace.clone()),
            cursor: 0,
        },
    )
    .await;
    send_router_server(
        socket,
        &ServerMessage::TaskAttemptChanged {
            workspace: workspace.clone(),
            task_id: fence.task_id,
            attempt: None,
            closed_attempt_id: None,
            current: Some(fence.clone()),
            stop_pending: None,
        },
    )
    .await;
}

async fn connect_provider_client(
    address: std::net::SocketAddr,
    workspace: &WorkspaceName,
) -> (RouterClient, client::ClientEvents) {
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        "provider-worker".to_owned(),
        Some(AgentSide::Codex),
        Some(AgentClient::CodexAppServer),
        vec![workspace.clone()],
    )
    .unwrap();
    RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("ws://{address}")).unwrap(),
        role: ClientRole::Primary {
            agent: AgentRegistration {
                agent_id: "provider-worker".to_owned(),
                side: AgentSide::Codex,
                client: AgentClient::CodexAppServer,
                activity: None,
                delivery_mode: DeliveryMode::Pull,
            },
            credential,
            delegation_token: None,
        },
        ca_file: None,
    })
    .await
    .unwrap()
}

async fn run_lifecycle_router(
    listener: tokio::net::TcpListener,
    workspace: WorkspaceName,
    fence: TaskFence,
    session_id: Uuid,
    stopped: tokio::sync::oneshot::Sender<Instant>,
    idle: tokio::sync::oneshot::Sender<()>,
) {
    let (stream, _) = listener.accept().await.expect("provider router connection");
    let mut socket = accept_async(stream)
        .await
        .expect("provider router websocket");
    register_lifecycle_client(&mut socket, &workspace, &fence, session_id).await;

    let mut saw_ready = false;
    let mut saw_busy = false;
    let mut stopped = Some(stopped);
    loop {
        match recv_router_client(&mut socket).await {
            ClientMessage::Readiness { ready } => {
                saw_ready |= ready;
                saw_busy |= !ready;
            }
            ClientMessage::Ping { request_id } => {
                send_router_server(&mut socket, &ServerMessage::Pong { request_id }).await;
            }
            ClientMessage::TaskExecutionStopped {
                request_id,
                workspace: stopped_workspace,
                task_id,
                attempt_id,
                ended_session_id,
                evidence,
                reason,
            } => {
                assert_eq!(stopped_workspace, workspace);
                assert_eq!(task_id, fence.task_id);
                assert_eq!(attempt_id, fence.attempt_id);
                assert_eq!(ended_session_id, session_id);
                assert_eq!(evidence, TaskExecutionEvidence::ProviderTerminal);
                assert_eq!(reason, PauseReason::OperatorInterrupt);
                stopped
                    .take()
                    .expect("one task stop")
                    .send(Instant::now())
                    .expect("report task stop");
                send_router_server(
                    &mut socket,
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
                ready,
            } => {
                assert_eq!(idle_workspace, workspace);
                assert_eq!(work_request_id, "routed-cancel");
                assert_eq!(task, Some(fence.clone()));
                assert!(ready);
                send_router_server(
                    &mut socket,
                    &ServerMessage::WorkIdleAck {
                        request_id,
                        workspace: workspace.clone(),
                        work_request_id,
                    },
                )
                .await;
                idle.send(()).expect("report work idle");
                break;
            }
            _ => panic!("unexpected provider router message"),
        }
    }
    assert!(saw_ready);
    assert!(saw_busy);
    assert!(matches!(
        recv_router_client(&mut socket).await,
        ClientMessage::Readiness { ready: false }
    ));
    let _ = socket.next().await;
}

#[test]
fn delegate_context_uses_private_child_under_system_temp_and_rejects_unsafe_files() {
    let file = DelegateContextFile::create(&context()).unwrap();
    let path = file.path().to_owned();
    assert_eq!(
        fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(validate_delegate_context(&path).unwrap().owner_id, "owner");

    let link = path.parent().unwrap().join("context-link.json");
    symlink(&path, &link).unwrap();
    assert!(matches!(
        validate_delegate_context(&link),
        Err(ProviderError::InvalidLaunchContext)
    ));
    fs::remove_file(&link).unwrap();

    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(
        validate_delegate_context(&path),
        Err(ProviderError::InvalidLaunchContext)
    ));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(
        &path,
        format!(
            "{{\"version\":1,\"routerUrl\":\"ws://127.0.0.1/ws\",\"ownerId\":\"owner\",\"delegationToken\":\"{}\",\"unknown\":true}}",
            "x".repeat(32)
        ),
    )
    .unwrap();
    assert!(matches!(
        validate_delegate_context(&path),
        Err(ProviderError::InvalidLaunchContext)
    ));
    fs::write(&path, vec![b'x'; 64 * 1024 + 1]).unwrap();
    assert!(matches!(
        validate_delegate_context(&path),
        Err(ProviderError::InvalidLaunchContext)
    ));

    drop(file);
    assert!(!path.exists());
    assert!(!path.parent().unwrap().exists());
}

#[test]
fn managed_launch_rejects_non_utf8_paths_and_filters_sensitive_environment() {
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let invalid = root
        .path()
        .join(StdOsString::from_vec(vec![b'a', b's', b'r', 0xff]));
    assert!(matches!(
        managed_path_utf8(&invalid),
        Err(ProviderError::InvalidLaunchContext)
    ));
    let result = ManagedLaunch::new_in(
        invalid,
        cwd.path().to_owned(),
        &context(),
        provider_environment(),
        root.path(),
    );
    assert!(matches!(result, Err(ProviderError::InvalidLaunchContext)));

    let filtered = filtered_provider_environment(provider_environment());
    assert!(
        filtered
            .iter()
            .any(|(key, value)| key == "HOME" && value == "/safe-home")
    );
    assert!(filtered.iter().any(|(key, _)| key == "OPENAI_API_KEY"));
    assert!(filtered.iter().any(|(key, _)| key == "ANTHROPIC_API_KEY"));
    assert!(!filtered.iter().any(|(key, _)| key == "ROUTER_TOKEN"));
    assert!(
        !filtered
            .iter()
            .any(|(key, _)| key == "AGENT_ROUTER_DELEGATION_TOKEN")
    );
    assert!(
        !filtered
            .iter()
            .any(|(key, _)| key == "ASR_INTEGRATIONS_DIR")
    );
}

#[test]
fn packaged_provider_assets_use_only_configured_or_installed_roots() {
    let temporary = tempfile::tempdir().unwrap();
    let package_root = temporary.path().join("package");
    let current = package_root.join("bin/asr");
    fs::create_dir_all(current.parent().unwrap()).unwrap();
    fs::write(&current, "binary").unwrap();
    let installed =
        package_root.join("share/agent-session-router/integrations/claude-sdk/bridge.js");
    fs::create_dir_all(installed.parent().unwrap()).unwrap();
    fs::write(&installed, "installed bridge").unwrap();

    assert_eq!(
        resolve_integration_asset(&[], &current, Path::new("claude-sdk/bridge.js")).unwrap(),
        installed
    );

    let configured_root = temporary.path().join("configured");
    let configured = configured_root.join("claude-sdk/bridge.js");
    fs::create_dir_all(configured.parent().unwrap()).unwrap();
    fs::write(&configured, "configured bridge").unwrap();
    let configured_environment = vec![(
        OsString::from("ASR_INTEGRATIONS_DIR"),
        configured_root.as_os_str().to_owned(),
    )];
    assert_eq!(
        resolve_integration_asset(
            &configured_environment,
            &current,
            Path::new("claude-sdk/bridge.js")
        )
        .unwrap(),
        configured
    );

    let launch = ManagedLaunch::new_in(
        current.clone(),
        temporary.path().to_owned(),
        &context(),
        configured_environment.clone(),
        temporary.path(),
    )
    .unwrap();
    let managed_claude = ClaudeConfig::managed(node_executable(), launch).unwrap();
    assert_eq!(managed_claude.bridge_asset, configured);

    let missing_environment = vec![(
        OsString::from("ASR_INTEGRATIONS_DIR"),
        temporary.path().join("missing").into_os_string(),
    )];
    assert!(matches!(
        resolve_integration_asset(
            &missing_environment,
            &current,
            Path::new("claude-sdk/bridge.js")
        ),
        Err(AssetError::Unavailable)
    ));
    assert!(matches!(
        resolve_integration_asset(&configured_environment, &current, Path::new("../escape.js")),
        Err(AssetError::InvalidPath)
    ));

    let checkout_executable = temporary.path().join("checkout/target/debug/asr");
    fs::create_dir_all(checkout_executable.parent().unwrap()).unwrap();
    fs::write(&checkout_executable, "binary").unwrap();
    let checkout_asset = temporary
        .path()
        .join("checkout/integrations/claude-sdk/bridge.js");
    fs::create_dir_all(checkout_asset.parent().unwrap()).unwrap();
    fs::write(checkout_asset, "checkout bridge").unwrap();
    assert!(matches!(
        resolve_integration_asset(&[], &checkout_executable, Path::new("claude-sdk/bridge.js")),
        Err(AssetError::Unavailable)
    ));
}

#[test]
fn inbox_releases_only_matching_membership_terminal_and_task_fence() {
    let inbox = ProviderInbox::new();
    let workspace = protocol::WorkspaceName::parse("provider-tests").unwrap();
    let other_workspace = protocol::WorkspaceName::parse("other-workspace").unwrap();
    let fence = TaskFence {
        task_id: 7,
        attempt_id: Uuid::new_v4(),
    };
    let mut work = request("request-1", Duration::from_secs(1));
    work.workspace = Some(workspace.clone());
    work.task = Some(TaskDispatch {
        id: 7,
        expected_version: 3,
    });
    inbox.begin(&work).unwrap();
    inbox.bind_task_fence("request-1", fence.clone()).unwrap();
    assert_eq!(inbox.phase(), InboxPhase::Running);
    assert!(inbox.result_settled("request-1"));
    assert!(!inbox.result_settled("request-1"));
    assert!(
        inbox
            .request_cancel(
                &other_workspace,
                "request-1",
                Some(&fence),
                CancelReason::TaskInterrupted,
            )
            .is_err()
    );
    assert!(
        inbox
            .request_cancel(&workspace, "request-1", None, CancelReason::TaskInterrupted,)
            .is_err()
    );
    assert!(
        inbox
            .request_cancel(
                &workspace,
                "request-1",
                Some(&fence),
                CancelReason::TaskInterrupted,
            )
            .unwrap()
    );
    assert!(inbox.acknowledge_cancel("request-1"));
    assert_eq!(inbox.phase(), InboxPhase::Cancelling);
    assert!(
        inbox
            .observe_terminal(TerminalEvidence {
                request_id: "old-request".to_owned(),
                reason: TerminalReason::TurnEnded,
                child_reaped: false,
            })
            .is_none()
    );
    assert_eq!(inbox.phase(), InboxPhase::Cancelling);
    let terminal = TerminalEvidence {
        request_id: "request-1".to_owned(),
        reason: TerminalReason::OperatorInterrupt,
        child_reaped: false,
    };
    let action = inbox.observe_terminal(terminal.clone()).unwrap();
    assert_eq!(inbox.phase(), InboxPhase::StopPending);
    assert_eq!(action.workspace, workspace);
    assert_eq!(action.evidence, terminal);
    assert_eq!(action.fence, Some(fence.clone()));
    assert!(action.cancelled);
    assert!(
        inbox
            .begin(&request("request-2", Duration::from_secs(1)))
            .is_err()
    );
    assert!(!inbox.confirm_stopped(&other_workspace, "request-1", Some(&fence)));
    assert!(inbox.confirm_stopped(&workspace, "request-1", Some(&fence)));
    assert_eq!(inbox.phase(), InboxPhase::Idle);
}

#[tokio::test]
async fn codex_real_process_filters_events_and_collects_final_answers() {
    let files = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let script = write_script(&files, "codex.js", CODEX_FAKE);
    let record = files.path().join("record.jsonl");
    let config = codex_config(
        "fragment",
        &record,
        &root,
        &cwd,
        &script,
        ThreadSelection::start(),
    );
    let context_path = config.launch.context_path().to_owned();
    let provider = CodexProvider::launch(config).await.unwrap();
    let mut terminal = provider.terminal_events();
    let result = provider
        .send(request("codex-normal", Duration::from_secs(2)))
        .await;
    assert_eq!(result, SessionResult::success("one\ntwo"));
    assert_eq!(terminal.recv().await.unwrap().request_id, "codex-normal");
    assert!(provider.is_ready());

    let records = fs::read_to_string(&record).unwrap();
    let first: Value = serde_json::from_str(records.lines().next().unwrap()).unwrap();
    assert_eq!(first["env"]["HOME"], "/safe-home");
    assert_eq!(first["env"]["OPENAI_API_KEY"], "openai-secret");
    assert!(first["env"]["ROUTER_TOKEN"].is_null());
    assert!(first["env"]["ASR_INTEGRATIONS_DIR"].is_null());
    let args = first["argv"].as_array().unwrap();
    assert!(args.iter().any(|arg| arg == "app-server"));
    assert!(args.iter().any(|arg| {
        arg.as_str()
            .is_some_and(|arg| arg.contains("enabled_tools=[\"agent_list\",\"agent_send\"]"))
    }));
    assert!(args.iter().any(|arg| {
        arg.as_str()
            .is_some_and(|arg| arg.ends_with("tool_timeout_sec=605"))
    }));
    assert!(args.iter().any(|arg| {
        arg.as_str()
            .is_some_and(|arg| arg.contains("mcp delegate --context-file"))
            || arg
                .as_str()
                .is_some_and(|arg| arg.contains("\"mcp\",\"delegate\",\"--context-file\",\"/"))
    }));
    assert!(!records.contains("router-secret"));
    assert!(!records.contains("delegate-secret"));
    provider.shutdown().await.unwrap();
    assert!(!context_path.exists());
}

#[tokio::test]
async fn codex_buffers_early_terminal_and_uses_resume() {
    let files = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let script = write_script(&files, "codex.js", CODEX_FAKE);
    let record = files.path().join("record.jsonl");
    let provider = CodexProvider::launch(codex_config(
        "early",
        &record,
        &root,
        &cwd,
        &script,
        ThreadSelection::resume("existing-thread"),
    ))
    .await
    .unwrap();
    assert_eq!(
        provider
            .send(request("early", Duration::from_secs(2)))
            .await,
        SessionResult::success("early")
    );
    let records = fs::read_to_string(&record).unwrap();
    let thread: Value = serde_json::from_str(records.lines().nth(1).unwrap()).unwrap();
    assert_eq!(thread["thread"]["method"], "thread/resume");
    assert_eq!(thread["thread"]["params"]["threadId"], "existing-thread");
    provider.shutdown().await.unwrap();
}

#[tokio::test]
async fn codex_declines_all_server_requests_and_unknown_methods() {
    let files = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let script = write_script(&files, "codex.js", CODEX_FAKE);
    let record = files.path().join("record.jsonl");
    let provider = CodexProvider::launch(codex_config(
        "approvals",
        &record,
        &root,
        &cwd,
        &script,
        ThreadSelection::start(),
    ))
    .await
    .unwrap();
    assert_eq!(
        provider
            .send(request("approval", Duration::from_secs(2)))
            .await,
        SessionResult::success("one\ntwo")
    );
    let records = fs::read_to_string(&record).unwrap();
    let approvals: Value = serde_json::from_str(records.lines().nth(2).unwrap()).unwrap();
    let values = approvals["approvals"].as_array().unwrap();
    assert!(
        values
            .iter()
            .any(|value| value["id"] == "command" && value["result"]["decision"] == "decline")
    );
    assert!(
        values
            .iter()
            .any(|value| value["id"] == "file" && value["result"]["decision"] == "decline")
    );
    assert!(values.iter().any(|value| {
        value["id"] == "permissions"
            && value["result"]["permissions"]
                .as_array()
                .unwrap()
                .is_empty()
    }));
    assert!(
        values
            .iter()
            .any(|value| value["id"] == "elicitation" && value["result"]["action"] == "decline")
    );
    assert!(
        values
            .iter()
            .any(|value| value["id"] == "unknown" && value["error"]["code"] == -32601)
    );
    provider.shutdown().await.unwrap();
}

#[tokio::test]
async fn codex_rejects_bad_utf8_oversize_and_unterminated_eof_processes() {
    for mode in ["bad-utf8", "oversize", "final-eof"] {
        let files = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let cwd = tempfile::tempdir().unwrap();
        let script = write_script(&files, "codex.js", CODEX_FAKE);
        let record = files.path().join("record.jsonl");
        let config = codex_config(
            mode,
            &record,
            &root,
            &cwd,
            &script,
            ThreadSelection::start(),
        );
        let context_path = config.launch.context_path().to_owned();
        let result = timeout(Duration::from_secs(2), CodexProvider::launch(config))
            .await
            .unwrap();
        assert!(
            matches!(result, Err(ProviderError::CodexInitializeFailed)),
            "{mode}"
        );
        assert!(!context_path.exists(), "{mode}");
    }
}

#[tokio::test]
async fn codex_maps_terminal_statuses_and_bounds_early_notifications() {
    let cases = [
        ("fallback", SessionResult::success("fallback-only")),
        (
            "no-final",
            SessionResult::failure(ProviderError::CodexNoFinalResponse),
        ),
        (
            "interrupted",
            SessionResult::failure(ProviderError::CodexTurnInterrupted),
        ),
        (
            "failed",
            SessionResult::failure(ProviderError::CodexTurnFailed),
        ),
        (
            "unknown-status",
            SessionResult::failure(ProviderError::CodexProtocolError),
        ),
    ];
    for (mode, expected) in cases {
        let files = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let cwd = tempfile::tempdir().unwrap();
        let script = write_script(&files, "codex.js", CODEX_FAKE);
        let record = files.path().join("record.jsonl");
        let provider = CodexProvider::launch(codex_config(
            mode,
            &record,
            &root,
            &cwd,
            &script,
            ThreadSelection::start(),
        ))
        .await
        .unwrap();
        assert_eq!(
            provider.send(request(mode, Duration::from_secs(2))).await,
            expected,
            "{mode}"
        );
        assert!(provider.is_ready(), "{mode}");
        provider.shutdown().await.unwrap();
    }

    let files = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let script = write_script(&files, "codex.js", CODEX_FAKE);
    let record = files.path().join("record.jsonl");
    let provider = CodexProvider::launch(codex_config(
        "early-overflow",
        &record,
        &root,
        &cwd,
        &script,
        ThreadSelection::start(),
    ))
    .await
    .unwrap();
    assert_eq!(
        provider
            .send(request("overflow", Duration::from_secs(2)))
            .await,
        SessionResult::failure(ProviderError::CodexProtocolError)
    );
    assert!(!provider.is_ready());
}

#[tokio::test]
async fn codex_timeout_waits_for_late_turn_id_and_exact_terminal() {
    let files = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let script = write_script(&files, "codex.js", CODEX_FAKE);
    let record = files.path().join("record.jsonl");
    let provider = CodexProvider::launch(codex_config(
        "timeout",
        &record,
        &root,
        &cwd,
        &script,
        ThreadSelection::start(),
    ))
    .await
    .unwrap();
    let mut terminal = provider.terminal_events();
    assert_eq!(
        provider
            .send(request("late", Duration::from_millis(80)))
            .await,
        SessionResult::failure(ProviderError::RequestTimeout)
    );
    assert!(!provider.is_ready());
    assert_eq!(
        provider
            .send(request("blocked", Duration::from_secs(1)))
            .await,
        SessionResult::failure(ProviderError::SessionBusy)
    );
    assert!(
        timeout(Duration::from_millis(150), terminal.recv())
            .await
            .is_err()
    );
    let evidence = timeout(Duration::from_secs(1), terminal.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evidence.request_id, "late");
    assert!(!evidence.child_reaped);
    assert!(provider.is_ready());
    provider.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn routed_codex_cancel_waits_for_terminal_and_reports_exact_task_stop() {
    let workspace = WorkspaceName::parse("provider-lifecycle").unwrap();
    let session_id = Uuid::new_v4();
    let fence = TaskFence {
        task_id: 23,
        attempt_id: Uuid::new_v4(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stopped_tx, mut stopped_rx) = tokio::sync::oneshot::channel();
    let (idle_tx, idle_rx) = tokio::sync::oneshot::channel();
    let fake_router = tokio::spawn(run_lifecycle_router(
        listener,
        workspace.clone(),
        fence.clone(),
        session_id,
        stopped_tx,
        idle_tx,
    ));
    let (client, _events) = connect_provider_client(address, &workspace).await;

    let files = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let script = write_script(&files, "codex.js", CODEX_FAKE);
    let record = files.path().join("record.jsonl");
    let provider = CodexProvider::launch(codex_config(
        "cancel-late",
        &record,
        &root,
        &cwd,
        &script,
        ThreadSelection::start(),
    ))
    .await
    .unwrap();
    let lifecycle = RouterClientLifecycle::new(client.clone(), workspace.clone());
    let managed_provider = Arc::new(RouterManagedProvider::new(provider, lifecycle));
    managed_provider.activate().await.unwrap();

    let mut routed_request = request("routed-cancel", Duration::from_secs(2));
    routed_request.workspace = Some(workspace.clone());
    routed_request.task = Some(TaskDispatch {
        id: fence.task_id,
        expected_version: 4,
    });
    let handling = {
        let managed_provider = Arc::clone(&managed_provider);
        tokio::spawn(async move { managed_provider.handle(routed_request).await })
    };
    timeout(Duration::from_secs(1), async {
        while managed_provider.ready() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    managed_provider
        .cancel(
            &workspace,
            "routed-cancel",
            Some(&fence),
            CancelReason::TaskInterrupted,
        )
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(60), &mut stopped_rx)
            .await
            .is_err(),
        "interrupt acknowledgement is not terminal evidence"
    );
    assert_eq!(
        handling.await.unwrap(),
        SessionResult::failure(ProviderError::ProviderDisconnected)
    );
    timeout(Duration::from_secs(1), &mut stopped_rx)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(1), idle_rx)
        .await
        .unwrap()
        .unwrap();
    managed_provider.close().await.unwrap();
    client.close().await.unwrap();
    fake_router.await.unwrap();
}

#[tokio::test]
async fn claude_real_process_initializes_warm_session_and_resumes_after_error() {
    let files = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let script = write_script(&files, "claude.js", CLAUDE_FAKE);
    let record = files.path().join("record.json");
    let mut config = claude_config("error-once", &record, &root, &cwd, &script);
    let context_path = config.launch.context_path().to_owned();
    config.resume_session_id = Some("33333333-3333-4333-8333-333333333333".to_owned());
    let provider = ClaudeProvider::launch(config).await.unwrap();
    assert_eq!(
        provider
            .send(request("first", Duration::from_secs(2)))
            .await,
        SessionResult::failure(ProviderError::ClaudeMaxTurns)
    );
    assert_eq!(
        provider.session_id().unwrap().to_string(),
        "11111111-1111-4111-8111-111111111111"
    );
    assert_eq!(
        provider
            .send(request("second", Duration::from_secs(2)))
            .await,
        SessionResult::success("answer-2")
    );
    assert_eq!(
        provider.session_id().unwrap().to_string(),
        "22222222-2222-4222-8222-222222222222"
    );

    let saved: Value = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
    assert_eq!(saved["env"]["HOME"], "/safe-home");
    assert_eq!(saved["env"]["ANTHROPIC_API_KEY"], "anthropic-secret");
    assert!(saved["env"]["ROUTER_TOKEN"].is_null());
    assert!(saved["env"]["ASR_INTEGRATIONS_DIR"].is_null());
    assert_eq!(
        saved["initialize"]["params"]["resumeSessionId"],
        "33333333-3333-4333-8333-333333333333"
    );
    let mcp = &saved["initialize"]["params"]["mcp"];
    assert_eq!(mcp["args"][0], "mcp");
    assert_eq!(mcp["args"][1], "delegate");
    assert_eq!(mcp["args"][2], "--context-file");
    assert_eq!(mcp["args"][3], context_path.to_str().unwrap());
    assert_eq!(mcp["env"].as_object().unwrap().len(), 0);
    assert!(!saved.to_string().contains("router-secret"));
    assert!(!saved.to_string().contains("delegate-secret"));
    assert!(context_path.exists());
    provider.shutdown().await.unwrap();
    assert!(!context_path.exists());
}

#[tokio::test]
async fn claude_maps_stable_result_errors_and_no_result() {
    let cases = [
        ("max-budget", ProviderError::ClaudeMaxBudget),
        ("structured", ProviderError::ClaudeStructuredOutputError),
        ("no-result", ProviderError::ClaudeNoResult),
        ("sdk-error", ProviderError::ClaudeSdkError),
        ("missing-content", ProviderError::ClaudeNoResult),
    ];
    for (mode, expected) in cases {
        let files = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let cwd = tempfile::tempdir().unwrap();
        let script = write_script(&files, "claude.js", CLAUDE_FAKE);
        let record = files.path().join("record.json");
        let provider = ClaudeProvider::launch(claude_config(mode, &record, &root, &cwd, &script))
            .await
            .unwrap();
        assert_eq!(
            provider.send(request(mode, Duration::from_secs(1))).await,
            SessionResult::failure(expected),
            "{mode}"
        );
        assert!(provider.is_ready(), "{mode}");
        provider.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn claude_rejects_bad_frames_ids_duplicates_and_eof() {
    for mode in ["bad-length", "bad-json", "invalid-init-id"] {
        let files = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let cwd = tempfile::tempdir().unwrap();
        let script = write_script(&files, "claude.js", CLAUDE_FAKE);
        let record = files.path().join("record.json");
        let config = claude_config(mode, &record, &root, &cwd, &script);
        let context_path = config.launch.context_path().to_owned();
        let result = ClaudeProvider::launch(config).await;
        assert!(result.is_err(), "{mode}");
        assert!(!context_path.exists(), "{mode}");
    }

    for (mode, expected, reaped) in [
        ("invalid-id", ProviderError::ClaudeProtocolError, true),
        ("invalid-session", ProviderError::ClaudeProtocolError, true),
        ("throw", ProviderError::BridgeProtocolError, false),
        ("eof", ProviderError::ProviderDisconnected, true),
    ] {
        let files = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let cwd = tempfile::tempdir().unwrap();
        let script = write_script(&files, "claude.js", CLAUDE_FAKE);
        let record = files.path().join("record.json");
        let provider = ClaudeProvider::launch(claude_config(mode, &record, &root, &cwd, &script))
            .await
            .unwrap();
        let mut terminal = provider.terminal_events();
        assert_eq!(
            provider.send(request(mode, Duration::from_secs(1))).await,
            SessionResult::failure(expected),
            "{mode}"
        );
        assert!(!provider.is_ready());
        assert_eq!(
            timeout(Duration::from_secs(1), terminal.recv())
                .await
                .unwrap()
                .unwrap()
                .child_reaped,
            reaped,
            "{mode}"
        );
    }

    let files = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let script = write_script(&files, "claude.js", CLAUDE_FAKE);
    let record = files.path().join("record.json");
    let provider =
        ClaudeProvider::launch(claude_config("duplicate", &record, &root, &cwd, &script))
            .await
            .unwrap();
    assert_eq!(
        provider
            .send(request("duplicate", Duration::from_secs(1)))
            .await,
        SessionResult::success("first")
    );
    timeout(Duration::from_secs(1), async {
        while provider.is_ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn claude_timeout_settles_once_then_kills_ignored_close() {
    let files = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let script = write_script(&files, "claude.js", CLAUDE_FAKE);
    let record = files.path().join("record.json");
    let provider = ClaudeProvider::launch(claude_config(
        "timeout-ignore",
        &record,
        &root,
        &cwd,
        &script,
    ))
    .await
    .unwrap();
    let mut terminal = provider.terminal_events();
    assert_eq!(
        provider
            .send(request("timeout", Duration::from_millis(250)))
            .await,
        SessionResult::failure(ProviderError::RequestTimeout)
    );
    assert!(!provider.is_ready());
    assert_eq!(
        provider
            .send(request("blocked", Duration::from_secs(1)))
            .await,
        SessionResult::failure(ProviderError::SessionBusy)
    );
    assert!(
        timeout(Duration::from_millis(200), terminal.recv())
            .await
            .is_err()
    );
    let evidence = timeout(Duration::from_secs(6), terminal.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evidence.request_id, "timeout");
    assert_eq!(evidence.reason, TerminalReason::RequestTimeout);
    assert!(evidence.child_reaped);
    assert!(!provider.is_ready());
}
