use std::{
    ffi::OsString,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt as _;
use serde_json::{Map, Value, json};
use tokio::{
    io::AsyncWriteExt as _,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{broadcast, mpsc, oneshot},
    time::{Instant, MissedTickBehavior, interval, sleep_until, timeout},
};
use tokio_util::codec::{FramedRead, LinesCodec};

use super::{
    CancelReason, MAX_EARLY_NOTIFICATIONS, MAX_PROVIDER_FRAME_BYTES, ManagedLaunch, OwnedProvider,
    PROVIDER_TERMINAL_GRACE, ProviderError, SessionRequest, SessionResult, TerminalEvidence,
    TerminalReason, drain_stderr, terminate_and_reap,
};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const CHILD_POLL_INTERVAL: Duration = Duration::from_millis(25);
const STATE_STARTING: u8 = 0;
const STATE_IDLE: u8 = 1;
const STATE_RUNNING: u8 = 2;
const STATE_CANCELLING: u8 = 3;
const STATE_CLOSED: u8 = 4;

pub enum ThreadSelection {
    Start {
        params: Map<String, Value>,
    },
    Resume {
        thread_id: String,
        params: Map<String, Value>,
    },
}

impl ThreadSelection {
    #[must_use]
    pub fn start() -> Self {
        Self::Start { params: Map::new() }
    }

    #[must_use]
    pub fn resume(thread_id: impl Into<String>) -> Self {
        Self::Resume {
            thread_id: thread_id.into(),
            params: Map::new(),
        }
    }
}

pub struct CodexConfig {
    pub executable: PathBuf,
    pub executable_arguments: Vec<OsString>,
    pub launch: ManagedLaunch,
    pub thread: ThreadSelection,
    pub enabled_tools: Vec<String>,
    pub client_info: Value,
    pub rpc_timeout: Duration,
}

impl CodexConfig {
    #[must_use]
    pub fn managed(
        executable: PathBuf,
        launch: ManagedLaunch,
        thread: ThreadSelection,
        enabled_tools: Vec<String>,
    ) -> Self {
        Self {
            executable,
            executable_arguments: Vec::new(),
            launch,
            thread,
            enabled_tools,
            client_info: json!({
                "name": "agent-session-router",
                "title": "Agent Session Router",
                "version": env!("CARGO_PKG_VERSION"),
            }),
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
        }
    }
}

pub struct CodexProvider {
    commands: mpsc::Sender<ActorCommand>,
    state: Arc<AtomicU8>,
    terminal: broadcast::Sender<TerminalEvidence>,
}

impl CodexProvider {
    pub async fn launch(config: CodexConfig) -> Result<Self, ProviderError> {
        if !config.executable.is_absolute() || config.rpc_timeout.is_zero() {
            return Err(ProviderError::LaunchFailed);
        }
        let mut command = Command::new(&config.executable);
        command.args(&config.executable_arguments).arg("app-server");
        command.args(config.launch.codex_mcp_overrides(&config.enabled_tools)?);
        command.arg("--listen").arg("stdio://");
        config.launch.configure_child(&mut command);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|_| ProviderError::LaunchFailed)?;
        let stdin = child.stdin.take().ok_or(ProviderError::LaunchFailed)?;
        let stdout = child.stdout.take().ok_or(ProviderError::LaunchFailed)?;
        let stderr = child.stderr.take().ok_or(ProviderError::LaunchFailed)?;
        tokio::spawn(drain_stderr(stderr));

        let (commands, command_rx) = mpsc::channel(16);
        let (wire_tx, wire_rx) = mpsc::channel(32);
        tokio::spawn(read_json_lines(stdout, wire_tx));
        let (terminal, _) = broadcast::channel(16);
        let state = Arc::new(AtomicU8::new(STATE_STARTING));
        let (started_tx, started_rx) = oneshot::channel();
        tokio::spawn(run_actor(
            child,
            stdin,
            wire_rx,
            command_rx,
            terminal.clone(),
            Arc::clone(&state),
            config,
            started_tx,
        ));
        match started_rx.await {
            Ok(Ok(())) => Ok(Self {
                commands,
                state,
                terminal,
            }),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(ProviderError::CodexInitializeFailed),
        }
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_IDLE
    }

    pub async fn send(&self, request: SessionRequest) -> SessionResult {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .commands
            .send(ActorCommand::Handle(request, reply_tx))
            .await
            .is_err()
        {
            return SessionResult::failure(ProviderError::ProviderDisconnected);
        }
        reply_rx
            .await
            .unwrap_or_else(|_| SessionResult::failure(ProviderError::ProviderDisconnected))
    }

    pub async fn interrupt(
        &self,
        request_id: String,
        reason: CancelReason,
    ) -> Result<(), ProviderError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(ActorCommand::Cancel {
                request_id,
                reason,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ProviderError::ProviderDisconnected)?;
        reply_rx
            .await
            .unwrap_or(Err(ProviderError::ProviderDisconnected))
    }

    pub async fn shutdown(&self) -> Result<(), ProviderError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(ActorCommand::Close(reply_tx))
            .await
            .map_err(|_| ProviderError::ProviderDisconnected)?;
        reply_rx
            .await
            .unwrap_or(Err(ProviderError::ProviderDisconnected))
    }

    #[must_use]
    pub fn terminal_events(&self) -> broadcast::Receiver<TerminalEvidence> {
        self.terminal.subscribe()
    }
}

impl OwnedProvider for CodexProvider {
    fn ready(&self) -> bool {
        self.is_ready()
    }

    fn handle(
        &self,
        request: SessionRequest,
    ) -> impl std::future::Future<Output = SessionResult> + Send {
        self.send(request)
    }

    fn cancel(
        &self,
        request_id: String,
        reason: CancelReason,
    ) -> impl std::future::Future<Output = Result<(), ProviderError>> + Send {
        self.interrupt(request_id, reason)
    }

    fn close(&self) -> impl std::future::Future<Output = Result<(), ProviderError>> + Send {
        self.shutdown()
    }

    fn subscribe_terminal(&self) -> broadcast::Receiver<TerminalEvidence> {
        self.terminal_events()
    }
}

enum ActorCommand {
    Handle(SessionRequest, oneshot::Sender<SessionResult>),
    Cancel {
        request_id: String,
        reason: CancelReason,
        reply: oneshot::Sender<Result<(), ProviderError>>,
    },
    Close(oneshot::Sender<Result<(), ProviderError>>),
}

enum ActorOutcome {
    Continue,
    Stop {
        error: ProviderError,
        close_reply: Option<oneshot::Sender<Result<(), ProviderError>>>,
    },
}

enum WireEvent {
    Message { value: Value, bytes: usize },
    ProtocolError,
    Eof,
}

struct ActiveTurn {
    request_id: String,
    result: Option<oneshot::Sender<SessionResult>>,
    turn_id: Option<String>,
    start_rpc_id: u64,
    start_rpc_deadline: Instant,
    deadline: Instant,
    cancel_deadline: Option<Instant>,
    interrupt_rpc_id: Option<u64>,
    interrupt_sent: bool,
    final_messages: Vec<String>,
    fallback_message: Option<String>,
    early_events: Vec<Value>,
    early_bytes: usize,
    terminal_reason: TerminalReason,
}

async fn read_json_lines(stdout: ChildStdout, sender: mpsc::Sender<WireEvent>) {
    let mut lines = FramedRead::new(
        stdout,
        LinesCodec::new_with_max_length(MAX_PROVIDER_FRAME_BYTES),
    );
    while let Some(frame) = lines.next().await {
        let Ok(line) = frame else {
            let _ = sender.send(WireEvent::ProtocolError).await;
            return;
        };
        let bytes = line.len();
        if bytes > MAX_PROVIDER_FRAME_BYTES {
            let _ = sender.send(WireEvent::ProtocolError).await;
            return;
        }
        let value = match serde_json::from_str::<Value>(&line) {
            Ok(value) if value.is_object() => value,
            _ => {
                let _ = sender.send(WireEvent::ProtocolError).await;
                return;
            }
        };
        if sender
            .send(WireEvent::Message { value, bytes })
            .await
            .is_err()
        {
            return;
        }
    }
    let _ = sender.send(WireEvent::Eof).await;
}

async fn run_actor(
    mut child: Child,
    mut stdin: ChildStdin,
    mut wire: mpsc::Receiver<WireEvent>,
    mut commands: mpsc::Receiver<ActorCommand>,
    terminal: broadcast::Sender<TerminalEvidence>,
    state: Arc<AtomicU8>,
    config: CodexConfig,
    started: oneshot::Sender<Result<(), ProviderError>>,
) {
    let initialized = initialize(
        &mut child,
        &mut stdin,
        &mut wire,
        config.client_info,
        config.thread,
        config.rpc_timeout,
    )
    .await;
    let Ok(thread_id) = initialized else {
        let _ = terminate_and_reap(&mut child).await;
        state.store(STATE_CLOSED, Ordering::Release);
        let _ = started.send(Err(ProviderError::CodexInitializeFailed));
        return;
    };
    state.store(STATE_IDLE, Ordering::Release);
    let _ = started.send(Ok(()));

    let (fatal_error, close_reply, active) = run_actor_loop(
        &mut child,
        &mut stdin,
        &mut wire,
        &mut commands,
        &terminal,
        &state,
        &thread_id,
        config.rpc_timeout,
    )
    .await;

    finish_actor(
        child,
        stdin,
        active,
        &terminal,
        &state,
        fatal_error,
        close_reply,
    )
    .await;
}

async fn run_actor_loop(
    child: &mut Child,
    stdin: &mut ChildStdin,
    wire: &mut mpsc::Receiver<WireEvent>,
    commands: &mut mpsc::Receiver<ActorCommand>,
    terminal: &broadcast::Sender<TerminalEvidence>,
    state: &AtomicU8,
    thread_id: &str,
    rpc_timeout: Duration,
) -> (
    ProviderError,
    Option<oneshot::Sender<Result<(), ProviderError>>>,
    Option<ActiveTurn>,
) {
    let mut next_rpc_id = 3_u64;
    let mut active: Option<ActiveTurn> = None;
    let mut poll = interval(CHILD_POLL_INTERVAL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let fatal_error;
    let mut close_reply = None;
    loop {
        let (far, request_deadline, start_deadline, cancel_deadline) =
            actor_deadlines(active.as_ref());
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    fatal_error = ProviderError::ProviderDisconnected;
                    break;
                };
                match handle_actor_command(
                    command,
                    stdin,
                    thread_id,
                    &mut active,
                    state,
                    &mut next_rpc_id,
                    rpc_timeout,
                ).await {
                    ActorOutcome::Continue => {}
                    ActorOutcome::Stop { error, close_reply: reply } => {
                        fatal_error = error;
                        close_reply = reply;
                        break;
                    }
                }
            }
            event = wire.recv() => {
                if let Err(error) = handle_wire_event(
                    event,
                    stdin,
                    thread_id,
                    &mut active,
                    terminal,
                    state,
                    &mut next_rpc_id,
                ).await {
                    fatal_error = error;
                    break;
                }
            }
            () = sleep_until(request_deadline), if request_deadline != far => {
                if handle_request_timeout(
                    stdin,
                    thread_id,
                    &mut active,
                    state,
                    &mut next_rpc_id,
                ).await.is_err() {
                    fatal_error = ProviderError::ProviderDisconnected;
                    break;
                }
            }
            () = sleep_until(start_deadline), if start_deadline != far => {
                fatal_error = ProviderError::CodexProtocolError;
                break;
            }
            () = sleep_until(cancel_deadline), if cancel_deadline != far => {
                fatal_error = ProviderError::RequestTimeout;
                break;
            }
            _ = poll.tick() => {
                match child.try_wait() {
                    Ok(Some(_)) | Err(_) => {
                        fatal_error = ProviderError::ProviderDisconnected;
                        break;
                    }
                    Ok(None) => {}
                }
            }
        }
    }
    (fatal_error, close_reply, active)
}

fn actor_deadlines(active: Option<&ActiveTurn>) -> (Instant, Instant, Instant, Instant) {
    let far = Instant::now() + Duration::from_hours(8760);
    let request = active
        .filter(|turn| turn.cancel_deadline.is_none())
        .map_or(far, |turn| turn.deadline);
    let start = active
        .filter(|turn| turn.turn_id.is_none() && turn.cancel_deadline.is_none())
        .map_or(far, |turn| turn.start_rpc_deadline);
    let cancel = active.and_then(|turn| turn.cancel_deadline).unwrap_or(far);
    (far, request, start, cancel)
}
async fn handle_actor_command(
    command: ActorCommand,
    stdin: &mut ChildStdin,
    thread_id: &str,
    active: &mut Option<ActiveTurn>,
    state: &AtomicU8,
    next_rpc_id: &mut u64,
    rpc_timeout: Duration,
) -> ActorOutcome {
    match command {
        ActorCommand::Handle(request, reply) => {
            if active.is_some() {
                let _ = reply.send(SessionResult::failure(ProviderError::SessionBusy));
                return ActorOutcome::Continue;
            }
            if state.load(Ordering::Acquire) == STATE_CLOSED {
                let _ = reply.send(SessionResult::failure(ProviderError::ProviderNotReady));
                return ActorOutcome::Continue;
            }
            if request.deadline <= Instant::now() {
                let _ = reply.send(SessionResult::failure(ProviderError::RequestTimeout));
                return ActorOutcome::Continue;
            }
            let outcome = start_actor_turn(
                request,
                reply,
                stdin,
                thread_id,
                active,
                next_rpc_id,
                rpc_timeout,
            )
            .await;
            if matches!(&outcome, ActorOutcome::Continue) {
                state.store(STATE_RUNNING, Ordering::Release);
            }
            outcome
        }
        ActorCommand::Cancel {
            request_id,
            reason,
            reply,
        } => {
            let Some(turn) = active.as_mut() else {
                let _ = reply.send(Err(ProviderError::ProviderNotReady));
                return ActorOutcome::Continue;
            };
            if turn.request_id != request_id {
                let _ = reply.send(Err(ProviderError::ProviderNotReady));
                return ActorOutcome::Continue;
            }
            if turn.cancel_deadline.is_none() {
                let error = match reason {
                    CancelReason::RequestTimeout => ProviderError::RequestTimeout,
                    CancelReason::ProviderDisconnected
                    | CancelReason::TaskInterrupted
                    | CancelReason::RequestCancelled => ProviderError::ProviderDisconnected,
                };
                settle_result(turn, SessionResult::failure(error));
                turn.terminal_reason = cancel_terminal_reason(reason);
                turn.cancel_deadline = Some(Instant::now() + PROVIDER_TERMINAL_GRACE);
                state.store(STATE_CANCELLING, Ordering::Release);
                if let Some(turn_id) = turn.turn_id.clone() {
                    let rpc_id = *next_rpc_id;
                    *next_rpc_id += 1;
                    if send_interrupt(stdin, rpc_id, thread_id, &turn_id)
                        .await
                        .is_err()
                    {
                        let _ = reply.send(Err(ProviderError::ProviderDisconnected));
                        return ActorOutcome::Stop {
                            error: ProviderError::ProviderDisconnected,
                            close_reply: None,
                        };
                    }
                    turn.interrupt_rpc_id = Some(rpc_id);
                    turn.interrupt_sent = true;
                }
            }
            let _ = reply.send(Ok(()));
            ActorOutcome::Continue
        }
        ActorCommand::Close(reply) => ActorOutcome::Stop {
            error: ProviderError::ProviderDisconnected,
            close_reply: Some(reply),
        },
    }
}

async fn start_actor_turn(
    request: SessionRequest,
    reply: oneshot::Sender<SessionResult>,
    stdin: &mut ChildStdin,
    thread_id: &str,
    active: &mut Option<ActiveTurn>,
    next_rpc_id: &mut u64,
    rpc_timeout: Duration,
) -> ActorOutcome {
    let rpc_id = *next_rpc_id;
    *next_rpc_id += 1;
    let start_rpc_deadline = Instant::now() + rpc_timeout;
    let message = json!({
        "id": rpc_id,
        "method": "turn/start",
        "params": {
            "threadId": thread_id,
            "input": [{"type": "text", "text": request.content}],
        },
    });
    if write_json_line(stdin, &message).await.is_err() {
        let _ = reply.send(SessionResult::failure(ProviderError::ProviderDisconnected));
        return ActorOutcome::Stop {
            error: ProviderError::ProviderDisconnected,
            close_reply: None,
        };
    }
    *active = Some(ActiveTurn {
        request_id: request.request_id,
        result: Some(reply),
        turn_id: None,
        start_rpc_id: rpc_id,
        start_rpc_deadline,
        deadline: request.deadline,
        cancel_deadline: None,
        interrupt_rpc_id: None,
        interrupt_sent: false,
        final_messages: Vec::new(),
        fallback_message: None,
        early_events: Vec::new(),
        early_bytes: 0,
        terminal_reason: TerminalReason::TurnEnded,
    });
    ActorOutcome::Continue
}

async fn handle_wire_event(
    event: Option<WireEvent>,
    stdin: &mut ChildStdin,
    thread_id: &str,
    active: &mut Option<ActiveTurn>,
    terminal: &broadcast::Sender<TerminalEvidence>,
    state: &AtomicU8,
    next_rpc_id: &mut u64,
) -> Result<(), ProviderError> {
    match event {
        Some(WireEvent::Message { value, bytes }) => {
            process_message(
                stdin,
                thread_id,
                active,
                terminal,
                state,
                next_rpc_id,
                value,
                bytes,
            )
            .await
        }
        Some(WireEvent::ProtocolError) => Err(ProviderError::CodexProtocolError),
        Some(WireEvent::Eof) | None => Err(ProviderError::ProviderDisconnected),
    }
}

async fn handle_request_timeout(
    stdin: &mut ChildStdin,
    thread_id: &str,
    active: &mut Option<ActiveTurn>,
    state: &AtomicU8,
    next_rpc_id: &mut u64,
) -> Result<(), ProviderError> {
    let turn = active.as_mut().expect("active turn has request deadline");
    settle_result(turn, SessionResult::failure(ProviderError::RequestTimeout));
    turn.terminal_reason = TerminalReason::RequestTimeout;
    turn.cancel_deadline = Some(Instant::now() + PROVIDER_TERMINAL_GRACE);
    state.store(STATE_CANCELLING, Ordering::Release);
    if let Some(turn_id) = turn.turn_id.clone() {
        let rpc_id = *next_rpc_id;
        *next_rpc_id += 1;
        send_interrupt(stdin, rpc_id, thread_id, &turn_id).await?;
        turn.interrupt_rpc_id = Some(rpc_id);
        turn.interrupt_sent = true;
    }
    Ok(())
}

async fn finish_actor(
    mut child: Child,
    stdin: ChildStdin,
    active: Option<ActiveTurn>,
    terminal: &broadcast::Sender<TerminalEvidence>,
    state: &AtomicU8,
    fatal_error: ProviderError,
    close_reply: Option<oneshot::Sender<Result<(), ProviderError>>>,
) {
    drop(stdin);
    let reaped = terminate_and_reap(&mut child).await;
    state.store(STATE_CLOSED, Ordering::Release);
    if let Some(mut turn) = active {
        if turn.result.is_some() {
            let visible = match fatal_error {
                ProviderError::RequestTimeout => ProviderError::RequestTimeout,
                ProviderError::CodexProtocolError => ProviderError::CodexProtocolError,
                _ => ProviderError::ProviderDisconnected,
            };
            settle_result(&mut turn, SessionResult::failure(visible));
        }
        let reason = if turn.cancel_deadline.is_some() {
            turn.terminal_reason
        } else if fatal_error == ProviderError::RequestTimeout {
            TerminalReason::RequestTimeout
        } else {
            TerminalReason::SessionEnded
        };
        let _ = terminal.send(TerminalEvidence {
            request_id: turn.request_id,
            reason,
            child_reaped: reaped,
        });
    }
    if let Some(reply) = close_reply {
        let _ = reply.send(Ok(()));
    }
}

async fn initialize(
    child: &mut Child,
    stdin: &mut ChildStdin,
    wire: &mut mpsc::Receiver<WireEvent>,
    client_info: Value,
    thread: ThreadSelection,
    rpc_timeout: Duration,
) -> Result<String, ProviderError> {
    write_json_line(
        stdin,
        &json!({"id": 1, "method": "initialize", "params": {"clientInfo": client_info}}),
    )
    .await?;
    let _ = await_response(child, stdin, wire, 1, rpc_timeout).await?;
    write_json_line(stdin, &json!({"method": "initialized", "params": {}})).await?;
    let (method, params) = match thread {
        ThreadSelection::Start { params } => ("thread/start", params),
        ThreadSelection::Resume {
            thread_id,
            mut params,
        } => {
            params.insert("threadId".to_owned(), Value::String(thread_id));
            ("thread/resume", params)
        }
    };
    write_json_line(stdin, &json!({"id": 2, "method": method, "params": params})).await?;
    let response = await_response(child, stdin, wire, 2, rpc_timeout).await?;
    response
        .get("thread")
        .and_then(|thread| thread.get("id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(ToOwned::to_owned)
        .ok_or(ProviderError::CodexInitializeFailed)
}

async fn await_response(
    child: &mut Child,
    stdin: &mut ChildStdin,
    wire: &mut mpsc::Receiver<WireEvent>,
    expected_id: u64,
    rpc_timeout: Duration,
) -> Result<Value, ProviderError> {
    timeout(rpc_timeout, async {
        loop {
            if child
                .try_wait()
                .map_err(|_| ProviderError::ProviderDisconnected)?
                .is_some()
            {
                return Err(ProviderError::ProviderDisconnected);
            }
            match wire.recv().await {
                Some(WireEvent::Message { value, .. }) if is_server_request(&value) => {
                    answer_server_request(stdin, &value).await?;
                }
                Some(WireEvent::Message { value, .. }) => {
                    if response_id(&value) != Some(expected_id) {
                        continue;
                    }
                    return response_result(&value);
                }
                Some(WireEvent::ProtocolError) => return Err(ProviderError::CodexProtocolError),
                Some(WireEvent::Eof) | None => return Err(ProviderError::ProviderDisconnected),
            }
        }
    })
    .await
    .map_err(|_| ProviderError::CodexInitializeFailed)?
}

async fn process_message(
    stdin: &mut ChildStdin,
    thread_id: &str,
    active: &mut Option<ActiveTurn>,
    terminal: &broadcast::Sender<TerminalEvidence>,
    state: &AtomicU8,
    next_rpc_id: &mut u64,
    value: Value,
    bytes: usize,
) -> Result<(), ProviderError> {
    if is_server_request(&value) {
        return answer_server_request(stdin, &value).await;
    }
    if let Some(id) = response_id(&value) {
        let Some(turn) = active.as_mut() else {
            return Ok(());
        };
        if id == turn.start_rpc_id {
            let result = response_result(&value)?;
            let turn_id = result
                .get("turn")
                .and_then(|turn| turn.get("id"))
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or(ProviderError::CodexProtocolError)?
                .to_owned();
            turn.turn_id = Some(turn_id.clone());
            let early = std::mem::take(&mut turn.early_events);
            turn.early_bytes = 0;
            for event in early {
                if let Some(completion) = apply_notification(thread_id, turn, &event) {
                    finish_active(active, terminal, state, completion);
                    return Ok(());
                }
            }
            if let Some(turn) = active
                .as_mut()
                .filter(|turn| turn.cancel_deadline.is_some())
                && !turn.interrupt_sent
            {
                let rpc_id = *next_rpc_id;
                *next_rpc_id += 1;
                send_interrupt(stdin, rpc_id, thread_id, &turn_id).await?;
                turn.interrupt_rpc_id = Some(rpc_id);
                turn.interrupt_sent = true;
            }
        } else if turn.interrupt_rpc_id == Some(id) {
            let _ = response_result(&value);
        }
        return Ok(());
    }
    if value.get("method").and_then(Value::as_str).is_none() {
        return Err(ProviderError::CodexProtocolError);
    }
    let Some(turn) = active.as_mut() else {
        return Ok(());
    };
    if turn.turn_id.is_none() && is_relevant_notification(thread_id, &value) {
        if turn.early_events.len() >= MAX_EARLY_NOTIFICATIONS
            || turn.early_bytes.saturating_add(bytes) > MAX_PROVIDER_FRAME_BYTES
        {
            return Err(ProviderError::CodexProtocolError);
        }
        turn.early_bytes += bytes;
        turn.early_events.push(value);
        return Ok(());
    }
    if let Some(completion) = apply_notification(thread_id, turn, &value) {
        finish_active(active, terminal, state, completion);
    }
    Ok(())
}

fn apply_notification(
    thread_id: &str,
    turn: &mut ActiveTurn,
    value: &Value,
) -> Option<SessionResult> {
    let method = value.get("method").and_then(Value::as_str)?;
    if method != "item/completed" && method != "turn/completed" {
        return None;
    }
    let params = value.get("params").and_then(Value::as_object)?;
    if params.get("threadId").and_then(Value::as_str) != Some(thread_id) {
        return None;
    }
    if method == "item/completed"
        && params.get("turnId").and_then(Value::as_str) != turn.turn_id.as_deref()
    {
        return None;
    }
    if method == "item/completed" {
        let item = params.get("item").and_then(Value::as_object)?;
        if item.get("type").and_then(Value::as_str) != Some("agentMessage") {
            return None;
        }
        let text = item.get("text").and_then(Value::as_str)?;
        match item.get("phase") {
            Some(Value::String(phase)) if phase == "final_answer" => {
                turn.final_messages.push(text.to_owned());
            }
            None => turn.fallback_message = Some(text.to_owned()),
            Some(_) => {}
        }
        return None;
    }
    let status = params
        .get("turn")
        .and_then(Value::as_object)
        .and_then(|completed| {
            (completed.get("id").and_then(Value::as_str) == turn.turn_id.as_deref())
                .then(|| completed.get("status").and_then(Value::as_str))
                .flatten()
        });
    match status {
        Some("completed") => {
            let content = if turn.final_messages.is_empty() {
                turn.fallback_message.clone()
            } else {
                Some(turn.final_messages.join("\n"))
            };
            Some(content.map_or_else(
                || SessionResult::failure(ProviderError::CodexNoFinalResponse),
                SessionResult::success,
            ))
        }
        Some("interrupted") => Some(SessionResult::failure(ProviderError::CodexTurnInterrupted)),
        Some("failed") => Some(SessionResult::failure(ProviderError::CodexTurnFailed)),
        Some(_) => Some(SessionResult::failure(ProviderError::CodexProtocolError)),
        None => None,
    }
}

fn finish_active(
    active: &mut Option<ActiveTurn>,
    terminal: &broadcast::Sender<TerminalEvidence>,
    state: &AtomicU8,
    result: SessionResult,
) {
    let mut turn = active.take().expect("completion requires active turn");
    settle_result(&mut turn, result);
    state.store(STATE_IDLE, Ordering::Release);
    let _ = terminal.send(TerminalEvidence {
        request_id: turn.request_id,
        reason: turn.terminal_reason,
        child_reaped: false,
    });
}

const fn cancel_terminal_reason(reason: CancelReason) -> TerminalReason {
    match reason {
        CancelReason::RequestTimeout => TerminalReason::RequestTimeout,
        CancelReason::ProviderDisconnected => TerminalReason::SessionEnded,
        CancelReason::TaskInterrupted => TerminalReason::OperatorInterrupt,
        CancelReason::RequestCancelled => TerminalReason::RequestCancelled,
    }
}

fn settle_result(turn: &mut ActiveTurn, result: SessionResult) {
    if let Some(reply) = turn.result.take() {
        let _ = reply.send(result);
    }
}

fn is_relevant_notification(thread_id: &str, value: &Value) -> bool {
    matches!(
        value.get("method").and_then(Value::as_str),
        Some("item/completed" | "turn/completed")
    ) && value
        .get("params")
        .and_then(Value::as_object)
        .and_then(|params| params.get("threadId"))
        .and_then(Value::as_str)
        == Some(thread_id)
}

fn response_id(value: &Value) -> Option<u64> {
    if value.get("method").is_some() {
        return None;
    }
    value.get("id").and_then(Value::as_u64)
}

fn response_result(value: &Value) -> Result<Value, ProviderError> {
    let object = value.as_object().ok_or(ProviderError::CodexProtocolError)?;
    match (object.get("result"), object.get("error")) {
        (Some(result), None) => Ok(result.clone()),
        _ => Err(ProviderError::CodexProtocolError),
    }
}

fn is_server_request(value: &Value) -> bool {
    value.get("method").and_then(Value::as_str).is_some()
        && value.get("id").is_some_and(valid_json_rpc_id)
}

fn valid_json_rpc_id(value: &Value) -> bool {
    value.is_string() || value.as_i64().is_some() || value.as_u64().is_some()
}

async fn answer_server_request(
    stdin: &mut ChildStdin,
    request: &Value,
) -> Result<(), ProviderError> {
    let id = request
        .get("id")
        .filter(|id| valid_json_rpc_id(id))
        .ok_or(ProviderError::CodexProtocolError)?;
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .ok_or(ProviderError::CodexProtocolError)?;
    let response = match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            json!({"id": id, "result": {"decision": "decline"}})
        }
        "item/permissions/requestApproval" => {
            json!({"id": id, "result": {"permissions": []}})
        }
        "mcpServer/elicitation/request" => {
            json!({"id": id, "result": {"action": "decline", "content": null}})
        }
        _ => json!({
            "id": id,
            "error": {"code": -32601, "message": "Unsupported server request"},
        }),
    };
    write_json_line(stdin, &response).await
}

async fn send_interrupt(
    stdin: &mut ChildStdin,
    rpc_id: u64,
    thread_id: &str,
    turn_id: &str,
) -> Result<(), ProviderError> {
    write_json_line(
        stdin,
        &json!({
            "id": rpc_id,
            "method": "turn/interrupt",
            "params": {"threadId": thread_id, "turnId": turn_id},
        }),
    )
    .await
}

async fn write_json_line(stdin: &mut ChildStdin, value: &Value) -> Result<(), ProviderError> {
    let payload = serde_json::to_vec(value).map_err(|_| ProviderError::CodexProtocolError)?;
    if payload.len() > MAX_PROVIDER_FRAME_BYTES {
        return Err(ProviderError::CodexProtocolError);
    }
    stdin
        .write_all(&payload)
        .await
        .map_err(|_| ProviderError::ProviderDisconnected)?;
    stdin
        .write_all(b"\n")
        .await
        .map_err(|_| ProviderError::ProviderDisconnected)?;
    stdin
        .flush()
        .await
        .map_err(|_| ProviderError::ProviderDisconnected)
}
