use std::{
    ffi::OsString,
    fs,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{broadcast, mpsc, oneshot},
    time::{Instant, MissedTickBehavior, interval, sleep_until, timeout},
};
use uuid::Uuid;

use super::{
    CancelReason, MAX_PROVIDER_FRAME_BYTES, ManagedLaunch, OwnedProvider, PROVIDER_TERMINAL_GRACE,
    ProviderError, SessionRequest, SessionResult, TerminalEvidence, TerminalReason, drain_stderr,
    graceful_reap, managed_path_utf8, remaining, terminate_and_reap,
};

const DEFAULT_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
const PROVIDER_DEADLINE_MARGIN: Duration = Duration::from_millis(100);
const CHILD_POLL_INTERVAL: Duration = Duration::from_millis(25);
const STATE_STARTING: u8 = 0;
const STATE_IDLE: u8 = 1;
const STATE_RUNNING: u8 = 2;
const STATE_CANCELLING: u8 = 3;
const STATE_CLOSED: u8 = 4;

pub struct ClaudeConfig {
    pub node_executable: PathBuf,
    pub node_arguments: Vec<OsString>,
    pub bridge_asset: PathBuf,
    pub bridge_arguments: Vec<OsString>,
    pub claude_executable: Option<PathBuf>,
    pub launch: ManagedLaunch,
    pub resume_session_id: Option<String>,
    pub initialize_timeout: Duration,
    pub max_turns: u32,
}

impl ClaudeConfig {
    pub fn managed(node_executable: PathBuf, launch: ManagedLaunch) -> Result<Self, ProviderError> {
        let bridge_asset =
            launch.resolve_integration_asset(std::path::Path::new("claude-sdk/bridge.js"))?;
        Ok(Self::with_bridge_asset(
            node_executable,
            bridge_asset,
            launch,
        ))
    }

    #[must_use]
    pub fn with_bridge_asset(
        node_executable: PathBuf,
        bridge_asset: PathBuf,
        launch: ManagedLaunch,
    ) -> Self {
        Self {
            node_executable,
            node_arguments: Vec::new(),
            bridge_asset,
            bridge_arguments: Vec::new(),
            claude_executable: None,
            launch,
            resume_session_id: None,
            initialize_timeout: DEFAULT_INITIALIZE_TIMEOUT,
            max_turns: 8,
        }
    }
}

pub struct ClaudeProvider {
    commands: mpsc::Sender<ActorCommand>,
    state: Arc<AtomicU8>,
    terminal: broadcast::Sender<TerminalEvidence>,
    session_id: Arc<Mutex<Option<Uuid>>>,
}

impl ClaudeProvider {
    pub async fn launch(config: ClaudeConfig) -> Result<Self, ProviderError> {
        validate_config(&config)?;
        let mut command = Command::new(&config.node_executable);
        command
            .args(&config.node_arguments)
            .arg(&config.bridge_asset)
            .args(&config.bridge_arguments);
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
        tokio::spawn(read_frames(stdout, wire_tx));
        let (terminal, _) = broadcast::channel(16);
        let state = Arc::new(AtomicU8::new(STATE_STARTING));
        let session_id = Arc::new(Mutex::new(None));
        let (started_tx, started_rx) = oneshot::channel();
        tokio::spawn(run_actor(
            child,
            stdin,
            wire_rx,
            command_rx,
            terminal.clone(),
            Arc::clone(&state),
            Arc::clone(&session_id),
            config,
            started_tx,
        ));
        match started_rx.await {
            Ok(Ok(())) => Ok(Self {
                commands,
                state,
                terminal,
                session_id,
            }),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(ProviderError::ClaudeInitializeFailed),
        }
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_IDLE
    }

    #[must_use]
    pub fn session_id(&self) -> Option<Uuid> {
        *self
            .session_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

impl OwnedProvider for ClaudeProvider {
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

enum WireEvent {
    Message(Value),
    ProtocolError,
    Eof,
}

enum ResponseAction {
    None,
    BeginClose(CancelReason),
}

enum ActorOutcome {
    Continue,
    ContinueClosing(oneshot::Sender<Result<(), ProviderError>>),
    Stop(ActorExit),
}

struct ActorExit {
    error: ProviderError,
    close_reply: Option<oneshot::Sender<Result<(), ProviderError>>>,
    graceful_shutdown: bool,
}

struct ActiveTurn {
    request_id: String,
    rpc_id: u64,
    result: Option<oneshot::Sender<SessionResult>>,
    deadline: Instant,
    close_deadline: Option<Instant>,
    cancel_rpc_id: Option<u64>,
    shutdown_rpc_id: Option<u64>,
    terminal_sent: bool,
    terminal_reason: TerminalReason,
}

fn validate_config(config: &ClaudeConfig) -> Result<(), ProviderError> {
    let bridge_is_regular = fs::symlink_metadata(&config.bridge_asset)
        .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink());
    if !config.node_executable.is_absolute()
        || !config.bridge_asset.is_absolute()
        || !bridge_is_regular
        || config.initialize_timeout.is_zero()
        || config.max_turns == 0
        || managed_path_utf8(config.launch.cwd()).is_err()
        || config
            .claude_executable
            .as_ref()
            .is_some_and(|path| !path.is_absolute() || managed_path_utf8(path).is_err())
        || config
            .resume_session_id
            .as_deref()
            .is_some_and(|id| Uuid::parse_str(id).is_err())
    {
        return Err(ProviderError::LaunchFailed);
    }
    Ok(())
}

async fn read_frames(mut stdout: ChildStdout, sender: mpsc::Sender<WireEvent>) {
    loop {
        let mut header = [0_u8; 4];
        match stdout.read_exact(&mut header).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                let _ = sender.send(WireEvent::Eof).await;
                return;
            }
            Err(_) => {
                let _ = sender.send(WireEvent::ProtocolError).await;
                return;
            }
        }
        let length = u32::from_be_bytes(header) as usize;
        if length == 0 || length > MAX_PROVIDER_FRAME_BYTES {
            let _ = sender.send(WireEvent::ProtocolError).await;
            return;
        }
        let mut payload = vec![0_u8; length];
        if stdout.read_exact(&mut payload).await.is_err() {
            let _ = sender.send(WireEvent::ProtocolError).await;
            return;
        }
        let value = match serde_json::from_slice::<Value>(&payload) {
            Ok(value) if value.is_object() => value,
            _ => {
                let _ = sender.send(WireEvent::ProtocolError).await;
                return;
            }
        };
        if sender.send(WireEvent::Message(value)).await.is_err() {
            return;
        }
    }
}

async fn run_actor(
    mut child: Child,
    mut stdin: ChildStdin,
    mut wire: mpsc::Receiver<WireEvent>,
    mut commands: mpsc::Receiver<ActorCommand>,
    terminal: broadcast::Sender<TerminalEvidence>,
    state: Arc<AtomicU8>,
    session_id: Arc<Mutex<Option<Uuid>>>,
    config: ClaudeConfig,
    started: oneshot::Sender<Result<(), ProviderError>>,
) {
    let initialized = initialize(&mut child, &mut stdin, &mut wire, &config).await;
    let initial_session_id = match initialized {
        Ok(value) => value,
        Err(error) => {
            let _ = terminate_and_reap(&mut child).await;
            state.store(STATE_CLOSED, Ordering::Release);
            let _ = started.send(Err(error));
            return;
        }
    };
    *session_id.lock().expect("Claude session mutex poisoned") = initial_session_id;
    state.store(STATE_IDLE, Ordering::Release);
    let _ = started.send(Ok(()));

    let (exit, active) = run_actor_loop(
        &mut child,
        &mut stdin,
        &mut wire,
        &mut commands,
        &terminal,
        &state,
        &session_id,
    )
    .await;

    finish_actor(child, stdin, active, &terminal, &state, exit).await;
}

async fn run_actor_loop(
    child: &mut Child,
    stdin: &mut ChildStdin,
    wire: &mut mpsc::Receiver<WireEvent>,
    commands: &mut mpsc::Receiver<ActorCommand>,
    terminal: &broadcast::Sender<TerminalEvidence>,
    state: &AtomicU8,
    session_id: &Mutex<Option<Uuid>>,
) -> (ActorExit, Option<ActiveTurn>) {
    let mut next_rpc_id = 2_u64;
    let mut active: Option<ActiveTurn> = None;
    let mut poll = interval(CHILD_POLL_INTERVAL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut close_reply = None;
    let exit = loop {
        let far = Instant::now() + Duration::from_hours(8760);
        let request_deadline = active
            .as_ref()
            .filter(|turn| turn.close_deadline.is_none())
            .map_or(far, |turn| turn.deadline);
        let close_deadline = active
            .as_ref()
            .and_then(|turn| turn.close_deadline)
            .unwrap_or(far);
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    break ActorExit {
                        error: ProviderError::ProviderDisconnected,
                        close_reply,
                        graceful_shutdown: false,
                    };
                };
                match handle_actor_command(
                    command,
                    stdin,
                    &mut active,
                    state,
                    &mut next_rpc_id,
                ).await {
                    ActorOutcome::Continue => {}
                    ActorOutcome::ContinueClosing(reply) => close_reply = Some(reply),
                    ActorOutcome::Stop(exit) => break exit,
                }
            }
            event = wire.recv() => {
                if let Err(error) = handle_wire_event(
                    event,
                    stdin,
                    &mut active,
                    terminal,
                    state,
                    session_id,
                    &mut next_rpc_id,
                ).await {
                    break ActorExit {
                        error,
                        close_reply,
                        graceful_shutdown: false,
                    };
                }
            }
            () = sleep_until(request_deadline), if request_deadline != far => {
                if handle_request_timeout(stdin, &mut active, state, &mut next_rpc_id)
                    .await
                    .is_err()
                {
                    break ActorExit {
                        error: ProviderError::ProviderDisconnected,
                        close_reply,
                        graceful_shutdown: false,
                    };
                }
            }
            () = sleep_until(close_deadline), if close_deadline != far => {
                break ActorExit {
                    error: ProviderError::RequestTimeout,
                    close_reply,
                    graceful_shutdown: false,
                };
            }
            _ = poll.tick() => {
                match child.try_wait() {
                    Ok(Some(_)) | Err(_) => {
                        break ActorExit {
                            error: ProviderError::ProviderDisconnected,
                            close_reply,
                            graceful_shutdown: false,
                        };
                    }
                    Ok(None) => {}
                }
            }
        }
    };
    (exit, active)
}

async fn handle_actor_command(
    command: ActorCommand,
    stdin: &mut ChildStdin,
    active: &mut Option<ActiveTurn>,
    state: &AtomicU8,
    next_rpc_id: &mut u64,
) -> ActorOutcome {
    match command {
        ActorCommand::Handle(request, reply) => {
            start_turn(request, reply, stdin, active, state, next_rpc_id).await
        }
        ActorCommand::Cancel {
            request_id,
            reason,
            reply,
        } => cancel_turn(request_id, reason, reply, stdin, active, state, next_rpc_id).await,
        ActorCommand::Close(reply) => close_actor(reply, stdin, active, state, next_rpc_id).await,
    }
}

async fn start_turn(
    request: SessionRequest,
    reply: oneshot::Sender<SessionResult>,
    stdin: &mut ChildStdin,
    active: &mut Option<ActiveTurn>,
    state: &AtomicU8,
    next_rpc_id: &mut u64,
) -> ActorOutcome {
    if active.is_some() {
        let _ = reply.send(SessionResult::failure(ProviderError::SessionBusy));
        return ActorOutcome::Continue;
    }
    if state.load(Ordering::Acquire) != STATE_IDLE {
        let _ = reply.send(SessionResult::failure(ProviderError::ProviderNotReady));
        return ActorOutcome::Continue;
    }
    if request.deadline <= Instant::now() {
        let _ = reply.send(SessionResult::failure(ProviderError::RequestTimeout));
        return ActorOutcome::Continue;
    }
    let rpc_id = *next_rpc_id;
    *next_rpc_id += 1;
    let timeout_ms = u64::try_from(remaining(request.deadline).as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let message = json!({
        "v": 1,
        "id": rpc_id,
        "method": "turn",
        "params": {
            "requestId": request.request_id,
            "prompt": request.content,
            "timeoutMs": timeout_ms,
        },
    });
    if write_frame(stdin, &message).await.is_err() {
        let _ = reply.send(SessionResult::failure(ProviderError::ProviderDisconnected));
        return ActorOutcome::Stop(ActorExit {
            error: ProviderError::ProviderDisconnected,
            close_reply: None,
            graceful_shutdown: false,
        });
    }
    let now = Instant::now();
    let deadline = request
        .deadline
        .checked_sub(PROVIDER_DEADLINE_MARGIN)
        .unwrap_or(now)
        .max(now);
    *active = Some(ActiveTurn {
        request_id: request.request_id,
        rpc_id,
        result: Some(reply),
        deadline,
        close_deadline: None,
        cancel_rpc_id: None,
        shutdown_rpc_id: None,
        terminal_sent: false,
        terminal_reason: TerminalReason::TurnEnded,
    });
    state.store(STATE_RUNNING, Ordering::Release);
    ActorOutcome::Continue
}

async fn cancel_turn(
    request_id: String,
    reason: CancelReason,
    reply: oneshot::Sender<Result<(), ProviderError>>,
    stdin: &mut ChildStdin,
    active: &mut Option<ActiveTurn>,
    state: &AtomicU8,
    next_rpc_id: &mut u64,
) -> ActorOutcome {
    let Some(turn) = active.as_mut() else {
        let _ = reply.send(Err(ProviderError::ProviderNotReady));
        return ActorOutcome::Continue;
    };
    if turn.request_id != request_id {
        let _ = reply.send(Err(ProviderError::ProviderNotReady));
        return ActorOutcome::Continue;
    }
    if turn.close_deadline.is_none() {
        let visible = if reason == CancelReason::RequestTimeout {
            ProviderError::RequestTimeout
        } else {
            ProviderError::ProviderDisconnected
        };
        settle_result(turn, SessionResult::failure(visible));
        turn.terminal_reason = terminal_reason(reason);
        if begin_close(stdin, turn, next_rpc_id, reason).await.is_err() {
            let _ = reply.send(Err(ProviderError::ProviderDisconnected));
            return ActorOutcome::Stop(ActorExit {
                error: ProviderError::ProviderDisconnected,
                close_reply: None,
                graceful_shutdown: false,
            });
        }
        state.store(STATE_CANCELLING, Ordering::Release);
    }
    let _ = reply.send(Ok(()));
    ActorOutcome::Continue
}

async fn close_actor(
    reply: oneshot::Sender<Result<(), ProviderError>>,
    stdin: &mut ChildStdin,
    active: &mut Option<ActiveTurn>,
    state: &AtomicU8,
    next_rpc_id: &mut u64,
) -> ActorOutcome {
    if let Some(turn) = active.as_mut() {
        settle_result(
            turn,
            SessionResult::failure(ProviderError::ProviderDisconnected),
        );
        turn.terminal_reason = TerminalReason::SessionEnded;
        if begin_close(stdin, turn, next_rpc_id, CancelReason::ProviderDisconnected)
            .await
            .is_err()
        {
            return ActorOutcome::Stop(ActorExit {
                error: ProviderError::ProviderDisconnected,
                close_reply: Some(reply),
                graceful_shutdown: false,
            });
        }
        state.store(STATE_CANCELLING, Ordering::Release);
        ActorOutcome::ContinueClosing(reply)
    } else {
        let rpc_id = *next_rpc_id;
        let graceful_shutdown = write_frame(
            stdin,
            &json!({"v": 1, "id": rpc_id, "method": "shutdown", "params": {}}),
        )
        .await
        .is_ok();
        ActorOutcome::Stop(ActorExit {
            error: ProviderError::ProviderDisconnected,
            close_reply: Some(reply),
            graceful_shutdown,
        })
    }
}

async fn handle_wire_event(
    event: Option<WireEvent>,
    stdin: &mut ChildStdin,
    active: &mut Option<ActiveTurn>,
    terminal: &broadcast::Sender<TerminalEvidence>,
    state: &AtomicU8,
    session_id: &Mutex<Option<Uuid>>,
    next_rpc_id: &mut u64,
) -> Result<(), ProviderError> {
    match event {
        Some(WireEvent::Message(value)) => {
            match process_response(&value, active, terminal, state, session_id)? {
                ResponseAction::None => Ok(()),
                ResponseAction::BeginClose(reason) => {
                    let turn = active
                        .as_mut()
                        .expect("close action requires an active Claude turn");
                    begin_close(stdin, turn, next_rpc_id, reason).await?;
                    state.store(STATE_CANCELLING, Ordering::Release);
                    Ok(())
                }
            }
        }
        Some(WireEvent::ProtocolError) => Err(ProviderError::BridgeProtocolError),
        Some(WireEvent::Eof) | None => Err(ProviderError::ProviderDisconnected),
    }
}

async fn handle_request_timeout(
    stdin: &mut ChildStdin,
    active: &mut Option<ActiveTurn>,
    state: &AtomicU8,
    next_rpc_id: &mut u64,
) -> Result<(), ProviderError> {
    let turn = active.as_mut().expect("active turn has request deadline");
    settle_result(turn, SessionResult::failure(ProviderError::RequestTimeout));
    turn.terminal_reason = TerminalReason::RequestTimeout;
    begin_close(stdin, turn, next_rpc_id, CancelReason::RequestTimeout).await?;
    state.store(STATE_CANCELLING, Ordering::Release);
    Ok(())
}

async fn finish_actor(
    mut child: Child,
    stdin: ChildStdin,
    active: Option<ActiveTurn>,
    terminal: &broadcast::Sender<TerminalEvidence>,
    state: &AtomicU8,
    exit: ActorExit,
) {
    drop(stdin);
    let reaped = if exit.graceful_shutdown {
        graceful_reap(&mut child, PROVIDER_TERMINAL_GRACE).await
    } else {
        terminate_and_reap(&mut child).await
    };
    state.store(STATE_CLOSED, Ordering::Release);
    if let Some(mut turn) = active {
        if turn.result.is_some() {
            let visible = match exit.error {
                ProviderError::RequestTimeout => ProviderError::RequestTimeout,
                ProviderError::BridgeProtocolError => ProviderError::BridgeProtocolError,
                ProviderError::ClaudeProtocolError => ProviderError::ClaudeProtocolError,
                _ => ProviderError::ProviderDisconnected,
            };
            settle_result(&mut turn, SessionResult::failure(visible));
        }
        if !turn.terminal_sent {
            let _ = terminal.send(TerminalEvidence {
                request_id: turn.request_id,
                reason: turn.terminal_reason,
                child_reaped: reaped,
            });
        }
    }
    if let Some(reply) = exit.close_reply {
        let _ = reply.send(Ok(()));
    }
}

async fn initialize(
    child: &mut Child,
    stdin: &mut ChildStdin,
    wire: &mut mpsc::Receiver<WireEvent>,
    config: &ClaudeConfig,
) -> Result<Option<Uuid>, ProviderError> {
    let mut params = Map::new();
    if let Some(resume) = &config.resume_session_id {
        params.insert("resumeSessionId".to_owned(), Value::String(resume.clone()));
    }
    params.insert(
        "cwd".to_owned(),
        Value::String(
            config
                .launch
                .cwd()
                .to_str()
                .ok_or(ProviderError::InvalidLaunchContext)?
                .to_owned(),
        ),
    );
    if let Some(executable) = &config.claude_executable {
        params.insert(
            "executablePath".to_owned(),
            Value::String(
                executable
                    .to_str()
                    .ok_or(ProviderError::InvalidLaunchContext)?
                    .to_owned(),
            ),
        );
    }
    params.insert(
        "initializeTimeoutMs".to_owned(),
        Value::Number(
            u64::try_from(config.initialize_timeout.as_millis())
                .unwrap_or(u64::MAX)
                .max(1)
                .into(),
        ),
    );
    params.insert(
        "maxTurns".to_owned(),
        Value::Number(config.max_turns.into()),
    );
    params.insert("mcp".to_owned(), config.launch.claude_mcp_config()?);
    write_frame(
        stdin,
        &json!({"v": 1, "id": 1, "method": "initialize", "params": params}),
    )
    .await?;
    let response = timeout(config.initialize_timeout, async {
        if child
            .try_wait()
            .map_err(|_| ProviderError::ProviderDisconnected)?
            .is_some()
        {
            return Err(ProviderError::ProviderDisconnected);
        }
        match wire.recv().await {
            Some(WireEvent::Message(value)) => {
                validate_response_envelope(&value, 1)?;
                Ok(value)
            }
            Some(WireEvent::ProtocolError) => Err(ProviderError::BridgeProtocolError),
            Some(WireEvent::Eof) | None => Err(ProviderError::ProviderDisconnected),
        }
    })
    .await
    .map_err(|_| ProviderError::ClaudeInitializeFailed)??;
    if !response.get("ok").and_then(Value::as_bool).unwrap_or(false) {
        return Err(map_bridge_error(&response));
    }
    let result = response
        .get("result")
        .and_then(Value::as_object)
        .ok_or(ProviderError::ClaudeInitializeFailed)?;
    if result.get("state").and_then(Value::as_str) != Some("ready") {
        return Err(ProviderError::ClaudeInitializeFailed);
    }
    result
        .get("sessionId")
        .map(parse_session_id)
        .transpose()
        .map_err(|_| ProviderError::ClaudeInitializeFailed)
}

fn process_response(
    value: &Value,
    active: &mut Option<ActiveTurn>,
    terminal: &broadcast::Sender<TerminalEvidence>,
    state: &AtomicU8,
    session_id: &Mutex<Option<Uuid>>,
) -> Result<ResponseAction, ProviderError> {
    let turn = active.as_mut().ok_or(ProviderError::ClaudeProtocolError)?;
    let id = value
        .get("id")
        .and_then(Value::as_u64)
        .ok_or(ProviderError::ClaudeProtocolError)?;
    if id == turn.rpc_id {
        validate_response_envelope(value, id)?;
        let result = decode_turn_result(value, session_id)?;
        if turn.close_deadline.is_some() {
            if !turn.terminal_sent {
                let _ = terminal.send(TerminalEvidence {
                    request_id: turn.request_id.clone(),
                    reason: turn.terminal_reason,
                    child_reaped: false,
                });
                turn.terminal_sent = true;
            }
            return Ok(ResponseAction::None);
        }
        if let Some(error) = result.error().filter(|error| {
            matches!(
                error,
                ProviderError::RequestTimeout
                    | ProviderError::ProviderDisconnected
                    | ProviderError::ClaudeProtocolError
                    | ProviderError::BridgeProtocolError
            )
        }) {
            settle_result(turn, result);
            turn.terminal_reason = if error == ProviderError::RequestTimeout {
                TerminalReason::RequestTimeout
            } else {
                TerminalReason::HostError
            };
            let _ = terminal.send(TerminalEvidence {
                request_id: turn.request_id.clone(),
                reason: turn.terminal_reason,
                child_reaped: false,
            });
            turn.terminal_sent = true;
            let reason = if error == ProviderError::RequestTimeout {
                CancelReason::RequestTimeout
            } else {
                CancelReason::ProviderDisconnected
            };
            return Ok(ResponseAction::BeginClose(reason));
        }
        let mut completed = active.take().expect("turn response requires active turn");
        settle_result(&mut completed, result);
        state.store(STATE_IDLE, Ordering::Release);
        let _ = terminal.send(TerminalEvidence {
            request_id: completed.request_id,
            reason: TerminalReason::TurnEnded,
            child_reaped: false,
        });
        return Ok(ResponseAction::None);
    }
    if turn.cancel_rpc_id == Some(id) {
        validate_response_envelope(value, id)?;
        let result = value
            .get("result")
            .and_then(Value::as_object)
            .ok_or(ProviderError::BridgeProtocolError)?;
        if result.get("cancelled").and_then(Value::as_bool).is_none() {
            return Err(ProviderError::BridgeProtocolError);
        }
        turn.cancel_rpc_id = None;
        return Ok(ResponseAction::None);
    }
    if turn.shutdown_rpc_id == Some(id) {
        validate_response_envelope(value, id)?;
        let result = value
            .get("result")
            .and_then(Value::as_object)
            .ok_or(ProviderError::BridgeProtocolError)?;
        if result.get("state").and_then(Value::as_str) != Some("closed") {
            return Err(ProviderError::BridgeProtocolError);
        }
        turn.shutdown_rpc_id = None;
        return Ok(ResponseAction::None);
    }
    Err(ProviderError::ClaudeProtocolError)
}

fn decode_turn_result(
    response: &Value,
    session_id: &Mutex<Option<Uuid>>,
) -> Result<SessionResult, ProviderError> {
    if response.get("ok").and_then(Value::as_bool) == Some(false) {
        if let Some(value) = response
            .get("error")
            .and_then(Value::as_object)
            .and_then(|error| error.get("sessionId"))
        {
            let parsed = parse_session_id(value)?;
            *session_id.lock().expect("Claude session mutex poisoned") = Some(parsed);
        }
        return Ok(SessionResult::failure(map_bridge_error(response)));
    }
    let result = response
        .get("result")
        .and_then(Value::as_object)
        .ok_or(ProviderError::BridgeProtocolError)?;
    let id = result
        .get("sessionId")
        .map(parse_session_id)
        .transpose()?
        .ok_or(ProviderError::BridgeProtocolError)?;
    *session_id.lock().expect("Claude session mutex poisoned") = Some(id);
    Ok(result.get("content").and_then(Value::as_str).map_or_else(
        || SessionResult::failure(ProviderError::ClaudeNoResult),
        SessionResult::success,
    ))
}

fn validate_response_envelope(value: &Value, expected_id: u64) -> Result<(), ProviderError> {
    let object = value
        .as_object()
        .ok_or(ProviderError::BridgeProtocolError)?;
    if object.get("v").and_then(Value::as_u64) != Some(1)
        || object.get("id").and_then(Value::as_u64) != Some(expected_id)
    {
        return Err(ProviderError::BridgeProtocolError);
    }
    match (
        object.get("ok").and_then(Value::as_bool),
        object.get("result"),
        object.get("error"),
    ) {
        (Some(true), Some(_), None) | (Some(false), None, Some(_)) => Ok(()),
        _ => Err(ProviderError::BridgeProtocolError),
    }
}

fn map_bridge_error(response: &Value) -> ProviderError {
    let code = response
        .get("error")
        .and_then(Value::as_object)
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str);
    match code {
        Some("claude_initialize_failed") => ProviderError::ClaudeInitializeFailed,
        Some("request_timeout") => ProviderError::RequestTimeout,
        Some("provider_disconnected") => ProviderError::ProviderDisconnected,
        Some("error_max_turns" | "claude_max_turns") => ProviderError::ClaudeMaxTurns,
        Some("error_max_budget_usd" | "claude_max_budget") => ProviderError::ClaudeMaxBudget,
        Some("error_max_structured_output_retries" | "claude_structured_output_error") => {
            ProviderError::ClaudeStructuredOutputError
        }
        Some("claude_no_result") => ProviderError::ClaudeNoResult,
        Some("claude_sdk_error") => ProviderError::ClaudeSdkError,
        Some("claude_protocol_error") => ProviderError::ClaudeProtocolError,
        Some("bridge_protocol_error" | "session_busy") | None => ProviderError::BridgeProtocolError,
        Some(_) => ProviderError::ClaudeExecutionError,
    }
}

fn parse_session_id(value: &Value) -> Result<Uuid, ProviderError> {
    value
        .as_str()
        .and_then(|id| Uuid::parse_str(id).ok())
        .ok_or(ProviderError::ClaudeProtocolError)
}

async fn begin_close(
    stdin: &mut ChildStdin,
    turn: &mut ActiveTurn,
    next_rpc_id: &mut u64,
    reason: CancelReason,
) -> Result<(), ProviderError> {
    let cancel_id = *next_rpc_id;
    *next_rpc_id += 1;
    let shutdown_id = *next_rpc_id;
    *next_rpc_id += 1;
    let bridge_reason = match reason {
        CancelReason::RequestTimeout => "request_timeout",
        CancelReason::TaskInterrupted | CancelReason::RequestCancelled => "task_interrupted",
        CancelReason::ProviderDisconnected => "provider_disconnected",
    };
    write_frame(
        stdin,
        &json!({
            "v": 1,
            "id": cancel_id,
            "method": "cancel",
            "params": {"requestId": turn.request_id, "reason": bridge_reason},
        }),
    )
    .await?;
    write_frame(
        stdin,
        &json!({"v": 1, "id": shutdown_id, "method": "shutdown", "params": {}}),
    )
    .await?;
    turn.cancel_rpc_id = Some(cancel_id);
    turn.shutdown_rpc_id = Some(shutdown_id);
    turn.close_deadline = Some(Instant::now() + PROVIDER_TERMINAL_GRACE);
    Ok(())
}

fn terminal_reason(reason: CancelReason) -> TerminalReason {
    match reason {
        CancelReason::RequestTimeout => TerminalReason::RequestTimeout,
        CancelReason::TaskInterrupted => TerminalReason::OperatorInterrupt,
        CancelReason::RequestCancelled => TerminalReason::RequestCancelled,
        CancelReason::ProviderDisconnected => TerminalReason::SessionEnded,
    }
}

fn settle_result(turn: &mut ActiveTurn, result: SessionResult) {
    if let Some(reply) = turn.result.take() {
        let _ = reply.send(result);
    }
}

async fn write_frame(stdin: &mut ChildStdin, value: &Value) -> Result<(), ProviderError> {
    let payload = serde_json::to_vec(value).map_err(|_| ProviderError::BridgeProtocolError)?;
    if payload.is_empty() || payload.len() > MAX_PROVIDER_FRAME_BYTES {
        return Err(ProviderError::BridgeProtocolError);
    }
    let length = u32::try_from(payload.len()).map_err(|_| ProviderError::BridgeProtocolError)?;
    stdin
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|_| ProviderError::ProviderDisconnected)?;
    stdin
        .write_all(&payload)
        .await
        .map_err(|_| ProviderError::ProviderDisconnected)?;
    stdin
        .flush()
        .await
        .map_err(|_| ProviderError::ProviderDisconnected)
}
