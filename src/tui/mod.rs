use std::{
    fmt,
    io::{self, IsTerminal as _},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crossterm::{
    cursor::{Hide, Show},
    event::{
        DisableBracketedPaste, EnableBracketedPaste, Event, EventStream, KeyCode, KeyEventKind,
        KeyModifiers,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures_util::StreamExt as _;
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::{
    io::AsyncWriteExt as _,
    process::Command,
    sync::mpsc,
    task::{AbortHandle, JoinHandle, JoinSet},
    time::{MissedTickBehavior, interval, timeout},
};
use uuid::Uuid;

use crate::{
    client::ClientConfig, onboarding::issue::IssuedPrompt, process::RuntimeRecord,
    protocol::WorkspaceName,
};

pub mod controller;
pub mod input;
pub mod state;
pub mod view;

#[derive(Clone)]
pub struct UiOptions {
    pub profile_name: Option<String>,
    pub workspace: Option<WorkspaceName>,
    pub owned_runtime: Option<RuntimeRecord>,
    pub ownership: state::Ownership,
    pub owned_data_dir: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UiExit {
    Detached,
    StopOwnedRouter,
}

#[derive(Debug)]
pub struct UiError {
    pub code: &'static str,
    pub message: &'static str,
}

impl UiError {
    #[must_use]
    pub const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }
}

impl fmt::Display for UiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for UiError {}

pub type SharedState = Arc<Mutex<state::UiState>>;

pub enum UiCommand {
    LoadWorkspaces,
    SelectWorkspace(WorkspaceName),
    LoadMembers,
    LoadTasks,
    LoadTaskDetail,
    LoadTaskHistory,
    RecentHistory,
    PreviousHistory,
    NextHistory,
    SubmitForm,
    SubmitChat,
    RetryMutation,
    RetryChat,
    ConfirmStop,
}

// This channel carries a bearer ticket. In particular, do not derive Debug.
pub enum UiNotice {
    Invite(IssuedPrompt),
    Exit(UiExit),
    Fatal(UiError),
}

pub enum UiInput {
    None,
    Command(UiCommand),
    Detach,
    PrintPrompt,
    CopyPrompt,
    DiscardPrompt,
}

const FRAME_INTERVAL: Duration = Duration::from_millis(50);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(3);
const CLIPBOARD_DEADLINE: Duration = Duration::from_secs(3);
static TERMINAL_ACTIVE: AtomicBool = AtomicBool::new(false);

type ConsoleTerminal = Terminal<CrosstermBackend<io::Stdout>>;
type PanicHook = Arc<dyn Fn(&std::panic::PanicHookInfo<'_>) + Send + Sync + 'static>;

#[derive(Default)]
struct TerminalModes {
    raw: AtomicBool,
    alternate: AtomicBool,
    paste: AtomicBool,
    cursor: AtomicBool,
}

impl TerminalModes {
    fn enter(&self) -> io::Result<()> {
        // Mark attempts first: an escape write can fail after partially succeeding.
        self.raw.store(true, Ordering::SeqCst);
        enable_raw_mode()?;
        self.alternate.store(true, Ordering::SeqCst);
        execute!(io::stdout(), EnterAlternateScreen)?;
        self.paste.store(true, Ordering::SeqCst);
        execute!(io::stdout(), EnableBracketedPaste)?;
        self.cursor.store(true, Ordering::SeqCst);
        execute!(io::stdout(), Hide)
    }

    fn restore(&self) -> io::Result<()> {
        let mut first_error = None;
        let mut restore = |flag: &AtomicBool, action: &mut dyn FnMut() -> io::Result<()>| {
            if flag.swap(false, Ordering::SeqCst)
                && let Err(error) = action()
            {
                flag.store(true, Ordering::SeqCst);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        };
        restore(&self.paste, &mut || {
            execute!(io::stdout(), DisableBracketedPaste)
        });
        restore(&self.alternate, &mut || {
            execute!(io::stdout(), LeaveAlternateScreen)
        });
        restore(&self.cursor, &mut || execute!(io::stdout(), Show));
        restore(&self.raw, &mut disable_raw_mode);
        first_error.map_or(Ok(()), Err)
    }
}

struct TerminalGuard {
    modes: Arc<TerminalModes>,
    previous_hook: PanicHook,
}

impl TerminalGuard {
    fn enter() -> Result<Self, UiError> {
        require_terminal()?;
        if TERMINAL_ACTIVE.swap(true, Ordering::SeqCst) {
            return Err(UiError::new(
                "terminal_busy",
                "A console already owns this terminal.",
            ));
        }
        let modes = Arc::new(TerminalModes::default());
        let previous_hook: PanicHook = std::panic::take_hook().into();
        let hook_modes = Arc::clone(&modes);
        let hook_previous = Arc::clone(&previous_hook);
        std::panic::set_hook(Box::new(move |info| {
            let _ = hook_modes.restore();
            hook_previous(info);
        }));
        let guard = Self {
            modes,
            previous_hook,
        };
        guard.resume()?;
        Ok(guard)
    }

    fn resume(&self) -> Result<(), UiError> {
        self.modes.enter().map_err(|_| {
            UiError::new(
                "terminal_init_failed",
                "Could not initialize the console terminal.",
            )
        })
    }

    fn restore(&self) -> Result<(), UiError> {
        self.modes.restore().map_err(|_| {
            UiError::new(
                "terminal_restore_failed",
                "Could not fully restore the terminal.",
            )
        })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = self.modes.restore();
        // set_hook itself panics during unwinding. The installed hook retains only
        // inert terminal flags, not the console state or its private prompt.
        if !std::thread::panicking() {
            let previous_hook = Arc::clone(&self.previous_hook);
            std::panic::set_hook(Box::new(move |info| previous_hook(info)));
        }
        TERMINAL_ACTIVE.store(false, Ordering::SeqCst);
    }
}

fn require_terminal() -> Result<(), UiError> {
    if !io::stdin().is_terminal()
        || !io::stdout().is_terminal()
        || std::env::var("TERM").is_ok_and(|term| term.eq_ignore_ascii_case("dumb"))
    {
        return Err(UiError::new(
            "terminal_required",
            "The console needs terminal stdin/stdout and TERM other than dumb; use --no-ui for headless operation.",
        ));
    }
    Ok(())
}

fn lock_state(state: &SharedState) -> Result<MutexGuard<'_, state::UiState>, UiError> {
    state
        .lock()
        .map_err(|_| UiError::new("console_state_failed", "The console state is unavailable."))
}

fn set_notice(state: &mut state::UiState, message: &'static str) {
    state.notice = Some(state::Notice {
        message,
        error: None,
    });
    state.mark_dirty();
}

fn make_header(config: &ClientConfig, options: &UiOptions) -> state::Header {
    state::Header {
        profile: options.profile_name.clone(),
        endpoint: view::display_endpoint(config.router_url.as_str()),
        transport: if config.router_url.scheme() == "wss" {
            "TLS WebSocket"
        } else {
            "WebSocket"
        }
        .to_owned(),
        ownership: options.ownership,
        instance_id: options
            .owned_runtime
            .as_ref()
            .map(|runtime| runtime.instance_id),
        // Credential-file claims are editable; only authenticated registration
        // may grant authority. The controller updates this after connection.
        is_admin: false,
    }
}

struct DetachFence(SharedState);

impl Drop for DetachFence {
    fn drop(&mut self) {
        // This also runs if the console future is cancelled or unwinds. A poisoned
        // state still needs its dispatch fence set before the controller is aborted.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .detaching = true;
    }
}

struct ControllerTask(JoinHandle<Result<(), UiError>>);

impl Drop for ControllerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum PrivateJob {
    Printed(Result<(), UiError>),
    Copied {
        invite_id: Uuid,
        result: ClipboardResult,
    },
}

// No token-bearing value is ever placed in UiState or included in diagnostics.
#[derive(Default)]
struct PrivateRuntime {
    prompt: Option<Arc<IssuedPrompt>>,
    jobs: JoinSet<PrivateJob>,
    clipboard: Option<AbortHandle>,
    suspended: bool,
    printing: bool,
    resume_requested: bool,
}

impl PrivateRuntime {
    fn discard_prompt(&mut self) {
        self.prompt = None;
        if let Some(job) = self.clipboard.take() {
            job.abort();
        }
    }

    fn sync_modal(&mut self, state: &state::UiState) {
        if !matches!(state.modal, Some(state::Modal::InviteReady { .. }))
            && !matches!(
                state.suspended_modal,
                Some(state::Modal::InviteReady { .. })
            )
        {
            self.discard_prompt();
        }
    }

    fn accept_invite(&mut self, prompt: IssuedPrompt, state: &mut state::UiState) {
        self.discard_prompt();
        let modal = state::Modal::InviteReady {
            workspace: prompt.workspace.clone(),
            provider: prompt.provider.map(|provider| provider.as_str().to_owned()),
            expires_at: prompt.expires_at,
        };
        self.prompt = Some(Arc::new(prompt));
        state.close_form();
        if matches!(state.modal, Some(state::Modal::Detach { .. })) {
            state.suspended_modal = Some(modal);
        } else {
            state.modal = Some(modal);
        }
        state.mark_dirty();
    }

    fn print_prompt(&mut self, guard: &TerminalGuard) -> Result<(), UiError> {
        let Some(prompt) = self.prompt.as_ref().map(Arc::clone) else {
            return Ok(());
        };
        guard.restore()?;
        self.suspended = true;
        self.printing = true;
        self.resume_requested = false;
        self.jobs.spawn(async move {
            let result = async {
                let mut stdout = tokio::io::stdout();
                stdout.write_all(prompt.text.as_bytes()).await?;
                stdout
                    .write_all(b"\nPress Enter to return to the console.\n")
                    .await?;
                stdout.flush().await
            }
            .await;
            PrivateJob::Printed(result.map_err(|_: io::Error| {
                UiError::new("terminal_output_failed", "Could not print the invitation.")
            }))
        });
        Ok(())
    }

    fn copy_prompt(&mut self, state: &mut state::UiState) {
        if self.clipboard.is_some() {
            set_notice(
                state,
                "Clipboard copy is still pending; p prints the prompt.",
            );
            return;
        }
        let Some(prompt) = self.prompt.as_ref().map(Arc::clone) else {
            return;
        };
        self.clipboard = Some(self.jobs.spawn(async move {
            let result = copy_to_clipboard(&prompt.text).await;
            PrivateJob::Copied {
                invite_id: prompt.invite_id,
                result,
            }
        }));
        set_notice(state, "Copying the invitation to the clipboard.");
    }

    fn resume(
        &mut self,
        terminal: &mut ConsoleTerminal,
        guard: &TerminalGuard,
        state: &SharedState,
    ) -> Result<(), UiError> {
        if !self.suspended {
            return Ok(());
        }
        self.resume_requested = true;
        if self.printing {
            return Ok(());
        }
        guard.resume()?;
        terminal
            .clear()
            .map_err(|_| UiError::new("terminal_draw_failed", "Could not redraw the console."))?;
        self.suspended = false;
        self.resume_requested = false;
        lock_state(state)?.mark_dirty();
        Ok(())
    }

    fn action(
        &mut self,
        action: UiInput,
        commands: &mpsc::Sender<UiCommand>,
        state: &SharedState,
        terminal: &mut ConsoleTerminal,
        guard: &TerminalGuard,
    ) -> Result<Option<UiExit>, UiError> {
        match action {
            UiInput::None => {}
            UiInput::Command(command) => {
                if let Err(error) = commands.try_send(command) {
                    let command = error.into_inner();
                    input::command_not_sent(&mut *lock_state(state)?, &command);
                }
            }
            UiInput::Detach => return Ok(Some(UiExit::Detached)),
            UiInput::PrintPrompt => self.print_prompt(guard)?,
            UiInput::CopyPrompt => self.copy_prompt(&mut *lock_state(state)?),
            UiInput::DiscardPrompt => self.discard_prompt(),
        }
        self.sync_modal(&*lock_state(state)?);
        if self.suspended && matches!(lock_state(state)?.modal, Some(state::Modal::Detach { .. })) {
            self.resume(terminal, guard, state)?;
        }
        Ok(None)
    }
}

enum ClipboardResult {
    Copied,
    Unavailable,
    Failed,
    TimedOut,
}

impl ClipboardResult {
    const fn message(&self) -> &'static str {
        match self {
            Self::Copied => {
                "Invitation copied. The clipboard contains a short-lived token; manage it explicitly."
            }
            Self::Unavailable => {
                "No supported clipboard helper is available; use p to print the prompt."
            }
            Self::Failed => "Clipboard copy failed; use p to print the prompt.",
            Self::TimedOut => "Clipboard copy timed out; use p to print the prompt.",
        }
    }
}

fn clipboard_command() -> Option<Command> {
    if cfg!(target_os = "macos") {
        Some(Command::new("pbcopy"))
    } else if cfg!(target_os = "linux") {
        if std::env::var_os("WAYLAND_DISPLAY").is_some_and(|value| !value.is_empty()) {
            Some(Command::new("wl-copy"))
        } else if std::env::var_os("DISPLAY").is_some_and(|value| !value.is_empty()) {
            let mut command = Command::new("xclip");
            command.args(["-selection", "clipboard"]);
            Some(command)
        } else {
            None
        }
    } else {
        None
    }
}

async fn copy_to_clipboard(prompt: &str) -> ClipboardResult {
    let Some(mut command) = clipboard_command() else {
        return ClipboardResult::Unavailable;
    };
    let mut child = match command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return ClipboardResult::Unavailable;
        }
        Err(_) => return ClipboardResult::Failed,
    };
    let operation = async {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("missing helper stdin"))?;
        stdin.write_all(prompt.as_bytes()).await?;
        stdin.shutdown().await?;
        drop(stdin);
        child.wait().await
    };
    match timeout(CLIPBOARD_DEADLINE, operation).await {
        Ok(Ok(status)) if status.success() => ClipboardResult::Copied,
        Ok(_) => ClipboardResult::Failed,
        Err(_) => {
            let _ = child.start_kill();
            let _ = timeout(Duration::from_millis(250), child.wait()).await;
            ClipboardResult::TimedOut
        }
    }
}

enum ConsoleSignal {
    Interrupt,
    Terminate,
}

#[cfg(unix)]
struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    hangup: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn new() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
            hangup: signal(SignalKind::hangup())?,
        })
    }

    async fn recv(&mut self) -> io::Result<ConsoleSignal> {
        tokio::select! {
            _ = self.interrupt.recv() => Ok(ConsoleSignal::Interrupt),
            _ = self.terminate.recv() => Ok(ConsoleSignal::Terminate),
            _ = self.hangup.recv() => Ok(ConsoleSignal::Terminate),
        }
    }
}

#[cfg(not(unix))]
struct Signals;

#[cfg(not(unix))]
impl Signals {
    fn new() -> io::Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) -> io::Result<ConsoleSignal> {
        tokio::signal::ctrl_c().await?;
        Ok(ConsoleSignal::Interrupt)
    }
}

pub async fn run(config: ClientConfig, options: UiOptions) -> Result<UiExit, UiError> {
    require_terminal()?;
    let mut signals = Signals::new().map_err(|_| {
        UiError::new(
            "signal_failed",
            "Could not register console signal handlers.",
        )
    })?;
    let guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout())).map_err(|_| {
        UiError::new(
            "terminal_init_failed",
            "Could not initialize the console display.",
        )
    })?;
    let size = terminal
        .size()
        .map_err(|_| UiError::new("terminal_size_failed", "Could not read the terminal size."))?;
    let mut initial_state = state::UiState::new(
        make_header(&config, &options),
        std::env::var_os("NO_COLOR").is_some(),
    );
    initial_state.resize(size.width, size.height);
    let state: SharedState = Arc::new(Mutex::new(initial_state));
    let (commands, command_receiver) = mpsc::channel(32);
    let (notice_sender, mut notices) = mpsc::channel(8);
    let mut controller = ControllerTask(tokio::spawn(controller::run(
        config,
        options,
        Arc::clone(&state),
        command_receiver,
        notice_sender,
    )));
    // Declared after the task so unwinding fences dispatch before task abortion.
    let detach_fence = DetachFence(Arc::clone(&state));
    let mut controller_result: Option<Result<(), UiError>> = None;
    let mut controller_finished = false;
    let mut notices_open = true;
    let mut events = EventStream::new();
    let mut frames = interval(FRAME_INTERVAL);
    frames.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut private = PrivateRuntime::default();

    let result = async {
        loop {
            if !notices_open
                && let Some(completed) = controller_result.take()
            {
                break completed.map(|()| UiExit::Detached);
            }
            tokio::select! {
                signal = signals.recv() => {
                    match signal.map_err(|_| UiError::new("signal_failed", "Console signal handling failed."))? {
                        ConsoleSignal::Terminate => break Ok(UiExit::Detached),
                        ConsoleSignal::Interrupt => {
                            let action = input::request_detach(&mut *lock_state(&state)?);
                            if let Some(exit) = private.action(action, &commands, &state, &mut terminal, &guard)? {
                                break Ok(exit);
                            }
                        }
                    }
                }
                event = events.next() => {
                    let event = event.ok_or_else(|| {
                        UiError::new("terminal_closed", "Terminal input closed; the router remains running.")
                    })?.map_err(|_| UiError::new("terminal_input_failed", "Could not read terminal input."))?;
                    if private.suspended {
                        match event {
                            Event::Resize(width, height) => lock_state(&state)?.resize(width, height),
                            Event::Key(key) if key.kind != KeyEventKind::Release && key.code == KeyCode::Enter => {
                                private.resume(&mut terminal, &guard, &state)?;
                            }
                            Event::Key(key)
                                if key.kind != KeyEventKind::Release
                                    && key.code == KeyCode::Char('c')
                                    && key.modifiers.contains(KeyModifiers::CONTROL) =>
                            {
                                let action = input::request_detach(&mut *lock_state(&state)?);
                                if let Some(exit) = private.action(action, &commands, &state, &mut terminal, &guard)? {
                                    break Ok(exit);
                                }
                            }
                            _ => {}
                        }
                    } else {
                        let action = input::handle(&mut *lock_state(&state)?, event);
                        if let Some(exit) = private.action(action, &commands, &state, &mut terminal, &guard)? {
                            break Ok(exit);
                        }
                    }
                }
                notice = notices.recv(), if notices_open => {
                    match notice {
                        Some(UiNotice::Invite(prompt)) => {
                            private.accept_invite(prompt, &mut *lock_state(&state)?);
                        }
                        Some(UiNotice::Exit(exit)) => break Ok(exit),
                        Some(UiNotice::Fatal(error)) => break Err(error),
                        None => notices_open = false,
                    }
                }
                completed = &mut controller.0, if !controller_finished => {
                    controller_finished = true;
                    controller_result = Some(completed.unwrap_or_else(|_| {
                        Err(UiError::new("controller_failed", "The console connection task failed."))
                    }));
                    // Drain any authorized exit/error already sent before considering
                    // a successful task return an ordinary detach.
                    notices.close();
                }
                completed = private.jobs.join_next(), if !private.jobs.is_empty() => {
                    match completed {
                        Some(Ok(PrivateJob::Printed(result))) => {
                            private.printing = false;
                            result?;
                            if private.resume_requested {
                                private.resume(&mut terminal, &guard, &state)?;
                            }
                        }
                        Some(Ok(PrivateJob::Copied { invite_id, result })) => {
                            if private.prompt.as_ref().is_some_and(|prompt| prompt.invite_id == invite_id) {
                                private.clipboard = None;
                                set_notice(&mut *lock_state(&state)?, result.message());
                            }
                        }
                        Some(Err(error)) if !error.is_cancelled() => {
                            break Err(UiError::new("console_worker_failed", "A console output worker failed."));
                        }
                        _ => {}
                    }
                }
                _ = frames.tick() => {
                    let mut state = lock_state(&state)?;
                    private.sync_modal(&state);
                    if state.render.dirty && !private.suspended {
                        terminal.draw(|frame| view::render(frame, &state)).map_err(|_| {
                            UiError::new("terminal_draw_failed", "Could not draw the console.")
                        })?;
                        state.render.dirty = false;
                        frames.reset();
                    }
                }
            }
        }
    }.await;

    // Closing the channel asks the owner of the real client to close its socket
    // and cancel/await its workers; q and signals never invoke server shutdown.
    drop(detach_fence);
    drop(commands);
    drop(events);
    notices.close();
    private.discard_prompt();
    private.jobs.abort_all();
    let restoration = guard.restore();
    private.jobs.shutdown().await;
    if !controller_finished && timeout(SHUTDOWN_GRACE, &mut controller.0).await.is_err() {
        controller.0.abort();
        let _ = (&mut controller.0).await;
    }
    drop(terminal);
    drop(guard);
    // In particular, never authorize a server stop before successful restoration.
    restoration?;
    result
}
