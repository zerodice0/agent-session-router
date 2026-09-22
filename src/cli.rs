use std::{
    ffi::OsString,
    fmt,
    io::{self, Read, Write},
    path::PathBuf,
};

use crate::mcp::McpRole;
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde_json::{Value, json};
use uuid::Uuid;

pub const MAX_STDIN_BYTES: usize = 128 * 1024;
pub const MAX_SHARED_STDIN_BYTES: usize = 64 * 1024;
pub const MAX_NOTE_BYTES: usize = 16 * 1024;
pub const MAX_HANDOFF_NOTE_BYTES: usize = 4 * 1024;
pub const MAX_OUTPUT_PAGE_BYTES: usize = 256 * 1024;
pub const DEFAULT_PAGE_LIMIT: u16 = 50;
pub const MAX_PAGE_LIMIT: u16 = 100;

#[derive(Clone, Debug, Parser)]
#[command(
    name = "asr",
    version,
    about = "Agent Session Router",
    subcommand_negates_reqs = true
)]
pub struct Cli {
    /// Select a saved router profile. Must precede the command.
    #[arg(long)]
    pub profile: Option<String>,
    /// Read the router credential from this file. Must precede the command.
    #[arg(long)]
    pub credential: Option<PathBuf>,
    /// Describe the operation without reading stdin or touching external state.
    #[arg(long)]
    pub dry_run: bool,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    Router(RouterArgs),
    /// Open the console for a running router without starting one.
    Ui(UiArgs),
    Codex(ProviderArgs),
    #[command(name = "codex-cli")]
    CodexCli(CodexCliArgs),
    Claude(ClaudeArgs),
    Omp(OmpArgs),
    #[command(hide = true)]
    Mcp(McpArgs),
    Gateway(GatewayArgs),
    #[command(name = "setup-claude")]
    SetupClaude,
    #[command(name = "setup-omp")]
    SetupOmp,
    Doctor,
    Install(InstallArgs),
    Profile(ProfileArgs),
    Workspace(WorkspaceArgs),
    Credential(CredentialArgs),
    Onboarding(OnboardingArgs),
    Task(TaskArgs),
    Integration(IntegrationArgs),
    Smoke(SmokeArgs),
}

#[derive(Clone, Debug, Args)]
pub struct OnboardingArgs {
    #[command(subcommand)]
    pub command: OnboardingCommand,
}

#[derive(Clone, Debug, Subcommand)]
pub enum OnboardingCommand {
    Prompt(OnboardingPromptArgs),
    Revoke {
        invite_id: Uuid,
    },
    Install {
        #[arg(long, value_enum)]
        provider: crate::onboarding::OnboardingProvider,
        #[arg(long, required = true)]
        stdin: bool,
    },
    Resume {
        invite_id: Uuid,
        #[arg(long, value_enum)]
        provider: crate::onboarding::OnboardingProvider,
    },
    Status {
        #[arg(long, value_enum)]
        provider: crate::onboarding::OnboardingProvider,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Debug, Args)]
pub struct OnboardingPromptArgs {
    #[arg(long)]
    pub workspace: String,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub create_workspace: bool,
    #[arg(long, value_enum)]
    pub provider: Option<crate::onboarding::OnboardingProvider>,
    #[arg(long = "endpoint", value_name = "KIND=URL")]
    pub endpoints: Vec<String>,
    #[arg(long)]
    pub ca_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Args)]
pub struct McpArgs {
    #[command(subcommand)]
    pub role: McpRoleArg,
}

#[derive(Clone, Debug, Subcommand)]
pub enum McpRoleArg {
    Delegate {
        #[arg(long, value_name = "PATH")]
        context_file: PathBuf,
    },
    #[command(name = "codex-cli")]
    CodexCli,
    #[command(name = "claude-channel")]
    ClaudeChannel,
    Omp,
}

impl McpRoleArg {
    #[must_use]
    pub const fn mcp_role(&self) -> McpRole {
        match self {
            Self::Delegate { .. } => McpRole::Delegate,
            Self::CodexCli => McpRole::CodexCli,
            Self::ClaudeChannel => McpRole::ClaudeChannel,
            Self::Omp => McpRole::Omp,
        }
    }

    #[must_use]
    pub const fn command_name(&self) -> &'static str {
        match self {
            Self::Delegate { .. } => "delegate",
            Self::CodexCli => "codex-cli",
            Self::ClaudeChannel => "claude-channel",
            Self::Omp => "omp",
        }
    }

    #[must_use]
    pub const fn operation_name(&self) -> &'static str {
        match self {
            Self::Delegate { .. } => "mcp.delegate",
            Self::CodexCli => "mcp.codex-cli",
            Self::ClaudeChannel => "mcp.claude-channel",
            Self::Omp => "mcp.omp",
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct RouterArgs {
    #[arg(value_enum, default_value_t = RouterAction::Start)]
    pub action: RouterAction,
    #[arg(long)]
    pub background: bool,
    /// Keep the router in the foreground without opening the console.
    #[arg(long, conflicts_with = "background")]
    pub no_ui: bool,
    #[arg(long, value_enum, num_args = 0..=1, default_missing_value = "auto")]
    pub share: Option<ShareMode>,
}

#[derive(Clone, Debug, Args)]
pub struct UiArgs {
    /// Select the initial workspace.
    #[arg(long)]
    pub workspace: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum RouterAction {
    Start,
    Stop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ShareMode {
    Auto,
    Tailscale,
    Lan,
}

#[derive(Clone, Debug, Args)]
pub struct ProviderArgs {
    pub agent: Option<String>,
    #[arg(long)]
    pub activity: Option<String>,
    #[arg(long)]
    pub workspace: Option<String>,
}

#[derive(Clone, Debug, Args)]
pub struct CodexCliArgs {
    pub agent: Option<String>,
    #[arg(long)]
    pub activity: Option<String>,
    #[arg(long)]
    pub workspace: Option<String>,
    #[arg(last = true, allow_hyphen_values = true)]
    pub arguments: Vec<OsString>,
}

#[derive(Clone, Debug, Args)]
pub struct OmpArgs {
    pub agent: Option<String>,
    #[arg(long)]
    pub workspace: Option<String>,
    #[arg(last = true, allow_hyphen_values = true)]
    pub arguments: Vec<OsString>,
}

#[derive(Clone, Debug, Args)]
pub struct ClaudeArgs {
    pub agent: Option<String>,
    #[arg(long)]
    pub activity: Option<String>,
    #[arg(long)]
    pub workspace: Option<String>,
    #[arg(long)]
    pub auto: bool,
    #[arg(long)]
    pub resume: Option<String>,
}

#[derive(Clone, Debug, Args)]
pub struct GatewayArgs {
    #[command(subcommand)]
    pub command: GatewayCommand,
}

#[derive(Clone, Debug, Subcommand)]
pub enum GatewayCommand {
    Claude(GatewayProviderArgs),
    Codex(GatewayProviderArgs),
}

#[derive(Clone, Debug, Args)]
pub struct GatewayProviderArgs {
    pub agent: Option<String>,
    #[arg(long)]
    pub workspace: Option<String>,
}

#[derive(Clone, Debug, Args)]
pub struct InstallArgs {
    #[arg(long)]
    pub bin_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, Args)]
pub struct ProfileArgs {
    #[command(subcommand)]
    pub command: ProfileCommand,
}

#[derive(Clone, Debug, Subcommand)]
pub enum ProfileCommand {
    List,
    Add {
        name: String,
        address: String,
        #[arg(long)]
        force: bool,
    },
    Use {
        name: String,
    },
}

#[derive(Clone, Debug, Args)]
pub struct WorkspaceArgs {
    #[command(subcommand)]
    pub command: WorkspaceCommand,
}

#[derive(Clone, Debug, Subcommand)]
pub enum WorkspaceCommand {
    Create {
        name: String,
    },
    List {
        #[arg(long)]
        json: bool,
    },
    Members {
        name: String,
        #[arg(long)]
        json: bool,
    },
    History {
        name: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = DEFAULT_PAGE_LIMIT)]
        limit: u16,
        #[arg(long)]
        json: bool,
    },
    Watch {
        name: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long)]
        json: bool,
    },
    Post {
        name: String,
        #[arg(long, required = true)]
        stdin: bool,
    },
    Send {
        name: String,
        target: String,
        #[arg(long, required = true)]
        stdin: bool,
        #[arg(long)]
        timeout_ms: Option<u64>,
    },
    Join {
        name: String,
    },
}

#[derive(Clone, Debug, Args)]
pub struct CredentialArgs {
    #[command(subcommand)]
    pub command: CredentialCommand,
}

#[derive(Clone, Debug, Subcommand)]
pub enum CredentialCommand {
    Issue(CredentialIssueArgs),
    List {
        #[arg(long)]
        json: bool,
    },
    Revoke {
        id: String,
    },
}

#[derive(Clone, Debug, Args)]
pub struct CredentialIssueArgs {
    #[arg(long, required_unless_present = "operator")]
    pub agent: Option<String>,
    #[arg(long, value_enum, required_unless_present = "operator")]
    pub side: Option<AgentSide>,
    #[arg(long, value_enum, required_unless_present = "operator")]
    pub client: Option<AgentClient>,
    #[arg(long, required_unless_present = "operator")]
    pub workspace: Vec<String>,
    #[arg(long, required_unless_present = "operator")]
    pub output: Option<PathBuf>,
    #[arg(long, conflicts_with_all = ["agent", "side", "client", "workspace", "output"])]
    pub operator: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum AgentSide {
    Claude,
    Codex,
    Generic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum AgentClient {
    Omp,
    #[value(name = "claude-code")]
    ClaudeCode,
    #[value(name = "claude-sdk")]
    ClaudeSdk,
    #[value(name = "codex-cli")]
    CodexCli,
    #[value(name = "codex-app-server")]
    CodexAppServer,
    Generic,
}

#[derive(Clone, Debug, Args)]
pub struct TaskArgs {
    #[command(subcommand)]
    pub command: TaskCommand,
}

#[derive(Clone, Debug, Subcommand)]
pub enum TaskCommand {
    List(TaskListArgs),
    Show(TaskIdentityArgs),
    History(TaskHistoryArgs),
    Watch(TaskWatchArgs),
    Create(TaskCreateArgs),
    Edit(TaskEditArgs),
    Assign(TaskAssignArgs),
    Note(TaskNoteArgs),
    Cancel(TaskTransitionArgs),
    Reopen(TaskTransitionArgs),
    Interrupt(TaskTransitionArgs),
    #[command(name = "confirm-stopped")]
    ConfirmStopped(TaskConfirmStoppedArgs),
    Request(TaskRequestArgs),
    Import(TaskImportArgs),
    Link(TaskLinkArgs),
    Publish(TaskPublishArgs),
    #[command(name = "external-status")]
    ExternalStatus(TaskExternalStatusArgs),
    #[command(name = "external-resolve")]
    ExternalResolve(TaskExternalResolveArgs),
}

#[derive(Clone, Debug, Args)]
pub struct TaskListArgs {
    pub room: String,
    #[arg(long, conflicts_with = "all")]
    pub state: Vec<String>,
    #[arg(long)]
    pub all: bool,
    #[arg(long)]
    pub assignee: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub after: i64,
    #[arg(long, default_value_t = DEFAULT_PAGE_LIMIT)]
    pub limit: u16,
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug, Args)]
pub struct TaskIdentityArgs {
    pub room: String,
    pub id: i64,
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug, Args)]
pub struct TaskHistoryArgs {
    pub room: String,
    pub id: i64,
    #[arg(long, default_value_t = 0)]
    pub after: i64,
    #[arg(long, default_value_t = DEFAULT_PAGE_LIMIT)]
    pub limit: u16,
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug, Args)]
pub struct TaskWatchArgs {
    pub room: String,
    #[arg(long)]
    pub task: Option<i64>,
    #[arg(long, default_value_t = 0)]
    pub after: i64,
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug, Args)]
pub struct TaskCreateArgs {
    pub room: String,
    #[arg(long, required = true)]
    pub stdin: bool,
    #[command(flatten)]
    pub mutation: MutationArgs,
}

#[derive(Clone, Debug, Args)]
pub struct TaskEditArgs {
    pub room: String,
    pub id: i64,
    #[arg(long)]
    pub expected_version: i64,
    #[arg(long, required = true)]
    pub stdin: bool,
    #[command(flatten)]
    pub mutation: MutationArgs,
}

#[derive(Clone, Debug, Args)]
pub struct TaskAssignArgs {
    pub room: String,
    pub id: i64,
    #[arg(long, conflicts_with = "unassigned")]
    pub agent: Option<String>,
    #[arg(long, conflicts_with = "agent")]
    pub unassigned: bool,
    #[arg(long)]
    pub expected_version: i64,
    #[command(flatten)]
    pub mutation: MutationArgs,
}

#[derive(Clone, Debug, Args)]
pub struct TaskNoteArgs {
    pub room: String,
    pub id: i64,
    #[arg(long, required = true)]
    pub stdin: bool,
    #[command(flatten)]
    pub mutation: MutationArgs,
}

#[derive(Clone, Debug, Args)]
pub struct TaskTransitionArgs {
    pub room: String,
    pub id: i64,
    #[arg(long)]
    pub expected_version: i64,
    #[arg(long, required = true)]
    pub stdin: bool,
    #[command(flatten)]
    pub mutation: MutationArgs,
}

#[derive(Clone, Debug, Args)]
pub struct TaskConfirmStoppedArgs {
    pub room: String,
    pub id: i64,
    #[arg(long)]
    pub attempt: Uuid,
    #[arg(long)]
    pub expected_version: i64,
    #[arg(long, required = true)]
    pub stdin: bool,
    #[command(flatten)]
    pub mutation: MutationArgs,
}

#[derive(Clone, Debug, Args)]
pub struct TaskRequestArgs {
    pub room: String,
    pub id: i64,
    #[arg(long)]
    pub expected_version: i64,
    #[arg(long)]
    pub timeout_ms: Option<u64>,
    #[arg(long)]
    pub stdin: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug, Args)]
pub struct TaskImportArgs {
    pub room: String,
    #[arg(value_enum)]
    pub provider: ExternalProvider,
    pub external_id: String,
    #[command(flatten)]
    pub mutation: MutationArgs,
}

#[derive(Clone, Debug, Args)]
pub struct TaskLinkArgs {
    pub room: String,
    pub id: i64,
    #[arg(value_enum)]
    pub provider: ExternalProvider,
    pub external_id: String,
    #[arg(long)]
    pub expected_version: i64,
    #[arg(long)]
    pub replace: bool,
    #[command(flatten)]
    pub mutation: MutationArgs,
}

#[derive(Clone, Debug, Args)]
pub struct TaskPublishArgs {
    pub room: String,
    pub id: i64,
    #[arg(value_enum)]
    pub provider: ExternalProvider,
    #[arg(long, value_enum)]
    pub kind: PublishKind,
    #[arg(long)]
    pub expected_version: i64,
    #[arg(long)]
    pub report: Option<Uuid>,
    #[command(flatten)]
    pub mutation: MutationArgs,
}

#[derive(Clone, Debug, Args)]
pub struct TaskExternalStatusArgs {
    pub room: String,
    pub operation_id: Uuid,
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug, Args)]
pub struct TaskExternalResolveArgs {
    pub room: String,
    pub operation_id: Uuid,
    #[command(flatten)]
    pub resolution: ExternalResolutionArgs,
    #[arg(long, required = true)]
    pub stdin: bool,
    #[arg(long)]
    pub resolution_id: Option<Uuid>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug, Args)]
pub struct ExternalResolutionArgs {
    #[arg(long, requires = "external_id", conflicts_with = "not_applied")]
    pub applied: bool,
    #[arg(long, requires = "applied")]
    pub external_id: Option<String>,
    #[arg(long, conflicts_with = "applied")]
    pub not_applied: bool,
}

#[derive(Clone, Debug, Args)]
pub struct MutationArgs {
    #[arg(long)]
    pub operation_id: Option<Uuid>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ExternalProvider {
    Github,
    Linear,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum PublishKind {
    Issue,
    Report,
}

#[derive(Clone, Debug, Args)]
pub struct IntegrationArgs {
    #[command(subcommand)]
    pub command: IntegrationCommand,
}

#[derive(Clone, Debug, Subcommand)]
pub enum IntegrationCommand {
    List {
        room: String,
        #[arg(long)]
        json: bool,
    },
    Check {
        room: String,
        #[arg(value_enum)]
        provider: ExternalProvider,
        #[arg(long)]
        json: bool,
    },
    Admin(IntegrationAdminArgs),
}

#[derive(Clone, Debug, Args)]
pub struct IntegrationAdminArgs {
    #[command(subcommand)]
    pub command: IntegrationAdminCommand,
}

#[derive(Clone, Debug, Subcommand)]
pub enum IntegrationAdminCommand {
    Reload {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Debug, Args)]
pub struct SmokeArgs {
    #[arg(long)]
    pub workspace: String,
    #[arg(long)]
    pub target: String,
    #[arg(long)]
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StdinPayload {
    Text(String),
    Json(Value),
}

#[derive(Clone, Debug)]
pub struct OutputItem {
    pub human: String,
    pub json: Value,
}

#[derive(Clone, Debug)]
pub struct OutputPage {
    pub items: Vec<OutputItem>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Clone, Debug)]
pub struct TaskPage {
    pub tasks: Vec<TaskRow>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskRunIdentity {
    pub executor: String,
    pub attempt_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopEvidence {
    None,
    RunningUnknown,
    InterruptedUnknown,
    Released,
    Confirmed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskRow {
    pub id: i64,
    pub state: String,
    pub current: Option<TaskRunIdentity>,
    pub last: Option<TaskRunIdentity>,
    pub last_executor_id: Option<String>,
    pub execution_session_id: Option<Uuid>,
    pub assigned_agent_id: Option<String>,
    pub checkpoint: Option<String>,
    pub stop_evidence: StopEvidence,
    pub title: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Usage,
    Runtime,
    Interrupted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliError {
    pub kind: ErrorKind,
    pub code: &'static str,
    pub message: String,
    pub operation_id: Option<Uuid>,
    pub resolution_id: Option<Uuid>,
}

impl CliError {
    #[must_use]
    pub fn usage(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Usage,
            code,
            message: message.into(),
            operation_id: None,
            resolution_id: None,
        }
    }

    #[must_use]
    pub fn runtime(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Runtime,
            code,
            message: message.into(),
            operation_id: None,
            resolution_id: None,
        }
    }
    #[must_use]
    pub fn interrupted(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Interrupted,
            code: "interrupted",
            message: message.into(),
            operation_id: None,
            resolution_id: None,
        }
    }

    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        match self.kind {
            ErrorKind::Usage => 2,
            ErrorKind::Runtime => 1,
            ErrorKind::Interrupted => 130,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}",
            self.code,
            escape_terminal(&self.message)
        )?;
        if let Some(operation_id) = self.operation_id {
            write!(formatter, " (operationId: {operation_id})")?;
        }
        if let Some(resolution_id) = self.resolution_id {
            write!(formatter, " (resolutionId: {resolution_id})")?;
        }
        Ok(())
    }
}

impl std::error::Error for CliError {}

pub trait OperationIdSource {
    fn next_operation_id(&mut self) -> Uuid;
}

#[derive(Default)]
pub struct RandomOperationIds;

impl OperationIdSource for RandomOperationIds {
    fn next_operation_id(&mut self) -> Uuid {
        Uuid::new_v4()
    }
}

/// Check authority-changing globals before reading stdin, including in dry runs.
pub fn validate_globals(cli: &Cli) -> Result<(), CliError> {
    match &cli.command {
        Some(Command::Onboarding(OnboardingArgs {
            command: OnboardingCommand::Install { .. } | OnboardingCommand::Resume { .. },
        })) if cli.profile.is_some() || cli.credential.is_some() => Err(CliError::usage(
            "onboarding_override_forbidden",
            "install and resume use the invitation authority; omit --profile and --credential",
        )),
        Some(Command::Onboarding(OnboardingArgs {
            command: OnboardingCommand::Status { .. },
        })) if cli.profile.is_none() || cli.credential.is_some() => Err(CliError::usage(
            "onboarding_profile_required",
            "status requires --profile NAME before onboarding and does not accept --credential",
        )),
        _ => Ok(()),
    }
}

pub fn validate_command(command: &Command) -> Result<(), CliError> {
    match command {
        Command::Onboarding(OnboardingArgs {
            command: OnboardingCommand::Resume { invite_id, .. },
        }) if invite_id.is_nil() => {
            return Err(CliError::usage(
                "invalid_invite_id",
                "invite ID must not be nil",
            ));
        }
        Command::Router(args)
            if args.action == RouterAction::Stop
                && (args.share.is_some() || args.background || args.no_ui) =>
        {
            return Err(CliError::usage(
                "invalid_arguments",
                "router stop does not accept --share, --background, or --no-ui",
            ));
        }
        Command::Router(args) if args.no_ui && args.background => {
            return Err(CliError::usage(
                "invalid_arguments",
                "--no-ui conflicts with --background",
            ));
        }
        Command::Ui(args) => {
            if let Some(workspace) = &args.workspace {
                crate::config::validate_workspace_argument(workspace).map_err(|_| {
                    CliError::usage("invalid_workspace", "invalid initial workspace name")
                })?;
            }
        }
        Command::Workspace(WorkspaceArgs {
            command: WorkspaceCommand::History { after, limit, .. },
        })
        | Command::Task(TaskArgs {
            command:
                TaskCommand::List(TaskListArgs { after, limit, .. })
                | TaskCommand::History(TaskHistoryArgs { after, limit, .. }),
        }) if *after < 0 || *limit == 0 || *limit > MAX_PAGE_LIMIT => {
            return Err(CliError::usage(
                "invalid_page",
                format!("after must be nonnegative and limit must be 1..={MAX_PAGE_LIMIT}"),
            ));
        }
        Command::Workspace(WorkspaceArgs {
            command: WorkspaceCommand::Watch { after, .. },
        })
        | Command::Task(TaskArgs {
            command: TaskCommand::Watch(TaskWatchArgs { after, .. }),
        }) if *after < 0 => {
            return Err(CliError::usage("invalid_page", "after must be nonnegative"));
        }
        Command::Task(TaskArgs {
            command: TaskCommand::List(args),
        }) if args.state.iter().any(|state| {
            !matches!(
                state.as_str(),
                "todo" | "in_progress" | "blocked" | "paused" | "done" | "cancelled"
            )
        }) =>
        {
            return Err(CliError::usage(
                "invalid_task_state",
                "--state must be todo, in_progress, blocked, paused, done, or cancelled",
            ));
        }
        Command::Task(TaskArgs {
            command: TaskCommand::Assign(args),
        }) if args.agent.is_some() == args.unassigned => {
            return Err(CliError::usage(
                "invalid_assignment",
                "exactly one of --agent or --unassigned is required",
            ));
        }
        Command::Task(TaskArgs {
            command: TaskCommand::ExternalResolve(args),
        }) if args.resolution.applied == args.resolution.not_applied => {
            return Err(CliError::usage(
                "invalid_resolution",
                "exactly one of --applied or --not-applied is required",
            ));
        }
        Command::Task(TaskArgs {
            command: TaskCommand::Publish(args),
        }) if (args.kind == PublishKind::Report) != args.report.is_some() => {
            return Err(CliError::usage(
                "invalid_publish",
                "--report is required only when --kind=report",
            ));
        }
        _ => {}
    }
    Ok(())
}

pub fn read_command_stdin(
    command: &Command,
    stdin: &mut dyn Read,
) -> Result<Option<StdinPayload>, CliError> {
    let mode = match command {
        Command::Workspace(WorkspaceArgs {
            command: WorkspaceCommand::Post { .. } | WorkspaceCommand::Send { .. },
        }) => Some(InputMode::Text(MAX_SHARED_STDIN_BYTES)),
        Command::Task(TaskArgs {
            command: TaskCommand::Create(_),
        }) => Some(InputMode::TaskCreate),
        Command::Task(TaskArgs {
            command: TaskCommand::Edit(_),
        }) => Some(InputMode::TaskEdit),
        Command::Task(TaskArgs {
            command: TaskCommand::Note(_),
        }) => Some(InputMode::Text(MAX_NOTE_BYTES)),
        Command::Task(TaskArgs {
            command:
                TaskCommand::Cancel(_)
                | TaskCommand::Reopen(_)
                | TaskCommand::Interrupt(_)
                | TaskCommand::ConfirmStopped(_)
                | TaskCommand::ExternalResolve(_),
        }) => Some(InputMode::Text(MAX_HANDOFF_NOTE_BYTES)),
        Command::Task(TaskArgs {
            command: TaskCommand::Request(args),
        }) if args.stdin => Some(InputMode::Text(MAX_SHARED_STDIN_BYTES)),
        _ => None,
    };
    mode.map(|mode| read_input(stdin, mode)).transpose()
}

#[derive(Clone, Copy)]
enum InputMode {
    Text(usize),
    TaskCreate,
    TaskEdit,
}

fn read_input(reader: &mut dyn Read, mode: InputMode) -> Result<StdinPayload, CliError> {
    let cap = match mode {
        InputMode::Text(cap) => cap,
        InputMode::TaskCreate | InputMode::TaskEdit => MAX_STDIN_BYTES,
    };
    let bytes = read_bounded(reader, cap)?;
    let text = String::from_utf8(bytes)
        .map_err(|_| CliError::usage("invalid_stdin", "stdin must be UTF-8"))?;
    match mode {
        InputMode::Text(_) => {
            if text.is_empty() {
                return Err(CliError::usage("invalid_stdin", "stdin must not be empty"));
            }
            Ok(StdinPayload::Text(text))
        }
        InputMode::TaskCreate => {
            let value = parse_task_object(&text, false)?;
            Ok(StdinPayload::Json(value))
        }
        InputMode::TaskEdit => {
            let value = parse_task_object(&text, true)?;
            Ok(StdinPayload::Json(value))
        }
    }
}

pub fn read_bounded(reader: &mut dyn Read, cap: usize) -> Result<Vec<u8>, CliError> {
    let mut limited = reader.take((cap as u64) + 1);
    let mut bytes = Vec::with_capacity(cap.min(8192));
    limited
        .read_to_end(&mut bytes)
        .map_err(|error| CliError::runtime("stdin_read_failed", error.to_string()))?;
    if bytes.len() > cap {
        return Err(CliError::usage(
            "stdin_too_large",
            format!("stdin exceeds {cap} bytes"),
        ));
    }
    Ok(bytes)
}

fn parse_task_object(text: &str, edit: bool) -> Result<Value, CliError> {
    let value: Value = serde_json::from_str(text)
        .map_err(|_| CliError::usage("invalid_stdin", "stdin must be a JSON object"))?;
    let object = value
        .as_object()
        .ok_or_else(|| CliError::usage("invalid_stdin", "stdin must be a JSON object"))?;
    let allowed = ["title", "description"];
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(CliError::usage(
            "invalid_stdin",
            "task input contains an unknown field",
        ));
    }
    if !edit
        && (object.len() != 2
            || !object.contains_key("title")
            || !object.contains_key("description"))
    {
        return Err(CliError::usage(
            "invalid_stdin",
            "task create requires title and description",
        ));
    }
    if edit && object.is_empty() {
        return Err(CliError::usage(
            "invalid_stdin",
            "task edit requires title or description",
        ));
    }
    if let Some(title) = object.get("title") {
        let title = title
            .as_str()
            .ok_or_else(|| CliError::usage("invalid_stdin", "title must be a string"))?;
        if title.is_empty() || title.len() > 1024 || title.chars().count() > 256 {
            return Err(CliError::usage(
                "invalid_stdin",
                "title must contain 1..=256 characters and at most 1024 bytes",
            ));
        }
    }
    if let Some(description) = object.get("description") {
        let description = description
            .as_str()
            .ok_or_else(|| CliError::usage("invalid_stdin", "description must be a string"))?;
        if description.len() > MAX_SHARED_STDIN_BYTES {
            return Err(CliError::usage(
                "invalid_stdin",
                "description exceeds 65536 bytes",
            ));
        }
    }
    Ok(value)
}

#[must_use]
pub fn command_operation_id(command: &Command) -> Option<Uuid> {
    match command {
        Command::Task(TaskArgs { command }) => match command {
            TaskCommand::Create(args) => args.mutation.operation_id,
            TaskCommand::Edit(args) => args.mutation.operation_id,
            TaskCommand::Assign(args) => args.mutation.operation_id,
            TaskCommand::Note(args) => args.mutation.operation_id,
            TaskCommand::Cancel(args)
            | TaskCommand::Reopen(args)
            | TaskCommand::Interrupt(args) => args.mutation.operation_id,
            TaskCommand::ConfirmStopped(args) => args.mutation.operation_id,
            TaskCommand::Import(args) => args.mutation.operation_id,
            TaskCommand::Link(args) => args.mutation.operation_id,
            TaskCommand::Publish(args) => args.mutation.operation_id,
            _ => None,
        },
        _ => None,
    }
}

#[must_use]
pub fn command_needs_operation_id(command: &Command) -> bool {
    matches!(
        command,
        Command::Task(TaskArgs {
            command: TaskCommand::Create(_)
                | TaskCommand::Edit(_)
                | TaskCommand::Assign(_)
                | TaskCommand::Note(_)
                | TaskCommand::Cancel(_)
                | TaskCommand::Reopen(_)
                | TaskCommand::Interrupt(_)
                | TaskCommand::ConfirmStopped(_)
                | TaskCommand::Import(_)
                | TaskCommand::Link(_)
                | TaskCommand::Publish(_)
        })
    )
}

#[must_use]
pub fn command_uses_json(command: &Command) -> bool {
    match command {
        Command::Onboarding(OnboardingArgs { command }) => match command {
            OnboardingCommand::Install { .. } | OnboardingCommand::Resume { .. } => true,
            OnboardingCommand::Status { json, .. } => *json,
            _ => false,
        },
        Command::Workspace(WorkspaceArgs {
            command:
                WorkspaceCommand::List { json }
                | WorkspaceCommand::Members { json, .. }
                | WorkspaceCommand::History { json, .. }
                | WorkspaceCommand::Watch { json, .. },
        }) => *json,
        Command::Credential(CredentialArgs { command }) => {
            matches!(command, CredentialCommand::List { json: true })
        }
        Command::Task(TaskArgs { command }) => match command {
            TaskCommand::List(args) => args.json,
            TaskCommand::Show(args) => args.json,
            TaskCommand::History(args) => args.json,
            TaskCommand::Watch(args) => args.json,
            TaskCommand::Create(args) => args.mutation.json,
            TaskCommand::Edit(args) => args.mutation.json,
            TaskCommand::Assign(args) => args.mutation.json,
            TaskCommand::Note(args) => args.mutation.json,
            TaskCommand::Cancel(args)
            | TaskCommand::Reopen(args)
            | TaskCommand::Interrupt(args) => args.mutation.json,
            TaskCommand::ConfirmStopped(args) => args.mutation.json,
            TaskCommand::Request(args) => args.json,
            TaskCommand::Import(args) => args.mutation.json,
            TaskCommand::Link(args) => args.mutation.json,
            TaskCommand::Publish(args) => args.mutation.json,
            TaskCommand::ExternalStatus(args) => args.json,
            TaskCommand::ExternalResolve(args) => args.json,
        },
        Command::Integration(IntegrationArgs { command }) => match command {
            IntegrationCommand::List { json, .. }
            | IntegrationCommand::Check { json, .. }
            | IntegrationCommand::Admin(IntegrationAdminArgs {
                command: IntegrationAdminCommand::Reload { json },
            }) => *json,
        },
        _ => false,
    }
}
#[must_use]
pub fn command_resolution_id(command: &Command) -> Option<Uuid> {
    match command {
        Command::Task(TaskArgs {
            command: TaskCommand::ExternalResolve(args),
        }) => args.resolution_id,
        _ => None,
    }
}

#[must_use]
pub fn command_needs_resolution_id(command: &Command) -> bool {
    matches!(
        command,
        Command::Task(TaskArgs {
            command: TaskCommand::ExternalResolve(_),
        })
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DryRunPlan {
    pub command: &'static str,
    pub room: Option<String>,
    pub task_id: Option<i64>,
    pub url: Option<String>,
    pub operation_id: Option<Uuid>,
    pub resolution_id: Option<Uuid>,
}

pub fn plan_dry_run(
    cli: &Cli,
    operation_ids: &mut dyn OperationIdSource,
) -> Result<DryRunPlan, CliError> {
    if !cli.dry_run {
        return Err(CliError::usage(
            "dry_run_required",
            "dry-run planning requires --dry-run",
        ));
    }
    let command = cli
        .command
        .as_ref()
        .ok_or_else(|| CliError::usage("command_required", "--dry-run requires a command"))?;
    validate_command(command)?;
    validate_globals(cli)?;
    let explicit_operation_id = command_operation_id(command);
    let operation_id = command_needs_operation_id(command)
        .then(|| explicit_operation_id.unwrap_or_else(|| operation_ids.next_operation_id()));
    let explicit_resolution_id = command_resolution_id(command);
    let resolution_id = command_needs_resolution_id(command)
        .then(|| explicit_resolution_id.unwrap_or_else(|| operation_ids.next_operation_id()));
    let (name, room, task_id) = command_identity(command);
    let url = match command {
        Command::Profile(ProfileArgs {
            command: ProfileCommand::Add { address, .. },
        }) => Some(address.clone()),
        _ => None,
    };
    Ok(DryRunPlan {
        command: name,
        room: room.map(str::to_owned),
        task_id,
        url,
        operation_id,
        resolution_id,
    })
}

pub fn write_dry_run_plan(output: &mut dyn Write, plan: &DryRunPlan) -> Result<(), CliError> {
    write_json_line(
        output,
        &json!({
            "dryRun": true,
            "command": plan.command,
            "room": plan.room,
            "taskId": plan.task_id,
            "url": plan.url,
            "operationId": plan.operation_id,
            "resolutionId": plan.resolution_id,
            "sensitiveInput": "[redacted]"
        }),
    )
}

fn command_identity(command: &Command) -> (&'static str, Option<&str>, Option<i64>) {
    match command {
        Command::Workspace(WorkspaceArgs { command }) => match command {
            WorkspaceCommand::Create { name } => ("workspace.create", Some(name), None),
            WorkspaceCommand::List { .. } => ("workspace.list", None, None),
            WorkspaceCommand::Members { name, .. } => ("workspace.members", Some(name), None),
            WorkspaceCommand::History { name, .. } => ("workspace.history", Some(name), None),
            WorkspaceCommand::Watch { name, .. } => ("workspace.watch", Some(name), None),
            WorkspaceCommand::Post { name, .. } => ("workspace.post", Some(name), None),
            WorkspaceCommand::Send { name, .. } => ("workspace.send", Some(name), None),
            WorkspaceCommand::Join { name } => ("workspace.join", Some(name), None),
        },
        Command::Task(TaskArgs { command }) => task_identity(command),
        Command::Router(_) => ("router", None, None),
        Command::Ui(args) => ("ui", args.workspace.as_deref(), None),
        Command::Codex(_) => ("codex", None, None),
        Command::CodexCli(_) => ("codex-cli", None, None),
        Command::Claude(_) => ("claude", None, None),
        Command::Omp(_) => ("omp", None, None),
        Command::Mcp(args) => (args.role.operation_name(), None, None),
        Command::Gateway(_) => ("gateway", None, None),
        Command::SetupClaude => ("setup-claude", None, None),
        Command::SetupOmp => ("setup-omp", None, None),
        Command::Doctor => ("doctor", None, None),
        Command::Install(_) => ("install", None, None),
        Command::Profile(_) => ("profile", None, None),
        Command::Credential(_) => ("credential", None, None),
        Command::Onboarding(OnboardingArgs {
            command: OnboardingCommand::Revoke { .. },
        }) => ("onboarding.revoke", None, None),
        Command::Onboarding(OnboardingArgs {
            command: OnboardingCommand::Prompt(args),
        }) => ("onboarding.prompt", Some(&args.workspace), None),
        Command::Onboarding(OnboardingArgs {
            command: OnboardingCommand::Install { .. },
        }) => ("onboarding.install", None, None),
        Command::Onboarding(OnboardingArgs {
            command: OnboardingCommand::Resume { .. },
        }) => ("onboarding.resume", None, None),
        Command::Onboarding(OnboardingArgs {
            command: OnboardingCommand::Status { .. },
        }) => ("onboarding.status", None, None),
        Command::Integration(_) => ("integration", None, None),
        Command::Smoke(args) => ("smoke", Some(&args.workspace), None),
    }
}

fn task_identity(command: &TaskCommand) -> (&'static str, Option<&str>, Option<i64>) {
    match command {
        TaskCommand::List(args) => ("task.list", Some(&args.room), None),
        TaskCommand::Show(args) => ("task.show", Some(&args.room), Some(args.id)),
        TaskCommand::History(args) => ("task.history", Some(&args.room), Some(args.id)),
        TaskCommand::Watch(args) => ("task.watch", Some(&args.room), args.task),
        TaskCommand::Create(args) => ("task.create", Some(&args.room), None),
        TaskCommand::Edit(args) => ("task.edit", Some(&args.room), Some(args.id)),
        TaskCommand::Assign(args) => ("task.assign", Some(&args.room), Some(args.id)),
        TaskCommand::Note(args) => ("task.note", Some(&args.room), Some(args.id)),
        TaskCommand::Cancel(args) => ("task.cancel", Some(&args.room), Some(args.id)),
        TaskCommand::Reopen(args) => ("task.reopen", Some(&args.room), Some(args.id)),
        TaskCommand::Interrupt(args) => ("task.interrupt", Some(&args.room), Some(args.id)),
        TaskCommand::ConfirmStopped(args) => {
            ("task.confirm-stopped", Some(&args.room), Some(args.id))
        }
        TaskCommand::Request(args) => ("task.request", Some(&args.room), Some(args.id)),
        TaskCommand::Import(args) => ("task.import", Some(&args.room), None),
        TaskCommand::Link(args) => ("task.link", Some(&args.room), Some(args.id)),
        TaskCommand::Publish(args) => ("task.publish", Some(&args.room), Some(args.id)),
        TaskCommand::ExternalStatus(args) => ("task.external-status", Some(&args.room), None),
        TaskCommand::ExternalResolve(args) => ("task.external-resolve", Some(&args.room), None),
    }
}

pub fn write_output_item(
    output: &mut dyn Write,
    item: &OutputItem,
    json_output: bool,
) -> Result<(), CliError> {
    if json_output {
        write_json_line(output, &item.json)
    } else {
        writeln!(output, "{}", escape_terminal(&item.human)).map_err(output_error)
    }
}

pub fn write_output_page(
    output: &mut dyn Write,
    page: &OutputPage,
    json_output: bool,
) -> Result<Option<String>, CliError> {
    let next = checked_next_cursor(page.has_more, page.next_cursor.as_deref())?;
    if json_output {
        let value = json!({
            "items": page.items.iter().map(|item| item.json.clone()).collect::<Vec<_>>(),
            "nextCursor": page.next_cursor,
            "hasMore": page.has_more,
        });
        write_json_line(output, &value)?;
    } else {
        for item in &page.items {
            write_output_item(output, item, false)?;
        }
        writeln!(
            output,
            "nextCursor={} hasMore={}",
            page.next_cursor.as_deref().unwrap_or("-"),
            page.has_more
        )
        .map_err(output_error)?;
    }
    Ok(next)
}

pub fn write_task_page(
    output: &mut dyn Write,
    page: &TaskPage,
    json_output: bool,
) -> Result<Option<String>, CliError> {
    let next = checked_next_cursor(page.has_more, page.next_cursor.as_deref())?;
    if json_output {
        let value = json!({
            "tasks": page.tasks.iter().map(task_row_json).collect::<Vec<_>>(),
            "nextCursor": page.next_cursor,
            "hasMore": page.has_more,
        });
        write_json_line(output, &value)?;
    } else {
        writeln!(
            output,
            "ID\tSTATE\tEXECUTOR\tRUN\tSESSION\tNEXT\tCHECKPOINT\tSTOP\tTITLE"
        )
        .map_err(output_error)?;
        for task in &page.tasks {
            writeln!(output, "{}", render_task_row(task)).map_err(output_error)?;
        }
        writeln!(
            output,
            "nextCursor={} hasMore={}",
            page.next_cursor.as_deref().unwrap_or("-"),
            page.has_more
        )
        .map_err(output_error)?;
    }
    Ok(next)
}

fn checked_next_cursor(
    has_more: bool,
    next_cursor: Option<&str>,
) -> Result<Option<String>, CliError> {
    if has_more && next_cursor.is_none() {
        return Err(CliError::runtime(
            "invalid_page",
            "hasMore requires nextCursor",
        ));
    }
    Ok(has_more.then(|| next_cursor.map(str::to_owned)).flatten())
}

pub fn write_ndjson<I>(output: &mut dyn Write, values: I) -> Result<(), CliError>
where
    I: IntoIterator<Item = Value>,
{
    for value in values {
        write_json_line(output, &value)?;
    }
    Ok(())
}

pub fn write_json_line(output: &mut dyn Write, value: &Value) -> Result<(), CliError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| CliError::runtime("output_encode_failed", error.to_string()))?;
    if bytes.len() > MAX_OUTPUT_PAGE_BYTES {
        return Err(CliError::runtime(
            "output_too_large",
            format!("output record exceeds {MAX_OUTPUT_PAGE_BYTES} bytes"),
        ));
    }
    output.write_all(&bytes).map_err(output_error)?;
    output.write_all(b"\n").map_err(output_error)
}

fn output_error(error: io::Error) -> CliError {
    let message = error.to_string();
    drop(error);
    CliError::runtime("output_failed", message)
}

#[must_use]
pub fn escape_terminal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character <= '\u{001f}' || ('\u{007f}'..='\u{009f}').contains(&character) {
            use fmt::Write as _;
            let _ = write!(escaped, "\\u{{{:04X}}}", u32::from(character));
        } else {
            escaped.push(character);
        }
    }
    escaped
}

#[must_use]
pub fn render_task_row(task: &TaskRow) -> String {
    let selected = task
        .current
        .as_ref()
        .map(|run| ("current", run))
        .or_else(|| task.last.as_ref().map(|run| ("last", run)));
    let (executor, attempt, session) = selected.map_or_else(
        || {
            (
                task.last_executor_id.as_deref().map_or_else(
                    || "-".to_owned(),
                    |executor| format!("last:{}", escape_terminal(executor)),
                ),
                "-".to_owned(),
                task.execution_session_id.map_or_else(
                    || "-".to_owned(),
                    |session_id| format!("last:{}", short_uuid(session_id)),
                ),
            )
        },
        |(label, run)| {
            (
                format!("{label}:{}", escape_terminal(&run.executor)),
                format!("{label}:{}", short_uuid(run.attempt_id)),
                format!("{label}:{}", short_uuid(run.session_id)),
            )
        },
    );
    let stop = match task.stop_evidence {
        StopEvidence::None => "-",
        StopEvidence::RunningUnknown => "running",
        StopEvidence::InterruptedUnknown => "stop unconfirmed; handoff blocked",
        StopEvidence::Released => "released by executor",
        StopEvidence::Confirmed => "stopped confirmed",
    };
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        task.id,
        escape_terminal(&task.state),
        executor,
        attempt,
        session,
        task.assigned_agent_id
            .as_deref()
            .map_or_else(|| "-".to_owned(), escape_terminal),
        task.checkpoint
            .as_deref()
            .map_or_else(|| "-".to_owned(), escape_terminal),
        stop,
        escape_terminal(&task.title),
    )
}

fn task_row_json(task: &TaskRow) -> Value {
    let run_json = |run: &TaskRunIdentity| {
        json!({
            "executor": run.executor,
            "attemptId": run.attempt_id,
            "sessionId": run.session_id,
        })
    };
    json!({
        "id": task.id,
        "state": task.state,
        "current": task.current.as_ref().map(run_json),
        "last": task.last.as_ref().map(run_json),
        "lastExecutorId": task.last_executor_id,
        "executionSessionId": task.execution_session_id,
        "assignedAgentId": task.assigned_agent_id,
        "checkpoint": task.checkpoint,
        "stopEvidence": match task.stop_evidence {
            StopEvidence::None => "none",
            StopEvidence::RunningUnknown => "running_unknown",
            StopEvidence::InterruptedUnknown => "interrupted_unknown",
            StopEvidence::Released => "released",
            StopEvidence::Confirmed => "confirmed",
        },
        "title": task.title,
    })
}

fn short_uuid(value: Uuid) -> String {
    value.simple().to_string()[..8].to_owned()
}
