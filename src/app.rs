use std::{
    env,
    ffi::OsString,
    io::{self, IsTerminal as _, Write as _},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

use clap::Parser as _;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    cli::{
        self, Cli, CliError, Command, CredentialCommand, CredentialIssueArgs, GatewayCommand,
        GatewayProviderArgs, IntegrationAdminCommand, IntegrationCommand, ProfileCommand,
        RandomOperationIds, RouterAction, ShareMode, SmokeArgs, StdinPayload, TaskCommand,
        WorkspaceCommand,
    },
    client::{ClientConfig, ClientEvent, ClientEvents, ClientRole, RouterClient},
    config::{self, Profile},
    credentials::{
        CredentialFile, CredentialRole, ensure_private_directory, read_credential,
        write_credential_exclusive,
    },
    hosts::{
        self, HostError, ManagedClaudeOptions, ManagedCodexOptions, ManagedProviderOptions,
        McpChildSelection,
    },
    install::{self, InstallOutcome},
    integrations::IntegrationPublic,
    onboarding::OnboardingProvider,
    process::{
        self, AdminControl, HealthProbe, NativeChildConfig, NativeChildSupervisor, NativeLauncher,
        OwnedServe, ProbeFailure, ProcessFuture, ProfilePublisher, ReqwestHealthProbe,
        RouterLaunchError, RuntimeRecord, RuntimeShareMode, RuntimeStore, ShareRequest,
        StartOptions, StartOutcome, StopOutcome, SystemTailscale, TailscaleControl,
        TailscaleSnapshot, child_startup_handshake, enter_owned_process_group,
        tls_settings_from_environment,
    },
    protocol::{
        AgentClient, AgentRegistration, AgentSide, ClientMessage, DeliveryMode, ServerMessage,
        TaskHistoryEvent, TaskHistoryPage, WorkspaceEvent, WorkspaceEventKind, WorkspaceName,
    },
    providers::codex::ThreadSelection,
    router::{RouterConfig, RouterExposure, RouterRuntime},
    tasks::{
        ExternalOperationSummary, ExternalPublishKind, ExternalResolution,
        ExternalResolutionOutcome, StopEvidence as TaskStopEvidence, TaskDetail, TaskEvent,
        TaskMutationResult, TaskState, TaskSummary,
    },
    tui::{self, UiExit, UiOptions, state::Ownership},
};

const CHILD_COMMAND: &str = "__router-child";
const DEVICE_PROFILE: &str = "this-device";
const ADMIN_CREDENTIAL: &str = "credentials/admin.json";
const DEFAULT_BIND: &str = "127.0.0.1:8787";

pub fn prepare_process(arguments: &[OsString]) -> Result<(), CliError> {
    if arguments.get(1).is_some_and(|value| value == CHILD_COMMAND) {
        if arguments.len() != 6
            || arguments.get(2).is_none_or(|value| value != "--bind")
            || arguments.get(4).is_none_or(|value| value != "--data-dir")
        {
            return Err(CliError::runtime(
                "startup_protocol_error",
                "invalid owned router child invocation",
            ));
        }
        let background = match env::var("ASR_BACKGROUND_CHILD").as_deref() {
            Ok("0") => false,
            Ok("1") => true,
            _ => {
                return Err(CliError::runtime(
                    "startup_protocol_error",
                    "missing owned router launch context",
                ));
            }
        };
        let instance_id = env::var("ASR_LAUNCH_INSTANCE_ID")
            .ok()
            .and_then(|value| Uuid::parse_str(&value).ok())
            .filter(|value| !value.is_nil())
            .ok_or_else(|| {
                CliError::runtime(
                    "startup_protocol_error",
                    "missing owned router launch identity",
                )
            })?;
        let _ = instance_id;
        enter_owned_process_group(background).map_err(|error| launch_error(&error))?;
    }
    Ok(())
}

pub async fn run<I, T>(arguments: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let arguments = arguments
        .into_iter()
        .map(Into::into)
        .collect::<Vec<OsString>>();
    let result = if arguments.get(1).is_some_and(|value| value == CHILD_COMMAND) {
        run_owned_child(&arguments).await
    } else {
        run_cli(&arguments).await
    };
    match result {
        Ok(code) => exit_code(code),
        Err(error) => {
            eprintln!("{error}");
            exit_code(error.exit_code())
        }
    }
}

async fn run_cli(arguments: &[OsString]) -> Result<i32, CliError> {
    let mut cli = match Cli::try_parse_from(arguments) {
        Ok(cli) => cli,
        Err(error) => {
            let code = error.exit_code();
            error.print().map_err(|print_error| {
                CliError::runtime("output_failed", print_error.to_string())
            })?;
            return Ok(code);
        }
    };
    if cli.command.is_none() {
        if !io::stdin().is_terminal() {
            return Err(CliError::usage(
                "command_required",
                "a command is required when stdin is not a terminal",
            ));
        }
        let Some(selected) = interactive_selection()? else {
            return Ok(0);
        };
        cli = selected;
    }
    if !cli.dry_run {
        let command = cli
            .command
            .as_mut()
            .ok_or_else(|| CliError::usage("command_required", "a command is required"))?;
        ensure_command_ids(command);
    }
    let command = cli
        .command
        .as_ref()
        .ok_or_else(|| CliError::usage("command_required", "a command is required"))?;
    cli::validate_command(command).map_err(|error| attach_command_ids(error, command))?;
    cli::validate_globals(&cli)?;
    if cli.dry_run {
        let plan = cli::plan_dry_run(&cli, &mut RandomOperationIds)?;
        cli::write_dry_run_plan(&mut io::stdout().lock(), &plan)?;
        return Ok(0);
    }
    let stdin = cli::read_command_stdin(command, &mut io::stdin().lock())
        .map_err(|error| attach_command_ids(error, command))?;
    dispatch(&cli, stdin)
        .await
        .map_err(|error| attach_command_ids(error, command))
}

fn ensure_command_ids(command: &mut Command) {
    let Command::Task(args) = command else {
        return;
    };
    let operation_id = match &mut args.command {
        TaskCommand::Create(args) => Some(&mut args.mutation.operation_id),
        TaskCommand::Edit(args) => Some(&mut args.mutation.operation_id),
        TaskCommand::Assign(args) => Some(&mut args.mutation.operation_id),
        TaskCommand::Note(args) => Some(&mut args.mutation.operation_id),
        TaskCommand::Cancel(args) | TaskCommand::Reopen(args) | TaskCommand::Interrupt(args) => {
            Some(&mut args.mutation.operation_id)
        }
        TaskCommand::ConfirmStopped(args) => Some(&mut args.mutation.operation_id),
        TaskCommand::Import(args) => Some(&mut args.mutation.operation_id),
        TaskCommand::Link(args) => Some(&mut args.mutation.operation_id),
        TaskCommand::Publish(args) => Some(&mut args.mutation.operation_id),
        TaskCommand::ExternalResolve(args) => {
            args.resolution_id.get_or_insert_with(Uuid::new_v4);
            None
        }
        TaskCommand::List(_)
        | TaskCommand::Show(_)
        | TaskCommand::History(_)
        | TaskCommand::Watch(_)
        | TaskCommand::Request(_)
        | TaskCommand::ExternalStatus(_) => None,
    };
    if let Some(operation_id) = operation_id {
        operation_id.get_or_insert_with(Uuid::new_v4);
    }
}

fn attach_command_ids(mut error: CliError, command: &Command) -> CliError {
    error.operation_id = error
        .operation_id
        .or_else(|| cli::command_operation_id(command));
    error.resolution_id = error
        .resolution_id
        .or_else(|| cli::command_resolution_id(command));
    error
}

fn interactive_selection() -> Result<Option<Cli>, CliError> {
    let mut output = io::stdout().lock();
    output
        .write_all(
            b"Agent Session Router\n\
              1) Start router and open console\n\
              2) Stop local router\n\
              3) List profiles\n\
              4) List workspaces on this device\n\
              5) List credentials on this device\n\
              6) Run doctor\n\
              7) Open console for running router\n\
              q) Cancel\n\
              Selection: ",
        )
        .map_err(|error| CliError::runtime("output_failed", error.to_string()))?;
    output
        .flush()
        .map_err(|error| CliError::runtime("output_failed", error.to_string()))?;
    drop(output);
    let mut choice = String::new();
    io::stdin()
        .read_line(&mut choice)
        .map_err(|error| CliError::runtime("input_failed", error.to_string()))?;
    let arguments: Option<&[&str]> = match choice.trim() {
        "1" => Some(&["asr", "router", "start"]),
        "2" => Some(&["asr", "router", "stop"]),
        "3" => Some(&["asr", "profile", "list"]),
        "4" => Some(&["asr", "--profile", DEVICE_PROFILE, "workspace", "list"]),
        "5" => Some(&["asr", "--profile", DEVICE_PROFILE, "credential", "list"]),
        "6" => Some(&["asr", "doctor"]),
        "7" => Some(&["asr", "ui"]),
        "q" | "Q" | "" => None,
        _ => {
            return Err(CliError::usage(
                "invalid_selection",
                "menu selection must be 1 through 7 or q",
            ));
        }
    };
    arguments
        .map(|arguments| {
            Cli::try_parse_from(arguments)
                .map_err(|_| CliError::runtime("menu_invalid", "internal menu command is invalid"))
        })
        .transpose()
}

async fn dispatch(cli: &Cli, stdin: Option<StdinPayload>) -> Result<i32, CliError> {
    let command = cli
        .command
        .as_ref()
        .ok_or_else(|| CliError::usage("command_required", "a command is required"))?;
    match command {
        Command::Profile(args) => profile_command(&args.command),
        Command::Install(args) => install_command(args.bin_dir.as_deref()),
        Command::Router(args) => router_command(cli, args).await,
        Command::Ui(args) => ui_command(cli, args).await,
        Command::Workspace(args) => workspace_command(cli, &args.command, stdin).await,
        Command::Task(args) => task_command(cli, &args.command, stdin).await,
        Command::Integration(args) => integration_command(cli, &args.command).await,
        Command::Credential(args) => credential_command(cli, &args.command).await,
        Command::Onboarding(args) => onboarding_command(cli, &args.command).await,
        Command::Doctor => doctor_command(cli).await,
        Command::Mcp(args) => mcp_command(cli, args).await,
        Command::Codex(args) => codex_command(cli, args).await,
        Command::CodexCli(args) => codex_cli_command(cli, args),
        Command::Claude(args) => claude_command(cli, args),
        Command::Omp(args) => omp_command(cli, args).await,
        Command::Gateway(args) => gateway_command(cli, &args.command).await,
        Command::SetupClaude => setup_claude_command(),
        Command::SetupOmp => setup_omp_command().await,
        Command::Smoke(args) => smoke_command(cli, args).await,
    }
}

async fn mcp_command(cli: &Cli, args: &cli::McpArgs) -> Result<i32, CliError> {
    let mut invocation = hosts::mcp_invocation(
        args,
        cli.profile.as_deref(),
        cli.credential.as_deref(),
        cli.dry_run,
    )
    .map_err(|error| host_error(&error))?;
    hosts::resolve_mcp_route(&mut invocation)
        .await
        .map_err(|error| host_error(&error))?;
    hosts::run_mcp_stdio(
        invocation.role,
        invocation.agent_id,
        invocation.config,
        invocation.initial_workspace,
    )
    .await
    .map_err(|error| host_error(&error))?;
    Ok(0)
}

fn selected_agent_credential(
    cli: &Cli,
    requested_agent: Option<&str>,
    side: AgentSide,
    client: AgentClient,
) -> Result<(config::ProviderSelection, PathBuf, CredentialFile), CliError> {
    let provider = match (side, client) {
        (AgentSide::Claude, AgentClient::ClaudeCode) => Some(OnboardingProvider::ClaudeCode),
        (AgentSide::Codex, AgentClient::CodexCli) => Some(OnboardingProvider::CodexCli),
        (AgentSide::Generic, AgentClient::Omp) => Some(OnboardingProvider::Omp),
        _ => None,
    };
    let selection = if let Some(provider) = provider {
        config::select_provider(cli.profile.as_deref(), cli.credential.as_deref(), provider)
            .map_err(|error| config_error(&error))?
    } else {
        config::ProviderSelection {
            selection: config::select(cli.profile.as_deref(), cli.credential.as_deref())
                .map_err(|error| config_error(&error))?,
            routes: Vec::new(),
            ca_file: None,
            initial_workspace: None,
            expected_server_id: None,
        }
    };
    let credential_path = selection
        .selection
        .credential_file
        .clone()
        .ok_or_else(|| config_error(&config::ConfigError::Required))?;
    let credential = read_credential(&credential_path)
        .map_err(|error| CliError::runtime("credential_invalid", error.to_string()))?;
    if credential.role != CredentialRole::Agent
        || credential.agent_side != Some(side)
        || credential.agent_client != Some(client)
        || requested_agent.is_some_and(|agent| agent != credential.subject)
    {
        return Err(CliError::runtime(
            "credential_claims_mismatch",
            "selected credential does not match the requested agent host",
        ));
    }
    Ok((selection, credential_path, credential))
}

fn child_selection(
    cli: &Cli,
    requested_agent: Option<&str>,
    side: AgentSide,
    client: AgentClient,
) -> Result<(McpChildSelection, Option<WorkspaceName>), CliError> {
    let (selection, credential_file, _) =
        selected_agent_credential(cli, requested_agent, side, client)?;
    let pinned = selection.expected_server_id.is_some();
    let workspace = selection.initial_workspace;
    // A binding belongs to its named profile, not to every MCP registration
    // inherited by this wrapper. Only promote a user-supplied override.
    let explicit_credential = cli.credential.is_some()
        || env::var_os("ASR_CREDENTIAL_FILE").is_some_and(|value| !value.is_empty());
    Ok((
        McpChildSelection {
            profile: if pinned {
                selection.selection.profile
            } else {
                cli.profile.clone()
            },
            credential_file: explicit_credential.then_some(credential_file),
        },
        workspace,
    ))
}

fn stock_workspace(
    explicit: Option<&str>,
    bound: Option<WorkspaceName>,
) -> Result<Option<WorkspaceName>, CliError> {
    if let Some(value) = explicit {
        return workspace_name(value).map(Some);
    }
    if let Some(value) = env::var_os("ASR_WORKSPACE").filter(|value| !value.is_empty()) {
        let value = value
            .into_string()
            .map_err(|_| config_error(&config::ConfigError::Invalid))?;
        return workspace_name(&value).map(Some);
    }
    Ok(bound)
}

fn primary_client_config(
    cli: &Cli,
    requested_agent: Option<&str>,
    side: AgentSide,
    client: AgentClient,
    delivery_mode: DeliveryMode,
    activity: Option<String>,
) -> Result<ClientConfig, CliError> {
    let (selection, _, credential) = selected_agent_credential(cli, requested_agent, side, client)?;
    let delegation_token = CredentialFile::generate(
        CredentialRole::Agent,
        credential.subject.clone(),
        Some(side),
        Some(client),
        Vec::new(),
    )
    .map_err(|error| CliError::runtime("credential_invalid", error.to_string()))?
    .token()
    .clone();
    Ok(ClientConfig {
        router_url: selection.selection.router_url,
        role: ClientRole::Primary {
            agent: AgentRegistration {
                agent_id: credential.subject.clone(),
                side,
                client,
                activity,
                delivery_mode,
            },
            credential,
            delegation_token: Some(delegation_token),
        },
        ca_file: env::var_os("ASR_CA_FILE")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from),
    })
}

fn required_executable(name: &str) -> Result<PathBuf, CliError> {
    find_executable(name).ok_or_else(|| {
        CliError::runtime(
            "executable_not_found",
            format!("{name} executable was not found on PATH"),
        )
    })
}

fn host_context() -> Result<(PathBuf, PathBuf, Vec<(OsString, OsString)>), CliError> {
    let current_executable = env::current_exe()
        .map_err(|error| CliError::runtime("host_launch_failed", error.to_string()))?;
    let caller_cwd = env::current_dir()
        .map_err(|error| CliError::runtime("host_launch_failed", error.to_string()))?;
    Ok((current_executable, caller_cwd, env::vars_os().collect()))
}

fn execute_host_plan(plan: &process::LaunchPlan) -> Result<i32, CliError> {
    process::execute(plan)
        .map_err(|error| CliError::runtime("host_launch_failed", error.to_string()))
}
async fn codex_command(cli: &Cli, args: &cli::ProviderArgs) -> Result<i32, CliError> {
    let client = primary_client_config(
        cli,
        args.agent.as_deref(),
        AgentSide::Codex,
        AgentClient::CodexAppServer,
        DeliveryMode::Push,
        args.activity.clone(),
    )?;
    let workspace = args.workspace.as_deref().map(workspace_name).transpose()?;
    let options = ManagedCodexOptions {
        common: managed_provider_options()?,
        executable: required_executable("codex")?,
        executable_arguments: Vec::new(),
        thread: ThreadSelection::start(),
    };
    let shutdown = CancellationToken::new();
    let run = hosts::run_interactive_codex(client, workspace, options, shutdown.clone());
    run_provider_until_signal(run, shutdown).await
}

fn codex_cli_command(cli: &Cli, args: &cli::CodexCliArgs) -> Result<i32, CliError> {
    let (selection, bound_workspace) = child_selection(
        cli,
        args.agent.as_deref(),
        AgentSide::Codex,
        AgentClient::CodexCli,
    )?;
    let workspace = stock_workspace(args.workspace.as_deref(), bound_workspace)?;
    let (current_executable, caller_cwd, _) = host_context()?;
    let plan = hosts::stock_codex_plan(
        required_executable("codex")?.into_os_string(),
        caller_cwd,
        &current_executable,
        &selection,
        workspace.as_ref(),
        args.arguments.clone(),
    )
    .map_err(|error| host_error(&error))?;
    execute_host_plan(&plan)
}

fn claude_command(cli: &Cli, args: &cli::ClaudeArgs) -> Result<i32, CliError> {
    let (selection, bound_workspace) = child_selection(
        cli,
        args.agent.as_deref(),
        AgentSide::Claude,
        AgentClient::ClaudeCode,
    )?;
    let workspace = stock_workspace(args.workspace.as_deref(), bound_workspace)?;
    let (_, caller_cwd, _) = host_context()?;
    let plan = hosts::stock_claude_plan(
        required_executable("claude")?.into_os_string(),
        caller_cwd,
        &selection,
        workspace.as_ref(),
        args.auto,
        args.resume.clone(),
    );
    execute_host_plan(&plan)
}

async fn omp_command(cli: &Cli, args: &cli::OmpArgs) -> Result<i32, CliError> {
    let (selection, bound_workspace) = child_selection(
        cli,
        args.agent.as_deref(),
        AgentSide::Generic,
        AgentClient::Omp,
    )?;
    let workspace = stock_workspace(args.workspace.as_deref(), bound_workspace)?;
    let (current_executable, caller_cwd, source_environment) = host_context()?;
    let program = required_executable("omp")?;
    hosts::preflight_omp_plugin(
        program.as_os_str(),
        &caller_cwd,
        &current_executable,
        &source_environment,
    )
    .await
    .map_err(|error| host_error(&error))?;
    let plan = hosts::stock_omp_plan(
        program.into_os_string(),
        caller_cwd,
        current_executable,
        &selection,
        workspace.as_ref(),
        args.arguments.clone(),
    )
    .map_err(|error| host_error(&error))?;
    execute_host_plan(&plan)
}

fn setup_claude_command() -> Result<i32, CliError> {
    let (current_executable, caller_cwd, _) = host_context()?;
    let plan = hosts::setup_claude_plan(
        required_executable("claude")?.into_os_string(),
        caller_cwd,
        current_executable,
    )
    .map_err(|error| host_error(&error))?;
    execute_host_plan(&plan)
}

async fn setup_omp_command() -> Result<i32, CliError> {
    let (current_executable, caller_cwd, source_environment) = host_context()?;
    let plan = hosts::setup_omp_checked_plan(
        required_executable("omp")?.into_os_string(),
        caller_cwd,
        &current_executable,
        &source_environment,
    )
    .await
    .map_err(|error| host_error(&error))?;
    plan.as_ref().map_or(Ok(0), execute_host_plan)
}

async fn gateway_command(cli: &Cli, command: &GatewayCommand) -> Result<i32, CliError> {
    match command {
        GatewayCommand::Codex(args) => gateway_codex(cli, args).await,
        GatewayCommand::Claude(args) => gateway_claude(cli, args).await,
    }
}

fn managed_provider_options() -> Result<ManagedProviderOptions, CliError> {
    let (asr_executable, caller_cwd, source_environment) = host_context()?;
    Ok(ManagedProviderOptions {
        asr_executable,
        caller_cwd,
        source_environment,
    })
}

async fn gateway_codex(cli: &Cli, args: &GatewayProviderArgs) -> Result<i32, CliError> {
    let client = primary_client_config(
        cli,
        args.agent.as_deref(),
        AgentSide::Codex,
        AgentClient::CodexAppServer,
        DeliveryMode::Push,
        None,
    )?;
    let workspace = args.workspace.as_deref().map(workspace_name).transpose()?;
    let options = ManagedCodexOptions {
        common: managed_provider_options()?,
        executable: required_executable("codex")?,
        executable_arguments: Vec::new(),
        thread: ThreadSelection::start(),
    };
    let shutdown = CancellationToken::new();
    let run = hosts::run_managed_codex(client, workspace, options, shutdown.clone());
    run_provider_until_signal(run, shutdown).await
}

async fn gateway_claude(cli: &Cli, args: &GatewayProviderArgs) -> Result<i32, CliError> {
    let client = primary_client_config(
        cli,
        args.agent.as_deref(),
        AgentSide::Claude,
        AgentClient::ClaudeSdk,
        DeliveryMode::Push,
        None,
    )?;
    let workspace = args.workspace.as_deref().map(workspace_name).transpose()?;
    let options = ManagedClaudeOptions {
        common: managed_provider_options()?,
        node_executable: required_executable("node")?,
        node_arguments: Vec::new(),
        claude_executable: find_executable("claude"),
        resume_session_id: None,
    };
    let shutdown = CancellationToken::new();
    let run = hosts::run_managed_claude(client, workspace, options, shutdown.clone());
    run_provider_until_signal(run, shutdown).await
}

async fn run_provider_until_signal(
    run: impl std::future::Future<Output = Result<(), HostError>>,
    shutdown: CancellationToken,
) -> Result<i32, CliError> {
    tokio::pin!(run);
    tokio::select! {
        result = &mut run => {
            result.map_err(|error| host_error(&error))?;
            Ok(0)
        }
        signal = provider_shutdown_signal() => {
            signal.map_err(|error| CliError::runtime("signal_failed", error.to_string()))?;
            shutdown.cancel();
            run.await.map_err(|error| host_error(&error))?;
            Err(CliError::interrupted("interrupted"))
        }
    }
}

async fn provider_shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

async fn smoke_command(cli: &Cli, args: &SmokeArgs) -> Result<i32, CliError> {
    let (client, mut events) = operator_client(cli).await?;
    let workspace = workspace_name(&args.workspace)?;
    let operation = async {
        client
            .workspace_join(workspace)
            .await
            .map_err(|error| client_error(&error))?;
        let id = request_id("smoke");
        let response = client
            .call(ClientMessage::Send {
                request_id: id.clone(),
                to: args.target.clone(),
                content: "smoke request".to_owned(),
                timeout_ms: args.timeout_ms,
            })
            .await
            .map_err(|error| client_error(&error))?;
        if !matches!(response, ServerMessage::Accepted { .. }) {
            return Err(unexpected_response());
        }
        let result = wait_send_result(&mut events, &id, args.timeout_ms).await?;
        if !result.ok {
            return Err(CliError::runtime(
                result.error.map_or("provider_error", |code| code.as_str()),
                "provider smoke test failed",
            ));
        }
        if result
            .content
            .is_none_or(|content| content.trim().is_empty())
        {
            return Err(CliError::runtime(
                "empty_response",
                "provider smoke test returned an empty response",
            ));
        }
        Ok(())
    }
    .await;
    let closed = leave_and_close(&client).await;
    operation?;
    closed?;
    println!("provider smoke test passed");
    Ok(0)
}

fn profile_command(command: &ProfileCommand) -> Result<i32, CliError> {
    let path = config::config_path().map_err(|error| config_error(&error))?;
    let mut stored = config::load_config(&path).map_err(|error| config_error(&error))?;
    match command {
        ProfileCommand::List => {
            let selected = stored
                .default_profile
                .as_deref()
                .unwrap_or(config::DEFAULT_PROFILE);
            println!(
                "{} {} {}",
                if selected == config::DEFAULT_PROFILE {
                    "*"
                } else {
                    " "
                },
                config::DEFAULT_PROFILE,
                config::DEFAULT_ROUTER_URL
            );
            for (name, profile) in &stored.profiles {
                println!(
                    "{} {} {}",
                    if selected == name { "*" } else { " " },
                    name,
                    profile.router_url
                );
            }
        }
        ProfileCommand::Add {
            name,
            address,
            force,
        } => {
            config::validate_profile_name(name).map_err(|error| config_error(&error))?;
            if name == config::DEFAULT_PROFILE {
                return Err(CliError::usage(
                    "invalid_profile",
                    "the built-in local profile cannot be replaced",
                ));
            }
            if stored.profiles.contains_key(name) && !force {
                return Err(CliError::runtime(
                    "profile_exists",
                    "profile already exists; pass --force to replace it",
                ));
            }
            let url =
                config::normalize_router_url(address).map_err(|error| config_error(&error))?;
            stored
                .profiles
                .insert(name.clone(), Profile::manual(url.to_string()));
            stored.default_profile = Some(name.clone());
            config::save_config(&path, &stored).map_err(|error| config_error(&error))?;
            println!("selected profile {name}");
        }
        ProfileCommand::Use { name } => {
            config::validate_profile_name(name).map_err(|error| config_error(&error))?;
            if name != config::DEFAULT_PROFILE && !stored.profiles.contains_key(name) {
                return Err(CliError::runtime(
                    "profile_not_found",
                    "profile does not exist",
                ));
            }
            stored.default_profile = Some(name.clone());
            config::save_config(&path, &stored).map_err(|error| config_error(&error))?;
            println!("selected profile {name}");
        }
    }
    Ok(0)
}

fn install_command(bin_dir: Option<&Path>) -> Result<i32, CliError> {
    let outcome = install::install_current(bin_dir)
        .map_err(|error| CliError::runtime("install_failed", error.to_string()))?;
    println!(
        "{}",
        match outcome {
            InstallOutcome::Installed => "installed",
            InstallOutcome::AlreadyInstalled => "already installed",
        }
    );
    Ok(0)
}

async fn router_command(cli: &Cli, args: &cli::RouterArgs) -> Result<i32, CliError> {
    let data_dir = absolute_data_dir()?;
    ensure_private_directory(&data_dir, true)
        .map_err(|error| CliError::runtime("launcher_permissions", error.to_string()))?;
    let bind = env::var("ASR_BIND")
        .unwrap_or_else(|_| DEFAULT_BIND.to_owned())
        .parse::<SocketAddr>()
        .map_err(|_| CliError::usage("invalid_bind", "ASR_BIND must be a socket address"))?;
    let tls = tls_settings_from_environment(bind, env::vars_os())
        .map_err(|error| launch_error(&error))?;
    let ca_file = tls
        .as_ref()
        .and_then(|settings| settings.ca_file.clone())
        .or_else(|| {
            env::var_os("ASR_CA_FILE")
                .filter(|path| !path.is_empty())
                .map(PathBuf::from)
        });
    let mut launcher = native_launcher(&data_dir, bind, ca_file.clone())?;
    match args.action {
        RouterAction::Start => {
            let auto_ui = terminal_available() && !args.background && !args.no_ui;
            let share = match args.share {
                None => ShareRequest::Local,
                Some(ShareMode::Auto) => ShareRequest::Auto,
                Some(ShareMode::Tailscale) => ShareRequest::Tailscale,
                Some(ShareMode::Lan) => ShareRequest::Lan,
            };
            let outcome = launcher
                .start(StartOptions {
                    bind,
                    share,
                    tls,
                    background: args.background || auto_ui,
                    instance_id: None,
                })
                .await
                .map_err(|error| launch_error(&error))?;
            if auto_ui {
                let (record, ownership) = match outcome {
                    StartOutcome::Started(record) => (record, Ownership::Owned),
                    StartOutcome::Reused(record) => (record, Ownership::Reused),
                    StartOutcome::ForegroundExited { code, .. } => return Ok(code),
                };
                let (config, options) =
                    owned_console_connection(cli, record, data_dir, ca_file, ownership, None)
                        .await
                        .map_err(console_start_error)?;
                return run_console(config, options, true).await;
            }
            match outcome {
                StartOutcome::Reused(record) => {
                    println!("router already running at {}", record.control_url);
                }
                StartOutcome::Started(record) => {
                    println!("router started at {}", record.control_url);
                }
                StartOutcome::ForegroundExited { code, .. } => return Ok(code),
            }
        }
        RouterAction::Stop => match launcher
            .stop()
            .await
            .map_err(|error| launch_error(&error))?
        {
            StopOutcome::NotRunning => println!("router is not running"),
            StopOutcome::StaleRecovered => println!("removed stale router state"),
            StopOutcome::Stopped(record) => println!("router stopped at {}", record.control_url),
        },
    }
    Ok(0)
}

fn terminal_available() -> bool {
    io::stdin().is_terminal()
        && io::stdout().is_terminal()
        && env::var_os("TERM").is_none_or(|term| term != "dumb")
}

async fn ui_command(cli: &Cli, args: &cli::UiArgs) -> Result<i32, CliError> {
    if !terminal_available() {
        return Err(CliError::runtime(
            "terminal_required",
            "the console requires terminal stdin and stdout and TERM other than dumb",
        ));
    }
    let (config, options) = ui_connection(cli, args).await?;
    run_console(config, options, false).await
}

async fn ui_connection(
    cli: &Cli,
    args: &cli::UiArgs,
) -> Result<(ClientConfig, UiOptions), CliError> {
    let workspace = args.workspace.as_deref().map(workspace_name).transpose()?;
    let explicit_endpoint =
        cli.profile.is_some() || env::var("ROUTER_URL").is_ok_and(|value| !value.trim().is_empty());
    let (config, options) = if explicit_endpoint {
        let (config, profile_name) = operator_client_config(cli)?;
        let owned = owned_runtime_for_url(&config.router_url);
        let (owned_runtime, owned_data_dir) = if let Some((data_dir, record)) = owned {
            if verify_owned_runtime(&data_dir, &record, &config, false).await? {
                (Some(record), Some(data_dir))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };
        let ownership = if owned_runtime.is_some() {
            Ownership::Reused
        } else {
            Ownership::Remote
        };
        (
            config,
            UiOptions {
                profile_name,
                workspace,
                owned_runtime,
                ownership,
                owned_data_dir,
            },
        )
    } else {
        let data_dir = absolute_data_dir()?;
        let record = read_owned_runtime(&data_dir)?.ok_or_else(router_not_running)?;
        let ca_file = env::var_os("ASR_CA_FILE")
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);
        owned_console_connection(cli, record, data_dir, ca_file, Ownership::Reused, workspace)
            .await?
    };
    Ok((config, options))
}

async fn owned_console_connection(
    cli: &Cli,
    record: RuntimeRecord,
    data_dir: PathBuf,
    ca_file: Option<PathBuf>,
    ownership: Ownership,
    workspace: Option<WorkspaceName>,
) -> Result<(ClientConfig, UiOptions), CliError> {
    let credential_path = cli.credential.clone().or_else(|| {
        env::var_os("ASR_CREDENTIAL_FILE")
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
    });
    let config = owned_ui_config(&record, &data_dir, ca_file, credential_path.as_deref())?;
    let admin = verify_owned_runtime(&data_dir, &record, &config, true).await?;
    if credential_path.is_none() && !admin {
        return Err(CliError::runtime(
            "permission_denied",
            "the owned credential is not an administrator",
        ));
    }
    Ok((
        config,
        UiOptions {
            profile_name: None,
            workspace,
            owned_runtime: admin.then_some(record),
            ownership: if admin { ownership } else { Ownership::Remote },
            owned_data_dir: admin.then_some(data_dir),
        },
    ))
}

fn read_owned_runtime(data_dir: &Path) -> Result<Option<RuntimeRecord>, CliError> {
    match data_dir.try_exists() {
        Ok(false) => return Ok(None),
        Ok(true) => {}
        Err(_) => return Err(launch_error(&RouterLaunchError::Io)),
    }
    RuntimeStore::new(data_dir.to_path_buf())
        .and_then(|store| store.read())
        .map_err(|error| launch_error(&error))
}

fn owned_runtime_for_url(router_url: &Url) -> Option<(PathBuf, RuntimeRecord)> {
    // An explicit endpoint never acquires authority from missing or unverifiable local state.
    let data_dir = absolute_data_dir().ok()?;
    let record = read_owned_runtime(&data_dir).ok()??;
    (record.control_url == router_url.as_str()).then_some((data_dir, record))
}

async fn verify_owned_runtime(
    data_dir: &Path,
    record: &RuntimeRecord,
    config: &ClientConfig,
    implicit_owned: bool,
) -> Result<bool, CliError> {
    let store = RuntimeStore::new(data_dir.to_path_buf()).map_err(|error| launch_error(&error))?;
    let _lock = store.lock().map_err(|error| launch_error(&error))?;
    if store.read().map_err(|error| launch_error(&error))?.as_ref() != Some(record) {
        return Err(launch_error(&RouterLaunchError::InstanceChanged));
    }
    let marker = ReqwestHealthProbe::new(config.ca_file.clone())
        .probe(record)
        .await
        .map_err(|failure| {
            if implicit_owned && failure == ProbeFailure::ConnectionRefused {
                router_not_running()
            } else {
                launch_error(&RouterLaunchError::Health(failure))
            }
        })?;
    marker
        .verify(record.instance_id)
        .map_err(|error| launch_error(&error))?;
    let (client, _) = RouterClient::connect(config.clone())
        .await
        .map_err(|error| client_error(&error))?;
    // The file's claims are not authority: a scoped token must not unlock local admin actions.
    let admin = client.operator_is_admin() == Some(true);
    client.close().await.map_err(|error| client_error(&error))?;
    Ok(admin)
}

fn owned_ui_config(
    record: &RuntimeRecord,
    data_dir: &Path,
    ca_file: Option<PathBuf>,
    explicit_credential: Option<&Path>,
) -> Result<ClientConfig, CliError> {
    let admin_path = data_dir.join(ADMIN_CREDENTIAL);
    let credential = read_credential(explicit_credential.unwrap_or(&admin_path)).map_err(|_| {
        CliError::runtime(
            "credential_invalid",
            "console operator credential is invalid",
        )
    })?;
    if credential.role != CredentialRole::Operator
        || (explicit_credential.is_none()
            && (credential.subject != "admin" || !credential.workspaces.is_empty()))
    {
        return Err(CliError::runtime(
            "credential_invalid",
            "console requires an operator credential",
        ));
    }
    Ok(ClientConfig {
        router_url: Url::parse(&record.control_url)
            .map_err(|_| launch_error(&RouterLaunchError::InvalidRuntimeRecord))?,
        role: ClientRole::Operator { credential },
        ca_file,
    })
}

async fn run_console(
    config: ClientConfig,
    options: UiOptions,
    known_running: bool,
) -> Result<i32, CliError> {
    let ca_file = config.ca_file.clone();
    let owned = options
        .owned_runtime
        .as_ref()
        .zip(options.owned_data_dir.as_ref())
        .map(|(record, data_dir)| (record.instance_id, data_dir.clone()));
    match tui::run(config, options).await {
        Ok(UiExit::Detached) => {
            println!(
                "Console detached; router was not stopped. Run `asr ui` to reattach; reuse explicit profile/credential options for remote or scoped sessions."
            );
        }
        Ok(UiExit::StopOwnedRouter) => {
            let (instance, data_dir) = owned.ok_or_else(|| {
                CliError::runtime(
                    "permission_denied",
                    "only a verified owned admin console may stop the router",
                )
            })?;
            // run has restored the terminal before returning this explicit stop request.
            let bind = DEFAULT_BIND.parse().expect("constant loopback bind");
            let mut launcher = native_launcher(&data_dir, bind, ca_file)?;
            match launcher
                .stop_if_instance(instance)
                .await
                .map_err(|error| launch_error(&error))?
            {
                StopOutcome::NotRunning => println!("router is not running"),
                StopOutcome::StaleRecovered => println!("removed stale router state"),
                StopOutcome::Stopped(record) => {
                    println!("router stopped at {}", record.control_url);
                }
            }
        }
        Err(error) => {
            let error = CliError::runtime(error.code, error.message);
            return Err(if known_running || owned.is_some() {
                console_start_error(error)
            } else {
                error
            });
        }
    }
    Ok(0)
}

fn console_start_error(mut error: CliError) -> CliError {
    error
        .message
        .push_str("; the server remains running; run `asr ui` to reattach");
    error
}

fn router_not_running() -> CliError {
    CliError::runtime(
        "router_not_running",
        "no running owned router; run `asr router start`",
    )
}

type ApplicationLauncher = NativeLauncher<
    ReqwestHealthProbe,
    AuthenticatedAdmin,
    AvailableTailscale,
    LocalProfilePublisher,
    NativeChildSupervisor,
>;

fn native_launcher(
    data_dir: &Path,
    bind: SocketAddr,
    ca_file: Option<PathBuf>,
) -> Result<ApplicationLauncher, CliError> {
    let current_exe = env::current_exe()
        .map_err(|error| CliError::runtime("child_launch_failed", error.to_string()))?;
    let cwd = env::current_dir()
        .map_err(|error| CliError::runtime("child_launch_failed", error.to_string()))?;
    let configured_assets = env::var_os("ASR_BOOTSTRAP_DIR").filter(|value| !value.is_empty());
    let assets_dir = process::bootstrap_assets_directory(data_dir, &cwd, configured_assets.clone());
    let bootstrap_validation = configured_assets.is_some() || assets_dir.exists();
    let mut environment: Vec<_> = env::vars_os()
        .filter(|(key, _)| {
            key != "ASR_LAUNCH_INSTANCE_ID"
                && key != "ASR_BACKGROUND_CHILD"
                && key != "ASR_RUNTIME_SHARE_MODE"
                && key != "ASR_BOOTSTRAP_DIR"
        })
        .collect();
    environment.push((
        OsString::from("ASR_BOOTSTRAP_DIR"),
        assets_dir.into_os_string(),
    ));
    let child = NativeChildSupervisor::new(NativeChildConfig {
        program: current_exe,
        arguments: vec![
            OsString::from(CHILD_COMMAND),
            OsString::from("--bind"),
            OsString::from(bind.to_string()),
            OsString::from("--data-dir"),
            data_dir.as_os_str().to_owned(),
        ],
        cwd,
        environment,
        stderr_file: data_dir.join("router.stderr.log"),
        signal_process_group: true,
    });
    let child = if bootstrap_validation {
        child.with_startup_timeout(Duration::from_secs(120))
    } else {
        child
    };
    let store = RuntimeStore::new(data_dir.to_path_buf()).map_err(|error| launch_error(&error))?;
    let admin = AuthenticatedAdmin {
        data_dir: data_dir.to_path_buf(),
        store: store.clone(),
        ca_file: ca_file.clone(),
    };
    let profiles = LocalProfilePublisher {
        path: config::config_path().map_err(|error| config_error(&error))?,
    };
    Ok(NativeLauncher::new(
        store,
        ReqwestHealthProbe::new(ca_file),
        admin,
        AvailableTailscale::discover(),
        profiles,
        child,
    ))
}

async fn run_owned_child(arguments: &[OsString]) -> Result<i32, CliError> {
    let bind = arguments
        .get(3)
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse::<SocketAddr>().ok())
        .ok_or_else(|| CliError::runtime("startup_protocol_error", "invalid child bind"))?;
    let data_dir = arguments.get(5).map(PathBuf::from).ok_or_else(|| {
        CliError::runtime("startup_protocol_error", "invalid child data directory")
    })?;
    let instance_id = env::var("ASR_LAUNCH_INSTANCE_ID")
        .ok()
        .and_then(|value| Uuid::parse_str(&value).ok())
        .filter(|value| !value.is_nil())
        .ok_or_else(|| CliError::runtime("startup_protocol_error", "invalid launch identity"))?;
    let share_mode = match env::var("ASR_RUNTIME_SHARE_MODE").as_deref() {
        Ok("local") => RuntimeShareMode::Local,
        Ok("tailscale") => RuntimeShareMode::Tailscale,
        Ok("lan") => RuntimeShareMode::Lan,
        _ => {
            return Err(CliError::runtime(
                "startup_protocol_error",
                "invalid share mode",
            ));
        }
    };
    let tls = tls_settings_from_environment(bind, env::vars_os())
        .map_err(|error| launch_error(&error))?;
    let exposure = if share_mode == RuntimeShareMode::Tailscale {
        RouterExposure::TailscaleServe
    } else {
        RouterExposure::Direct
    };
    let cwd = env::current_dir()
        .map_err(|error| CliError::runtime("child_launch_failed", error.to_string()))?;
    let assets_dir =
        process::bootstrap_assets_directory(&data_dir, &cwd, env::var_os("ASR_BOOTSTRAP_DIR"));
    let mut runtime = RouterRuntime::start_paused(RouterConfig {
        onboarding_assets_dir: Some(assets_dir),
        bind,
        data_dir,
        instance_id,
        tls_cert_file: tls.as_ref().map(|value| value.certificate_file.clone()),
        tls_key_file: tls.as_ref().map(|value| value.private_key_file.clone()),
        public_url: tls
            .as_ref()
            .map(|value| Url::parse(&value.public_url))
            .transpose()
            .map_err(|_| {
                CliError::runtime("tls_invalid", "ROUTER_PUBLIC_URL is not a valid URL")
            })?,
        exposure,
    })
    .await
    .map_err(|error| router_error(&error))?;
    let control_url = tls.map_or_else(
        || format!("ws://127.0.0.1:{}/ws", runtime.address.port()),
        |settings| settings.public_url,
    );
    let ready = crate::process::StartupReady {
        instance_id,
        control_url,
    };
    let handshake =
        child_startup_handshake(&mut tokio::io::stdout(), &mut tokio::io::stdin(), &ready).await;
    if let Err(error) = handshake {
        let _ = runtime.shutdown().await;
        let _ = runtime.wait().await;
        return Err(launch_error(&error));
    }
    runtime.activate().map_err(|error| router_error(&error))?;
    runtime.wait().await.map_err(|error| router_error(&error))?;
    Ok(0)
}

#[derive(Clone)]
struct AuthenticatedAdmin {
    data_dir: PathBuf,
    store: RuntimeStore,
    ca_file: Option<PathBuf>,
}

impl AuthenticatedAdmin {
    async fn connect(&self, record: &RuntimeRecord) -> Result<RouterClient, RouterLaunchError> {
        let owned = self
            .store
            .read()?
            .filter(|owned| {
                owned.instance_id == record.instance_id && owned.control_url == record.control_url
            })
            .ok_or(RouterLaunchError::AdminAuthentication)?;
        let credential = read_credential(&self.data_dir.join(ADMIN_CREDENTIAL))
            .map_err(|_| RouterLaunchError::AdminAuthentication)?;
        let url = Url::parse(&owned.control_url).map_err(|_| RouterLaunchError::AdminProtocol)?;
        let (client, _) = RouterClient::connect(ClientConfig {
            router_url: url,
            role: ClientRole::Operator { credential },
            ca_file: self.ca_file.clone(),
        })
        .await
        .map_err(|_| RouterLaunchError::AdminAuthentication)?;
        Ok(client)
    }
}

impl AdminControl for AuthenticatedAdmin {
    fn verify_owner<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        Box::pin(async move {
            let client = self.connect(record).await?;
            let response = client
                .call(ClientMessage::Ping {
                    request_id: format!("owner:{}", Uuid::new_v4()),
                })
                .await
                .map_err(|_| RouterLaunchError::AdminProtocol)?;
            if !matches!(response, ServerMessage::Pong { .. }) {
                return Err(RouterLaunchError::AdminProtocol);
            }
            client
                .close()
                .await
                .map_err(|_| RouterLaunchError::AdminProtocol)
        })
    }

    fn shutdown<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        Box::pin(async move {
            let client = self.connect(record).await?;
            let response = client
                .call(ClientMessage::RouterShutdown {
                    request_id: format!("shutdown:{}", Uuid::new_v4()),
                })
                .await
                .map_err(|_| RouterLaunchError::AdminProtocol)?;
            if !matches!(response, ServerMessage::RouterStopping { .. }) {
                return Err(RouterLaunchError::AdminProtocol);
            }
            Ok(())
        })
    }

    fn wait_stopped<'a>(
        &'a mut self,
        record: &'a RuntimeRecord,
        deadline: Duration,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        Box::pin(async move {
            let expires = tokio::time::Instant::now() + deadline;
            let mut health = ReqwestHealthProbe::new(self.ca_file.clone());
            loop {
                match health.probe(record).await {
                    Err(ProbeFailure::ConnectionRefused) => return Ok(()),
                    Ok(_) | Err(ProbeFailure::Transport)
                        if tokio::time::Instant::now() < expires =>
                    {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Ok(_) | Err(ProbeFailure::Transport) => {
                        return Err(RouterLaunchError::ShutdownTimeout);
                    }
                    Err(error) => return Err(RouterLaunchError::Health(error)),
                }
            }
        })
    }
}

struct LocalProfilePublisher {
    path: PathBuf,
}

impl ProfilePublisher for LocalProfilePublisher {
    fn publish(
        &mut self,
        router_url: &str,
        _record: &RuntimeRecord,
    ) -> Result<(), RouterLaunchError> {
        let mut stored = config::load_config(&self.path).map_err(|_| RouterLaunchError::Profile)?;
        if stored
            .profiles
            .get(DEVICE_PROFILE)
            .is_some_and(|profile| profile.server_id.is_some())
        {
            return Ok(());
        }
        stored.profiles.insert(
            DEVICE_PROFILE.to_owned(),
            Profile::manual(router_url.to_owned()),
        );
        config::save_config(&self.path, &stored).map_err(|_| RouterLaunchError::Profile)
    }
}

enum AvailableTailscale {
    System(SystemTailscale),
    Unavailable,
}

impl AvailableTailscale {
    fn discover() -> Self {
        find_executable("tailscale")
            .and_then(|path| SystemTailscale::new(path).ok())
            .map_or(Self::Unavailable, Self::System)
    }
}

impl TailscaleControl for AvailableTailscale {
    fn snapshot(&mut self) -> ProcessFuture<'_, Result<TailscaleSnapshot, RouterLaunchError>> {
        match self {
            Self::System(system) => system.snapshot(),
            Self::Unavailable => Box::pin(async { Err(RouterLaunchError::TailscaleUnavailable) }),
        }
    }

    fn enable<'a>(
        &'a mut self,
        serve: &'a OwnedServe,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        match self {
            Self::System(system) => system.enable(serve),
            Self::Unavailable => Box::pin(async { Err(RouterLaunchError::TailscaleUnavailable) }),
        }
    }

    fn disable<'a>(
        &'a mut self,
        serve: &'a OwnedServe,
    ) -> ProcessFuture<'a, Result<(), RouterLaunchError>> {
        match self {
            Self::System(system) => system.disable(serve),
            Self::Unavailable => Box::pin(async { Err(RouterLaunchError::TailscaleUnavailable) }),
        }
    }
}

pub(crate) fn find_executable(name: &str) -> Option<PathBuf> {
    let cwd = env::current_dir().ok()?;
    env::var_os("PATH").and_then(|path| {
        env::split_paths(&path).find_map(|directory| {
            let candidate = directory.join(name);
            let candidate = if candidate.is_absolute() {
                candidate
            } else {
                cwd.join(candidate)
            };
            candidate
                .metadata()
                .ok()
                .filter(std::fs::Metadata::is_file)?;
            Some(candidate)
        })
    })
}

async fn workspace_command(
    cli: &Cli,
    command: &WorkspaceCommand,
    stdin: Option<StdinPayload>,
) -> Result<i32, CliError> {
    let (client, mut events) = operator_client(cli).await?;
    match command {
        WorkspaceCommand::Create { name } => {
            let name = workspace_name(name)?;
            let response = client
                .call(ClientMessage::WorkspaceCreate {
                    request_id: request_id("workspace-create"),
                    name,
                })
                .await
                .map_err(|error| client_error(&error))?;
            let ServerMessage::WorkspaceCreated { workspace, .. } = response else {
                return Err(unexpected_response());
            };
            println!("created {}", workspace.name);
        }
        WorkspaceCommand::List { json } => {
            let mut after = None;
            loop {
                let response = client
                    .call(ClientMessage::WorkspaceList {
                        request_id: request_id("workspace-list"),
                        after: after.clone(),
                        limit: Some(cli::MAX_PAGE_LIMIT),
                    })
                    .await
                    .map_err(|error| client_error(&error))?;
                let ServerMessage::Workspaces {
                    workspaces,
                    next_cursor,
                    has_more,
                    ..
                } = response
                else {
                    return Err(unexpected_response());
                };
                for workspace in workspaces {
                    if *json {
                        write_json(&workspace)?;
                    } else {
                        println!("{}\t{}", workspace.name, workspace.connected_agents);
                    }
                }
                if !has_more {
                    break;
                }
                after = next_cursor;
            }
        }
        WorkspaceCommand::Members { name, json } => {
            let name = workspace_name(name)?;
            client
                .workspace_join(name)
                .await
                .map_err(|error| client_error(&error))?;
            let response = client
                .call(ClientMessage::WorkspaceMembers {
                    request_id: request_id("workspace-members"),
                })
                .await
                .map_err(|error| client_error(&error))?;
            let ServerMessage::Agents { agents, .. } = response else {
                return Err(unexpected_response());
            };
            for agent in agents {
                if *json {
                    write_json(&agent)?;
                } else {
                    println!(
                        "{}\t{:?}\tready={}",
                        cli::escape_terminal(&agent.agent_id),
                        agent.status,
                        agent.ready
                    );
                }
            }
        }
        WorkspaceCommand::History {
            name,
            after,
            limit,
            json,
        } => {
            client
                .workspace_join(workspace_name(name)?)
                .await
                .map_err(|error| client_error(&error))?;
            let page = client
                .workspace_history(Some(*after), Some(*limit))
                .await
                .map_err(|error| client_error(&error))?;
            for event in page.events {
                if *json {
                    write_json(&event)?;
                } else {
                    println!(
                        "{}\t{:?}\t{}",
                        event.seq,
                        event.kind,
                        cli::escape_terminal(&event.actor_id)
                    );
                }
            }
        }
        WorkspaceCommand::Post { name, .. } => {
            client
                .workspace_join(workspace_name(name)?)
                .await
                .map_err(|error| client_error(&error))?;
            let content = stdin_text(stdin)?;
            let response = client
                .call(ClientMessage::WorkspacePost {
                    request_id: request_id("workspace-post"),
                    content,
                })
                .await
                .map_err(|error| client_error(&error))?;
            let ServerMessage::WorkspacePosted { seq, .. } = response else {
                return Err(unexpected_response());
            };
            println!("posted {seq}");
        }
        WorkspaceCommand::Send {
            name,
            target,
            timeout_ms,
            ..
        } => {
            client
                .workspace_join(workspace_name(name)?)
                .await
                .map_err(|error| client_error(&error))?;
            let content = stdin_text(stdin)?;
            let id = request_id("workspace-send");
            let response = client
                .call(ClientMessage::Send {
                    request_id: id.clone(),
                    to: target.clone(),
                    content,
                    timeout_ms: *timeout_ms,
                })
                .await
                .map_err(|error| client_error(&error))?;
            if !matches!(response, ServerMessage::Accepted { .. }) {
                return Err(unexpected_response());
            }
            let result = wait_send_result(&mut events, &id, *timeout_ms).await?;
            if result.ok {
                if let Some(content) = result.content {
                    println!("{}", cli::escape_terminal(&content));
                }
            } else {
                return Err(CliError::runtime(
                    result.error.map_or("provider_error", |code| code.as_str()),
                    "agent request failed",
                ));
            }
        }
        WorkspaceCommand::Join { name } => {
            return workspace_join_repl(client, events, workspace_name(name)?).await;
        }
        WorkspaceCommand::Watch { name, after, json } => {
            return watch_workspace(
                client,
                events,
                workspace_name(name)?,
                *after,
                WatchMode::Workspace { json: *json },
                false,
            )
            .await;
        }
    }
    client.close().await.map_err(|error| client_error(&error))?;
    Ok(0)
}
#[derive(Clone, Copy)]
enum WatchMode {
    Workspace { json: bool },
    Task { task_id: Option<i64>, json: bool },
}

async fn workspace_join_repl(
    client: RouterClient,
    mut events: ClientEvents,
    workspace: WorkspaceName,
) -> Result<i32, CliError> {
    let (workspace, cursor) = client
        .workspace_join(workspace)
        .await
        .map_err(|error| client_error(&error))?;
    println!("joined {workspace} at {cursor}; /leave or EOF to leave");

    let (_, mut cursor, _) = subscribe_until_live(
        &client,
        workspace.clone(),
        cursor,
        WatchMode::Workspace { json: false },
    )
    .await?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|error| CliError::runtime("signal_failed", error.to_string()))?;
                leave_and_close(&client).await?;
                return Err(CliError::interrupted("interrupted"));
            }
            line = lines.next_line() => {
                let line = line
                    .map_err(|error| CliError::runtime("stdin_read_failed", error.to_string()))?;
                let Some(line) = line else {
                    break;
                };
                if line == "/leave" {
                    break;
                }
                if line.is_empty() {
                    continue;
                }
                let response = client
                    .call(ClientMessage::WorkspacePost {
                        request_id: request_id("workspace-post"),
                        content: line,
                    })
                    .await
                    .map_err(|error| client_error(&error))?;
                if !matches!(response, ServerMessage::WorkspacePosted { .. }) {
                    return Err(unexpected_response());
                }
            }
            event = events.recv() => {
                let Some(event) = event else {
                    return Err(CliError::runtime(
                        "gateway_disconnected",
                        "router connection closed",
                    ));
                };
                match event.event {
                    ClientEvent::WorkspaceEvent(event) => {
                        cursor = cursor.max(event.seq);
                        output_watch_event(
                            &client,
                            &workspace,
                            event,
                            WatchMode::Workspace { json: false },
                        )?;
                    }
                    ClientEvent::Closed(code) => {
                        return Err(CliError::runtime(code.as_str(), "router connection closed"));
                    }
                    _ => {}
                }
            }
        }
    }
    let _ = cursor;
    client
        .workspace_leave()
        .await
        .map_err(|error| client_error(&error))?;
    client.close().await.map_err(|error| client_error(&error))?;
    println!("left {workspace}");
    Ok(0)
}

async fn watch_workspace(
    client: RouterClient,
    mut events: ClientEvents,
    workspace: WorkspaceName,
    after: i64,
    mode: WatchMode,
    prejoined: bool,
) -> Result<i32, CliError> {
    let workspace = if prejoined {
        workspace
    } else {
        client
            .workspace_join(workspace)
            .await
            .map_err(|error| client_error(&error))?
            .0
    };
    let (_, _, live) = subscribe_until_live(&client, workspace.clone(), after, mode).await?;
    if !live {
        return Err(CliError::runtime(
            "subscription_failed",
            "workspace subscription did not become live",
        ));
    }

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|error| CliError::runtime("signal_failed", error.to_string()))?;
                leave_and_close(&client).await?;
                return Err(CliError::interrupted("interrupted"));
            }
            event = events.recv() => {
                let Some(event) = event else {
                    return Err(CliError::runtime(
                        "gateway_disconnected",
                        "router connection closed",
                    ));
                };
                match event.event {
                    ClientEvent::WorkspaceEvent(event) => {
                        output_watch_event(&client, &workspace, event, mode)?;
                    }
                    ClientEvent::Closed(code) => {
                        return Err(CliError::runtime(code.as_str(), "router connection closed"));
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn subscribe_until_live(
    client: &RouterClient,
    workspace: WorkspaceName,
    after: i64,
    mode: WatchMode,
) -> Result<(WorkspaceName, i64, bool), CliError> {
    let mut cursor = after;
    loop {
        let (page, next_cursor, live) = client
            .workspace_subscribe(cursor)
            .await
            .map_err(|error| client_error(&error))?;
        for event in page {
            output_watch_event(client, &workspace, event, mode)?;
        }
        cursor = next_cursor;
        if live {
            return Ok((workspace, cursor, true));
        }
    }
}

fn output_watch_event(
    client: &RouterClient,
    workspace: &WorkspaceName,
    event: WorkspaceEvent,
    mode: WatchMode,
) -> Result<(), CliError> {
    let should_output = match mode {
        WatchMode::Workspace { json } => {
            if json {
                write_json(&event)?;
            } else {
                print_workspace_event(&event);
            }
            true
        }
        WatchMode::Task { task_id, json } => {
            if event.kind != WorkspaceEventKind::Task
                || task_id.is_some_and(|task_id| event.task_id != Some(task_id))
            {
                false
            } else {
                let content = event.content.as_deref().ok_or_else(|| {
                    CliError::runtime("invalid_task_event", "task event content is missing")
                })?;
                let task_event: TaskEvent = serde_json::from_str(content)
                    .map_err(|error| CliError::runtime("invalid_task_event", error.to_string()))?;
                let history_event = TaskHistoryEvent {
                    seq: event.seq,
                    actor_id: event.actor_id,
                    created_at: event.created_at,
                    event: task_event,
                    attempt: None,
                    report: None,
                };
                if json {
                    write_json(&history_event)?;
                } else {
                    print_task_history_event(&history_event)?;
                }
                true
            }
        }
    };
    let _ = should_output;
    client
        .ack_event(workspace.clone(), event.seq)
        .map_err(|error| client_error(&error))
}

fn print_workspace_event(event: &WorkspaceEvent) {
    println!(
        "{}\t{:?}\t{}\t{}\t{}\t{}",
        event.seq,
        event.kind,
        cli::escape_terminal(&event.actor_id),
        event
            .task_id
            .map_or_else(|| "-".to_owned(), |id| id.to_string()),
        event
            .target_id
            .as_deref()
            .map_or_else(|| "-".to_owned(), cli::escape_terminal),
        event
            .content
            .as_deref()
            .map_or_else(|| "-".to_owned(), cli::escape_terminal),
    );
}

async fn leave_and_close(client: &RouterClient) -> Result<(), CliError> {
    let response = client
        .call(ClientMessage::WorkspaceUnsubscribe {
            request_id: request_id("workspace-unsubscribe"),
        })
        .await
        .map_err(|error| client_error(&error))?;
    if !matches!(response, ServerMessage::WorkspaceUnsubscribed { .. }) {
        return Err(unexpected_response());
    }
    client
        .workspace_leave()
        .await
        .map_err(|error| client_error(&error))?;
    client.close().await.map_err(|error| client_error(&error))
}

async fn task_command(
    cli: &Cli,
    command: &TaskCommand,
    stdin: Option<StdinPayload>,
) -> Result<i32, CliError> {
    let (client, mut events) = operator_client(cli).await?;
    if let TaskCommand::Watch(args) = command {
        let workspace = workspace_name(&args.room)?;
        let prejoined = if let Some(task_id) = args.task {
            client
                .workspace_join(workspace.clone())
                .await
                .map_err(|error| client_error(&error))?;
            client
                .task_get(workspace.clone(), task_id)
                .await
                .map_err(|error| client_error(&error))?;
            true
        } else {
            false
        };
        return watch_workspace(
            client,
            events,
            workspace,
            args.after,
            WatchMode::Task {
                task_id: args.task,
                json: args.json,
            },
            prejoined,
        )
        .await;
    }

    let workspace = workspace_name(task_room(command))?;
    client
        .workspace_join(workspace.clone())
        .await
        .map_err(|error| client_error(&error))?;

    match command {
        TaskCommand::List(args) => {
            let states = task_state_filter(args.all, &args.state)?;
            let (summaries, next_cursor, has_more) = client
                .task_list(
                    workspace.clone(),
                    states,
                    args.assignee.clone(),
                    Some(args.after),
                    Some(args.limit),
                )
                .await
                .map_err(|error| client_error(&error))?;
            let tasks = summaries.iter().map(task_summary_row).collect();
            cli::write_task_page(
                &mut io::stdout().lock(),
                &cli::TaskPage {
                    tasks,
                    next_cursor: Some(next_cursor.to_string()),
                    has_more,
                },
                args.json,
            )?;
        }
        TaskCommand::Show(args) => {
            let task = client
                .task_get(workspace.clone(), args.id)
                .await
                .map_err(|error| client_error(&error))?;
            if args.json {
                write_json(&task)?;
            } else {
                print_task_detail(&task)?;
            }
        }
        TaskCommand::History(args) => {
            let page = client
                .task_history(
                    workspace.clone(),
                    args.id,
                    Some(args.after),
                    Some(args.limit),
                )
                .await
                .map_err(|error| client_error(&error))?;
            if args.json {
                write_json(&page)?;
            } else {
                print_task_history(&page)?;
            }
        }
        TaskCommand::Create(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            let object = stdin_json(stdin)?;
            let title = json_string(&object, "title")?;
            let description = json_string(&object, "description")?;
            run_task_mutation(
                &client,
                ClientMessage::TaskCreate {
                    request_id: request_id("task-create"),
                    workspace: workspace.clone(),
                    operation_id,
                    title,
                    description,
                },
                operation_id,
                args.mutation.json,
            )
            .await?;
        }
        TaskCommand::Edit(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            let object = stdin_json(stdin)?;
            let title = json_optional_string(&object, "title")?;
            let description = json_optional_string(&object, "description")?;
            run_task_mutation(
                &client,
                ClientMessage::TaskEdit {
                    request_id: request_id("task-edit"),
                    workspace: workspace.clone(),
                    operation_id,
                    task_id: args.id,
                    expected_version: args.expected_version,
                    title,
                    description,
                },
                operation_id,
                args.mutation.json,
            )
            .await?;
        }
        TaskCommand::Assign(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            run_task_mutation(
                &client,
                ClientMessage::TaskAssign {
                    request_id: request_id("task-assign"),
                    workspace: workspace.clone(),
                    operation_id,
                    task_id: args.id,
                    expected_version: args.expected_version,
                    agent_id: args.agent.clone(),
                },
                operation_id,
                args.mutation.json,
            )
            .await?;
        }
        TaskCommand::Note(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            run_task_mutation(
                &client,
                ClientMessage::TaskNote {
                    request_id: request_id("task-note"),
                    workspace: workspace.clone(),
                    operation_id,
                    task_id: args.id,
                    text: stdin_text(stdin)?,
                },
                operation_id,
                args.mutation.json,
            )
            .await?;
        }
        TaskCommand::Cancel(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            run_task_mutation(
                &client,
                ClientMessage::TaskCancel {
                    request_id: request_id("task-cancel"),
                    workspace: workspace.clone(),
                    operation_id,
                    task_id: args.id,
                    expected_version: args.expected_version,
                    note: stdin_text(stdin)?,
                },
                operation_id,
                args.mutation.json,
            )
            .await?;
        }
        TaskCommand::Reopen(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            run_task_mutation(
                &client,
                ClientMessage::TaskReopen {
                    request_id: request_id("task-reopen"),
                    workspace: workspace.clone(),
                    operation_id,
                    task_id: args.id,
                    expected_version: args.expected_version,
                    note: stdin_text(stdin)?,
                },
                operation_id,
                args.mutation.json,
            )
            .await?;
        }
        TaskCommand::Interrupt(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            run_task_mutation(
                &client,
                ClientMessage::TaskInterrupt {
                    request_id: request_id("task-interrupt"),
                    workspace: workspace.clone(),
                    operation_id,
                    task_id: args.id,
                    expected_version: args.expected_version,
                    note: stdin_text(stdin)?,
                },
                operation_id,
                args.mutation.json,
            )
            .await?;
        }
        TaskCommand::ConfirmStopped(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            run_task_mutation(
                &client,
                ClientMessage::TaskConfirmStopped {
                    request_id: request_id("task-confirm-stopped"),
                    workspace: workspace.clone(),
                    operation_id,
                    task_id: args.id,
                    attempt_id: args.attempt,
                    expected_version: args.expected_version,
                    note: stdin_text(stdin)?,
                },
                operation_id,
                args.mutation.json,
            )
            .await?;
        }
        TaskCommand::Request(args) => {
            let id = request_id("task-request");
            let response = client
                .call(ClientMessage::TaskRequest {
                    request_id: id.clone(),
                    workspace: workspace.clone(),
                    task_id: args.id,
                    expected_version: args.expected_version,
                    message: stdin
                        .map(|payload| match payload {
                            StdinPayload::Text(value) => Ok(value),
                            StdinPayload::Json(_) => Err(CliError::usage(
                                "invalid_stdin",
                                "task request input must be text",
                            )),
                        })
                        .transpose()?,
                    timeout_ms: args.timeout_ms,
                })
                .await
                .map_err(|error| client_error(&error))?;
            if !matches!(response, ServerMessage::Accepted { .. }) {
                return Err(unexpected_response());
            }
            let result = wait_send_result(&mut events, &id, args.timeout_ms).await?;
            if !result.ok {
                return Err(CliError::runtime(
                    result.error.map_or("provider_error", |code| code.as_str()),
                    "task request failed",
                ));
            }
            if args.json {
                write_json(&json!({
                    "requestId": result.request_id,
                    "workspace": result.workspace,
                    "from": result.from,
                    "ok": result.ok,
                    "content": result.content,
                    "error": result.error,
                }))?;
            } else if let Some(content) = result.content {
                println!("{}", cli::escape_terminal(&content));
            } else {
                println!("completed");
            }
        }
        TaskCommand::Import(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            let operation = client
                .task_import(
                    workspace.clone(),
                    external_provider(args.provider),
                    args.external_id.clone(),
                    operation_id,
                )
                .await
                .map_err(|error| mutation_error(&error, operation_id))?;
            print_external_operation(&operation, args.mutation.json)?;
        }
        TaskCommand::Link(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            let operation = client
                .task_link(
                    workspace.clone(),
                    external_provider(args.provider),
                    args.external_id.clone(),
                    args.id,
                    args.expected_version,
                    operation_id,
                    args.replace,
                )
                .await
                .map_err(|error| mutation_error(&error, operation_id))?;
            print_external_operation(&operation, args.mutation.json)?;
        }
        TaskCommand::Publish(args) => {
            let operation_id = mutation_id(args.mutation.operation_id);
            let operation = client
                .task_publish(
                    workspace.clone(),
                    external_provider(args.provider),
                    args.id,
                    args.expected_version,
                    operation_id,
                    publish_kind(args.kind),
                    args.report,
                )
                .await
                .map_err(|error| mutation_error(&error, operation_id))?;
            print_external_operation(&operation, args.mutation.json)?;
        }
        TaskCommand::ExternalStatus(args) => {
            let (operation, resolution) = client
                .task_external_status(workspace.clone(), args.operation_id)
                .await
                .map_err(|error| mutation_error(&error, args.operation_id))?;
            print_external_status(&operation, resolution.as_ref(), args.json)?;
        }
        TaskCommand::ExternalResolve(args) => {
            let resolution_id = args.resolution_id.unwrap_or_else(Uuid::new_v4);
            let outcome = if args.resolution.applied {
                ExternalResolutionOutcome::Applied
            } else {
                ExternalResolutionOutcome::NotApplied
            };
            let (operation, resolution) = client
                .task_external_resolve(
                    workspace.clone(),
                    args.operation_id,
                    resolution_id,
                    outcome,
                    args.resolution.external_id.clone(),
                    stdin_text(stdin)?,
                )
                .await
                .map_err(|error| resolution_error(&error, resolution_id))?;
            print_external_status(&operation, Some(&resolution), args.json)?;
        }
        TaskCommand::Watch(_) => unreachable!("task watch returned before workspace setup"),
    }

    client.close().await.map_err(|error| client_error(&error))?;
    Ok(0)
}

async fn integration_command(cli: &Cli, command: &IntegrationCommand) -> Result<i32, CliError> {
    let (client, _events) = operator_client(cli).await?;
    let (integrations, json_output) = match command {
        IntegrationCommand::List { room, json } => {
            let workspace = workspace_name(room)?;
            client
                .workspace_join(workspace.clone())
                .await
                .map_err(|error| client_error(&error))?;
            (
                client
                    .integration_list(workspace)
                    .await
                    .map_err(|error| client_error(&error))?,
                *json,
            )
        }
        IntegrationCommand::Check {
            room,
            provider,
            json,
        } => {
            let workspace = workspace_name(room)?;
            client
                .workspace_join(workspace.clone())
                .await
                .map_err(|error| client_error(&error))?;
            (
                vec![
                    client
                        .integration_check(workspace, external_provider(*provider))
                        .await
                        .map_err(|error| client_error(&error))?,
                ],
                *json,
            )
        }
        IntegrationCommand::Admin(args) => match &args.command {
            IntegrationAdminCommand::Reload { json } => (
                client
                    .integration_reload()
                    .await
                    .map_err(|error| client_error(&error))?,
                *json,
            ),
        },
    };
    print_integrations(&integrations, json_output)?;
    client.close().await.map_err(|error| client_error(&error))?;
    Ok(0)
}

fn task_room(command: &TaskCommand) -> &str {
    match command {
        TaskCommand::List(args) => &args.room,
        TaskCommand::Show(args) => &args.room,
        TaskCommand::History(args) => &args.room,
        TaskCommand::Watch(args) => &args.room,
        TaskCommand::Create(args) => &args.room,
        TaskCommand::Edit(args) => &args.room,
        TaskCommand::Assign(args) => &args.room,
        TaskCommand::Note(args) => &args.room,
        TaskCommand::Cancel(args) | TaskCommand::Reopen(args) | TaskCommand::Interrupt(args) => {
            &args.room
        }
        TaskCommand::ConfirmStopped(args) => &args.room,
        TaskCommand::Request(args) => &args.room,
        TaskCommand::Import(args) => &args.room,
        TaskCommand::Link(args) => &args.room,
        TaskCommand::Publish(args) => &args.room,
        TaskCommand::ExternalStatus(args) => &args.room,
        TaskCommand::ExternalResolve(args) => &args.room,
    }
}

fn task_state_filter(all: bool, states: &[String]) -> Result<Option<Vec<TaskState>>, CliError> {
    if all {
        return Ok(Some(vec![
            TaskState::Todo,
            TaskState::InProgress,
            TaskState::Blocked,
            TaskState::Paused,
            TaskState::Done,
            TaskState::Cancelled,
        ]));
    }
    if states.is_empty() {
        return Ok(None);
    }
    states
        .iter()
        .map(|state| match state.as_str() {
            "todo" => Ok(TaskState::Todo),
            "in_progress" => Ok(TaskState::InProgress),
            "blocked" => Ok(TaskState::Blocked),
            "paused" => Ok(TaskState::Paused),
            "done" => Ok(TaskState::Done),
            "cancelled" => Ok(TaskState::Cancelled),
            _ => Err(CliError::usage(
                "invalid_task_state",
                format!("unsupported task state: {}", cli::escape_terminal(state)),
            )),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn task_summary_row(task: &TaskSummary) -> cli::TaskRow {
    let current = task
        .current_attempt_id
        .zip(task.execution_session_id)
        .zip(task.last_executor_id.as_ref())
        .map(
            |((attempt_id, session_id), executor)| cli::TaskRunIdentity {
                executor: executor.clone(),
                attempt_id,
                session_id,
            },
        );
    let stop_evidence = match task.stop_evidence {
        None => cli::StopEvidence::None,
        Some(TaskStopEvidence::Released) => cli::StopEvidence::Released,
        Some(TaskStopEvidence::Confirmed) => cli::StopEvidence::Confirmed,
        Some(TaskStopEvidence::Unknown) if current.is_some() => cli::StopEvidence::RunningUnknown,
        Some(TaskStopEvidence::Unknown) => cli::StopEvidence::InterruptedUnknown,
    };
    cli::TaskRow {
        id: task.id,
        state: task.state.as_str().to_owned(),
        current,
        last: None,
        last_executor_id: task.last_executor_id.clone(),
        execution_session_id: task.execution_session_id,
        assigned_agent_id: task.assigned_agent_id.clone(),
        checkpoint: task.last_checkpoint_at.map(|value| value.to_string()),
        stop_evidence,
        title: task.title.clone(),
    }
}

async fn run_task_mutation(
    client: &RouterClient,
    message: ClientMessage,
    operation_id: Uuid,
    json_output: bool,
) -> Result<(), CliError> {
    let result = client
        .task_mutation(message)
        .await
        .map_err(|error| mutation_error(&error, operation_id))?;
    print_task_mutation(&result, json_output)
}

fn print_task_mutation(result: &TaskMutationResult, json_output: bool) -> Result<(), CliError> {
    if json_output {
        write_json(result)
    } else {
        println!(
            "operationId={}\ttaskId={}\tversion={}\tstate={}\treportId={}",
            result.operation_id,
            result.task.summary.id,
            result.applied_version,
            result.task.summary.state.as_str(),
            result
                .report_id
                .map_or_else(|| "-".to_owned(), |id| id.to_string()),
        );
        Ok(())
    }
}

fn print_task_detail(task: &TaskDetail) -> Result<(), CliError> {
    let summary = &task.summary;
    print_field("id", &summary.id.to_string());
    print_field("workspace", &summary.workspace);
    print_field("state", summary.state.as_str());
    print_field("version", &summary.version.to_string());
    print_field("title", &summary.title);
    print_field("description", &task.description);
    print_field(
        "assignedAgentId",
        optional_string(summary.assigned_agent_id.as_deref()),
    );
    print_field(
        "currentAttemptId",
        &optional_uuid(summary.current_attempt_id),
    );
    print_field(
        "lastExecutorId",
        optional_string(summary.last_executor_id.as_deref()),
    );
    print_field(
        "executionSessionId",
        &optional_uuid(summary.execution_session_id),
    );
    print_field(
        "lastCheckpointAt",
        &summary
            .last_checkpoint_at
            .map_or_else(|| "-".to_owned(), |value| value.to_string()),
    );
    print_field(
        "pauseReason",
        summary.pause_reason.map_or("-", |reason| reason.as_str()),
    );
    print_field(
        "stopEvidence",
        summary
            .stop_evidence
            .map_or("-", |evidence| evidence.as_str()),
    );
    print_field("createdAt", &summary.created_at.to_string());
    print_field("updatedAt", &summary.updated_at.to_string());
    print_field("createdBy", &task.created_by);
    print_field("updatedBy", &task.updated_by);
    print_serialized_field("currentAttempt", &task.current_attempt)?;
    print_serialized_field("lastAttempt", &task.last_attempt)?;
    print_serialized_field("checkpoint", &task.checkpoint)?;
    print_serialized_field("result", &task.result)?;
    print_serialized_field("links", &task.links)?;
    print_serialized_field("externalOperations", &task.external_operations)
}

fn print_task_history(page: &TaskHistoryPage) -> Result<(), CliError> {
    println!(
        "SEQ\tCREATED\tACTOR\tCHANGE\tTASK\tVERSION\tSTATE\tATTEMPT\tREPORT\tEXTERNAL_OPERATION\tTITLE\tATTEMPT_DETAIL\tREPORT_DETAIL"
    );
    for event in &page.events {
        print_task_history_event(event)?;
    }
    println!("nextCursor={} hasMore={}", page.next_cursor, page.has_more);
    Ok(())
}

fn print_task_history_event(event: &TaskHistoryEvent) -> Result<(), CliError> {
    let attempt = serde_json::to_string(&event.attempt)
        .map_err(|error| CliError::runtime("output_encode_failed", error.to_string()))?;
    let report = serde_json::to_string(&event.report)
        .map_err(|error| CliError::runtime("output_encode_failed", error.to_string()))?;
    println!(
        "{}\t{}\t{}\t{:?}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        event.seq,
        event.created_at,
        cli::escape_terminal(&event.actor_id),
        event.event.change,
        event.event.task.id,
        event.event.task.version,
        event.event.task.state.as_str(),
        optional_uuid(event.event.attempt_id),
        optional_uuid(event.event.report_id),
        optional_uuid(event.event.external_operation_id),
        cli::escape_terminal(&event.event.task.title),
        cli::escape_terminal(&attempt),
        cli::escape_terminal(&report),
    );
    Ok(())
}

fn print_external_operation(
    operation: &ExternalOperationSummary,
    json_output: bool,
) -> Result<(), CliError> {
    if json_output {
        write_json(operation)
    } else {
        println!(
            "operationId={}\ttaskId={}\tprovider={}\tkind={:?}\tstatus={:?}\texternalId={}\turl={}\terror={}",
            operation.id,
            operation
                .task_id
                .map_or_else(|| "-".to_owned(), |id| id.to_string()),
            operation.provider.as_str(),
            operation.kind,
            operation.status,
            operation
                .external_id
                .as_deref()
                .map_or_else(|| "-".to_owned(), cli::escape_terminal),
            operation
                .url
                .as_deref()
                .map_or_else(|| "-".to_owned(), cli::escape_terminal),
            operation
                .error
                .as_deref()
                .map_or_else(|| "-".to_owned(), cli::escape_terminal),
        );
        Ok(())
    }
}

fn print_external_status(
    operation: &ExternalOperationSummary,
    resolution: Option<&ExternalResolution>,
    json_output: bool,
) -> Result<(), CliError> {
    if json_output {
        write_json(&json!({
            "operation": operation,
            "resolution": resolution,
        }))
    } else {
        print_external_operation(operation, false)?;
        match resolution {
            Some(resolution) => println!(
                "resolutionId={}\toutcome={:?}\texternalId={}\tnote={}",
                resolution.id,
                resolution.outcome,
                resolution
                    .external_id
                    .as_deref()
                    .map_or_else(|| "-".to_owned(), cli::escape_terminal),
                cli::escape_terminal(&resolution.note),
            ),
            None => println!("resolution=-"),
        }
        Ok(())
    }
}

fn print_integrations(
    integrations: &[IntegrationPublic],
    json_output: bool,
) -> Result<(), CliError> {
    if json_output {
        write_json(&json!({ "integrations": integrations }))
    } else {
        println!("PROVIDER\tWORKSPACE\tTARGET\tACCESS\tAVAILABLE\tERROR");
        for integration in integrations {
            println!(
                "{}\t{}\t{}\t{:?}\t{}\t{}",
                integration.provider.as_str(),
                integration.workspace,
                cli::escape_terminal(&integration.target),
                integration.access,
                integration.available,
                integration
                    .error
                    .as_deref()
                    .map_or_else(|| "-".to_owned(), cli::escape_terminal),
            );
        }
        Ok(())
    }
}

fn print_field(label: &str, value: &str) {
    println!("{label}\t{}", cli::escape_terminal(value));
}

fn print_serialized_field<T: Serialize>(label: &str, value: &T) -> Result<(), CliError> {
    let value = serde_json::to_string(value)
        .map_err(|error| CliError::runtime("output_encode_failed", error.to_string()))?;
    print_field(label, &value);
    Ok(())
}

fn optional_uuid(value: Option<Uuid>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn optional_string(value: Option<&str>) -> &str {
    value.unwrap_or("-")
}

fn mutation_id(explicit: Option<Uuid>) -> Uuid {
    explicit.unwrap_or_else(Uuid::new_v4)
}

fn mutation_error(error: &crate::client::ClientError, operation_id: Uuid) -> CliError {
    let mut error = client_error(error);
    error.operation_id = Some(operation_id);
    error
}
fn resolution_error(error: &crate::client::ClientError, resolution_id: Uuid) -> CliError {
    let mut error = client_error(error);
    error.resolution_id = Some(resolution_id);
    error
}

fn stdin_json(stdin: Option<StdinPayload>) -> Result<Value, CliError> {
    match stdin {
        Some(StdinPayload::Json(value)) => Ok(value),
        _ => Err(CliError::usage(
            "invalid_stdin",
            "this command requires JSON on stdin",
        )),
    }
}

fn json_string(value: &Value, field: &str) -> Result<String, CliError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| CliError::usage("invalid_stdin", format!("{field} must be a string")))
}

fn json_optional_string(value: &Value, field: &str) -> Result<Option<String>, CliError> {
    value
        .get(field)
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                CliError::usage("invalid_stdin", format!("{field} must be a string"))
            })
        })
        .transpose()
}

fn external_provider(provider: cli::ExternalProvider) -> crate::tasks::ExternalProvider {
    match provider {
        cli::ExternalProvider::Github => crate::tasks::ExternalProvider::Github,
        cli::ExternalProvider::Linear => crate::tasks::ExternalProvider::Linear,
    }
}

fn publish_kind(kind: cli::PublishKind) -> ExternalPublishKind {
    match kind {
        cli::PublishKind::Issue => ExternalPublishKind::Issue,
        cli::PublishKind::Report => ExternalPublishKind::Report,
    }
}

async fn wait_send_result(
    events: &mut ClientEvents,
    request_id: &str,
    timeout_ms: Option<u64>,
) -> Result<crate::client::AgentSendResult, CliError> {
    let duration = Duration::from_millis(timeout_ms.unwrap_or(60_000).saturating_add(1_000));
    tokio::time::timeout(duration, async {
        loop {
            let event = events.recv().await.ok_or_else(|| {
                CliError::runtime("gateway_disconnected", "router connection closed")
            })?;
            if let ClientEvent::SendResult(result) = event.event
                && result.request_id == request_id
            {
                return Ok(result);
            }
        }
    })
    .await
    .map_err(|_| CliError::runtime("request_timeout", "agent request timed out"))?
}

async fn onboarding_command(cli: &Cli, command: &cli::OnboardingCommand) -> Result<i32, CliError> {
    match command {
        cli::OnboardingCommand::Install { provider, .. } => {
            // Keep token-bearing input out of the general, Debug-visible stdin payload.
            let bytes =
                cli::read_bounded(&mut io::stdin().lock(), crate::onboarding::MAX_TICKET_BYTES)?;
            let ticket = crate::onboarding::OnboardingTicket::parse(&bytes)
                .map_err(|error| CliError::usage("invalid_ticket", error.to_string()))?;
            let report = crate::onboarding::install::install(&ticket, *provider).await?;
            write_json(&report)?;
        }
        cli::OnboardingCommand::Resume {
            invite_id,
            provider,
        } => {
            let report = crate::onboarding::install::resume(*invite_id, *provider).await?;
            write_json(&report)?;
        }
        cli::OnboardingCommand::Status { provider, json } => {
            let profile = cli.profile.as_deref().ok_or_else(|| {
                CliError::usage("onboarding_profile_required", "use --profile NAME")
            })?;
            let report = crate::onboarding::install::status(profile, *provider).await?;
            if *json {
                write_json(&report)?;
            } else {
                print_field("profile", &report.profile);
                print_field("provider", report.provider.as_str());
                print_field("serverId", &report.server_id.to_string());
                print_field("workspace", report.workspace.as_str());
                print_field("route", &report.route);
                print_serialized_field("stage", &report.stage)?;
                print_field("transport", report.transport.unwrap_or("unreachable"));
                print_field("activation", report.activation);
                print_field("nextAction", &report.next_action);
            }
        }
        cli::OnboardingCommand::Prompt(args) => {
            if cli.profile.is_some() || cli.credential.is_some() {
                return Err(CliError::usage(
                    "owned_server_required",
                    "onboarding prompt uses the owned server; omit --profile and --credential",
                ));
            }
            let prompt = crate::onboarding::issue::issue_prompt(&absolute_data_dir()?, crate::onboarding::issue::PromptOptions {
                workspace: workspace_name(&args.workspace)?, create_workspace: args.create_workspace,
                name: args.name.clone(), provider: args.provider, endpoints: args.endpoints.clone(), ca_file: args.ca_file.clone(),
            }).await.map_err(|error| {
                let message = match error.0 {
                    "router_not_running" => "start the owned server with asr router start",
                    "bootstrap_assets_missing" => "place the asr-bootstrap-bundle Actions artifact in ASR_BOOTSTRAP_DIR or ASR_DATA_DIR/bootstrap before issuing invitations",
                    _ => error.0,
                };
                CliError::runtime(error.0, message)
            })?;
            print!("{}", prompt.text);
        }
        cli::OnboardingCommand::Revoke { invite_id } => {
            let (client, _events) = operator_client(cli).await?;
            let response = client
                .call(ClientMessage::OnboardingInviteRevoke {
                    request_id: request_id("onboarding-revoke"),
                    invite_id: *invite_id,
                })
                .await
                .map_err(|error| client_error(&error))?;
            client.close().await.map_err(|error| client_error(&error))?;
            match response {
                ServerMessage::OnboardingInviteRevoked { .. } => println!("revoked {invite_id}"),
                ServerMessage::Error { code, .. } => {
                    return Err(CliError::runtime(code.as_str(), code.to_string()));
                }
                _ => return Err(unexpected_response()),
            }
        }
    }
    Ok(0)
}

async fn credential_command(cli: &Cli, command: &CredentialCommand) -> Result<i32, CliError> {
    let (client, _events) = operator_client(cli).await?;
    match command {
        CredentialCommand::Issue(args) => issue_credential(&client, args).await?,
        CredentialCommand::List { json } => {
            let mut after = None;
            loop {
                let response = client
                    .call(ClientMessage::CredentialList {
                        request_id: request_id("credential-list"),
                        after,
                        limit: Some(cli::MAX_PAGE_LIMIT),
                    })
                    .await
                    .map_err(|error| client_error(&error))?;
                let ServerMessage::Credentials {
                    credentials,
                    next_cursor,
                    has_more,
                    ..
                } = response
                else {
                    return Err(unexpected_response());
                };
                for credential in credentials {
                    if *json {
                        write_json(&credential)?;
                    } else {
                        println!(
                            "{}\t{}\t{}\trevoked={}",
                            credential.claims.id,
                            credential.claims.role.as_str(),
                            credential.claims.subject,
                            credential.revoked_at.is_some()
                        );
                    }
                }
                if !has_more {
                    break;
                }
                after = next_cursor;
            }
        }
        CredentialCommand::Revoke { id } => {
            let id = Uuid::parse_str(id)
                .map_err(|_| CliError::usage("invalid_credential", "invalid credential id"))?;
            let response = client
                .call(ClientMessage::CredentialRevoke {
                    request_id: request_id("credential-revoke"),
                    id,
                })
                .await
                .map_err(|error| client_error(&error))?;
            if !matches!(response, ServerMessage::CredentialRevoked { .. }) {
                return Err(unexpected_response());
            }
            println!("revoked {id}");
        }
    }
    client.close().await.map_err(|error| client_error(&error))?;
    Ok(0)
}

async fn issue_credential(
    client: &RouterClient,
    args: &CredentialIssueArgs,
) -> Result<(), CliError> {
    let (role, subject, side, agent_client, workspaces) = if let Some(operator) = &args.operator {
        (
            CredentialRole::Operator,
            operator.clone(),
            None,
            None,
            Vec::new(),
        )
    } else {
        let subject = args
            .agent
            .clone()
            .ok_or_else(|| CliError::usage("invalid_credential", "--agent is required"))?;
        let side = args
            .side
            .map(map_agent_side)
            .ok_or_else(|| CliError::usage("invalid_credential", "--side is required"))?;
        let agent_client = args
            .client
            .map(map_agent_client)
            .ok_or_else(|| CliError::usage("invalid_credential", "--client is required"))?;
        let workspaces = args
            .workspace
            .iter()
            .map(|workspace| workspace_name(workspace))
            .collect::<Result<Vec<_>, _>>()?;
        (
            CredentialRole::Agent,
            subject,
            Some(side),
            Some(agent_client),
            workspaces,
        )
    };
    let response = client
        .call(ClientMessage::CredentialIssue {
            request_id: request_id("credential-issue"),
            role,
            subject,
            agent_side: side,
            agent_client,
            workspaces,
        })
        .await
        .map_err(|error| client_error(&error))?;
    let ServerMessage::CredentialIssued { credential, .. } = response else {
        return Err(unexpected_response());
    };
    let output = if let Some(path) = &args.output {
        path.clone()
    } else {
        absolute_data_dir()?
            .join("credentials")
            .join(format!("issued-{}.json", credential.id))
    };
    if let Err(error) = write_credential_exclusive(&output, &credential) {
        let _ = client
            .call(ClientMessage::CredentialRevoke {
                request_id: request_id("credential-rollback"),
                id: credential.id,
            })
            .await;
        return Err(CliError::runtime(
            "credential_write_failed",
            error.to_string(),
        ));
    }
    println!("issued {} to {}", credential.id, output.display());
    Ok(())
}

fn operator_client_config(cli: &Cli) -> Result<(ClientConfig, Option<String>), CliError> {
    let selection = config::select(cli.profile.as_deref(), cli.credential.as_deref())
        .map_err(|error| config_error(&error))?;
    let credential_path = match selection.credential_file {
        Some(path) => path,
        None => owned_local_admin_path(&selection.router_url)?.ok_or_else(|| {
            CliError::runtime(
                "configuration_required",
                "pass --credential for non-owned or non-loopback routers",
            )
        })?,
    };
    let credential = read_credential(&credential_path)
        .map_err(|error| CliError::runtime("credential_invalid", error.to_string()))?;
    let ca_file = env::var_os("ASR_CA_FILE").map(PathBuf::from);
    Ok((
        ClientConfig {
            router_url: selection.router_url,
            role: ClientRole::Operator { credential },
            ca_file,
        },
        selection.profile,
    ))
}

async fn operator_client(cli: &Cli) -> Result<(RouterClient, ClientEvents), CliError> {
    let (config, _) = operator_client_config(cli)?;
    RouterClient::connect(config)
        .await
        .map_err(|error| client_error(&error))
}

fn owned_local_admin_path(router_url: &Url) -> Result<Option<PathBuf>, CliError> {
    let local = router_url.host_str().is_some_and(|host| {
        host == "localhost" || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
    });
    if !local {
        return Ok(None);
    }
    let data_dir = absolute_data_dir()?;
    if ensure_private_directory(&data_dir, false).is_err() {
        return Ok(None);
    }
    let store = RuntimeStore::new(data_dir.clone()).map_err(|error| launch_error(&error))?;
    let Some(record) = store.read().map_err(|error| launch_error(&error))? else {
        return Ok(None);
    };
    if record.control_url != router_url.as_str() {
        return Ok(None);
    }
    Ok(Some(data_dir.join(ADMIN_CREDENTIAL)))
}

async fn doctor_command(cli: &Cli) -> Result<i32, CliError> {
    let selection = config::select(cli.profile.as_deref(), cli.credential.as_deref())
        .map_err(|error| config_error(&error))?;
    println!("router\t{}", selection.router_url);
    for executable in ["tailscale", "codex", "claude", "omp"] {
        println!(
            "{executable}\t{}",
            if find_executable(executable).is_some() {
                "available"
            } else {
                "optional-unavailable"
            }
        );
    }
    let data_dir = absolute_data_dir()?;
    if ensure_private_directory(&data_dir, false).is_ok() {
        let store = RuntimeStore::new(data_dir).map_err(|error| launch_error(&error))?;
        if let Some(record) = store.read().map_err(|error| launch_error(&error))? {
            let mut health = ReqwestHealthProbe::new(env::var_os("ASR_CA_FILE").map(PathBuf::from));
            health
                .probe(&record)
                .await
                .map_err(|failure| CliError::runtime("router_unhealthy", format!("{failure:?}")))?;
            println!("health\tok");
        } else {
            println!("health\tnot-running");
        }
    } else {
        println!("health\tnot-running");
    }
    Ok(0)
}

fn absolute_data_dir() -> Result<PathBuf, CliError> {
    let path = config::data_dir().map_err(|error| config_error(&error))?;
    if path.is_absolute() {
        Ok(path)
    } else {
        env::current_dir()
            .map(|current| current.join(path))
            .map_err(|error| CliError::runtime("configuration_io", error.to_string()))
    }
}

fn workspace_name(value: &str) -> Result<WorkspaceName, CliError> {
    config::validate_workspace_argument(value).map_err(|error| config_error(&error))
}

fn stdin_text(stdin: Option<StdinPayload>) -> Result<String, CliError> {
    match stdin {
        Some(StdinPayload::Text(value)) => Ok(value),
        _ => Err(CliError::usage(
            "invalid_stdin",
            "this command requires text on stdin",
        )),
    }
}

fn map_agent_side(side: cli::AgentSide) -> AgentSide {
    match side {
        cli::AgentSide::Claude => AgentSide::Claude,
        cli::AgentSide::Codex => AgentSide::Codex,
        cli::AgentSide::Generic => AgentSide::Generic,
    }
}

fn map_agent_client(client: cli::AgentClient) -> AgentClient {
    match client {
        cli::AgentClient::Omp => AgentClient::Omp,
        cli::AgentClient::ClaudeCode => AgentClient::ClaudeCode,
        cli::AgentClient::ClaudeSdk => AgentClient::ClaudeSdk,
        cli::AgentClient::CodexCli => AgentClient::CodexCli,
        cli::AgentClient::CodexAppServer => AgentClient::CodexAppServer,
        cli::AgentClient::Generic => AgentClient::Generic,
    }
}

fn request_id(prefix: &str) -> String {
    format!("{prefix}:{}", Uuid::new_v4())
}

fn write_json(value: &impl Serialize) -> Result<(), CliError> {
    let value = serde_json::to_value(value)
        .map_err(|error| CliError::runtime("output_encode_failed", error.to_string()))?;
    cli::write_json_line(&mut io::stdout().lock(), &value)
}

fn config_error(error: &config::ConfigError) -> CliError {
    let message = match error {
        config::ConfigError::Required => "configuration_required",
        config::ConfigError::Invalid => "invalid_configuration",
        config::ConfigError::InvalidUrl => "invalid_router_url",
        config::ConfigError::InvalidProfile => "invalid_profile",
        config::ConfigError::InvalidDelegateContext => "invalid_delegate_context",
        config::ConfigError::ProfileConflict => "profile_conflict",
        config::ConfigError::BindingConflict => "binding_conflict",
        config::ConfigError::Io(_) => "configuration_io",
    };
    CliError::runtime("configuration_error", message)
}

fn host_error(error: &HostError) -> CliError {
    CliError::runtime("host_error", error.to_string())
}

fn client_error(error: &crate::client::ClientError) -> CliError {
    let message = match error {
        crate::client::ClientError::Router(code) => code.as_str(),
        crate::client::ClientError::Disconnected => "gateway_disconnected",
        crate::client::ClientError::MessageTooLarge => "message_too_large",
        crate::client::ClientError::QueueFull => "client_queue_full",
        crate::client::ClientError::Closed => "client_closed",
        crate::client::ClientError::Transport => "transport_error",
    };
    CliError::runtime("router_error", message)
}

fn launch_error(error: &RouterLaunchError) -> CliError {
    if matches!(error, RouterLaunchError::InstanceChanged) {
        return CliError::runtime(
            "router_instance_changed",
            "the owned router instance changed; the replacement was not stopped; run `asr ui` to reattach",
        );
    }
    let message = match error {
        RouterLaunchError::RouterBusy => "router_busy",
        RouterLaunchError::InstanceChanged => "router_instance_changed",
        RouterLaunchError::Interrupted => {
            return CliError::interrupted("foreground router startup interrupted");
        }
        RouterLaunchError::Permissions => "launcher_permissions",
        RouterLaunchError::Io => "launcher_io",
        RouterLaunchError::InvalidRuntimeRecord => "runtime_record_invalid",
        RouterLaunchError::RuntimeRecordTooLarge => "runtime_record_too_large",
        RouterLaunchError::StartupProtocol => "startup_protocol_error",
        RouterLaunchError::StartupTimeout => "startup_timeout",
        RouterLaunchError::ShutdownTimeout => "shutdown_timeout",
        RouterLaunchError::ChildLaunch => "child_launch_failed",
        RouterLaunchError::ChildTerminated => "child_terminated",
        RouterLaunchError::Health(_) => "health_check_failed",
        RouterLaunchError::AdminAuthentication => "admin_authentication_failed",
        RouterLaunchError::AdminProtocol => "admin_protocol_error",
        RouterLaunchError::TlsRequired => "tls_configuration_required",
        RouterLaunchError::TlsInvalid => "tls_configuration_invalid",
        RouterLaunchError::TailscaleUnavailable => "tailscale_unavailable",
        RouterLaunchError::TailscaleStatus => "tailscale_status_invalid",
        RouterLaunchError::TailscaleMappingConflict => "tailscale_mapping_conflict",
        RouterLaunchError::TailscaleCommand => "tailscale_command_failed",
        RouterLaunchError::Profile => "profile_update_failed",
    };
    CliError::runtime("router_launch_failed", message)
}

fn router_error(error: &crate::router::RouterRuntimeError) -> CliError {
    let message = match error {
        crate::router::RouterRuntimeError::Configuration => "configuration_required",
        crate::router::RouterRuntimeError::Store(_) => "store_error",
        crate::router::RouterRuntimeError::Credential(_) => "credential_error",
        crate::router::RouterRuntimeError::Bootstrap(_) => "bootstrap_assets_invalid",
        crate::router::RouterRuntimeError::Io(_) => "router_io",
        crate::router::RouterRuntimeError::ActorStopped => "router_actor_stopped",
    };
    CliError::runtime("router_runtime_failed", message)
}

fn unexpected_response() -> CliError {
    CliError::runtime("protocol_error", "router returned an unexpected response")
}

fn exit_code(code: i32) -> ExitCode {
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}

#[cfg(all(test, unix))]
mod console_tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt as _};

    fn isolated(test: &str, environment: Option<&str>) -> bool {
        if env::var("ASR_CONSOLE_TEST_CHILD").as_deref() == Ok(test) {
            return false;
        }
        let root = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()
            .expect("private console test home");
        let home = root
            .path()
            .canonicalize()
            .expect("canonical console test home");
        let mut command = std::process::Command::new(env::current_exe().expect("test executable"));
        command
            .args(["--exact", test, "--nocapture"])
            .env_clear()
            .env("ASR_CONSOLE_TEST_CHILD", test)
            .env("HOME", &home)
            .env("ASR_DATA_DIR", home.join("data"))
            .env("ASR_CONFIG_PATH", home.join("config.json"));
        if environment.is_some() {
            command.env("ASR_CREDENTIAL_FILE", home.join("scoped.json"));
        }
        if environment == Some("route") {
            command.env("ROUTER_URL", "wss://environment.example.test/ws");
        }
        let output = command.output().expect("isolated selection test");
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        true
    }

    async fn fixture() -> (RouterRuntime, RuntimeRecord, PathBuf) {
        let data_dir = absolute_data_dir().unwrap();
        let instance_id = Uuid::new_v4();
        let runtime = RouterRuntime::start(RouterConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            data_dir: data_dir.clone(),
            instance_id,
            tls_cert_file: None,
            tls_key_file: None,
            public_url: None,
            exposure: RouterExposure::Direct,
            onboarding_assets_dir: None,
        })
        .await
        .unwrap();
        let record = RuntimeRecord {
            instance_id,
            control_url: format!("ws://{}/ws", runtime.address),
            share_mode: RuntimeShareMode::Local,
            advertised_url: None,
            owned_serve: None,
        };
        RuntimeStore::new(data_dir.clone())
            .unwrap()
            .write(&record)
            .unwrap();
        let config = config::ConfigFile {
            version: config::CONFIG_VERSION,
            default_profile: Some("remote".to_owned()),
            profiles: [
                (
                    "remote".to_owned(),
                    Profile::manual("wss://saved.example.test/ws".to_owned()),
                ),
                (
                    "owned".to_owned(),
                    Profile::manual(record.control_url.clone()),
                ),
            ]
            .into(),
        };
        config::save_config(&config::config_path().unwrap(), &config).unwrap();
        (runtime, record, data_dir)
    }

    async fn scoped_credential(record: &RuntimeRecord, data_dir: &Path) -> CredentialFile {
        let admin = owned_ui_config(record, data_dir, None, None).unwrap();
        let (client, _) = RouterClient::connect(admin).await.unwrap();
        let workspace = WorkspaceName::parse("project-room").unwrap();
        let created = client
            .call(ClientMessage::WorkspaceCreate {
                request_id: "create-room".to_owned(),
                name: workspace.clone(),
            })
            .await
            .unwrap();
        assert!(matches!(created, ServerMessage::WorkspaceCreated { .. }));
        let response = client
            .call(ClientMessage::CredentialIssue {
                request_id: "issue-scoped".to_owned(),
                role: CredentialRole::Operator,
                subject: "scoped".to_owned(),
                agent_side: None,
                agent_client: None,
                workspaces: vec![workspace],
            })
            .await
            .unwrap();
        client.close().await.unwrap();
        let ServerMessage::CredentialIssued { credential, .. } = response else {
            panic!("scoped operator was not issued");
        };
        credential
    }

    fn ui_cli(profile: Option<&str>, credential: Option<PathBuf>) -> Cli {
        Cli {
            profile: profile.map(str::to_owned),
            credential,
            dry_run: false,
            command: Some(Command::Ui(cli::UiArgs { workspace: None })),
        }
    }

    fn remote_only(options: &UiOptions) {
        assert_eq!(options.ownership, Ownership::Remote);
        assert!(options.owned_runtime.is_none());
        assert!(options.owned_data_dir.is_none());
    }

    #[test]
    fn implicit_console_ignores_saved_default_and_tailnet_advertisement() {
        if isolated(
            "app::console_tests::implicit_console_ignores_saved_default_and_tailnet_advertisement",
            None,
        ) {
            return;
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (runtime, mut record, data_dir) = fixture().await;
            record.share_mode = RuntimeShareMode::Tailscale;
            record.advertised_url = Some("ws://100.64.0.1:8787/ws".to_owned());
            record.owned_serve = Some(OwnedServe::loopback(
                record.instance_id,
                runtime.address.port(),
            ));
            let store = RuntimeStore::new(data_dir.clone()).unwrap();
            store.write(&record).unwrap();
            let cli = ui_cli(None, None);
            let args = cli::UiArgs {
                workspace: Some("project-room".to_owned()),
            };
            let (config, options) = ui_connection(&cli, &args).await.unwrap();
            assert_eq!(config.router_url.as_str(), record.control_url);
            assert_eq!(options.ownership, Ownership::Reused);
            assert_eq!(options.owned_runtime, Some(record.clone()));
            assert_eq!(options.owned_data_dir.as_deref(), Some(data_dir.as_path()));
            assert_eq!(
                options.workspace.as_ref().map(WorkspaceName::as_str),
                Some("project-room")
            );
            let ClientRole::Operator { credential } = config.role else {
                panic!("operator role")
            };
            assert_eq!(credential.subject, "admin");

            let agent_path = data_dir.join("credentials/provider.json");
            let agent = CredentialFile::generate(
                CredentialRole::Agent,
                "provider".to_owned(),
                Some(AgentSide::Generic),
                Some(AgentClient::Omp),
                vec![WorkspaceName::parse("project-room").unwrap()],
            )
            .unwrap();
            write_credential_exclusive(&agent_path, &agent).unwrap();
            let config_path = config::config_path().unwrap();
            let mut saved = config::load_config(&config_path).unwrap();
            let remote = saved.profiles.get_mut("remote").unwrap();
            remote.server_id = Some(Uuid::new_v4());
            remote.routes.push(config::StoredOnboardingRoute {
                kind: crate::onboarding::RouteKind::Public,
                router_url: remote.router_url.clone(),
                ca_file: None,
            });
            remote.bindings.insert(
                OnboardingProvider::Omp,
                config::ProviderBinding {
                    credential_file: agent_path,
                    workspace: WorkspaceName::parse("project-room").unwrap(),
                },
            );
            config::save_config(&config_path, &saved).unwrap();
            // Existing operator selection still uses the remote default and cannot borrow admin.
            let error = operator_client_config(&cli)
                .err()
                .expect("remote requires credential");
            assert_eq!(error.code, "configuration_required");
            let error = ui_connection(&ui_cli(Some("remote"), None), &args)
                .await
                .err()
                .expect("remote provider binding is not operator authority");
            assert_eq!(error.code, "configuration_required");
            fs::write(
                config::config_path().unwrap(),
                b"malformed saved configuration",
            )
            .unwrap();
            let (config, options) = ui_connection(&cli, &args).await.unwrap();
            assert_eq!(config.router_url.as_str(), record.control_url);
            assert_eq!(options.ownership, Ownership::Reused);
            store.remove_if_instance(record.instance_id).unwrap();
            let error = ui_connection(&cli, &args)
                .await
                .err()
                .expect("owned record required");
            assert_eq!(error.code, "router_not_running");
            assert!(error.message.contains("asr router start"));
            runtime.shutdown().await.unwrap();
        });
    }

    #[test]
    fn scoped_console_credentials_never_borrow_admin_even_with_forged_claims() {
        if isolated(
            "app::console_tests::scoped_console_credentials_never_borrow_admin_even_with_forged_claims",
            None,
        ) {
            return;
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (runtime, record, data_dir) = fixture().await;
            let scoped = scoped_credential(&record, &data_dir).await;
            let scoped_path = data_dir.join("credentials/scoped.json");
            write_credential_exclusive(&scoped_path, &scoped).unwrap();
            let admin_path = data_dir.join(ADMIN_CREDENTIAL);
            let selected_admin = data_dir.join("credentials/selected-admin.json");
            let admin = read_credential(&admin_path).unwrap();
            write_credential_exclusive(&selected_admin, &admin).unwrap();
            fs::remove_file(&admin_path).unwrap();
            let args = cli::UiArgs { workspace: None };
            for profile in [None, Some("owned")] {
                let cli = ui_cli(profile, Some(scoped_path.clone()));
                let (config, options) = ui_connection(&cli, &args).await.unwrap();
                assert_eq!(config.router_url.as_str(), record.control_url);
                let ClientRole::Operator { credential } = config.role else {
                    panic!("operator role")
                };
                assert_eq!(credential.token_hash(), scoped.token_hash());
                remote_only(&options);
            }
            // Auto-start's connection preparation follows the same explicit-credential authority.
            let cli = ui_cli(None, Some(scoped_path.clone()));
            let (_, options) = owned_console_connection(
                &cli,
                record.clone(),
                data_dir.clone(),
                None,
                Ownership::Owned,
                None,
            )
            .await
            .unwrap();
            remote_only(&options);
            let mut forged = scoped;
            forged.subject = "admin".to_owned();
            forged.workspaces.clear();
            let forged_path = data_dir.join("credentials/forged.json");
            write_credential_exclusive(&forged_path, &forged).unwrap();
            let (_, options) = ui_connection(&ui_cli(Some("owned"), Some(forged_path)), &args)
                .await
                .unwrap();
            remote_only(&options);
            let (_, options) = ui_connection(&ui_cli(Some("owned"), Some(selected_admin)), &args)
                .await
                .unwrap();
            assert_eq!(options.ownership, Ownership::Reused);
            assert_eq!(options.owned_runtime, Some(record));
            let error = ui_connection(&ui_cli(None, Some(data_dir.join("missing.json"))), &args)
                .await
                .err()
                .expect("invalid explicit credential");
            assert_eq!(error.code, "credential_invalid");
            let agent = CredentialFile::generate(
                CredentialRole::Agent,
                "provider".to_owned(),
                Some(AgentSide::Generic),
                Some(AgentClient::Omp),
                Vec::new(),
            )
            .unwrap();
            let agent_path = data_dir.join("credentials/agent.json");
            write_credential_exclusive(&agent_path, &agent).unwrap();
            let error = ui_connection(&ui_cli(None, Some(agent_path)), &args)
                .await
                .err()
                .expect("agent cannot fall back to owned admin");
            assert_eq!(error.code, "credential_invalid");
            assert!(!admin_path.exists());
            runtime.shutdown().await.unwrap();
        });
    }

    #[test]
    fn console_operator_selection_preserves_environment_and_profile_precedence() {
        if isolated(
            "app::console_tests::console_operator_selection_preserves_environment_and_profile_precedence",
            Some("route"),
        ) {
            return;
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (runtime, record, data_dir) = fixture().await;
            let scoped = scoped_credential(&record, &data_dir).await;
            let environment_credential = PathBuf::from(env::var_os("ASR_CREDENTIAL_FILE").unwrap());
            write_credential_exclusive(&environment_credential, &scoped).unwrap();
            let args = cli::UiArgs { workspace: None };
            let (config, options) = ui_connection(&ui_cli(None, None), &args).await.unwrap();
            assert_eq!(
                config.router_url.as_str(),
                "wss://environment.example.test/ws"
            );
            remote_only(&options);
            let (config, options) = ui_connection(&ui_cli(Some("owned"), None), &args)
                .await
                .unwrap();
            assert_eq!(config.router_url.as_str(), record.control_url);
            remote_only(&options);
            let ClientRole::Operator { credential } = config.role else {
                panic!("operator role")
            };
            assert_eq!(credential.token_hash(), scoped.token_hash());
            let (_, options) = ui_connection(
                &ui_cli(Some("owned"), Some(data_dir.join(ADMIN_CREDENTIAL))),
                &args,
            )
            .await
            .unwrap();
            assert_eq!(options.ownership, Ownership::Reused);
            runtime.shutdown().await.unwrap();
        });
    }

    #[test]
    fn implicit_console_preserves_environment_credential_without_a_profile() {
        if isolated(
            "app::console_tests::implicit_console_preserves_environment_credential_without_a_profile",
            Some("credential"),
        ) {
            return;
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (runtime, record, data_dir) = fixture().await;
            let scoped = scoped_credential(&record, &data_dir).await;
            let credential_path = PathBuf::from(env::var_os("ASR_CREDENTIAL_FILE").unwrap());
            write_credential_exclusive(&credential_path, &scoped).unwrap();
            fs::remove_file(data_dir.join(ADMIN_CREDENTIAL)).unwrap();
            let (config, options) =
                ui_connection(&ui_cli(None, None), &cli::UiArgs { workspace: None })
                    .await
                    .unwrap();
            assert_eq!(config.router_url.as_str(), record.control_url);
            let ClientRole::Operator { credential } = config.role else {
                panic!("operator role")
            };
            assert_eq!(credential.token_hash(), scoped.token_hash());
            remote_only(&options);
            runtime.shutdown().await.unwrap();
        });
    }
}
