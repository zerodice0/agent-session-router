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
              1) Start local router in background\n\
              2) Stop local router\n\
              3) List profiles\n\
              4) List workspaces on this device\n\
              5) List credentials on this device\n\
              6) Run doctor\n\
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
        "1" => Some(&["asr", "router", "start", "--background"]),
        "2" => Some(&["asr", "router", "stop"]),
        "3" => Some(&["asr", "profile", "list"]),
        "4" => Some(&["asr", "--profile", DEVICE_PROFILE, "workspace", "list"]),
        "5" => Some(&["asr", "--profile", DEVICE_PROFILE, "credential", "list"]),
        "6" => Some(&["asr", "doctor"]),
        "q" | "Q" | "" => None,
        _ => {
            return Err(CliError::usage(
                "invalid_selection",
                "menu selection must be 1 through 6 or q",
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
        Command::Router(args) => router_command(args.action, args.background, args.share).await,
        Command::Workspace(args) => workspace_command(cli, &args.command, stdin).await,
        Command::Task(args) => task_command(cli, &args.command, stdin).await,
        Command::Integration(args) => integration_command(cli, &args.command).await,
        Command::Credential(args) => credential_command(cli, &args.command).await,
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
    let invocation = hosts::mcp_invocation(
        args,
        cli.profile.as_deref(),
        cli.credential.as_deref(),
        cli.dry_run,
    )
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
) -> Result<(config::Selection, PathBuf, CredentialFile), CliError> {
    let selection = config::select(cli.profile.as_deref(), cli.credential.as_deref())
        .map_err(|error| config_error(&error))?;
    let credential_path = selection
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
) -> Result<McpChildSelection, CliError> {
    let (_, credential_file, _) = selected_agent_credential(cli, requested_agent, side, client)?;
    Ok(McpChildSelection {
        profile: cli.profile.clone(),
        credential_file: Some(credential_file),
    })
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
        router_url: selection.router_url,
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
    let selection = child_selection(
        cli,
        args.agent.as_deref(),
        AgentSide::Codex,
        AgentClient::CodexCli,
    )?;
    let workspace = args.workspace.as_deref().map(workspace_name).transpose()?;
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
    let selection = child_selection(
        cli,
        args.agent.as_deref(),
        AgentSide::Claude,
        AgentClient::ClaudeCode,
    )?;
    let workspace = args.workspace.as_deref().map(workspace_name).transpose()?;
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
    let selection = child_selection(
        cli,
        args.agent.as_deref(),
        AgentSide::Generic,
        AgentClient::Omp,
    )?;
    let workspace = args.workspace.as_deref().map(workspace_name).transpose()?;
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
            stored.profiles.insert(
                name.clone(),
                Profile {
                    router_url: url.to_string(),
                },
            );
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

async fn router_command(
    action: RouterAction,
    background: bool,
    share: Option<ShareMode>,
) -> Result<i32, CliError> {
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
    let mut launcher = native_launcher(&data_dir, bind, ca_file)?;
    match action {
        RouterAction::Start => {
            let share = match share {
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
                    background,
                    instance_id: None,
                })
                .await
                .map_err(|error| launch_error(&error))?;
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
    let environment = env::vars_os()
        .filter(|(key, _)| {
            key != "ASR_LAUNCH_INSTANCE_ID"
                && key != "ASR_BACKGROUND_CHILD"
                && key != "ASR_RUNTIME_SHARE_MODE"
        })
        .collect();
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
    let mut runtime = RouterRuntime::start_paused(RouterConfig {
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
        stored.profiles.insert(
            DEVICE_PROFILE.to_owned(),
            Profile {
                router_url: router_url.to_owned(),
            },
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

fn find_executable(name: &str) -> Option<PathBuf> {
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

async fn operator_client(cli: &Cli) -> Result<(RouterClient, ClientEvents), CliError> {
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
    RouterClient::connect(ClientConfig {
        router_url: selection.router_url,
        role: ClientRole::Operator { credential },
        ca_file,
    })
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
    let message = match error {
        RouterLaunchError::RouterBusy => "router_busy",
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
