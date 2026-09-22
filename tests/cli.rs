use agent_session_router::{cli, process};

use std::{
    cell::Cell,
    ffi::{OsStr, OsString},
    io::{self, Cursor, Read, Write},
    path::PathBuf,
    rc::Rc,
};

use clap::{CommandFactory as _, Parser};
use cli::{
    Cli, Command, McpRoleArg, OperationIdSource, OutputItem, OutputPage, StopEvidence, TaskPage,
    TaskRow, TaskRunIdentity,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
#[allow(clippy::too_many_lines)]
fn parses_complete_public_command_surface() {
    let operation = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    let attempt = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    let report = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    let commands: Vec<Vec<&str>> = vec![
        vec!["asr", "router"],
        vec!["asr", "router", "start", "--background", "--share"],
        vec!["asr", "router", "start", "--no-ui"],
        vec!["asr", "ui"],
        vec!["asr", "ui", "--workspace", "project-room"],
        vec!["asr", "router", "stop"],
        vec!["asr", "onboarding", "revoke", operation],
        vec![
            "asr",
            "onboarding",
            "install",
            "--provider",
            "codex-cli",
            "--stdin",
        ],
        vec![
            "asr",
            "onboarding",
            "resume",
            operation,
            "--provider",
            "omp",
        ],
        vec![
            "asr",
            "--profile",
            "office",
            "onboarding",
            "status",
            "--provider",
            "claude-code",
            "--json",
        ],
        vec![
            "asr",
            "onboarding",
            "prompt",
            "--workspace",
            "room",
            "--name",
            "office",
            "--create-workspace",
            "--provider",
            "omp",
            "--endpoint",
            "local=ws://127.0.0.1:8787/ws",
        ],
        vec![
            "asr",
            "codex",
            "worker",
            "--activity",
            "review",
            "--workspace",
            "room",
        ],
        vec![
            "asr",
            "codex-cli",
            "worker",
            "--workspace",
            "room",
            "--",
            "resume",
            "session",
        ],
        vec![
            "asr",
            "claude",
            "reviewer",
            "--activity",
            "review",
            "--workspace",
            "room",
            "--auto",
            "--resume",
            "session",
        ],
        vec![
            "asr",
            "omp",
            "worker",
            "--workspace",
            "room",
            "--",
            "--resume",
            "session",
        ],
        vec!["asr", "gateway", "claude", "worker", "--workspace", "room"],
        vec!["asr", "gateway", "codex", "worker", "--workspace", "room"],
        vec!["asr", "setup-claude"],
        vec!["asr", "setup-omp"],
        vec!["asr", "doctor"],
        vec!["asr", "install", "--bin-dir", "/tmp/bin"],
        vec!["asr", "profile", "list"],
        vec![
            "asr",
            "profile",
            "add",
            "remote",
            "wss://router.example/ws",
            "--force",
        ],
        vec!["asr", "profile", "use", "remote"],
        vec!["asr", "workspace", "create", "room"],
        vec!["asr", "workspace", "list", "--json"],
        vec!["asr", "workspace", "members", "room", "--json"],
        vec![
            "asr",
            "workspace",
            "history",
            "room",
            "--after",
            "3",
            "--limit",
            "10",
            "--json",
        ],
        vec![
            "asr",
            "workspace",
            "watch",
            "room",
            "--after",
            "3",
            "--json",
        ],
        vec!["asr", "workspace", "post", "room", "--stdin"],
        vec![
            "asr",
            "workspace",
            "send",
            "room",
            "worker",
            "--stdin",
            "--timeout-ms",
            "1000",
        ],
        vec!["asr", "workspace", "join", "room"],
        vec!["asr", "credential", "issue", "--operator", "operator"],
        vec![
            "asr",
            "credential",
            "issue",
            "--agent",
            "worker",
            "--side",
            "codex",
            "--client",
            "codex-cli",
            "--workspace",
            "room",
            "--output",
            "/tmp/credential",
        ],
        vec!["asr", "credential", "list", "--json"],
        vec!["asr", "credential", "revoke", "public-id"],
        vec![
            "asr",
            "task",
            "list",
            "room",
            "--state",
            "todo",
            "--state",
            "running",
            "--assignee",
            "worker",
            "--after",
            "1",
            "--limit",
            "20",
            "--json",
        ],
        vec!["asr", "task", "show", "room", "1", "--json"],
        vec![
            "asr", "task", "history", "room", "1", "--after", "2", "--limit", "20", "--json",
        ],
        vec![
            "asr", "task", "watch", "room", "--task", "1", "--after", "2", "--json",
        ],
        vec![
            "asr",
            "task",
            "create",
            "room",
            "--stdin",
            "--operation-id",
            operation,
            "--json",
        ],
        vec![
            "asr",
            "task",
            "edit",
            "room",
            "1",
            "--expected-version",
            "2",
            "--stdin",
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "assign",
            "room",
            "1",
            "--agent",
            "worker",
            "--expected-version",
            "2",
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "assign",
            "room",
            "1",
            "--unassigned",
            "--expected-version",
            "2",
        ],
        vec![
            "asr",
            "task",
            "note",
            "room",
            "1",
            "--stdin",
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "cancel",
            "room",
            "1",
            "--expected-version",
            "2",
            "--stdin",
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "reopen",
            "room",
            "1",
            "--expected-version",
            "2",
            "--stdin",
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "interrupt",
            "room",
            "1",
            "--expected-version",
            "2",
            "--stdin",
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "confirm-stopped",
            "room",
            "1",
            "--attempt",
            attempt,
            "--expected-version",
            "2",
            "--stdin",
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "request",
            "room",
            "1",
            "--expected-version",
            "2",
            "--timeout-ms",
            "1000",
            "--stdin",
            "--json",
        ],
        vec![
            "asr",
            "task",
            "import",
            "room",
            "github",
            "42",
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "link",
            "room",
            "1",
            "linear",
            "ENG-42",
            "--expected-version",
            "2",
            "--replace",
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "publish",
            "room",
            "1",
            "github",
            "--kind",
            "report",
            "--expected-version",
            "2",
            "--report",
            report,
            "--operation-id",
            operation,
        ],
        vec![
            "asr",
            "task",
            "external-status",
            "room",
            operation,
            "--json",
        ],
        vec![
            "asr",
            "task",
            "external-resolve",
            "room",
            operation,
            "--applied",
            "--external-id",
            "42",
            "--stdin",
            "--resolution-id",
            report,
            "--json",
        ],
        vec!["asr", "integration", "list", "room", "--json"],
        vec!["asr", "integration", "check", "room", "github", "--json"],
        vec!["asr", "integration", "admin", "reload", "--json"],
        vec![
            "asr",
            "smoke",
            "--workspace",
            "room",
            "--target",
            "worker",
            "--timeout-ms",
            "1000",
        ],
    ];

    for arguments in commands {
        Cli::try_parse_from(&arguments).unwrap_or_else(|error| {
            panic!("failed to parse {arguments:?}: {error}");
        });
    }
}

#[test]
fn internal_mcp_command_is_strict_typed_and_hidden() {
    let parsed = Cli::try_parse_from([
        "asr",
        "--profile",
        "default",
        "--credential",
        "/private/credential.json",
        "mcp",
        "codex-cli",
    ])
    .unwrap();
    assert_eq!(parsed.profile.as_deref(), Some("default"));
    assert_eq!(
        parsed.credential.as_deref(),
        Some(std::path::Path::new("/private/credential.json"))
    );
    let Command::Mcp(args) = parsed.command.unwrap() else {
        panic!("expected MCP command");
    };
    assert_eq!(
        args.role.mcp_role(),
        agent_session_router::mcp::McpRole::CodexCli
    );

    for (name, expected) in [
        ("codex-cli", agent_session_router::mcp::McpRole::CodexCli),
        (
            "claude-channel",
            agent_session_router::mcp::McpRole::ClaudeChannel,
        ),
        ("omp", agent_session_router::mcp::McpRole::Omp),
    ] {
        let parsed = Cli::try_parse_from(["asr", "mcp", name]).unwrap();
        let Command::Mcp(args) = parsed.command.unwrap() else {
            panic!("expected MCP command");
        };
        assert_eq!(args.role.mcp_role(), expected);
    }

    let parsed = Cli::try_parse_from([
        "asr",
        "mcp",
        "delegate",
        "--context-file",
        "/private/context.json",
    ])
    .unwrap();
    let Command::Mcp(args) = parsed.command.unwrap() else {
        panic!("expected MCP command");
    };
    let McpRoleArg::Delegate { context_file } = args.role else {
        panic!("expected delegate role");
    };
    assert_eq!(context_file, PathBuf::from("/private/context.json"));

    let help = Cli::command().render_help().to_string();
    assert!(
        !help
            .lines()
            .any(|line| line.trim_start().starts_with("mcp"))
    );
}

#[test]
fn internal_mcp_command_rejects_identity_spoofing_and_delegate_tokens() {
    for arguments in [
        vec!["asr", "mcp", "codex-cli", "--agent-id", "local:spoof"],
        vec!["asr", "mcp", "omp", "--client", "generic"],
        vec![
            "asr",
            "mcp",
            "delegate",
            "--context-file",
            "/private/context.json",
            "--owner-id",
            "local:spoof",
        ],
        vec!["asr", "mcp", "delegate", "--delegation-token", "secret"],
    ] {
        assert!(Cli::try_parse_from(arguments).is_err());
    }
}

#[test]
fn globals_are_accepted_only_before_the_command() {
    assert!(Cli::try_parse_from(["asr", "--profile", "remote", "--dry-run", "doctor"]).is_ok());
    assert!(Cli::try_parse_from(["asr", "doctor", "--dry-run"]).is_err());
    assert!(Cli::try_parse_from(["asr", "doctor", "--credential", "/tmp/secret"]).is_err());

    let no_args = Cli::try_parse_from(["asr"]).expect("no-arg TTY selection is modeled");
    assert!(no_args.command.is_none());
}

#[test]
fn semantic_parse_boundaries_are_rejected_before_effects() {
    let stopped_share = Cli::try_parse_from(["asr", "router", "stop", "--share"]).unwrap();
    assert_eq!(
        cli::validate_command(stopped_share.command.as_ref().unwrap())
            .unwrap_err()
            .exit_code(),
        2
    );

    let missing_assignment = Cli::try_parse_from([
        "asr",
        "task",
        "assign",
        "room",
        "1",
        "--expected-version",
        "2",
    ])
    .unwrap();
    assert!(cli::validate_command(missing_assignment.command.as_ref().unwrap()).is_err());

    assert!(
        Cli::try_parse_from(["asr", "task", "list", "room", "--all", "--state", "todo"]).is_err()
    );
}

#[test]
fn console_parser_rejects_invalid_headless_modes_and_workspace_before_effects() {
    assert!(Cli::try_parse_from(["asr", "router", "start", "--background", "--no-ui"]).is_err());
    let stopped = Cli::try_parse_from(["asr", "router", "stop", "--no-ui"]).unwrap();
    assert_eq!(
        cli::validate_command(stopped.command.as_ref().unwrap())
            .unwrap_err()
            .exit_code(),
        2
    );
    let invalid = Cli::try_parse_from(["asr", "--dry-run", "ui", "--workspace", ""]).unwrap();
    assert_eq!(
        cli::plan_dry_run(&invalid, &mut FixedIds(Uuid::nil()))
            .unwrap_err()
            .exit_code(),
        2
    );
    assert!(Cli::try_parse_from(["asr", "ui", "--background"]).is_err());
    assert!(Cli::try_parse_from(["asr", "ui", "--no-ui"]).is_err());
}

#[test]
fn console_dry_run_preserves_initial_workspace_without_opening_credentials_or_stdin() {
    let parsed = Cli::try_parse_from([
        "asr",
        "--credential",
        "/missing/operator.json",
        "--profile",
        "missing",
        "--dry-run",
        "ui",
        "--workspace",
        "project-room",
    ])
    .unwrap();
    let plan = cli::plan_dry_run(&parsed, &mut FixedIds(Uuid::nil())).unwrap();
    assert_eq!(plan.command, "ui");
    assert_eq!(plan.room.as_deref(), Some("project-room"));
    assert_eq!(plan.operation_id, None);
    assert_eq!(
        cli::read_command_stdin(parsed.command.as_ref().unwrap(), &mut PanicReader).unwrap(),
        None
    );
    let foreground =
        Cli::try_parse_from(["asr", "--dry-run", "router", "start", "--no-ui"]).unwrap();
    let Some(Command::Router(args)) = &foreground.command else {
        panic!("router command")
    };
    assert!(args.no_ui);
    assert!(!args.background);
    assert_eq!(
        cli::plan_dry_run(&foreground, &mut FixedIds(Uuid::nil()))
            .unwrap()
            .command,
        "router"
    );
}

#[test]
fn explicit_console_rejects_nonterminal_before_reading_configuration_or_starting_router() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("absent-data");
    let config = directory.path().join("invalid-config");
    std::fs::write(&config, b"not JSON").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_asr"))
        .args(["--profile", "missing", "ui"])
        .env("ASR_DATA_DIR", &data)
        .env("ASR_CONFIG_PATH", &config)
        .env("TERM", "xterm-256color")
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("terminal_required"));
    assert!(output.stdout.is_empty());
    assert!(!data.exists());
    let dry_run = std::process::Command::new(env!("CARGO_BIN_EXE_asr"))
        .args([
            "--profile",
            "missing",
            "--dry-run",
            "ui",
            "--workspace",
            "room",
        ])
        .env("ASR_DATA_DIR", &data)
        .env("ASR_CONFIG_PATH", &config)
        .env("TERM", "dumb")
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert!(dry_run.status.success());
    let plan: Value = serde_json::from_slice(&dry_run.stdout).unwrap();
    assert_eq!(plan["command"], "ui");
    assert_eq!(plan["room"], "room");
    assert!(!data.exists());
}

struct FixedIds(Uuid);

impl OperationIdSource for FixedIds {
    fn next_operation_id(&mut self) -> Uuid {
        self.0
    }
}

#[test]
fn dry_run_plan_is_redacted_and_requires_no_input_or_runtime_handle() {
    let expected = Uuid::parse_str("dddddddd-dddd-4ddd-8ddd-dddddddddddd").unwrap();
    let parsed = Cli::try_parse_from([
        "asr",
        "--dry-run",
        "task",
        "create",
        "project-room",
        "--stdin",
    ])
    .unwrap();
    let mut ids = FixedIds(expected);
    let plan = cli::plan_dry_run(&parsed, &mut ids).unwrap();
    assert_eq!(plan.command, "task.create");
    assert_eq!(plan.room.as_deref(), Some("project-room"));
    assert_eq!(plan.operation_id, Some(expected));

    let mut rendered = Vec::new();
    cli::write_dry_run_plan(&mut rendered, &plan).unwrap();
    let rendered = String::from_utf8(rendered).unwrap();
    assert!(rendered.contains("[redacted]"));
    assert!(!rendered.contains("title"));
    assert!(!rendered.contains("description"));
}

#[test]
fn explicit_operation_id_is_preserved_and_generated_id_is_stable_in_plan() {
    let explicit = Uuid::parse_str("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee").unwrap();
    let generated = Uuid::parse_str("ffffffff-ffff-4fff-8fff-ffffffffffff").unwrap();
    let parsed = Cli::try_parse_from([
        "asr",
        "--dry-run",
        "task",
        "create",
        "room",
        "--stdin",
        "--operation-id",
        &explicit.to_string(),
    ])
    .unwrap();
    let mut ids = FixedIds(generated);
    let plan = cli::plan_dry_run(&parsed, &mut ids).unwrap();
    assert_eq!(plan.operation_id, Some(explicit));
}

#[test]
fn bounded_input_accepts_cap_and_rejects_one_extra_byte() {
    let mut exact = Cursor::new(vec![b'x'; 8]);
    assert_eq!(cli::read_bounded(&mut exact, 8).unwrap().len(), 8);

    let mut oversized = Cursor::new(vec![b'x'; 9]);
    let error = cli::read_bounded(&mut oversized, 8).unwrap_err();
    assert_eq!(error.code, "stdin_too_large");
    assert_eq!(error.exit_code(), 2);
}

#[test]
fn command_input_validates_utf8_json_fields_and_command_specific_caps() {
    let parsed = Cli::try_parse_from(["asr", "task", "create", "room", "--stdin"]).unwrap();
    let command = parsed.command.as_ref().unwrap();
    let mut valid = Cursor::new(br#"{"title":"work","description":"details"}"#.to_vec());
    assert!(matches!(
        cli::read_command_stdin(command, &mut valid).unwrap(),
        Some(cli::StdinPayload::Json(_))
    ));

    let mut unknown =
        Cursor::new(br#"{"title":"work","description":"details","token":"secret"}"#.to_vec());
    assert!(cli::read_command_stdin(command, &mut unknown).is_err());

    let note = Cli::try_parse_from(["asr", "task", "note", "room", "1", "--stdin"]).unwrap();
    let mut oversized = Cursor::new(vec![b'x'; cli::MAX_NOTE_BYTES + 1]);
    assert_eq!(
        cli::read_command_stdin(note.command.as_ref().unwrap(), &mut oversized)
            .unwrap_err()
            .code,
        "stdin_too_large"
    );
}

#[test]
fn human_escaping_is_terminal_safe_while_json_round_trips_raw_values() {
    let hostile = "safe\u{1b}]0;owned\u{7}line\nnext\u{009b}31m";
    let escaped = cli::escape_terminal(hostile);
    assert!(!escaped.contains('\u{1b}'));
    assert!(!escaped.contains('\u{7}'));
    assert!(!escaped.contains('\n'));
    assert!(!escaped.contains('\u{009b}'));
    assert!(escaped.contains("\\u{001B}"));
    assert!(escaped.contains("\\u{009B}"));

    let mut json_output = Vec::new();
    cli::write_json_line(&mut json_output, &json!({"body": hostile})).unwrap();
    let decoded: Value = serde_json::from_slice(&json_output).unwrap();
    assert_eq!(decoded["body"], hostile);
    assert!(!json_output.contains(&0x1b));
    assert!(!json_output.contains(&0x07));
}

#[test]
fn task_request_has_json_but_no_operation_id_receipt() {
    let parsed = Cli::try_parse_from([
        "asr",
        "task",
        "request",
        "room",
        "7",
        "--expected-version",
        "3",
        "--stdin",
        "--json",
    ])
    .unwrap();
    let command = parsed.command.as_ref().unwrap();
    assert!(cli::command_uses_json(command));
    assert!(!cli::command_needs_operation_id(command));
    assert_eq!(cli::command_operation_id(command), None);
    assert!(
        Cli::try_parse_from([
            "asr",
            "task",
            "request",
            "room",
            "7",
            "--expected-version",
            "3",
            "--operation-id",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        ])
        .is_err()
    );
}

struct StreamingValues {
    next: usize,
    count: usize,
    written_lines: Rc<Cell<usize>>,
}

impl Iterator for StreamingValues {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.count {
            return None;
        }
        assert_eq!(
            self.written_lines.get(),
            self.next,
            "records were accumulated before writing"
        );
        let value = json!({"page": self.next});
        self.next += 1;
        Some(value)
    }
}

struct LineTrackingWriter {
    bytes: Vec<u8>,
    written_lines: Rc<Cell<usize>>,
}

impl Write for LineTrackingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let lines = newline_count(bytes);
        self.written_lines.set(self.written_lines.get() + lines);
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn newline_count(bytes: &[u8]) -> usize {
    let mut count = 0;
    for byte in bytes {
        if *byte == b'\n' {
            count += 1;
        }
    }
    count
}
#[test]
fn ndjson_writes_each_bounded_record_before_requesting_the_next() {
    let lines = Rc::new(Cell::new(0));
    let values = StreamingValues {
        next: 0,
        count: 4,
        written_lines: Rc::clone(&lines),
    };
    let mut writer = LineTrackingWriter {
        bytes: Vec::new(),
        written_lines: Rc::clone(&lines),
    };
    cli::write_ndjson(&mut writer, values).unwrap();
    assert_eq!(lines.get(), 4);
    assert_eq!(newline_count(&writer.bytes), 4);
}
#[test]
fn page_helpers_return_cursor_and_do_not_hide_empty_terminal_pages() {
    let page = OutputPage {
        items: vec![OutputItem {
            human: "first".to_owned(),
            json: json!({"id": 1}),
        }],
        next_cursor: Some("cursor-1".to_owned()),
        has_more: true,
    };
    let mut output = Vec::new();
    assert_eq!(
        cli::write_output_page(&mut output, &page, true)
            .unwrap()
            .as_deref(),
        Some("cursor-1")
    );

    let terminal = OutputPage {
        items: Vec::new(),
        next_cursor: Some("terminal".to_owned()),
        has_more: false,
    };
    assert_eq!(
        cli::write_output_page(&mut output, &terminal, true).unwrap(),
        None
    );
}

#[test]
fn task_renderer_distinguishes_current_last_and_stop_evidence() {
    let current_attempt = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let current_session = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
    let last_attempt = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let last_session = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    let row = TaskRow {
        id: 9,
        state: "running".to_owned(),
        current: Some(TaskRunIdentity {
            executor: "worker".to_owned(),
            attempt_id: current_attempt,
            session_id: current_session,
        }),
        last: Some(TaskRunIdentity {
            executor: "old-worker".to_owned(),
            attempt_id: last_attempt,
            session_id: last_session,
        }),
        last_executor_id: Some("old-worker".to_owned()),
        execution_session_id: Some(last_session),
        assigned_agent_id: Some("next-worker".to_owned()),
        checkpoint: Some("checkpoint\u{1b}[31m".to_owned()),
        stop_evidence: StopEvidence::RunningUnknown,
        title: "A title that is not truncated".to_owned(),
    };
    let rendered = cli::render_task_row(&row);
    assert!(rendered.contains("current:worker"));
    assert!(!rendered.contains("old-worker"));
    assert!(rendered.contains("running"));
    assert!(!rendered.contains('\u{1b}'));

    let interrupted = TaskRow {
        current: None,
        last: row.last.clone(),
        stop_evidence: StopEvidence::InterruptedUnknown,
        ..row.clone()
    };
    let rendered = cli::render_task_row(&interrupted);
    assert!(rendered.contains("last:old-worker"));
    assert!(rendered.contains("stop unconfirmed; handoff blocked"));
    let summary_only = TaskRow {
        current: None,
        last: None,
        ..row.clone()
    };
    let rendered = cli::render_task_row(&summary_only);
    assert!(rendered.contains("last:old-worker"));
    assert!(rendered.contains("last:44444444"));

    for (evidence, expected) in [
        (StopEvidence::None, "\t-\tA title"),
        (StopEvidence::Released, "released by executor"),
        (StopEvidence::Confirmed, "stopped confirmed"),
    ] {
        let variant = TaskRow {
            stop_evidence: evidence,
            ..row.clone()
        };
        assert!(cli::render_task_row(&variant).contains(expected));
    }

    let mut json_output = Vec::new();
    cli::write_task_page(
        &mut json_output,
        &TaskPage {
            tasks: vec![row],
            next_cursor: None,
            has_more: false,
        },
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_slice(&json_output).unwrap();
    assert_eq!(
        value["tasks"][0]["current"]["attemptId"],
        current_attempt.to_string()
    );
    assert_eq!(value["tasks"][0]["title"], "A title that is not truncated");
    assert_eq!(value["tasks"][0]["checkpoint"], "checkpoint\u{1b}[31m");
}

#[test]
fn standard_operation_ids_are_v4_and_interrupted_errors_map_to_130() {
    let mut ids = cli::RandomOperationIds;
    let id = ids.next_operation_id();
    assert_eq!(id.get_version_num(), 4);
    let error = cli::CliError {
        kind: cli::ErrorKind::Interrupted,
        code: "interrupted",
        message: "interrupted".to_owned(),
        operation_id: None,
        resolution_id: None,
    };
    assert_eq!(error.exit_code(), 130);
}

#[cfg(unix)]
#[test]
fn launch_plans_preserve_non_utf8_passthrough_and_caller_cwd() {
    use std::os::unix::ffi::OsStringExt as _;

    let cwd = PathBuf::from("/tmp/caller cwd");
    let opaque = OsString::from_vec(vec![b'-', b'-', b'x', 0xff, b'y']);
    let plan = process::LaunchPlan::passthrough(
        OsString::from("omp"),
        cwd.clone(),
        [OsString::from("--resume"), opaque.clone()],
    );
    assert_eq!(plan.caller_cwd, cwd);
    assert_eq!(plan.arguments[1], opaque);
    assert_eq!(plan.mode, process::LaunchMode::ReplaceForeground);

    let codex = process::LaunchPlan::stock_codex(
        OsString::from("codex"),
        PathBuf::from("/tmp/caller cwd"),
        [opaque.clone()],
    );
    assert_eq!(
        codex.arguments,
        vec![
            OsString::from("-C"),
            OsString::from("/tmp/caller cwd"),
            opaque
        ]
    );
}

#[test]
fn setup_claude_plan_is_project_local_and_uses_absolute_asr() {
    let plan =
        process::LaunchPlan::setup_claude("claude", "/tmp/project", "/opt/asr/bin/asr").unwrap();
    assert_eq!(plan.caller_cwd, PathBuf::from("/tmp/project"));
    assert_eq!(plan.mode, process::LaunchMode::Wait);
    assert_eq!(plan.arguments[0], OsStr::new("mcp"));
    assert_eq!(plan.arguments[5], OsStr::new("local"));
    assert_eq!(plan.arguments[8], OsStr::new("/opt/asr/bin/asr"));
    assert!(process::LaunchPlan::setup_claude("claude", "/tmp/project", "relative/asr").is_err());
}

#[test]
fn owned_codex_plan_uses_configured_or_caller_directory() {
    let configured = process::LaunchPlan::owned_codex(
        "codex-app-server",
        "/tmp/caller",
        Some(OsString::from("/tmp/configured")),
        [OsString::from("serve")],
    );
    assert_eq!(
        configured.environment,
        vec![(
            OsString::from("CODEX_CWD"),
            OsString::from("/tmp/configured")
        )]
    );
    let fallback = process::LaunchPlan::owned_codex(
        "codex-app-server",
        "/tmp/caller",
        None,
        std::iter::empty(),
    );
    assert_eq!(
        fallback.environment,
        vec![(OsString::from("CODEX_CWD"), OsString::from("/tmp/caller"))]
    );
}

#[cfg(unix)]
#[test]
fn wait_launch_executes_in_caller_cwd_with_environment_and_maps_status() {
    let plan = process::LaunchPlan::passthrough(
        "/bin/sh",
        "/tmp",
        [
            OsString::from("-c"),
            OsString::from("test \"$ASR_PROCESS_TEST\" = present; exit $?"),
        ],
    )
    .with_environment("ASR_PROCESS_TEST", "present")
    .with_mode(process::LaunchMode::Wait);
    plan.validate().unwrap();
    assert_eq!(process::execute(&plan).unwrap(), 0);
}

#[cfg(unix)]
#[test]
fn process_exit_mapping_distinguishes_usage_and_sigint() {
    use std::os::unix::process::ExitStatusExt as _;

    assert_eq!(
        process::map_exit_status(std::process::ExitStatus::from_raw(0)),
        0
    );
    assert_eq!(
        process::map_exit_status(std::process::ExitStatus::from_raw(2 << 8)),
        2
    );
    assert_eq!(
        process::map_exit_status(std::process::ExitStatus::from_raw(2)),
        130
    );
    assert_eq!(
        process::map_exit_status(std::process::ExitStatus::from_raw(15)),
        1
    );
}

struct PanicReader;

impl Read for PanicReader {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        panic!("dry-run planning must not read stdin")
    }
}

#[test]
fn dry_run_planning_api_has_no_stdin_parameter() {
    let parsed =
        Cli::try_parse_from(["asr", "--dry-run", "workspace", "post", "room", "--stdin"]).unwrap();
    let mut ids = FixedIds(Uuid::nil());
    std::hint::black_box(PanicReader);
    let plan = cli::plan_dry_run(&parsed, &mut ids).unwrap();
    assert_eq!(plan.command, "workspace.post");
    assert_eq!(plan.room.as_deref(), Some("room"));
}

#[test]
fn mcp_dry_run_planning_does_not_open_credentials() {
    let parsed = Cli::try_parse_from([
        "asr",
        "--credential",
        "/definitely/missing/credential.json",
        "--dry-run",
        "mcp",
        "claude-channel",
    ])
    .unwrap();
    let mut ids = FixedIds(Uuid::nil());
    let plan = cli::plan_dry_run(&parsed, &mut ids).unwrap();
    assert_eq!(plan.command, "mcp.claude-channel");
}

#[test]
fn command_model_keeps_os_arguments_opaque() {
    let parsed = Cli::try_parse_from([
        OsString::from("asr"),
        OsString::from("omp"),
        OsString::from("worker"),
        OsString::from("--"),
        OsString::from("--resume"),
        OsString::from("session with spaces"),
    ])
    .unwrap();
    match parsed.command.unwrap() {
        Command::Omp(arguments) => {
            assert_eq!(
                arguments.arguments,
                vec![
                    OsString::from("--resume"),
                    OsString::from("session with spaces")
                ]
            );
        }
        command => panic!("unexpected command: {command:?}"),
    }
}

#[test]
fn onboarding_authority_overrides_are_rejected_even_during_dry_run() {
    for arguments in [
        vec![
            "asr",
            "--profile",
            "other",
            "--dry-run",
            "onboarding",
            "install",
            "--provider",
            "omp",
            "--stdin",
        ],
        vec![
            "asr",
            "--credential",
            "/missing",
            "--dry-run",
            "onboarding",
            "resume",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "--provider",
            "codex-cli",
        ],
        vec![
            "asr",
            "--dry-run",
            "onboarding",
            "status",
            "--provider",
            "omp",
            "--json",
        ],
    ] {
        let parsed = Cli::try_parse_from(arguments).unwrap();
        let error = cli::plan_dry_run(&parsed, &mut FixedIds(Uuid::nil())).unwrap_err();
        assert_eq!(error.exit_code(), 2);
    }
    for arguments in [
        vec!["asr", "onboarding", "install", "--stdin"],
        vec!["asr", "onboarding", "install", "--provider", "omp"],
        vec![
            "asr",
            "onboarding",
            "install",
            "--provider",
            "unknown",
            "--stdin",
        ],
    ] {
        assert!(Cli::try_parse_from(arguments).is_err());
    }
}
