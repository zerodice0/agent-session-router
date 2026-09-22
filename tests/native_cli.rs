use agent_session_router::{
    client::{ClientConfig, ClientError, ClientEvent, ClientRole, RouterClient},
    credentials::{CredentialFile, CredentialRole, read_credential, write_credential_exclusive},
    process::{HealthMarker, RuntimeRecord},
    protocol::{
        AgentClient, AgentRegistration, AgentSide, ClientMessage, DeliveryMode,
        TaskExecutionEvidence, WorkspaceName,
    },
    tasks::{PauseReason, TaskCheckpoint},
    tls::load_client_config,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rmcp::{ServiceExt as _, model::CallToolRequestParams};
use std::{
    ffi::{OsStr, OsString},
    fs,
    io::{BufRead as _, BufReader, Write as _},
    net::{Ipv4Addr, TcpListener, TcpStream},
    os::unix::{
        ffi::{OsStrExt as _, OsStringExt as _},
        fs::{PermissionsExt as _, symlink},
    },
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use url::Url;

use serde_json::Value;
use tempfile::TempDir;
use uuid::Uuid;

struct NativeHarness {
    root: TempDir,
    executable: PathBuf,
}

impl NativeHarness {
    fn new() -> Self {
        let executable = std::env::var_os("ASR_TEST_EXECUTABLE")
            .map_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_asr")), PathBuf::from);
        let executable = if executable.is_absolute() {
            executable
        } else {
            std::env::current_dir()
                .expect("native CLI working directory")
                .join(executable)
        };
        let root = tempfile::Builder::new()
            .prefix("router-native-")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(
                std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .expect("HOME directory"),
            )
            .expect("private temporary directory");
        for name in [".config", ".codex", ".claude", ".omp"] {
            fs::create_dir(root.path().join(name)).expect("isolated provider directory");
            fs::set_permissions(root.path().join(name), fs::Permissions::from_mode(0o700))
                .expect("private provider directory");
        }
        Self { root, executable }
    }

    fn data_dir(&self) -> PathBuf {
        self.root.path().join("agent-session-router")
    }

    fn process(&self) -> Command {
        let mut command = Command::new(&self.executable);
        command
            .env("HOME", self.root.path())
            .env("XDG_CONFIG_HOME", self.root.path().join(".config"))
            .env("XDG_DATA_HOME", self.root.path().join(".local/share"))
            .env("CODEX_HOME", self.root.path().join(".codex"))
            .env("CLAUDE_CONFIG_DIR", self.root.path().join(".claude"))
            .env("OMP_CONFIG_DIR", self.root.path().join(".omp"))
            .env("ASR_DATA_DIR", self.data_dir())
            .env("ASR_CONFIG_PATH", self.root.path().join("config.json"))
            .env("ASR_BIND", "127.0.0.1:0");
        for key in [
            "ROUTER_URL",
            "ASR_CREDENTIAL_FILE",
            "ASR_BOOTSTRAP_DIR",
            "ASR_INTEGRATIONS_DIR",
            "ASR_CA_FILE",
            "NODE_EXTRA_CA_CERTS",
            "ROUTER_PUBLIC_URL",
            "ROUTER_TLS_CERT",
            "ROUTER_TLS_KEY",
            "SSL_CERT_DIR",
            "SSL_CERT_FILE",
        ] {
            command.env_remove(key);
        }
        command
    }

    fn command(&self, arguments: impl IntoIterator<Item = impl AsRef<OsStr>>) -> Output {
        self.process()
            .args(arguments)
            .output()
            .expect("native CLI invocation")
    }
    fn command_with_stdin(
        &self,
        arguments: impl IntoIterator<Item = impl AsRef<OsStr>>,
        input: &str,
    ) -> Output {
        let mut child = self
            .process()
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("native CLI invocation");
        child
            .stdin
            .take()
            .expect("command stdin")
            .write_all(input.as_bytes())
            .expect("write command stdin");
        child.wait_with_output().expect("native CLI output")
    }
    fn spawn(&self, arguments: impl IntoIterator<Item = impl AsRef<OsStr>>) -> Child {
        self.process()
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("native CLI process")
    }

    fn stop(&self) {
        let _ = self.command(["router", "stop"]);
    }
}

impl Drop for NativeHarness {
    fn drop(&mut self) {
        self.stop();
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("UTF-8 stdout")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("UTF-8 stderr")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed: stdout={} stderr={}",
        stdout(output),
        stderr(output)
    );
}

struct CertificateFixture {
    certificate: PathBuf,
    private_key: PathBuf,
    ca: PathBuf,
}

fn certificate_fixture(root: &Path) -> CertificateFixture {
    let directory = root.join("tls");
    fs::create_dir(&directory).expect("create certificate directory");
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
        .expect("protect certificate directory");

    let mut ca_params =
        CertificateParams::new(vec!["asr-native-test-ca".to_owned()]).expect("CA parameters");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().expect("CA key");
    let ca_certificate = ca_params.self_signed(&ca_key).expect("CA certificate");
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let mut server_params =
        CertificateParams::new(vec!["localhost".to_owned()]).expect("server parameters");
    server_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().expect("server key");
    let server_certificate = server_params
        .signed_by(&server_key, &issuer)
        .expect("server certificate");

    let certificate_file = directory.join("server-chain.pem");
    let private_key_file = directory.join("server-key.pem");
    let ca_file = directory.join("ca.pem");
    fs::write(
        &certificate_file,
        format!("{}{}", server_certificate.pem(), ca_certificate.pem()),
    )
    .expect("write server chain");
    fs::write(&private_key_file, server_key.serialize_pem()).expect("write private key");
    fs::set_permissions(&private_key_file, fs::Permissions::from_mode(0o600))
        .expect("protect private key");
    fs::write(&ca_file, ca_certificate.pem()).expect("write CA certificate");
    CertificateFixture {
        certificate: certificate_file,
        private_key: private_key_file,
        ca: ca_file,
    }
}

fn reserve_loopback_port() -> u16 {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .expect("reserve loopback port")
        .local_addr()
        .expect("reserved address")
        .port()
}

fn tls_process(harness: &NativeHarness, fixture: &CertificateFixture, port: u16) -> Command {
    let mut command = harness.process();
    command
        .env("ASR_BIND", format!("127.0.0.1:{port}"))
        .env("ASR_CA_FILE", &fixture.ca)
        .env("ROUTER_TLS_CERT", &fixture.certificate)
        .env("ROUTER_TLS_KEY", &fixture.private_key)
        .env("ROUTER_PUBLIC_URL", format!("wss://localhost:{port}/ws"));
    command
}

fn trusted_control_process(
    harness: &NativeHarness,
    fixture: &CertificateFixture,
    port: u16,
) -> Command {
    let mut command = harness.process();
    command
        .env("ASR_BIND", format!("127.0.0.1:{port}"))
        .env("ASR_CA_FILE", &fixture.ca);
    command
}

fn trusted_control_command(
    harness: &NativeHarness,
    fixture: &CertificateFixture,
    port: u16,
    arguments: impl IntoIterator<Item = impl AsRef<OsStr>>,
) -> Output {
    trusted_control_process(harness, fixture, port)
        .args(arguments)
        .output()
        .expect("trusted router control invocation")
}

fn tls_command(
    harness: &NativeHarness,
    fixture: &CertificateFixture,
    port: u16,
    arguments: impl IntoIterator<Item = impl AsRef<OsStr>>,
) -> Output {
    tls_process(harness, fixture, port)
        .args(arguments)
        .output()
        .expect("TLS CLI invocation")
}

fn assert_router_still_listening(port: u16) {
    TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}").parse().expect("socket address"),
        Duration::from_secs(1),
    )
    .expect("router socket remains available");
}

fn agent_credential_file(
    harness: &NativeHarness,
    name: &str,
    side: AgentSide,
    client: AgentClient,
) -> PathBuf {
    let directory = harness.root.path().join("host-credentials");
    fs::create_dir_all(&directory).expect("create credential directory");
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
        .expect("protect credential directory");
    let path = directory.join(format!("{name}.credential.json"));
    let credential = CredentialFile::generate(
        CredentialRole::Agent,
        name.to_owned(),
        Some(side),
        Some(client),
        Vec::new(),
    )
    .expect("generate agent credential");
    write_credential_exclusive(&path, &credential).expect("write agent credential");
    path
}

fn write_host_fixture(path: &Path, omp_plugin: &Path) {
    let script = format!(
        "#!/bin/sh\nif [ \"$1\" = plugin ] && [ \"$2\" = list ] && [ \"$3\" = --json ]; then\n  case \"${{ASR_TEST_OMP_STATE-}}\" in\n    missing) printf '%s' '{{\"npm\":[]}}' ;;\n    disabled) printf '%s' '{{\"npm\":[{{\"name\":\"@agent-session-router/omp-integration\",\"path\":\"{0}\",\"enabled\":false}}]}}' ;;\n    conflict) printf '%s' '{{\"npm\":[{{\"name\":\"@agent-session-router/omp-integration\",\"path\":\"/\",\"enabled\":true}}]}}' ;;\n    *) printf '%s' '{{\"npm\":[{{\"name\":\"@agent-session-router/omp-integration\",\"path\":\"{0}\",\"enabled\":true}}]}}' ;;\n  esac\n  exit 0\nfi\nprintf '%s\\n' \"$PWD\" > \"${{ASR_TEST_LOG}}.cwd\"\n: > \"${{ASR_TEST_LOG}}.args\"\nfor argument in \"$@\"; do printf '%s\\n' \"$argument\" >> \"${{ASR_TEST_LOG}}.args\"; done\nprintf '%s' \"${{ASR_EXECUTABLE-}}\" > \"${{ASR_TEST_LOG}}.executable\"\nprintf '%s' \"${{ASR_CREDENTIAL_FILE-}}\" > \"${{ASR_TEST_LOG}}.credential\"\nprintf '%s' \"${{ASR_WORKSPACE-}}\" > \"${{ASR_TEST_LOG}}.workspace\"\nexit \"${{ASR_TEST_EXIT:-0}}\"\n",
        omp_plugin.display()
    );
    fs::write(path, script).expect("write host fixture");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("host fixture mode");
}

const CODEX_APP_SERVER_FIXTURE: &str = r"
const fs = require('fs');
const readline = require('readline');
const record = process.argv[2];
function append(value) {
  fs.appendFileSync(record, JSON.stringify(value) + '\n');
}
function send(value) {
  process.stdout.write(JSON.stringify(value) + '\n');
}
const rl = readline.createInterface({input: process.stdin, crlfDelay: Infinity});
rl.on('line', raw => {
  const message = JSON.parse(raw);
  if (message.method === 'initialize') {
    append({
      event: 'initialize',
      pid: process.pid,
      cwd: process.cwd(),
      argv: process.argv.slice(3),
      sensitiveEnvironment: Object.fromEntries(
        Object.entries(process.env).filter(([key]) =>
          key.includes('ROUTER_TOKEN') ||
          key.includes('DELEGATION_TOKEN') ||
          key.includes('CREDENTIAL_FILE'))
      )
    });
    send({id: message.id, result: {}});
    return;
  }
  if (message.method === 'initialized') return;
  if (message.method === 'thread/start') {
    send({id: message.id, result: {thread: {id: 'thread-1'}}});
    return;
  }
  if (message.method === 'turn/start') {
    const turnId = 'turn-' + message.id;
    const text = message.params.input[0].text;
    append({event: 'turn', text});
    send({id: message.id, result: {turn: {id: turnId}}});
    send({
      method: 'item/completed',
      params: {
        threadId: 'thread-1',
        turnId,
        item: {type: 'agentMessage', text: 'answer:' + text, phase: 'final_answer'}
      }
    });
    send({
      method: 'turn/completed',
      params: {threadId: 'thread-1', turn: {id: turnId, status: 'completed'}}
    });
  }
});
rl.on('close', () => {
  append({event: 'closed'});
  process.exit(0);
});
";

fn write_codex_app_server_fixture(bin: &Path, record: &Path) {
    let script = bin.join("codex-fixture.js");
    fs::write(&script, CODEX_APP_SERVER_FIXTURE).expect("write Codex fixture script");
    let executable = bin.join("codex");
    fs::write(
        &executable,
        format!(
            "#!/bin/sh\nexec node '{}' '{}' \"$@\"\n",
            script.display(),
            record.display()
        ),
    )
    .expect("write Codex fixture executable");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
        .expect("Codex fixture mode");
}

fn recorded_arguments(log: &Path) -> Vec<Vec<u8>> {
    fs::read(log.with_extension("args"))
        .expect("recorded arguments")
        .split(|byte| *byte == b'\n')
        .filter(|argument| !argument.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

fn run_host_fixture(
    harness: &NativeHarness,
    arguments: &[OsString],
    bin: &Path,
    integrations: &Path,
    caller_cwd: &Path,
    log: &Path,
    exit: i32,
) -> Output {
    harness
        .process()
        .args(arguments)
        .current_dir(caller_cwd)
        .env("PATH", bin)
        .env("ASR_INTEGRATIONS_DIR", integrations)
        .env("ASR_TEST_LOG", log)
        .env("ASR_TEST_EXIT", exit.to_string())
        .output()
        .expect("host fixture command")
}

fn profile_arguments(command: &[&str]) -> Vec<String> {
    ["--profile", "this-device"]
        .into_iter()
        .chain(command.iter().copied())
        .map(str::to_owned)
        .collect()
}

#[test]
fn native_router_start_preserves_onboarded_this_device_profile() {
    use agent_session_router::{
        config::{self, ConfigFile, Profile, ProviderBinding, StoredOnboardingRoute},
        onboarding::{OnboardingProvider, RouteKind},
    };

    let harness = NativeHarness::new();
    let credential_file = agent_credential_file(
        &harness,
        "bound-claude",
        AgentSide::Claude,
        AgentClient::ClaudeCode,
    );
    let endpoint = "wss://onboarded.example/ws".to_owned();
    let profile = Profile {
        router_url: endpoint.clone(),
        server_id: Some(Uuid::new_v4()),
        routes: vec![StoredOnboardingRoute {
            kind: RouteKind::Public,
            router_url: endpoint,
            ca_file: None,
        }],
        bindings: [(
            OnboardingProvider::ClaudeCode,
            ProviderBinding {
                credential_file,
                workspace: WorkspaceName::parse("existing-room").unwrap(),
            },
        )]
        .into_iter()
        .collect(),
    };
    let expected = serde_json::to_value(&profile).unwrap();
    let path = harness.root.path().join("config.json");
    config::save_config(
        &path,
        &ConfigFile {
            version: config::CONFIG_VERSION,
            default_profile: Some("this-device".to_owned()),
            profiles: [("this-device".to_owned(), profile)].into_iter().collect(),
        },
    )
    .unwrap();

    // Both initial startup and reuse publish the runtime's convenience profile.
    for _ in 0..2 {
        assert_success(&harness.command(["router", "start", "--background"]));
        let stored = config::load_config(&path).unwrap();
        assert_eq!(
            serde_json::to_value(&stored.profiles["this-device"]).unwrap(),
            expected
        );
        assert_eq!(stored.default_profile.as_deref(), Some("this-device"));
    }
    assert_success(&harness.command(["router", "stop"]));
}

#[allow(clippy::too_many_lines)]
#[test]
fn native_cli_owns_router_profiles_install_workspaces_and_credentials() {
    let harness = NativeHarness::new();

    let install_dir = harness.root.path().join("bin");
    let install = harness.command([
        OsStr::new("install"),
        OsStr::new("--bin-dir"),
        install_dir.as_os_str(),
    ]);
    assert_success(&install);
    assert!(install_dir.join("asr").is_file());

    let profile = harness.command(["profile", "add", "office", "wss://router.example.test/ws"]);
    assert_success(&profile);
    let profiles = harness.command(["profile", "list"]);
    assert_success(&profiles);
    assert!(stdout(&profiles).contains("office wss://router.example.test/ws"));

    let first_start = harness.command(["router", "start", "--background"]);
    assert_success(&first_start);
    assert!(stdout(&first_start).contains("router started at ws://127.0.0.1:"));
    let second_start = harness.command(["router", "start", "--background"]);
    assert_success(&second_start);
    assert!(stdout(&second_start).contains("router already running at"));

    let create = harness.command(profile_arguments(&["workspace", "create", "native-room"]));
    assert_success(&create);
    let list = harness.command(profile_arguments(&["workspace", "list", "--json"]));
    assert_success(&list);
    let workspace: Value = serde_json::from_str(stdout(&list).trim()).expect("workspace JSON");
    assert_eq!(workspace["name"], "native-room");
    let integrations = harness.command(profile_arguments(&[
        "integration",
        "list",
        "native-room",
        "--json",
    ]));
    assert_success(&integrations);
    let integrations: Value =
        serde_json::from_str(stdout(&integrations).trim()).expect("integration list JSON");
    assert_eq!(integrations["integrations"], serde_json::json!([]));
    let check = harness.command(profile_arguments(&[
        "integration",
        "check",
        "native-room",
        "github",
        "--json",
    ]));
    assert_eq!(check.status.code(), Some(1));
    assert!(stderr(&check).contains("integration_not_configured"));

    let reload = harness.command(profile_arguments(&[
        "integration",
        "admin",
        "reload",
        "--json",
    ]));
    assert_success(&reload);
    let reload: Value =
        serde_json::from_str(stdout(&reload).trim()).expect("integration reload JSON");
    assert_eq!(reload["integrations"], serde_json::json!([]));
    let join = harness.command(profile_arguments(&["workspace", "join", "native-room"]));
    assert_success(&join);
    assert!(stdout(&join).contains("joined native-room"));
    let target_operation = Uuid::new_v4();
    let resolution_id = Uuid::new_v4();
    let resolve = harness.command_with_stdin(
        profile_arguments(&[
            "task",
            "external-resolve",
            "native-room",
            &target_operation.to_string(),
            "--not-applied",
            "--stdin",
            "--resolution-id",
            &resolution_id.to_string(),
        ]),
        "private reconciliation note",
    );
    assert_eq!(resolve.status.code(), Some(1));
    assert!(stderr(&resolve).contains(&format!("resolutionId: {resolution_id}")));
    assert!(!stderr(&resolve).contains("operationId:"));
    assert!(!stderr(&resolve).contains("private reconciliation note"));
    let generated_resolve = harness.command_with_stdin(
        profile_arguments(&[
            "task",
            "external-resolve",
            "native-room",
            &Uuid::new_v4().to_string(),
            "--not-applied",
            "--stdin",
        ]),
        "another private note",
    );
    assert_eq!(generated_resolve.status.code(), Some(1));
    let generated_error = stderr(&generated_resolve);
    let generated_resolution_id = generated_error
        .split("resolutionId: ")
        .nth(1)
        .and_then(|value| value.trim().strip_suffix(')'))
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("generated resolution id in error");
    assert_eq!(generated_resolution_id.get_version_num(), 4);
    assert!(!generated_error.contains("operationId:"));
    assert!(!generated_error.contains("another private note"));
    assert!(stdout(&join).contains("left native-room"));
    let missing_watch = harness.command(profile_arguments(&[
        "task",
        "watch",
        "native-room",
        "--task",
        "999999",
        "--json",
    ]));
    assert_eq!(missing_watch.status.code(), Some(1));
    assert!(stderr(&missing_watch).contains("task_not_found"));

    let credential_path = harness.root.path().join("worker.json");
    let issue = harness.command([
        OsStr::new("--profile"),
        OsStr::new("this-device"),
        OsStr::new("credential"),
        OsStr::new("issue"),
        OsStr::new("--agent"),
        OsStr::new("native-worker"),
        OsStr::new("--side"),
        OsStr::new("generic"),
        OsStr::new("--client"),
        OsStr::new("omp"),
        OsStr::new("--workspace"),
        OsStr::new("native-room"),
        OsStr::new("--output"),
        credential_path.as_os_str(),
    ]);
    assert_success(&issue);
    let credential: Value =
        serde_json::from_slice(&fs::read(&credential_path).expect("credential file"))
            .expect("credential JSON");
    let credential_id = credential["id"]
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("credential id");
    let token = credential["token"].as_str().expect("credential token");
    assert!(!stdout(&issue).contains(token));

    let credentials = harness.command(profile_arguments(&["credential", "list", "--json"]));
    assert_success(&credentials);
    assert!(stdout(&credentials).contains(&credential_id.to_string()));
    assert!(!stdout(&credentials).contains(token));
    let create_operation = Uuid::new_v4();
    let create_arguments = profile_arguments(&[
        "task",
        "create",
        "native-room",
        "--stdin",
        "--json",
        "--operation-id",
        &create_operation.to_string(),
    ]);
    let task_input =
        serde_json::json!({"title": "Native\u{001b} task", "description": "CLI lifecycle"})
            .to_string();
    let create = harness.command_with_stdin(&create_arguments, &task_input);
    assert_success(&create);
    let created: Value = serde_json::from_str(stdout(&create).trim()).expect("task create JSON");
    assert_eq!(created["operationId"], create_operation.to_string());
    let task_id = created["task"]["id"].as_i64().expect("task id");
    let created_version = created["appliedVersion"].as_i64().expect("task version");

    let replay = harness.command_with_stdin(&create_arguments, &task_input);
    assert_success(&replay);
    let replayed: Value = serde_json::from_str(stdout(&replay).trim()).expect("task replay JSON");
    assert_eq!(replayed["task"]["id"], task_id);
    assert_eq!(replayed["appliedVersion"], created_version);

    let assigned = harness.command(profile_arguments(&[
        "task",
        "assign",
        "native-room",
        &task_id.to_string(),
        "--agent",
        "native-worker",
        "--expected-version",
        &created_version.to_string(),
        "--json",
    ]));
    assert_success(&assigned);
    let assigned: Value = serde_json::from_str(stdout(&assigned).trim()).expect("task assign JSON");
    assert_eq!(assigned["task"]["assignedAgentId"], "native-worker");
    let noted = harness.command_with_stdin(
        profile_arguments(&[
            "task",
            "note",
            "native-room",
            &task_id.to_string(),
            "--stdin",
            "--json",
        ]),
        "immutable history note",
    );
    assert_success(&noted);
    let noted: Value = serde_json::from_str(stdout(&noted).trim()).expect("task note JSON");
    let noted_version = noted["appliedVersion"].as_i64().expect("noted version");

    let show = harness.command(profile_arguments(&[
        "task",
        "show",
        "native-room",
        &task_id.to_string(),
        "--json",
    ]));
    assert_success(&show);
    let shown: Value = serde_json::from_str(stdout(&show).trim()).expect("task show JSON");
    assert_eq!(shown["id"], task_id);
    assert_eq!(shown["version"], noted_version);
    assert_eq!(shown["assignedAgentId"], "native-worker");

    let show_human = harness.command(profile_arguments(&[
        "task",
        "show",
        "native-room",
        &task_id.to_string(),
    ]));
    assert_success(&show_human);
    assert!(!show_human.stdout.contains(&0x1b));
    assert!(stdout(&show_human).contains("\\u{001B}"));

    let tasks = harness.command(profile_arguments(&[
        "task",
        "list",
        "native-room",
        "--json",
    ]));
    assert_success(&tasks);
    let tasks: Value = serde_json::from_str(stdout(&tasks).trim()).expect("task list JSON");
    assert_eq!(tasks["tasks"][0]["id"], task_id);
    assert_eq!(tasks["tasks"][0]["assignedAgentId"], "native-worker");
    assert!(tasks["nextCursor"].is_string());

    let history = harness.command(profile_arguments(&[
        "task",
        "history",
        "native-room",
        &task_id.to_string(),
        "--json",
    ]));
    assert_success(&history);
    let history: Value = serde_json::from_str(stdout(&history).trim()).expect("task history JSON");
    assert_eq!(history["taskId"], task_id);
    assert_eq!(history["events"][0]["change"], "created");
    assert_eq!(history["events"][1]["change"], "assigned");
    assert_eq!(history["events"][2]["change"], "noted");
    assert_eq!(
        history["events"][2]["report"]["body"],
        "immutable history note"
    );

    let revoke = harness.command(profile_arguments(&[
        "credential",
        "revoke",
        &credential_id.to_string(),
    ]));
    assert_success(&revoke);

    let stop = harness.command(["router", "stop"]);
    assert_success(&stop);
    assert!(stdout(&stop).contains("router stopped at"));
    assert!(!harness.data_dir().join("router-runtime.json").exists());
}

#[cfg(unix)]
#[test]
fn native_foreground_no_ui_sigint_stops_server_and_removes_owned_runtime() {
    let harness = NativeHarness::new();
    let mut foreground = harness.spawn(["router", "start", "--no-ui"]);
    let runtime_path = harness.data_dir().join("router-runtime.json");
    let deadline = Instant::now() + Duration::from_secs(15);
    let record: RuntimeRecord = loop {
        if let Ok(bytes) = fs::read(&runtime_path)
            && let Ok(record) = serde_json::from_slice(&bytes)
        {
            break record;
        }
        assert!(
            foreground.try_wait().expect("foreground status").is_none(),
            "foreground router exited before publishing its runtime"
        );
        if Instant::now() >= deadline {
            foreground.kill().expect("kill timed-out foreground CLI");
            let _ = foreground.wait();
            panic!("foreground router did not become ready");
        }
        thread::sleep(Duration::from_millis(10));
    };
    // Authenticate against the actual server, not just a published startup record.
    let listed = harness
        .process()
        .env("ROUTER_URL", &record.control_url)
        .args(["workspace", "list", "--json"])
        .output()
        .expect("foreground workspace list");
    assert_success(&listed);
    let pid = rustix::process::Pid::from_raw(i32::try_from(foreground.id()).unwrap()).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::INT).expect("signal private CLI");
    let deadline =
        Instant::now() + agent_session_router::process::SHUTDOWN_TIMEOUT + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = foreground.try_wait().expect("foreground exit") {
            break status;
        }
        if Instant::now() >= deadline {
            foreground.kill().expect("kill timed-out foreground CLI");
            let _ = foreground.wait();
            panic!("foreground CLI did not reap its router after SIGINT");
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(status.code(), Some(130));
    let address = Url::parse(&record.control_url)
        .unwrap()
        .socket_addrs(|| None)
        .unwrap()[0];
    assert!(
        TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_err(),
        "the foreground router must stop before the CLI exits"
    );
    assert!(
        agent_session_router::process::RuntimeStore::new(harness.data_dir())
            .unwrap()
            .read()
            .unwrap()
            .is_none()
    );
}

#[cfg(unix)]
#[test]
fn workspace_watch_streams_json_from_cursor_and_handles_sigint() {
    let harness = NativeHarness::new();
    assert_success(&harness.command(["router", "start", "--background"]));
    assert_success(&harness.command(profile_arguments(&["workspace", "create", "watch-room"])));

    let first = watch_for_chat(&harness, 0, "first message");
    let first_seq = first["seq"].as_i64().expect("first event sequence");
    assert_eq!(first["kind"], "chat");
    assert_eq!(first["content"], "first message");

    let second = watch_for_chat(&harness, first_seq, "second message");
    assert_eq!(second["kind"], "chat");

    assert_eq!(second["content"], "second message");
    assert!(second["seq"].as_i64().expect("second event sequence") > first_seq);
}
#[test]
fn task_list_page_over_credential_burst_has_no_per_task_rpc() {
    let harness = NativeHarness::new();
    assert_success(&harness.command(["router", "start", "--background"]));
    assert_success(&harness.command(profile_arguments(&[
        "workspace",
        "create",
        "many-task-room",
    ])));

    for index in 0..45 {
        let input =
            serde_json::json!({"title": format!("task-{index:02}"), "description": ""}).to_string();
        let create = harness.command_with_stdin(
            profile_arguments(&["task", "create", "many-task-room", "--stdin"]),
            &input,
        );
        assert_success(&create);
        thread::sleep(Duration::from_millis(110));
    }

    let list = harness.command(profile_arguments(&[
        "task",
        "list",
        "many-task-room",
        "--limit",
        "45",
        "--json",
    ]));
    assert_success(&list);
    let page: Value = serde_json::from_str(stdout(&list).trim()).expect("task list page JSON");
    assert_eq!(page["tasks"].as_array().expect("task rows").len(), 45);
    assert_eq!(page["hasMore"], false);
}

#[cfg(unix)]
fn watch_for_chat(harness: &NativeHarness, after: i64, content: &str) -> Value {
    let mut watcher = harness.spawn(profile_arguments(&[
        "workspace",
        "watch",
        "watch-room",
        "--after",
        &after.to_string(),
        "--json",
    ]));
    let stdout = watcher.stdout.take().expect("watch stdout");
    let (sender, receiver) = mpsc::channel();
    let expected = content.to_owned();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.expect("watch output line");
            let event: Value = serde_json::from_str(&line).expect("workspace event JSON");
            if event["kind"] == "chat" && event["content"] == expected {
                sender.send(event).expect("deliver workspace event");
                return;
            }
        }
    });

    thread::sleep(Duration::from_millis(150));
    let post = harness.command_with_stdin(
        profile_arguments(&["workspace", "post", "watch-room", "--stdin"]),
        content,
    );
    assert_success(&post);
    let event = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("workspace watch event");

    let signal = Command::new("kill")
        .args(["-INT", &watcher.id().to_string()])
        .status()
        .expect("send SIGINT");
    assert!(signal.success());
    let status = watcher.wait().expect("watcher exit");
    assert_eq!(status.code(), Some(130));
    event
}

#[cfg(unix)]
#[test]
fn task_watch_streams_typed_json_and_handles_sigint() {
    let harness = NativeHarness::new();
    assert_success(&harness.command(["router", "start", "--background"]));
    assert_success(&harness.command(profile_arguments(&[
        "workspace",
        "create",
        "task-watch-room",
    ])));

    let mut watcher = harness.spawn(profile_arguments(&[
        "task",
        "watch",
        "task-watch-room",
        "--json",
    ]));
    let watch_stdout = watcher.stdout.take().expect("watch stdout");
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(watch_stdout).lines() {
            let event: Value =
                serde_json::from_str(&line.expect("watch output line")).expect("task event JSON");
            if event["change"] == "created" {
                sender.send(event).expect("deliver task event");
                return;
            }
        }
    });

    thread::sleep(Duration::from_millis(150));
    let create = harness.command_with_stdin(
        profile_arguments(&["task", "create", "task-watch-room", "--stdin", "--json"]),
        r#"{"title":"watched","description":"typed event"}"#,
    );
    assert_success(&create);
    let created: Value = serde_json::from_str(stdout(&create).trim()).expect("create JSON");
    let event = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("task watch event");
    assert_eq!(event["task"]["id"], created["task"]["id"]);
    assert!(event["seq"].as_i64().is_some());

    let signal = Command::new("kill")
        .args(["-INT", &watcher.id().to_string()])
        .status()
        .expect("send SIGINT");
    assert!(signal.success());
    assert_eq!(watcher.wait().expect("watcher exit").code(), Some(130));
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_cli_interrupt_queues_handoff_until_executor_stops() {
    let harness = NativeHarness::new();
    assert_success(&harness.command(["router", "start", "--background"]));
    assert_success(&harness.command(profile_arguments(&["workspace", "create", "handoff-room"])));

    let credential_a_path = harness.root.path().join("worker-a.json");
    let credential_b_path = harness.root.path().join("worker-b.json");
    for (agent, path) in [
        ("worker-a", credential_a_path.as_path()),
        ("worker-b", credential_b_path.as_path()),
    ] {
        assert_success(&harness.command([
            OsStr::new("--profile"),
            OsStr::new("this-device"),
            OsStr::new("credential"),
            OsStr::new("issue"),
            OsStr::new("--agent"),
            OsStr::new(agent),
            OsStr::new("--side"),
            OsStr::new("generic"),
            OsStr::new("--client"),
            OsStr::new("omp"),
            OsStr::new("--workspace"),
            OsStr::new("handoff-room"),
            OsStr::new("--output"),
            path.as_os_str(),
        ]));
    }

    let create = harness.command_with_stdin(
        profile_arguments(&["task", "create", "handoff-room", "--stdin", "--json"]),
        r#"{"title":"handoff","description":"interrupt lifecycle"}"#,
    );
    assert_success(&create);
    let created: Value = serde_json::from_str(stdout(&create).trim()).expect("create JSON");
    let task_id = created["task"]["id"].as_i64().expect("task id");
    let created_version = created["appliedVersion"].as_i64().expect("create version");
    let assign = harness.command(profile_arguments(&[
        "task",
        "assign",
        "handoff-room",
        &task_id.to_string(),
        "--agent",
        "worker-a",
        "--expected-version",
        &created_version.to_string(),
        "--json",
    ]));
    assert_success(&assign);
    let assigned: Value = serde_json::from_str(stdout(&assign).trim()).expect("assign JSON");
    let assigned_version = assigned["appliedVersion"].as_i64().expect("assign version");

    let runtime: RuntimeRecord = serde_json::from_slice(
        &fs::read(harness.data_dir().join("router-runtime.json")).expect("runtime record"),
    )
    .expect("runtime record JSON");
    let credential: CredentialFile =
        serde_json::from_slice(&fs::read(&credential_a_path).expect("agent credential"))
            .expect("agent credential JSON");
    let (agent, mut events) = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&runtime.control_url).expect("router URL"),
        role: ClientRole::Primary {
            agent: AgentRegistration {
                agent_id: "worker-a".to_owned(),
                side: AgentSide::Generic,
                client: AgentClient::Omp,
                activity: None,
                delivery_mode: DeliveryMode::Push,
            },
            credential,
            delegation_token: None,
        },
        ca_file: None,
    })
    .await
    .expect("connect worker");
    let room = WorkspaceName::parse("handoff-room").expect("workspace name");
    agent
        .workspace_join(room.clone())
        .await
        .expect("join workspace");

    let mut request = harness.spawn(profile_arguments(&[
        "task",
        "request",
        "handoff-room",
        &task_id.to_string(),
        "--expected-version",
        &assigned_version.to_string(),
        "--timeout-ms",
        "10000",
        "--json",
    ]));
    let (work_request_id, dispatch_version) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let item = events.recv().await.expect("worker event stream");
            match item.event {
                ClientEvent::Delivery {
                    request_id,
                    task: Some(task),
                    ..
                } => break (request_id, task.expected_version),
                ClientEvent::Closed(code) => panic!("worker connection closed: {code:?}"),
                _ => {}
            }
        }
    })
    .await
    .expect("task request delivery");
    let begun = agent
        .task_mutation(ClientMessage::TaskBegin {
            request_id: "native-begin".to_owned(),
            workspace: room.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            work_request_id: work_request_id.clone(),
            expected_version: dispatch_version,
            last_checkpoint_id: None,
            resume_note: "starting".to_owned(),
        })
        .await
        .expect("begin task");
    let attempt = begun
        .task
        .current_attempt
        .as_ref()
        .expect("running attempt")
        .clone();
    let checkpointed = agent
        .task_mutation(ClientMessage::TaskCheckpoint {
            request_id: "native-checkpoint".to_owned(),
            workspace: room.clone(),
            operation_id: Uuid::new_v4(),
            task_id,
            attempt_id: attempt.id,
            expected_version: begun.applied_version,
            checkpoint: TaskCheckpoint {
                summary: "checkpoint summary".to_owned(),
                next_steps: "handoff next".to_owned(),
                artifacts: vec!["artifact://checkpoint".to_owned()],
                risks: "none".to_owned(),
            },
        })
        .await
        .expect("checkpoint task");

    let interrupted = harness.command_with_stdin(
        profile_arguments(&[
            "task",
            "interrupt",
            "handoff-room",
            &task_id.to_string(),
            "--expected-version",
            &checkpointed.applied_version.to_string(),
            "--stdin",
            "--json",
        ]),
        "operator handoff",
    );
    assert_success(&interrupted);
    let interrupted: Value =
        serde_json::from_str(stdout(&interrupted).trim()).expect("interrupt JSON");
    let interrupted_version = interrupted["appliedVersion"]
        .as_i64()
        .expect("interrupt version");

    let cancellation = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let item = events.recv().await.expect("worker event stream");
            if let ClientEvent::WorkCancelled {
                request_id,
                task: Some(fence),
                ..
            } = item.event
                && request_id == work_request_id
            {
                break fence;
            }
        }
    })
    .await
    .expect("task cancellation");
    assert_eq!(cancellation.attempt_id, attempt.id);

    let queued_handoff = harness.command(profile_arguments(&[
        "task",
        "assign",
        "handoff-room",
        &task_id.to_string(),
        "--agent",
        "worker-b",
        "--expected-version",
        &interrupted_version.to_string(),
        "--json",
    ]));
    assert_success(&queued_handoff);
    let queued_handoff: Value =
        serde_json::from_str(stdout(&queued_handoff).trim()).expect("queued handoff JSON");
    assert_eq!(queued_handoff["task"]["assignedAgentId"], "worker-b");
    assert_eq!(queued_handoff["task"]["stopEvidence"], "unknown");

    agent
        .work_idle(room.clone(), work_request_id, Some(cancellation), true)
        .await
        .expect("worker idle");
    agent
        .task_execution_stopped(
            room,
            task_id,
            attempt.id,
            attempt.session_id,
            TaskExecutionEvidence::ProviderTerminal,
            PauseReason::OperatorInterrupt,
        )
        .await
        .expect("execution stopped");

    let _request_status = request.wait().expect("task request exit");
    let show = harness.command(profile_arguments(&[
        "task",
        "show",
        "handoff-room",
        &task_id.to_string(),
        "--json",
    ]));
    assert_success(&show);
    let shown: Value = serde_json::from_str(stdout(&show).trim()).expect("show JSON");
    assert_eq!(shown["state"], "paused");
    assert_eq!(shown["stopEvidence"], "confirmed");
    assert_eq!(shown["assignedAgentId"], "worker-b");
    let history = harness.command(profile_arguments(&[
        "task",
        "history",
        "handoff-room",
        &task_id.to_string(),
        "--json",
    ]));
    assert_success(&history);
    let history: Value = serde_json::from_str(stdout(&history).trim()).expect("history JSON");
    let begun_event = history["events"]
        .as_array()
        .expect("history events")
        .iter()
        .find(|event| event["change"] == "begun")
        .expect("begun history event");
    assert_eq!(begun_event["attempt"]["id"], attempt.id.to_string());
    assert_eq!(begun_event["attempt"]["stopEvidence"], "confirmed");
    let checkpoint_event = history["events"]
        .as_array()
        .expect("history events")
        .iter()
        .find(|event| event["change"] == "checkpoint")
        .expect("checkpoint history event");
    assert_eq!(checkpoint_event["attempt"]["id"], attempt.id.to_string());
    assert_eq!(
        checkpoint_event["report"]["body"]["summary"],
        "checkpoint summary"
    );
    assert_eq!(
        checkpoint_event["report"]["body"]["nextSteps"],
        "handoff next"
    );
    agent.close().await.expect("close worker");
}

#[test]
#[allow(clippy::too_many_lines)]
fn native_dispatch_executes_stock_and_setup_host_plans_exactly() {
    let harness = NativeHarness::new();
    let bin = harness.root.path().join("host-bin");
    let integrations = harness.root.path().join("integrations");
    let omp_plugin = integrations.join("omp");
    let caller_cwd = harness.root.path().join("caller");
    fs::create_dir(&bin).expect("create host bin");
    fs::create_dir_all(&omp_plugin).expect("create OMP integration");
    fs::create_dir(&caller_cwd).expect("create caller cwd");
    fs::write(omp_plugin.join("package.json"), "{}").expect("write OMP package");
    let omp_plugin = fs::canonicalize(omp_plugin).expect("canonical OMP plugin");
    for name in ["codex", "claude", "omp"] {
        write_host_fixture(&bin.join(name), &omp_plugin);
    }
    let missing_credential_log = harness.root.path().join("missing-credential");
    let missing_credential = run_host_fixture(
        &harness,
        &[
            OsString::from("codex-cli"),
            OsString::from("local:stock-codex"),
        ],
        &bin,
        &integrations,
        &caller_cwd,
        &missing_credential_log,
        99,
    );
    assert!(!missing_credential.status.success());
    assert!(stderr(&missing_credential).contains("configuration_required"));
    assert!(!missing_credential_log.with_extension("args").exists());

    let codex_agent = "local:stock-codex";
    let codex_credential = agent_credential_file(
        &harness,
        codex_agent,
        AgentSide::Codex,
        AgentClient::CodexCli,
    );
    let wrong_subject = run_host_fixture(
        &harness,
        &[
            OsString::from("--credential"),
            codex_credential.clone().into_os_string(),
            OsString::from("codex-cli"),
            OsString::from("local:not-stock-codex"),
        ],
        &bin,
        &integrations,
        &caller_cwd,
        &harness.root.path().join("wrong-subject"),
        99,
    );
    assert!(!wrong_subject.status.success());
    assert!(stderr(&wrong_subject).contains("credential_claims_mismatch"));
    let wrong_claims_credential = agent_credential_file(
        &harness,
        "local:wrong-claims",
        AgentSide::Generic,
        AgentClient::Omp,
    );
    let wrong_claims = run_host_fixture(
        &harness,
        &[
            OsString::from("--credential"),
            wrong_claims_credential.into_os_string(),
            OsString::from("codex-cli"),
            OsString::from("local:wrong-claims"),
        ],
        &bin,
        &integrations,
        &caller_cwd,
        &harness.root.path().join("wrong-claims"),
        99,
    );
    assert!(!wrong_claims.status.success());
    assert!(stderr(&wrong_claims).contains("credential_claims_mismatch"));
    let opaque = OsString::from_vec(vec![b'o', 0xff, b'x']);
    let codex_log = harness.root.path().join("codex-stock");
    let codex = run_host_fixture(
        &harness,
        &[
            OsString::from("--credential"),
            codex_credential.clone().into_os_string(),
            OsString::from("codex-cli"),
            OsString::from(codex_agent),
            OsString::from("--workspace"),
            OsString::from("host-room"),
            OsString::from("--"),
            OsString::from("resume"),
            opaque.clone(),
        ],
        &bin,
        &integrations,
        &caller_cwd,
        &codex_log,
        23,
    );
    assert_eq!(codex.status.code(), Some(23));
    let codex_arguments = recorded_arguments(&codex_log);
    assert_eq!(codex_arguments[0], b"-C");
    assert_eq!(codex_arguments[1], caller_cwd.as_os_str().as_bytes());
    assert_eq!(codex_arguments[codex_arguments.len() - 2], b"resume");
    assert_eq!(
        codex_arguments.last().map(Vec::as_slice),
        Some(opaque.as_os_str().as_bytes())
    );
    assert!(
        codex_arguments
            .iter()
            .all(|argument| !argument.windows(13).any(|bytes| bytes == b"AGENT_ROUTER_"))
    );
    assert_eq!(
        fs::read_to_string(codex_log.with_extension("cwd")).expect("codex cwd"),
        format!("{}\n", caller_cwd.display())
    );
    assert_eq!(
        fs::read_to_string(codex_log.with_extension("workspace")).expect("Codex initial workspace"),
        "host-room"
    );

    let claude_agent = "local:stock-claude";
    let claude_credential = agent_credential_file(
        &harness,
        claude_agent,
        AgentSide::Claude,
        AgentClient::ClaudeCode,
    );
    let claude_log = harness.root.path().join("claude-stock");
    let claude = run_host_fixture(
        &harness,
        &[
            OsString::from("--credential"),
            claude_credential.clone().into_os_string(),
            OsString::from("claude"),
            OsString::from(claude_agent),
            OsString::from("--workspace"),
            OsString::from("host-room"),
            OsString::from("--auto"),
            OsString::from("--resume"),
            OsString::from("session-1"),
        ],
        &bin,
        &integrations,
        &caller_cwd,
        &claude_log,
        24,
    );
    assert_eq!(claude.status.code(), Some(24));
    assert_eq!(
        recorded_arguments(&claude_log),
        [
            b"--permission-mode".to_vec(),
            b"auto".to_vec(),
            b"--resume".to_vec(),
            b"session-1".to_vec(),
            b"--dangerously-load-development-channels".to_vec(),
            b"server:agent-session-router-channel".to_vec(),
        ]
    );
    assert_eq!(
        fs::read_to_string(claude_log.with_extension("credential"))
            .expect("Claude credential selection"),
        claude_credential.to_string_lossy()
    );
    assert_eq!(
        fs::read_to_string(claude_log.with_extension("workspace"))
            .expect("Claude initial workspace"),
        "host-room"
    );

    let omp_agent = "local:stock-omp";
    let omp_credential =
        agent_credential_file(&harness, omp_agent, AgentSide::Generic, AgentClient::Omp);
    let omp_log = harness.root.path().join("omp-stock");
    let omp = run_host_fixture(
        &harness,
        &[
            OsString::from("--credential"),
            omp_credential.clone().into_os_string(),
            OsString::from("omp"),
            OsString::from(omp_agent),
            OsString::from("--workspace"),
            OsString::from("host-room"),
            OsString::from("--"),
            OsString::from("--resume"),
            OsString::from("turn-1"),
        ],
        &bin,
        &integrations,
        &caller_cwd,
        &omp_log,
        25,
    );
    assert_eq!(omp.status.code(), Some(25), "{}", stderr(&omp));
    let omp_arguments = recorded_arguments(&omp_log);
    assert_eq!(omp_arguments, [b"--resume".to_vec(), b"turn-1".to_vec()]);
    assert!(
        omp_arguments
            .iter()
            .all(|argument| argument != b"-e" && argument != b"--extension")
    );
    let asr_executable =
        fs::read_to_string(omp_log.with_extension("executable")).expect("OMP ASR executable");
    assert!(Path::new(&asr_executable).is_absolute());
    assert_eq!(
        fs::read_to_string(omp_log.with_extension("credential")).expect("OMP credential"),
        omp_credential.to_string_lossy()
    );
    assert_eq!(
        fs::read_to_string(omp_log.with_extension("workspace")).expect("OMP workspace"),
        "host-room"
    );

    let setup_claude_log = harness.root.path().join("setup-claude");
    let setup_claude = run_host_fixture(
        &harness,
        &[OsString::from("setup-claude")],
        &bin,
        &integrations,
        &caller_cwd,
        &setup_claude_log,
        26,
    );
    assert_eq!(setup_claude.status.code(), Some(26));
    let setup_claude_arguments = recorded_arguments(&setup_claude_log);
    assert_eq!(
        &setup_claude_arguments[..7],
        [
            b"mcp".as_slice(),
            b"add".as_slice(),
            b"--transport".as_slice(),
            b"stdio".as_slice(),
            b"--scope".as_slice(),
            b"local".as_slice(),
            b"agent-session-router-channel".as_slice(),
        ]
    );
    let separator = setup_claude_arguments
        .iter()
        .position(|argument| argument == b"--")
        .expect("setup Claude separator");
    assert!(Path::new(OsStr::from_bytes(&setup_claude_arguments[separator + 1])).is_absolute());
    assert_eq!(setup_claude_arguments[separator + 2], b"mcp");
    assert_eq!(
        setup_claude_arguments[setup_claude_arguments.len() - 2..],
        [b"mcp".to_vec(), b"claude-channel".to_vec()]
    );

    let setup_omp_already_log = harness.root.path().join("setup-omp-already");
    let setup_omp_already = harness
        .process()
        .arg("setup-omp")
        .current_dir(&caller_cwd)
        .env("PATH", &bin)
        .env("ASR_INTEGRATIONS_DIR", &integrations)
        .env("ASR_TEST_LOG", &setup_omp_already_log)
        .env("ASR_TEST_EXIT", "99")
        .output()
        .expect("inspect existing OMP setup");
    assert_success(&setup_omp_already);
    assert!(!setup_omp_already_log.with_extension("args").exists());

    let setup_omp_disabled = harness
        .process()
        .arg("setup-omp")
        .current_dir(&caller_cwd)
        .env("PATH", &bin)
        .env("ASR_INTEGRATIONS_DIR", &integrations)
        .env("ASR_TEST_OMP_STATE", "disabled")
        .output()
        .expect("inspect disabled OMP setup");
    assert_eq!(setup_omp_disabled.status.code(), Some(1));
    assert!(stderr(&setup_omp_disabled).contains(
        "OMP integration is disabled; run `omp plugin enable \
         @agent-session-router/omp-integration`"
    ));

    let setup_omp_conflict = harness
        .process()
        .arg("setup-omp")
        .current_dir(&caller_cwd)
        .env("PATH", &bin)
        .env("ASR_INTEGRATIONS_DIR", &integrations)
        .env("ASR_TEST_OMP_STATE", "conflict")
        .output()
        .expect("inspect conflicting OMP setup");
    assert_eq!(setup_omp_conflict.status.code(), Some(1));
    assert!(
        stderr(&setup_omp_conflict)
            .contains("a different agent-session-router OMP integration is already installed")
    );

    let setup_omp_log = harness.root.path().join("setup-omp");
    let setup_omp = harness
        .process()
        .arg("setup-omp")
        .current_dir(&caller_cwd)
        .env("PATH", &bin)
        .env("ASR_INTEGRATIONS_DIR", &integrations)
        .env("ASR_TEST_LOG", &setup_omp_log)
        .env("ASR_TEST_EXIT", "27")
        .env("ASR_TEST_OMP_STATE", "missing")
        .output()
        .expect("setup OMP fixture command");
    assert_eq!(setup_omp.status.code(), Some(27), "{}", stderr(&setup_omp));
    assert_eq!(
        recorded_arguments(&setup_omp_log),
        [
            b"plugin".to_vec(),
            b"link".to_vec(),
            omp_plugin.as_os_str().as_bytes().to_vec(),
        ]
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn native_codex_gateway_and_interactive_host_use_managed_api_and_close_on_sigint() {
    let harness = NativeHarness::new();
    assert_success(&harness.command(["router", "start", "--background"]));
    assert_success(&harness.command(profile_arguments(&[
        "workspace",
        "create",
        "codex-host-room",
    ])));

    let credential_directory = harness.root.path().join("codex-host-credentials");
    fs::create_dir(&credential_directory).expect("create Codex host credential directory");
    fs::set_permissions(&credential_directory, fs::Permissions::from_mode(0o700))
        .expect("protect Codex host credential directory");
    let credential_path = credential_directory.join("codex-host.json");
    let issue = harness.command([
        OsStr::new("--profile"),
        OsStr::new("this-device"),
        OsStr::new("credential"),
        OsStr::new("issue"),
        OsStr::new("--agent"),
        OsStr::new("local:codex-host"),
        OsStr::new("--side"),
        OsStr::new("codex"),
        OsStr::new("--client"),
        OsStr::new("codex-app-server"),
        OsStr::new("--workspace"),
        OsStr::new("codex-host-room"),
        OsStr::new("--output"),
        credential_path.as_os_str(),
    ]);
    assert_success(&issue);
    let credential = read_credential(&credential_path).expect("read Codex host credential");

    let bin = harness.root.path().join("codex-host-bin");
    let caller_cwd = harness.root.path().join("codex-host-cwd");
    fs::create_dir(&bin).expect("create Codex host bin");
    fs::create_dir(&caller_cwd).expect("create Codex host cwd");
    let record = harness.root.path().join("codex-gateway.jsonl");
    write_codex_app_server_fixture(&bin, &record);
    let mut path_entries = vec![bin.clone()];
    path_entries.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let fixture_path = std::env::join_paths(path_entries).expect("Codex fixture PATH");

    let mut gateway = harness
        .process()
        .args([
            OsStr::new("--profile"),
            OsStr::new("this-device"),
            OsStr::new("--credential"),
            credential_path.as_os_str(),
            OsStr::new("gateway"),
            OsStr::new("codex"),
            OsStr::new("local:codex-host"),
            OsStr::new("--workspace"),
            OsStr::new("codex-host-room"),
        ])
        .current_dir(&caller_cwd)
        .env("PATH", &fixture_path)
        .env("ROUTER_TOKEN", "must-not-reach-provider")
        .env("AGENT_ROUTER_DELEGATION_TOKEN", "must-not-reach-provider")
        .env("AGENT_ROUTER_TOKEN", "must-not-reach-provider")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn Codex gateway");

    let deadline = Instant::now() + Duration::from_secs(5);
    while !record.exists() && Instant::now() < deadline {
        assert!(gateway.try_wait().expect("poll Codex gateway").is_none());
        thread::sleep(Duration::from_millis(20));
    }
    assert!(record.exists(), "Codex app-server did not initialize");
    let initialized: Value = serde_json::from_str(
        fs::read_to_string(&record)
            .expect("read Codex gateway record")
            .lines()
            .next()
            .expect("Codex initialize record"),
    )
    .expect("Codex initialize JSON");
    assert_eq!(
        fs::canonicalize(
            initialized["cwd"]
                .as_str()
                .expect("Codex app-server working directory")
        )
        .expect("canonical Codex app-server working directory"),
        fs::canonicalize(&caller_cwd).expect("canonical caller cwd")
    );
    assert!(
        initialized["argv"]
            .as_array()
            .expect("Codex app-server arguments")
            .iter()
            .any(|argument| argument == "app-server")
    );
    assert_eq!(initialized["sensitiveEnvironment"], serde_json::json!({}));
    let initialized_text = initialized.to_string();
    assert!(!initialized_text.contains("must-not-reach-provider"));
    assert!(!initialized_text.contains(credential.token().expose()));

    let member_text = {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let members = harness.command(profile_arguments(&[
                "workspace",
                "members",
                "codex-host-room",
                "--json",
            ]));
            assert_success(&members);
            let output = stdout(&members);
            if !output.trim().is_empty() {
                break output;
            }
            if gateway.try_wait().expect("poll Codex gateway").is_some() {
                let output = gateway
                    .wait_with_output()
                    .expect("early Codex gateway exit");
                panic!(
                    "Codex gateway exited before registration: status={:?} stdout={} stderr={}",
                    output.status.code(),
                    stdout(&output),
                    stderr(&output)
                );
            }
            assert!(Instant::now() < deadline, "Codex gateway did not register");
            thread::sleep(Duration::from_millis(20));
        }
    };
    let member: Value =
        serde_json::from_str(member_text.trim()).expect("Codex gateway member JSON");
    assert_eq!(member["agentId"], "local:codex-host");
    assert_eq!(member["side"], "codex");
    assert_eq!(member["client"], "codex-app-server");
    assert_eq!(member["deliveryMode"], "push");
    assert_eq!(member["ready"], true);

    let signal = Command::new("kill")
        .args(["-INT", &gateway.id().to_string()])
        .status()
        .expect("send Codex gateway SIGINT");
    assert!(signal.success());
    let gateway_output = gateway.wait_with_output().expect("Codex gateway exit");
    assert_eq!(
        gateway_output.status.code(),
        Some(130),
        "{}",
        stderr(&gateway_output)
    );
    assert!(stderr(&gateway_output).contains("interrupted"));
    let provider_pid = initialized["pid"]
        .as_u64()
        .expect("Codex app-server process id");
    assert!(
        !Command::new("kill")
            .args(["-0", &provider_pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("probe Codex app-server process")
            .success(),
        "Codex app-server subprocess survived gateway shutdown"
    );
    let members_after_close = harness.command(profile_arguments(&[
        "workspace",
        "members",
        "codex-host-room",
        "--json",
    ]));
    assert_success(&members_after_close);
    assert!(stdout(&members_after_close).trim().is_empty());

    let interactive_record = harness.root.path().join("codex-interactive.jsonl");
    write_codex_app_server_fixture(&bin, &interactive_record);
    let mut interactive = harness
        .process()
        .args([
            OsStr::new("--profile"),
            OsStr::new("this-device"),
            OsStr::new("--credential"),
            credential_path.as_os_str(),
            OsStr::new("codex"),
            OsStr::new("local:codex-host"),
            OsStr::new("--workspace"),
            OsStr::new("codex-host-room"),
        ])
        .current_dir(&caller_cwd)
        .env("PATH", fixture_path)
        .env("ROUTER_TOKEN", "must-not-reach-provider")
        .env("AGENT_ROUTER_DELEGATION_TOKEN", "must-not-reach-provider")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn interactive Codex host");
    let mut interactive_stdin = interactive.stdin.take().expect("interactive Codex stdin");
    let interactive_stdout = interactive.stdout.take().expect("interactive Codex stdout");
    let (response_tx, response_rx) = mpsc::channel();
    let response_reader = thread::spawn(move || {
        let mut reader = BufReader::new(interactive_stdout);
        let mut line = String::new();
        let result = reader.read_line(&mut line);
        let _ = response_tx.send((result, line));
    });
    interactive_stdin
        .write_all(b"hello from cli\n")
        .expect("write interactive Codex prompt");
    interactive_stdin
        .flush()
        .expect("flush interactive Codex prompt");
    let (read_result, response) = match response_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(response) => response,
        Err(error) => {
            let _ = interactive.kill();
            let _ = response_reader.join();
            panic!("interactive Codex response timed out: {error}");
        }
    };
    read_result.expect("read interactive Codex response");
    assert!(response.contains("answer:hello from cli"), "{response}");
    interactive_stdin
        .write_all(b"quit\n")
        .expect("write interactive Codex quit");
    drop(interactive_stdin);
    response_reader.join().expect("join Codex response reader");
    let interactive_output = interactive
        .wait_with_output()
        .expect("interactive Codex exit");
    assert_success(&interactive_output);
    let interactive_records =
        fs::read_to_string(interactive_record).expect("read interactive Codex record");
    assert!(interactive_records.contains(r#""event":"turn","text":"hello from cli""#));
    assert!(!interactive_records.contains("must-not-reach-provider"));
    assert!(!interactive_records.contains(credential.token().expose()));
}

#[tokio::test(flavor = "multi_thread")]
async fn internal_mcp_child_roundtrips_with_official_client() {
    let harness = NativeHarness::new();
    let start = harness.command(["router", "start", "--background"]);
    assert_success(&start);
    let create = harness.command(profile_arguments(&[
        "workspace",
        "create",
        "mcp-child-room",
    ]));
    assert_success(&create);

    let credential_directory = harness.root.path().join("mcp-credentials");
    fs::create_dir(&credential_directory).expect("create MCP credential directory");
    fs::set_permissions(&credential_directory, fs::Permissions::from_mode(0o700))
        .expect("protect MCP credential directory");
    let credential = credential_directory.join("codex-cli.json");
    let issue = harness.command([
        OsStr::new("--profile"),
        OsStr::new("this-device"),
        OsStr::new("credential"),
        OsStr::new("issue"),
        OsStr::new("--agent"),
        OsStr::new("local:mcp-child"),
        OsStr::new("--side"),
        OsStr::new("codex"),
        OsStr::new("--client"),
        OsStr::new("codex-cli"),
        OsStr::new("--workspace"),
        OsStr::new("mcp-child-room"),
        OsStr::new("--output"),
        credential.as_os_str(),
    ]);
    assert_success(&issue);

    let mut child = tokio::process::Command::new(&harness.executable);
    child
        .args([
            OsStr::new("--profile"),
            OsStr::new("this-device"),
            OsStr::new("--credential"),
            credential.as_os_str(),
            OsStr::new("mcp"),
            OsStr::new("codex-cli"),
        ])
        .env("ASR_DATA_DIR", harness.data_dir())
        .env("ASR_CONFIG_PATH", harness.root.path().join("config.json"))
        .env_remove("SSL_CERT_FILE")
        .env_remove("SSL_CERT_DIR")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = child.spawn().expect("spawn MCP child");
    let child_stdin = child.stdin.take().expect("MCP child stdin");
    let child_stdout = child.stdout.take().expect("MCP child stdout");
    let client =
        ().serve((child_stdout, child_stdin))
            .await
            .expect("official MCP client initialization");

    let tools = client.list_tools(None).await.expect("list MCP child tools");
    assert!(tools.tools.iter().any(|tool| tool.name == "workspace_list"));
    let arguments = serde_json::json!({"after": null, "limit": 20})
        .as_object()
        .expect("workspace list arguments")
        .clone();
    let workspaces = client
        .call_tool(CallToolRequestParams::new("workspace_list").with_arguments(arguments))
        .await
        .expect("workspace list through MCP child");
    assert_eq!(
        workspaces.structured_content.expect("workspace content")["workspaces"][0]["name"],
        "mcp-child-room"
    );

    client.cancel().await.expect("close official MCP client");
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("MCP child exit timeout")
        .expect("wait MCP child");
    assert!(status.success());
    let stop = harness.command(["router", "stop"]);
    assert_success(&stop);
}
struct TlsStopGuard<'a> {
    harness: &'a NativeHarness,
    fixture: &'a CertificateFixture,
    port: u16,

    runtime_path: PathBuf,
    original_runtime: Vec<u8>,
}

#[tokio::test(flavor = "multi_thread")]
async fn smoke_command_requires_a_real_targeted_loopback_reply() {
    let harness = NativeHarness::new();
    assert_success(&harness.command(["router", "start", "--background"]));
    assert_success(&harness.command(profile_arguments(&["workspace", "create", "smoke-room"])));

    let credential_directory = harness.root.path().join("smoke-credentials");
    fs::create_dir(&credential_directory).expect("create smoke credential directory");
    fs::set_permissions(&credential_directory, fs::Permissions::from_mode(0o700))
        .expect("protect smoke credential directory");
    let credential_path = credential_directory.join("worker.json");
    let issue = harness.command([
        OsStr::new("--profile"),
        OsStr::new("this-device"),
        OsStr::new("credential"),
        OsStr::new("issue"),
        OsStr::new("--agent"),
        OsStr::new("local:smoke-worker"),
        OsStr::new("--side"),
        OsStr::new("generic"),
        OsStr::new("--client"),
        OsStr::new("omp"),
        OsStr::new("--workspace"),
        OsStr::new("smoke-room"),
        OsStr::new("--output"),
        credential_path.as_os_str(),
    ]);
    assert_success(&issue);

    let runtime: RuntimeRecord = serde_json::from_slice(
        &fs::read(harness.data_dir().join("router-runtime.json")).expect("runtime record"),
    )
    .expect("runtime record JSON");
    let credential = read_credential(&credential_path).expect("worker credential");
    let (worker, mut events) = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&runtime.control_url).expect("router URL"),
        role: ClientRole::Primary {
            agent: AgentRegistration {
                agent_id: "local:smoke-worker".to_owned(),
                side: AgentSide::Generic,
                client: AgentClient::Omp,
                activity: Some("smoke".to_owned()),
                delivery_mode: DeliveryMode::Push,
            },
            credential,
            delegation_token: None,
        },
        ca_file: None,
    })
    .await
    .expect("connect smoke worker");
    worker
        .workspace_join(WorkspaceName::parse("smoke-room").expect("workspace"))
        .await
        .expect("join smoke worker");
    let responder = tokio::spawn({
        let worker = worker.clone();
        async move {
            loop {
                let event = events.recv().await.expect("smoke worker event");
                if let ClientEvent::Delivery {
                    request_id,
                    from,
                    content,
                    ..
                } = event.event
                {
                    assert_eq!(from, "operator:admin");
                    assert_eq!(content, "smoke request");
                    worker
                        .reply(request_id, true, Some("smoke response".to_owned()), None)
                        .await
                        .expect("reply to smoke");
                    break;
                }
            }
        }
    });

    let smoke = harness.command(profile_arguments(&[
        "smoke",
        "--workspace",
        "smoke-room",
        "--target",
        "local:smoke-worker",
        "--timeout-ms",
        "3000",
    ]));
    assert_success(&smoke);
    assert_eq!(stdout(&smoke).trim(), "provider smoke test passed");
    responder.await.expect("smoke responder");
    worker.close().await.expect("close smoke worker");
    assert_success(&harness.command(["router", "stop"]));
}

impl Drop for TlsStopGuard<'_> {
    fn drop(&mut self) {
        if self.runtime_path.exists() {
            let _ = fs::write(&self.runtime_path, &self.original_runtime);
            let _ = fs::set_permissions(&self.runtime_path, fs::Permissions::from_mode(0o600));
        }
        let _ = trusted_control_command(self.harness, self.fixture, self.port, ["router", "stop"]);
    }
}

#[test]
fn native_cli_rejects_partial_or_insecure_tls_before_launch() {
    let harness = NativeHarness::new();
    let fixture = certificate_fixture(harness.root.path());
    let port = reserve_loopback_port();

    let partial = harness
        .process()
        .args(["router", "start", "--background"])
        .env("ASR_BIND", format!("127.0.0.1:{port}"))
        .env("ROUTER_TLS_CERT", &fixture.certificate)
        .output()
        .expect("partial TLS command");
    assert_eq!(partial.status.code(), Some(1));
    assert!(stderr(&partial).contains("tls_configuration_invalid"));

    fs::set_permissions(&fixture.private_key, fs::Permissions::from_mode(0o644))
        .expect("make private key insecure");
    let insecure = tls_command(
        &harness,
        &fixture,
        port,
        ["router", "start", "--background"],
    );
    assert_eq!(insecure.status.code(), Some(1));
    assert!(stderr(&insecure).contains("tls_configuration_invalid"));

    fs::set_permissions(&fixture.private_key, fs::Permissions::from_mode(0o600))
        .expect("restore private key");
    let linked_key = harness.root.path().join("linked-server-key.pem");
    symlink(&fixture.private_key, &linked_key).expect("link private key");
    let linked = tls_process(&harness, &fixture, port)
        .args(["router", "start", "--background"])
        .env("ROUTER_TLS_KEY", linked_key)
        .output()
        .expect("linked TLS key command");
    assert_eq!(linked.status.code(), Some(1));
    assert!(stderr(&linked).contains("tls_configuration_invalid"));

    assert!(!harness.data_dir().join("router-runtime.json").exists());
    assert!(TcpStream::connect(format!("127.0.0.1:{port}")).is_err());
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread")]
async fn native_cli_tls_process_is_trusted_reused_authenticated_and_fail_closed() {
    let harness = NativeHarness::new();
    let fixture = certificate_fixture(harness.root.path());
    let port = reserve_loopback_port();
    let runtime_path = harness.data_dir().join("router-runtime.json");

    let started = tls_command(
        &harness,
        &fixture,
        port,
        ["router", "start", "--background"],
    );
    assert_success(&started);
    assert!(stdout(&started).contains(&format!("router started at wss://localhost:{port}/ws")));

    let original_runtime = fs::read(&runtime_path).expect("published runtime record");
    let record: RuntimeRecord =
        serde_json::from_slice(&original_runtime).expect("runtime record JSON");
    assert_eq!(record.control_url, format!("wss://localhost:{port}/ws"));
    let _cleanup = TlsStopGuard {
        harness: &harness,
        fixture: &fixture,
        port,
        runtime_path: runtime_path.clone(),
        original_runtime: original_runtime.clone(),
    };

    let create = tls_command(
        &harness,
        &fixture,
        port,
        profile_arguments(&["workspace", "create", "tls-room"]),
    );
    assert_success(&create);
    let list = tls_command(
        &harness,
        &fixture,
        port,
        profile_arguments(&["workspace", "list", "--json"]),
    );
    assert_success(&list);
    let listed: Value = serde_json::from_str(stdout(&list).trim()).expect("workspace list JSON");
    assert_eq!(listed["name"], "tls-room");

    let shared_tls = load_client_config(Some(&fixture.ca)).expect("load temporary CA");
    let http = reqwest::Client::builder()
        .tls_backend_preconfigured((*shared_tls).clone())
        .no_proxy()
        .build()
        .expect("HTTPS client");
    let health = http
        .get(format!("https://localhost:{port}/healthz"))
        .send()
        .await
        .expect("trusted HTTPS health")
        .error_for_status()
        .expect("successful HTTPS health")
        .json::<HealthMarker>()
        .await
        .expect("health marker");
    assert_eq!(health.service, "agent-session-router");
    assert_eq!(health.protocol_version, 2);
    assert_eq!(health.status, "ok");
    assert_eq!(health.instance_id, record.instance_id);

    let admin = read_credential(&harness.data_dir().join("credentials/admin.json"))
        .expect("admin credential");
    let wrong_hostname = RouterClient::connect(ClientConfig {
        router_url: Url::parse(&format!("wss://127.0.0.1:{port}/ws")).expect("wrong-host WSS URL"),
        role: ClientRole::Operator {
            credential: admin.clone(),
        },
        ca_file: Some(fixture.ca.clone()),
    })
    .await;
    assert!(matches!(wrong_hostname, Err(ClientError::Transport)));
    assert!(
        http.get(format!("https://127.0.0.1:{port}/healthz"))
            .send()
            .await
            .is_err()
    );

    let reused = trusted_control_command(
        &harness,
        &fixture,
        port,
        ["router", "start", "--background"],
    );
    assert_success(&reused);
    assert!(stdout(&reused).contains(&format!(
        "router already running at wss://localhost:{port}/ws"
    )));

    for action in [
        vec!["router", "start", "--background", "--share", "auto"],
        vec!["router", "stop"],
    ] {
        let rejected = harness
            .process()
            .args(action)
            .env("ASR_BIND", format!("127.0.0.1:{port}"))
            .env("ASR_CA_FILE", &fixture.ca)
            .env("ROUTER_TLS_CERT", &fixture.certificate)
            .output()
            .expect("partial TLS command against live router");
        assert_eq!(rejected.status.code(), Some(1));
        assert!(stderr(&rejected).contains("tls_configuration_invalid"));
        assert!(runtime_path.exists());
        assert_router_still_listening(port);
    }

    fs::set_permissions(&fixture.private_key, fs::Permissions::from_mode(0o644))
        .expect("make live private key insecure");
    for action in [
        vec!["router", "start", "--background", "--share", "auto"],
        vec!["router", "stop"],
    ] {
        let rejected = tls_process(&harness, &fixture, port)
            .args(action)
            .output()
            .expect("insecure key command against live router");
        assert_eq!(rejected.status.code(), Some(1));
        assert!(stderr(&rejected).contains("tls_configuration_invalid"));
        assert!(runtime_path.exists());
        assert_router_still_listening(port);
    }
    fs::set_permissions(&fixture.private_key, fs::Permissions::from_mode(0o600))
        .expect("restore live private key");

    let linked_key = harness.root.path().join("live-linked-server-key.pem");
    symlink(&fixture.private_key, &linked_key).expect("link live private key");
    for action in [
        vec!["router", "start", "--background", "--share", "auto"],
        vec!["router", "stop"],
    ] {
        let rejected = tls_process(&harness, &fixture, port)
            .args(action)
            .env("ROUTER_TLS_KEY", &linked_key)
            .output()
            .expect("linked key command against live router");
        assert_eq!(rejected.status.code(), Some(1));
        assert!(stderr(&rejected).contains("tls_configuration_invalid"));
        assert!(runtime_path.exists());
        assert_router_still_listening(port);
    }

    let wrong_ca_file = harness.root.path().join("wrong-ca.pem");
    let wrong_ca = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("wrong CA certificate");
    fs::write(&wrong_ca_file, wrong_ca.cert.pem()).expect("write wrong CA");
    for action in [
        vec!["router", "start", "--background", "--share", "auto"],
        vec!["router", "stop"],
    ] {
        let rejected = trusted_control_process(&harness, &fixture, port)
            .args(action)
            .env("ASR_CA_FILE", &wrong_ca_file)
            .output()
            .expect("wrong CA command");
        assert_eq!(rejected.status.code(), Some(1));
        assert!(stderr(&rejected).contains("health_check_failed"));
        assert!(runtime_path.exists());
        assert_router_still_listening(port);
    }

    for variable in ["SSL_CERT_FILE", "SSL_CERT_DIR"] {
        let rejected = trusted_control_process(&harness, &fixture, port)
            .args(["router", "start", "--background", "--share", "auto"])
            .env(variable, &fixture.ca)
            .output()
            .expect("forbidden trust override command");
        assert_eq!(rejected.status.code(), Some(1), "{variable}");
        assert!(
            stderr(&rejected).contains("health_check_failed"),
            "{variable}"
        );
        assert!(runtime_path.exists());
        assert_router_still_listening(port);
    }

    let node_extra_only = harness
        .process()
        .args(["router", "start", "--background", "--share", "auto"])
        .env("ASR_BIND", format!("127.0.0.1:{port}"))
        .env("NODE_EXTRA_CA_CERTS", &fixture.ca)
        .output()
        .expect("NODE_EXTRA_CA_CERTS command");
    assert_eq!(node_extra_only.status.code(), Some(1));
    assert!(stderr(&node_extra_only).contains("health_check_failed"));
    assert!(runtime_path.exists());
    assert_router_still_listening(port);

    let wrong_host_url = format!("wss://127.0.0.1:{port}/ws");
    let mut wrong_host_record: Value =
        serde_json::from_slice(&original_runtime).expect("runtime record value");
    wrong_host_record["controlUrl"] = Value::String(wrong_host_url.clone());
    fs::write(
        &runtime_path,
        serde_json::to_vec(&wrong_host_record).expect("wrong-host runtime JSON"),
    )
    .expect("write wrong-host runtime");
    for action in [
        vec!["router", "start", "--background", "--share", "auto"],
        vec!["router", "stop"],
    ] {
        let rejected = trusted_control_command(&harness, &fixture, port, action);
        assert_eq!(rejected.status.code(), Some(1));
        assert!(stderr(&rejected).contains("health_check_failed"));
        let retained: RuntimeRecord =
            serde_json::from_slice(&fs::read(&runtime_path).expect("retained runtime"))
                .expect("retained runtime JSON");
        assert_eq!(retained.control_url, wrong_host_url);
        assert_router_still_listening(port);
    }
    fs::write(&runtime_path, &original_runtime).expect("restore runtime URL");
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o600))
        .expect("restore runtime permissions");

    let mismatched_instance = Uuid::new_v4();
    let mut tampered: Value =
        serde_json::from_slice(&original_runtime).expect("runtime record value");
    tampered["instanceId"] = Value::String(mismatched_instance.to_string());
    fs::write(
        &runtime_path,
        serde_json::to_vec(&tampered).expect("tampered runtime JSON"),
    )
    .expect("tamper runtime marker");
    for action in [
        vec!["router", "start", "--background", "--share", "auto"],
        vec!["router", "stop"],
    ] {
        let rejected = trusted_control_command(&harness, &fixture, port, action);
        assert_eq!(rejected.status.code(), Some(1));
        assert!(stderr(&rejected).contains("health_check_failed"));
        let retained: RuntimeRecord =
            serde_json::from_slice(&fs::read(&runtime_path).expect("retained runtime"))
                .expect("retained runtime JSON");
        assert_eq!(retained.instance_id, mismatched_instance);
        assert_router_still_listening(port);
    }

    fs::write(&runtime_path, &original_runtime).expect("restore runtime marker");
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o600))
        .expect("restore runtime permissions");
    let stopped = trusted_control_command(&harness, &fixture, port, ["router", "stop"]);
    assert_success(&stopped);
    assert!(stdout(&stopped).contains(&format!("router stopped at wss://localhost:{port}/ws")));
    assert!(!runtime_path.exists());

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(TcpStream::connect(format!("127.0.0.1:{port}")).is_err());
}

#[test]
fn dry_run_and_noninteractive_usage_have_stable_exit_contracts() {
    let harness = NativeHarness::new();
    let dry_data = harness.root.path().join("dry-data");
    let output = Command::new(&harness.executable)
        .args([
            "--dry-run",
            "profile",
            "add",
            "office",
            "wss://router.example.test/ws",
        ])
        .env("ASR_DATA_DIR", &dry_data)
        .env(
            "ASR_CONFIG_PATH",
            harness.root.path().join("dry-config.json"),
        )
        .output()
        .expect("dry-run CLI invocation");
    assert_success(&output);
    let plan: Value = serde_json::from_str(stdout(&output).trim()).expect("dry-run JSON");
    assert_eq!(plan["dryRun"], true);
    assert!(!dry_data.exists());

    let usage = harness.command(std::iter::empty::<&str>());
    assert_eq!(usage.status.code(), Some(2));
    assert!(stderr(&usage).contains("command_required"));
}

#[test]
fn native_onboarding_dry_run_never_reads_stdin_or_opens_configuration() {
    let harness = NativeHarness::new();
    let forbidden = harness.root.path().join("forbidden-config");
    symlink("/definitely/not/an/asr/config", &forbidden).unwrap();
    let mut child = harness
        .process()
        .args([
            "--dry-run",
            "onboarding",
            "install",
            "--provider",
            "codex-cli",
            "--stdin",
        ])
        .env("ASR_CONFIG_PATH", forbidden)
        .env("PATH", harness.root.path().join("missing-bin"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Keep the pipe open and empty: any attempt to read stdin would block.
    let input = child.stdin.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("onboarding dry-run blocked on stdin");
        }
        thread::sleep(Duration::from_millis(10));
    }
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert_success(&output);
    let plan: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(plan["command"], "onboarding.install");
    assert!(!harness.root.path().join("onboarding").exists());

    let sentinel = "stdin-secret-must-not-appear";
    let invalid = harness.command_with_stdin(
        ["onboarding", "install", "--provider", "omp", "--stdin"],
        &format!("{{\"inviteToken\":\"{sentinel}\"}}"),
    );
    assert_eq!(invalid.status.code(), Some(2));
    assert!(stderr(&invalid).contains("invalid_ticket"));
    assert!(!stdout(&invalid).contains(sentinel));
    assert!(!stderr(&invalid).contains(sentinel));
}

fn onboarding_archive(
    harness: &NativeHarness,
) -> (
    agent_session_router::onboarding::BootstrapManifest,
    String,
    PathBuf,
) {
    use agent_session_router::onboarding::{BootstrapArtifact, BootstrapManifest, VERSION};
    use sha2::{Digest as _, Sha256};
    let binary = fs::read(&harness.executable).unwrap();
    let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::fast(),
    ));
    let mut append = |name: &str, contents: &[u8], mode| {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(mode);
        header.set_cksum();
        builder.append_data(&mut header, name, contents).unwrap();
    };
    append("bin/asr", &binary, 0o755);
    let omp_package = serde_json::json!({
        "name": "@agent-session-router/omp-integration", "version": env!("CARGO_PKG_VERSION"),
    })
    .to_string();
    for asset in [
        "omp/index.js",
        "omp/package.json",
        "claude-sdk/bridge.js",
        "claude-sdk/manifest.json",
        "claude-sdk/package.json",
        "claude-plugin/.claude-plugin/marketplace.json",
        "claude-plugin/plugins/asr/.claude-plugin/plugin.json",
        "claude-plugin/plugins/asr/skills/workspace/SKILL.md",
        "codex/skills/asr/SKILL.md",
        "omp/skills/asr/SKILL.md",
    ] {
        let contents = if asset.ends_with("SKILL.md") {
            "---\nname: asr\ndescription: Workspace operations\n---\nUse workspace_list and workspace_members.\n"
        } else if asset == "omp/package.json" {
            &omp_package
        } else if std::path::Path::new(asset)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        {
            "{}"
        } else {
            "export {};\n"
        };
        append(
            &format!("share/agent-session-router/integrations/{asset}"),
            contents.as_bytes(),
            0o644,
        );
    }
    let archive = builder.into_inner().unwrap().finish().unwrap();
    let target = agent_session_router::bootstrap::install::host_target().unwrap();
    let artifact = BootstrapArtifact {
        target: target.to_owned(),
        binary_file: format!("asr-{target}"),
        binary_sha256: format!("{:x}", Sha256::digest(&binary)),
        archive_file: format!("agent-session-router-{target}.tar.gz"),
        archive_sha256: format!("{:x}", Sha256::digest(&archive)),
        binary_bytes: binary.len() as u64,
        archive_bytes: archive.len() as u64,
    };
    let assets = harness.root.path().join("bootstrap");
    fs::create_dir(&assets).unwrap();
    fs::set_permissions(&assets, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(assets.join(&artifact.binary_file), binary).unwrap();
    fs::write(assets.join(&artifact.archive_file), archive).unwrap();
    let manifest = BootstrapManifest {
        version: VERSION,
        asr_version: env!("CARGO_PKG_VERSION").to_owned(),
        artifacts: vec![artifact],
    };
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(assets.join("bootstrap-manifest.json"), bytes).unwrap();
    (manifest, digest, assets)
}

fn write_onboarding_codex(path: &Path) {
    // This fixture persists and inspects the actual native adapter's registration.
    // Enrollment and all subsequent credential assertions use the real router.
    fs::write(path, r#"#!/bin/sh
printf '%s\n' "$*" >> "$HOME/provider-calls"
for arg in "$@"; do
  if [ "$arg" = --help ]; then
    if [ -f "$HOME/provider-unsupported" ]; then exit 2; fi
    printf '%s\n' 'Usage: codex mcp add get list NAME -- COMMAND [ARGS] --json --env --url'
    exit 0
  fi
done
if [ "$1" = mcp ] && [ "$2" = get ]; then
  if [ -f "$CODEX_HOME/asr-registry.json" ]; then
    cat "$CODEX_HOME/asr-registry.json"
    exit 0
  fi
  printf '%s\n' "Error: No MCP server named 'agent_session_router' found." >&2
  exit 1
fi
if [ "$1" = mcp ] && [ "$2" = list ]; then
  if [ -f "$CODEX_HOME/asr-registry.json" ]; then
    printf '['; cat "$CODEX_HOME/asr-registry.json"; printf ']'
  else printf '[]'; fi
  exit 0
fi
if [ "$1" = mcp ] && [ "$2" = add ]; then
  if [ -f "$HOME/provider-fail-add" ]; then
    printf '%s\n' 'provider-secret-error-must-not-appear' >&2
    exit 1
  fi
  [ "$3" = agent_session_router ] && [ "$4" = -- ] || exit 2
  shift 4
  printf '{"name":"agent_session_router","enabled":true,"transport":{"type":"stdio","command":"%s","args":["%s","%s","%s","%s"],"env":null,"env_vars":[],"cwd":null}}\n' "$1" "$2" "$3" "$4" "$5" > "$CODEX_HOME/asr-registry.json"
  exit 0
fi
exit 2
"#).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

fn onboarding_native_command(
    harness: &NativeHarness,
    bin: &Path,
    args: &[&str],
    input: Option<&[u8]>,
) -> Output {
    let mut process = harness.process();
    process
        .args(args)
        .env("PATH", format!("{}:/usr/bin:/bin", bin.display()));
    if let Some(input) = input {
        let mut child = process
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(input).unwrap();
        child.wait_with_output().unwrap()
    } else {
        process.output().unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn native_onboarding_preflight_resume_and_status_keep_one_identity_without_agent_connections()
{
    use agent_session_router::{
        onboarding::{OnboardingProvider, OnboardingRoute, OnboardingTicket, RouteKind, VERSION},
        router::{RouterConfig, RouterExposure, RouterRuntime},
        store::{RouterStore, now_millis},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let harness = NativeHarness::new();
    let (manifest, manifest_sha256, assets) = onboarding_archive(&harness);
    let mut store = RouterStore::open(&harness.data_dir()).unwrap();
    let invite = store
        .issue_onboarding_invite(
            &WorkspaceName::parse("install-room").unwrap(),
            true,
            Some(OnboardingProvider::CodexCli),
            now_millis().unwrap(),
        )
        .unwrap();
    store.close().unwrap();
    let runtime = RouterRuntime::start(RouterConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        data_dir: harness.data_dir(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: Some(assets.clone()),
    })
    .await
    .unwrap();
    let posts = Arc::new(AtomicUsize::new(0));
    let sockets = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let upstream = format!("http://{}", runtime.address);
    let handler = {
        let posts = posts.clone();
        let sockets = sockets.clone();
        move |request: axum::extract::Request| {
            let posts = posts.clone();
            let sockets = sockets.clone();
            let upstream = upstream.clone();
            async move {
                let path = request.uri().path().to_owned();
                if path == "/ws" {
                    sockets.fetch_add(1, Ordering::SeqCst);
                    return axum::http::Response::builder()
                        .status(503)
                        .body(axum::body::Body::empty())
                        .unwrap();
                }
                if path == "/onboarding/enroll" {
                    posts.fetch_add(1, Ordering::SeqCst);
                }
                let method = request.method().clone();
                let body = axum::body::to_bytes(request.into_body(), 128 * 1024)
                    .await
                    .unwrap();
                let response = reqwest::Client::new()
                    .request(method, format!("{upstream}{path}"))
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await
                    .unwrap();
                let status = response.status();
                let mime = response.headers().get("content-type").cloned();
                let bytes = response.bytes().await.unwrap();
                let mut builder = axum::http::Response::builder().status(status);
                if let Some(mime) = mime {
                    builder = builder.header("content-type", mime);
                }
                builder.body(axum::body::Body::from(bytes)).unwrap()
            }
        }
    };
    let proxy = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().fallback(handler))
            .await
            .unwrap();
    });
    let ticket = OnboardingTicket {
        version: VERSION,
        server_id: invite.server_id,
        invite_id: invite.invite_id,
        invite_token: invite.invite_token,
        expires_at: invite.expires_at,
        profile_name: "office".into(),
        workspace: invite.workspace,
        provider: invite.provider,
        routes: vec![OnboardingRoute {
            kind: RouteKind::Local,
            router_url: format!("ws://{address}/ws"),
            ca_pem: None,
        }],
        manifest_sha256,
        artifacts: manifest.artifacts,
    };
    let input = serde_json::to_vec(&ticket).unwrap();
    let bin = harness.root.path().join("provider-bin");
    fs::create_dir(&bin).unwrap();
    write_onboarding_codex(&bin.join("codex"));
    let install = [
        "onboarding",
        "install",
        "--provider",
        "codex-cli",
        "--stdin",
    ];
    let oversized = vec![b'x'; 128 * 1024 + 1];
    let too_large = onboarding_native_command(&harness, &bin, &install, Some(&oversized));
    assert_eq!(too_large.status.code(), Some(2));
    assert!(stderr(&too_large).contains("stdin_too_large"));
    let mismatch = onboarding_native_command(
        &harness,
        &bin,
        &["onboarding", "install", "--provider", "omp", "--stdin"],
        Some(&input),
    );
    assert_eq!(mismatch.status.code(), Some(2));
    assert!(stderr(&mismatch).contains("provider_mismatch"));
    let mut extra_field: Value = serde_json::from_slice(&input).unwrap();
    extra_field["credentialToken"] = Value::String("unknown-secret-field".into());
    let invalid = onboarding_native_command(
        &harness,
        &bin,
        &install,
        Some(&serde_json::to_vec(&extra_field).unwrap()),
    );
    assert_eq!(invalid.status.code(), Some(2));
    assert!(!stderr(&invalid).contains("unknown-secret-field"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    fs::write(harness.root.path().join("provider-unsupported"), "").unwrap();
    let refused = onboarding_native_command(&harness, &bin, &install, Some(&input));
    assert!(!refused.status.success());
    assert!(
        stderr(&refused).contains("host_upgrade_required"),
        "unexpected preflight error: {}",
        stderr(&refused)
    );
    assert_eq!(
        posts.load(Ordering::SeqCst),
        0,
        "feature failure must not consume invitation"
    );
    fs::remove_file(harness.root.path().join("provider-unsupported")).unwrap();
    fs::write(
        harness.root.path().join("config.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": 2, "profiles": {"office": {"routerUrl": "ws://127.0.0.1:8787/ws"}}
        }))
        .unwrap(),
    )
    .unwrap();
    let profile_conflict = onboarding_native_command(&harness, &bin, &install, Some(&input));
    assert!(stderr(&profile_conflict).contains("profile_conflict"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    fs::remove_file(harness.root.path().join("config.json")).unwrap();
    let skill = harness.root.path().join(".agents/skills/asr/SKILL.md");
    fs::create_dir_all(skill.parent().unwrap()).unwrap();
    fs::write(&skill, "unknown user skill").unwrap();
    let conflict = onboarding_native_command(&harness, &bin, &install, Some(&input));
    assert!(stderr(&conflict).contains("provider_configuration_conflict"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    assert_eq!(fs::read_to_string(&skill).unwrap(), "unknown user skill");
    fs::remove_dir_all(skill.parent().unwrap()).unwrap();

    fs::write(harness.root.path().join("provider-fail-add"), "").unwrap();
    let interrupted = onboarding_native_command(&harness, &bin, &install, Some(&input));
    assert!(!interrupted.status.success());
    assert!(!stderr(&interrupted).contains("provider-secret-error-must-not-appear"));
    assert!(stderr(&interrupted).contains("onboarding resume"));
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    let state_path = harness
        .root
        .path()
        .join("onboarding")
        .join(ticket.invite_id.to_string())
        .join("codex-cli/state.json");
    let enrolled: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(enrolled["stage"], "enrolled");
    let credential_path = PathBuf::from(enrolled["credentialFile"].as_str().unwrap());
    let credential = read_credential(&credential_path).unwrap();
    let saved_credential = fs::read(&credential_path).unwrap();
    fs::remove_file(harness.root.path().join("provider-fail-add")).unwrap();
    // The server may discard bootstrap assets immediately after initial install.
    fs::remove_dir_all(assets).unwrap();
    let invite_id = ticket.invite_id.to_string();
    let resume = [
        "onboarding",
        "resume",
        &invite_id,
        "--provider",
        "codex-cli",
    ];
    let installed = PathBuf::from(enrolled["executable"].as_str().unwrap());
    let installed_skill = agent_session_router::install::installed_integrations_dir(&installed)
        .unwrap()
        .join("codex/skills/asr/SKILL.md");
    let trusted_skill = fs::read(&installed_skill).unwrap();
    fs::write(&installed_skill, "changed after enrollment").unwrap();
    let changed_bundle = onboarding_native_command(&harness, &bin, &resume, None);
    assert!(stderr(&changed_bundle).contains("bootstrap_install_conflict"));
    assert_eq!(
        posts.load(Ordering::SeqCst),
        1,
        "untrusted local assets must not be reused"
    );
    fs::write(&installed_skill, trusted_skill).unwrap();
    let completed = onboarding_native_command(&harness, &bin, &resume, None);
    assert_success(&completed);
    let result: Value = serde_json::from_slice(&completed.stdout).unwrap();
    assert_eq!(result["stage"], "configured");
    assert_eq!(result["activation"], "restart_required");
    assert_eq!(
        result
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [
            "activation",
            "nextAction",
            "profile",
            "provider",
            "route",
            "serverId",
            "stage",
            "workspace"
        ]
    );
    assert_eq!(
        posts.load(Ordering::SeqCst),
        2,
        "resume replays the same enrollment"
    );
    assert_eq!(fs::read(&credential_path).unwrap(), saved_credential);
    let configured: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(configured["enrollmentId"], enrolled["enrollmentId"]);
    assert!(configured.get("ticket").is_none());
    let calls = fs::read(harness.root.path().join("provider-calls")).unwrap();
    let again = onboarding_native_command(&harness, &bin, &resume, None);
    assert_success(&again);
    let status = onboarding_native_command(
        &harness,
        &bin,
        &[
            "--profile",
            "office",
            "onboarding",
            "status",
            "--provider",
            "codex-cli",
            "--json",
        ],
        None,
    );
    assert_success(&status);
    let report: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(report["transport"], "reachable");
    assert_eq!(report["activation"], "restart_required");
    assert_eq!(report.as_object().unwrap().len(), 9);
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    assert_eq!(
        sockets.load(Ordering::SeqCst),
        0,
        "installer must not impersonate a provider session"
    );
    assert_eq!(
        fs::read(harness.root.path().join("provider-calls")).unwrap(),
        calls
    );
    for output in [
        &too_large,
        &mismatch,
        &invalid,
        &refused,
        &profile_conflict,
        &conflict,
        &interrupted,
        &completed,
        &again,
        &status,
    ] {
        for secret in [ticket.invite_token.expose(), credential.token.expose()] {
            assert!(!stdout(output).contains(secret));
            assert!(!stderr(output).contains(secret));
        }
    }
    let config = fs::read_to_string(harness.root.path().join("config.json")).unwrap();
    assert!(!config.contains(credential.token.expose()));
    assert!(!config.contains(ticket.invite_token.expose()));
    proxy.abort();
    runtime.shutdown().await.unwrap();
    runtime.wait().await.unwrap();
    let store = RouterStore::open(&harness.data_dir()).unwrap();
    assert_eq!(
        store.credential_count().unwrap(),
        2,
        "admin plus exactly one invited identity"
    );
    store.close().unwrap();
}
