use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use agent_session_router::{
    credentials::SecretToken,
    onboarding::{
        BootstrapArtifact, OnboardingProvider, OnboardingRoute, OnboardingTicket, RouteKind,
        TARGETS, VERSION, prompt::render_prompt,
    },
    protocol::WorkspaceName,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

// This downloaded fixture really executes after verification. It records only
// into the test's private HOME; no real provider or ASR configuration is used.
const EXECUTABLE: &[u8] = br#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "$FIXTURE_OUTPUT/argv"
printf '%s\n' "$0" > "$FIXTURE_OUTPUT/executable"
env > "$FIXTURE_OUTPUT/environment"
umask > "$FIXTURE_OUTPUT/umask"
cat > "$FIXTURE_OUTPUT/ticket.json"
exit "${FIXTURE_EXIT:-0}"
"#;

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn ticket(routes: Vec<OnboardingRoute>) -> OnboardingTicket {
    OnboardingTicket {
        version: VERSION,
        server_id: Uuid::new_v4(),
        invite_id: Uuid::new_v4(),
        invite_token: SecretToken::parse("A".repeat(43)).unwrap(),
        expires_at: 1_900_000_000_000,
        profile_name: "office".into(),
        workspace: WorkspaceName::parse("team-room").unwrap(),
        provider: None,
        routes,
        manifest_sha256: "b".repeat(64),
        artifacts: TARGETS
            .iter()
            .map(|target| BootstrapArtifact {
                target: (*target).into(),
                binary_file: format!("asr-{target}"),
                binary_sha256: digest(EXECUTABLE),
                binary_bytes: EXECUTABLE.len() as u64,
                archive_file: format!("agent-session-router-{target}.tar.gz"),
                archive_sha256: "c".repeat(64),
                archive_bytes: 1,
            })
            .collect(),
    }
}

fn route(kind: RouteKind, router_url: impl Into<String>) -> OnboardingRoute {
    OnboardingRoute {
        kind,
        router_url: router_url.into(),
        ca_pem: None,
    }
}

fn shell(ticket: &OnboardingTicket) -> String {
    render_prompt(ticket)
        .unwrap()
        .split_once("```sh\n")
        .unwrap()
        .1
        .split_once("\n```\n")
        .unwrap()
        .0
        .to_owned()
}

fn write_executable(path: &Path, contents: &[u8]) {
    fs::write(path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

struct Client {
    _root: TempDir,
    root: PathBuf,
    bin: PathBuf,
    output: PathBuf,
    temporary: PathBuf,
}

impl Client {
    fn new() -> Self {
        let temporary_root = tempfile::tempdir().unwrap();
        let root = temporary_root.path().canonicalize().unwrap();
        let bin = root.join("tools");
        let output = root.join("output");
        // An apostrophe and spaces also exercise safe handling of local paths.
        let temporary = root.join("private temp's files");
        for path in [&bin, &output, &temporary] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        for name in ["claude", "codex", "omp"] {
            // A presence prerequisite only: the bootstrap must not invoke these.
            write_executable(&bin.join(name), b"#!/bin/sh\nexit 97\n");
        }
        fs::write(root.join("raw-binary"), EXECUTABLE).unwrap();
        Self {
            _root: temporary_root,
            root,
            bin,
            output,
            temporary,
        }
    }

    fn command(&self, provider: Option<&str>) -> Command {
        let mut command = Command::new("/bin/sh");
        command
            .env_clear()
            .env("HOME", &self.root)
            .env("TMPDIR", &self.temporary)
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin:/usr/local/bin", self.bin.display()),
            )
            .env("FIXTURE_OUTPUT", &self.output)
            .env("FIXTURE_BINARY", self.root.join("raw-binary"))
            .env("LC_ALL", "C")
            .current_dir(&self.root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(provider) = provider {
            command.env("ASR_PROVIDER", provider);
        }
        command
    }

    fn run(&self, ticket: &OnboardingTicket, provider: Option<&str>) -> Output {
        execute(self.command(provider), &shell(ticket))
    }

    fn platform(&self, os: &str, arch: &str) {
        write_executable(&self.bin.join("uname"), format!(
            "#!/bin/sh\ncase \"$1\" in -s) printf '%s\\n' '{os}' ;; -m) printf '%s\\n' '{arch}' ;; *) exit 1 ;; esac\n"
        ).as_bytes());
    }

    fn assert_clean(&self) {
        assert_eq!(
            fs::read_dir(&self.temporary).unwrap().count(),
            0,
            "bootstrap temporary files were not removed"
        );
    }

    fn assert_not_executed(&self) {
        assert!(!self.output.join("argv").exists());
        assert!(!self.output.join("ticket.json").exists());
        self.assert_clean();
    }

    fn curl_fixture(&self) {
        write_executable(
            &self.bin.join("curl"),
            br#"#!/bin/sh
set -eu
printf '%s\n' "$@" >> "$FIXTURE_OUTPUT/curl-argv"
out=
url=
ca=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output) out=$2; shift 2 ;;
        --url) url=$2; shift 2 ;;
        --cacert) ca=$2; shift 2 ;;
        *) shift ;;
    esac
done
printf '%s\n' "$url" >> "$FIXTURE_OUTPUT/requests"
case "$url" in
    http://100.64.0.1:8787/*) exit "${FIXTURE_TAILNET_CODE:-7}" ;;
    https://lan.example/*)
        if [ "${FIXTURE_LAN_CODE:-0}" != 0 ]; then exit "$FIXTURE_LAN_CODE"; fi ;;
esac
if [ -n "$ca" ]; then
    cat "$ca" > "$FIXTURE_OUTPUT/ca.pem"
fi
cp "$FIXTURE_BINARY" "$out"
printf '200'
"#,
        );
    }
}

fn execute(mut command: Command, script: &str) -> Output {
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

struct Server {
    url: String,
    requests: Receiver<String>,
    stopped: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Server {
    fn new(body: &[u8], redirect: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let stopped = Arc::new(AtomicBool::new(false));
        let (worker_requests, requests) = mpsc::channel();
        let worker_stopped = Arc::clone(&stopped);
        let body = body.to_vec();
        let worker = thread::spawn(move || {
            while !worker_stopped.load(Ordering::Acquire) {
                let (mut stream, _) = match listener.accept() {
                    Ok(accepted) => accepted,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("accept fixture HTTP connection: {error}"),
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while request.len() < 8192
                    && !request.windows(4).any(|window| window == b"\r\n\r\n")
                {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => request.extend_from_slice(&buffer[..count]),
                    }
                }
                worker_requests
                    .send(String::from_utf8(request).unwrap())
                    .unwrap();
                let header = if redirect {
                    "HTTP/1.1 302 Found\r\nLocation: /not-allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned()
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                };
                if stream.write_all(header.as_bytes()).is_ok() && !redirect {
                    let _ = stream.write_all(&body);
                }
            }
        });
        Self {
            url,
            requests,
            stopped,
            worker: Some(worker),
        }
    }

    fn ticket(&self) -> OnboardingTicket {
        ticket(vec![route(RouteKind::Local, &self.url)])
    }

    fn requests(&self) -> Vec<String> {
        self.requests.try_iter().collect()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap();
    }
}

#[test]
fn generated_inner_script_and_copy_paste_wrapper_are_posix_shell_syntax() {
    let client = Client::new();
    let ca = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .unwrap()
        .cert
        .pem();
    let mut invitation = ticket(vec![OnboardingRoute {
        kind: RouteKind::Local,
        router_url: "wss://localhost:8787/ws".into(),
        ca_pem: Some(ca),
    }]);
    for provider in [
        None,
        Some(OnboardingProvider::ClaudeCode),
        Some(OnboardingProvider::CodexCli),
        Some(OnboardingProvider::Omp),
    ] {
        invitation.provider = provider;
        let script = shell(&invitation);
        let inner = script
            .strip_prefix("sh <<'ASR_BOOTSTRAP_SCRIPT'\n")
            .unwrap()
            .strip_suffix("ASR_BOOTSTRAP_SCRIPT")
            .unwrap();
        for source in [&script[..], inner] {
            let mut command = client.command(None);
            command.arg("-n");
            let result = execute(command, source);
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }
    client.assert_not_executed();
}

#[test]
fn each_supported_target_executes_only_the_verified_download_with_ticket_on_stdin() {
    let server = Server::new(EXECUTABLE, false);
    let invitation = server.ticket();
    for (os, arch, target) in [
        ("Darwin", "arm64", "aarch64-apple-darwin"),
        ("Darwin", "x86_64", "x86_64-apple-darwin"),
        ("Linux", "aarch64", "aarch64-unknown-linux-gnu"),
        ("Linux", "x86_64", "x86_64-unknown-linux-gnu"),
    ] {
        let client = Client::new();
        client.platform(os, arch);
        let result = client.run(&invitation, Some("omp"));
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            fs::read_to_string(client.output.join("argv")).unwrap(),
            "onboarding\ninstall\n--provider\nomp\n--stdin\n"
        );
        let received =
            OnboardingTicket::parse(&fs::read(client.output.join("ticket.json")).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(received).unwrap(),
            serde_json::to_value(&invitation).unwrap()
        );
        let token = invitation.invite_token.expose();
        assert!(
            !fs::read_to_string(client.output.join("environment"))
                .unwrap()
                .contains(token)
        );
        assert!(!String::from_utf8_lossy(&result.stdout).contains(token));
        assert!(!String::from_utf8_lossy(&result.stderr).contains(token));
        assert_eq!(
            fs::read_to_string(client.output.join("umask"))
                .unwrap()
                .trim(),
            "0077"
        );
        let executable = fs::read_to_string(client.output.join("executable")).unwrap();
        assert!(Path::new(executable.trim()).starts_with(&client.temporary));
        assert!(!Path::new(executable.trim()).exists());
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        let request = requests.last().unwrap();
        let expected = format!("GET /onboarding/files/asr-{target} HTTP/1.1\r\n");
        let request_line = request.lines().next().unwrap_or("<empty>");
        let diagnostic_line = if request_line.contains(token) {
            "<redacted: contains invite token>"
        } else {
            request_line
        };
        assert!(
            request.starts_with(&expected),
            "os={os} arch={arch} target={target}: expected request line {:?}, got {:?}",
            expected.trim_end(),
            diagnostic_line
        );
        assert!(requests.iter().all(|request| !request.contains(token)));
        client.assert_clean();
    }
}

#[test]
fn same_size_tampering_is_never_executed_and_cleans_the_temporary_download() {
    let mut tampered = EXECUTABLE.to_vec();
    tampered[0] = b'?';
    let server = Server::new(&tampered, false);
    let client = Client::new();
    let result = client.run(&server.ticket(), Some("omp"));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("binary_digest_mismatch"));
    assert_eq!(server.requests().len(), 1);
    client.assert_not_executed();
}

#[test]
fn wrong_size_and_redirects_do_not_reach_execution_or_follow_another_url() {
    for (body, redirect, code) in [
        (
            &EXECUTABLE[..EXECUTABLE.len() - 1],
            false,
            "binary_size_mismatch",
        ),
        (EXECUTABLE, true, "bootstrap_redirect_rejected"),
    ] {
        let server = Server::new(body, redirect);
        let client = Client::new();
        let trace = client.root.join("curl-trace");
        fs::write(
            client.root.join(".curlrc"),
            format!(
                "location\ninsecure\nverbose\ntrace = \"{}\"\n",
                trace.display()
            ),
        )
        .unwrap();
        let result = client.run(&server.ticket(), Some("omp"));
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains(code));
        assert_eq!(server.requests().len(), 1);
        assert!(
            !trace.exists(),
            "bootstrap loaded the user's unsafe curl configuration"
        );
        client.assert_not_executed();
    }
}

#[test]
fn current_host_selection_is_explicit_and_enforces_the_invitation_provider() {
    let server = Server::new(EXECUTABLE, false);
    let mut invitation = server.ticket();
    invitation.provider = Some(OnboardingProvider::ClaudeCode);
    // All three provider commands are present; that must never select a host.
    for (provider, code) in [
        (None, "provider_required"),
        (Some("omp"), "provider_mismatch"),
        (Some("omp;touch unexpected"), "provider_required"),
    ] {
        let client = Client::new();
        let result = client.run(&invitation, provider);
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains(code));
        client.assert_not_executed();
    }
    assert!(server.requests().is_empty());
}

#[test]
fn unsupported_platform_and_unavailable_target_fail_before_downloading() {
    let server = Server::new(EXECUTABLE, false);
    let client = Client::new();
    client.platform("MINGW64_NT", "x86_64");
    let result = client.run(&server.ticket(), Some("omp"));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("unsupported_platform"));
    client.assert_not_executed();
    client.platform("Linux", "x86_64");
    let mut invitation = server.ticket();
    invitation
        .artifacts
        .retain(|artifact| artifact.target != "x86_64-unknown-linux-gnu");
    let result = client.run(&invitation, Some("omp"));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("target_unavailable"));
    assert!(server.requests().is_empty());
    client.assert_not_executed();
}

#[test]
fn unavailable_tailnet_uses_only_the_next_supplied_route_but_tls_failure_is_terminal() {
    let invitation = ticket(vec![
        route(RouteKind::Public, "wss://public.example/ws"),
        route(RouteKind::Lan, "wss://lan.example/ws"),
        route(RouteKind::Tailnet, "ws://100.64.0.1:8787/ws"),
    ]);
    let client = Client::new();
    client.platform("Linux", "x86_64");
    client.curl_fixture();
    let result = client.run(&invitation, Some("codex-cli"));
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        fs::read_to_string(client.output.join("requests")).unwrap(),
        "http://100.64.0.1:8787/onboarding/files/asr-x86_64-unknown-linux-gnu\nhttps://lan.example/onboarding/files/asr-x86_64-unknown-linux-gnu\n"
    );
    client.assert_clean();

    let client = Client::new();
    client.platform("Linux", "x86_64");
    client.curl_fixture();
    let mut command = client.command(Some("codex-cli"));
    command.env("FIXTURE_LAN_CODE", "60");
    let result = execute(command, &shell(&invitation));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("bootstrap_trust_failed"));
    assert_eq!(
        fs::read_to_string(client.output.join("requests")).unwrap(),
        "http://100.64.0.1:8787/onboarding/files/asr-x86_64-unknown-linux-gnu\nhttps://lan.example/onboarding/files/asr-x86_64-unknown-linux-gnu\n"
    );
    client.assert_not_executed();
}

#[test]
fn a_pinned_digest_failure_does_not_fall_back_to_a_different_supplied_route() {
    let client = Client::new();
    client.curl_fixture();
    let mut tampered = EXECUTABLE.to_vec();
    tampered[0] = b'?';
    fs::write(client.root.join("raw-binary"), tampered).unwrap();
    let invitation = ticket(vec![
        route(RouteKind::Lan, "wss://lan.example/ws"),
        route(RouteKind::Public, "wss://public.example/ws"),
    ]);
    let result = client.run(&invitation, Some("omp"));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("binary_digest_mismatch"));
    let requests = fs::read_to_string(client.output.join("requests")).unwrap();
    assert_eq!(requests.lines().count(), 1);
    assert!(requests.starts_with("https://lan.example/onboarding/files/asr-"));
    client.assert_not_executed();
}

#[test]
fn public_ca_is_delivered_to_curl_and_literal_route_values_are_not_shell_expanded() {
    let client = Client::new();
    client.curl_fixture();
    let ca = rcgen::generate_simple_self_signed(vec!["localhost".into()])
        .unwrap()
        .cert
        .pem();
    // These ASCII characters are allowed in a URL's domain but must remain data
    // in the shell. A bare or incorrectly single-quoted interpolation executes it.
    let invitation = ticket(vec![OnboardingRoute {
        kind: RouteKind::Public,
        router_url: "wss://quote'$(id)'host.example/ws".into(),
        ca_pem: Some(ca.clone()),
    }]);
    let result = client.run(&invitation, Some("omp"));
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        fs::read_to_string(client.output.join("ca.pem")).unwrap(),
        ca
    );
    let requests = fs::read_to_string(client.output.join("requests")).unwrap();
    assert!(requests.starts_with("https://quote'$(id)'host.example/onboarding/files/asr-"));
    let received =
        OnboardingTicket::parse(&fs::read(client.output.join("ticket.json")).unwrap()).unwrap();
    assert_eq!(received.routes, invitation.routes);
    client.assert_clean();
}

#[test]
fn downloaded_installer_failure_is_preserved_and_temporary_executable_is_removed() {
    let server = Server::new(EXECUTABLE, false);
    let client = Client::new();
    let mut command = client.command(Some("omp"));
    command.env("FIXTURE_EXIT", "17");
    let result = execute(command, &shell(&server.ticket()));
    assert_eq!(result.status.code(), Some(17));
    assert!(client.output.join("ticket.json").is_file());
    client.assert_clean();
}

#[test]
fn missing_bootstrap_prerequisites_fail_without_downloading_or_installing_packages() {
    let server = Server::new(EXECUTABLE, false);
    for missing in ["curl", "mktemp", "chmod", "wc", "sha256sum-or-shasum"] {
        let client = Client::new();
        for name in ["sh", "curl", "mktemp", "chmod", "wc", "uname", "rm"] {
            if name == missing {
                continue;
            }
            let source = ["/bin", "/usr/bin", "/usr/local/bin"]
                .into_iter()
                .map(|directory| Path::new(directory).join(name))
                .find(|path| path.is_file())
                .expect("test host's bootstrap prerequisite");
            symlink(source, client.bin.join(name)).unwrap();
        }
        let mut command = client.command(Some("omp"));
        command.env("PATH", &client.bin);
        let result = execute(command, &shell(&server.ticket()));
        assert!(!result.status.success());
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(error.contains("prerequisite_missing"));
        if missing == "sha256sum-or-shasum" {
            assert!(error.contains("sha256sum or shasum"));
        } else {
            assert!(error.contains(missing));
        }
        client.assert_not_executed();
    }
    assert!(server.requests().is_empty());
}

#[test]
fn exhausting_supplied_routes_never_invents_a_public_download() {
    let client = Client::new();
    client.platform("Linux", "x86_64");
    client.curl_fixture();
    let invitation = ticket(vec![route(RouteKind::Tailnet, "ws://100.64.0.1:8787/ws")]);
    let result = client.run(&invitation, Some("omp"));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("no_reachable_route"));
    assert_eq!(
        fs::read_to_string(client.output.join("requests")).unwrap(),
        "http://100.64.0.1:8787/onboarding/files/asr-x86_64-unknown-linux-gnu\n"
    );
    client.assert_not_executed();
}
