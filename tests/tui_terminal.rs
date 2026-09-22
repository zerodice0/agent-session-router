use std::{
    io,
    net::TcpListener,
    process::{Command, Stdio},
    time::Duration,
};

use agent_session_router::{
    client::{ClientConfig, ClientRole},
    credentials::{CredentialFile, CredentialRole},
    tui::{UiOptions, run, state::Ownership},
};

#[test]
fn non_tty_rejects_before_connection_or_terminal_output() {
    const CHILD_ENDPOINT: &str = "ASR_TEST_NON_TTY_ENDPOINT";
    if let Ok(endpoint) = std::env::var(CHILD_ENDPOINT) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let credential = CredentialFile::generate(
                CredentialRole::Operator,
                "admin".to_owned(),
                None,
                None,
                Vec::new(),
            )
            .unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                run(
                    ClientConfig {
                        router_url: endpoint.parse().unwrap(),
                        role: ClientRole::Operator { credential },
                        ca_file: None,
                    },
                    UiOptions {
                        profile_name: None,
                        workspace: None,
                        owned_runtime: None,
                        ownership: Ownership::Remote,
                        owned_data_dir: None,
                    },
                ),
            )
            .await
            .expect("nonterminal rejection must not wait for a router");
            assert_eq!(result.unwrap_err().code, "terminal_required");
        });
        return;
    }

    // Isolate terminal descriptors without mutating the test runner's process or
    // depending on whether the developer launched cargo from a real terminal.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "non_tty_rejects_before_connection_or_terminal_output",
            "--nocapture",
            "--color=never",
        ])
        .env(
            CHILD_ENDPOINT,
            format!("ws://{}/ws", listener.local_addr().unwrap()),
        )
        .env("TERM", "xterm-256color")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "nonterminal child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.stdout.contains(&0x1b),
        "terminal escapes reached stdout"
    );
    assert!(
        !output.stderr.contains(&0x1b),
        "terminal escapes reached stderr"
    );
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock,
        "a rejected console must not open an operator connection"
    );
}
