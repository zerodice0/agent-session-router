use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    process::Command,
    time::Duration,
};

use agent_session_router::{
    credentials::{CredentialFile, read_credential},
    protocol::{
        ClientMessage, MAX_WEBSOCKET_MESSAGE_BYTES, PROTOCOL_VERSION, RouterErrorCode,
        ServerMessage, parse_server_message,
    },
    router::{
        DIRECT_PREAUTH_PER_IP_LIMIT, RouterConfig, RouterExposure, RouterRuntime,
        TAILSCALE_SERVE_PREAUTH_LIMIT,
    },
};
use futures_util::{SinkExt, StreamExt};
use tempfile::tempdir;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, protocol::CloseFrame},
};
use url::Url;
use uuid::Uuid;

type RawSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn config(data_dir: &Path, exposure: RouterExposure) -> RouterConfig {
    RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        data_dir: data_dir.to_owned(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure,
    }
}

async fn start(
    exposure: RouterExposure,
) -> (tempfile::TempDir, RouterRuntime, Url, CredentialFile) {
    let directory = tempdir().expect("temporary directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("router-data");
    let runtime = RouterRuntime::start(config(&data_dir, exposure))
        .await
        .expect("start router");
    let admin = read_credential(&data_dir.join("credentials/admin.json"))
        .expect("read bootstrap credential");
    let url = Url::parse(&format!("ws://{}/ws", runtime.address)).expect("router URL");
    (directory, runtime, url, admin)
}

async fn next_close(socket: &mut RawSocket) -> CloseFrame {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(Some(frame)))) => return frame,
                Some(Ok(Message::Ping(payload))) => socket
                    .send(Message::Pong(payload))
                    .await
                    .expect("answer server ping"),
                Some(Ok(_)) => {}
                Some(Err(error)) => panic!("websocket receive failed before close: {error}"),
                None => panic!("websocket ended without close frame"),
            }
        }
    })
    .await
    .expect("close timeout")
}

async fn next_server(socket: &mut RawSocket) -> (String, ServerMessage) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => {
                    let raw = text.to_string();
                    let parsed = parse_server_message(&raw).expect("valid server message");
                    return (raw, parsed);
                }
                Some(Ok(Message::Ping(payload))) => socket
                    .send(Message::Pong(payload))
                    .await
                    .expect("answer server ping"),
                Some(Ok(Message::Close(frame))) => {
                    panic!("websocket closed before server response: {frame:?}")
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => panic!("websocket receive failed: {error}"),
                None => panic!("websocket ended before server response"),
            }
        }
    })
    .await
    .expect("server response timeout")
}

async fn connect_operator(url: &Url, token: &str) -> RawSocket {
    let (mut socket, _) = connect_async(url.as_str())
        .await
        .expect("connect websocket");
    let registration = ClientMessage::RegisterOperator {
        protocol_version: PROTOCOL_VERSION,
        token: token.to_owned(),
    };
    socket
        .send(Message::Text(
            serde_json::to_string(&registration)
                .expect("serialize registration")
                .into(),
        ))
        .await
        .expect("send registration");
    assert!(matches!(
        next_server(&mut socket).await.1,
        ServerMessage::RegisteredOperator { .. }
    ));
    socket
}

async fn assert_pre_auth_limit(exposure: RouterExposure, limit: usize) {
    let (_directory, runtime, url, _admin) = start(exposure).await;
    let mut sockets = Vec::with_capacity(limit);
    for _ in 0..limit {
        let (socket, _) = connect_async(url.as_str())
            .await
            .expect("connect within cap");
        sockets.push(socket);
    }
    let (mut rejected, _) = connect_async(url.as_str())
        .await
        .expect("overload websocket upgrade");
    let close = next_close(&mut rejected).await;
    assert_eq!(u16::from(close.code), 1013);
    drop(rejected);
    drop(sockets);
    runtime.shutdown().await.expect("shutdown router");
    runtime.wait().await.expect("join router");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_exposure_modes_enforce_their_pre_auth_caps() {
    assert_pre_auth_limit(RouterExposure::Direct, DIRECT_PREAUTH_PER_IP_LIMIT).await;
    assert_pre_auth_limit(
        RouterExposure::TailscaleServe,
        TAILSCALE_SERVE_PREAUTH_LIMIT,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credential_burst_is_bounded_by_fixed_rate_limited_responses() {
    let (_directory, runtime, url, admin) = start(RouterExposure::Direct).await;
    let mut socket = connect_operator(&url, admin.token().expose()).await;
    for index in 0..=40 {
        let ping = ClientMessage::Ping {
            request_id: format!("burst-{index}"),
        };
        socket
            .send(Message::Text(
                serde_json::to_string(&ping).expect("serialize ping").into(),
            ))
            .await
            .expect("send ping burst");
    }
    let mut rate_limited = 0;
    for _ in 0..=40 {
        match next_server(&mut socket).await.1 {
            ServerMessage::Pong { .. } => {}
            ServerMessage::Error {
                code: RouterErrorCode::RateLimited,
                ..
            } => rate_limited += 1,
            message => panic!("unexpected burst response: {:?}", message.request_id()),
        }
    }
    assert!(rate_limited >= 1);
    drop(socket);
    runtime.shutdown().await.expect("shutdown router");
    runtime.wait().await.expect("join router");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_frames_close_with_1009_and_shutdown_wins_ingress_pressure() {
    let (_directory, runtime, url, admin) = start(RouterExposure::Direct).await;
    let (mut oversized, _) = connect_async(url.as_str())
        .await
        .expect("connect oversized peer");
    oversized
        .send(Message::Text(
            "x".repeat(MAX_WEBSOCKET_MESSAGE_BYTES + 1).into(),
        ))
        .await
        .expect("send oversized frame");
    let close = next_close(&mut oversized).await;
    assert_eq!(u16::from(close.code), 1009);
    drop(oversized);

    let mut sockets = Vec::new();
    for _ in 0..DIRECT_PREAUTH_PER_IP_LIMIT {
        sockets.push(connect_operator(&url, admin.token().expose()).await);
    }
    let pressure = sockets
        .into_iter()
        .enumerate()
        .map(|(socket_index, mut socket)| {
            tokio::spawn(async move {
                for message_index in 0..2_048 {
                    let ping = ClientMessage::Ping {
                        request_id: format!("pressure-{socket_index}-{message_index}"),
                    };
                    let Ok(encoded) = serde_json::to_string(&ping) else {
                        break;
                    };
                    if socket.send(Message::Text(encoded.into())).await.is_err() {
                        break;
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    tokio::time::sleep(Duration::from_millis(10)).await;
    tokio::time::timeout(Duration::from_secs(3), runtime.shutdown())
        .await
        .expect("shutdown control starved by ingress")
        .expect("shutdown router");
    for task in pressure {
        let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    }
    tokio::time::timeout(Duration::from_secs(5), runtime.wait())
        .await
        .expect("router wait timeout")
        .expect("join router");
}

const SECRET_SENTINEL: &str = "SECRET_SENTINEL_DO_NOT_LOG_0000000000000000";

#[tokio::test(flavor = "multi_thread")]
async fn secret_log_probe() {
    if std::env::var_os("ASR_SECRET_LOG_PROBE").is_none() {
        return;
    }
    assert_eq!(SECRET_SENTINEL.len(), 43);
    let (_directory, runtime, url, _admin) = start(RouterExposure::Direct).await;
    let (mut socket, _) = connect_async(url.as_str()).await.expect("connect probe");
    let registration = ClientMessage::RegisterOperator {
        protocol_version: PROTOCOL_VERSION,
        token: SECRET_SENTINEL.to_owned(),
    };
    socket
        .send(Message::Text(
            serde_json::to_string(&registration)
                .expect("serialize probe")
                .into(),
        ))
        .await
        .expect("send probe");
    let (raw, response) = next_server(&mut socket).await;
    assert!(!raw.contains(SECRET_SENTINEL));
    assert!(matches!(
        response,
        ServerMessage::Error {
            code: RouterErrorCode::Unauthorized,
            ..
        }
    ));
    drop(socket);
    runtime.shutdown().await.expect("shutdown probe router");
    runtime.wait().await.expect("join probe router");
}

#[test]
fn secret_sentinel_is_absent_from_operational_output() {
    let executable = std::env::current_exe().expect("current test executable");
    let output = Command::new(executable)
        .args(["--exact", "secret_log_probe", "--nocapture"])
        .env("ASR_SECRET_LOG_PROBE", "1")
        .output()
        .expect("run secret log probe");
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(SECRET_SENTINEL));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET_SENTINEL));
}
