use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use agent_session_router::{
    client::{ClientConfig, ClientRole, RouterClient},
    credentials::read_credential,
    protocol::{ClientMessage, RouterErrorCode, ServerMessage, WorkspaceEventKind, WorkspaceName},
    router::{RouterConfig, RouterExposure, RouterRuntime},
};
use tempfile::tempdir;
use url::Url;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn operator_can_create_join_post_and_read_history_over_websocket() {
    let directory = tempdir().expect("temporary directory");
    let data_dir = directory
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("data");
    let runtime = RouterRuntime::start(RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        data_dir: data_dir.clone(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    })
    .await
    .expect("start router");
    let credential = read_credential(&data_dir.join("credentials/admin.json"))
        .expect("read bootstrap credential");
    let url = Url::parse(&format!("ws://{}/ws", runtime.address)).expect("router URL");
    let (client, _events) = RouterClient::connect(ClientConfig {
        router_url: url,
        role: ClientRole::Operator {
            credential: credential.clone(),
        },
        ca_file: None,
    })
    .await
    .expect("connect operator");

    let workspace = WorkspaceName::parse("loopback").expect("valid workspace");
    let created = client
        .call(ClientMessage::WorkspaceCreate {
            request_id: "create-1".to_owned(),
            name: workspace.clone(),
        })
        .await
        .expect("create workspace");
    match created {
        ServerMessage::WorkspaceCreated {
            workspace: summary, ..
        } => {
            assert_eq!(summary.name, workspace);
        }
        _ => panic!("unexpected create response"),
    }

    let joined = client
        .call(ClientMessage::WorkspaceJoin {
            request_id: "join-1".to_owned(),
            name: workspace.clone(),
        })
        .await
        .expect("join workspace");
    match joined {
        ServerMessage::WorkspaceJoined {
            workspace: joined,
            cursor,
            ..
        } => {
            assert_eq!(joined, workspace);
            assert_eq!(cursor, 0);
        }
        _ => panic!("unexpected join response"),
    }

    let posted = client
        .call(ClientMessage::WorkspacePost {
            request_id: "post-1".to_owned(),
            content: "hello from loopback".to_owned(),
        })
        .await
        .expect("post message");
    let seq = match posted {
        ServerMessage::WorkspacePosted {
            workspace: posted,
            seq,
            ..
        } => {
            assert_eq!(posted, workspace);
            seq
        }
        _ => panic!("unexpected post response"),
    };

    let replayed = client
        .call(ClientMessage::WorkspacePost {
            request_id: "post-1".to_owned(),
            content: "hello from loopback".to_owned(),
        })
        .await
        .expect("replay post");
    match replayed {
        ServerMessage::WorkspacePosted {
            request_id,
            workspace: replayed_workspace,
            seq: replayed_seq,
        } => {
            assert_eq!(request_id, "post-1");
            assert_eq!(replayed_workspace, workspace);
            assert_eq!(replayed_seq, seq);
        }
        _ => panic!("unexpected replay response"),
    }

    let conflict = client
        .call(ClientMessage::WorkspacePost {
            request_id: "post-1".to_owned(),
            content: "different body".to_owned(),
        })
        .await
        .expect("receive conflict response");
    assert!(matches!(
        conflict,
        ServerMessage::Error {
            code: RouterErrorCode::RequestConflict,
            ..
        }
    ));

    let history = client
        .call(ClientMessage::WorkspaceHistory {
            request_id: "history-1".to_owned(),
            after: Some(0),
            limit: Some(10),
        })
        .await
        .expect("read history");
    match history {
        ServerMessage::WorkspaceHistory { page, .. } => {
            assert_eq!(page.workspace, workspace);
            assert_eq!(page.next_cursor, seq);
            assert_eq!(page.events.len(), 1);
            let event = &page.events[0];
            assert_eq!(event.kind, WorkspaceEventKind::Chat);
            assert_eq!(event.content.as_deref(), Some("hello from loopback"));
        }
        _ => panic!("unexpected history response"),
    }

    client.close().await.expect("close client");
    runtime.shutdown().await.expect("shutdown router");
    runtime.wait().await.expect("join router");

    let runtime = RouterRuntime::start(RouterConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        data_dir,
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    })
    .await
    .expect("restart router");
    let url = Url::parse(&format!("ws://{}/ws", runtime.address)).expect("router URL");
    let (client, _events) = RouterClient::connect(ClientConfig {
        router_url: url,
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .expect("reconnect operator");
    client
        .call(ClientMessage::WorkspaceJoin {
            request_id: "join-2".to_owned(),
            name: workspace.clone(),
        })
        .await
        .expect("rejoin workspace");
    let replayed = client
        .call(ClientMessage::WorkspacePost {
            request_id: "post-1".to_owned(),
            content: "hello from loopback".to_owned(),
        })
        .await
        .expect("replay durable post");
    match replayed {
        ServerMessage::WorkspacePosted {
            seq: replayed_seq, ..
        } => assert_eq!(replayed_seq, seq),
        _ => panic!("unexpected durable replay response"),
    }
    client.close().await.expect("close restarted client");
    runtime.shutdown().await.expect("shutdown restarted router");
    runtime.wait().await.expect("join restarted router");
}
