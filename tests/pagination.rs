use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use agent_session_router::{
    client::{ClientConfig, ClientRole, RouterClient},
    credentials::read_credential,
    protocol::{ClientMessage, MAX_RESPONSE_BYTES, ServerMessage, WorkspaceName},
    router::{RouterConfig, RouterExposure, RouterRuntime},
};
use tempfile::tempdir;
use url::Url;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn history_pages_fit_the_wire_budget_with_deterministic_continuation() {
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
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .expect("connect operator");
    let workspace = WorkspaceName::parse("pagination").expect("valid workspace");
    client
        .call(ClientMessage::WorkspaceCreate {
            request_id: "create-pagination".to_owned(),
            name: workspace.clone(),
        })
        .await
        .expect("create workspace");
    client
        .call(ClientMessage::WorkspaceJoin {
            request_id: "join-pagination".to_owned(),
            name: workspace.clone(),
        })
        .await
        .expect("join workspace");

    let large_content = "\n".repeat(60 * 1024);
    for index in 1..=5 {
        client
            .call(ClientMessage::WorkspacePost {
                request_id: format!("large-{index}"),
                content: large_content.clone(),
            })
            .await
            .expect("post large event");
    }

    let mut after = 0;
    let mut observed = Vec::new();
    loop {
        let response = client
            .call(ClientMessage::WorkspaceHistory {
                request_id: format!("history-after-{after}"),
                after: Some(after),
                limit: Some(100),
            })
            .await
            .expect("read bounded history");
        assert!(
            serde_json::to_vec(&response)
                .expect("serialize response")
                .len()
                <= MAX_RESPONSE_BYTES
        );
        let ServerMessage::WorkspaceHistory { page, .. } = response else {
            panic!("unexpected history response");
        };
        assert!(!page.events.is_empty());
        for event in &page.events {
            assert_eq!(
                event.seq,
                i64::try_from(observed.len()).expect("event count fits i64") + 1
            );
            observed.push(event.seq);
        }
        assert_eq!(page.next_cursor, *observed.last().expect("page event"));
        after = page.next_cursor;
        if !page.has_more {
            break;
        }
    }
    assert_eq!(observed, vec![1, 2, 3, 4, 5]);

    let recent = client
        .call(ClientMessage::WorkspaceHistory {
            request_id: "recent-history".to_owned(),
            after: None,
            limit: Some(2),
        })
        .await
        .expect("read recent history");
    let ServerMessage::WorkspaceHistory { page, .. } = recent else {
        panic!("unexpected recent history response");
    };
    assert_eq!(
        page.events
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
    assert_eq!(page.next_cursor, 5);
    assert!(!page.has_more);

    for index in 0..100 {
        client
            .call(ClientMessage::WorkspaceCreate {
                request_id: format!("create-list-{index}"),
                name: WorkspaceName::parse(format!("listed-{index:03}"))
                    .expect("valid listed workspace"),
            })
            .await
            .expect("create listed workspace");
        tokio::time::sleep(Duration::from_millis(55)).await;
    }
    let mut after = None;
    let mut listed = Vec::new();
    loop {
        let response = client
            .call(ClientMessage::WorkspaceList {
                request_id: format!("workspace-list-{}", listed.len()),
                after: after.clone(),
                limit: Some(17),
            })
            .await
            .expect("list workspaces");
        assert!(
            serde_json::to_vec(&response)
                .expect("serialize workspace list")
                .len()
                <= MAX_RESPONSE_BYTES
        );
        let ServerMessage::Workspaces {
            workspaces,
            next_cursor,
            has_more,
            ..
        } = response
        else {
            panic!("unexpected workspace list response");
        };
        listed.extend(
            workspaces
                .iter()
                .map(|workspace| workspace.name.as_str().to_owned()),
        );
        if !has_more {
            break;
        }
        assert_ne!(next_cursor, after);
        after = next_cursor;
    }
    let mut expected = (0..100)
        .map(|index| format!("listed-{index:03}"))
        .collect::<Vec<_>>();
    expected.push("pagination".to_owned());
    expected.sort();
    assert_eq!(listed, expected);

    client.close().await.expect("close client");
    runtime.shutdown().await.expect("shutdown router");
    runtime.wait().await.expect("join router");
}
