use agent_session_router::{
    client::{ClientConfig, ClientRole, RouterClient},
    credentials::{CredentialRole, read_credential},
    onboarding::OnboardingProvider,
    protocol::{ClientMessage, RouterErrorCode, ServerMessage, WorkspaceName},
    router::{RouterConfig, RouterExposure, RouterRuntime},
};
use uuid::Uuid;

#[tokio::test]
async fn invitation_issue_requires_admin_before_workspace_creation() {
    let directory = tempfile::tempdir().unwrap();
    let data_dir = directory.path().canonicalize().unwrap().join("data");
    let runtime = RouterRuntime::start(RouterConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        data_dir: data_dir.clone(),
        instance_id: Uuid::new_v4(),
        tls_cert_file: None,
        tls_key_file: None,
        public_url: None,
        exposure: RouterExposure::Direct,
        onboarding_assets_dir: None,
    })
    .await
    .unwrap();
    let url = format!("ws://{}/ws", runtime.address).parse().unwrap();
    let admin = read_credential(&data_dir.join("credentials/admin.json")).unwrap();
    let (client, _events) = RouterClient::connect(ClientConfig {
        router_url: url,
        role: ClientRole::Operator { credential: admin },
        ca_file: None,
    })
    .await
    .unwrap();
    let workspace = WorkspaceName::parse("team-room").unwrap();
    assert!(matches!(
        client
            .call(ClientMessage::OnboardingInviteIssue {
                request_id: "missing".into(),
                workspace: workspace.clone(),
                create_workspace: false,
                provider: None,
            })
            .await
            .unwrap(),
        ServerMessage::Error {
            code: RouterErrorCode::WorkspaceNotFound,
            ..
        }
    ));
    let response = client
        .call(ClientMessage::OnboardingInviteIssue {
            request_id: "create".into(),
            workspace: workspace.clone(),
            create_workspace: true,
            provider: Some(OnboardingProvider::CodexCli),
        })
        .await
        .unwrap();
    let invite_id = match response {
        ServerMessage::OnboardingInviteIssued {
            invite_id,
            workspace: actual,
            ..
        } => {
            assert_eq!(actual, workspace);
            invite_id
        }
        _ => panic!("expected issued invitation"),
    };
    for index in 0..2 {
        assert!(matches!(
            client
                .call(ClientMessage::OnboardingInviteRevoke {
                    request_id: format!("revoke-{index}"),
                    invite_id,
                })
                .await
                .unwrap(),
            ServerMessage::OnboardingInviteRevoked { .. }
        ));
    }
    let ServerMessage::CredentialIssued { credential, .. } = client
        .call(ClientMessage::CredentialIssue {
            request_id: "scoped".into(),
            role: CredentialRole::Operator,
            subject: "scoped".into(),
            agent_side: None,
            agent_client: None,
            workspaces: vec![workspace.clone()],
        })
        .await
        .unwrap()
    else {
        panic!("expected scoped credential");
    };
    let (scoped, _events) = RouterClient::connect(ClientConfig {
        router_url: format!("ws://{}/ws", runtime.address).parse().unwrap(),
        role: ClientRole::Operator { credential },
        ca_file: None,
    })
    .await
    .unwrap();
    assert_scoped_invitation_cannot_create_workspace(&client, &scoped).await;
    client.close().await.unwrap();
    scoped.close().await.unwrap();
    runtime.shutdown().await.unwrap();
    runtime.wait().await.unwrap();
}

async fn assert_scoped_invitation_cannot_create_workspace(
    client: &RouterClient,
    scoped: &RouterClient,
) {
    for create_workspace in [false, true] {
        assert!(matches!(
            scoped
                .call(ClientMessage::OnboardingInviteIssue {
                    request_id: format!("denied-{create_workspace}"),
                    workspace: WorkspaceName::parse("must-not-exist").unwrap(),
                    create_workspace,
                    provider: None,
                })
                .await
                .unwrap(),
            ServerMessage::Error {
                code: RouterErrorCode::PermissionDenied,
                ..
            }
        ));
    }
    assert!(matches!(
        client
            .call(ClientMessage::OnboardingInviteIssue {
                request_id: "unchanged".into(),
                workspace: WorkspaceName::parse("must-not-exist").unwrap(),
                create_workspace: false,
                provider: None,
            })
            .await
            .unwrap(),
        ServerMessage::Error {
            code: RouterErrorCode::WorkspaceNotFound,
            ..
        }
    ));
}

#[test]
fn ticket_routes_reject_insecure_and_ambiguous_authorities() {
    use agent_session_router::onboarding::{OnboardingRoute, RouteKind, validate_routes};
    for (kind, router_url) in [
        (RouteKind::Lan, "ws://192.168.1.2/ws"),
        (RouteKind::Public, "wss://user@example.com/ws"),
        (RouteKind::Public, "wss://example.com/ws?token=secret"),
        (RouteKind::Public, "wss://example.com/prefix/ws"),
        (RouteKind::Local, "ws://192.168.1.2/ws"),
        (RouteKind::Tailnet, "ws://100.63.255.255/ws"),
        (RouteKind::Tailnet, "ws://peer.example/ws"),
        (RouteKind::Lan, "wss://[fe80::1]/ws"),
    ] {
        assert!(
            validate_routes(&[OnboardingRoute {
                kind,
                router_url: router_url.into(),
                ca_pem: None
            }])
            .is_err()
        );
    }
    let local = OnboardingRoute {
        kind: RouteKind::Local,
        router_url: "ws://127.0.0.1/ws".into(),
        ca_pem: None,
    };
    let public = OnboardingRoute {
        kind: RouteKind::Public,
        router_url: "wss://example.com/ws".into(),
        ca_pem: None,
    };
    assert!(validate_routes(std::slice::from_ref(&local)).is_ok());
    assert!(validate_routes(&[local, public.clone()]).is_err());
    assert!(validate_routes(&[public.clone(), public]).is_err());
}

#[test]
fn generated_profile_names_are_server_bound_and_local_is_reserved() {
    use agent_session_router::onboarding::profile_name;
    let server = Uuid::parse_str("abcdef12-3456-4789-abcd-0123456789ab").unwrap();
    assert_eq!(profile_name(None, server).unwrap(), "asr-abcdef12");
    assert!(profile_name(Some("local"), server).is_err());
    assert!(profile_name(Some("../office"), server).is_err());
    assert!(profile_name(None, Uuid::nil()).is_err());
}
