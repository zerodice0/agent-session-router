use std::{
    env,
    path::{Path, PathBuf},
    time::Duration,
};

use uuid::Uuid;

use super::{
    MAX_TICKET_BYTES, OnboardingInfo, OnboardingProvider, OnboardingTicket, VERSION, profile_name,
    prompt::render_prompt, routes::prepare_routes,
};
use crate::{
    bootstrap::{BootstrapAssets, routes::onboarding_url},
    client::{ClientConfig, ClientRole, RouterClient},
    credentials::{CredentialRole, SecretToken, read_credential},
    process::{HealthProbe, ReqwestHealthProbe, RuntimeStore, bootstrap_assets_directory},
    protocol::{ClientMessage, PROTOCOL_VERSION, ServerMessage, WorkspaceName},
};

pub struct PromptOptions {
    pub workspace: WorkspaceName,
    pub create_workspace: bool,
    pub name: Option<String>,
    pub provider: Option<OnboardingProvider>,
    pub endpoints: Vec<String>,
    pub ca_file: Option<PathBuf>,
}

pub struct IssuedPrompt {
    pub text: String,
    pub invite_id: Uuid,
    pub expires_at: i64,
    pub workspace: WorkspaceName,
    pub provider: Option<OnboardingProvider>,
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("{0}")]
pub struct PromptError(pub &'static str);

/// Issuance is always through the owned server's authenticated management connection.
/// Neither a saved default profile nor `ROUTER_URL` can redirect this authority.
pub async fn issue_prompt(
    data_dir: &Path,
    options: PromptOptions,
) -> Result<IssuedPrompt, PromptError> {
    if let Err(error) = std::fs::symlink_metadata(data_dir) {
        return Err(PromptError(
            if error.kind() == std::io::ErrorKind::NotFound {
                "router_not_running"
            } else {
                "owned_runtime_invalid"
            },
        ));
    }
    let runtime_store = RuntimeStore::new(data_dir.to_path_buf())
        .map_err(|_| PromptError("owned_runtime_invalid"))?;
    let runtime = runtime_store
        .read()
        .map_err(|_| PromptError("owned_runtime_invalid"))?
        .ok_or(PromptError("router_not_running"))?;
    let ca_file = options.ca_file.clone().or_else(|| {
        env::var_os("ASR_CA_FILE")
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
    });
    ReqwestHealthProbe::new(ca_file.clone())
        .probe(&runtime)
        .await
        .map_err(|_| PromptError("owned_router_unreachable"))?;
    let admin = read_credential(&data_dir.join("credentials/admin.json"))
        .map_err(|_| PromptError("admin_credential_invalid"))?;
    if admin.role != CredentialRole::Operator
        || admin.subject != "admin"
        || !admin.workspaces.is_empty()
    {
        return Err(PromptError("permission_denied"));
    }
    let cwd = env::current_dir().map_err(|_| PromptError("bootstrap_assets_invalid"))?;
    let assets_dir = bootstrap_assets_directory(data_dir, &cwd, env::var_os("ASR_BOOTSTRAP_DIR"));
    let assets = BootstrapAssets::load(&assets_dir)
        .map_err(|_| PromptError("bootstrap_assets_invalid"))?
        .ok_or(PromptError("bootstrap_assets_missing"))?;
    let info = owned_info(&runtime.control_url, ca_file.as_deref()).await?;
    if info.manifest_sha256.as_deref() != Some(assets.manifest_sha256()) {
        return Err(PromptError("bootstrap_manifest_mismatch"));
    }
    let name = profile_name(options.name.as_deref(), info.server_id)
        .map_err(|_| PromptError("invalid_profile"))?;
    let routes = prepare_routes(
        &runtime,
        &options.endpoints,
        ca_file.as_deref(),
        info.server_id,
        assets.manifest_sha256(),
        &data_dir.join("onboarding-ca"),
    )
    .await
    .map_err(|error| PromptError(error.code()))?;
    // Validate the full rendered shape before any workspace/invitation mutation.
    // Fixed-width stand-ins are not credentials and are never returned to the caller.
    let mut ticket = OnboardingTicket {
        version: VERSION,
        server_id: info.server_id,
        invite_id: Uuid::new_v4(),
        invite_token: SecretToken::parse("A".repeat(43))
            .map_err(|_| PromptError("invalid_ticket"))?,
        expires_at: 9_007_199_254_740_991,
        profile_name: name,
        workspace: options.workspace.clone(),
        provider: options.provider,
        routes,
        manifest_sha256: assets.manifest_sha256().into(),
        artifacts: assets.manifest().artifacts.clone(),
    };
    let encoded = serde_json::to_vec(&ticket).map_err(|_| PromptError("invalid_ticket"))?;
    if encoded.len() > MAX_TICKET_BYTES {
        return Err(PromptError("ticket_too_large"));
    }
    render_prompt(&ticket).map_err(|_| PromptError("invalid_ticket"))?;
    let (client, _events) = RouterClient::connect(ClientConfig {
        router_url: runtime
            .control_url
            .parse()
            .map_err(|_| PromptError("owned_runtime_invalid"))?,
        role: ClientRole::Operator { credential: admin },
        ca_file,
    })
    .await
    .map_err(|_| PromptError("admin_connection_failed"))?;
    let response = client
        .call(ClientMessage::OnboardingInviteIssue {
            request_id: format!("onboarding-issue:{}", Uuid::new_v4()),
            workspace: options.workspace,
            create_workspace: options.create_workspace,
            provider: options.provider,
        })
        .await;
    let _ = client.close().await;
    match response.map_err(|_| PromptError("invitation_issue_unconfirmed"))? {
        ServerMessage::OnboardingInviteIssued {
            server_id,
            invite_id,
            invite_token,
            expires_at,
            workspace,
            provider,
            ..
        } if server_id == ticket.server_id
            && workspace == ticket.workspace
            && provider == ticket.provider =>
        {
            ticket.invite_id = invite_id;
            ticket.invite_token = invite_token;
            ticket.expires_at = expires_at;
        }
        ServerMessage::Error { code, .. } => return Err(PromptError(code.as_str())),
        _ => return Err(PromptError("server_identity_mismatch")),
    }
    let text = render_prompt(&ticket).map_err(|_| PromptError("invalid_ticket"))?;
    Ok(IssuedPrompt {
        text,
        invite_id: ticket.invite_id,
        expires_at: ticket.expires_at,
        workspace: ticket.workspace,
        provider: ticket.provider,
    })
}

async fn owned_info(
    router_url: &str,
    ca_file: Option<&Path>,
) -> Result<OnboardingInfo, PromptError> {
    let url = onboarding_url(router_url, "/onboarding/info")
        .map_err(|_| PromptError("owned_runtime_invalid"))?;
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5));
    if url.scheme() == "https" {
        let tls =
            crate::tls::load_client_config(ca_file).map_err(|_| PromptError("route_tls_failed"))?;
        builder = builder.tls_backend_preconfigured((*tls).clone());
    }
    let mut response = builder
        .build()
        .map_err(|_| PromptError("owned_router_unreachable"))?
        .get(url)
        .send()
        .await
        .map_err(|_| PromptError("owned_router_unreachable"))?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(PromptError("owned_router_unreachable"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| PromptError("owned_router_unreachable"))?
    {
        if bytes.len() + chunk.len() > 16 * 1024 {
            return Err(PromptError("invalid_server_info"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let info: OnboardingInfo =
        serde_json::from_slice(&bytes).map_err(|_| PromptError("invalid_server_info"))?;
    if info.version != VERSION
        || info.protocol_version != PROTOCOL_VERSION
        || info.server_id.is_nil()
    {
        return Err(PromptError("invalid_server_info"));
    }
    Ok(info)
}
