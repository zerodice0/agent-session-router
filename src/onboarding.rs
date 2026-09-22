pub mod install;
pub mod issue;
pub mod journal;
pub mod prompt;
pub mod providers;
pub mod routes;

use std::fmt;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    credentials::{PublicCredentialClaims, SecretToken},
    protocol::{AgentClient, AgentSide, WorkspaceName},
};

pub const VERSION: u8 = 1;
pub const INVITE_LIFETIME_MS: i64 = 600_000;
pub const MAX_TICKET_BYTES: usize = 128 * 1024;
pub const MAX_ENROLLMENT_BYTES: usize = 4096;
pub const MAX_CA_BYTES: usize = 64 * 1024;
pub const TARGETS: [&str; 4] = [
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "aarch64-unknown-linux-gnu",
    "x86_64-unknown-linux-gnu",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum OnboardingProvider {
    ClaudeCode,
    CodexCli,
    Omp,
}

impl OnboardingProvider {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClaudeCode => "claude-code",
            Self::CodexCli => "codex-cli",
            Self::Omp => "omp",
        }
    }
    #[must_use]
    pub const fn identity(self) -> (AgentSide, AgentClient) {
        match self {
            Self::ClaudeCode => (AgentSide::Claude, AgentClient::ClaudeCode),
            Self::CodexCli => (AgentSide::Codex, AgentClient::CodexCli),
            Self::Omp => (AgentSide::Generic, AgentClient::Omp),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteKind {
    Tailnet,
    Lan,
    Public,
    Local,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OnboardingRoute {
    pub kind: RouteKind,
    pub router_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_pem: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BootstrapArtifact {
    pub target: String,
    pub binary_file: String,
    pub binary_sha256: String,
    pub archive_file: String,
    pub archive_sha256: String,
    pub binary_bytes: u64,
    pub archive_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BootstrapManifest {
    pub version: u8,
    pub asr_version: String,
    pub artifacts: Vec<BootstrapArtifact>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OnboardingInfo {
    pub version: u8,
    pub server_id: Uuid,
    pub protocol_version: u8,
    pub manifest_sha256: Option<String>,
    pub available_targets: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OnboardingTicket {
    pub version: u8,
    pub server_id: Uuid,
    pub invite_id: Uuid,
    pub invite_token: SecretToken,
    pub expires_at: i64,
    pub profile_name: String,
    pub workspace: WorkspaceName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<OnboardingProvider>,
    pub routes: Vec<OnboardingRoute>,
    pub manifest_sha256: String,
    pub artifacts: Vec<BootstrapArtifact>,
}

impl fmt::Debug for OnboardingTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnboardingTicket")
            .field("invite_id", &self.invite_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnrollmentRequest {
    pub version: u8,
    pub server_id: Uuid,
    pub invite_id: Uuid,
    pub invite_token: SecretToken,
    pub enrollment_id: Uuid,
    pub provider: OnboardingProvider,
    pub credential_id: Uuid,
    pub credential_token: SecretToken,
}

impl fmt::Debug for EnrollmentRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnrollmentRequest")
            .field("invite_id", &self.invite_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnrollmentResponse {
    pub version: u8,
    pub server_id: Uuid,
    pub invite_id: Uuid,
    pub claims: PublicCredentialClaims,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IssuedInvite {
    pub server_id: Uuid,
    pub invite_id: Uuid,
    pub invite_token: SecretToken,
    pub expires_at: i64,
    pub workspace: WorkspaceName,
    pub provider: Option<OnboardingProvider>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnrollmentError {
    #[error("malformed_request")]
    Malformed,
    #[error("invite_unavailable")]
    InviteUnavailable,
    #[error("service_unavailable")]
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TicketError {
    #[error("invalid_ticket")]
    Invalid,
    #[error("unsupported_target")]
    UnsupportedTarget,
    #[error("invalid_route")]
    InvalidRoute,
    #[error("invalid_ca")]
    InvalidCa,
}

impl OnboardingTicket {
    pub fn parse(bytes: &[u8]) -> Result<Self, TicketError> {
        if bytes.len() > MAX_TICKET_BYTES {
            return Err(TicketError::Invalid);
        }
        let ticket: Self = serde_json::from_slice(bytes).map_err(|_| TicketError::Invalid)?;
        ticket.validate()?;
        Ok(ticket)
    }

    pub fn validate(&self) -> Result<(), TicketError> {
        if self.version != VERSION
            || self.server_id.is_nil()
            || self.invite_id.is_nil()
            || self.expires_at <= 0
            || self.profile_name == "local"
            || crate::config::validate_profile_name(&self.profile_name).is_err()
            || !valid_sha256(&self.manifest_sha256)
        {
            return Err(TicketError::Invalid);
        }
        SecretToken::parse(self.invite_token.expose().to_owned())
            .map_err(|_| TicketError::Invalid)?;
        validate_routes(&self.routes)?;
        validate_artifacts(&self.artifacts)
    }
}

#[must_use]
pub fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn validate_artifacts(artifacts: &[BootstrapArtifact]) -> Result<(), TicketError> {
    if artifacts.is_empty() || artifacts.len() > TARGETS.len() {
        return Err(TicketError::Invalid);
    }
    let mut seen = std::collections::BTreeSet::new();
    for artifact in artifacts {
        if !TARGETS.contains(&artifact.target.as_str()) {
            return Err(TicketError::UnsupportedTarget);
        }
        if !seen.insert(&artifact.target)
            || artifact.binary_file != format!("asr-{}", artifact.target)
            || artifact.archive_file != format!("agent-session-router-{}.tar.gz", artifact.target)
            || !valid_sha256(&artifact.binary_sha256)
            || !valid_sha256(&artifact.archive_sha256)
            || artifact.binary_bytes == 0
            || artifact.binary_bytes > 512 * 1024 * 1024
            || artifact.archive_bytes == 0
            || artifact.archive_bytes > 512 * 1024 * 1024
        {
            return Err(TicketError::Invalid);
        }
    }
    Ok(())
}

pub fn validate_routes(routes: &[OnboardingRoute]) -> Result<(), TicketError> {
    if routes.is_empty() || routes.len() > 4 {
        return Err(TicketError::InvalidRoute);
    }
    let mut kinds = std::collections::BTreeSet::new();
    for route in routes {
        if !kinds.insert(route.kind) || (route.kind == RouteKind::Local && routes.len() != 1) {
            return Err(TicketError::InvalidRoute);
        }
        let url = url::Url::parse(&route.router_url).map_err(|_| TicketError::InvalidRoute)?;
        if url.path() != "/ws"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(TicketError::InvalidRoute);
        }
        let host = url.host().ok_or(TicketError::InvalidRoute)?;
        let loopback = match host {
            url::Host::Ipv4(ip) => {
                if ip.is_unspecified()
                    || ip.is_link_local()
                    || ip.is_multicast()
                    || ip.is_broadcast()
                {
                    return Err(TicketError::InvalidRoute);
                }
                ip.is_loopback()
            }
            url::Host::Ipv6(ip) => {
                if ip.is_unspecified()
                    || ip.is_unicast_link_local()
                    || ip.is_multicast()
                    || ip.to_ipv4_mapped().is_some()
                {
                    return Err(TicketError::InvalidRoute);
                }
                ip.is_loopback()
            }
            url::Host::Domain(name) => name.eq_ignore_ascii_case("localhost"),
        };
        match route.kind {
            RouteKind::Local if loopback && matches!(url.scheme(), "ws" | "wss") => {}
            RouteKind::Lan | RouteKind::Public if !loopback && url.scheme() == "wss" => {}
            RouteKind::Tailnet if url.scheme() == "ws" => {
                let url::Host::Ipv4(ip) = host else {
                    return Err(TicketError::InvalidRoute);
                };
                let octets = ip.octets();
                if octets[0] != 100 || !(64..=127).contains(&octets[1]) {
                    return Err(TicketError::InvalidRoute);
                }
            }
            _ => return Err(TicketError::InvalidRoute),
        }
        if let Some(pem) = &route.ca_pem {
            if url.scheme() != "wss" {
                return Err(TicketError::InvalidCa);
            }
            validate_ca_pem(pem)?;
        }
    }
    Ok(())
}

pub fn validate_ca_pem(pem: &str) -> Result<(), TicketError> {
    if pem.is_empty() || pem.len() > MAX_CA_BYTES {
        return Err(TicketError::InvalidCa);
    }
    let mut active = false;
    for line in pem.lines() {
        match line {
            "-----BEGIN CERTIFICATE-----" if !active => active = true,
            "-----END CERTIFICATE-----" if active => active = false,
            value if !active && value.trim().is_empty() => {}
            value
                if active
                    && !value.is_empty()
                    && value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=')) => {}
            _ => return Err(TicketError::InvalidCa),
        }
    }
    if active {
        return Err(TicketError::InvalidCa);
    }
    let mut reader = std::io::Cursor::new(pem.as_bytes());
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls_pemfile::certs(&mut reader) {
        roots
            .add(certificate.map_err(|_| TicketError::InvalidCa)?)
            .map_err(|_| TicketError::InvalidCa)?;
    }
    if roots.is_empty() {
        return Err(TicketError::InvalidCa);
    }
    Ok(())
}

#[must_use]
pub fn invite_subject(invite_id: Uuid) -> String {
    format!("asr-{}", invite_id.simple())
}

pub fn profile_name(explicit: Option<&str>, server_id: Uuid) -> Result<String, TicketError> {
    if server_id.is_nil() {
        return Err(TicketError::Invalid);
    }
    let name = explicit.map_or_else(
        || format!("asr-{:08x}", server_id.as_fields().0),
        str::to_owned,
    );
    if name == crate::config::DEFAULT_PROFILE
        || crate::config::validate_profile_name(&name).is_err()
    {
        return Err(TicketError::Invalid);
    }
    Ok(name)
}
