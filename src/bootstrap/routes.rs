use std::{
    fmt,
    fs::File,
    io::{self, Read, Write},
    net::IpAddr,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use rustix::fs::{AtFlags, Mode, OFlags, linkat, open, openat, unlinkat};
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

use crate::{
    credentials::ensure_private_directory,
    onboarding::{
        MAX_CA_BYTES, OnboardingInfo, OnboardingRoute, RouteKind, TARGETS, VERSION, valid_sha256,
        validate_ca_pem, validate_routes,
    },
    process::{
        RouterLaunchError, SystemTailscale, TailscaleControl, TailscaleSnapshot,
        validate_tailscale_router_url,
    },
    protocol::PROTOCOL_VERSION,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const ROUTE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_INFO_BYTES: usize = 16 * 1024;
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOFOLLOW);

pub struct VerifiedRoute {
    pub route: OnboardingRoute,
    pub ca_file: Option<PathBuf>,
    pub info: OnboardingInfo,
    pub client: reqwest::Client,
}

/// Diagnostics deliberately carry no URL, pathname, response body, or library error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateFailure {
    pub kind: RouteKind,
    pub code: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteError {
    InvalidRoutes,
    KindRequired,
    InvalidCa,
    InvalidRuntime,
    TailnetUnverified,
    TailnetUnavailable,
    Tls,
    IdentityMismatch,
    ProtocolMismatch,
    ManifestMismatch,
    InvalidResponse,
    Redirect,
    NoReachableRoute { failures: Vec<CandidateFailure> },
}

impl RouteError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidRoutes => "route_invalid",
            Self::KindRequired => "route_kind_required",
            Self::InvalidCa => "ca_invalid",
            Self::InvalidRuntime => "runtime_record_invalid",
            Self::TailnetUnverified => "tailnet_unverified",
            Self::TailnetUnavailable => "tailnet_unavailable",
            Self::Tls => "route_tls_failed",
            Self::IdentityMismatch => "server_identity_mismatch",
            Self::ProtocolMismatch => "protocol_mismatch",
            Self::ManifestMismatch => "manifest_mismatch",
            Self::InvalidResponse => "route_response_invalid",
            Self::Redirect => "route_redirect_rejected",
            Self::NoReachableRoute { .. } => "no_reachable_route",
        }
    }
}

impl fmt::Display for RouteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())?;
        if let Self::NoReachableRoute { failures } = self {
            for failure in failures {
                let kind = match failure.kind {
                    RouteKind::Tailnet => "tailnet",
                    RouteKind::Lan => "lan",
                    RouteKind::Public => "public",
                    RouteKind::Local => "local",
                };
                write!(formatter, "; {kind}: {}", failure.code)?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for RouteError {}

/// Converts only a root router endpoint and a known onboarding resource.
/// Artifact names are fixed by the supported-target contract, not arbitrary paths.
pub fn onboarding_url(router_url: &str, path: &str) -> Result<Url, RouteError> {
    let known_path = matches!(path, "/onboarding/info" | "/onboarding/enroll")
        || path == "/onboarding/files/bootstrap-manifest.json"
        || path.strip_prefix("/onboarding/files/").is_some_and(|name| {
            name.strip_prefix("asr-")
                .is_some_and(|target| TARGETS.contains(&target))
                || name
                    .strip_prefix("agent-session-router-")
                    .and_then(|name| name.strip_suffix(".tar.gz"))
                    .is_some_and(|target| TARGETS.contains(&target))
        });
    if !known_path {
        return Err(RouteError::InvalidRoutes);
    }
    let mut url = Url::parse(router_url).map_err(|_| RouteError::InvalidRoutes)?;
    if !matches!(url.scheme(), "ws" | "wss")
        || url.host().is_none()
        || url.path() != "/ws"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.port() == Some(0)
        || match url.host() {
            Some(url::Host::Ipv4(ip)) => forbidden_address(IpAddr::V4(ip)),
            Some(url::Host::Ipv6(ip)) => forbidden_address(IpAddr::V6(ip)),
            _ => false,
        }
        || url.fragment().is_some()
    {
        return Err(RouteError::InvalidRoutes);
    }
    let scheme = if url.scheme() == "wss" {
        "https"
    } else {
        "http"
    };
    url.set_scheme(scheme)
        .map_err(|()| RouteError::InvalidRoutes)?;
    url.set_path(path);
    Ok(url)
}

/// Only tokenless info requests are made. A trust/identity failure is terminal;
/// transport unavailability alone permits moving to the next configured route.
pub async fn probe_routes(
    routes: &[OnboardingRoute],
    expected_server: Uuid,
    expected_manifest: Option<&str>,
    ca_directory: &Path,
) -> Result<VerifiedRoute, RouteError> {
    validate_routes(routes).map_err(|error| match error {
        crate::onboarding::TicketError::InvalidCa => RouteError::InvalidCa,
        _ => RouteError::InvalidRoutes,
    })?;
    if expected_server.is_nil() {
        return Err(RouteError::IdentityMismatch);
    }
    if expected_manifest.is_some_and(|digest| !valid_sha256(digest)) {
        return Err(RouteError::ManifestMismatch);
    }
    let mut failures = Vec::with_capacity(routes.len());
    for kind in [
        RouteKind::Tailnet,
        RouteKind::Lan,
        RouteKind::Public,
        RouteKind::Local,
    ] {
        let Some(route) = routes.iter().find(|route| route.kind == kind) else {
            continue;
        };
        let result = tokio::time::timeout(
            ROUTE_TIMEOUT,
            probe_route(route, expected_server, expected_manifest, ca_directory),
        )
        .await;
        match result {
            Ok(Ok(verified)) => return Ok(verified),
            Ok(Err(ProbeError::Fatal(error))) => return Err(error),
            Ok(Err(ProbeError::Unavailable(code))) => {
                failures.push(CandidateFailure { kind, code });
            }
            Err(_) => failures.push(CandidateFailure {
                kind,
                code: "route_timeout",
            }),
        }
    }
    Err(RouteError::NoReachableRoute { failures })
}

enum ProbeError {
    Fatal(RouteError),
    Unavailable(&'static str),
}

impl From<RouteError> for ProbeError {
    fn from(error: RouteError) -> Self {
        Self::Fatal(error)
    }
}

async fn probe_route(
    route: &OnboardingRoute,
    expected_server: Uuid,
    expected_manifest: Option<&str>,
    ca_directory: &Path,
) -> Result<VerifiedRoute, ProbeError> {
    if route.kind == RouteKind::Tailnet {
        let url = Url::parse(&route.router_url).map_err(|_| RouteError::InvalidRoutes)?;
        if url.port().is_none() || url.port() == Some(0) {
            return Err(RouteError::TailnetUnverified.into());
        }
        let snapshot = match tailscale_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(RouteError::TailnetUnavailable) => {
                return Err(ProbeError::Unavailable("tailnet_unavailable"));
            }
            Err(error) => return Err(error.into()),
        };
        let address = url
            .host_str()
            .and_then(|host| host.parse().ok())
            .ok_or(RouteError::TailnetUnverified)?;
        // An offline/absent peer is unreachable, not permission to use unverified raw TCP.
        if !snapshot.self_ipv4.contains(&address) && !snapshot.online_peer_ipv4.contains(&address) {
            return Err(ProbeError::Unavailable("tailnet_peer_offline"));
        }
        validate_tailscale_router_url(&route.router_url, &snapshot)
            .map_err(|_| RouteError::TailnetUnverified)?;
    }

    let url = onboarding_url(&route.router_url, "/onboarding/info")?;
    let secure = url.scheme() == "https";
    crate::tls::install_crypto_provider().map_err(|_| RouteError::Tls)?;
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(ROUTE_TIMEOUT);
    let ca_file = if let Some(pem) = &route.ca_pem {
        // Use validated bytes for TLS, never reopen a pathname after validating it.
        let tls = crate::tls::load_client_config_with_ca_pem(pem.as_bytes())
            .map_err(|_| RouteError::Tls)?;
        let path = persist_ca(ca_directory, pem)?;
        builder = builder.tls_backend_preconfigured((*tls).clone());
        Some(path)
    } else {
        if secure {
            let tls = crate::tls::load_client_config(None).map_err(|_| RouteError::Tls)?;
            builder = builder.tls_backend_preconfigured((*tls).clone());
        }
        None
    };

    // Resolve once and pin that result for this verified client. DNS failures are
    // reachability failures; forbidden addresses must never reach HTTP or enrollment.
    if let Some(url::Host::Domain(host)) = url.host() {
        let port = url
            .port_or_known_default()
            .ok_or(RouteError::InvalidRoutes)?;
        let addresses: Vec<_> =
            tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::lookup_host((host, port)))
                .await
                .map_err(|_| ProbeError::Unavailable("route_timeout"))?
                .map_err(|_| ProbeError::Unavailable("dns_unavailable"))?
                .collect();
        if addresses.is_empty() {
            return Err(ProbeError::Unavailable("dns_unavailable"));
        }
        if addresses.iter().any(|address| {
            forbidden_address(address.ip())
                || (route.kind == RouteKind::Local && !address.ip().is_loopback())
        }) {
            return Err(RouteError::InvalidRoutes.into());
        }
        builder = builder.resolve_to_addrs(host, &addresses);
    }
    let client = builder.build().map_err(|_| RouteError::Tls)?;
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|error| classify_transport(&error))?;
    if response.status().is_redirection() {
        return Err(RouteError::Redirect.into());
    }
    if response.status() != reqwest::StatusCode::OK {
        return Err(RouteError::InvalidResponse.into());
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_INFO_BYTES as u64)
    {
        return Err(RouteError::InvalidResponse.into());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| classify_transport(&error))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_INFO_BYTES {
            return Err(RouteError::InvalidResponse.into());
        }
        body.extend_from_slice(&chunk);
    }
    let info: OnboardingInfo =
        serde_json::from_slice(&body).map_err(|_| RouteError::InvalidResponse)?;
    if info.version != VERSION || info.protocol_version != PROTOCOL_VERSION {
        return Err(RouteError::ProtocolMismatch.into());
    }
    if info.server_id != expected_server {
        return Err(RouteError::IdentityMismatch.into());
    }
    if expected_manifest.is_some_and(|expected| info.manifest_sha256.as_deref() != Some(expected)) {
        return Err(RouteError::ManifestMismatch.into());
    }
    if info
        .manifest_sha256
        .as_deref()
        .is_some_and(|digest| !valid_sha256(digest))
        || info.available_targets.len() > TARGETS.len()
        || info
            .available_targets
            .iter()
            .enumerate()
            .any(|(index, target)| {
                !TARGETS.contains(&target.as_str())
                    || info.available_targets[..index].contains(target)
            })
    {
        return Err(RouteError::InvalidResponse.into());
    }
    Ok(VerifiedRoute {
        route: route.clone(),
        ca_file,
        info,
        client,
    })
}

fn forbidden_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(ip) => {
            ip.is_unspecified() || ip.is_link_local() || ip.is_multicast() || ip.is_broadcast()
        }
        IpAddr::V6(ip) => {
            ip.is_unspecified()
                || ip.is_unicast_link_local()
                || ip.is_multicast()
                || ip.to_ipv4_mapped().is_some()
        }
    }
}

fn classify_transport(error: &reqwest::Error) -> ProbeError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    let mut reachability = None;
    while let Some(current) = source {
        if current.downcast_ref::<rustls::Error>().is_some() {
            return RouteError::Tls.into();
        }
        if let Some(error) = current.downcast_ref::<io::Error>() {
            reachability = match error.kind() {
                io::ErrorKind::ConnectionRefused => Some("connection_refused"),
                io::ErrorKind::NetworkUnreachable | io::ErrorKind::HostUnreachable => {
                    Some("network_unreachable")
                }
                io::ErrorKind::TimedOut => Some("route_timeout"),
                _ => reachability,
            };
            // rustls errors can be wrapped as the payload of an io::Error.
            if error
                .get_ref()
                .is_some_and(|inner| inner.downcast_ref::<rustls::Error>().is_some())
            {
                return RouteError::Tls.into();
            }
        }
        source = current.source();
    }
    if error.is_timeout() {
        ProbeError::Unavailable("route_timeout")
    } else if let Some(code) = reachability {
        ProbeError::Unavailable(code)
    } else {
        // Unknown handshake/HTTP errors are not permission to downgrade routes.
        RouteError::Tls.into()
    }
}

pub(crate) async fn tailscale_snapshot() -> Result<TailscaleSnapshot, RouteError> {
    let executable =
        crate::app::find_executable("tailscale").ok_or(RouteError::TailnetUnavailable)?;
    let mut control =
        SystemTailscale::new(executable).map_err(|_| RouteError::TailnetUnavailable)?;
    let snapshot = tokio::time::timeout(ROUTE_TIMEOUT, control.snapshot())
        .await
        .map_err(|_| RouteError::TailnetUnavailable)?;
    snapshot.map_err(|error| match error {
        RouterLaunchError::TailscaleUnavailable | RouterLaunchError::TailscaleCommand => {
            RouteError::TailnetUnavailable
        }
        _ => RouteError::TailnetUnverified,
    })
}

pub(crate) fn persist_ca(directory: &Path, pem: &str) -> Result<PathBuf, RouteError> {
    validate_ca_pem(pem).map_err(|_| RouteError::InvalidCa)?;
    ensure_private_directory(directory, true).map_err(|_| RouteError::InvalidCa)?;
    let parent = open_ca_directory(directory)?;
    let name = format!("{:x}.pem", Sha256::digest(pem.as_bytes()));
    let flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK;
    match openat(&parent, name.as_str(), flags, Mode::empty()) {
        Ok(fd) => verify_ca_file(File::from(fd), pem)?,
        Err(rustix::io::Errno::NOENT) => {
            let temporary = format!(".ca-{}.tmp", Uuid::new_v4());
            let fd = openat(
                &parent,
                temporary.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::RUSR | Mode::WUSR,
            )
            .map_err(|_| RouteError::InvalidCa)?;
            let result = (|| {
                let mut file = File::from(fd);
                file.write_all(pem.as_bytes())
                    .map_err(|_| RouteError::InvalidCa)?;
                file.sync_all().map_err(|_| RouteError::InvalidCa)?;
                match linkat(
                    &parent,
                    temporary.as_str(),
                    &parent,
                    name.as_str(),
                    AtFlags::empty(),
                ) {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                    Err(_) => return Err(RouteError::InvalidCa),
                }
                Ok(())
            })();
            let removed = unlinkat(&parent, temporary.as_str(), AtFlags::empty());
            result?;
            removed.map_err(|_| RouteError::InvalidCa)?;
            let stored = openat(&parent, name.as_str(), flags, Mode::empty())
                .map_err(|_| RouteError::InvalidCa)?;
            verify_ca_file(File::from(stored), pem)?;
            parent.sync_all().map_err(|_| RouteError::InvalidCa)?;
        }
        Err(_) => return Err(RouteError::InvalidCa),
    }
    Ok(directory.join(name))
}

fn open_ca_directory(path: &Path) -> Result<File, RouteError> {
    let mut directory = File::from(
        open(
            if path.is_absolute() { "/" } else { "." },
            DIRECTORY_FLAGS,
            Mode::empty(),
        )
        .map_err(|_| RouteError::InvalidCa)?,
    );
    for component in path.components() {
        match component {
            Component::Normal(name) => {
                let child = File::from(
                    openat(&directory, name, DIRECTORY_FLAGS, Mode::empty())
                        .map_err(|_| RouteError::InvalidCa)?,
                );
                // Sync user-owned parents that may contain newly-created cache
                // directories, not system ancestors on read-only OS volumes.
                if directory
                    .metadata()
                    .map_err(|_| RouteError::InvalidCa)?
                    .uid()
                    == rustix::process::getuid().as_raw()
                {
                    directory.sync_all().map_err(|_| RouteError::InvalidCa)?;
                }
                directory = child;
            }
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) => return Err(RouteError::InvalidCa),
        }
    }
    let metadata = directory.metadata().map_err(|_| RouteError::InvalidCa)?;
    if metadata.uid() != rustix::process::getuid().as_raw() || metadata.mode() & 0o077 != 0 {
        return Err(RouteError::InvalidCa);
    }
    Ok(directory)
}

fn verify_ca_file(file: File, pem: &str) -> Result<(), RouteError> {
    let metadata = file.metadata().map_err(|_| RouteError::InvalidCa)?;
    if !metadata.is_file()
        || metadata.uid() != rustix::process::getuid().as_raw()
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
        || metadata.len() != pem.len() as u64
    {
        return Err(RouteError::InvalidCa);
    }
    let mut bytes = Vec::with_capacity(pem.len());
    file.take((MAX_CA_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| RouteError::InvalidCa)?;
    if bytes != pem.as_bytes() {
        return Err(RouteError::InvalidCa);
    }
    Ok(())
}
