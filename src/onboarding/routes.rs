use std::{
    fs::File,
    io::Read,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::{Path, PathBuf},
    time::Duration,
};

use rustix::fs::{Mode, OFlags, open};
use url::{Host, Url};
use uuid::Uuid;

use crate::{
    bootstrap::routes::{RouteError, probe_routes, tailscale_snapshot},
    onboarding::{MAX_CA_BYTES, OnboardingRoute, RouteKind, validate_ca_pem, validate_routes},
    process::{RuntimeRecord, RuntimeShareMode, TailscaleSnapshot, validate_tailscale_router_url},
};

/// Validate every advertised route before the caller creates a workspace or invitation.
/// Explicit endpoints replace, rather than supplement, the runtime's actual endpoint.
pub async fn prepare_routes(
    runtime: &RuntimeRecord,
    endpoints: &[String],
    ca_file: Option<&Path>,
    server_id: Uuid,
    manifest_sha256: &str,
    ca_directory: &Path,
) -> Result<Vec<OnboardingRoute>, RouteError> {
    runtime.validate().map_err(|_| RouteError::InvalidRuntime)?;
    if endpoints.len() > 4 {
        return Err(RouteError::InvalidRoutes);
    }
    let mut routes = if endpoints.is_empty() {
        let router_url = runtime
            .advertised_url
            .as_ref()
            .unwrap_or(&runtime.control_url);
        let url = parse_router_url(router_url)?;
        let kind = if runtime.share_mode == RuntimeShareMode::Tailscale {
            RouteKind::Tailnet
        } else {
            let addresses = resolve_addresses(&url).await?;
            let kind = classify_addresses(&addresses)?;
            if kind != RouteKind::Local && url.scheme() != "wss" {
                return Err(RouteError::InvalidRoutes);
            }
            kind
        };
        vec![OnboardingRoute {
            kind,
            router_url: router_url.clone(),
            ca_pem: None,
        }]
    } else {
        endpoints
            .iter()
            .map(|endpoint| {
                let (kind, router_url) =
                    endpoint.split_once('=').ok_or(RouteError::InvalidRoutes)?;
                let kind = match kind {
                    "tailnet" => RouteKind::Tailnet,
                    "lan" => RouteKind::Lan,
                    "public" => RouteKind::Public,
                    "local" => RouteKind::Local,
                    _ => return Err(RouteError::InvalidRoutes),
                };
                parse_router_url(router_url)?;
                Ok(OnboardingRoute {
                    kind,
                    router_url: router_url.to_owned(),
                    ca_pem: None,
                })
            })
            .collect::<Result<Vec<_>, RouteError>>()?
    };
    validate_routes(&routes).map_err(|_| RouteError::InvalidRoutes)?;

    let environment_ca = std::env::var_os("ASR_CA_FILE")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let ca = ca_file
        .or(environment_ca.as_deref())
        .map(read_ca)
        .transpose()?;
    for route in &mut routes {
        if route.router_url.starts_with("wss:") {
            route.ca_pem.clone_from(&ca);
        }
    }
    validate_routes(&routes).map_err(|_| RouteError::InvalidRoutes)?;

    // Inspect only: never enable/disable Serve or change the machine's network.
    let tailnet = if routes.iter().any(|route| route.kind == RouteKind::Tailnet) {
        Some(tailscale_snapshot().await?)
    } else {
        None
    };
    for route in &routes {
        let url = parse_router_url(&route.router_url)?;
        let addresses = resolve_addresses(&url).await?;
        validate_addresses(&addresses, route.kind)?;
        if let Some(snapshot) = &tailnet
            && route.kind == RouteKind::Tailnet
        {
            validate_owned_tailnet(runtime, &url, snapshot)?;
        }
    }
    // A successful first candidate is not sufficient for a server-issued ticket.
    // Each alias must serve this exact server identity and bootstrap manifest.
    for route in &routes {
        probe_routes(
            std::slice::from_ref(route),
            server_id,
            Some(manifest_sha256),
            ca_directory,
        )
        .await?;
    }
    Ok(routes)
}

fn parse_router_url(value: &str) -> Result<Url, RouteError> {
    let url = Url::parse(value).map_err(|_| RouteError::InvalidRoutes)?;
    if !matches!(url.scheme(), "ws" | "wss")
        || url.host().is_none()
        || url.path() != "/ws"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port() == Some(0)
    {
        return Err(RouteError::InvalidRoutes);
    }
    Ok(url)
}

fn read_ca(path: &Path) -> Result<String, RouteError> {
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|_| RouteError::InvalidCa)?;
    let file = File::from(descriptor);
    if !file
        .metadata()
        .map_err(|_| RouteError::InvalidCa)?
        .is_file()
    {
        return Err(RouteError::InvalidCa);
    }
    let mut bytes = Vec::new();
    file.take((MAX_CA_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| RouteError::InvalidCa)?;
    let pem = String::from_utf8(bytes).map_err(|_| RouteError::InvalidCa)?;
    validate_ca_pem(&pem).map_err(|_| RouteError::InvalidCa)?;
    Ok(pem)
}

async fn resolve_addresses(url: &Url) -> Result<Vec<IpAddr>, RouteError> {
    match url.host().ok_or(RouteError::InvalidRoutes)? {
        Host::Ipv4(ip) => Ok(vec![IpAddr::V4(ip)]),
        Host::Ipv6(ip) => Ok(vec![IpAddr::V6(ip)]),
        Host::Domain(host) => {
            let port = url
                .port_or_known_default()
                .ok_or(RouteError::InvalidRoutes)?;
            let addresses = tokio::time::timeout(
                Duration::from_secs(3),
                tokio::net::lookup_host((host, port)),
            )
            .await
            .map_err(|_| RouteError::KindRequired)?
            .map_err(|_| RouteError::KindRequired)?;
            let addresses: Vec<_> = addresses.map(|address| address.ip()).collect();
            if addresses.is_empty() {
                return Err(RouteError::KindRequired);
            }
            Ok(addresses)
        }
    }
}

fn validate_addresses(addresses: &[IpAddr], kind: RouteKind) -> Result<(), RouteError> {
    if addresses.is_empty() {
        return Err(RouteError::KindRequired);
    }
    for address in addresses {
        let forbidden = match address {
            IpAddr::V4(ip) => {
                ip.is_unspecified() || ip.is_link_local() || ip.is_multicast() || ip.is_broadcast()
            }
            IpAddr::V6(ip) => {
                ip.is_unspecified()
                    || ip.is_unicast_link_local()
                    || ip.is_multicast()
                    || ip.to_ipv4_mapped().is_some()
            }
        };
        if forbidden || address.is_loopback() != (kind == RouteKind::Local) {
            return Err(RouteError::InvalidRoutes);
        }
    }
    Ok(())
}

fn classify_addresses(addresses: &[IpAddr]) -> Result<RouteKind, RouteError> {
    let mut classified = None;
    for address in addresses {
        let kind = if address.is_loopback() {
            RouteKind::Local
        } else {
            match address {
                IpAddr::V4(ip) if ip.is_private() => RouteKind::Lan,
                IpAddr::V6(ip) if ip.is_unique_local() => RouteKind::Lan,
                IpAddr::V4(ip) if global_ipv4(*ip) => RouteKind::Public,
                IpAddr::V6(ip) if global_ipv6(*ip) => RouteKind::Public,
                _ => {
                    // Reject unusable distribution addresses even with an explicit kind.
                    validate_addresses(&[*address], RouteKind::Public)?;
                    return Err(RouteError::KindRequired);
                }
            }
        };
        if classified.is_some_and(|previous| previous != kind) {
            return Err(RouteError::KindRequired);
        }
        classified = Some(kind);
    }
    classified.ok_or(RouteError::KindRequired)
}

fn global_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, d] = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_documentation()
        || a == 0
        || a >= 224
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && c == 0 && !matches!(d, 9 | 10))
        || (a == 192 && b == 88 && c == 99)
        || (a == 198 && matches!(b, 18 | 19)))
}

fn global_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    // The well-known translation prefix is globally reachable; local-use
    // translation and the rest of the reserved 0000::/3 space are not.
    if segments[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return true;
    }
    let global_protocol_assignment =
        (segments[1] == 1 && segments[2..7] == [0, 0, 0, 0, 0] && matches!(segments[7], 1..=3))
            || segments[1] == 3
            || (segments[1] == 4 && segments[2] == 0x112)
            || (0x20..=0x3f).contains(&segments[1]);
    segments[0] & 0xe000 == 0x2000
        && !(segments[0] == 0x2001
            && ((segments[1] <= 0x01ff && !global_protocol_assignment) || segments[1] == 0x0db8))
        && segments[0] != 0x2002
        && !(segments[0] == 0x3fff && segments[1] <= 0x0fff)
}

fn validate_owned_tailnet(
    runtime: &RuntimeRecord,
    url: &Url,
    snapshot: &TailscaleSnapshot,
) -> Result<(), RouteError> {
    validate_tailscale_router_url(url.as_str(), snapshot)
        .map_err(|_| RouteError::TailnetUnverified)?;
    let serve = runtime
        .owned_serve
        .as_ref()
        .ok_or(RouteError::TailnetUnverified)?;
    let Some(Host::Ipv4(address)) = url.host() else {
        return Err(RouteError::TailnetUnverified);
    };
    let control = parse_router_url(&runtime.control_url)?;
    if runtime.share_mode != RuntimeShareMode::Tailscale
        || !snapshot.self_ipv4.contains(&address)
        || url.port() != Some(serve.port)
        || control.scheme() != "ws"
        || control.port() != Some(serve.port)
        || control.host() != Some(Host::Ipv4(Ipv4Addr::LOCALHOST))
        || snapshot.tcp_forwards.get(&serve.port) != Some(&serve.target)
    {
        return Err(RouteError::TailnetUnverified);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(values: &[&str]) -> Result<RouteKind, RouteError> {
        classify_addresses(
            &values
                .iter()
                .map(|value| value.parse().unwrap())
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn classification_requires_consistent_routable_addresses() {
        assert_eq!(
            classify(&["10.2.3.4", "172.31.0.1", "192.168.0.1", "fd00::1"]).unwrap(),
            RouteKind::Lan
        );
        assert_eq!(
            classify(&["8.8.8.8", "2606:4700:4700::1111"]).unwrap(),
            RouteKind::Public
        );
        assert_eq!(classify(&["127.0.0.1", "::1"]).unwrap(), RouteKind::Local);
        for addresses in [
            &["10.0.0.1", "8.8.8.8"][..],
            &["127.0.0.1", "10.0.0.1"],
            &["100.64.0.1"],
            &["192.0.2.1"],
            &["2001:db8::1"],
            &[],
        ] {
            assert_eq!(
                classify(addresses).unwrap_err().code(),
                "route_kind_required"
            );
        }
        for address in [
            "0.0.0.0",
            "::",
            "169.254.1.1",
            "fe80::1",
            "224.0.0.1",
            "ff02::1",
            "::ffff:127.0.0.1",
        ] {
            assert_eq!(classify(&[address]).unwrap_err().code(), "route_invalid");
        }
    }

    #[test]
    fn tailnet_advertisement_requires_self_and_the_owned_mapping() {
        let instance_id = Uuid::new_v4();
        let serve = crate::process::OwnedServe::loopback(instance_id, 8787);
        let runtime = RuntimeRecord {
            instance_id,
            control_url: "ws://127.0.0.1:8787/ws".into(),
            share_mode: RuntimeShareMode::Tailscale,
            advertised_url: None,
            owned_serve: Some(serve.clone()),
        };
        let mut snapshot = TailscaleSnapshot {
            backend_running: true,
            self_ipv4: vec!["100.64.0.1".parse().unwrap()],
            online_peer_ipv4: vec!["100.64.0.2".parse().unwrap()],
            tcp_forwards: [(8787, serve.target)].into(),
        };
        let own = Url::parse("ws://100.64.0.1:8787/ws").unwrap();
        validate_owned_tailnet(&runtime, &own, &snapshot).unwrap();
        let peer = Url::parse("ws://100.64.0.2:8787/ws").unwrap();
        assert!(validate_owned_tailnet(&runtime, &peer, &snapshot).is_err());
        snapshot
            .tcp_forwards
            .insert(8787, "tcp://127.0.0.1:9999".into());
        assert!(validate_owned_tailnet(&runtime, &own, &snapshot).is_err());
        snapshot.tcp_forwards.clear();
        assert!(validate_owned_tailnet(&runtime, &own, &snapshot).is_err());
    }
}
