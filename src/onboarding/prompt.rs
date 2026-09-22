use std::fmt::Write as _;

use crate::bootstrap::routes::onboarding_url;

use super::{MAX_TICKET_BYTES, OnboardingTicket, RouteKind, TARGETS, TicketError};

const TEMPLATE: &str = include_str!("../../scripts/onboarding-bootstrap.sh");

/// Render a deliberately token-bearing invitation for explicit user output only.
/// Callers must not include the result in ordinary diagnostics or event logs.
pub fn render_prompt(ticket: &OnboardingTicket) -> Result<String, TicketError> {
    ticket.validate()?;
    let json = serde_json::to_string(ticket).map_err(|_| TicketError::Invalid)?;
    if json.len() > MAX_TICKET_BYTES {
        return Err(TicketError::Invalid);
    }
    let artifacts: [(&str, String); 4] = std::array::from_fn(|index| {
        ticket
            .artifacts
            .iter()
            .find(|artifact| artifact.target == TARGETS[index])
            .map_or_else(
                || ("", String::from("0")),
                |artifact| {
                    (
                        artifact.binary_sha256.as_str(),
                        artifact.binary_bytes.to_string(),
                    )
                },
            )
    });
    let mut routes: [(String, &str); 4] = std::array::from_fn(|_| (String::new(), ""));
    for (slot, route) in [
        RouteKind::Tailnet,
        RouteKind::Lan,
        RouteKind::Public,
        RouteKind::Local,
    ]
    .into_iter()
    .filter_map(|kind| ticket.routes.iter().find(|route| route.kind == kind))
    .enumerate()
    {
        // Reuse the native bootstrap endpoint validator, then set a fixed files
        // prefix. Neither a ticket filename nor a user-supplied path is executed.
        let mut url = onboarding_url(&route.router_url, "/onboarding/info")
            .map_err(|_| TicketError::InvalidRoute)?;
        url.set_path("/onboarding/files/");
        routes[slot] = (url.into(), route.ca_pem.as_deref().unwrap_or(""));
    }

    let mut output = String::with_capacity(TEMPLATE.len() + json.len() + 4_096);
    writeln!(output, "# ASR workspace onboarding\n").expect("write to string");
    writeln!(
        output,
        "Server: `{}`\nProfile: `{}`\nWorkspace: `{}`\n",
        ticket.server_id,
        ticket.profile_name,
        ticket.workspace.as_str()
    )
    .expect("write to string");
    writeln!(output, "This prompt contains a **one-time invitation valid for 10 minutes from issuance**. \
        Its server-authoritative expiry is `{}` (Unix milliseconds). The invitation remains in this \
        conversation: anyone who obtains it before expiry can take it over first. Use it only for one \
        device and one provider identity; another device/provider needs a new invitation. The prompt \
        contains no long-lived credential, private key, or administrator token.\n", ticket.expires_at)
        .expect("write to string");
    output.push_str("Before running anything, confirm that the user trusts this prompt and the server operator \
        who supplied it. Obtain OS shell execution permission through the current provider's normal \
        approval procedure. Do not bypass approvals, use sudo, install system packages, change provider \
        logins, alter Tailscale/firewalls, or invent a public download URL. If permission is unavailable, \
        report `host_permission_required` and ask the user to grant it normally.\n\n");
    output.push_str("Choose the provider from **the host running this conversation**, never by counting or \
        inspecting which provider executables happen to be installed. Install only that host. Set the \
        shell environment variable `ASR_PROVIDER` to exactly one value: `claude-code` for Claude Code, \
        `codex-cli` for Codex CLI, or `omp` for OMP. For example, in an OMP conversation use \
        `export ASR_PROVIDER=omp` in the same approved shell before the block below. This variable \
        contains only the provider name, never an invitation or credential.\n\n");
    if let Some(provider) = ticket.provider {
        writeln!(output, "**Provider restriction: `{}` only.** If this conversation is in another host, \
            stop and request a new prompt; do not set a false provider value. The shell also rejects \
            any nonmatching `ASR_PROVIDER`.\n", provider.as_str()).expect("write to string");
    }
    output.push_str("Use only the invited workspace. Workspace assignment and creation belong to the server \
        administrator; request a new server-issued prompt rather than attempting an authority bypass.\n\n");
    output.push_str("This server supplies these targets (only these can be installed):\n");
    for target in TARGETS {
        if ticket
            .artifacts
            .iter()
            .any(|artifact| artifact.target == target)
        {
            writeln!(output, "- `{target}`").expect("write to string");
        }
    }
    output.push_str("\nWindows, unsupported architectures, and a missing current-provider CLI are prerequisites \
        to resolve manually, not a reason to auto-install packages or build from source. Linux binaries \
        target GNU/Linux. The shell requires POSIX `sh`, `uname`, `curl`, `mktemp`, `chmod`, `wc`, \
        `rm`, and either `sha256sum` or `shasum`.\n\n");
    if ticket.routes[0].kind == RouteKind::Local {
        output.push_str("**LOCAL-ONLY WARNING:** this invitation points at loopback on the same machine as the \
            server. Do not distribute it to a remote client. The verified native installer must still \
            compare the persistent server ID so a different local router cannot receive the invitation.\n\n");
    }
    output.push_str("Review and execute the following fixed POSIX shell block with tracing/verbose logging off. \
        Feed the script to `sh` on stdin as shown, not as a `sh -c` argument. Do not copy the ticket \
        into command arguments, environment variables, URLs, or diagnostic output, and do not read or \
        print credential/profile/journal files into this conversation. The raw executable is downloaded \
        without a token and must match both its pinned size and SHA-256 before execution. Only supplied \
        routes are tried, in Tailnet → LAN → public order; a trust/hash failure is final, not permission \
        to bypass TLS. Public CA certificates, if supplied, are written privately for this download. \
        The native installer separately validates server identity, Tailscale/TLS, the manifest and archive \
        before enrollment. Existing PATH executables and shell startup files must not be replaced.\n\n");
    output.push_str("```sh\nsh <<'ASR_BOOTSTRAP_SCRIPT'\n");
    for (index, fragment) in TEMPLATE.split("@@").enumerate() {
        if index % 2 == 0 {
            output.push_str(fragment);
            continue;
        }
        if fragment == "TICKET_JSON" {
            // serde's compact JSON escapes newlines; it cannot end either fixed
            // here-document, and the quoted delimiter disables shell expansion.
            output.push_str(&json);
            continue;
        }
        let value = match fragment {
            "PINNED_PROVIDER" => ticket
                .provider
                .map_or("", super::OnboardingProvider::as_str),
            "DARWIN_ARM64_SHA256" => artifacts[0].0,
            "DARWIN_ARM64_BYTES" => &artifacts[0].1,
            "DARWIN_X64_SHA256" => artifacts[1].0,
            "DARWIN_X64_BYTES" => &artifacts[1].1,
            "LINUX_ARM64_SHA256" => artifacts[2].0,
            "LINUX_ARM64_BYTES" => &artifacts[2].1,
            "LINUX_X64_SHA256" => artifacts[3].0,
            "LINUX_X64_BYTES" => &artifacts[3].1,
            "ROUTE_1" => &routes[0].0,
            "CA_1" => routes[0].1,
            "ROUTE_2" => &routes[1].0,
            "CA_2" => routes[1].1,
            "ROUTE_3" => &routes[2].0,
            "CA_3" => routes[2].1,
            "ROUTE_4" => &routes[3].0,
            "CA_4" => routes[3].1,
            _ => return Err(TicketError::Invalid),
        };
        push_shell_literal(&mut output, value);
    }
    output.push_str("ASR_BOOTSTRAP_SCRIPT\n```\n\n");
    output.push_str("After the installer returns, report its real result and reproduce its `nextAction` command \
        exactly. A successful installation/registration is **not** proof that this current conversation \
        is connected. Respect `restart_required` or `not_checked`; if MCP tools are unavailable, follow \
        the reported normal restart/reload procedure and then check in the new provider session. \
        Do not use an operator CLI connection to impersonate the provider. Only report connected after \
        this host's actual MCP `workspace_list` and `workspace_members` confirm this provider identity \
        in the invited workspace. Claude Channel push/automatic execution additionally requires its \
        normal organization policy and development Channel opt-in; do not bypass either. If anything \
        fails, report the redacted error and required prerequisite, not a false installation or \
        connection success.\n");
    Ok(output)
}

fn push_shell_literal(output: &mut String, value: &str) {
    output.push('\'');
    let mut parts = value.split('\'');
    output.push_str(parts.next().unwrap_or(""));
    for part in parts {
        output.push_str("'\\''");
        output.push_str(part);
    }
    output.push('\'');
}
