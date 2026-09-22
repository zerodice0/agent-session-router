#!/bin/sh
# This fixed POSIX template is rendered by onboarding::prompt. Only quoted data
# slots and the compact JSON stdin document are substituted; no server commands.
set +vx
set -eu
set -f
umask 077
IFS=' 	
'

asr_fail() {
    printf '%s\n' "$1" >&2
    exit 1
}

for asr_command in sh curl mktemp chmod wc uname rm; do
    command -v "$asr_command" >/dev/null 2>&1 || asr_fail "prerequisite_missing: $asr_command; install it yourself using your normal approved procedure, then retry"
done
if command -v sha256sum >/dev/null 2>&1; then
    asr_hash_tool=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    asr_hash_tool=shasum
else
    asr_fail 'prerequisite_missing: sha256sum or shasum; no packages were installed'
fi

asr_pinned_provider=@@PINNED_PROVIDER@@
asr_provider=${ASR_PROVIDER:-}
case "$asr_provider" in
    claude-code) asr_host_command=claude ;;
    codex-cli) asr_host_command=codex ;;
    omp) asr_host_command=omp ;;
    *) asr_fail 'provider_required: explicitly set ASR_PROVIDER to the provider running this conversation: claude-code, codex-cli, or omp' ;;
esac
if [ -n "$asr_pinned_provider" ] && [ "$asr_provider" != "$asr_pinned_provider" ]; then
    asr_fail 'provider_mismatch: this invitation is restricted to a different provider; request a new prompt'
fi
command -v "$asr_host_command" >/dev/null 2>&1 || asr_fail "provider_cli_missing: $asr_host_command; install or update the current provider yourself before retrying"

asr_os=$(uname -s) || asr_fail 'unsupported_platform: cannot identify operating system'
asr_arch=$(uname -m) || asr_fail 'unsupported_platform: cannot identify architecture'
case "$asr_os/$asr_arch" in
    Darwin/arm64|Darwin/aarch64)
        asr_target=aarch64-apple-darwin
        asr_sha256=@@DARWIN_ARM64_SHA256@@
        asr_bytes=@@DARWIN_ARM64_BYTES@@
        ;;
    Darwin/x86_64|Darwin/amd64)
        asr_target=x86_64-apple-darwin
        asr_sha256=@@DARWIN_X64_SHA256@@
        asr_bytes=@@DARWIN_X64_BYTES@@
        ;;
    Linux/arm64|Linux/aarch64)
        asr_target=aarch64-unknown-linux-gnu
        asr_sha256=@@LINUX_ARM64_SHA256@@
        asr_bytes=@@LINUX_ARM64_BYTES@@
        ;;
    Linux/x86_64|Linux/amd64)
        asr_target=x86_64-unknown-linux-gnu
        asr_sha256=@@LINUX_X64_SHA256@@
        asr_bytes=@@LINUX_X64_BYTES@@
        ;;
    *) asr_fail 'unsupported_platform: only macOS/Linux on arm64/x86_64 are supported; nothing was installed' ;;
esac
[ -n "$asr_sha256" ] || asr_fail "target_unavailable: $asr_target is not supplied by this server; request a matching bootstrap bundle"

# Routes are ordered tailnet, LAN, public; local is allowed only by itself.
# The tokenless raw download is hash-pinned. The verified native installer must
# check server identity, manifest, TLS/Tailscale trust before sending credentials.
asr_route_1=@@ROUTE_1@@
asr_ca_1=@@CA_1@@
asr_route_2=@@ROUTE_2@@
asr_ca_2=@@CA_2@@
asr_route_3=@@ROUTE_3@@
asr_ca_3=@@CA_3@@
asr_route_4=@@ROUTE_4@@
asr_ca_4=@@CA_4@@

case "${TMPDIR:-/tmp}" in
    /*) ;;
    *) asr_fail 'unsafe_temporary_directory: TMPDIR must be an absolute path' ;;
esac
asr_tmp=$(mktemp -d "${TMPDIR:-/tmp}/asr-onboarding.XXXXXXXXXX") || asr_fail 'temporary_directory_failed'
trap 'rm -rf "$asr_tmp"' 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
asr_downloaded=false
for asr_index in 1 2 3 4; do
    case "$asr_index" in
        1) asr_base=$asr_route_1; asr_ca=$asr_ca_1 ;;
        2) asr_base=$asr_route_2; asr_ca=$asr_ca_2 ;;
        3) asr_base=$asr_route_3; asr_ca=$asr_ca_3 ;;
        4) asr_base=$asr_route_4; asr_ca=$asr_ca_4 ;;
    esac
    [ -n "$asr_base" ] || continue
    set --
    if [ -n "$asr_ca" ]; then
        printf '%s' "$asr_ca" > "$asr_tmp/ca.pem" || asr_fail 'ca_write_failed'
        set -- --cacert "$asr_tmp/ca.pem"
    fi
    # --disable is the first curl option: a user's curlrc must not enable
    # redirects, insecure TLS, tracing, alternate output paths, or extra URLs.
    asr_curl_status=0
    asr_http_status=$(curl --disable --silent --fail --globoff --proto '=http,https' \
        --noproxy '*' --max-redirs 0 --connect-timeout 3 --max-time 120 \
        --max-filesize "$asr_bytes" --output "$asr_tmp/asr" \
        --write-out '%{http_code}' "$@" \
        --url "${asr_base}asr-${asr_target}") || asr_curl_status=$?
    case "$asr_curl_status" in
        0) ;;
        5|6|7|28|52|55|56)
            printf 'route_unreachable: candidate %s; trying only the next supplied route\n' "$asr_index" >&2
            continue
            ;;
        35|51|58|59|60|64|66|77|80|82|83|90|91)
            asr_fail 'bootstrap_trust_failed: TLS verification failed; do not bypass it or try another route'
            ;;
        63) asr_fail 'binary_size_mismatch: refusing to execute or try another route' ;;
        22) asr_fail 'bootstrap_http_rejected: server did not provide the required binary' ;;
        *) asr_fail 'bootstrap_download_failed: refusing to execute or bypass this failure' ;;
    esac
    case "$asr_http_status" in
        200) ;;
        3??) asr_fail 'bootstrap_redirect_rejected: redirects are not followed' ;;
        *) asr_fail 'bootstrap_http_rejected: expected HTTP 200' ;;
    esac
    asr_actual_bytes=$(wc -c < "$asr_tmp/asr") || asr_fail 'binary_size_check_failed'
    set -- $asr_actual_bytes
    [ "$#" -eq 1 ] && [ "$1" = "$asr_bytes" ] || asr_fail 'binary_size_mismatch: refusing to execute or try another route'
    case "$asr_hash_tool" in
        sha256sum) asr_actual_digest=$(sha256sum < "$asr_tmp/asr") || asr_fail 'binary_digest_check_failed' ;;
        shasum) asr_actual_digest=$(shasum -a 256 < "$asr_tmp/asr") || asr_fail 'binary_digest_check_failed' ;;
    esac
    set -- $asr_actual_digest
    [ "${1:-}" = "$asr_sha256" ] || asr_fail 'binary_digest_mismatch: refusing to execute or try another route'
    asr_downloaded=true
    break
done
[ "$asr_downloaded" = true ] || asr_fail 'no_reachable_route: none of the supplied routes worked; ask the server operator for a reachable endpoint'
chmod 700 "$asr_tmp/asr" || asr_fail 'binary_permission_failed'

# Never move this JSON to arguments, environment variables, a URL, or a log.
# Keep both here-document delimiters fixed; the renderer emits compact JSON.
"$asr_tmp/asr" onboarding install --provider "$asr_provider" --stdin <<'ASR_ONBOARDING_TICKET'
@@TICKET_JSON@@
ASR_ONBOARDING_TICKET
