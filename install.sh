#!/bin/sh
set -eu

repository=${OTELVIEW_POSTGRES_REPOSITORY:-tsirysndr/otelview-postgres}
version=${OTELVIEW_POSTGRES_VERSION:-latest}
install_systemd=${INSTALL_SYSTEMD:-1}

say() {
    printf '%s\n' "$*"
}

fail() {
    say "error: $*" >&2
    exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v tar >/dev/null 2>&1 || fail "tar is required"

case "$(uname -s)" in
    Darwin)
        case "$(uname -m)" in
            arm64 | aarch64) target=aarch64-apple-darwin ;;
            *) fail "macOS is supported only on ARM64" ;;
        esac
        ;;
    Linux)
        case "$(uname -m)" in
            x86_64 | amd64) target=x86_64-unknown-linux-gnu ;;
            arm64 | aarch64) target=aarch64-unknown-linux-gnu ;;
            *) fail "unsupported Linux architecture: $(uname -m)" ;;
        esac
        ;;
    *) fail "unsupported operating system: $(uname -s)" ;;
esac

asset="otelview-postgres-${target}.tar.gz"
if [ "$version" = latest ]; then
    release_url="https://github.com/${repository}/releases/latest/download"
else
    release_url="https://github.com/${repository}/releases/download/${version}"
fi

tmp_dir=$(mktemp -d 2>/dev/null || mktemp -d -t otelview-postgres)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

say "Downloading ${asset}..."
curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
    "${release_url}/${asset}" -o "${tmp_dir}/${asset}"
curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
    "${release_url}/SHA256SUMS" -o "${tmp_dir}/SHA256SUMS"

expected=$(awk -v asset="$asset" '$2 == asset { print $1; exit }' "${tmp_dir}/SHA256SUMS")
[ -n "$expected" ] || fail "${asset} is missing from SHA256SUMS"
if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "${tmp_dir}/${asset}" | awk '{ print $1 }')
elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "${tmp_dir}/${asset}" | awk '{ print $1 }')
else
    fail "sha256sum or shasum is required"
fi
[ "$actual" = "$expected" ] || fail "checksum verification failed for ${asset}"

tar -xzf "${tmp_dir}/${asset}" -C "$tmp_dir"
package_dir="${tmp_dir}/otelview-postgres-${target}"
[ -x "${package_dir}/otelview-postgres" ] || fail "release archive does not contain the binary"

if [ "$(id -u)" -eq 0 ]; then
    install_dir=${INSTALL_DIR:-/usr/local/bin}
else
    install_dir=${INSTALL_DIR:-${HOME}/.local/bin}
fi
mkdir -p "$install_dir"
install -m 0755 "${package_dir}/otelview-postgres" "${install_dir}/otelview-postgres"
say "Installed otelview-postgres to ${install_dir}/otelview-postgres"

if [ "$(uname -s)" = Linux ] && [ "$(id -u)" -eq 0 ] && [ "$install_systemd" = 1 ]; then
    if [ "$install_dir" != /usr/local/bin ]; then
        say "Skipping systemd unit because INSTALL_DIR is not /usr/local/bin."
    elif command -v systemctl >/dev/null 2>&1; then
        mkdir -p /etc/otelview-postgres
        if [ ! -e /etc/otelview-postgres/env ]; then
            umask 077
            {
                say '# Required: PostgreSQL connection string'
                say 'DATABASE_URL='
                say '# LISTEN_ADDR=0.0.0.0:17271'
                say '# DATABASE_MAX_CONNECTIONS=20'
                say '# MAX_SEARCH_DEPTH=1000'
                say '# RUST_LOG=otelview_postgres=info'
            } > /etc/otelview-postgres/env
        fi
        install -m 0644 "${package_dir}/systemd/otelview-postgres.service" \
            /etc/systemd/system/otelview-postgres.service
        systemctl daemon-reload
        say "Installed systemd unit. Configure /etc/otelview-postgres/env, then run:"
        say "  systemctl enable --now otelview-postgres"
    else
        say "systemctl was not found; skipping the systemd unit."
    fi
fi

case ":${PATH}:" in
    *":${install_dir}:"*) ;;
    *) say "Add ${install_dir} to PATH to run otelview-postgres directly." ;;
esac
