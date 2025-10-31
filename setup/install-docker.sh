#!/usr/bin/env bash
set -euo pipefail

# install-docker.sh
# Installs Docker Engine (CE), containerd, Buildx, and Compose plugin using the official Docker repository.
# Target: Debian/Ubuntu (uses /etc/apt/keyrings). Best effort detection for distro.

SCRIPT_NAME=$(basename "$0")

log() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*" >&2; }
err() { echo "[ERROR] $*" >&2; }

usage() {
  cat <<USAGE
$SCRIPT_NAME - Install Docker Engine and Compose (Ubuntu/Debian)

Usage:
  sudo ./$SCRIPT_NAME [options]

Options:
  -t         Run a quick 'hello-world' test after install
  -h         Show this help

What it does:
  - Removes legacy docker packages (docker, docker.io, podman-docker)
  - Adds Docker's official apt repository and GPG key (keyring)
  - Installs: docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  - Enables and starts the docker service
  - Adds your user to the 'docker' group (requires re-login to take effect)
USAGE
}

RUN_TEST=0

while getopts ":th" opt; do
  case "$opt" in
    t) RUN_TEST=1 ;;
    h) usage; exit 0 ;;
    :) err "Option -$OPTARG requires an argument"; usage; exit 1 ;;
    \?) err "Unknown option: -$OPTARG"; usage; exit 1 ;;
  esac
done
shift $((OPTIND-1))

# Require root
if [[ $EUID -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    exec sudo -E "$0" "$@"
  else
    err "Please run as root (sudo not found)."
    exit 1
  fi
fi

# Detect OS
if [[ -f /etc/os-release ]]; then
  . /etc/os-release
else
  err "/etc/os-release not found; unsupported distribution"
  exit 1
fi

case "${ID:-}" in
  ubuntu|debian)
    DIST_ID="$ID" ;;
  *)
    if [[ "${ID_LIKE:-}" == *debian* ]]; then
      DIST_ID="debian"
    else
      err "Unsupported distro: ID='${ID:-}' ID_LIKE='${ID_LIKE:-}'. This script supports Ubuntu/Debian."
      exit 1
    fi
    ;;
 esac

# Uninstall old packages (no-op if absent)
log "Removing any old Docker packages (safe if not present)"
apt-get remove -y docker docker.io docker-doc docker-compose podman-docker containerd runc || true

log "Installing prerequisites"
apt-get update -y
apt-get install -y ca-certificates curl gnupg lsb-release apt-transport-https

install_keyring() {
  install -m 0755 -d /etc/apt/keyrings
  local url
  if [[ "$DIST_ID" == "ubuntu" ]]; then
    url="https://download.docker.com/linux/ubuntu/gpg"
  else
    url="https://download.docker.com/linux/debian/gpg"
  fi
  curl -fsSL "$url" | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
}

add_apt_repo() {
  local arch codename repo_line repo_file
  arch=$(dpkg --print-architecture)
  codename=${VERSION_CODENAME:-}
  if [[ -z "$codename" && -n "${UBUNTU_CODENAME:-}" ]]; then
    codename="$UBUNTU_CODENAME"
  fi
  if [[ -z "$codename" ]]; then
    err "Could not determine release codename (VERSION_CODENAME)."
    exit 1
  fi
  if [[ "$DIST_ID" == "ubuntu" ]]; then
    repo_line="deb [arch=${arch} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${codename} stable"
  else
    repo_line="deb [arch=${arch} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian ${codename} stable"
  fi
  repo_file="/etc/apt/sources.list.d/docker.list"
  echo "$repo_line" > "$repo_file"
}

log "Adding Docker GPG key and apt repository"
install_keyring
add_apt_repo

log "Updating apt and installing Docker packages"
aptdpkg() { DEBIAN_FRONTEND=noninteractive apt-get -y "$@"; }
aptdpkg update
aptdpkg install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

log "Enabling and starting docker service"
systemctl enable --now docker

# Add user to docker group
TARGET_USER=${SUDO_USER:-}
if [[ -z "$TARGET_USER" ]]; then
  # Fallback to the login name (may be root in containers)
  TARGET_USER=$(logname 2>/dev/null || echo "")
fi
if [[ -n "$TARGET_USER" && "$TARGET_USER" != "root" ]]; then
  log "Adding user '$TARGET_USER' to 'docker' group"
  usermod -aG docker "$TARGET_USER" || warn "Failed to add $TARGET_USER to docker group"
  warn "You must log out and back in (or 'newgrp docker') for group changes to take effect."
fi

log "Docker versions:"
if command -v docker >/dev/null 2>&1; then
  docker --version || true
  docker compose version || true
fi

if [[ $RUN_TEST -eq 1 ]]; then
  log "Running 'docker run hello-world' (first run will download the image)"
  # Try without sudo first; if permission denied, retry with sudo
  if docker run --rm hello-world >/dev/null 2>&1; then
    log "hello-world: SUCCESS"
  else
    warn "hello-world without sudo failed; retrying with sudo"
    sudo docker run --rm hello-world && log "hello-world: SUCCESS (with sudo)" || warn "hello-world test failed"
  fi
fi

log "Done. Docker Engine and Compose are installed."
