#!/usr/bin/env bash
set -euo pipefail

# kvm-create-vm.sh
# Create a KVM virtual machine using virt-install
# Supports two flows:
#  - ISO installer (-i): boots installer ISO and creates a fresh disk
#  - Cloud image (-I): clones/resizes a cloud image and boots with cloud-init seed
#  - Delete VM (-X): destroy, undefine, and remove associated disk/seed
#
# Requirements: virt-install, virsh, qemu-img; optional: cloud-localds, osinfo-query

SCRIPT_NAME=$(basename "$0")

# Defaults
NAME=""
VCPUS=2
MEM_MB=2048
DISK_SIZE_GB=20
DISK_PATH=""
NETWORK="network=default"
BRIDGE=""
OS_VARIANT="ubuntu24.04"
ISO_PATH=""
CLOUD_IMAGE=""
USER_NAME="ubuntu"
SSH_KEY_PATH="${HOME}/.ssh/id_rsa.pub"
PASSWORD=""
DEFINE_ONLY=0
NO_START=0
FORCE=0
GRAPHICS="vnc"   # for ISO installs; cloud images default to headless
DELETE_VM=0
AUTO_DOWNLOAD=0

# Ubuntu defaults (24.04 LTS - noble)
UBUNTU_IMAGE_LOCAL="/var/lib/libvirt/images/ubuntu-24.04-server-cloudimg-amd64.img"
UBUNTU_IMAGE_URL="https://cloud-images.ubuntu.com/releases/24.04/release/ubuntu-24.04-server-cloudimg-amd64.img"

LIBVIRT_IMAGES_DIR="/var/lib/libvirt/images"

log() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*" >&2; }
err() { echo "[ERROR] $*" >&2; }

usage() {
  cat <<USAGE
$SCRIPT_NAME - Create a KVM virtual machine using virt-install

Usage:
  # Minimal (defaults to Ubuntu 24.04 cloud image, auto name):
  sudo $SCRIPT_NAME -c <vcpus> -m <mem_mb>

  # Explicit name, auto Ubuntu cloud image:
  sudo $SCRIPT_NAME -n <name> -c <vcpus> -m <mem_mb>

  # Explicit install sources:
  sudo $SCRIPT_NAME -n <name> [options] (-i <installer.iso> | -I <cloud-image.qcow2>)
  sudo $SCRIPT_NAME -n <name> -X [-f] [-d <disk_path>]

Required:
  -n NAME                  VM name (unique)
  One of:
    -i ISO                 Path to OS installer ISO (ISO mode)
    -I CLOUD_IMG           Path to cloud image qcow2 (Cloud mode)

Common options:
  -c VCPUS                 Number of vCPUs (default: $VCPUS)
  -m MEM_MB                Memory in MB (default: $MEM_MB)
  -s DISK_GB               Disk size in GB (default: $DISK_SIZE_GB)
  -d DISK_PATH             Disk path (default: $LIBVIRT_IMAGES_DIR/NAME.qcow2)
  -o OS_VARIANT            osinfo variant (e.g., ubuntu22.04, rhel9)
                           default: $OS_VARIANT
  -N NETWORK               Libvirt network name (default: default)
  -b BRIDGE                Bridge name (use bridged networking instead of libvirt network)
  -f                       Force overwrite existing disk/seed files if present
  -D                       Define only (do not start the VM)
  -S                       Do not auto-start (best-effort, only for ISO mode)
  -X                       Delete VM: destroy+undefine and remove disk/seed (requires -n)
  -A                       Auto-download Ubuntu cloud image if missing (uses curl or wget)
  -h                       Show this help

Cloud-image mode extras:
  -u USER                  Default user (default: $USER_NAME)
  -k PUBKEY                SSH public key path (default: $SSH_KEY_PATH if exists)
  -p PASSWORD              Set login password (enables password auth). If omitted, a random password is generated.

Examples:
  # ISO install of Ubuntu Server
  sudo $SCRIPT_NAME -n demo-iso -i /isos/ubuntu-24.04-live-server-amd64.iso -c 4 -m 4096 -s 30 -o ubuntu24.04

  # Cloud image boot with cloud-init (headless)
  sudo $SCRIPT_NAME -n demo-cloud -I /images/ubuntu-24.04-server-cloudimg-amd64.img -c 2 -m 2048 -s 20 -u ubuntu -k ~/.ssh/id_ed25519.pub -o ubuntu24.04

  # Delete a VM and its default disk/seed
  sudo $SCRIPT_NAME -n demo-cloud -X -f

  # Minimal Ubuntu with 4 vCPU / 8GB (auto name and image; download if needed)
  sudo $SCRIPT_NAME -c 4 -m 8192 -A

Notes:
  - Run as root or via sudo. Members of the 'libvirt' group may not need sudo on some distros.
  - Ensure libvirtd is running and the 'default' network is active, or use -b to specify a bridge.
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    err "Required command not found: $cmd"
    exit 1
  fi
}

check_optional() {
  command -v "$1" >/dev/null 2>&1
}

ensure_libvirt_running() {
  if command -v systemctl >/dev/null 2>&1; then
    if ! systemctl is-active --quiet libvirtd; then
      warn "libvirtd is not active; attempting to start..."
      systemctl start libvirtd || true
    fi
  fi
}

ensure_default_network() {
  # Ensure default network exists and is active when using network=default
  if [[ "$NETWORK" == network=default* ]]; then
    if virsh net-info default >/dev/null 2>&1; then
      if ! virsh net-info default | grep -q "Active:.*yes"; then
        warn "Libvirt 'default' network is inactive; attempting to start..."
        virsh net-start default || true
        virsh net-autostart default || true
      fi
    else
      warn "Libvirt 'default' network not found; you may need to define it or use -b for bridge."
    fi
  fi
}

maybe_reexec_sudo() {
  if [[ $EUID -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
      exec sudo -E "$0" "$@"
    else
      err "Please run as root (sudo not found)."
      exit 1
    fi
  fi
}

download_if_needed() {
  local url="$1"
  local dst="$2"
  if [[ -f "$dst" ]]; then
    return 0
  fi
  if [[ $AUTO_DOWNLOAD -ne 1 ]]; then
    err "Cloud image not found: $dst"
    echo "Hint: download manually, e.g.:"
    echo "  sudo curl -L -o '$dst' '$url'"
    echo "or"
    echo "  sudo wget -O '$dst' '$url'"
    exit 1
  fi
  log "Auto-downloading Ubuntu cloud image -> $dst"
  mkdir -p "$(dirname "$dst")"
  local tmp="$dst.part"
  if command -v curl >/dev/null 2>&1; then
    curl -L "$url" -o "$tmp"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$tmp" "$url"
  else
    err "Neither curl nor wget is available to download $url"
    exit 1
  fi
  mv "$tmp" "$dst"
}

delete_vm() {
  local name="$NAME"
  if [[ -z "$name" ]]; then
    err "-n NAME is required for delete (-X)"
    exit 1
  fi

  if [[ $FORCE -ne 1 ]]; then
    read -r -p "Delete VM '$name' and remove associated disk/seed? [y/N]: " ans
    case "$ans" in
      y|Y|yes|YES) ;;
      *) err "Deletion cancelled"; exit 1 ;;
    esac
  fi

  ensure_libvirt_running

  if virsh dominfo "$name" >/dev/null 2>&1; then
    state=$(virsh domstate "$name" 2>/dev/null || true)
    if [[ "$state" == "running" || "$state" == "paused" ]]; then
      log "Stopping running domain: $name"
      virsh destroy "$name" || true
    fi

    log "Undefining domain: $name"
    # Try to remove all possible managed storage, ignore failures
    virsh undefine "$name" --nvram --managed-save --remove-all-storage >/dev/null 2>&1 || \
    virsh undefine "$name" --remove-all-storage >/dev/null 2>&1 || \
    virsh undefine "$name" >/dev/null 2>&1 || true
  else
    warn "Domain not found: $name (continuing with file cleanup)"
  fi

  # Guess default disk and seed paths if not provided
  if [[ -z "$DISK_PATH" ]]; then
    DISK_PATH="$LIBVIRT_IMAGES_DIR/$NAME.qcow2"
  fi
  local seed_iso="$LIBVIRT_IMAGES_DIR/${NAME}-seed.iso"

  for f in "$DISK_PATH" "$seed_iso"; do
    if [[ -e "$f" ]]; then
      log "Removing file: $f"
      rm -f -- "$f" || warn "Failed to remove $f"
    fi
  done

  log "Delete completed for VM: $name"
}

# Parse args
while getopts ":n:c:m:s:d:N:b:o:i:I:u:k:p:DSfXAh" opt; do
  case "$opt" in
    n) NAME="$OPTARG" ;;
    c) VCPUS="$OPTARG" ;;
    m) MEM_MB="$OPTARG" ;;
    s) DISK_SIZE_GB="$OPTARG" ;;
    d) DISK_PATH="$OPTARG" ;;
    N) NETWORK="network=$OPTARG" ;;
    b) BRIDGE="$OPTARG" ;;
    o) OS_VARIANT="$OPTARG" ;;
    i) ISO_PATH="$OPTARG" ;;
    I) CLOUD_IMAGE="$OPTARG" ;;
    u) USER_NAME="$OPTARG" ;;
    k) SSH_KEY_PATH="$OPTARG" ;;
    p) PASSWORD="$OPTARG" ;;
    D) DEFINE_ONLY=1 ;;
    S) NO_START=1 ;;
    f) FORCE=1 ;;
  X) DELETE_VM=1 ;;
    A) AUTO_DOWNLOAD=1 ;;
    h) usage; exit 0 ;;
    :) err "Option -$OPTARG requires an argument"; echo; usage; exit 1 ;;
    \?) err "Unknown option: -$OPTARG"; echo; usage; exit 1 ;;
  esac
done
shift $((OPTIND-1))

# If delete mode, handle and exit early
if [[ $DELETE_VM -eq 1 ]]; then
  maybe_reexec_sudo "$@"
  delete_vm
  exit 0
fi

# Validate inputs for creation modes
# If no name provided, auto-generate for convenience
if [[ -z "$NAME" ]]; then
  NAME="ubuntu-$(date +%Y%m%d-%H%M%S)"
  log "No name provided. Using auto-generated name: $NAME"
fi

if [[ -z "$DISK_PATH" ]]; then
  DISK_PATH="$LIBVIRT_IMAGES_DIR/$NAME.qcow2"
fi

if [[ -n "$BRIDGE" ]]; then
  NETWORK="bridge=$BRIDGE,model=virtio"
fi

MODE=""
if [[ -n "$ISO_PATH" && -n "$CLOUD_IMAGE" ]]; then
  err "Provide only one of -i ISO or -I CLOUD_IMG"
  exit 1
elif [[ -n "$ISO_PATH" ]]; then
  MODE="iso"
elif [[ -n "$CLOUD_IMAGE" ]]; then
  MODE="cloud"
else
  MODE="cloud"
  CLOUD_IMAGE="$UBUNTU_IMAGE_LOCAL"
  log "No -i/-I provided. Defaulting to Ubuntu cloud image: $CLOUD_IMAGE"
fi

# Require core tools
require_cmd virt-install
require_cmd virsh
require_cmd qemu-img

# Re-exec with sudo if not root (needed for /var/lib/libvirt/images and libvirt control)
maybe_reexec_sudo "$@"

# Post-sudo: ensure services and network
ensure_libvirt_running
ensure_default_network

# Prepare per-mode
if [[ "$MODE" == "iso" ]]; then
  if [[ ! -f "$ISO_PATH" ]]; then
    err "ISO not found: $ISO_PATH"
    exit 1
  fi

  if [[ -e "$DISK_PATH" && $FORCE -ne 1 ]]; then
    err "Disk already exists: $DISK_PATH (use -f to overwrite)"
    exit 1
  fi

  if [[ $FORCE -eq 1 && -e "$DISK_PATH" ]]; then
    warn "Removing existing disk: $DISK_PATH"
    rm -f -- "$DISK_PATH"
  fi

  log "Starting ISO install VM '$NAME' with disk $DISK_PATH (${DISK_SIZE_GB}G)"

  # virt-install will create the disk with size when path does not exist
  CMD=(
    virt-install
    --name "$NAME"
    --vcpus "$VCPUS"
    --memory "$MEM_MB"
    --disk "path=$DISK_PATH,format=qcow2,size=$DISK_SIZE_GB"
    --cdrom "$ISO_PATH"
    --network "$NETWORK"
    --noautoconsole
    --graphics "$GRAPHICS"
    --wait -1
  )
  if [[ -n "$OS_VARIANT" ]]; then
    CMD+=(--os-variant "$OS_VARIANT")
  fi

  if [[ $DEFINE_ONLY -eq 1 ]]; then
    log "Defining (not starting) domain via printed XML"
    "${CMD[@]}" --dry-run --print-xml | virsh define /dev/stdin
  else
    if [[ $NO_START -eq 1 ]]; then
      warn "--no-start best-effort: defining then starting disabled"
      "${CMD[@]}" --dry-run --print-xml | virsh define /dev/stdin
      log "Domain defined: $NAME (not started)"
      exit 0
    else
      "${CMD[@]}"
    fi
  fi

  if [[ $DEFINE_ONLY -eq 0 && $NO_START -eq 0 ]]; then
    log "VM created. Connect via: virt-viewer --connect qemu:///system $NAME (or check 'virsh vncdisplay $NAME')"
  fi

elif [[ "$MODE" == "cloud" ]]; then
  # Ensure the cloud image exists (download if requested)
  download_if_needed "$UBUNTU_IMAGE_URL" "$CLOUD_IMAGE"

  mkdir -p "$LIBVIRT_IMAGES_DIR"

  if [[ -e "$DISK_PATH" && $FORCE -ne 1 ]]; then
    err "Disk already exists: $DISK_PATH (use -f to overwrite)"
    exit 1
  fi
  if [[ $FORCE -eq 1 && -e "$DISK_PATH" ]]; then
    warn "Removing existing disk: $DISK_PATH"
    rm -f -- "$DISK_PATH"
  fi

  log "Copying base image to $DISK_PATH"
  if cp --reflink=auto "$CLOUD_IMAGE" "$DISK_PATH" 2>/dev/null; then
    :
  else
    cp "$CLOUD_IMAGE" "$DISK_PATH"
  fi

  log "Resizing disk to ${DISK_SIZE_GB}G"
  qemu-img resize "$DISK_PATH" "${DISK_SIZE_GB}G"

  # Prepare cloud-init seed
  SEED_ISO="$LIBVIRT_IMAGES_DIR/${NAME}-seed.iso"
  if [[ -e "$SEED_ISO" && $FORCE -ne 1 ]]; then
    err "Seed ISO already exists: $SEED_ISO (use -f to overwrite)"
    exit 1
  fi
  if [[ $FORCE -eq 1 && -e "$SEED_ISO" ]]; then
    warn "Removing existing seed ISO: $SEED_ISO"
    rm -f -- "$SEED_ISO"
  fi

  TMPDIR=$(mktemp -d)
  trap 'rm -rf "$TMPDIR"' EXIT

  # user-data
  USER_DATA="$TMPDIR/user-data"
  META_DATA="$TMPDIR/meta-data"

  # SSH key content (optional)
  SSH_KEY_CONTENT=""
  if [[ -f "$SSH_KEY_PATH" ]]; then
    SSH_KEY_CONTENT=$(cat "$SSH_KEY_PATH")
  else
    warn "SSH public key not found at $SSH_KEY_PATH; proceeding without key"
  fi

  if [[ -z "$PASSWORD" ]]; then
    if command -v openssl >/dev/null 2>&1; then
      PASSWORD=$(openssl rand -base64 12)
      warn "Generated random password for $USER_NAME: $PASSWORD"
    else
      PASSWORD="changeme"
      warn "openssl not found; using default password 'changeme' (please change on first login)"
    fi
  fi

  cat >"$USER_DATA" <<CLOUDCFG
#cloud-config
users:
  - name: ${USER_NAME}
    sudo: ALL=(ALL) NOPASSWD:ALL
    groups: sudo
    shell: /bin/bash
    lock_passwd: false
    ssh_authorized_keys:
CLOUDCFG
  if [[ -n "$SSH_KEY_CONTENT" ]]; then
    printf "      - %s\n" "$SSH_KEY_CONTENT" >>"$USER_DATA"
  fi
  cat >>"$USER_DATA" <<CLOUDCFG
chpasswd:
  list: |
    ${USER_NAME}:${PASSWORD}
  expire: false
ssh_pwauth: true
package_update: true
packages:
  - qemu-guest-agent
runcmd:
  - systemctl enable --now qemu-guest-agent || true
growpart:
  mode: auto
  devices: ['']
CLOUDCFG

  cat >"$META_DATA" <<META
instance-id: ${NAME}
local-hostname: ${NAME}
META

  if check_optional cloud-localds; then
    log "Creating cloud-init seed ISO via cloud-localds"
    cloud-localds "$SEED_ISO" "$USER_DATA" "$META_DATA"
  else
    warn "cloud-localds not found. Attempting genisoimage fallback."
    require_cmd genisoimage
    (cd "$TMPDIR" && genisoimage -output "$SEED_ISO" -volid cidata -joliet -rock user-data meta-data)
  fi

  log "Starting cloud-image VM '$NAME'"
  CMD=(
    virt-install
    --name "$NAME"
    --vcpus "$VCPUS"
    --memory "$MEM_MB"
    --disk "path=$DISK_PATH,format=qcow2"
    --disk "path=$SEED_ISO,device=cdrom"
    --network "$NETWORK"
    --import
    --noautoconsole
    --graphics none
    --wait -1
  )
  if [[ -n "$OS_VARIANT" ]]; then
    CMD+=(--os-variant "$OS_VARIANT")
  fi

  if [[ $DEFINE_ONLY -eq 1 ]]; then
    log "Defining (not starting) domain via printed XML"
    "${CMD[@]}" --dry-run --print-xml | virsh define /dev/stdin
  else
    "${CMD[@]}"
  fi

  log "VM ready. Try: virsh domifaddr $NAME --source agent (after guest agent starts)"
fi
