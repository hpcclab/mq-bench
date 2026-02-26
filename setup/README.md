# KVM VM Setup for Multi-Node Benchmarking

This guide covers creating KVM VMs on baremetal nodes (e.g., mq-bench-1, mq-bench-2) that are accessible from other baremetal hosts via a shared network (oaas-net) using macvtap networking.

## Overview

The `kvm-create-delete.sh` script creates Ubuntu 24.04 VMs with:
- **Macvtap networking**: Places the VM directly on your oaas-net L2 segment
- **Host-side macvlan**: Enables host↔guest communication (macvtap alone blocks this)
- **Static IP support**: For both host macvlan and guest VM (useful when DHCP isn't available)
- **Cloud-init automation**: Automatic user setup, SSH keys, and network configuration

## Prerequisites

### 1. Install Required Packages

On each baremetal host where you want to create VMs:

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y \
  qemu-kvm \
  libvirt-daemon-system \
  libvirt-clients \
  virtinst \
  bridge-utils \
  cloud-image-utils \
  genisoimage \
  cpu-checker

# Verify KVM support
kvm-ok
# Should output: "KVM acceleration can be used"

# Add your user to libvirt group (optional, reduces need for sudo)
sudo usermod -aG libvirt $USER
# Log out and back in for group changes to take effect
```

### 2. Verify Libvirt Service

```bash
# Check libvirt daemon
sudo systemctl status libvirtd
sudo systemctl enable --now libvirtd

# Verify default network (not needed for macvtap, but useful for troubleshooting)
sudo virsh net-list --all
sudo virsh net-start default || true
sudo virsh net-autostart default || true
```

### 3. Identify Your Network Configuration

Determine which interface on your host is connected to oaas-net:

```bash
# List all IPv4 interfaces
ip -4 addr show

# Example output:
# 6: enp152s0np0    inet 192.168.0.182/24 ...
#                   ^^^ This is your oaas-net interface
```

Note:
- **Interface name**: e.g., `enp152s0np0`, `eno2`, `eth1`
- **CIDR**: e.g., `192.168.0.0/24`
- **Gateway**: in this lab use `192.168.0.10` on your subnet

### 4. Pick Unused IP Addresses

For each VM, you need two IP addresses in the same subnet:
- **Host macvlan IP**: For the host-side interface (e.g., `192.168.0.250`)
- **VM IP**: For the guest (e.g., `192.168.0.251`)

Check if IPs are available:

```bash
# Quick probe
ping -c 1 -W 1 192.168.0.250
ping -c 1 -W 1 192.168.0.251
# If no response → likely free
```

## Creating a VM

### Basic Usage (Auto-Detect)

The simplest command auto-detects the oaas-net interface and downloads Ubuntu 24.04 cloud image:

```bash
sudo setup/kvm-create-delete.sh \
  -n vm-oaas \
  -c 2 \
  -m 2048 \
  -M auto \
  -a 192.168.0.250/24 \
  -g 192.168.0.10 \
  -q 192.168.0.251/24 \
  -w 192.168.0.10 \
  -r 1.1.1.1,8.8.8.8 \
  -A
```

To create another vm just update:
```
Guest IP = 192.168.0.251 → inside VM

Host-side IP = 192.168.0.250 → special IP on host to reach VM
```

Set these values something within 254. -q or 192.168.0.251 is the IP which is used to ssh into the vm.

To something else, also change the name and other configs;

**What each flag does:**
- `-n vm-oaas` — VM name (unique per host)
- `-c 2` — 2 vCPUs
- `-m 2048` — 2048 MB RAM
- `-M auto` — Auto-detect oaas-net interface by CIDR (192.168.0.0/16)
- `-a 192.168.0.250/24` — Static IP for host macvlan (host↔guest access)
- `-g 192.168.0.10` — Gateway for host macvlan
- `-q 192.168.0.251/24` — Static IP for the VM inside the guest
- `-w 192.168.0.10` — Gateway for the VM
- `-r 1.1.1.1,8.8.8.8` — DNS servers for the VM (comma-separated)
- `-A` — Auto-download Ubuntu 24.04 cloud image if not present

### Explicit Interface (Recommended for Production)

If auto-detection fails or you want explicit control:

```bash
sudo setup/kvm-create-delete.sh \
  -n vm-oaas \
  -c 4 \
  -m 4096 \
  -M enp152s0np0 \
  -a 192.168.0.250/24 \
  -g 192.168.0.10 \
  -q 192.168.0.251/24 \
  -w 192.168.0.10 \
  -r 1.1.1.1,8.8.8.8 \
  -A
```

Replace `enp152s0np0` with your actual oaas-net interface name.

### With SSH Key (Passwordless Login)

To add your SSH public key to the VM:

```bash
sudo setup/kvm-create-delete.sh \
  -n vm-oaas \
  -c 2 \
  -m 2048 \
  -M auto \
  -k ~/.ssh/id_rsa.pub \
  -a 192.168.0.250/24 \
  -g 192.168.0.10 \
  -q 192.168.0.251/24 \
  -w 192.168.0.10 \
  -r 1.1.1.1,8.8.8.8 \
  -A
```

**Note:** The script runs as root (via sudo), so it looks for keys in `/root/.ssh/` by default. Use `-k` to specify your user's key path.

### Advanced: DHCP for Host Macvlan

If your oaas-net provides DHCP for additional interfaces, you can try DHCP for the host macvlan:

```bash
sudo setup/kvm-create-delete.sh \
  -n vm-oaas \
  -c 2 \
  -m 2048 \
  -M auto \
  -H \
  -q 192.168.0.251/24 \
  -w 192.168.0.10 \
  -r 1.1.1.1,8.8.8.8 \
  -A
```

The `-H` flag attempts DHCP with a 12-second timeout. If no lease is obtained, the script continues with a warning.

## After Creation

### 1. Verify VM Status

```bash
# Check if VM is running
sudo virsh list --all

# View VM details
sudo virsh dominfo vm-oaas

# Check host macvlan interface
ip addr show vm-oaas-macvlan
```

### 2. Get VM IP (if using DHCP inside guest)

If you didn't use `-q` for static guest IP:

```bash
# Wait ~30 seconds for guest agent to start, then:
sudo virsh domifaddr vm-oaas --source agent
```

### 3. Test Connectivity

**From the host (mq-bench-1):**

```bash
# Ping the VM
ping -c 3 192.168.0.251

# SSH into the VM
ssh ubuntu@192.168.0.251
```

**From another baremetal node (mq-bench-2):**

```bash
# Direct ping over oaas-net
ping -c 3 192.168.0.251

# SSH from another node
ssh ubuntu@192.168.0.251
```

### 4. Retrieve Generated Password

If you didn't set a password and didn't add an SSH key, the script generates a random password:

```bash
# Look for this line in the creation output:
# [WARN] Generated random password for ubuntu: <password>
```

Or access via console:

```bash
sudo virsh console vm-oaas
# Press Enter, then login with username 'ubuntu' and the generated password
# Ctrl+] to exit console
```

## Managing VMs

### Start/Stop/Restart

```bash
# Start
sudo virsh start vm-oaas

# Shutdown gracefully
sudo virsh shutdown vm-oaas

# Force stop
sudo virsh destroy vm-oaas

# Reboot
sudo virsh reboot vm-oaas

# Autostart on host boot
sudo virsh autostart vm-oaas
```

### Delete a VM

To completely remove a VM and its disk/seed:

```bash
sudo setup/kvm-create-delete.sh -n vm-oaas -X -f
```

This will:
- Stop the VM if running
- Undefine the domain
- Remove disk and cloud-init seed ISO
- Delete the host-side macvlan interface

## Troubleshooting

### Auto-Detection Fails

If `-M auto` doesn't find your oaas-net interface:

1. **Check the CIDR:** Default is `192.168.0.0/16`. Change with `-C`:
   ```bash
   sudo setup/kvm-create-delete.sh -n vm-oaas -M auto -C 10.0.0.0/8 ...
   ```

2. **Use explicit interface:**
   ```bash
   sudo setup/kvm-create-delete.sh -n vm-oaas -M enp152s0np0 ...
   ```

### VM Not Reachable from Host

- **Check host macvlan is up:**
  ```bash
  ip addr show vm-oaas-macvlan
  ```

- **Check VM IP inside guest:**
  ```bash
  sudo virsh console vm-oaas
  # Login, then: ip addr show eth0
  ```

- **Verify guest networking:**
  ```bash
  ssh ubuntu@192.168.0.251
  ip addr
  ip route
  cat /etc/netplan/50-cloud-init.yaml
  ```

### VM Not Reachable from Other Baremetal

- **Verify L2 connectivity:**
  - Check cables/switches
  - Ensure both hosts are on the same VLAN/subnet

- **Check firewall rules:**
  ```bash
  # On the baremetal host running the VM
  sudo iptables -L -v -n
  
  # Inside the VM
  ssh ubuntu@192.168.0.251
  sudo ufw status
  ```

- **Test from VM to other baremetal:**
  ```bash
  ssh ubuntu@192.168.0.251
  ping 192.168.0.182  # IP of mq-bench-1
  ```

### DHCP Hangs (Historical Issue)

**Fixed** — The script now skips DHCP by default. If you use `-H` and it times out:
- The script continues after 12 seconds
- Use `-a` for static host macvlan IP instead

### Disk Already Exists Error

If you see:
```
[ERROR] Disk already exists: /var/lib/libvirt/images/vm-oaas.qcow2 (use -f to overwrite)
```

Either:
- Delete the old VM first: `sudo setup/kvm-create-delete.sh -n vm-oaas -X -f`
- Or add `-f` to force overwrite: `sudo setup/kvm-create-delete.sh ... -f`

### Invalid Characters in Model Name

**Fixed** — This was a network parameter formatting bug. Ensure you're using the latest version of the script.

## Multiple VMs on the Same Host

You can create multiple VMs, each needs:
- Unique name: `-n vm-oaas-1`, `-n vm-oaas-2`
- Unique host macvlan IP: `-a 192.168.0.250/24`, `-a 192.168.0.252/24`
- Unique VM IP: `-q 192.168.0.251/24`, `-q 192.168.0.253/24`

Example:

```bash
# VM 1
sudo setup/kvm-create-delete.sh \
  -n vm-1 -c 2 -m 2048 -M auto \
  -a 192.168.0.250/24 -g 192.168.0.10 \
  -q 192.168.0.251/24 -w 192.168.0.10 -r 1.1.1.1 -A

# VM 2
sudo setup/kvm-create-delete.sh \
  -n vm-2 -c 2 -m 2048 -M auto \
  -a 192.168.0.252/24 -g 192.168.0.10 \
  -q 192.168.0.253/24 -w 192.168.0.10 -r 1.1.1.1 -A
```

## Advanced Options

### Custom Disk Size and Resources

```bash
sudo setup/kvm-create-delete.sh \
  -n vm-large \
  -c 8 \
  -m 16384 \
  -s 100 \
  -M auto \
  -a 192.168.0.250/24 -g 192.168.0.10 \
  -q 192.168.0.251/24 -w 192.168.0.10 -r 1.1.1.1 -A
```

- `-s 100` — 100 GB disk

### Custom User and Password

```bash
sudo setup/kvm-create-delete.sh \
  -n vm-oaas \
  -u myuser \
  -p mypassword \
  -M auto \
  -a 192.168.0.250/24 -g 192.168.0.10 \
  -q 192.168.0.251/24 -w 192.168.0.10 -r 1.1.1.1 -A
```

### Use a Different Cloud Image

```bash
# Download a specific image
sudo curl -L -o /var/lib/libvirt/images/ubuntu-22.04-server-cloudimg-amd64.img \
  https://cloud-images.ubuntu.com/releases/22.04/release/ubuntu-22.04-server-cloudimg-amd64.img

# Use it
sudo setup/kvm-create-delete.sh \
  -n vm-ubuntu22 \
  -I /var/lib/libvirt/images/ubuntu-22.04-server-cloudimg-amd64.img \
  -o ubuntu22.04 \
  -M auto \
  -a 192.168.0.250/24 -g 192.168.0.10 \
  -q 192.168.0.251/24 -w 192.168.0.10 -r 1.1.1.1
```

### Define Only (Don't Start)

```bash
sudo setup/kvm-create-delete.sh \
  -n vm-oaas \
  -D \
  -M auto \
  -a 192.168.0.250/24 -g 192.168.0.10 \
  -q 192.168.0.251/24 -w 192.168.0.10 -r 1.1.1.1 -A

# Later, start manually:
sudo virsh start vm-oaas
```

## Complete Reference

Run the script with `-h` to see all options:

```bash
./setup/kvm-create-delete.sh -h
```

### Key Flags Summary

| Flag | Description | Example |
|------|-------------|---------|
| `-n` | VM name (required) | `-n vm-oaas` |
| `-c` | vCPUs | `-c 4` |
| `-m` | Memory (MB) | `-m 4096` |
| `-s` | Disk size (GB) | `-s 50` |
| `-M` | Macvtap interface (auto or explicit) | `-M auto` or `-M enp152s0np0` |
| `-C` | CIDR for auto-detection | `-C 192.168.0.0/24` |
| `-a` | Static IP for host macvlan | `-a 192.168.0.250/24` |
| `-g` | Gateway for host macvlan | `-g 192.168.0.10` |
| `-H` | Try DHCP on host macvlan (12s timeout) | `-H` |
| `-q` | Static IP for VM (cloud-init) | `-q 192.168.0.251/24` |
| `-w` | Gateway for VM | `-w 192.168.0.10` |
| `-r` | DNS for VM (comma-separated) | `-r 1.1.1.1,8.8.8.8` |
| `-u` | Username in VM | `-u ubuntu` |
| `-k` | SSH public key path | `-k ~/.ssh/id_rsa.pub` |
| `-p` | Password for VM user | `-p mypassword` |
| `-A` | Auto-download Ubuntu 24.04 cloud image | `-A` |
| `-f` | Force overwrite existing disk/seed | `-f` |
| `-D` | Define only, don't start | `-D` |
| `-X` | Delete VM and files | `-X` |

## Integration with mq-bench

Once your VMs are created and accessible from all baremetal nodes:

1. **Set up benchmarking tools inside VMs:**
   ```bash
   ssh ubuntu@192.168.0.251
   
   # Clone mq-bench or install benchmark agents
   git clone https://github.com/hpcclab/mq-bench.git
   cd mq-bench
   # ... follow benchmark setup
   ```

2. **Run distributed benchmarks:**
   - Publishers on mq-bench-1
   - Subscribers on mq-bench-2 or VMs
   - Broker on VM or separate node

3. **Network topology examples:**
   - **Test 1:** Broker on VM, pub/sub on baremetal
   - **Test 2:** All components on VMs across different hosts
   - **Test 3:** Mixed: pub on baremetal, broker on VM1, sub on VM2

## Security Notes

- VMs are on your production oaas-net; apply appropriate firewall rules
- Change default passwords immediately after creation
- Use SSH keys instead of passwords in production
- Consider creating a dedicated VLAN/subnet for benchmark VMs if possible

## Performance Considerations

- **CPU:** Pass-through mode provides best performance; use nested virtualization carefully
- **Network:** Macvtap offers near-native performance; avoid NAT/bridging overhead
- **Disk:** Use qcow2 with allocation=falloc or raw format for I/O-intensive workloads
- **Memory:** Don't overcommit if running latency-sensitive benchmarks

## Quick Start Checklist

- [ ] Install KVM/libvirt packages
- [ ] Identify oaas-net interface and subnet
- [ ] Pick two unused IPs (host macvlan + VM)
- [ ] Copy your SSH public key to a known location
- [ ] Run creation command with `-M auto -a -q -w -r -k -A`
- [ ] Verify: `ping <vm-ip>` from host and other baremetal
- [ ] SSH into VM: `ssh ubuntu@<vm-ip>`
- [ ] Deploy benchmark tools

## Example: Full Setup for Two Baremetal Hosts

**On mq-bench-1 (192.168.0.182):**

```bash
sudo setup/kvm-create-delete.sh \
  -n bench-vm-1 \
  -c 4 -m 8192 -s 50 \
  -M enp152s0np0 \
  -a 192.168.0.200/24 -g 192.168.0.10 \
  -q 192.168.0.201/24 -w 192.168.0.10 -r 1.1.1.1 \
  -k /home/cc/.ssh/id_rsa.pub \
  -A
```

**On mq-bench-2:**

```bash
sudo setup/kvm-create-delete.sh \
  -n bench-vm-2 \
  -c 4 -m 8192 -s 50 \
  -M <interface-name> \
  -a 192.168.0.202/24 -g 192.168.0.10 \
  -q 192.168.0.203/24 -w 192.168.0.10 -r 1.1.1.1 \
  -k /home/cc/.ssh/id_rsa.pub \
  -A
```

**Test cross-node connectivity:**

```bash
# From mq-bench-1
ping 192.168.0.203

# From mq-bench-2
ping 192.168.0.201

# From bench-vm-1
ssh ubuntu@192.168.0.201
ping 192.168.0.203
```

All four endpoints (2 baremetal + 2 VMs) should be able to reach each other.

---

**For questions or issues, check:**
- Script help: `./setup/kvm-create-delete.sh -h`
- Libvirt logs: `sudo journalctl -u libvirtd -f`
- VM console: `sudo virsh console <vm-name>`
