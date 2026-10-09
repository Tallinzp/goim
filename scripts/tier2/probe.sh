#!/usr/bin/env bash
# Tier 2 host probe (Plan 6 prep, choices #65–#67): reports what the Tier 2
# host offers for the load tests (6a–6c) and for Tier 3 TARGET=local on kind
# (6d). Read-only: it changes no setting, installs nothing, and never prompts
# (sudo -n only checks whether passwordless sudo works).
#
# Usage (on the Tier 2 host, from the repo root): make test-tier2 SCENARIO=probe
#   Writes ONE file to send back: tier2-results/probe-<timestamp>.txt
#   Exit code: always 0 (it reports, it does not judge).
set -u

cd "$(git rev-parse --show-toplevel)"
out="$PWD/tier2-results/probe-$(date +%Y%m%d-%H%M%S).txt"
mkdir -p "$(dirname "$out")"

section() { printf '\n===== %s =====\n' "$1"; }
have() { command -v "$1" >/dev/null 2>&1; }
# show <label> <command...>: the command's output, or why there is none.
show() {
  local label=$1; shift
  if have "$1"; then
    printf '%s: ' "$label"; "$@" 2>&1 | head -n 40
  else
    printf '%s: (%s not installed)\n' "$label" "$1"
  fi
}
sysctl_of() { printf '%s = %s\n' "$1" "$(cat "/proc/sys/${1//.//}" 2>/dev/null || echo '(absent)')"; }

{
  echo "goim Tier 2 host probe"
  section "host"
  echo "date:     $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "commit:   $(git rev-parse HEAD) ($(git rev-parse --abbrev-ref HEAD))"
  echo "kernel:   $(uname -srm)"
  echo "distro:   $(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME")"
  echo "cores:    $(getconf _NPROCESSORS_ONLN)"
  show "lscpu" sh -c "lscpu | grep -E '^(Model name|Socket|Core|Thread|NUMA node)'"
  show "memory" free -g
  show "disk (repo)" df -h .
  show "numactl" numactl --hardware

  section "privileges (load tests: sysctl, ulimit, conntrack, iptables DROP for b2, IP aliases, cgroups)"
  echo "user:     $(id)"
  if sudo -n true 2>/dev/null; then echo "sudo:     passwordless"; else echo "sudo:     needs a password or is not allowed"; fi

  section "limits and sysctls"
  echo "ulimit -n soft/hard: $(ulimit -Sn) / $(ulimit -Hn)"
  for k in fs.nr_open fs.file-max net.core.somaxconn net.ipv4.ip_local_port_range \
           net.ipv4.tcp_tw_reuse net.ipv4.tcp_max_syn_backlog net.ipv4.tcp_mem \
           net.ipv4.tcp_rmem net.ipv4.tcp_wmem net.netfilter.nf_conntrack_max \
           net.nf_conntrack_max vm.max_map_count vm.overcommit_memory; do
    sysctl_of "$k"
  done
  show "conntrack modules" sh -c "lsmod | grep -E '^nf_conntrack|^ip_vs' || echo '(none loaded)'"

  section "cgroups and CPU pinning"
  echo "cgroup fs: $(stat -fc %T /sys/fs/cgroup 2>/dev/null) (cgroup2fs = v2)"
  echo "controllers: $(cat /sys/fs/cgroup/cgroup.controllers 2>/dev/null || echo '(not v2)')"
  echo "subtree_control: $(cat /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null)"
  show "systemd-run" systemd-run --version
  show "taskset" sh -c "taskset -p $$"

  section "network (≥ 10 source IPs for loadgen at 600k)"
  show "addresses" ip -br addr
  show "routes" ip route
  show "iptables" iptables --version
  show "nft" nft --version

  section "tools"
  show "go" go version
  show "gcc" sh -c "gcc --version | head -1"
  show "make" sh -c "make --version | head -1"
  show "redis-server (PATH)" redis-server --version
  echo "redis-server (.cache): $(.cache/redis/bin/redis-server --version 2>/dev/null || echo '(not built)')"
  show "perf" perf --version

  section "containers and Kubernetes (Tier 3 TARGET=local on kind, choice #67)"
  show "docker" docker version --format '{{.Server.Version}} (server), {{.Client.Version}} (client)'
  if have docker; then
    if docker info >/dev/null 2>&1; then echo "docker without sudo: yes"; else echo "docker without sudo: no ($(docker info 2>&1 | tail -1))"; fi
    show "docker cgroup" docker info --format '{{.CgroupDriver}} / cgroup v{{.CgroupVersion}}'
    show "docker containers" docker ps -a --format '{{.Names}} {{.Image}} {{.Status}}'
  fi
  show "kind" kind version
  show "kind clusters" kind get clusters
  show "kubectl" kubectl version --client
  show "ipvsadm" ipvsadm --version
  echo "ip_vs module available: $(modinfo ip_vs >/dev/null 2>&1 && echo yes || echo no)"
  echo "inotify max_user_instances/watches (kind needs headroom): $(cat /proc/sys/fs/inotify/max_user_instances 2>/dev/null) / $(cat /proc/sys/fs/inotify/max_user_watches 2>/dev/null)"
} >"$out" 2>&1

echo "probe: $out"
