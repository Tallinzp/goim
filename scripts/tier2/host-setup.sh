#!/usr/bin/env bash
# One-time root setup of the Tier 2 host (choice #68). Run it by hand, once,
# and again whenever sysctl.conf, modules.conf or goim-netdrop change (the
# Tier 2 guard says so):
#
#   sudo scripts/tier2/host-setup.sh
#
# It is idempotent and installs:
#   /etc/sysctl.d/90-goim-tier2.conf       from scripts/tier2/sysctl.conf, applied now
#   /etc/modules-load.d/goim-tier2.conf    from scripts/tier2/modules.conf, loaded now
#   /usr/local/sbin/goim-netdrop           root-owned, 0755 (the b2 DROP rule only)
#   /etc/sudoers.d/goim-netdrop            $SUDO_USER may run goim-netdrop, and only
#                                          it, without a password (checked with visudo)
# Everything else the runs need (nofile up to the hard limit, loopback
# source addresses, CPU pinning) needs no root.
#
# Test mode, no root, nothing applied: host-setup.sh --root DIR --user NAME
# writes the same files under DIR.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root="" user=${SUDO_USER:-}
while [ $# -gt 0 ]; do
  case $1 in
    --root) root=$2; shift 2 ;;
    --user) user=$2; shift 2 ;;
    *) echo "usage: sudo $0   (test mode: $0 --root DIR --user NAME)" >&2; exit 2 ;;
  esac
done
if [ -z "$root" ] && [ "$(id -u)" != 0 ]; then
  echo "host-setup: run it as root: sudo $0" >&2
  exit 2
fi
case $user in
  ''|root) echo "host-setup: run it through sudo from the Tier 2 user's account (SUDO_USER is '$user')" >&2; exit 2 ;;
  *[!A-Za-z0-9._-]*) echo "host-setup: unexpected user name '$user'" >&2; exit 2 ;;
esac

# put SRC DEST MODE: installs SRC as DEST unless it is already identical.
put() {
  local dest=$root$2
  mkdir -p "$(dirname "$dest")"
  if cmp -s "$1" "$dest"; then
    echo "unchanged: $2"
  else
    install -m "$3" "$1" "$dest"
    [ -n "$root" ] || chown root:root "$dest"
    echo "installed: $2"
  fi
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

put "$here/sysctl.conf" /etc/sysctl.d/90-goim-tier2.conf 0644
put "$here/modules.conf" /etc/modules-load.d/goim-tier2.conf 0644
put "$here/goim-netdrop" /usr/local/sbin/goim-netdrop 0755
echo "$user ALL=(root) NOPASSWD: /usr/local/sbin/goim-netdrop" >"$tmp/sudoers"
if [ -z "$root" ]; then
  visudo -cqf "$tmp/sudoers" || { echo "host-setup: visudo rejected the sudoers line" >&2; exit 1; }
fi
put "$tmp/sudoers" /etc/sudoers.d/goim-netdrop 0440

if [ -n "$root" ]; then
  echo "test mode: nothing applied"
  exit 0
fi
grep -vE '^[[:space:]]*(#|$)' "$here/modules.conf" | while read -r m; do modprobe "$m"; done
sysctl -q -p /etc/sysctl.d/90-goim-tier2.conf
command -v iptables >/dev/null || echo "host-setup: warning: iptables is not installed; goim-netdrop needs it (apt install iptables)" >&2
echo "host-setup: done. Check: sudo -n /usr/local/sbin/goim-netdrop list"
