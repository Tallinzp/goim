#!/usr/bin/env bash
# One-time root setup of the Tier 2 host (choice #68). Run it by hand, once,
# and again whenever sysctl.conf, modules.conf or goim-netdrop change (the
# Tier 2 guard says so):
#
#   sudo scripts/tier2/host-setup.sh
#
# It is idempotent and installs:
#   /etc/sysctl.d/90-goim-tier2.conf       from scripts/tier2/sysctl.conf, applied now; a
#                                          key the host already has higher in every
#                                          field is left out, so it is never lowered
#                                          (fs.nr_open: systemd raises it at boot)
#   /etc/modules-load.d/goim-tier2.conf    from scripts/tier2/modules.conf, loaded now
#   /usr/local/sbin/goim-netdrop           root-owned, 0755 (the b2 DROP rule only)
#   /etc/sudoers.d/goim-netdrop            $SUDO_USER may run goim-netdrop, and only
#                                          it, without a password (checked with visudo)
#   /etc/security/limits.d/90-goim-tier2.conf  and
#   /etc/systemd/system/user@UID.service.d/90-goim-tier2.conf
#                                          $SUDO_USER's nofile hard limit 2,097,152,
#                                          soft 1024 (the runs raise it, t2_nofile)
#                                          (reconnect's ghost phase, #136.5): new
#                                          sessions only, and a running user manager
#                                          keeps its old limit (the script says so)
#   NOTRACK rules for ports 17000-17199 on lo (goim-netdrop notrack, #123): not
#                                          kept across a reboot, so run it again
#                                          after one (the Tier 2 guard says so)
# Everything else the runs need (nofile up to the hard limit, loopback
# source addresses, CPU pinning) needs no root.
#
# Test mode, no root, nothing applied: host-setup.sh --root DIR --user NAME
# [--uid N] writes the same files under DIR, reading DIR/proc/sys.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root="" user=${SUDO_USER:-} uid=""
while [ $# -gt 0 ]; do
  case $1 in
    --root) root=$2; shift 2 ;;
    --user) user=$2; shift 2 ;;
    --uid) uid=$2; shift 2 ;;
    *) echo "usage: sudo $0   (test mode: $0 --root DIR --user NAME [--uid N])" >&2; exit 2 ;;
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
[ -n "$uid" ] || uid=$(id -u "$user" 2>/dev/null) || { echo "host-setup: no user '$user'" >&2; exit 2; }

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

# sysctl.conf without the keys the host has higher in every field.
while IFS= read -r line; do
  case $line in
    '' | '#'*) echo "$line"; continue ;;
  esac
  key=${line%%=*} key=${key// /} want=${line#*=}
  have=$(tr -s '\t ' '  ' 2>/dev/null <"$root/proc/sys/${key//.//}") || have=""
  if [ -n "$have" ] && awk -v w="$want" -v h="$have" 'BEGIN {
    n = split(w, a, " "); if (split(h, b, " ") != n) exit 1
    for (i = 1; i <= n; i++) { if (b[i] + 0 < a[i] + 0) exit 1; if (b[i] + 0 > a[i] + 0) hi = 1 }
    exit !hi }'; then
    echo "# $key: the host's $have is higher, not lowered"
  else
    echo "$line"
  fi
done <"$here/sysctl.conf" >"$tmp/sysctl"
put "$tmp/sysctl" /etc/sysctl.d/90-goim-tier2.conf 0644
put "$here/modules.conf" /etc/modules-load.d/goim-tier2.conf 0644
put "$here/goim-netdrop" /usr/local/sbin/goim-netdrop 0755
echo "$user ALL=(root) NOPASSWD: /usr/local/sbin/goim-netdrop" >"$tmp/sudoers"
if [ -z "$root" ]; then
  visudo -cqf "$tmp/sudoers" || { echo "host-setup: visudo rejected the sudoers line" >&2; exit 1; }
fi
put "$tmp/sudoers" /etc/sudoers.d/goim-netdrop 0440
echo "$user hard nofile 2097152" >"$tmp/limits"
put "$tmp/limits" /etc/security/limits.d/90-goim-tier2.conf 0644
# $user's manager only, with the default soft limit (review M5).
printf '[Service]\nLimitNOFILE=1024:2097152\n' >"$tmp/user-service"
put "$tmp/user-service" "/etc/systemd/system/user@$uid.service.d/90-goim-tier2.conf" 0644
if [ -f "$root/etc/systemd/system/user@.service.d/90-goim-tier2.conf" ]; then
  rm -f "$root/etc/systemd/system/user@.service.d/90-goim-tier2.conf"
  rmdir "$root/etc/systemd/system/user@.service.d" 2>/dev/null || true
  echo "removed: /etc/systemd/system/user@.service.d/90-goim-tier2.conf (every user's)"
fi

if [ -n "$root" ]; then
  echo "test mode: nothing applied"
  exit 0
fi
grep -vE '^[[:space:]]*(#|$)' "$here/modules.conf" | while read -r m; do modprobe "$m"; done
sysctl -q -p /etc/sysctl.d/90-goim-tier2.conf
systemctl daemon-reload
# daemon-reload does not restart a running user manager (review M7).
mpid=$(systemctl show -p MainPID --value "user@$uid.service" 2>/dev/null || echo 0)
if [ "${mpid:-0}" != 0 ] && ! grep -Eq '^Max open files +[0-9]+ +(2097152|unlimited)' "/proc/$mpid/limits" 2>/dev/null; then
  echo "host-setup: $user's running user manager keeps its old nofile limit: log out of every session of $user (or: loginctl terminate-user $user), or raise one shell's with prlimit (below)"
fi
if [ -x /usr/sbin/iptables ]; then
  /usr/local/sbin/goim-netdrop notrack
  echo "applied: NOTRACK for ports 17000-17199 on lo"
else
  echo "host-setup: iptables is not installed; goim-netdrop needs it (apt install iptables), then run this again" >&2
  exit 1
fi
echo "host-setup: done. Check: sudo -n /usr/local/sbin/goim-netdrop list; sudo -n /usr/local/sbin/goim-netdrop notrack-check"
echo "host-setup: the nofile hard limit 2,097,152 applies to new sessions: log out and in again (ulimit -Hn shows it), or raise one shell's: sudo prlimit --pid \$\$ --nofile=1048576:2097152"
echo "host-setup: NOTRACKed packets are UNTRACKED, not ESTABLISHED: check that iptables -S INPUT, iptables -S OUTPUT and nft list ruleset accept lo before any conntrack-state rule"
