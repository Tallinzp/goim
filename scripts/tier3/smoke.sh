#!/usr/bin/env bash
# Tier 3 TARGET=local: smoke (Plan 6d1, #150.3), the kind foundation before
# Plan 6d2: kind.yaml's cluster, the deploy/k8s manifests, the host's Redis
# Cluster on the kind bridge, ~6k light logins from loadgen on the host
# through the Logic node's NodePort. No SLO is judged here.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh:
#   GOIM_TEST_TIER=3 make test-tier3 SCENARIO=smoke TARGET=local
#   Results: tier3-results/smoke-<timestamp>/ (result.json, stack/ with
#   logs, the pods' logs and artifacts), plus ONE file to send back:
#   tier3-results/smoke-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 6 minutes (kind create ~3 min).
#
# Checks: cluster, pinning, ready, redis_from_pods, admin_policy (#85),
# online (SMOKE_CLIENTS, default 6,000), balance (each gateway within 10%
# of the mean), ipvs (lc, 6 servers), pods (none restarted), processes.
set -uo pipefail

. "$(dirname "$0")/../tier2/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
. "$(dirname "$0")/lib.sh"
t3_init smoke
t2_require_host_setup
t2_mem_budget 10
t2_nofile 65536
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
CLIENTS=${SMOKE_CLIENTS:-6000}
t2_not_covered "rolling restart and drain (Plan 6d2)" "node loss and disruption budgets (Plan 6d3)" \
  "HPA scaling (kind has no metrics-server; Logic stays at minReplicas)"

t2_report_extra() {
  local f
  for f in "$STACK_DIR"/logs/*.log "$STACK_DIR"/logs/k8s/*.log; do
    [ -f "$f" ] || continue
    t2_section "$(basename "$f") (last 30 lines)"
    tail -n 30 "$f"
  done
  [ -f "$STACK_DIR/logs/k8s/events.txt" ] && { t2_section "events (last 40)"; tail -n 40 "$STACK_DIR/logs/k8s/events.txt"; }
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
t3_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration build $((SECONDS - t))

t=$SECONDS
t3_cluster_up >"$T2_OUT/cluster.log" 2>&1 || { echo "kind cluster failed, see $T2_OUT/cluster.log" >&2; exit 1; }
t2_duration cluster $((SECONDS - t))
ready=$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | grep -c '^True$')
proxy=$(kubectl -n kube-system get configmap kube-proxy -o jsonpath='{.data.config\.conf}')
mode=$(awk '/^mode:/ { print $2 }' <<<"$proxy") sched=$(awk '/^  scheduler:/ { print $2 }' <<<"$proxy")
t2_check cluster "$(t2_bool [ "$ready" = 8 ] && [ "$mode" = ipvs ] && [ "$sched" = lc ])" \
  "\"nodes_ready\": $ready, \"mode\": $(t2_json_str "$mode"), \"scheduler\": $(t2_json_str "$sched")"

t=$SECONDS
t3_redis_up >"$T2_OUT/redis.log" 2>&1 || { echo "Redis Cluster failed, see $T2_OUT/redis.log" >&2; exit 1; }
t3_deploy >"$T2_OUT/deploy.log" 2>&1; rc=$?
t2_duration deploy $((SECONDS - t))
pods=$(kubectl -n im get pods -o jsonpath='{range .items[*]}{.metadata.labels.app} {.metadata.name} {.spec.nodeName} {.status.podIP} {.status.containerStatuses[0].ready}{"\n"}{end}')
echo "$pods" >"$T2_OUT/pods.txt"
gw_nodes=$(kubectl get nodes -l goim.io/role=gateway -o name | sed 's|node/||' | sort | paste -sd' ' -)
logic_node=$(kubectl get nodes -l goim.io/role=logic -o jsonpath='{.items[0].metadata.name}')
gws=$(awk '$1 == "gateway" && $5 == "true"' <<<"$pods")
t2_check ready "$(t2_bool [ "$rc" = 0 ] && [ "$(awk '{ print $2 }' <<<"$gws" | sort | paste -sd' ' -)" = "gw-0 gw-1 gw-2 gw-3 gw-4 gw-5" ] &&
  [ "$(awk '{ print $3 }' <<<"$gws" | sort | paste -sd' ' -)" = "$gw_nodes" ] &&
  [ "$(awk -v n="$logic_node" '$1 == "logic" && $3 == n && $5 == "true"' <<<"$pods" | wc -l)" = 2 ])" \
  "\"rollout\": $rc, \"pods\": $(t2_json_str "$pods")"
[ "$rc" = 0 ] || exit 1

# Pinning: as planned on the containers, and as seen inside a pod.
want="" have="" n i=0
for n in $(kubectl get nodes -l goim.io/role=gateway -o name | sed 's|node/||' | sort -V); do
  want+="$n=${T3_GW_CPUS[i]} " i=$((i + 1))
done
for n in $T3_CLUSTER-control-plane $logic_node; do want+="$n=$T2_CPUS_LOGIC "; done
for n in ${want}; do n=${n%%=*}; have+="$n=$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$n") "; done
wrong=""
for n in $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
loops=$(for p in gw-0 gw-1 gw-2 gw-3 gw-4 gw-5; do kubectl -n im logs "$p" | grep -m 1 -o 'loops=[0-9]*'; done | sort | uniq -c | awk '{ print $2 "x" $1 }')
t2_check pinning "$(t2_bool [ "$want" = "$have" ] && [ -z "$wrong" ] && [ "$loops" = loops=2x6 ])" \
  "\"nodes\": $(t2_json_str "${have% }"), \"redis_off_set\": $(t2_json_str "${wrong# }"), \"gateway_loops\": $(t2_json_str "$loops")"

t3_probes >"$T2_OUT/probes.log" 2>&1 || { echo "probe pods failed, see $T2_OUT/probes.log" >&2; exit 1; }
pong=""
for j in 0 1 2 3 4 5; do
  pong+=$(t3_exec monitoring sh -c "printf 'PING\r\n' | nc -w 2 $T3_HOST $((${STACK_PORT_BASE:-17000} + 20 + j))" | tr -d '\r')
done
t2_check redis_from_pods "$(t2_bool [ "$pong" = "+PONG+PONG+PONG+PONG+PONG+PONG" ])" "\"host\": $(t2_json_str "$T3_HOST"), \"replies\": $(t2_json_str "$pong")"

gw_ip=$(awk '$2 == "gw-0" { print $4 }' <<<"$pods") logic_ip=$(awk '$1 == "logic" { print $4; exit }' <<<"$pods")
reach() { if t3_exec "$1" "${@:2}" >/dev/null 2>&1; then echo open; else echo closed; fi; }
p=$(printf '"%s": "%s", ' \
  mon_gw_admin "$(reach monitoring wget -q -T 3 -O /dev/null "http://$gw_ip:9090/readyz")" \
  mon_logic_admin "$(reach monitoring wget -q -T 3 -O /dev/null "http://$logic_ip:9090/readyz")" \
  other_gw_admin "$(reach goim-other wget -q -T 3 -O /dev/null "http://$gw_ip:9090/readyz")" \
  other_logic_admin "$(reach goim-other wget -q -T 3 -O /dev/null "http://$logic_ip:9090/readyz")" \
  other_gw_client "$(reach goim-other nc -w 3 "$gw_ip" 8000)" \
  other_gw_kick "$(reach goim-other nc -w 3 "$gw_ip" 9100)" \
  other_logic_grpc "$(reach goim-other nc -w 3 "$logic_ip" 9000)")
p="{${p%, }}"
t2_check admin_policy "$(lg_true - 'd == {"mon_gw_admin": "open", "mon_logic_admin": "open", "other_gw_admin": "closed",
  "other_logic_admin": "closed", "other_gw_client": "open", "other_gw_kick": "open", "other_logic_grpc": "open"}' <<<"$p")" "\"reach\": $p"

# Load through the NodePort on the Logic node.
t3_sources $(((CLIENTS + 4999) / 5000)) >"$T2_OUT/sources.log" 2>&1 || { echo "goim-srcaddr failed, see $T2_OUT/sources.log" >&2; exit 1; }
t2_on_exit 'lg_stop'
lg_start smoke -mode light -clients "$CLIENTS" -rate 500 -uid-prefix t3smoke -source-cidr "$T3_SOURCE_CIDR"
lg_wait_online "$CLIENTS" 120; rc=$?
t2_check online "$(t2_bool [ "$rc" = 0 ])" "\"target\": $(t2_json_str "$T3_LB"), \"stats\": $(lg_get /stats 'd')"

counts=""
for p in gw-0 gw-1 gw-2 gw-3 gw-4 gw-5; do
  ip=$(awk -v n="$p" '$2 == n { print $4 }' <<<"$pods")
  counts+="$p=$(stack_metric_in "$(t3_metrics "$ip")" goim_gateway_connections 'state="online"') "
done
b=$(t3_balance $counts)
t2_check balance "$(lg_true - "d is not None and d['ok'] and d['total'] == $CLIENTS" <<<"$b")" "\"balance\": $b"
docker exec "$logic_node" cat /proc/net/ip_vs >"$T2_OUT/ip_vs.txt" 2>&1
v=$(t3_ipvs "$T2_OUT/ip_vs.txt" "$T3_LB")
t2_check ipvs "$(lg_true - 'd is not None and d["scheduler"] == "lc" and d["servers"] == 6' <<<"$v")" "\"service\": $v"

lg_stop
after=$(kubectl -n im get pods -o jsonpath='{range .items[*]}{.metadata.name} {.status.containerStatuses[0].ready} {.status.containerStatuses[0].restartCount}{"\n"}{end}')
t2_check pods "$(t2_bool [ "$(awk '$2 == "true" && $3 == 0' <<<"$after" | wc -l)" = 8 ])" "\"pods\": $(t2_json_str "$after")"
stack_down
problems=$(stack_health)
t2_check processes "$(t2_bool [ -z "$problems" ] && [ "$LG_STATUS" = 0 ])" \
  "\"problems\": $(t2_json_str "$problems"), \"loadgen_exit\": ${LG_STATUS:-null}"
t2_finish
