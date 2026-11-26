# Plan 6d1: Tier 3 Foundation on kind Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run gateway and Logic on Kubernetes for Tier 3 `TARGET=local`: the `deploy/k8s` manifests with a check that runs on any machine, a kind cluster on the Tier 2 host with kube-proxy IPVS least connections, and a `smoke` scenario that proves the foundation before Plan 6d2's rolling restart.

**Architecture:** Four tasks:
- **Manifests** (Task 1): `deploy/k8s/base` (gateway StatefulSet, Logic Deployment with HPA, both PDBs, the Services, the admin-port NetworkPolicies), `overlays/local` (the kind layout), a `FROM scratch` Dockerfile, and a Go test that decodes every manifest strictly into `k8s.io/api` types.
- **Host side** (Task 2): loadgen `-source-cidr`; the root helper `goim-srcaddr` (aliases on the kind bridge) installed by `host-setup.sh`, which also loads `ip_vs_wlc`; the Redis Cluster bound, announced and joined on a host address; `lib.sh` at Tier 3 (`T2_TIER=3`).
- **Tier 3 scripts** (Task 3): pinned kind and kubectl (`scripts/k8s-tools.sh`), `scripts/tier3/lib.sh` (cluster, pinning, image, deploy, probes), `kind.yaml`, the `smoke` scenario, `make test-tier3`.
- **Dry run, host run, record** (Task 4).

Only gateway and Logic run in kind. Redis and loadgen stay on the host (#149.2). loadgen dials the client NodePort on the Logic node; IPVS picks the gateway.

**Tech Stack:** Go 1.27.1, `k8s.io/api` v0.37.0, `sigs.k8s.io/yaml` v1.6.0, kustomize (in kubectl v1.37.0), kind v0.33.0 with `kindest/node:v1.37.0`, bash, python3, the pinned `redis-server` 7.4.11.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`: *Deployment requirements* (l.65–82: least connections, PDBs, `safe-to-evict`), *Gateway* → *Readiness* and *Graceful drain* (l.232–235: grace period longer than the drain, `publishNotReadyAddresses`), *Verification* → *Test execution model* (l.468–490: Tier 3 target, automation contract, host guard, `not_covered`). Nothing here changes the spec.

Inputs: the roadmap's **#149** (6d split and shape) and **#150** (6d1 shape), both decided by the user 2026-11-25 and followed here; #67 (kind, IPVS), #68 (root helper pattern), #69 (source layout), #71 (CPU split), #85 (admin-port policy), P5bI-3 (05b1 plan l.164: the `-faults`/`-clock-offset` check), the host line (roadmap ~l.148).

**Premises checked in the prototype** (of #150):
- **kubelet probes pass the policy:** kindnet (kind v0.33.0) polices with kube-network-policies `f67f0fb35e2b`, which accepts the node's root user first (`meta skuid 0 accept`). It is also **fail-open**, so the smoke checks the admin port is *closed* from another namespace, not only open from `monitoring`.
- **kustomize drops a Secret data key named `1`** (kubectl v1.37.0): an error in `secretGenerator`, silently lost as a resource. The gateway reads key files by decimal `key_id`, so `noise-keys` is made with `kubectl create secret`. kustomize also refuses an absolute resource path (the run overlay uses a relative one).
- **IPVS `rr` would balance the smoke's counts as well as `lc`:** the smoke also reads the scheduler from kube-proxy's ConfigMap and `/proc/net/ip_vs`.
- kind decodes `kind.yaml` strictly (anchors included) before it needs Docker. Static builds, Logic Ready on the fallback JWKS alone, the Secret volume layout, and Redis announced on a second address all work (Task 4 Step 1).

**Implementation choices this plan makes (proposed; the user decides them before execution, as #151):**
1. **Sources** `198.18.0.0/24` (RFC 2544 benchmark block, fixed in `goim-srcaddr`) as `/32` aliases on the kind bridge. *Alt:* addresses in the kind subnet (Docker picks it per host).
2. **Logic on the `jwks-fallback` ConfigMap only** (`JWKS_URL` empty, no devtoken). *Alt:* devtoken on the bridge address as JWKS URL.
3. **Probes:** `busybox:1.37.0` pods in `monitoring` (metrics, policy) and `goim-other`. *Alt:* the API server proxy (the policy would have to admit node addresses).
4. **Run values** in a generated run kustomization: ConfigMaps `goim-config` (`REDIS_ADDRS`, `JWKS_URL`, `MAX_CONNS` = 250k × `TIER3_SCALE`) and `jwks-fallback`; the Secret by kubectl. *Alt:* Secret key `noise-1` mapped to file `1` by volume `items`.
5. **Placeholder sizing:** Logic replicas = `minReplicas` = PDB `minAvailable` = 2, HPA max 8 at 70%; requests gateway 1 CPU / 1Gi, Logic 500m / 512Mi, **no limits** (a CPU limit sets GOMAXPROCS); grace 90s; gateway nodes tainted for gateways; NodePort 30800. *Alt:* `minReplicas` 1 (Tier 2's one Logic); no taint.
6. **Tier 3 reuses `scripts/tier2/lib.sh`** via `T2_TIER=3` (same guard, `tier3-results/`, `"tier": 3`). *Alt:* a separate Tier 3 lib.

**Branch:** `feat/plan6d1` (from `main` `a91e416`; #150 is `9992340`). Execute from its head, this plan's own commit or later.

**Size:** 4 tasks; code ~525 (Task 1), +205/−29 (Task 2), ~525 lines (Task 3). The prototype `proto-6d1` (`40b546a`, `5b9589a`, `815c97a` on `9992340`) ran every dev step; its diff but `go.mod`/`go.sum` (written by Task 1's `go get`) is the patch below, one line of context. On the dev machine (2 cores, 2GB, no Docker, no root): `make test`, `go vet ./...` green; `make test-scripts` green (682 ok) but for a known flake (`lib_test.sh` "two named windows pass" fails when the machine swaps; unchanged `main` too, 1 run in 3); 10 manifest mutations caught; `scripts/tier3/lib_test.sh` 26 ok; the pod-shaped dry run as expected. **Replayed 2026-11-25** in a fresh worktree from `feat/plan6d1`: Tasks 1–3 as written give `proto-6d1`'s tree (empty `git diff`). Everything in kind is unverified until the user's host run (Task 4 Step 2).

## Global Constraints

- The dev machine never runs kind or Docker; `make test`, `go vet ./...`, `make test-scripts` stay green there (`test-scripts` runs `make k8s-tools`, a one-time ~70MB download).
- No change to production Go code, the wire, Lua, `stack.sh` or `loadgen.sh`; loadgen gains one flag.
- Pins: kind v0.33.0 (SHA-256 `aee61515…`), kubectl v1.37.0 (`6129359f…`), `kindest/node:v1.37.0@sha256:a1ed56cf…`, `k8s.io/api` v0.37.0, `sigs.k8s.io/yaml` v1.6.0, `busybox:1.37.0`.
- No manifest file under `deploy/k8s` contains `-faults` or `-clock-offset`, not even in a comment (P5bI-3).
- CPU sets (#150.1): gateway node k on `4+k,16+k`; control plane, Logic node and Redis on `10,11,22,23`; loadgen on `0-3,12-15`.
- The smoke is `GOIM_TEST_TIER=3 make test-tier3 SCENARIO=smoke TARGET=local` at its defaults: 6,000 light clients at 500/s, `TIER3_SCALE` 0.15 (cap 37,500), NodePort 30800, 2 source aliases.
- result.json: `"tier": 3`, `"target": "local"`, `"scale"`, `not_covered` (the spec's three local gaps, plus rolling restart, node loss, HPA scaling).

## Review Focus

1. **busybox `nc` as a port test** (not run here): `admin_policy` expects `nc -w 3` to the client, Kick and Logic gRPC ports from `goim-other` to read open. If only those fields fail while the `wget` fields are right, the tool is wrong, not the policy.
2. **A host firewall between pods and Redis:** pods open new connections to the host's bridge address; a desktop firewall (ufw) drops them. No gateway gets its first lease: `ready` fails, `redis_from_pods` shows no `+PONG`.
3. **The gateway's limits self-check in a pod:** it needs `RLIMIT_NOFILE` ≥ 41,250 and `nf_conntrack_max` ≥ 75,000 as the pod sees them; otherwise NotReady with the limit in its log (`ready`).
4. **Pinning after the nodes start:** our pods start after `docker update`, so GOMAXPROCS is 2; `pinning` reads `loops=2` from each gateway. kindnet and kube-proxy share each gateway core.
5. **IPVS state is per node:** least connections sees only the connections its node forwards; every loadgen dial goes to the Logic node, so its view is global (`balance`, `ipvs`).

Each line's test is a smoke check, read in Task 4 Step 3.

## File Structure

- `deploy/k8s/base/{kustomization,namespace,gateway,logic,netpol}.yaml`, `deploy/k8s/overlays/local/{kustomization,gateway,logic}.yaml`, `deploy/k8s/Dockerfile`, `deploy/k8s/manifests_test.go` (new; Task 1); `go.mod`, `go.sum`.
- `internal/loadgen/{balancer,runner}.go` and their tests, `cmd/loadgen/main.go`: `SourcesFrom`, `Config.SourceCIDR`, `-source-cidr` (Task 2).
- `scripts/tier2/goim-srcaddr` (new), `host-setup.sh`, `modules.conf`, `lib.sh`, `lib_test.sh`; `scripts/redis-cluster.sh`, `scripts/stack_cluster_test.sh` (Task 2).
- `scripts/k8s-tools.sh`, `scripts/tier3/{lib.sh,lib_test.sh,kind.yaml,smoke.sh}` (new), `Makefile`, `.gitignore` (Task 3).
- This plan's *Execution Notes*, the roadmap's Plan 6 row (Task 4).

---

### Task 1: Manifests, image and the manifest check

**Files:**
- Create: `deploy/k8s/` (10 files above)
- Modify: `go.mod`, `go.sum`
- Test: `deploy/k8s/manifests_test.go`

**Interfaces:**
- Produces: kustomize base `deploy/k8s/base` and overlay `deploy/k8s/overlays/local`; the base needs ConfigMaps `goim-config` (keys `REDIS_ADDRS`, `JWKS_URL`, `MAX_CONNS`) and `jwks-fallback` (`jwks.json`) and Secret `noise-keys` (file `1`). Names: StatefulSet `gw` (pods `gw-0..5`, label `app: gateway`), Services `gateway` (headless, Kick 9100), `gateway-client` (client 8000; NodePort 30800 in `local`), `logic` (headless, 9000/9001); Deployment and HPA `logic`; PDBs `gateway`, `logic`; NetworkPolicies `gateway`, `logic`. Image `goim` (`goim:tier3` in `local`, never pulled), binaries `/gateway`, `/logic`. Node label `goim.io/role` = `gateway` | `logic`.
- Consumed by: Task 3.

- [ ] **Step 1: The test**

Save the patch at the end of this task to a file in the session scratchpad (`PATCH`). From the repository root:
```bash
git apply --include=deploy/k8s/manifests_test.go PATCH
go get k8s.io/api@v0.37.0 sigs.k8s.io/yaml@v1.6.0 && go mod tidy
```
`git status --short` shows ` M go.mod`, ` M go.sum`, `?? deploy/k8s/`.

- [ ] **Step 2: Run it to see it fail**

Run: `go test -count=1 ./deploy/k8s/`
Expected: FAIL, all 5 tests; the first says `0 documents, want the base's 11 and the local overlay's 3`.

- [ ] **Step 3: The manifests**

`git apply --include='deploy/k8s/*' --exclude=deploy/k8s/manifests_test.go PATCH` (the excluding order matters: the first match wins, so put `--exclude` first if your git reads it that way; the replay used the order `--exclude=… --include=…`). Then `go test -count=1 ./deploy/k8s/` and `go vet ./deploy/k8s/`: PASS.

- [ ] **Step 4: Each mutation is caught**

With `S` the session scratchpad, each line breaks one manifest, runs the test, restores the file:
```bash
mut() { cp "$1" "$S/m.bak"; sed -i "$2" "$1"; echo "$3: $(go test -count=1 ./deploy/k8s/ 2>&1 | grep -- '--- FAIL' | awk '{print $3}' | paste -sd, -)"; cp "$S/m.bak" "$1"; }
B=deploy/k8s/base
mut $B/gateway.yaml 's/\["-max-conns=$(MAX_CONNS)"\]/["-max-conns=$(MAX_CONNS)", "-faults"]/' faults
mut $B/logic.yaml 's/"-jwks-url=$(JWKS_URL)"\]/"-jwks-url=$(JWKS_URL)", "-clock-offset=1s"]/' clock
mut deploy/k8s/overlays/local/gateway.yaml 's/  type: NodePort/  type: NodePort\n  publishNotReadyAddresses: true/' client-notready
mut $B/gateway.yaml 's/maxUnavailable: 1/maxUnavailable: 2/' pdb
mut $B/gateway.yaml 's/  maxUnavailable: 1/  minAvailable: 5/' pdb-min
mut $B/logic.yaml 's/minAvailable: 2/minAvailable: 1/' logic-pdb
mut $B/gateway.yaml 's/terminationGracePeriodSeconds: 90/terminationGracePeriodSeconds: 60/' grace
mut $B/netpol.yaml '0,/        - namespaceSelector:/s//        - podSelector: {}\n          namespaceSelector:/' netpol-from
mut $B/netpol.yaml '0,/        - port: 9100/s//        - port: 9100\n        - port: 9090/' netpol-open
mut $B/logic.yaml 's/  replicas: 2/  replica: 2/' strict
git status --short deploy/
```
Expected (prototype): `faults` and `clock` → `TestNoTestOnlyFlags`; `client-notready` → `TestNotReadyAddresses`; `pdb`, `pdb-min`, `logic-pdb` → `TestDisruptionBudgets`; `grace` → `TestGatewayPods`; `netpol-from`, `netpol-open` → `TestAdminPortPolicy`; `strict` → all five. `git status` shows no modified manifest.

- [ ] **Step 5: Commit**

`git add deploy/k8s go.mod go.sum`, commit `feat(deploy): add k8s manifests and their strict check` with a short body and `Refs: #85, #149, #150, P5bI-3` (with the session's attribution line). `git diff proto-6d1 -- deploy go.mod go.sum` is empty.

**The patch (Tasks 1–3):**

````diff
diff --git a/.gitignore b/.gitignore
index f24bf8e..a345f47 100644
--- a/.gitignore
+++ b/.gitignore
@@ -5,2 +5,3 @@ pkg/
 tier2-results/
+tier3-results/
 *.DS_Store
diff --git a/Makefile b/Makefile
index fea69c6..5286442 100644
--- a/Makefile
+++ b/Makefile
@@ -9,3 +9,3 @@ TIER2_GUARD := test "$$GOIM_TEST_TIER" = 2 || { echo "Tier 2 host only: set GOIM
 
-.PHONY: test test-race-suite test-scripts test-load-probe redis-server test-redis-local test-redis-cluster-local proto redis-up redis-down test-redis test-redis-cluster test-tier2 deploy deploy-down deploy-status deploy-logs deploy-token
+.PHONY: test test-race-suite test-scripts test-load-probe redis-server k8s-tools test-tier3 test-redis-local test-redis-cluster-local proto redis-up redis-down test-redis test-redis-cluster test-tier2 deploy deploy-down deploy-status deploy-logs deploy-token
 
@@ -25,5 +25,7 @@ test-race-suite:
 # readers, and a real stack.sh stack.
-# Needs python3, curl and redis-server (make redis-server).
-test-scripts:
+# Needs python3, curl and redis-server (make redis-server); the Tier 3
+# tests (Plan 6d1) need the pinned kind and kubectl (make k8s-tools).
+test-scripts: k8s-tools
 	scripts/tier2/lib_test.sh
+	scripts/tier3/lib_test.sh
 	scripts/tier2/floods_test.sh
@@ -51,2 +53,7 @@ redis-server:
 
+# The pinned kind and kubectl for Tier 3 (Plan 6d1), user-space binaries in
+# .cache/k8s/bin, checked against their SHA-256; no root, no Docker needed.
+k8s-tools:
+	@scripts/k8s-tools.sh .cache/k8s >/dev/null
+
 # Tier 1a on real Redis, on the dev machine: every test, including the ones
@@ -97,2 +104,9 @@ test-tier2:
 
+# Tier 3 scenario entry point (spec: Automation contract, Tier 3 target), on
+# the Tier 2 host with Docker: GOIM_TEST_TIER=3 make test-tier3 SCENARIO=smoke
+# TARGET=local. TARGET=cloud is refused until a cloud cluster exists.
+# Scenarios: smoke (Plan 6d1). Exit codes as test-tier2's.
+test-tier3:
+	@TARGET=$(TARGET) scripts/tier3/$(SCENARIO).sh; rc=$$?; [ $$rc -eq 0 ] || { echo "test-tier3: $(SCENARIO) exited $$rc" >&2; exit $$rc; }
+
 # A single-node stack for a real client app, on Ubuntu or macOS (scripts/deploy.sh,
diff --git a/cmd/loadgen/main.go b/cmd/loadgen/main.go
index b84ecf2..7dbd679 100644
--- a/cmd/loadgen/main.go
+++ b/cmd/loadgen/main.go
@@ -59,2 +59,3 @@ func run(ctx context.Context, args []string, stderr io.Writer) error {
 	fs.IntVar(&cfg.SourceNet, "source-net", 0, "third octet of the source addresses, one per instance on a stack; floods: required, 1–255")
+	fs.StringVar(&cfg.SourceCIDR, "source-cidr", "", "source addresses from this IPv4 network's second host on instead of 127.0.<source-net>.2…, e.g. 198.18.0.0/24 (Tier 3)")
 	fs.DurationVar(&cfg.PingEvery, "ping-every", 150*time.Second, "light: Ping interval (the gateway's -heartbeat)")
diff --git a/deploy/k8s/Dockerfile b/deploy/k8s/Dockerfile
new file mode 100644
index 0000000..3ae3281
--- /dev/null
+++ b/deploy/k8s/Dockerfile
@@ -0,0 +1,4 @@
+# The gateway and Logic, built static on the host (Plan 6d1, t3_build).
+FROM scratch
+COPY gateway logic /
+USER 65532:65532
diff --git a/deploy/k8s/base/gateway.yaml b/deploy/k8s/base/gateway.yaml
new file mode 100644
index 0000000..74d370c
--- /dev/null
+++ b/deploy/k8s/base/gateway.yaml
@@ -0,0 +1,109 @@
+# gw-k is the gateway id ($POD_NAME) and Logic's Kick target
+# gw-k.gateway.im.svc.cluster.local:9100; the binary's defaults are these
+# ports and Logic at logic.im.svc.cluster.local:9000/9001.
+apiVersion: apps/v1
+kind: StatefulSet
+metadata:
+  name: gw
+spec:
+  serviceName: gateway
+  replicas: 6
+  podManagementPolicy: Parallel
+  selector:
+    matchLabels:
+      app: gateway
+  template:
+    metadata:
+      labels:
+        app: gateway
+      annotations:
+        cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
+    spec:
+      # > -drain-timeout (85s).
+      terminationGracePeriodSeconds: 90
+      securityContext:
+        runAsNonRoot: true
+        runAsUser: 65532
+      affinity:
+        podAntiAffinity:
+          requiredDuringSchedulingIgnoredDuringExecution:
+            - labelSelector:
+                matchLabels:
+                  app: gateway
+              topologyKey: kubernetes.io/hostname
+      containers:
+        - name: gateway
+          image: goim
+          command: ["/gateway"]
+          args: ["-max-conns=$(MAX_CONNS)"]
+          env:
+            - name: POD_NAME
+              valueFrom:
+                fieldRef:
+                  fieldPath: metadata.name
+            - name: MAX_CONNS
+              valueFrom:
+                configMapKeyRef:
+                  name: goim-config
+                  key: MAX_CONNS
+          ports:
+            - name: client
+              containerPort: 8000
+            - name: kick
+              containerPort: 9100
+            - name: admin
+              containerPort: 9090
+          readinessProbe:
+            httpGet:
+              path: /readyz
+              port: 9090
+            periodSeconds: 2
+          resources:
+            requests:
+              cpu: "1"
+              memory: 1Gi
+          volumeMounts:
+            - name: noise-keys
+              mountPath: /etc/goim/noise
+              readOnly: true
+      volumes:
+        - name: noise-keys
+          secret:
+            secretName: noise-keys
+---
+# Kicks reach a draining (NotReady) pod.
+apiVersion: v1
+kind: Service
+metadata:
+  name: gateway
+spec:
+  clusterIP: None
+  publishNotReadyAddresses: true
+  selector:
+    app: gateway
+  ports:
+    - name: kick
+      port: 9100
+---
+# Clients: Ready pods only (R4M-7); the cloud LB uses least connections.
+apiVersion: v1
+kind: Service
+metadata:
+  name: gateway-client
+spec:
+  type: LoadBalancer
+  selector:
+    app: gateway
+  ports:
+    - name: client
+      port: 8000
+---
+apiVersion: policy/v1
+kind: PodDisruptionBudget
+metadata:
+  name: gateway
+spec:
+  maxUnavailable: 1
+  selector:
+    matchLabels:
+      app: gateway
diff --git a/deploy/k8s/base/kustomization.yaml b/deploy/k8s/base/kustomization.yaml
new file mode 100644
index 0000000..d779736
--- /dev/null
+++ b/deploy/k8s/base/kustomization.yaml
@@ -0,0 +1,11 @@
+# A deployment adds the ConfigMaps goim-config (REDIS_ADDRS, JWKS_URL,
+# MAX_CONNS) and jwks-fallback (jwks.json) and the Secret noise-keys (a
+# file per key_id, made with kubectl: kustomize drops a data key named 1).
+apiVersion: kustomize.config.k8s.io/v1beta1
+kind: Kustomization
+namespace: im
+resources:
+  - namespace.yaml
+  - gateway.yaml
+  - logic.yaml
+  - netpol.yaml
diff --git a/deploy/k8s/base/logic.yaml b/deploy/k8s/base/logic.yaml
new file mode 100644
index 0000000..93cadc1
--- /dev/null
+++ b/deploy/k8s/base/logic.yaml
@@ -0,0 +1,101 @@
+# minReplicas covers the storm peak (R2-O7); the PDB keeps that many.
+apiVersion: apps/v1
+kind: Deployment
+metadata:
+  name: logic
+spec:
+  replicas: 2
+  selector:
+    matchLabels:
+      app: logic
+  template:
+    metadata:
+      labels:
+        app: logic
+    spec:
+      securityContext:
+        runAsNonRoot: true
+        runAsUser: 65532
+      containers:
+        - name: logic
+          image: goim
+          command: ["/logic"]
+          args: ["-redis=$(REDIS_ADDRS)", "-jwks-url=$(JWKS_URL)"]
+          env:
+            - name: REDIS_ADDRS
+              valueFrom:
+                configMapKeyRef:
+                  name: goim-config
+                  key: REDIS_ADDRS
+            - name: JWKS_URL
+              valueFrom:
+                configMapKeyRef:
+                  name: goim-config
+                  key: JWKS_URL
+          ports:
+            - name: grpc
+              containerPort: 9000
+            - name: lease
+              containerPort: 9001
+            - name: admin
+              containerPort: 9090
+          readinessProbe:
+            httpGet:
+              path: /readyz
+              port: 9090
+            periodSeconds: 2
+          resources:
+            requests:
+              cpu: 500m
+              memory: 512Mi
+          volumeMounts:
+            - name: jwks-fallback
+              mountPath: /etc/goim/jwks-fallback
+              readOnly: true
+      volumes:
+        - name: jwks-fallback
+          configMap:
+            name: jwks-fallback
+---
+apiVersion: v1
+kind: Service
+metadata:
+  name: logic
+spec:
+  clusterIP: None
+  selector:
+    app: logic
+  ports:
+    - name: grpc
+      port: 9000
+    - name: lease
+      port: 9001
+---
+apiVersion: autoscaling/v2
+kind: HorizontalPodAutoscaler
+metadata:
+  name: logic
+spec:
+  scaleTargetRef:
+    apiVersion: apps/v1
+    kind: Deployment
+    name: logic
+  minReplicas: 2
+  maxReplicas: 8
+  metrics:
+    - type: Resource
+      resource:
+        name: cpu
+        target:
+          type: Utilization
+          averageUtilization: 70
+---
+apiVersion: policy/v1
+kind: PodDisruptionBudget
+metadata:
+  name: logic
+spec:
+  minAvailable: 2
+  selector:
+    matchLabels:
+      app: logic
diff --git a/deploy/k8s/base/namespace.yaml b/deploy/k8s/base/namespace.yaml
new file mode 100644
index 0000000..3369cf5
--- /dev/null
+++ b/deploy/k8s/base/namespace.yaml
@@ -0,0 +1,4 @@
+apiVersion: v1
+kind: Namespace
+metadata:
+  name: im
diff --git a/deploy/k8s/base/netpol.yaml b/deploy/k8s/base/netpol.yaml
new file mode 100644
index 0000000..962bb68
--- /dev/null
+++ b/deploy/k8s/base/netpol.yaml
@@ -0,0 +1,42 @@
+# The admin port (metrics, pprof, /readyz) from the monitoring namespace
+# only (#85); the kubelet's probe comes from the node (kindnet does not
+# police the node's root user). Every other port stays open.
+apiVersion: networking.k8s.io/v1
+kind: NetworkPolicy
+metadata:
+  name: gateway
+spec:
+  podSelector:
+    matchLabels:
+      app: gateway
+  policyTypes: ["Ingress"]
+  ingress:
+    - ports:
+        - port: 8000
+        - port: 9100
+    - from:
+        - namespaceSelector:
+            matchLabels:
+              kubernetes.io/metadata.name: monitoring
+      ports:
+        - port: 9090
+---
+apiVersion: networking.k8s.io/v1
+kind: NetworkPolicy
+metadata:
+  name: logic
+spec:
+  podSelector:
+    matchLabels:
+      app: logic
+  policyTypes: ["Ingress"]
+  ingress:
+    - ports:
+        - port: 9000
+        - port: 9001
+    - from:
+        - namespaceSelector:
+            matchLabels:
+              kubernetes.io/metadata.name: monitoring
+      ports:
+        - port: 9090
diff --git a/deploy/k8s/manifests_test.go b/deploy/k8s/manifests_test.go
new file mode 100644
index 0000000..5aa1d03
--- /dev/null
+++ b/deploy/k8s/manifests_test.go
@@ -0,0 +1,201 @@
+// Package k8s_test checks deploy/k8s on any machine (Plan 6d1, #150.7):
+// every manifest decodes strictly into its k8s.io/api type, none sets a
+// test-only flag (P5bI-3), and the fields the spec relies on are set.
+package k8s_test
+
+import (
+	"bytes"
+	"io/fs"
+	"os"
+	"path/filepath"
+	"slices"
+	"testing"
+
+	appsv1 "k8s.io/api/apps/v1"
+	autoscalingv2 "k8s.io/api/autoscaling/v2"
+	corev1 "k8s.io/api/core/v1"
+	networkingv1 "k8s.io/api/networking/v1"
+	policyv1 "k8s.io/api/policy/v1"
+	"sigs.k8s.io/yaml"
+)
+
+var kinds = map[string]func() any{
+	"Namespace": func() any { return &corev1.Namespace{} }, "Service": func() any { return &corev1.Service{} },
+	"StatefulSet": func() any { return &appsv1.StatefulSet{} }, "Deployment": func() any { return &appsv1.Deployment{} },
+	"PodDisruptionBudget":     func() any { return &policyv1.PodDisruptionBudget{} },
+	"HorizontalPodAutoscaler": func() any { return &autoscalingv2.HorizontalPodAutoscaler{} },
+	"NetworkPolicy":           func() any { return &networkingv1.NetworkPolicy{} },
+}
+
+// load decodes every document of the .yaml files under dir but the
+// kustomization.yaml files; an unknown field or kind fails the test.
+func load(t *testing.T, dir string) (docs []any) {
+	t.Helper()
+	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
+		if err != nil || d.IsDir() || filepath.Ext(path) != ".yaml" || d.Name() == "kustomization.yaml" {
+			return err
+		}
+		b, err := os.ReadFile(path)
+		for i, part := range bytes.Split(b, []byte("\n---\n")) {
+			var tm struct{ Kind string }
+			_ = yaml.Unmarshal(part, &tm)
+			mk, ok := kinds[tm.Kind]
+			if !ok {
+				t.Errorf("%s #%d: kind %q is not checked here", path, i, tm.Kind)
+				continue
+			}
+			obj := mk()
+			if err := yaml.UnmarshalStrict(part, obj); err != nil {
+				t.Errorf("%s #%d: %v", path, i, err)
+				continue
+			}
+			docs = append(docs, obj)
+		}
+		return err
+	})
+	if err != nil {
+		t.Fatal(err)
+	}
+	return docs
+}
+
+func only[T any](docs []any) (out []T) {
+	for _, d := range docs {
+		if o, ok := d.(T); ok {
+			out = append(out, o)
+		}
+	}
+	return out
+}
+
+func TestManifestsDecodeStrictly(t *testing.T) {
+	if n := len(load(t, ".")); n != 14 {
+		t.Fatalf("%d documents, want the base's 11 and the local overlay's 3", n)
+	}
+}
+
+// The fault API is unauthenticated on the admin port; -clock-offset
+// shifts the wall clock (P5bI-3).
+func TestNoTestOnlyFlags(t *testing.T) {
+	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
+		if err != nil || d.IsDir() || filepath.Ext(path) == ".go" {
+			return err
+		}
+		b, err := os.ReadFile(path)
+		for _, flag := range []string{"-faults", "-clock-offset"} {
+			if bytes.Contains(b, []byte(flag)) {
+				t.Errorf("%s sets %s", path, flag)
+			}
+		}
+		return err
+	})
+	if err != nil {
+		t.Fatal(err)
+	}
+}
+
+// Kicks reach a draining gateway through the headless Service gateway;
+// every other Service, the client one above all, has Ready pods only.
+func TestNotReadyAddresses(t *testing.T) {
+	found := false
+	for _, s := range only[*corev1.Service](load(t, ".")) {
+		if s.Name == "gateway" && s.Spec.ClusterIP == corev1.ClusterIPNone {
+			found = s.Spec.PublishNotReadyAddresses
+		} else if s.Spec.PublishNotReadyAddresses {
+			t.Errorf("Service %s publishes NotReady addresses", s.Name)
+		}
+	}
+	if !found {
+		t.Error("the headless Service gateway does not publish NotReady addresses")
+	}
+}
+
+// R5S-O5: one gateway at a time, Logic never below minReplicas, gateway
+// pods not evicted by the autoscaler.
+func TestDisruptionBudgets(t *testing.T) {
+	docs := load(t, "base")
+	pdbs := map[string]*policyv1.PodDisruptionBudget{}
+	for _, p := range only[*policyv1.PodDisruptionBudget](docs) {
+		pdbs[p.Spec.Selector.MatchLabels["app"]] = p
+	}
+	if p := pdbs["gateway"]; p == nil || p.Spec.MaxUnavailable == nil || p.Spec.MaxUnavailable.String() != "1" || p.Spec.MinAvailable != nil {
+		t.Errorf("gateway PDB %+v, want maxUnavailable 1 only", p)
+	}
+	hpa, dep := only[*autoscalingv2.HorizontalPodAutoscaler](docs), only[*appsv1.Deployment](docs)
+	if len(hpa) != 1 || hpa[0].Spec.MinReplicas == nil || len(dep) != 1 || dep[0].Spec.Replicas == nil {
+		t.Fatalf("want one HPA with minReplicas and one Deployment with replicas, got %d, %d", len(hpa), len(dep))
+	}
+	min := int(*hpa[0].Spec.MinReplicas)
+	if p := pdbs["logic"]; p == nil || p.Spec.MinAvailable == nil || p.Spec.MinAvailable.IntValue() != min || p.Spec.MaxUnavailable != nil {
+		t.Errorf("logic PDB %+v, want minAvailable %d (minReplicas) only", p, min)
+	}
+	if r := int(*dep[0].Spec.Replicas); r != min {
+		t.Errorf("logic replicas %d, want minReplicas %d", r, min)
+	}
+	for _, s := range only[*appsv1.StatefulSet](docs) {
+		if v := s.Spec.Template.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"]; v != "false" {
+			t.Errorf("StatefulSet %s: safe-to-evict %q, want \"false\"", s.Name, v)
+		}
+	}
+}
+
+// The pod name is the gateway id and its Kick DNS name, the grace period
+// outlasts the drain (-drain-timeout 85s), and both binaries are probed on
+// /readyz.
+func TestGatewayPods(t *testing.T) {
+	docs := load(t, "base")
+	sts := only[*appsv1.StatefulSet](docs)
+	if len(sts) != 1 || sts[0].Spec.ServiceName != "gateway" {
+		t.Fatalf("want one StatefulSet with serviceName gateway, got %d", len(sts))
+	}
+	pod := sts[0].Spec.Template.Spec
+	if g := pod.TerminationGracePeriodSeconds; g == nil || *g <= 85 {
+		t.Errorf("terminationGracePeriodSeconds %v, want > 85", g)
+	}
+	if e := pod.Containers[0].Env; len(e) == 0 || e[0].Name != "POD_NAME" || e[0].ValueFrom == nil ||
+		e[0].ValueFrom.FieldRef == nil || e[0].ValueFrom.FieldRef.FieldPath != "metadata.name" {
+		t.Errorf("gateway env %+v, want POD_NAME from metadata.name first", e)
+	}
+	specs := []corev1.PodSpec{pod}
+	for _, d := range only[*appsv1.Deployment](docs) {
+		specs = append(specs, d.Spec.Template.Spec)
+	}
+	for _, s := range specs {
+		if p := s.Containers[0].ReadinessProbe; p == nil || p.HTTPGet == nil || p.HTTPGet.Path != "/readyz" || p.HTTPGet.Port.IntValue() != 9090 {
+			t.Errorf("%s: readiness probe %+v, want GET /readyz on 9090", s.Containers[0].Name, p)
+		}
+	}
+}
+
+// #85: each binary's pods are selected by a policy that admits 9090 only
+// from the monitoring namespace and keeps its other ports open to all.
+func TestAdminPortPolicy(t *testing.T) {
+	want := map[string][]int{"gateway": {8000, 9100}, "logic": {9000, 9001}}
+	for _, np := range only[*networkingv1.NetworkPolicy](load(t, "base")) {
+		app := np.Spec.PodSelector.MatchLabels["app"]
+		open, ok := want[app]
+		delete(want, app)
+		var opened []int
+		for _, r := range np.Spec.Ingress {
+			if len(r.Ports) == 0 {
+				t.Errorf("%s: a rule without ports opens every port", np.Name)
+			}
+			for _, p := range r.Ports {
+				if p.Port.IntValue() != 9090 {
+					if len(r.From) == 0 {
+						opened = append(opened, p.Port.IntValue())
+					}
+				} else if f := r.From; len(f) != 1 || f[0].PodSelector != nil || f[0].IPBlock != nil || f[0].NamespaceSelector == nil ||
+					len(f[0].NamespaceSelector.MatchLabels) != 1 || f[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "monitoring" {
+					t.Errorf("%s: 9090 admitted from %+v, want the monitoring namespace only", np.Name, f)
+				}
+			}
+		}
+		if !ok || !slices.Equal(opened, open) {
+			t.Errorf("NetworkPolicy %s (app %q): open to all %v, want %v", np.Name, app, opened, open)
+		}
+	}
+	if len(want) != 0 {
+		t.Errorf("no NetworkPolicy for %v", want)
+	}
+}
diff --git a/deploy/k8s/overlays/local/gateway.yaml b/deploy/k8s/overlays/local/gateway.yaml
new file mode 100644
index 0000000..37a88a8
--- /dev/null
+++ b/deploy/k8s/overlays/local/gateway.yaml
@@ -0,0 +1,27 @@
+apiVersion: apps/v1
+kind: StatefulSet
+metadata:
+  name: gw
+spec:
+  template:
+    spec:
+      nodeSelector:
+        goim.io/role: gateway
+      tolerations:
+        - key: goim.io/role
+          value: gateway
+          effect: NoSchedule
+      containers:
+        - name: gateway
+          imagePullPolicy: Never
+---
+apiVersion: v1
+kind: Service
+metadata:
+  name: gateway-client
+spec:
+  type: NodePort
+  ports:
+    - name: client
+      port: 8000
+      nodePort: 30800
diff --git a/deploy/k8s/overlays/local/kustomization.yaml b/deploy/k8s/overlays/local/kustomization.yaml
new file mode 100644
index 0000000..0bf8348
--- /dev/null
+++ b/deploy/k8s/overlays/local/kustomization.yaml
@@ -0,0 +1,14 @@
+# Tier 3 TARGET=local on scripts/tier3/kind.yaml (#150); the client
+# NodePort's IPVS (least connections) stands in for the cloud LB (#67).
+apiVersion: kustomize.config.k8s.io/v1beta1
+kind: Kustomization
+namespace: im
+resources:
+  - ../../base
+images:
+  - name: goim
+    newName: goim
+    newTag: tier3
+patches:
+  - path: gateway.yaml
+  - path: logic.yaml
diff --git a/deploy/k8s/overlays/local/logic.yaml b/deploy/k8s/overlays/local/logic.yaml
new file mode 100644
index 0000000..d5a3254
--- /dev/null
+++ b/deploy/k8s/overlays/local/logic.yaml
@@ -0,0 +1,12 @@
+apiVersion: apps/v1
+kind: Deployment
+metadata:
+  name: logic
+spec:
+  template:
+    spec:
+      nodeSelector:
+        goim.io/role: logic
+      containers:
+        - name: logic
+          imagePullPolicy: Never
diff --git a/internal/loadgen/balancer.go b/internal/loadgen/balancer.go
index 3967f7d..5c880f9 100644
--- a/internal/loadgen/balancer.go
+++ b/internal/loadgen/balancer.go
@@ -7,2 +7,3 @@ import (
 	"net"
+	"net/netip"
 	"sync"
@@ -57,2 +58,25 @@ func SourcesIn(netOctet, n int) ([]net.IP, error) {
 
+// SourcesFrom are n addresses of the IPv4 network cidr from its second
+// host on, as #69 lays out 127.0.0.x: 198.18.0.0/24 gives 198.18.0.2, …,
+// up to the last below the broadcast address. Tier 3 (Plan 6d1, #150.2):
+// kind's nodes are reached over the kind bridge, not loopback, so the
+// sources are aliases goim-srcaddr adds there.
+func SourcesFrom(cidr string, n int) ([]net.IP, error) {
+	p, err := netip.ParsePrefix(cidr)
+	if err != nil || !p.Addr().Is4() || p != p.Masked() {
+		return nil, fmt.Errorf("loadgen: source CIDR %q, want an IPv4 network address such as 198.18.0.0/24", cidr)
+	}
+	most := min(maxSources, 1<<(32-p.Bits())-3)
+	if n < 1 || n > most {
+		return nil, fmt.Errorf("loadgen: %d sources in %s, want 1–%d", n, cidr, most)
+	}
+	ips := make([]net.IP, n)
+	a := p.Addr().Next()
+	for i := range ips {
+		a = a.Next()
+		ips[i] = net.IP(a.AsSlice())
+	}
+	return ips, nil
+}
+
 // NewBalancer balances over addrs from n source addresses.
@@ -68,4 +92,8 @@ func NewBalancerIn(addrs []string, netOctet, sources int) (*Balancer, error) {
 	}
+	return newBalancer(addrs, srcs), nil
+}
+
+func newBalancer(addrs []string, srcs []net.IP) *Balancer {
 	return &Balancer{addrs: addrs, srcs: srcs, open: make([]int, len(addrs)), down: make([]bool, len(addrs)),
-		next: make([]int, len(addrs))}, nil
+		next: make([]int, len(addrs))}
 }
diff --git a/internal/loadgen/balancer_test.go b/internal/loadgen/balancer_test.go
index c0326ea..70a209e 100644
--- a/internal/loadgen/balancer_test.go
+++ b/internal/loadgen/balancer_test.go
@@ -177 +177,28 @@ func TestBalancerEveryGatewayUsesEverySource(t *testing.T) {
 }
+
+// Tier 3 sources (Plan 6d1, #150.2): kind's nodes are not on loopback, so
+// loadgen's sources are host aliases from a CIDR, in #69's layout.
+func TestSourcesFrom(t *testing.T) {
+	ips, err := SourcesFrom("198.18.0.0/24", 253)
+	if err != nil || ips[0].String() != "198.18.0.2" || ips[252].String() != "198.18.0.254" {
+		t.Fatalf("sources %v … %v, err %v; want 198.18.0.2 … 198.18.0.254", ips[0], ips[len(ips)-1], err)
+	}
+	if ips, _ := SourcesFrom("10.1.0.0/16", 2); len(ips) != 2 || ips[1].String() != "10.1.0.3" {
+		t.Fatalf("a /16: %v", ips)
+	}
+	for _, tc := range []struct {
+		cidr string
+		n    int
+	}{
+		{"198.18.0.0/24", 254}, // past .254
+		{"198.18.0.0/24", 0},
+		{"198.18.0.0/30", 2}, // .2 only
+		{"198.18.0.1/24", 1}, // not a network address
+		{"fd00::/64", 1},
+		{"198.18.0.0", 1},
+	} {
+		if _, err := SourcesFrom(tc.cidr, tc.n); err == nil {
+			t.Errorf("SourcesFrom(%q, %d) accepted", tc.cidr, tc.n)
+		}
+	}
+}
diff --git a/internal/loadgen/runner.go b/internal/loadgen/runner.go
index 46dcee6..a661881 100644
--- a/internal/loadgen/runner.go
+++ b/internal/loadgen/runner.go
@@ -35,2 +35,3 @@ type Config struct {
 	SourceNet  int           // source addresses 127.0.SourceNet.2…, so instances keep apart (Plan 6b3a)
+	SourceCIDR string        // source addresses from this network instead (SourcesFrom; Tier 3, Plan 6d1)
 	UIDPrefix  string        // distinct per loadgen run, so runs never replace each other's sessions
@@ -108,2 +109,4 @@ func New(cfg Config, env Env) (*Runner, error) {
 		return nil, fmt.Errorf("loadgen: source net %d, want 0–255", cfg.SourceNet)
+	case cfg.SourceCIDR != "" && cfg.SourceNet != 0:
+		return nil, errors.New("loadgen: a source CIDR or a source net, not both")
 	}
@@ -135,3 +138,6 @@ func New(cfg Config, env Env) (*Runner, error) {
 	}
-	bal, err := NewBalancerIn(env.GatewayAddrs, cfg.SourceNet, cfg.Sources)
+	srcs, err := SourcesIn(cfg.SourceNet, cfg.Sources)
+	if cfg.SourceCIDR != "" {
+		srcs, err = SourcesFrom(cfg.SourceCIDR, cfg.Sources)
+	}
 	if err != nil {
@@ -139,2 +145,3 @@ func New(cfg Config, env Env) (*Runner, error) {
 	}
+	bal := newBalancer(env.GatewayAddrs, srcs)
 	for _, gw := range down {
diff --git a/internal/loadgen/runner_test.go b/internal/loadgen/runner_test.go
index 578f7da..cacdb50 100644
--- a/internal/loadgen/runner_test.go
+++ b/internal/loadgen/runner_test.go
@@ -83 +83,17 @@ func TestNewRefusesPrecomputeInLightMode(t *testing.T) {
 }
+
+// -source-cidr replaces 127.0.<source-net>.x; both at once is a mistake.
+func TestNewTakesSourcesFromACIDR(t *testing.T) {
+	env := Env{GatewayIDs: []string{"lb"}, GatewayAddrs: []string{"172.18.0.3:30800"}}
+	r, err := New(Config{Mode: "sdk", Clients: 6_000, Rate: 1, UIDPrefix: "x", SourceCIDR: "198.18.0.0/24"}, env)
+	if err != nil {
+		t.Fatal(err)
+	}
+	if r.cfg.Sources != 2 || len(r.bal.srcs) != 2 || r.bal.srcs[1].String() != "198.18.0.3" {
+		t.Fatalf("sources %d %v, want 198.18.0.2 and .3", r.cfg.Sources, r.bal.srcs)
+	}
+	_, err = New(Config{Mode: "sdk", Clients: 1, Rate: 1, UIDPrefix: "x", SourceCIDR: "198.18.0.0/24", SourceNet: 1}, env)
+	if err == nil || !strings.Contains(err.Error(), "source") {
+		t.Fatalf("err %v, want a source error", err)
+	}
+}
diff --git a/scripts/k8s-tools.sh b/scripts/k8s-tools.sh
new file mode 100755
index 0000000..1045a4e
--- /dev/null
+++ b/scripts/k8s-tools.sh
@@ -0,0 +1,28 @@
+#!/usr/bin/env bash
+# Fetches the pinned kind and kubectl (Plan 6d1), checked against their
+# SHA-256, unless PREFIX/bin holds them at these versions. No root.
+#
+# Usage: scripts/k8s-tools.sh [PREFIX]   (default .cache/k8s; make k8s-tools)
+set -euo pipefail
+kind_version=v0.33.0
+kind_sha256=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
+kubectl_version=v1.37.0
+kubectl_sha256=6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f
+
+prefix=${1:-.cache/k8s}
+mkdir -p "$prefix/bin"
+prefix=$(cd "$prefix" && pwd)
+
+fetch() {
+  local tmp
+  tmp=$(mktemp "$prefix/bin/.$1.XXXXXX")
+  curl -fsSL -o "$tmp" "$2" && echo "$3  $tmp" | sha256sum -c --quiet || { rm -f "$tmp"; return 1; }
+  chmod 0755 "$tmp" && mv "$tmp" "$prefix/bin/$1"
+}
+
+"$prefix/bin/kind" version 2>/dev/null | grep -q "^kind $kind_version " ||
+  fetch kind "https://github.com/kubernetes-sigs/kind/releases/download/$kind_version/kind-linux-amd64" "$kind_sha256"
+"$prefix/bin/kubectl" version --client 2>/dev/null | grep -q "^Client Version: $kubectl_version\$" ||
+  fetch kubectl "https://dl.k8s.io/release/$kubectl_version/bin/linux/amd64/kubectl" "$kubectl_sha256"
+"$prefix/bin/kind" version
+"$prefix/bin/kubectl" version --client | head -n 1
diff --git a/scripts/redis-cluster.sh b/scripts/redis-cluster.sh
index 31585bb..6780214 100644
--- a/scripts/redis-cluster.sh
+++ b/scripts/redis-cluster.sh
@@ -20,3 +20,7 @@
 # 20.6-22.7s, #139);
-# REDIS_CLUSTER_JOIN_TIMEOUT_S (how long the join waits, default 30).
+# REDIS_CLUSTER_JOIN_TIMEOUT_S (how long the join waits, default 30);
+# REDIS_CLUSTER_HOST (default 127.0.0.1): the address the nodes also bind,
+# announce and join on, so clients elsewhere reach every node: Tier 3's
+# pods reach the host's Cluster on the kind bridge's address (#150.2).
+# Local tools keep reaching each node on 127.0.0.1.
 
@@ -34,3 +38,5 @@ redis_cluster_layout() {
 redis_cluster_args() {
-  REDIS_CLUSTER_ARGS=(--port "$1" --cluster-port "$2" --bind 127.0.0.1 --dir "$3"
+  local host=${REDIS_CLUSTER_HOST:-127.0.0.1} bind=(127.0.0.1)
+  [ "$host" = 127.0.0.1 ] || bind+=("$host")
+  REDIS_CLUSTER_ARGS=(--port "$1" --cluster-port "$2" --bind "${bind[@]}" --dir "$3"
     --save "" --appendonly no --maxmemory-policy noeviction
@@ -39,2 +45,3 @@ redis_cluster_args() {
     --repl-diskless-sync yes --repl-diskless-sync-delay 0)
+  [ "$host" = 127.0.0.1 ] || REDIS_CLUSTER_ARGS+=(--cluster-announce-ip "$host")
 }
@@ -45,3 +52,3 @@ redis_cluster_args() {
 redis_cluster_join() {
-  local cli=$1 i p id ok deadline
+  local cli=$1 i p id ok deadline h=${REDIS_CLUSTER_HOST:-127.0.0.1}
   shift
@@ -57,3 +64,3 @@ redis_cluster_join() {
   done
-  "$cli" --cluster create "127.0.0.1:${ports[0]}" "127.0.0.1:${ports[1]}" "127.0.0.1:${ports[2]}" \
+  "$cli" --cluster create "$h:${ports[0]}" "$h:${ports[1]}" "$h:${ports[2]}" \
     --cluster-yes >/dev/null || { echo "redis-cluster: --cluster create failed" >&2; return 1; }
@@ -61,3 +68,3 @@ redis_cluster_join() {
     id=$("$cli" -p "${ports[i]}" cluster myid)
-    "$cli" --cluster add-node "127.0.0.1:${ports[i + 3]}" "127.0.0.1:${ports[0]}" \
+    "$cli" --cluster add-node "$h:${ports[i + 3]}" "$h:${ports[0]}" \
       --cluster-slave --cluster-master-id "$id" >/dev/null ||
diff --git a/scripts/stack_cluster_test.sh b/scripts/stack_cluster_test.sh
index 0706b53..c13da20 100755
--- a/scripts/stack_cluster_test.sh
+++ b/scripts/stack_cluster_test.sh
@@ -102,5 +102,10 @@ expect "down: no port left" "$(stack_busy_ports)" ""
 # A second stack_up in the same STACK_DIR forms a new Cluster: the old
-# nodes.conf files must not survive.
-if stack_up >"$STACK_DIR/up2.log" 2>&1; then ok "up again: the same STACK_DIR forms a Cluster"; else bad "up again: $(tail -n 3 "$STACK_DIR/up2.log" | paste -sd' ' -)"; fi
+# nodes.conf files must not survive. It joins on another host address
+# (REDIS_CLUSTER_HOST, Tier 3's kind bridge, Plan 6d1): every node
+# announces it and answers there, and Logic, seeded with 127.0.0.1, follows
+# the announced addresses.
+if REDIS_CLUSTER_HOST=127.0.0.9 stack_up >"$STACK_DIR/up2.log" 2>&1; then ok "up again: the same STACK_DIR forms a Cluster"; else bad "up again: $(tail -n 3 "$STACK_DIR/up2.log" | paste -sd' ' -)"; fi
 expect "up again: 3 ranges" "$(stack_redis_slots | wc -l)" 3
+expect "host: every node announces 127.0.0.9" "$(stack_redis_node_cli redis-0 cluster nodes | awk '$2 !~ /^127\.0\.0\.9:/' | wc -l)" 0
+expect "host: a node answers there" "$(stack_redis_node_cli redis-5 -h 127.0.0.9 ping)" PONG
 stack_down
diff --git a/scripts/tier2/goim-srcaddr b/scripts/tier2/goim-srcaddr
new file mode 100755
index 0000000..27842a3
--- /dev/null
+++ b/scripts/tier2/goim-srcaddr
@@ -0,0 +1,56 @@
+#!/bin/bash
+# goim-srcaddr: the one root action Tier 3 runs need (Plan 6d1, #150.2, the
+# #68 pattern): loadgen's source addresses (-source-cidr 198.18.0.0/24) as
+# aliases on the kind bridge, since kind's nodes are not on loopback.
+# host-setup.sh installs it root-owned with goim-netdrop's sudoers line.
+#
+#   sudo -n /usr/local/sbin/goim-srcaddr add BRIDGE N   198.18.0.2 … 198.18.0.(N+1)/32 on BRIDGE
+#   sudo -n /usr/local/sbin/goim-srcaddr del BRIDGE     every 198.18.0.0/24 address off BRIDGE
+#   sudo -n /usr/local/sbin/goim-srcaddr list BRIDGE    show them
+#   goim-srcaddr --print add BRIDGE N                   print the ip commands, run nothing
+#
+# It runs only ip, from a fixed path, on a fixed range: 198.18.0.0/24, in
+# the block reserved for network benchmarks (RFC 2544), outside kind's
+# pod (10.244/16) and Service (10.96/12) ranges. BRIDGE must be a Docker
+# bridge, br- and 12 hex digits, that exists; N must be 1-253. It reads no
+# environment variable.
+set -eu
+PATH=/usr/sbin:/usr/bin:/sbin:/bin
+IP=/usr/sbin/ip
+NET=198.18.0
+
+usage() { echo "usage: goim-srcaddr [--print] add BRIDGE N | del BRIDGE | list BRIDGE" >&2; exit 2; }
+
+print=0
+if [ "${1:-}" = --print ]; then print=1; shift; fi
+cmd=${1:-}
+br=${2:-}
+case $br in br-[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;; *) usage ;; esac
+if [ $print = 0 ] && [ ! -d "/sys/class/net/$br/bridge" ]; then
+  echo "goim-srcaddr: $br is not a bridge" >&2
+  exit 1
+fi
+
+# ours: our addresses on the bridge now, one per line.
+ours() { $IP -4 -o addr show dev "$br" | awk -v p="$NET." '{ split($4, a, "/"); if (index(a[1], p) == 1) print a[1] }'; }
+
+case $cmd in
+  add)
+    [ $# -eq 3 ] || usage
+    n=$3
+    case $n in ''|0*|*[!0-9]*) usage ;; esac
+    [ "$n" -le 253 ] || usage
+    for ((i = 2; i <= n + 1; i++)); do
+      if [ $print = 1 ]; then echo "ip addr replace $NET.$i/32 dev $br"; else $IP addr replace "$NET.$i/32" dev "$br"; fi
+    done
+    ;;
+  del)
+    [ $# -eq 2 ] && [ $print = 0 ] || usage
+    for a in $(ours); do $IP addr del "$a/32" dev "$br"; done
+    ;;
+  list)
+    [ $# -eq 2 ] && [ $print = 0 ] || usage
+    ours
+    ;;
+  *) usage ;;
+esac
diff --git a/scripts/tier2/host-setup.sh b/scripts/tier2/host-setup.sh
index 53e394f..479400e 100755
--- a/scripts/tier2/host-setup.sh
+++ b/scripts/tier2/host-setup.sh
@@ -14,4 +14,6 @@
 #   /usr/local/sbin/goim-netdrop           root-owned, 0755 (the b2 DROP rule only)
-#   /etc/sudoers.d/goim-netdrop            $SUDO_USER may run goim-netdrop, and only
-#                                          it, without a password (checked with visudo)
+#   /usr/local/sbin/goim-srcaddr           root-owned, 0755 (Tier 3's loadgen sources
+#                                          on the kind bridge only, Plan 6d1)
+#   /etc/sudoers.d/goim-netdrop            $SUDO_USER may run these two, and only
+#                                          them, without a password (checked with visudo)
 #   /etc/security/limits.d/90-goim-tier2.conf  and
@@ -87,3 +89,4 @@ put "$here/modules.conf" /etc/modules-load.d/goim-tier2.conf 0644
 put "$here/goim-netdrop" /usr/local/sbin/goim-netdrop 0755
-echo "$user ALL=(root) NOPASSWD: /usr/local/sbin/goim-netdrop" >"$tmp/sudoers"
+put "$here/goim-srcaddr" /usr/local/sbin/goim-srcaddr 0755
+echo "$user ALL=(root) NOPASSWD: /usr/local/sbin/goim-netdrop, /usr/local/sbin/goim-srcaddr" >"$tmp/sudoers"
 if [ -z "$root" ]; then
diff --git a/scripts/tier2/lib.sh b/scripts/tier2/lib.sh
index d15c071..ea08b45 100644
--- a/scripts/tier2/lib.sh
+++ b/scripts/tier2/lib.sh
@@ -32,2 +32,6 @@ T2_MIN_CORES=12
 T2_MIN_MEM_GB=24
+# The tier a script runs at: Tier 3 scripts (scripts/tier3/lib.sh, Plan
+# 6d1) set 3 before t2_init. It names the GOIM_TEST_TIER the guard wants,
+# result.json's "tier" and the results directory.
+: "${T2_TIER:=2}"
 
@@ -37,4 +41,4 @@ t2_guard_verdict() {
   local tier=$1 cores=$2 mem=$3
-  if [ "$tier" != 2 ]; then
-    echo "refused: Tier 2 scenarios need GOIM_TEST_TIER=2 set by the caller, e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race"
+  if [ "$tier" != "$T2_TIER" ]; then
+    echo "refused: Tier $T2_TIER scenarios need GOIM_TEST_TIER=$T2_TIER set by the caller, e.g. GOIM_TEST_TIER=$T2_TIER make test-tier$T2_TIER SCENARIO=$([ "$T2_TIER" = 2 ] && echo race || echo smoke)"
     return 1
@@ -48,3 +52,3 @@ t2_guard_verdict() {
   if [ "$cores" -lt "$T2_MIN_CORES" ] || [ "$mem" -lt "$T2_MIN_MEM_GB" ]; then
-    echo "refused: Tier 2 needs >= $T2_MIN_CORES cores and >= ${T2_MIN_MEM_GB}GB RAM (have $cores cores, ${mem}GB)"
+    echo "refused: Tier $T2_TIER needs >= $T2_MIN_CORES cores and >= ${T2_MIN_MEM_GB}GB RAM (have $cores cores, ${mem}GB)"
     return 1
@@ -140,3 +144,3 @@ t2_result_json() {
   "scenario": $(t2_json_str "$T2_SCENARIO"),
-  "tier": 2,
+  "tier": $T2_TIER,
   "commit": $(t2_json_str "$(git rev-parse HEAD 2>/dev/null)"),
@@ -174,3 +178,3 @@ t2_write_report() {
   {
-    echo "goim Tier 2 report: scenario $T2_SCENARIO"
+    echo "goim Tier $T2_TIER report: scenario $T2_SCENARIO"
     t2_section "environment"
@@ -253,7 +257,9 @@ t2_init() {
   t2_need go gcc make git
-  kind=$(t2_kind_state)
-  why=$(t2_kind_verdict "${kind%%|*}" "${kind#*|}") || { echo "$why" >&2; exit 2; }
+  if [ "$T2_TIER" = 2 ]; then
+    kind=$(t2_kind_state)
+    why=$(t2_kind_verdict "${kind%%|*}" "${kind#*|}") || { echo "$why" >&2; exit 2; }
+  fi
   export GOIM_TEST_TIER CGO_ENABLED=1
   T2_T0=$SECONDS
-  T2_OUT="$PWD/tier2-results/$T2_SCENARIO-$(date +%Y%m%d-%H%M%S)"
+  T2_OUT="$PWD/tier$T2_TIER-results/$T2_SCENARIO-$(date +%Y%m%d-%H%M%S)"
   T2_REPORT="$T2_OUT.report.txt"
@@ -594,3 +600,3 @@ t2_record_end() {
 
-# t2_host_setup_missing DIR PROC_SYS SYS_MODULE NETDROP: prints one line per
+# t2_host_setup_missing DIR PROC_SYS SYS_MODULE NETDROP SRCADDR: prints one line per
 # item host-setup.sh would fix: a sysctl below sysctl.conf (every field of a
@@ -599,3 +605,3 @@ t2_record_end() {
 t2_host_setup_missing() {
-  local dir=$1 proc=$2 mods=$3 netdrop=$4 key want have m
+  local dir=$1 proc=$2 mods=$3 netdrop=$4 srcaddr=$5 key want have m
   while IFS=$'\t' read -r key want; do
@@ -611,2 +617,3 @@ t2_host_setup_missing() {
   cmp -s "$dir/goim-netdrop" "$netdrop" || echo "$netdrop is missing or differs from scripts/tier2/goim-netdrop"
+  cmp -s "$dir/goim-srcaddr" "$srcaddr" || echo "$srcaddr is missing or differs from scripts/tier2/goim-srcaddr"
 }
@@ -646,3 +653,3 @@ t2_require_host_setup() {
   local missing rules why
-  missing=$(t2_host_setup_missing scripts/tier2 /proc/sys /sys/module /usr/local/sbin/goim-netdrop)
+  missing=$(t2_host_setup_missing scripts/tier2 /proc/sys /sys/module /usr/local/sbin/goim-netdrop /usr/local/sbin/goim-srcaddr)
   if [ -z "$missing" ] && ! rules=$(sudo -n /usr/local/sbin/goim-netdrop list 2>/dev/null); then
diff --git a/scripts/tier2/lib_test.sh b/scripts/tier2/lib_test.sh
index 9a40f65..d37ea2a 100755
--- a/scripts/tier2/lib_test.sh
+++ b/scripts/tier2/lib_test.sh
@@ -28,2 +28,5 @@ t2_guard_verdict 2 "" 26 >/dev/null; expect "guard: unknown cores are refused" "
 t2_guard_verdict 2 24 26 >/dev/null; expect "guard: the Tier 2 host passes" "$?" 0
+why=$(T2_TIER=3 t2_guard_verdict 2 24 26); expect "guard: a Tier 3 script refuses GOIM_TEST_TIER=2" "$?" 1
+contains "guard: Tier 3 names its tier" "$why" "GOIM_TEST_TIER=3 make test-tier3"
+T2_TIER=3 t2_guard_verdict 3 24 26 >/dev/null; expect "guard: Tier 3 on the Tier 2 host passes" "$?" 0
 t2_kind_verdict "" "" >/dev/null; expect "kind: none passes" "$?" 0
@@ -213,5 +216,6 @@ echo old >"$tmp/hs/root/etc/systemd/system/user@.service.d/90-goim-tier2.conf"
 expect "setup: test mode exits 0" "$?" 0
-expect "setup: sudoers line" "$(cat "$tmp/hs/root/etc/sudoers.d/goim-netdrop")" "alice ALL=(root) NOPASSWD: /usr/local/sbin/goim-netdrop"
+expect "setup: sudoers line" "$(cat "$tmp/hs/root/etc/sudoers.d/goim-netdrop")" "alice ALL=(root) NOPASSWD: /usr/local/sbin/goim-netdrop, /usr/local/sbin/goim-srcaddr"
 expect "setup: sudoers mode" "$(stat -c %a "$tmp/hs/root/etc/sudoers.d/goim-netdrop")" 440
 expect "setup: helper mode" "$(stat -c %a "$tmp/hs/root/usr/local/sbin/goim-netdrop")" 755
+expect "setup: Tier 3 helper mode" "$(stat -c %a "$tmp/hs/root/usr/local/sbin/goim-srcaddr")" 755
 expect "setup: nofile hard limit" "$(cat "$tmp/hs/root/etc/security/limits.d/90-goim-tier2.conf")" "alice hard nofile 2097152"
@@ -221,3 +225,3 @@ expect "setup: sysctl.conf as is on a host with nothing higher" "$(cmp -s script
 again=$(scripts/tier2/host-setup.sh --root "$tmp/hs/root" --user alice --uid 1001 | grep -c '^unchanged')
-expect "setup: idempotent" "$again" 6
+expect "setup: idempotent" "$again" 7
 scripts/tier2/host-setup.sh --root "$tmp/hs/root" --user 'a b' >/dev/null 2>&1; expect "setup: odd user refused" "$?" 2
@@ -240,3 +244,4 @@ for m in $(grep -vE '^[[:space:]]*(#|$)' scripts/tier2/modules.conf); do mkdir -
 cp scripts/tier2/goim-netdrop "$fake/netdrop"
-expect "setup check: a set-up host" "$(t2_host_setup_missing scripts/tier2 "$fake/proc" "$fake/mod" "$fake/netdrop")" ""
+cp scripts/tier2/goim-srcaddr "$fake/srcaddr"
+expect "setup check: a set-up host" "$(t2_host_setup_missing scripts/tier2 "$fake/proc" "$fake/mod" "$fake/netdrop" "$fake/srcaddr")" ""
 printf '1048576\t1000000\t2097152\n' >"$fake/proc/net/ipv4/tcp_mem"
@@ -245,3 +250,4 @@ rmdir "$fake/mod/ip_vs"
 echo "# old" >>"$fake/netdrop"
-m=$(t2_host_setup_missing scripts/tier2 "$fake/proc" "$fake/mod" "$fake/netdrop")
+rm "$fake/srcaddr"
+m=$(t2_host_setup_missing scripts/tier2 "$fake/proc" "$fake/mod" "$fake/netdrop" "$fake/srcaddr")
 contains "setup check: low conntrack" "$m" "sysctl net.netfilter.nf_conntrack_max is 262144, want >= 4194304"
@@ -250,3 +256,4 @@ contains "setup check: module" "$m" "module ip_vs is not loaded"
 contains "setup check: outdated helper" "$m" "differs from scripts/tier2/goim-netdrop"
-expect "setup check: nothing else" "$(echo "$m" | wc -l)" 4
+contains "setup check: Tier 3's helper missing" "$m" "$fake/srcaddr is missing"
+expect "setup check: nothing else" "$(echo "$m" | wc -l)" 5
 t2_netdrop_leftover "" >/dev/null; expect "leftover: no rules pass" "$?" 0
@@ -255,2 +262,12 @@ contains "leftover: says how to remove it" "$why" "sudo -n /usr/local/sbin/goim-
 
+# goim-srcaddr (Plan 6d1) prints its addresses and refuses anything else.
+expect "srcaddr: add" "$(scripts/tier2/goim-srcaddr --print add br-0123456789ab 2)" \
+"ip addr replace 198.18.0.2/32 dev br-0123456789ab
+ip addr replace 198.18.0.3/32 dev br-0123456789ab"
+for args in "add br-0123456789ab 254" "add br-0123456789ab 0" "add br-0123456789ab 07" "add eth0 1" "add br-0123456789AB 1" "del br-0123456789ab" "flush br-0123456789ab"; do
+  # shellcheck disable=SC2086
+  scripts/tier2/goim-srcaddr --print $args >/dev/null 2>&1; expect "srcaddr: refuses $args" "$?" 2
+done
+scripts/tier2/goim-srcaddr list br-0123456789ab >/dev/null 2>&1; expect "srcaddr: refuses a bridge that does not exist" "$?" 1
+
 # goim-netdrop prints its rules and refuses anything else.
diff --git a/scripts/tier2/modules.conf b/scripts/tier2/modules.conf
index c4994af..b2ee4c3 100644
--- a/scripts/tier2/modules.conf
+++ b/scripts/tier2/modules.conf
@@ -2,3 +2,4 @@
 # /etc/modules-load.d/goim-tier2.conf. nf_conntrack must be loaded for its
-# sysctls; ip_vs and its schedulers are kube-proxy's IPVS mode in kind (#67).
+# sysctls; ip_vs and its schedulers are kube-proxy's IPVS mode in kind (#67);
+# wlc for Plan 6d2's funnel (#132).
 nf_conntrack
@@ -9 +10,2 @@ ip_vs_sh
 ip_vs_lc
+ip_vs_wlc
diff --git a/scripts/tier3/kind.yaml b/scripts/tier3/kind.yaml
new file mode 100644
index 0000000..9e25398
--- /dev/null
+++ b/scripts/tier3/kind.yaml
@@ -0,0 +1,32 @@
+# Tier 3 TARGET=local (#67, #150.1): 6 tainted gateway nodes, 1 Logic node.
+kind: Cluster
+apiVersion: kind.x-k8s.io/v1alpha4
+networking:
+  kubeProxyMode: ipvs
+kubeadmConfigPatches:
+  - |
+    kind: KubeProxyConfiguration
+    ipvs:
+      scheduler: lc
+nodes:
+  - role: control-plane
+  - &gateway
+    role: worker
+    labels:
+      goim.io/role: gateway
+    kubeadmConfigPatches:
+      - |
+        kind: JoinConfiguration
+        nodeRegistration:
+          taints:
+            - key: goim.io/role
+              value: gateway
+              effect: NoSchedule
+  - *gateway
+  - *gateway
+  - *gateway
+  - *gateway
+  - *gateway
+  - role: worker
+    labels:
+      goim.io/role: logic
diff --git a/scripts/tier3/lib.sh b/scripts/tier3/lib.sh
new file mode 100644
index 0000000..7e6491e
--- /dev/null
+++ b/scripts/tier3/lib.sh
@@ -0,0 +1,211 @@
+# shellcheck shell=bash
+# lib.sh: what Tier 3 TARGET=local scenarios share (Plan 6d1, #149, #150):
+# gateway and Logic in a kind cluster on the Tier 2 host, the Redis Cluster
+# and loadgen on the host. Source it after scripts/tier2/lib.sh, stack.sh
+# and loadgen.sh, then t3_init SCENARIO, t3_build, t3_cluster_up,
+# t3_redis_up, t3_deploy, t3_probes, t3_sources N (each below).
+# Settings: TARGET (local; cloud is refused until a cloud cluster exists),
+# TIER3_SCALE (default 0.15: the per-gateway cap is 250k x scale).
+
+T3_CLUSTER=goim-t3
+T3_NODE_IMAGE=kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
+T3_PROBE_IMAGE=busybox:1.37.0
+T3_NODEPORT=30800
+T3_SOURCE_CIDR=198.18.0.0/24
+# Gateway node k on one Zen 5c core, both SMT siblings (#150.1); the
+# control plane and the Logic node share T2_CPUS_LOGIC with Redis.
+T3_GW_CPUS=(4,16 5,17 6,18 7,19 8,20 9,21)
+T3_NOT_COVERED=("the real NLB's RST/FIN pass-through, flow-hash spreading and deregistration delay"
+  "cloud network latency" "the 1M absolute scale")
+
+# t3_cap SCALE: the per-gateway cap 250,000 x SCALE, rounded; fails unless
+# 0 < SCALE <= 1.
+t3_cap() {
+  awk -v s="$1" 'BEGIN { if (s !~ /^(0|1)?(\.[0-9]+)?$/ || s + 0 <= 0 || s + 0 > 1) exit 1; printf "%d\n", 250000 * s + 0.5 }'
+}
+
+t3_init() {
+  T2_TIER=3
+  t2_init "$1"
+  case ${TARGET:-} in
+    local) ;;
+    cloud) t2_refuse "refused: TARGET=cloud needs a cloud cluster, which does not exist yet (spec: Tier 3 target)" ;;
+    *) t2_refuse "refused: set TARGET=local or TARGET=cloud, e.g. GOIM_TEST_TIER=3 make test-tier3 SCENARIO=$1 TARGET=local" ;;
+  esac
+  T3_SCALE=${TIER3_SCALE:-0.15}
+  T3_CAP=$(t3_cap "$T3_SCALE") || t2_refuse "refused: TIER3_SCALE=$T3_SCALE, want 0 < scale <= 1"
+  t2_field target '"local"'
+  t2_field scale "$T3_SCALE"
+  t2_not_covered "${T3_NOT_COVERED[@]}"
+  t2_need docker curl python3 sudo
+  docker info >/dev/null 2>&1 || t2_refuse "refused: docker info fails (is the daemon up and usable without sudo?)"
+  scripts/k8s-tools.sh .cache/k8s >"$T2_OUT/k8s-tools.log" 2>&1 || t2_refuse "refused: scripts/k8s-tools.sh failed, see $T2_OUT/k8s-tools.log"
+  PATH=$PWD/.cache/k8s/bin:$PATH
+  # kind writes the context to the run's kubeconfig, not ~/.kube/config.
+  export KUBECONFIG=$T2_OUT/kubeconfig STACK_DIR=$T2_OUT/stack STACK_PIN=1
+}
+
+# t3_build: host binaries; the image goim:tier3 (static, #150.5).
+t3_build() {
+  stack_build || return 1
+  mkdir -p "$T2_OUT/image"
+  CGO_ENABLED=0 go build -trimpath -o "$T2_OUT/image/" ./cmd/gateway ./cmd/logic || return 1
+  docker build -q -t goim:tier3 -f deploy/k8s/Dockerfile "$T2_OUT/image" || return 1
+  docker image inspect "$T3_PROBE_IMAGE" >/dev/null 2>&1 || docker pull -q "$T3_PROBE_IMAGE"
+}
+
+# t3_cluster_up: a fresh cluster, nodes pinned before our pods start.
+# Sets T3_BRIDGE (the kind bridge) and T3_HOST (its address).
+t3_cluster_up() {
+  t2_on_exit 't3_cluster_down'
+  kind delete cluster --name "$T3_CLUSTER" >/dev/null 2>&1
+  kind create cluster --name "$T3_CLUSTER" --image "$T3_NODE_IMAGE" --config scripts/tier3/kind.yaml --wait 0s || return 1
+  local i=0 n
+  for n in $(kubectl get nodes -l goim.io/role=gateway -o name | sort -V); do
+    docker update --cpuset-cpus "${T3_GW_CPUS[i]}" "${n#node/}" >/dev/null || return 1
+    i=$((i + 1))
+  done
+  [ "$i" -eq 6 ] || { echo "tier3: $i gateway nodes, want 6" >&2; return 1; }
+  for n in $(kubectl get nodes -l 'goim.io/role!=gateway' -o name); do
+    docker update --cpuset-cpus "$T2_CPUS_LOGIC" "${n#node/}" >/dev/null || return 1
+  done
+  kind load docker-image --name "$T3_CLUSTER" goim:tier3 "$T3_PROBE_IMAGE" || return 1
+  kubectl wait --for=condition=Ready nodes --all --timeout=300s >/dev/null || return 1
+  local id
+  id=$(docker network inspect kind -f '{{.Id}}') || return 1
+  T3_BRIDGE=br-${id:0:12}
+  T3_HOST=$(ip -4 -o addr show dev "$T3_BRIDGE" | awk '{ split($4, a, "/"); if (a[1] !~ /^198\.18\.0\./) { print a[1]; exit } }')
+  [ -n "$T3_HOST" ] || { echo "tier3: no IPv4 address on $T3_BRIDGE" >&2; return 1; }
+}
+
+# t3_cluster_down: logs and events, then the cluster, aliases and Redis.
+t3_cluster_down() {
+  local p
+  mkdir -p "$STACK_DIR/logs/k8s"
+  kubectl get events -A --sort-by=.lastTimestamp >"$STACK_DIR/logs/k8s/events.txt" 2>&1
+  for p in $(kubectl -n im get pods -o name 2>/dev/null); do
+    kubectl -n im logs "$p" >"$STACK_DIR/logs/k8s/${p#pod/}.log" 2>&1
+  done
+  kind delete cluster --name "$T3_CLUSTER"
+  [ -n "${T3_BRIDGE:-}" ] && sudo -n /usr/local/sbin/goim-srcaddr del "$T3_BRIDGE"
+  stack_down
+}
+
+# t3_redis_up: the Cluster, also on T3_HOST; sets T3_REDIS_ADDRS.
+t3_redis_up() {
+  local base=${STACK_PORT_BASE:-17000} j
+  mkdir -p "$STACK_DIR/logs" "$STACK_DIR/redis"
+  REDIS_CLUSTER_HOST=$T3_HOST stack_redis_cluster_up "$STACK_REDIS_SERVER" "$T2_CPUS_LOGIC" || return 1
+  T3_REDIS_ADDRS=""
+  for j in 0 1 2 3 4 5; do T3_REDIS_ADDRS+="${T3_REDIS_ADDRS:+,}$T3_HOST:$((base + 20 + j))"; done
+}
+
+# t3_overlay DIR REDIS_ADDRS MAX_CONNS: the run's kustomization: the local
+# overlay (by a relative path; kustomize refuses an absolute one), the
+# goim-config values, DIR/keys/jwks.json as jwks-fallback (no JWKS URL:
+# Logic runs on the fallback set). Not the Secret noise-keys: kustomize
+# drops a data key named 1, the key_id file name (t3_deploy makes it).
+t3_overlay() {
+  cat >"$1/kustomization.yaml" <<YAML
+apiVersion: kustomize.config.k8s.io/v1beta1
+kind: Kustomization
+namespace: im
+resources:
+  - $(realpath --relative-to="$1" deploy/k8s/overlays/local)
+configMapGenerator:
+  - name: goim-config
+    literals:
+      - REDIS_ADDRS=$2
+      - JWKS_URL=
+      - MAX_CONNS=$3
+  - name: jwks-fallback
+    files:
+      - jwks.json=keys/jwks.json
+YAML
+}
+
+# t3_deploy: new keys, the run overlay, apply, rollout; sets T3_NOISE_PUBLIC.
+t3_deploy() {
+  local k=$T2_OUT/k8s
+  mkdir -p "$k/keys"
+  T3_NOISE_PUBLIC=$("$STACK_DIR/bin/gateway" noise-keygen -key-id 1 -out "$k/keys/noise-1") || return 1
+  T3_NOISE_PUBLIC=${T3_NOISE_PUBLIC##* public }
+  "$STACK_DIR/bin/devtoken" keygen -kid k1 -out "$k/keys/jwt.jwk" || return 1
+  "$STACK_DIR/bin/devtoken" jwks -out "$k/keys/jwks.json" "$k/keys/jwt.jwk" || return 1
+  t3_overlay "$k" "$T3_REDIS_ADDRS" "$T3_CAP"
+  kubectl kustomize "$k" >"$k/rendered.yaml" || return 1
+  kubectl -n im create secret generic noise-keys --from-file=1="$k/keys/noise-1" --dry-run=client -o yaml >"$k/noise-keys.yaml" || return 1
+  # In order: the Namespace (kustomize puts it first), then the Secret.
+  kubectl apply -f "$k/rendered.yaml" -f "$k/noise-keys.yaml" >"$k/apply.log" || return 1
+  kubectl -n im rollout status statefulset/gw --timeout=300s && kubectl -n im rollout status deployment/logic --timeout=300s
+}
+
+# t3_probes: a busybox pod in monitoring (admitted to 9090, #85) and in
+# goim-other (not), on the Logic node (the others are tainted).
+t3_probes() {
+  local ns
+  for ns in monitoring goim-other; do
+    kubectl create namespace "$ns" >/dev/null &&
+      kubectl -n "$ns" run probe --image="$T3_PROBE_IMAGE" --image-pull-policy=Never --restart=Never --command -- sleep 86400 >/dev/null || return 1
+  done
+  for ns in monitoring goim-other; do
+    kubectl -n "$ns" wait --for=condition=Ready pod/probe --timeout=120s >/dev/null || return 1
+  done
+}
+
+t3_exec() { local ns=$1; shift; timeout 20 kubectl -n "$ns" exec probe -- "$@"; }
+t3_metrics() { t3_exec monitoring wget -q -T 5 -O - "http://$1:9090/metrics"; }
+
+# t3_sources N: N aliases (goim-srcaddr); stack.env names one target, the
+# Logic node's NodePort, where IPVS picks the gateway (#150.2). Sets T3_LB.
+t3_sources() {
+  sudo -n /usr/local/sbin/goim-srcaddr add "$T3_BRIDGE" "$1" || return 1
+  T3_LB=$(kubectl get nodes -l goim.io/role=logic -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}'):$T3_NODEPORT
+  cat >"$STACK_DIR/stack.env" <<ENV
+GATEWAY_ADDRS=$T3_LB
+GATEWAY_IDS=lb
+NOISE_KEY_ID=1
+NOISE_PUBLIC=$T3_NOISE_PUBLIC
+JWT_KEY=$T2_OUT/k8s/keys/jwt.jwk
+ENV
+}
+
+# t3_ipvs PROC_NET_IP_VS ADDR: the IPVS service for ADDR (IP:PORT) in a
+# /proc/net/ip_vs listing: {"scheduler", "servers", "active": [ActiveConn
+# per real server]}, or null.
+t3_ipvs() {
+  python3 - "$1" "$2" <<'PY'
+import json, socket, struct, sys
+ip, port = sys.argv[2].rsplit(":", 1)
+want = "%08X:%04X" % (struct.unpack("!I", socket.inet_aton(ip))[0], int(port))
+svc = cur = None
+for line in open(sys.argv[1]):
+    f = line.split()
+    if len(f) >= 3 and f[0] in ("TCP", "UDP"):
+        cur = {"scheduler": f[2], "servers": 0, "active": []} if f[:2] == ["TCP", want] else None
+        svc = svc or cur
+    elif cur is not None and len(f) >= 6 and f[0] == "->":
+        cur["servers"] += 1
+        cur["active"].append(int(f[4]))
+print(json.dumps(svc))
+PY
+}
+
+# t3_balance COUNTS: from "NAME=N" words, {"total", "mean", "max_dev_pct",
+# "ok"}: ok when every count is within 10% of the mean (#150.3; R5S-O4's
+# 10% on a least-connections LB); null without counts or with a non-number.
+t3_balance() {
+  python3 - "$@" <<'PY'
+import json, sys
+try:
+    c = {k: int(v) for k, v in (a.split("=", 1) for a in sys.argv[1:])}
+except ValueError:
+    c = {}
+if not c:
+    print("null"); sys.exit()
+mean = sum(c.values()) / len(c)
+dev = max(abs(v - mean) for v in c.values()) / mean * 100 if mean else 100.0
+print(json.dumps({"counts": c, "total": sum(c.values()), "mean": round(mean, 1),
+                  "max_dev_pct": round(dev, 2), "ok": mean > 0 and dev <= 10}))
+PY
+}
diff --git a/scripts/tier3/lib_test.sh b/scripts/tier3/lib_test.sh
new file mode 100755
index 0000000..cb7005a
--- /dev/null
+++ b/scripts/tier3/lib_test.sh
@@ -0,0 +1,99 @@
+#!/usr/bin/env bash
+# Tests for scripts/tier3/lib.sh that run on any machine (make
+# test-scripts, Plan 6d1): the guard's Tier 3 refusals, the scale, the run
+# overlay rendered by the pinned kubectl, kind.yaml decoded by the pinned
+# kind, and the readers. Needs python3 and make k8s-tools.
+set -uo pipefail
+cd "$(git rev-parse --show-toplevel)"
+tmp=$(mktemp -d)
+trap 'rm -rf "$tmp"' EXIT
+fails=0
+ok() { echo "ok   $1"; }
+bad() { echo "FAIL $1"; fails=$((fails + 1)); }
+expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
+contains() { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1: [$2] lacks [$3]" ;; esac; }
+get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
+K8S=$PWD/.cache/k8s/bin
+[ -x "$K8S/kubectl" ] && [ -x "$K8S/kind" ] || { echo "tier3 lib_test: run make k8s-tools first" >&2; exit 2; }
+. scripts/tier2/lib.sh
+. scripts/stack.sh
+. scripts/loadgen.sh
+. scripts/tier3/lib.sh
+
+# The guard: GOIM_TEST_TIER=3 and TARGET, before anything else runs.
+run_init() { (cd "$tmp/repo" && env "$@" bash -c ". '$OLDPWD/scripts/tier2/lib.sh'; . '$OLDPWD/scripts/tier3/lib.sh'; T2_MIN_CORES=1 T2_MIN_MEM_GB=1; t3_init x; echo reached" 2>&1); }
+mkdir -p "$tmp/repo" && git -C "$tmp/repo" init -q
+out=$(run_init GOIM_TEST_TIER=2 TARGET=local); expect "init: GOIM_TEST_TIER=2 is refused" "$?" 2
+contains "init: names Tier 3" "$out" "GOIM_TEST_TIER=3"
+out=$(run_init GOIM_TEST_TIER=3 TARGET=); expect "init: no TARGET is refused" "$?" 2
+contains "init: names TARGET" "$out" "TARGET=local"
+out=$(run_init GOIM_TEST_TIER=3 TARGET=cloud); expect "init: TARGET=cloud is refused" "$?" 2
+contains "init: cloud does not exist yet" "$out" "cloud cluster"
+out=$(run_init GOIM_TEST_TIER=3 TARGET=local TIER3_SCALE=2); expect "init: scale 2 is refused" "$?" 2
+r=$(ls "$tmp/repo/tier3-results"/x-*/result.json 2>/dev/null | head -n 1)
+expect "init: a refused run's result.json says tier 3" "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["tier"])' "$r" 2>&1)" 3
+
+# The scale (spec: TIER3_SCALE, the per-gateway cap 250k x scale).
+expect "cap: the default scale" "$(t3_cap 0.15)" 37500
+expect "cap: scale 1" "$(t3_cap 1)" 250000
+for s in 0 1.5 -0.1 abc ""; do t3_cap "$s" >/dev/null; expect "cap: '$s' is refused" "$?" 1; done
+
+# The run overlay, rendered with the pinned kubectl: the local overlay with
+# the run's values, every name reference rewritten to the hashed names.
+mkdir -p "$tmp/k8s/keys" && echo '{"keys": []}' >"$tmp/k8s/keys/jwks.json"
+t3_overlay "$tmp/k8s" 172.18.0.1:17020,172.18.0.1:17021 37500
+"$K8S/kubectl" kustomize "$tmp/k8s" >"$tmp/rendered.yaml" 2>"$tmp/render.err"; expect "overlay: renders" "$?" 0
+r=$(python3 - "$tmp/rendered.yaml" <<'PY'
+import json, re, sys
+docs = open(sys.argv[1]).read().split("\n---\n")
+def has(kind, name, *subs):
+    return any(f"kind: {kind}\n" in d and re.search(rf"^  name: {name}$", d, re.M) and all(s in d for s in subs) for d in docs)
+cm = re.search(r"name: (goim-config-\w+)", open(sys.argv[1]).read()).group(1)
+print(json.dumps({
+  "config": has("ConfigMap", cm, "MAX_CONNS: \"37500\"", "REDIS_ADDRS: 172.18.0.1:17020,172.18.0.1:17021", "JWKS_URL: \"\""),
+  "gw": has("StatefulSet", "gw", f"name: {cm}", "image: goim:tier3", "imagePullPolicy: Never", "goim.io/role: gateway", "secretName: noise-keys"),
+  "logic": has("Deployment", "logic", f"name: {cm}", "name: jwks-fallback-", "goim.io/role: logic"),
+  "client": has("Service", "gateway-client", "type: NodePort", "nodePort: 30800"),
+  "ns": all("namespace: im" in d for d in docs if "kind: Namespace" not in d)}))
+PY
+)
+expect "overlay: config, gateways, Logic, client NodePort, namespace" "$r" \
+  '{"config": true, "gw": true, "logic": true, "client": true, "ns": true}'
+
+# kind.yaml decodes strictly (kind refuses an unknown field before it
+# needs docker): with a docker that fails, the error is docker's.
+mkdir -p "$tmp/bin" && printf '#!/bin/sh\nexit 1\n' >"$tmp/bin/docker" && chmod +x "$tmp/bin/docker"
+out=$(PATH=$tmp/bin:$PATH "$K8S/kind" create cluster --name t3-test --config scripts/tier3/kind.yaml 2>&1)
+contains "kind.yaml: decodes, kind goes on to docker" "$out" "docker info"
+sed 's/    labels:/    labelz:/' scripts/tier3/kind.yaml >"$tmp/bad.yaml"
+out=$(PATH=$tmp/bin:$PATH "$K8S/kind" create cluster --name t3-test --config "$tmp/bad.yaml" 2>&1)
+contains "kind.yaml: a typo is refused" "$out" "field labelz not found"
+
+# t3_balance: within 10% of the mean (#150.3).
+b=$(t3_balance gw-0=1000 gw-1=1050 gw-2=950 gw-3=1000 gw-4=1000 gw-5=1000)
+expect "balance: 5% passes" "$(get "$b" '(d["total"], d["max_dev_pct"], d["ok"])')" "(6000, 5.0, True)"
+b=$(t3_balance gw-0=1200 gw-1=960 gw-2=960 gw-3=960 gw-4=960 gw-5=960)
+expect "balance: 20% fails" "$(get "$b" '(d["max_dev_pct"], d["ok"])')" "(20.0, False)"
+expect "balance: zero connections fail" "$(get "$(t3_balance gw-0=0 gw-1=0)" 'd["ok"]')" False
+expect "balance: no counts" "$(t3_balance)" null
+expect "balance: an unreadable count" "$(t3_balance gw-0=1000 gw-1=null)" null
+
+# t3_ipvs: the NodePort's virtual service on the node's own address.
+cat >"$tmp/ip_vs" <<'IPVS'
+IP Virtual Server version 1.2.1 (size=4096)
+Prot LocalAddress:Port Scheduler Flags
+  -> RemoteAddress:Port Forward Weight ActiveConn InActConn
+TCP  0A600001:01BB lc
+  -> AC120005:192B                Masq    1      0          0
+TCP  AC120009:7850 lc
+  -> 0AF40101:1F40                Masq    1      1001       0
+  -> 0AF40201:1F40                Masq    1      999        2
+TCP  0AF40001:7850 rr
+  -> 0AF40101:1F40                Masq    1      0          0
+IPVS
+expect "ipvs: the service on the node's address" "$(t3_ipvs "$tmp/ip_vs" 172.18.0.9:30800)" \
+  '{"scheduler": "lc", "servers": 2, "active": [1001, 999]}'
+expect "ipvs: no such service" "$(t3_ipvs "$tmp/ip_vs" 172.18.0.8:30800)" null
+
+echo "$fails failure(s)"
+[ "$fails" -eq 0 ]
diff --git a/scripts/tier3/smoke.sh b/scripts/tier3/smoke.sh
new file mode 100755
index 0000000..387cb7b
--- /dev/null
+++ b/scripts/tier3/smoke.sh
@@ -0,0 +1,135 @@
+#!/usr/bin/env bash
+# Tier 3 TARGET=local: smoke (Plan 6d1, #150.3), the kind foundation before
+# Plan 6d2: kind.yaml's cluster, the deploy/k8s manifests, the host's Redis
+# Cluster on the kind bridge, ~6k light logins from loadgen on the host
+# through the Logic node's NodePort. No SLO is judged here.
+#
+# Usage (on the Tier 2 host, from the repo root), after the one-time
+#   sudo scripts/tier2/host-setup.sh:
+#   GOIM_TEST_TIER=3 make test-tier3 SCENARIO=smoke TARGET=local
+#   Results: tier3-results/smoke-<timestamp>/ (result.json, stack/ with
+#   logs, the pods' logs and artifacts), plus ONE file to send back:
+#   tier3-results/smoke-<timestamp>.report.txt
+#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
+#   About 6 minutes (kind create ~3 min).
+#
+# Checks: cluster, pinning, ready, redis_from_pods, admin_policy (#85),
+# online (SMOKE_CLIENTS, default 6,000), balance (each gateway within 10%
+# of the mean), ipvs (lc, 6 servers), pods (none restarted), processes.
+set -uo pipefail
+
+. "$(dirname "$0")/../tier2/lib.sh"
+. "$(dirname "$0")/../stack.sh"
+. "$(dirname "$0")/../loadgen.sh"
+. "$(dirname "$0")/lib.sh"
+t3_init smoke
+t2_require_host_setup
+t2_mem_budget 10
+t2_nofile 65536
+STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
+CLIENTS=${SMOKE_CLIENTS:-6000}
+t2_not_covered "rolling restart and drain (Plan 6d2)" "node loss and disruption budgets (Plan 6d3)" \
+  "HPA scaling (kind has no metrics-server; Logic stays at minReplicas)"
+
+t2_report_extra() {
+  local f
+  for f in "$STACK_DIR"/logs/*.log "$STACK_DIR"/logs/k8s/*.log; do
+    [ -f "$f" ] || continue
+    t2_section "$(basename "$f") (last 30 lines)"
+    tail -n 30 "$f"
+  done
+  [ -f "$STACK_DIR/logs/k8s/events.txt" ] && { t2_section "events (last 40)"; tail -n 40 "$STACK_DIR/logs/k8s/events.txt"; }
+}
+
+t=$SECONDS
+if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
+  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
+  exit 1
+fi
+t3_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
+t2_duration build $((SECONDS - t))
+
+t=$SECONDS
+t3_cluster_up >"$T2_OUT/cluster.log" 2>&1 || { echo "kind cluster failed, see $T2_OUT/cluster.log" >&2; exit 1; }
+t2_duration cluster $((SECONDS - t))
+ready=$(kubectl get nodes -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | grep -c '^True$')
+proxy=$(kubectl -n kube-system get configmap kube-proxy -o jsonpath='{.data.config\.conf}')
+mode=$(awk '/^mode:/ { print $2 }' <<<"$proxy") sched=$(awk '/^  scheduler:/ { print $2 }' <<<"$proxy")
+t2_check cluster "$(t2_bool [ "$ready" = 8 ] && [ "$mode" = ipvs ] && [ "$sched" = lc ])" \
+  "\"nodes_ready\": $ready, \"mode\": $(t2_json_str "$mode"), \"scheduler\": $(t2_json_str "$sched")"
+
+t=$SECONDS
+t3_redis_up >"$T2_OUT/redis.log" 2>&1 || { echo "Redis Cluster failed, see $T2_OUT/redis.log" >&2; exit 1; }
+t3_deploy >"$T2_OUT/deploy.log" 2>&1; rc=$?
+t2_duration deploy $((SECONDS - t))
+pods=$(kubectl -n im get pods -o jsonpath='{range .items[*]}{.metadata.labels.app} {.metadata.name} {.spec.nodeName} {.status.podIP} {.status.containerStatuses[0].ready}{"\n"}{end}')
+echo "$pods" >"$T2_OUT/pods.txt"
+gw_nodes=$(kubectl get nodes -l goim.io/role=gateway -o name | sed 's|node/||' | sort | paste -sd' ' -)
+logic_node=$(kubectl get nodes -l goim.io/role=logic -o jsonpath='{.items[0].metadata.name}')
+gws=$(awk '$1 == "gateway" && $5 == "true"' <<<"$pods")
+t2_check ready "$(t2_bool [ "$rc" = 0 ] && [ "$(awk '{ print $2 }' <<<"$gws" | sort | paste -sd' ' -)" = "gw-0 gw-1 gw-2 gw-3 gw-4 gw-5" ] &&
+  [ "$(awk '{ print $3 }' <<<"$gws" | sort | paste -sd' ' -)" = "$gw_nodes" ] &&
+  [ "$(awk -v n="$logic_node" '$1 == "logic" && $3 == n && $5 == "true"' <<<"$pods" | wc -l)" = 2 ])" \
+  "\"rollout\": $rc, \"pods\": $(t2_json_str "$pods")"
+[ "$rc" = 0 ] || exit 1
+
+# Pinning: as planned on the containers, and as seen inside a pod.
+want="" have="" n i=0
+for n in $(kubectl get nodes -l goim.io/role=gateway -o name | sed 's|node/||' | sort -V); do
+  want+="$n=${T3_GW_CPUS[i]} " i=$((i + 1))
+done
+for n in $T3_CLUSTER-control-plane $logic_node; do want+="$n=$T2_CPUS_LOGIC "; done
+for n in ${want}; do n=${n%%=*}; have+="$n=$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$n") "; done
+wrong=""
+for n in $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+loops=$(for p in gw-0 gw-1 gw-2 gw-3 gw-4 gw-5; do kubectl -n im logs "$p" | grep -m 1 -o 'loops=[0-9]*'; done | sort | uniq -c | awk '{ print $2 "x" $1 }')
+t2_check pinning "$(t2_bool [ "$want" = "$have" ] && [ -z "$wrong" ] && [ "$loops" = loops=2x6 ])" \
+  "\"nodes\": $(t2_json_str "${have% }"), \"redis_off_set\": $(t2_json_str "${wrong# }"), \"gateway_loops\": $(t2_json_str "$loops")"
+
+t3_probes >"$T2_OUT/probes.log" 2>&1 || { echo "probe pods failed, see $T2_OUT/probes.log" >&2; exit 1; }
+pong=""
+for j in 0 1 2 3 4 5; do
+  pong+=$(t3_exec monitoring sh -c "printf 'PING\r\n' | nc -w 2 $T3_HOST $((${STACK_PORT_BASE:-17000} + 20 + j))" | tr -d '\r')
+done
+t2_check redis_from_pods "$(t2_bool [ "$pong" = "+PONG+PONG+PONG+PONG+PONG+PONG" ])" "\"host\": $(t2_json_str "$T3_HOST"), \"replies\": $(t2_json_str "$pong")"
+
+gw_ip=$(awk '$2 == "gw-0" { print $4 }' <<<"$pods") logic_ip=$(awk '$1 == "logic" { print $4; exit }' <<<"$pods")
+reach() { if t3_exec "$1" "${@:2}" >/dev/null 2>&1; then echo open; else echo closed; fi; }
+p=$(printf '"%s": "%s", ' \
+  mon_gw_admin "$(reach monitoring wget -q -T 3 -O /dev/null "http://$gw_ip:9090/readyz")" \
+  mon_logic_admin "$(reach monitoring wget -q -T 3 -O /dev/null "http://$logic_ip:9090/readyz")" \
+  other_gw_admin "$(reach goim-other wget -q -T 3 -O /dev/null "http://$gw_ip:9090/readyz")" \
+  other_logic_admin "$(reach goim-other wget -q -T 3 -O /dev/null "http://$logic_ip:9090/readyz")" \
+  other_gw_client "$(reach goim-other nc -w 3 "$gw_ip" 8000)" \
+  other_gw_kick "$(reach goim-other nc -w 3 "$gw_ip" 9100)" \
+  other_logic_grpc "$(reach goim-other nc -w 3 "$logic_ip" 9000)")
+p="{${p%, }}"
+t2_check admin_policy "$(lg_true - 'd == {"mon_gw_admin": "open", "mon_logic_admin": "open", "other_gw_admin": "closed",
+  "other_logic_admin": "closed", "other_gw_client": "open", "other_gw_kick": "open", "other_logic_grpc": "open"}' <<<"$p")" "\"reach\": $p"
+
+# Load through the NodePort on the Logic node.
+t3_sources 2 >"$T2_OUT/sources.log" 2>&1 || { echo "goim-srcaddr failed, see $T2_OUT/sources.log" >&2; exit 1; }
+t2_on_exit 'lg_stop'
+lg_start smoke -mode light -clients "$CLIENTS" -rate 500 -uid-prefix t3smoke -source-cidr "$T3_SOURCE_CIDR"
+lg_wait_online "$CLIENTS" 120; rc=$?
+t2_check online "$(t2_bool [ "$rc" = 0 ])" "\"target\": $(t2_json_str "$T3_LB"), \"stats\": $(lg_get /stats 'd')"
+
+counts=""
+for p in gw-0 gw-1 gw-2 gw-3 gw-4 gw-5; do
+  ip=$(awk -v n="$p" '$2 == n { print $4 }' <<<"$pods")
+  counts+="$p=$(stack_metric_in "$(t3_metrics "$ip")" goim_gateway_connections 'state="online"') "
+done
+b=$(t3_balance $counts)
+t2_check balance "$(lg_true - "d is not None and d['ok'] and d['total'] == $CLIENTS" <<<"$b")" "\"balance\": $b"
+docker exec "$logic_node" cat /proc/net/ip_vs >"$T2_OUT/ip_vs.txt" 2>&1
+v=$(t3_ipvs "$T2_OUT/ip_vs.txt" "$T3_LB")
+t2_check ipvs "$(lg_true - 'd is not None and d["scheduler"] == "lc" and d["servers"] == 6' <<<"$v")" "\"service\": $v"
+
+lg_stop
+after=$(kubectl -n im get pods -o jsonpath='{range .items[*]}{.metadata.name} {.status.containerStatuses[0].ready} {.status.containerStatuses[0].restartCount}{"\n"}{end}')
+t2_check pods "$(t2_bool [ "$(awk '$2 == "true" && $3 == 0' <<<"$after" | wc -l)" = 8 ])" "\"pods\": $(t2_json_str "$after")"
+stack_down
+problems=$(stack_health)
+t2_check processes "$(t2_bool [ -z "$problems" ] && [ "$LG_STATUS" = 0 ])" \
+  "\"problems\": $(t2_json_str "$problems"), \"loadgen_exit\": ${LG_STATUS:-null}"
+t2_finish
````

---

### Task 2: Host side: loadgen sources, the root helper, Redis on the bridge, Tier 3 in lib.sh

**Files:**
- Create: `scripts/tier2/goim-srcaddr`
- Modify: `internal/loadgen/balancer.go`, `internal/loadgen/runner.go`, `cmd/loadgen/main.go`, `scripts/tier2/{host-setup.sh,modules.conf,lib.sh}`, `scripts/redis-cluster.sh`
- Test: `internal/loadgen/{balancer,runner}_test.go`, `scripts/tier2/lib_test.sh`, `scripts/stack_cluster_test.sh`

**Interfaces:**
- Produces: `loadgen.SourcesFrom(cidr string, n int) ([]net.IP, error)` (IPv4 network address only; second host on; n ≤ min(253, size − 3)); `Config.SourceCIDR` (exclusive with `SourceNet`); flag `-source-cidr`. `goim-srcaddr add BRIDGE N | del BRIDGE | list BRIDGE` (`--print add`), installed to `/usr/local/sbin`, sudoers line `NOPASSWD: /usr/local/sbin/goim-netdrop, /usr/local/sbin/goim-srcaddr`. `REDIS_CLUSTER_HOST` (redis-cluster.sh: also bind, announce, join on it). `T2_TIER` (lib.sh, default 2): guard tier, result.json `tier`, `tier$T2_TIER-results/`, the kind refusal at tier 2 only. `t2_host_setup_missing DIR PROC_SYS SYS_MODULE NETDROP SRCADDR`.
- Consumed by: Task 3.

- [ ] **Step 1: The tests**

`git apply --include='internal/loadgen/*_test.go' --include=scripts/tier2/lib_test.sh --include=scripts/stack_cluster_test.sh PATCH`

- [ ] **Step 2: Run them to see them fail**

Run: `go test -count=1 ./internal/loadgen/` → build failure: `undefined: SourcesFrom`, `unknown field SourceCIDR`. `scripts/tier2/lib_test.sh` → the Tier 3 guard, `setup: sudoers line`, `setup: Tier 3 helper mode`, `setup: idempotent`, `setup check`, `srcaddr:` lines fail. `STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_cluster_test.sh` → `host: every node announces 127.0.0.9` and `host: a node answers there` fail.

- [ ] **Step 3: The code**

`git apply --exclude='*_test.go' --exclude='*_test.sh' --include='internal/loadgen/*' --include=cmd/loadgen/main.go --include='scripts/tier2/*' --include=scripts/redis-cluster.sh PATCH`

- [ ] **Step 4: Run them to see them pass**

`gofmt -l internal cmd` empty; `go vet ./internal/loadgen/ ./cmd/loadgen/`; `go test -count=1 ./internal/loadgen/ ./cmd/loadgen/` PASS; `scripts/tier2/lib_test.sh` all passed (the swap flake aside, Global Constraints); `stack_cluster_test.sh` 0 failures.

- [ ] **Step 5: Commit**

`git add internal/loadgen cmd/loadgen scripts/tier2 scripts/redis-cluster.sh scripts/stack_cluster_test.sh`, commit `feat(tier3): add host-side pieces for kind runs`, `Refs: #68, #69, #132, #150`. `git diff proto-6d1 -- internal cmd scripts/tier2 scripts/redis-cluster.sh scripts/stack_cluster_test.sh` is empty.

---

### Task 3: Tier 3 scripts and the smoke scenario

**Files:**
- Create: `scripts/k8s-tools.sh`, `scripts/tier3/{lib.sh,kind.yaml,smoke.sh,lib_test.sh}`
- Modify: `Makefile` (`k8s-tools`, `test-tier3`, `test-scripts`), `.gitignore`
- Test: `scripts/tier3/lib_test.sh`

**Interfaces:**
- Consumes: Task 1's names; Task 2's `-source-cidr`, `goim-srcaddr`, `REDIS_CLUSTER_HOST`, `T2_TIER`; unchanged `stack.sh` (`stack_build`, `stack_redis_cluster_up`, `stack_down`, `stack_health`, `stack_metric_in`, `stack_pinned`, `stack_redis_names`) and `loadgen.sh` (`lg_start`, `lg_wait_online`, `lg_get`, `lg_true`, `lg_stop`).
- Produces (for 6d2, 6d3): `t3_init`, `t3_build`, `t3_cluster_up` (`T3_BRIDGE`, `T3_HOST`; cluster deleted at exit), `t3_redis_up` (`T3_REDIS_ADDRS`), `t3_overlay DIR REDIS_ADDRS MAX_CONNS`, `t3_deploy` (`T3_NOISE_PUBLIC`), `t3_probes`, `t3_exec NS CMD…`, `t3_metrics IP`, `t3_sources N` (`T3_LB`), `t3_cap SCALE`, `t3_ipvs FILE IP:PORT`, `t3_balance NAME=N…`. The smoke's 10 checks: `cluster`, `pinning`, `ready`, `redis_from_pods`, `admin_policy`, `online`, `balance`, `ipvs`, `pods`, `processes`.

- [ ] **Step 1: The test**

`git apply --include=scripts/k8s-tools.sh --include=scripts/tier3/lib_test.sh PATCH`, then `scripts/k8s-tools.sh` (prints `kind v0.33.0 …` and `Client Version: v1.37.0`; a few minutes on a slow link).

- [ ] **Step 2: Run it to see it fail**

Run: `scripts/tier3/lib_test.sh` → exit 1: `scripts/tier3/lib.sh: No such file or directory`, every `init:`, `cap:`, `overlay:`, `balance:`, `ipvs:` line fails (the `kind.yaml` lines fail too: no file).

- [ ] **Step 3: The scripts**

`git apply --exclude=scripts/tier3/lib_test.sh --exclude=scripts/k8s-tools.sh --include='scripts/tier3/*' --include=Makefile --include=.gitignore PATCH`

- [ ] **Step 4: Run them to see them pass**

`scripts/tier3/lib_test.sh`: 26 ok, `0 failure(s)`. `bash -n scripts/tier3/smoke.sh`. `GOIM_TEST_TIER=3 make test-tier3 SCENARIO=smoke TARGET=local` on the dev machine: `refused: Tier 3 needs >= 12 cores …`, exit 2, nothing written. `make test-scripts` green (the swap flake aside) and `make test` green.

- [ ] **Step 5: Commit**

`git add scripts/k8s-tools.sh scripts/tier3 Makefile .gitignore`, commit `feat(tier3): add the kind cluster and the smoke scenario`, `Refs: #67, #149, #150`. `git diff proto-6d1 -- Makefile .gitignore scripts deploy internal cmd go.mod go.sum` is empty. Push.

---

### Task 4: Dry run, the host run, the record

**Files:**
- Modify: this plan's *Execution Notes*, the roadmap (the Plan 6 row, #151's outcome)

- [ ] **Step 1: Pod-shaped dry run on the dev machine**

The image's binaries as the manifests run them (their arguments and environment, a Secret volume's layout, the fallback JWKS, Redis on a second address standing for the bridge), loadgen with `t3_sources`'s stack.env and `-source-cidr`. Save as `$S/podshape.sh`, run `bash $S/podshape.sh $S/run` from the repository root (~1 minute):
```bash
set -uo pipefail
. scripts/tier2/lib.sh; . scripts/stack.sh; . scripts/loadgen.sh; . scripts/tier3/lib.sh
D=$1; rm -rf "$D"; mkdir -p "$D/img" "$D/k8s/keys" "$D/noise/..2026_x"
export STACK_DIR=$D/stack STACK_PORT_BASE=17700 STACK_PIN=0
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
trap 'lg_stop; kill $(jobs -p) 2>/dev/null; stack_down' EXIT
CGO_ENABLED=0 go build -trimpath -o "$D/img/" ./cmd/gateway ./cmd/logic || exit 1
ldd "$D/img/gateway" "$D/img/logic" 2>&1 | grep -c 'not a dynamic'
stack_build >/dev/null || exit 1
pub=$("$STACK_DIR/bin/gateway" noise-keygen -key-id 1 -out "$D/noise/..2026_x/1"); pub=${pub##* public }
ln -s ..2026_x "$D/noise/..data"; ln -s ..data/1 "$D/noise/1"
"$STACK_DIR/bin/devtoken" keygen -kid k1 -out "$D/k8s/keys/jwt.jwk" && "$STACK_DIR/bin/devtoken" jwks -out "$D/k8s/keys/jwks.json" "$D/k8s/keys/jwt.jwk"
T3_HOST=127.0.0.9; t3_redis_up >"$D/redis.log" 2>&1 || { cat "$D/redis.log"; exit 1; }
REDIS_ADDRS=$T3_REDIS_ADDRS JWKS_URL=
"$D/img/logic" "-redis=$REDIS_ADDRS" "-jwks-url=$JWKS_URL" -jwks-fallback "$D/k8s/keys/jwks.json" >"$D/logic.log" 2>&1 &
POD_NAME=gw-0 "$D/img/gateway" "-max-conns=$(t3_cap 0.15)" -noise-keys "$D/noise" -logic 127.0.0.1:9000 -lease 127.0.0.1:9001 -admin-addr :9091 >"$D/gw.log" 2>&1 &
for u in 9090 9091; do
  for i in $(seq 100); do curl -sf -o /dev/null "http://127.0.0.1:$u/readyz" && break; sleep 0.3; done
  echo "readyz :$u $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$u/readyz")"
done
grep -o 'id=[^ ]*\|loops=[0-9]*\|keys=[0-9]*' "$D/gw.log" | paste -sd' ' -; grep -o 'fallback_keys=[a-z]*' "$D/logic.log"
printf 'GATEWAY_ADDRS=127.0.0.1:8000\nGATEWAY_IDS=lb\nNOISE_KEY_ID=1\nNOISE_PUBLIC=%s\nJWT_KEY=%s\n' "$pub" "$D/k8s/keys/jwt.jwk" >"$STACK_DIR/stack.env"
lg_start dry -mode light -clients 600 -rate 300 -uid-prefix t3dry -source-cidr 127.0.0.0/24 && lg_wait_online 600 60 && echo "online 600"
echo "sources: $(lg_get /stats '[d["sources"], d["balancer_open"]]')"
c=$(stack_metric_in "$(curl -s http://127.0.0.1:9091/metrics)" goim_gateway_connections 'state="online"')
echo "balance: $(t3_balance gw-0=$c)"
```
Expected (prototype): `2` (both static); `readyz :9090 200`, `readyz :9091 200`; `id=gw-0 loops=2 keys=1`; `fallback_keys=true`; `online 600`; `sources: [1, {"lb": 600}]` (all from 127.0.0.2); balance `ok` with 600.

- [ ] **Step 2: The user runs the smoke on the host**

Ask the user to run, on the Tier 2 host, from a fresh pull of `feat/plan6d1`, with the desktop apps closed:
```bash
sudo scripts/tier2/host-setup.sh        # once: goim-srcaddr, the sudoers line, ip_vs_wlc
GOIM_TEST_TIER=3 make test-tier3 SCENARIO=smoke TARGET=local
```
and to send back `tier3-results/smoke-<timestamp>.report.txt`. About 6 minutes, more the first time (the node image and busybox are pulled).

- [ ] **Step 3: Read the report**

Expected: exit 0, **10/10** checks. Read and record: `cluster` (8 nodes, `ipvs`, `lc`); `pinning` (each node's cpuset, `loops=2x6`); `ready` (pod → node); `redis_from_pods` (6 × `+PONG`, the bridge address); `admin_policy` (the 7 `reach` fields; Review Focus 1); `online` (time to 6,000, `failed`); `balance` (counts, `max_dev_pct`); `ipvs` (`active` per real server); `pods`; `processes`; the durations (`build`, `cluster`, `deploy`). A failing check: record it with its numbers and ask the user (AskUserQuestion) before any change; Review Focus names the likely causes.

- [ ] **Step 4: Record and commit**

In this plan's *Execution Notes*: the dry run and the host run as in Step 3. In the roadmap: the Plan 6 row (6d1 executed, the run and its result) and #151's outcome. Commit (docs), push.

---

## After this plan

- **6d2** (rolling restart at 1M × `TIER3_SCALE`, drain assertions, the fresh-gateway funnel on IPVS `lc`/`wlc`) is written against this run's results: kind's startup time, the pinned layout, the IPVS view.
- **6d3** (node loss, disruption budgets) builds the node-loss controller (#149.3) on these manifests.

## Execution Notes

Not executed yet.
