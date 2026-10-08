# 子项目 1：连接与会话核心 —— 设计（第 6 版）

> 本文是 `2026-10-03-connection-session-core-design-rev6.md` 的中文译本。两者不一致时，以英文版为准。标识符、命令、Redis key、指标名和引用标记（如 *(R1-O4)*）保持英文原样。

> 本版是 `2026-10-03-connection-session-core-design-rev5.md` 的第 6 版修订，rev5 保持不变。本版落实了对 rev5 的系统设计评审 `docs/superpowers/objections/connection-session-core-design-rev5-system-design.md` 的全部决议，决议记录在 `docs/superpowers/objections/connection-session-core-design-rev5-dispositions.md`，引用标记为 *(R5S-O1)* … *(R5S-O9)*。以下说明沿用自 rev5。

> 本版是 `2026-10-03-connection-session-core-design-rev4.md` 的第 5 版修订，rev4 保持不变。本版落实了用户本人对 rev4 的评审 `docs/superpowers/objections/connection-session-core-design-rev4-human-view.md.md`，其决议直接写入本规格，引用标记为 *(R4H-O1)* 等；用户对 rev4 剩余待定问题的决议标记为 *(R4H-OP1)*。本版还落实了 `docs/superpowers/objections/connection-session-core-design-rev4-dispositions.md` 中记录的、针对 rev4 两份外部评审的全部决议，引用标记为 *(R4M-<n>)*（MiniMax）和 *(R4G-O<n>)*（GPT）。以下说明沿用自 rev4。

> rev4 是 `2026-10-03-connection-session-core-design-rev3.md` 的第 4 版修订，rev3 保持不变。rev4 落实了 `docs/superpowers/objections/connection-session-core-design-rev3-dispositions.md` 中的全部决议（第三轮评审的 O1–O7，以及它对 rev3 待定问题 4–6 的澄清），以及同一文件中记录的、针对 rev4 自身待定问题的后续决议（*R3-OP2*、*R3-OP4*–*R3-OP7*）。每轮评审的异议都从 O1 开始编号，因此引用带前缀：*(R1-O4)* 指 `connection-session-core-design-dispositions.md`（第一轮评审），*(R2-O4)* 指 rev2 决议（第二轮评审），*(R3-O4)* 指 rev3 决议（第三轮评审）。需要自行解读的地方列在文末的 **待定问题** 中。

## 背景

我们要为一款类微信社交 App（Android、iOS、Web）构建 Go 语言的 IM 后端，目标约 100 万同时在线用户。用户此前用 C++ 做过一个支撑 260 万 TCP 连接（单网关 60 万）的 IM。两份早期草稿（`gpt_design.md`、`claude_design.md`，在 `4ce1fa6` 中加入，在 `05cd570` 中删除）在在线模型和范围上有分歧，并且都没有解决"延迟断开删除新会话"的竞态。

整个系统拆分为 5 个子项目（1 连接/会话核心，2 消息，3 推送链路，4 社交动态，5 扩容/加固）。**本文只覆盖子项目 1**：建连、加密握手、登录、心跳、踢下线、吊销、断开——在重连竞态下保持正确，并满足风暴恢复 SLO。子项目 1 交付基于 TCP 的 Android/iOS；Web 传输在此设计，之后再实现。

**成功标准**
- **容量上限** *(R1-O1)*：单台网关承载 60 万连接（测量 CPU / 内存 / GC）。这是压测基准，不是运行点。
- **风暴 SLO** *(R1-O1, R1-O7, R2-O3)*：覆盖**单台网关**丢失，按 20 万的运行点计算，按故障类别定义：

  | 故障类别 | 客户端如何发现 | SLO |
  |---|---|---|
  | 进程崩溃 / Pod 被杀（节点存活） | 内核 RST/FIN，经四层 NLB 透传 | 99% 在**网关死亡后** 30 秒内重新登录成功（测试中：loadgen 发出 SIGKILL 的时刻，记为 `T_death`；生产中：由死亡 epoch 交叉核对近似）*(R4M-17)* |
  | 节点丢失 / 静默网络分区 | 客户端心跳（Pong 超时） | 99% 在**客户端发现故障后** 30 秒内重新登录成功。发现时间由设计保证有上界：最多一个心跳间隔加 Pong 超时。 |

  算术：20 万用户分散在 0–20 秒的重连抖动中，约每秒 1 万次登录，在每秒 2 万的峰值余量之内。SLO 以默认风暴窗口（0–20 秒）为前提；通过 `LoginResp` 放宽该窗口 *(R5S-O6)*，是有意用重新登录时间换取负载。

  同时失去多台网关（例如整个可用区）属于**降级、尽力而为、无 SLO** *(R2-O3)*：客户端可能被 `at_capacity` 拒绝，并持续重试，直到丢失的 Pod 被重新调度。

  **全体用户重连** *(R5S-O6)*（LB 故障切换或重置、某地区或运营商网络恢复）同样是**尽力而为、无 SLO**。预期恢复时间 ≈ 用户数 ÷ 准入的登录速率（100 万 ÷ 每秒 2 万 ≈ 50 秒），再加上触及拒绝上限的客户端的退避长尾。准入控制（握手令牌桶、连接上限、登录队列上限）对其整形；任何组件都不得因此失败或宕掉。

  **节点丢失恢复** *(R5S-O3)*：节点丢失后，替代网关在节点丢失阈值（默认 2 分钟）加上网关启动时间内（合计约 2–3 分钟）变为 Ready，之后集群回到 N 台（见 *部署要求*）。
- **正确性：** 任何过期事件（延迟断开、同设备或其他设备的延迟登录、僵尸网关、延迟的 Reconcile 恢复、延迟的清扫）都不能删除或覆盖更新的会话；由自动化测试覆盖。

**密度** *(R1-O1, R2-O2, R2-O3)*：运行点为单网关最多 20 万连接。100 万用户至少需要 6 台网关（5 台各 20 万，加 1 台用于 N−1），稳态下每台约 16.7 万。在 Kubernetes 上 Pod 很便宜，真正的代价是爆炸半径；60 万仍作为效率基准。单网关硬连接上限（默认 25 万，见 *准入控制*）限制了倾斜：一台网关宕机时，其余五台最多承载 5 × 25 万 = 125 万 ≥ 100 万；整个可用区宕机（剩 4 台）时仍能勉强承载 100 万。**25 万是一个配置值** *(R4M-8, R4G-O2)*：只有当压测 (a) 表明在生产网关的核数下上限至少为 60 万时，它才成立（在 Tier 2 主机上按核测量，再按生产节点规格换算，见 *测试执行模型*）。如果实测上限更低，则在上线前据此重新推导运行点、连接上限和网关数量，保持相同比例（运行点 ≈ 上限 / 3；连接上限介于运行点和上限之间）。

## 决定（已达成一致）

| 主题 | 决定 |
|---|---|
| 在线模型 | 每个 **槽位（slot）** 一个会话，slot = `userid + appid + platform`（`mobile` 涵盖 Android 和 iOS；另有 `pad`、`web`、`pc`）。同一槽位上的新登录替换旧登录。platform 会与 token 校验。不同平台是不同槽位，永不冲突。 |
| 客户端传输 | 移动端：通过 gnet 的原生 TCP。Web：基于 wss 的 WebSocket（TLS 在云 LB 终结），现在在传输接口之后完成设计（见 *网关 → 传输接缝* *(R4G-O5)*），作为后期切片实现。 |
| 客户端加密 | 自定义应用层加密，使用 **Noise Protocol Framework 的 NK 模式**（`flynn/noise`）：服务端静态公钥固定在 App 中，带 key ID 以便轮换（见 *网关 → Noise 密钥操作手册* *(R4G-O6)*）；每帧使用 ChaCha20-Poly1305。不做会话恢复票据（一次握手约 100µs，风暴中可忽略）。Web 依赖 wss 的 TLS，不使用 Noise。**为什么移动端不用 TLS** *(R4G-O3)*：Go 的 `crypto/tls` 需要阻塞式 `net.Conn`（每个连接一个 goroutine，违背 gnet 模型），而 gnet v2.10.0 没有 TLS；在 NLB 上终结 TLS 会让 NLB 与 Pod 之间是明文，并把 100 万个 TLS 会话压到 LB 的限额和计费上；Noise NK 只需一次往返（约 100µs），没有证书链，编解码由网关自己掌握。如果压测 (a) 表明 Noise 开销过大，回退方案是在 NLB 上终结 TLS。 |
| 内部 RPC | 双向 gRPC。网关→Logic：`dns:///` headless service + `round_robin`；`Login` 对每个条目在其完成后立即流式返回结果，因此慢的 Redis 分片只会拖住它自己的条目 *(R5S-O1)*；Logic 服务端设置 `MaxConnectionAge` ≈5 分钟，**以及 `MaxConnectionAgeGrace` = 30 秒**（大于 6 秒的 Login 超时），使进行中的批次在 GOAWAY 后能完成 *(R1-O12)*。`GatewayLease` 使用独立的 gRPC 连接 *(R1-O2)*。Logic→网关：Kick 按 StatefulSet Pod DNS（`gw-17.gateway.im.svc`）拨号，无需注册中心；网关的 headless service 设置 `publishNotReadyAddresses: true`，使正在下线的 Pod 仍可被解析 *(R1-O8)*。 |
| 认证 | 独立的账号服务签发 Ed25519 签名的 JWT，作为**短期访问 token（例如 1 小时），只在 Login 时校验**。token 过期不影响已认证的连接；长连接的安全依靠吊销 *(R1-O10)*。Logic 用按 `kid` 索引的 JWKS 在本地校验 token（登录时不查数据库），冷启动时可回退到 ConfigMap *(R2-O7)*。`cmd/devtoken` 签发 token 并发布测试用 JWKS。 |
| 事实来源 | 三个事实，三个归属方 *(R4M-11)*：**这条连接存在吗？**——网关；**哪条连接拥有这个槽位？**——`sess:{uid}`，通过周期性反熵（`Reconcile` + `ReconcileEnd`）与网关对齐 *(R1-O3, R3-O1)*；**这个网关 epoch 还活着吗？**——`gwlease:gw:{epoch}`（`gwepochs` 只是一个参考性索引 *(R4M-3)*）。 |
| 部署 | 云上 Kubernetes。网关以 StatefulSet 运行在四层 NLB 之后，在 `topology.kubernetes.io/zone` 和 `kubernetes.io/hostname` 上设置 `topologySpreadConstraints`，`maxSkew: 1`，使单节点故障是真正的 N−1 事件 *(R2-O3)*。Logic 是带 HPA 的无状态 Deployment；它的 `minReplicas` 必须**在不扩容的情况下承受每秒 2 万次登录的峰值**，以压测测得为准。HPA 在此之上扩容；SLO 不依赖它 *(R2-O7)*。中断预算、节点丢失恢复、LB 算法、Redis 部署位置以及生产节点与 LB 限制：见 *部署要求* *(R5S-O3, R5S-O4, R5S-O5, R5S-O7, R5S-O9)*。 |
| 分层规则 | 网关从不访问 Redis。只有 Logic 访问 Redis。 |
| 舍弃 | 按用户的登录限流、分布式锁、反向映射、会话版本号、按 IP 限流（各自理由见下文）。 |

## 架构

```
Android/iOS ─TCP──┐
                  ├─ 云 NLB ─► 网关 StatefulSet（gnet loop · Noise · 分帧 · 心跳 · 准入控制）
Web ─wss (TLS)────┘                 │ gRPC Login/Logout/Reconcile（批量）、       ▲ gRPC Kick（Pod DNS）
                                    │      ReconcileEnd                          │
                                    │ gRPC GatewayLease（独立连接）               │
                                    ▼                                            │
                              logic Deployment（无状态，HPA）────────────────────┘
                                    │                ▲ gRPC Revoke（账号服务）
                               Redis Cluster
```

仓库：一个 Go module，包含 `cmd/{gateway,logic,devtoken,loadgen}`、`internal/...`、`api/{client,internal}.proto`、`deploy/{compose,k8s}`。

### 部署要求

- **负载均衡算法** *(R5S-O4)*：云 LB 在提供**最少连接**算法时使用它（例如阿里云 CLB/NLB 的 `wlc`、腾讯云 CLB 的 `least_conn`）。若 LB 只做流哈希（例如 AWS NLB），则由 SDK 的握手前重试规则（见 *客户端 SDK 规则*）在滚动重启期间保住 SLO，而倾斜会一直存在，直到连接自然流转把它摊平（主动再均衡属于子项目 5）。原因：滚动重启期间，不感知负载的 LB 会把尚未重启的 Pod 推到上限；6 个 Pod 在 100 万时滚动，从第三步起 5 个目标中有 3 个已满。
- **中断预算** *(R5S-O5)*：
  - 网关 PodDisruptionBudget `maxUnavailable: 1`。有意的副作用：如果已有一台网关不可用（例如节点丢失后），PDB 会阻止后续的自愿驱逐，直到它恢复，因此升级会等待，而不是把 1 台网关的丢失变成 2 台；
  - 网关 Pod 带 `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"`，使 autoscaler 永远不会自行移除网关节点；网关容量只通过 StatefulSet 变化；
  - Logic PodDisruptionBudget `minAvailable` = `minReplicas`，因为风暴 SLO 假定这些副本处于 Ready。
- **节点丢失恢复** *(R5S-O3)*：在 Pod 被确认消失之前，StatefulSet 不会重建位于不可达节点上的 Pod。一个控制器（一个小型 operator，或云厂商的节点自动修复——前提是它施加同样的污点）对运行网关 Pod、且 NotReady 超过节点丢失阈值（默认 2 分钟，可配置）的节点施加 `node.kubernetes.io/out-of-service=nodeshutdown:NoExecute`。随后 Kubernetes 删除该 Pod 及其卷挂载，StatefulSet 在另一个节点上重建 `gw-k`。若无法施加污点，回退方案是强制删除 Pod（`--grace-period=0 --force`）。节点恢复或被替换后移除污点。这是安全的，因为设计从不依赖"每个 Pod 名最多一个实例"：仍在被隔离节点上运行的旧 `gw-k` 拥有不同的 epoch，它的租约、Kick 和 Redis 条目都不会与新实例冲突（竞态 4）。结束卡住的 Pod 也会移除它的 Endpoint，因此 Kick 不再解析到一个死 IP。
- **Redis 部署位置** *(R5S-O9)*：Redis Cluster 有**分布在 ≥ 3 个可用区的 ≥ 3 个主节点，且每个主节点的副本与其主节点位于不同可用区**，因此丢失一个可用区永远不会失去多数主节点，且每个主节点在其所在可用区之外都有副本。若使用托管 Redis，在部署配置中写明对应的多可用区设置。
- **生产节点与 LB 限制** *(R5S-O7)*，均由配置的上限（`maxConnsPerGateway`）推导：
  - 容器 `nofile`（`RLIMIT_NOFILE`）≥ 上限 + 余量（默认余量 10%，用于 gRPC、待处理集合和正在关闭的 socket）；
  - 每个网关节点 `nf_conntrack_max` ≥ 2 × 上限，**或**对网关端口禁用 conntrack（`NOTRACK` / CNI 等效机制），并在网关配置中声明为 `conntrack: notrack`；
  - `net.core.somaxconn` 和 gnet 的监听 backlog 足以承受每台网关的 accept 突发（≥ 4096；由压测 (b1) 确定）；
  - 云 LB：并发连接数 ≥ 集群总上限（默认 6 × 25 万 = 150 万；N−1 时 ≥ 125 万），每秒新建连接数 ≥ 它必须整形的全体用户重连速率 *(R5S-O6)*，而不仅是每秒 2 万的风暴峰值；
  - LB 空闲超时 > 心跳间隔并留有余量（默认 150 秒）：存活的客户端至少每个间隔发送一帧，因此 LB 不会看到它空闲更久。若 LB 的超时固定且更低，则下调 `LoginResp` 中的心跳间隔以适配。

  网关会强制检查它能读到的两项限制（见 *就绪*）。LB 限制无法从 Pod 内读取，由 Tier 3 `cloud` 运行检查。

## 客户端协议

- **外层帧** *(R4H-O1)*：固定 6 字节帧头，后接 payload。所有字段均为大端序。网关缓冲不完整的帧，并拆分一起到达的多个帧。

  ```
  +---------+---------+---------+---------+---------+
  | Version |   Cmd   |  Flags  |  Length | Payload |
  | (1 B)   | (2 B)   | (1 B)   | (2 B)   | (N B)   |
  +---------+---------+---------+---------+---------+
  ```

  - `Length` 只计 payload，不含帧头（最大 65535）。
  - `Version`：不支持的版本在握手前关闭，并计一次 `bad_version` 拒绝；客户端把这次关闭当作握手前关闭处理（见 *客户端 SDK 规则*）。
  - `Flags`：v1 中所有位都保留且必须为 0；任何位被置位的帧按坏帧关闭。
  - `Cmd` 无需解密即可读取，因此网关可以在任何加密运算之前拒绝或分发一个帧。它会向旁观者暴露命令类型；帧大小和时序本来就已暴露了其中的大部分。
- **第 1 帧**：`Cmd = HANDSHAKE`，payload 为 `key_id(2) || noise_msg`（原始 Noise NK 消息），没有 `req_id`，不做 AEAD。`key_id` 放在 Noise 消息之外，因为网关需要先用它选出自己的静态密钥，才能处理该消息 *(R4G-O6)*。帧头加 `key_id` 作为 Noise prologue 混入握手哈希，因此两者都受认证。**握手消息不携带任何应用数据**（LoginReq 是握手之后的第一帧）*(R4G-O6)*。
- **之后的每一帧**：payload = AEAD-seal(`req_id(4) || body`)，每个方向各有一个 nonce 计数器。6 字节帧头作为 AEAD 关联数据（associated data）传入，因此被篡改的 `Version`、`Cmd`、`Flags` 或 `Length` 都会导致解密失败，连接随即关闭。`Length` 包含 16 字节的认证标签，因此 body 最大为 65535 − 4 − 16 = 65515 字节。
- **Web**：每条 WebSocket 二进制消息携带相同的帧头和 payload（为统一起见保留 `Length`）。没有 Noise，因此 payload 是明文 `req_id || body`，也没有 AD 绑定；由 wss 的 TLS 保护。
- 命令（`Cmd`）：`Login`、`LoginResp`（包含心跳间隔和重连窗口，见下文）、`Ping/Pong`、`Logout`、`LogoutResp`（服务端发出：表示网关已接受登出，而不是 Redis 已更新 *(R4H-O8)*）、`Kick{reason}`（服务端发出）、`Reconnect{jitter_ms}`（服务端发出；客户端关闭连接，在 `[0, jitter_ms]` 内均匀随机等待，然后以 `reconnect_reason = Reconnect from drain` 重连 *(R4H-O3)*）、`Error`、`Busy{retry_after}`。
- **`LoginResp` 中的重连窗口** *(R5S-O6)*：除心跳间隔外，`LoginResp` 还携带 SDK 各类等待所用的窗口：风暴等待（默认 0–20 秒）、本地触发（默认 0–1 秒）、握手前重试（默认 0.5–1.5 秒 *(R5S-O4)*）和其他可重试拒绝（默认 1–3 秒）。每个字段都是可选的；缺省的字段保持 SDK 当前值。运维因此可以在事故期间或测量之后放宽某个窗口，而无需发版（持久化和钳制见 *客户端 SDK 规则*）。
- `LoginReq` 携带：
  - `token`、`appid`、`platform`；
  - `device_id`（见下文）和 `conn_seq`（设备上的计数器，每次连接尝试递增）；
  - `reconnect_reason` *(R1-O7)*：枚举——首次连接、RST/FIN、Pong 超时、Busy 重试、因下线收到 Reconnect，……。握手完成前服务端关闭连接后的重试上报为 `Busy retry` *(R3-O5)*；
  - `offline_ms` *(R1-O7)*：客户端发现断线以来的毫秒数，基于客户端单调时钟（是一段时长，因此手机与服务器时钟无需一致）。
- 登录错误（在 `Error` 中）：

  | 错误码 | 含义 | 客户端动作 |
  |---|---|---|
  | `STALE` | 本设备一个更新的连接已持有该槽位。携带已存的 `conn_seq` *(R1-O5)*。 | 把计数器设为 `max(local, stored) + 1`，持久化，退避后重试。 |
  | `SUPERSEDED` | 同一槽位上另一台设备登录得更晚 *(R1-O4)*。 | 可重试；重试是一次新的、更晚的登录，会正常胜出。 |
  | `EXPIRED` | 登录到达 Redis 的时间比网关收到它晚了超过 `login_deadline` *(R1-O4)*。这是一个背压信号。 | 放弃这次 Login 及其连接；以新的 `conn_seq` 重连，按**可重试拒绝**处理（见 *客户端 SDK 规则*）*(R4H-O2)*。 |
  | `REVOKED` | token 签发时间早于用户的吊销标记 *(R1-O10)*。 | 丢弃 token，显示登录页。 |
  | `KEY_UNKNOWN` | token 的 `kid` 不在 Logic 的密钥集中；后台可能正在刷新 *(R4H-O10)*。 | 可重试拒绝（见 *客户端 SDK 规则*）。 |
- 踢下线原因。**任何一种都不会触发自动重连：**
  - `REPLACED`：同一设备，更新的连接；不向用户展示。客户端进入空闲，只在用户操作或 App 回到前台时重连。SDK 将其记录为异常，因为存活的客户端只有在另一个实例共用其 device_id 时才会看到它 *(R1-O6)*。
  - `KICKED_BY_OTHER_DEVICE`：向用户展示。
  - `REVOKED` *(R1-O10)*：客户端丢弃 token 并显示登录页。
- **device_id** *(R1-O6)* 标识一个持有连接的客户端实例。SDK 必须保证同一时刻每个 device_id 最多只有一条连接在用。移动端和桌面端：每次安装一个，外加进程级保护（例如 Android 上的单一连接服务）。服务端的正确性并不依赖这一点：违反它只会让这些实例在用户自己的槽位上互相替换（竞态 10）*(R4G-O1)*。

### 客户端 SDK 规则

- **conn_seq 持久化** *(R1-O5)*：在每次连接尝试**之前**持久化 `conn_seq`，存放位置和生命周期与 device_id 相同（例如两者都放在 iOS Keychain），这样清除一个就会清除另一个。
- **重连** *(R1-O7, R3-O5)*：
  - **连接丢失**（RST/FIN 或 Pong 超时）：意外失去一条**已建立的**连接后，首次尝试前**均匀分布**地随机等待 **0–20 秒** *(R4G-O12)*（为握手、登录和一次 Busy 重试留出 10 秒），之后指数退避（上限 5 分钟）。遵守 `retry_after`。客户端无法区分大规模断线和自己单独断线，因此不论原因都适用这条规则 *(R4H-O3)*。
  - **本地触发** *(R4H-O3)*：当操作系统报告网络变化，或 App 在断线状态下回到前台时，取消任何尚未结束的 0–20 秒等待，在均匀随机的 **0–1 秒**内发起尝试。这类事件在用户之间通常互不相关。当它们相关时（某个运营商或城市网络恢复），突发由网关准入控制和下文的重试规则整形，运维也可以通过 `LoginResp` 放宽本地窗口 *(R5S-O6)*。这也覆盖 Wi-Fi ↔ 蜂窝网络切换：切换会断开 socket 并排上一次 0–20 秒等待，随后的网络变化事件再把它提前。
  - **优先级** *(R4H-O3)*：本地触发只取消 0–20 秒的风暴等待。它不会缩短 `Busy{retry_after}` 的等待、`Reconnect{jitter_ms}` 的等待，也不会缩短达到重试上限之后的指数退避，因此网络来回抖动无法绕过服务端背压。
  - **`Reconnect{jitter_ms}`** *(R4H-O3)*：按服务端给定的 `[0, jitter_ms]` 均匀等待，而不是 0–20 秒；这是计划内的迁移，不是意外断线。之后的失败按常规规则处理。
  - **可重试拒绝** *(R3-O5, R4H-O2, R4H-O7, R4H-O10, R5S-O4)*：以下五种结果会被快速重试，使用**两个计数器**：
    - **握手前关闭**：服务端在 Noise 握手完成前关闭的 TCP 连接（`at_capacity`、`handshake_rate`、`bad_version` 和 `noise_unknown_key` 拒绝都在任何分帧之前关闭 socket，因此无法发送 `Busy{retry_after}`）。均匀随机等待 **0.5–1.5 秒**，然后重连。**连续 10 次**后，回退到指数退避（上限 5 分钟）。
    - **其他可重试拒绝**，共用第二个计数器：
      - `Busy{retry_after}`；
      - 被拒为 `EXPIRED` 的 Login；
      - 被拒为 `KEY_UNKNOWN` 的 Login *(R4H-O10)*；
      - Noise 握手之后、收到任何 `LoginResp` 或 `Error` 之前被关闭的连接（例如 `AUTH_PENDING` 超时，或失败时 `Error` 帧丢失）。客户端从未登录成功，因此这不属于风暴场景 *(R4H-O7)*。

      每种都均匀随机等待 1–3 秒（服务端给出 `retry_after` 时遵守它），然后重连。**连续 5 次**后，回退到指数退避（上限 5 分钟）。

    一种结果会清零另一种的计数器；登录成功后两个计数器都清零。**为什么用两个计数器** *(R5S-O4)*：一次握手前关闭只花网关一次 accept 和一次 close，没有加密运算，因此更多、更快的重试对服务端很便宜；当不感知负载的 LB 把客户端送到已满的网关时，正是它们保住了 SLO（5 个目标中有 3 个已满时，0.6¹⁰ ≈ 0.6% 的被拒客户端会进入退避，而旧的 5 次上限下为 0.6⁵ ≈ 7.8%；10 次重试约 10 秒，在 5 秒下线抖动之后仍来得及）。客户端无法区分 `at_capacity` 和其他握手前关闭；`handshake_rate` 仍由网关的令牌桶整形，而持续性的 `bad_version` / `noise_unknown_key` 只是晚几秒进入退避。

    背压下快速重试是安全的：一个客户端最多只有一次尝试在途；一轮 `EXPIRED` 本身就要 ≥ `login_deadline`，因此 Logic 越慢，重试频率越低；连续次数上限把持续的过载交给指数退避。只要 App 希望在线，客户端就不会永久放弃。
  - **服务端设定的窗口** *(R5S-O6)*：0–20 秒风暴等待、0–1 秒本地触发、0.5–1.5 秒握手前重试和 1–3 秒重试都是默认值；SDK 使用最近一次携带它们的 `LoginResp` 中的值。SDK 把它们与 `device_id`/`conn_seq` **持久化**在一起，并用于之后的每次等待，包括下一次登录之前。SDK 把每个值**钳制**在 App 内置的安全范围内（范围在计划中确定：风暴等待的上端被钳制在 10–120 秒，因为 20 万次重连若分散在不到 10 秒内会超过每秒 2 万的余量；本地触发 1–10 秒；重试 0.25–10 秒），因此错误的配置不会造成零抖动风暴或无界等待。新安装使用内置默认值。
  - **停止条件** *(R4H-O2)*：登出、切换账号和关闭 App 会取消所有尚未执行的重连或重试，不论其原因。
  - 每次尝试使用新的源端口，使 NLB 的流哈希可以选中另一台网关 *(R2-O2)*。
- **服务端失联检测** *(R1-O7)*：发出 Ping 后 **10 秒**内没有收到任何帧，就关闭并重连。
- **Web**（现在设计，之后实现）*(R1-O6)*：每个浏览器 profile 一条连接。各标签页通过 `SharedWorker` 选举 leader，回退方案为 Web Locks 加 `BroadcastChannel`。leader 持有 wss 连接并向其他标签页转发消息；leader 标签页关闭时，由另一个标签页接管。device_id 和 conn_seq 存放在 localStorage。

## 网关

- gnet 在 `go.mod` 中精确锁定为 **`github.com/panjf2000/gnet/v2 v2.10.0`** *(R4G-O7)*（需要 `Conn.EventLoop()`，v2.8.0 起提供）。任何 gnet 升级都必须先通过 `-race` 下的 loop 归属测试和 loopState 测试（Tier 1a）。不为每个连接开 goroutine。`OnTraffic` 从不阻塞；RPC 通过有界工作池执行。
- 连接状态：`HANDSHAKE → AUTH_PENDING → ONLINE → CLOSED`。
- **传输接缝** *(R4G-O5)*：网关核心（状态机、超时、心跳、除 Noise 限流外的准入控制、Login/Logout/Reconcile、Kick）只处理解码后的帧 `(cmd, req_id, body)`。一个传输把连接变成：一个打开事件、一串解码后的帧、`Send(cmd, req_id, body)`、`Close()`，以及一个"握手完成"信号，使连接从 `HANDSHAKE` 进入 `AUTH_PENDING`。
  - **TCP 传输**：按帧头的 `Length` 从字节流中切出帧；运行 Noise NK，帧头作为 AEAD 关联数据 *(R4H-O1)*；它的 `HANDSHAKE` 状态就是 Noise 握手。
  - **wss 传输（后期切片）**：一条 WebSocket 二进制消息恰好承载一个帧（`Length` 与消息大小不符即为坏帧）；TLS 由 LB 终结，没有 Noise；它的 `HANDSHAKE` 状态就是 HTTP upgrade。wss 跑在 gnet 上还是单独的监听器上，在 Web 切片中决定。
- **Noise 密钥操作手册** *(R4G-O6)*：
  - **服务端密钥集**：网关从 Kubernetes Secret 加载 `{key_id → 私钥}`（Readiness 依赖它），按握手中的 `key_id` 选择密钥。未知或已退役的 `key_id` 在握手前关闭，并计一次 `noise_unknown_key` 拒绝；客户端把它当作握手前关闭处理。
  - **轮换**（受 App 发版周期约束）：(1) 把新密钥加入 Secret 并滚动重启网关；(2) 发布同时固定当前密钥和新密钥的 App 版本；(3) 只有当所有仅固定旧密钥的 App 版本都低于最低支持版本时，才移除旧密钥。
  - **泄露**：NK 的会话密钥混入了 `ee`，因此被盗的静态密钥无法解密录下的流量（前提是握手消息不携带应用数据，见 *客户端协议*）。但它能让攻击者对固定了该密钥的 App 冒充服务端。应对：紧急发布带新密钥的 App 版本，然后从 Secret 中移除泄露的密钥；只固定了该密钥的 App 版本随后握手失败（可重试拒绝，之后退避），必须通过 App 自身的更新渠道升级，这不在本协议范围内。
- **标识：** `gateway_id` = Pod 名；`epoch` = 启动时随机生成的 uint64；`conn_id = (worker_id << 48) | loopState.counter++`。
  - 已对照 gnet **v2.10.0** 源码确认 *(R4G-O7)*：`OnOpen` 在两条 accept 路径上都运行在所属 loop 的 goroutine 上（主 reactor：`accept0` 通过负载均衡器选出 loop，把 `register` 投递到该 loop 的 poller，由它在那里执行 `register0 → open → OnOpen`；reuseport：负责 accept 的 loop 自己调用 `register0`）。loop 序号未导出，但 `Conn.EventLoop()` 给出了 loop 的身份。
  - **loopState 在启动时构建，只有绑定是惰性的** *(R4H-O5)*：启动时网关在一个切片中构建 `numLoops` 个 loopState（`numLoops` = 配置的 gnet `NumEventLoop`），每个都带 `worker_id = i`、预分配好的连接表和计数器。gnet 无法在启动时列出各个 loop（`Engine` 没有这样的方法，`OnBoot` 只拿到 `Engine`；已对照 gnet v2.10.0 确认），因此 `OnOpen` 第一次见到某个 loop 时，从切片中认领下一个 loopState（一次原子递增），并缓存在从 EventLoop 到 loopState 的 `sync.Map` 中。请求路径上不做任何重量级分配。出现超过 `numLoops` 个 loop 属于启动缺陷：网关直接 panic。
  - 按 loop 的连接表和计数器始终绑定在各自的 loop 上，从不从共享池中取用：正是这种绑定让它们无需加锁，而 Kick 的路由依赖 `conn_id >> 48`。
  - 普通的 `counter++` 之所以安全，只因为只有 `OnOpen` 会访问它。计数器不导出，并有一个 `-race` 测试来保证这条规则。
- **无锁的按 loop 连接表：** `map[conn_id]*conn`，只由所属 loop 访问。来自 gRPC 的 Kick 读取 `conn_id >> 48`，拿到该 loop 的句柄，调用 `loop.Execute(...)` 查找连接、检查 epoch 和状态并执行动作。这同时满足了 gnet 要求 Close 在所属 loop 上执行的规则。
  - **预分配** *(R4H-O4)*：每个 loop 的 map 用 `make(map[uint64]*conn, maxConnsPerGateway / numLoops × 1.5)` 创建，使其在风暴中不会扩容（分配和重哈希）。在默认 25 万上限下总计约 37.5 万个槽位，约 8–10MB。
    - 这是提示值，不是上限：超过它的 map 只会正常扩容；真正的上限是 accept 时的连接上限，因此提示值估错只影响性能，不影响正确性。
    - 1.5 倍系数用于覆盖 loop 之间的不均衡：gnet 的轮询负载均衡器（`gnet.WithLoadBalancing(gnet.RoundRobin)`，即默认值，这里显式指定）把 accept 均匀分开，关闭会让各 loop 逐渐不均，等待 Logout 栅栏的 `CLOSED` 条目（通常 ≤ `login_deadline + 1s`，不计入连接上限）也会多占一点。
    - 提示值随配置的连接上限变化，因此 60 万基准测试会自动得到更大的提示值。
  - **被跟踪的连接** *(R3-O1)*：连接从 accept 起一直留在其 loop 的表中，直到它不再需要 Logout——即它从未发送过 Login 就关闭了，或它被栅栏延后的 Logout 已被确认或已从重试队列中丢弃。等待 Logout 栅栏的 `CLOSED` 条目仍被跟踪。`ReconcileEnd` 上报的就是这个跟踪集合。
- **Kick 处理** *(R1-O8)*：
  - `ONLINE`：发送 Kick 帧并关闭。
  - `AUTH_PENDING`（Login 进行中）：设置带原因的 kick-pending 标记。Login 回复到达时，发送 Kick 帧并关闭，而不是进入 `ONLINE`。
  - `HANDSHAKE`、`CLOSED`、未知 conn_id 或 epoch 不匹配：忽略。
- **心跳：** **默认 150 秒，由服务端在 `LoginResp` 中下发**。任何帧都会重置计时器（惰性方式，见 *计时器*）。2 × 间隔 + 10 秒（= 310 秒）无响应即判定连接死亡，网关随即发送 Logout（受栅栏约束）。运行在所属 loop 的时间轮上。
- **超时** *(R1-O11, R4H-O7)*：`HANDSHAKE`（accept → 握手完成）和 `AUTH_PENDING`（握手完成 → 收到 LoginReq）。超时后静默关闭连接。两者都运行在所属 loop 的时间轮上（见 *计时器*）。
  - 两者都在网关 ConfigMap 中可配置（整个 StatefulSet 一个值）：默认 **5 秒**，允许范围 **3–30 秒**。任一超出范围时网关拒绝启动。3 秒下限足以覆盖弱移动网络上一次客户端→服务端传输加一次重传；限制慢速客户端靠的是仅为内存的待处理上限，而不是很短的超时。
  - `AUTH_PENDING` 计时器在收到 LoginReq 时停止。连接在其 Login 排队或在途期间仍处于 `AUTH_PENDING`，但这段等待由 `login_deadline` 和 Login RPC 超时管辖，因此积压的登录队列永远不会触发 `auth_timeout`。
- **计时器** *(R4H-O6)*：按连接和按块的计时器运行在时间轮上；全网关范围的周期性工作使用普通的 Go 计时器。

  | 计时器 | 归属 | 机制 |
  |---|---|---|
  | `HANDSHAKE` 5 秒、`AUTH_PENDING` 5 秒 | 每连接 | 时间轮 |
  | 心跳（310 秒后判定死亡） | 每连接 | 时间轮 |
  | Logout 栅栏 (b)：`recv_ms + login_deadline + 1s` | 每连接 | 时间轮 |
  | Reconcile 块截止时间（栅栏 c） | 每块 | 时间轮（每块一个条目） |
  | Logout / `ReconcileEnd` 重试队列（60 秒） | 全网关 | 独立 goroutine，`time.Timer` |
  | 每 10 秒的租约、约 10 分钟的周期式 Reconcile | 全网关 | 独立 goroutine，`time.Ticker` |
  | 清扫器 | Logic | Logic 自己的循环 |

  客户端的重试退避在 SDK 中运行。排队 Login 的 `login_deadline` 检查在其批次发送时进行，不用计时器。
  - **每个 loop 一个时间轮**，存放在 loopState 中，和连接表一样只在 loop goroutine 上访问：无需加锁，到期回调可以直接读取该 loop 的 map。由一个 ticker goroutine 每个 tick 向每个 loop 投递一次 `loop.Execute(advance)`。不使用 gnet 的 `OnTick`：它是整个引擎一个回调，不是每个 loop 一个。
  - **1 秒 tick，单层时间轮**，512 个槽；更长的延迟带一个圈数计数器。最短的计时器是 5 秒。
  - **绝不提前触发**：截止时间向上取整到下一个 tick。栅栏最多可能晚 1 秒触发，这是安全的；它绝不能提前触发。
  - **心跳惰性重挂**：收到帧只更新 `last_active`。条目到期时，loop 检查 `now − last_active`；若仍在 310 秒内，就按剩余时间重新挂上条目，否则判定连接死亡。每条连接每 310 秒一次时间轮操作，而不是每帧一次。
- **准入控制** *(R1-O11, R2-O2)*：
  - **连接上限** *(R2-O2, R3 澄清)*：单网关对打开的 **socket** 设硬上限，从 accept 到关闭计数，覆盖所有状态（可配置，默认 **25 万**；必须介于 20 万运行点和 60 万上限之间）。仅为等待 Logout 而保留在表中的已关闭条目不计入。达到上限时，网关在 accept 时、任何加密运算之前关闭新连接，并计一次 `at_capacity` 拒绝。实现上就是一个计数器加 accept 时的一次检查。上限以下的倾斜无害；主动再均衡不在范围内（子项目 5）。
  - **Noise 握手限流（单网关全局；绝不按 IP）：** 对同时进行的握手计算设并发上限（信号量，大小等于专用于握手的核数），再加一个全网关的每秒握手令牌桶，按实测 NK 开销（约 100µs）设定，避免握手拖垮已建立的连接。令牌桶必须至少允许每秒 2 万的风暴峰值并留有余量；在正常风暴中它起整形作用而非拒绝。任一耗尽时，网关不开始握手：关闭连接并计一次 `handshake_rate` 拒绝。客户端把这种关闭当作一次 `Busy` 重试（见 *客户端 SDK 规则*）*(R3-O5)*。
  - **登录队列上限：** 限制等待或正在调用 Logic RPC 的 Login 数量（反映 Logic 的背压）。达到上限时，回复 `Busy{retry_after}` 并关闭。**容量设定** *(R4H-O2)*：`login_queue_cap ≤ 每台网关实测的 Logic 登录吞吐 × (login_deadline − 余量)`，使排队中的 Login 无法到达截止时间。这样过载就表现为网关上代价很低的 `Busy`，而不是在 Logic 已经做完工作之后才返回的 `EXPIRED`；`EXPIRED` 只留给少见的停顿（GC 停顿、Redis 故障切换）。
  - **仅为内存的待处理上限：** 对 `HANDSHAKE` + `AUTH_PENDING` 连接总数设一个较大的上限，只为保护网关内存。正常风暴中不得触发。
  - `Busy{retry_after}` 使用较短的值，**1–3 秒，均匀分布** *(R4G-O12)*，使被拒客户端仍能落在 SLO 内 *(R1-O7)*。
  - 每条连接只允许一次登录尝试（失败即关闭）。
  - 排队时间超过 `login_deadline`（从 `recv_ms` 算起）的 Login 在发送前丢弃；它们从未到达 Logic，因此不需要 Logout。
- **recv_ms** *(R1-O4)*：网关在收到 Login 帧时打上 `recv_ms`（挂钟时间，NTP 同步），并在 Login RPC 中传递。
- **Login 批处理：** 每个 RPC 最多 200 个登录或 5 毫秒。批次限定的是多少条目一起发出，而不是它们何时返回：Logic 在每个条目完成后立即流式返回其结果 *(R5S-O1)*，因此慢分片上的条目永远不会拖住健康分片上的条目。Login 超时至少为 `login_deadline + 1s`（默认 6 秒），从 `recv_ms` 算起，并**按条目**生效 *(R1-O9, R5S-O1)*。超时已过、或流结束或失败时仍没有结果的条目，没有确定性回复，归入栅栏 (b)。
- **Logout 栅栏** *(R1-O9, R2-O1)*——网关不变式：只有当以下**两项**都满足时，网关才为一条连接发送 Logout。
  - **Login 条件**——满足其一：
    - (a) 它已有**明确的 Login 回复**（成功或拒绝），此时 Lua 写入要么已经发生，要么永远不会发生；或
    - (b) 已过了 `recv_ms + login_deadline + 1s`（1 秒用于吸收网关↔Redis 时钟偏差）。适用于**任何**没有明确回复的 Login：超时、RPC 错误、被 `MaxConnectionAge` 重置的 gRPC 连接，或 Logic 崩溃。
  - **Reconcile 条件** *(R2-O1)*：
    - (c) 没有任何包含该连接的 Reconcile 块尚未了结。一个块在收到明确回复，或已过 `sent_ms + login_deadline + 1s`（超时、RPC 错误、Logic 崩溃）时算了结，因为 Reconcile 的 compare-and-set Lua 在 `sent_ms + login_deadline` 之后拒绝写入。`refused` 条目结果也算明确回复 *(R3-O6)*。实现为每连接一个 `in_reconcile_chunk` 标记，构建块时设置，块了结时清除。

  - **客户端发起的 Logout** *(R4H-O8)*：栅栏同样适用。收到 `Logout` 时，网关回复 `LogoutResp`、关闭连接，并通过带栅栏的路径发送 Logout RPC。对 ONLINE 连接，栅栏 (a) 总是已经满足（`AUTH_PENDING` 只接受 LoginReq），因此只有栅栏 (c) 可能推迟它，而且只在其块在途期间（通常几毫秒，最多 `login_deadline + 1s`）。如果豁免它，在途的块可能在删除之后按规则 4 恢复会话，给一次明确的登出留下最多一个周期的幽灵条目。客户端无需等待 Redis 删除：切换账号用的是另一个 `sess:{uid}` key，同一槽位上的下一次登录也会替换任何残留条目。

  在栅栏满足之前关闭的连接保留一个待发 Logout，栅栏满足时发出。Logout 经过有界重试队列（60 秒）；丢弃是安全的，因为下一轮 Reconcile 会修复 *(R1-O3, R3-O1)*。
- **租约** *(R1-O2, R3-O3)*：每 10 秒在**专用 gRPC 连接**上调用一次 `GatewayLease`，从不与 Login/Logout/Reconcile 合批或排在它们之后。回复说明租约是 **live** 还是 **missing**：只要续期时发现租约 key 不存在，就是 `missing`，不论 `gwepochs` 成员处于什么状态。续期本身会重新登记该 epoch（租约 key 和 `gwepochs` 分数）。收到 `missing` 时，网关立即启动一轮触发式 Reconcile。启动后的第一次租约调用总会发现 key 不存在（新 epoch）；网关忽略那一次的 `missing`，既不计数也不处理 *(R3-OP5)*。租约过期但从未被清扫的情况会多花一轮不必要的触发式对账，由于 TTL 的推导方式，这很少见 *(R2-O5)*。每次调用都携带网关的挂钟时间 `gw_now_ms`，用于时钟偏差测量 *(R4M-5)*。
- **Reconcile** *(R1-O3, R1-O8, R1-O10, R2-O1, R2-O8, R3-O1, R3-O6)*：网关把它的 ONLINE 集合——`(uid, appid:platform, conn_id, device_id, conn_seq, login_ms, tok_iat)`——分成限速的块发送，然后以 `ReconcileEnd` 结束本轮。
  - **轮次机制** *(R4H-O9)*：一轮由**网关中的一个轮次控制 goroutine** 执行（绝不由 Logic 执行：只有网关进程内的代码能调用 `loop.Execute`）。所有连接表访问都发生在所属 loop 上，因此没有数据竞争，也不需要锁。
    - **键快照**：轮次开始时，对每个 loop 执行一次 `Execute`，把该 loop 的 ONLINE conn_id 复制到本轮队列中，队列**按 loop 分区**。每个队列条目为 `(conn_id, requeues)`，初始 `requeues = 0`。只复制 ID：Go 的 map `range` 无法跨多次 `Execute` 暂停再继续，而一开始就读取并标记所有条目，会让每个 Logout 在整轮期间都被栅栏 (c) 挡住。
    - **构建块**：控制器从**某一个** loop 的分区中最多取 N 个 ID，各 loop 之间轮流取（跳过已空的分区），因此每个块恰好花费该 loop 上的**一次 `Execute`**。回调逐个查找 ID，跳过已不是 ONLINE 的连接，复制当前字段并设置 `in_reconcile_chunk`，然后通过带缓冲的 channel 把块交回，loop 不会阻塞。块由控制器按本轮的限速发送，绝不从 loop 中发送；可以同时有多个块在途。重新入队的条目同样计入本轮的限速：重新入队只会拉长一轮，绝不会提高其速率 *(R4G-O8)*。
    - **了结块**：块了结时（收到回复，或已过截止时间），在其所属 loop 上执行**一次 `Execute`**：清除 `in_reconcile_chunk`，对 `not_owner` 连接发送 Kick 并关闭，并放行栅栏 (c) 挡住的 Logout。重新入队在控制器上进行：对每个 `refused` 条目，若 `requeues < 3`，把 `(conn_id, requeues + 1)` 放回其 loop 的分区，否则丢弃并计一次"达到重新入队上限"。这些计数只存在于本轮队列中（一轮期间约 16 字节 × 20 万 ≈ 3.2MB），轮次结束时丢弃。
    - **开销**：在 20 万连接、16 个 loop、每块 200 条的情况下，每轮约 2000 次块 `Execute`，加上快照和收集各 16 次；周期式约每秒 3.3 次，触发式约每秒 50 次。每次是一次任务队列投递，加上约 10–20µs 的 loop 时间。
    - **收集跟踪集合**：轮次结束时，控制器先打上 `as_of_ms`，**然后**对每个 loop 投递一次 `Execute`，收集该 loop 的跟踪 conn_id，再合并各片段（conn_id 唯一，无需去重）。安全规则：**每个片段都必须在 `as_of_ms` 当时或之后取得**。这样取得的片段中缺失的 conn_id 已永久通过其栅栏；而 Login 在 `as_of_ms` 之后才被收到的连接，受 `login_ms < as_of_ms` 检查保护。在轮次结束时收集（而不是扫描过程中收集）保住了竞态 16：一轮中任何时刻被丢弃的 Logout 都由该轮修复。
  - **实时块** *(R2-O1)*：每个块在发送的那一刻从按 loop 连接表中读取（通过 `loop.Execute`，见 *轮次机制*），而不是从本轮开始时冻结的快照中读取。每个块带 `sent_ms`（发送时的网关挂钟时间）。构建块时对其中每条连接设置 `in_reconcile_chunk`（见 Logout 栅栏）。
  - **逐条结果** *(R3-O6)*：Logic 对每条返回 `written`、`unchanged`（compare-and-set 输给了并发修改）*(R3-OP7)*、`refused`（超过截止时间）或 `not_owner`（附原因）。这四种都算栅栏 (c) 的明确回复。对每条 `not_owner`，网关发送带该原因的 Kick 帧并关闭连接。`refused` 条目的 conn_id 放回本轮队列，在其新块发送时从实时表中重新构建；如果连接已不是 ONLINE 则跳过。每条在一轮中最多重新入队 3 次，因此一轮总会结束；之后留给下一轮。
  - **轮次结束** *(R3-O1)*：当所有块（包括重新入队的条目）都已发送并了结，一轮即完成。网关随后打上 `as_of_ms`（网关挂钟，与 `recv_ms` 同一时钟），再收集它仍在**跟踪**的所有 conn_id（见 *按 loop 连接表*），不论状态，包括 Logout 尚未发出的已关闭连接，顺序不可颠倒（见 *轮次机制*）*(R4H-O9)*，然后发送 `ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`。`ReconcileEnd` 在 Logic 的删除阶段完成后才返回，因此其 RPC 截止时间要覆盖这个限速的阶段（≥ 被跟踪 uid 数 ÷ 删除阶段速率 + 余量）*(R5S-O8)*。若 `ReconcileEnd` 失败，用相同 payload 通过与 Logout 相同的有界重试队列（60 秒）重试，之后丢弃；下一轮会补上遗漏 *(R3-OP4)*。延迟重试是安全的：在 `as_of_ms` 时不在列表中的 conn_id 已通过其 Logout 栅栏，且 conn_id 永不复用，所以之后不可能再出现针对它的写入。被丢弃的 `ReconcileEnd` 会让该轮的幽灵会话上界延长到两个周期。大小：8 字节 × 25 万（上限）≈ 2MB，低于 gRPC 默认的 4MB；在 60 万基准下需拆分列表或调高上限。
  - **两种节奏** *(R2-O8)*，均可配置：
    - **周期式**：约每 10 分钟一次，每台网关带随机抖动，分摊在整个周期内（20 万时约每秒 330 条）；
    - **由 `missing` 租约回复触发**：每台网关约每秒 5000 条（20 万约 40 秒；若 6 台网关同时进行，全集群约每秒 3 万次 Lua 调用）。
  - **同一时间只跑一轮** *(R3 澄清)*：每台网关最多同时运行一轮。周期轮进行中收到 `missing` 回复会取消它：被取消的轮次不再构建或发送任何块，也不发送 `ReconcileEnd`。触发式轮次在被取消轮次的在途块全部了结后（≤ `login_deadline + 1s`）开始，因此一条连接永远不会同时处于两个未了结的块中。
- **就绪（Readiness）** *(R4H-O5)*：只有当到 Logic 的 gRPC 连接（Login/Logout/Reconcile 通道和专用租约通道）已建立、Noise 静态密钥集已加载、且第一次 `GatewayLease` 调用已成功之后，网关才报告 Ready，NLB 才会向它分发流量。没有客户端需要承担冷启动的代价。
  - **限制自检** *(R5S-O7)*：网关启动时读取 `RLIMIT_NOFILE`，并在未配置 `conntrack: notrack` 时读取 `/proc/sys/net/netfilter/nf_conntrack_max`。任一项低于配置上限所需（见 *部署要求*），网关就保持 NotReady，并记录哪项限制不满足以及所需的值。`nf_conntrack_max` 是节点级的；文件不存在表示未加载 conntrack，视为通过。
  - 是否对每连接对象（conn 结构体、Noise 加密状态、写缓冲）做池化，留给压测 (a) 的性能剖析决定。
- **优雅下线：** 收到 SIGTERM 后，变为 NotReady 并停止接受连接，在约 60 秒内分批发送 `Reconnect{jitter_ms}`（默认 `jitter_ms` = 5 秒，可配置 *(R4H-O3)*），关闭剩余连接，然后调用 `EpochEnded`。`terminationGracePeriodSeconds` 必须长于下线窗口。驱逐（节点升级、`kubectl drain`）走同样的 SIGTERM 下线流程，且网关 PDB 一次只允许一个 *(R5S-O5)*，因此宽限期也限定了一次驱逐会让节点升级等待多久。正在下线的网关仍能收到 Kick（`publishNotReadyAddresses`）。

## Logic

- 无状态。每个节点都处理所有 RPC，并能踢任何网关上的连接。水平扩展（HPA 基于 CPU）；`minReplicas` 自身就能承受风暴峰值 *(R2-O7)*。
- **RPC** *(R1-O12, R3-O1)*：`Login`（批量请求，按条目流式返回结果 *(R5S-O1)*）、`Logout`（批量）、`GatewayLease`（独立通道和**独立工作池**，使 Login 负载无法饿死它 *(R1-O2)*）、`EpochEnded`（`DEL` 掉该 epoch 的租约 key，使该 epoch 立即失效，清扫器的租约 key 检查也立即通过；此时网关已完成下线 *(R4M-3)*）、`Reconcile`（批量或流式）、`ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`、`Revoke`。
- **Login：** 在本地校验 JWT（按 `kid` 查 JWKS），然后用 `recv_ms` 和 token 的 `iat` 执行登录 Lua 脚本（EVALSHA，管道化），再做后续处理：SADD 到 epoch 索引；如果结果需要踢人，使用下文的加固 Kick 发送。
  - **按条目返回结果** *(R5S-O1)*：每个条目的脚本各自执行，完成后立即单独流式返回结果；Logic 从不等待整个管道完成再回复。原因：跨 Redis Cluster 的管道要等每个节点都应答才返回，因此一个分片故障切换会拖住每个批次，把 1/N 的故障变成所有用户的登录故障。
  - **结果之后再 Kick** *(R5S-O2)*：Kick 后续处理在该条目的结果返回**之后**才入队；Login 结果从不等待 Kick。
- **GatewayLease** *(R3-O3)*：用 `SET gwlease:gw:{epoch} 1 PX <TTL> GET` 续期并更新 `gwepochs` 分数。旧值为 nil 表示 key 不存在：回复 `missing`；否则回复 `live`。**时钟偏差测量** *(R4M-5)*：在租约 key 所在分片上读取 Redis `TIME`，导出 `clock_skew_ms{gw} = redis_time − gw_now_ms`（RPC 延迟会带来几毫秒噪声）；|偏差| > 500ms（预算的一半）时告警。节点的 NTP 指标覆盖不到托管 Redis 的时钟；而租约 RPC 本来就每台网关每 10 秒一次，走专用的低延迟路径。
- **加固 Kick** *(R1-O8, R2-O6)*：
  - **第一次尝试总是发送**，即使目标的租约看起来已过期（网关上的 epoch 不匹配会使误发的 Kick 无害）。
  - **重试**（传输错误时，约 30 秒内最多 3 次）只在目标 `gw|epoch` 租约存活时进行。Logic 把租约状态缓存约 1 秒；epoch 只有寥寥几个。
  - Kick RPC 采用 **fail-fast**（不使用 `WaitForReady`）。
  - **每次尝试的截止时间** *(R5S-O2)*：每次尝试（首次和重试）都有自己的截止时间（默认约 1 秒，可配置），覆盖建连和 RPC。原因：节点丢失时，死 Pod 的 DNS 名仍解析到一个永不应答的 IP，否则在那里的 gRPC 建连会阻塞长达 20 秒。
  - **每个目标一个有界队列** *(R5S-O2)*：Kick 按目标 `gw|epoch` 放入有界队列（大小可配置；在计划中确定），由每个目标有限数量的 worker 消费；重试走同一队列。队列满时丢弃该次尝试并计数（`kick_dropped{target_lease=live|dead}`）；Reconcile 兜底（竞态 8）。这保留了 R2-O6 "首次尝试总是发送"的规则——它保护被饿死但仍存活的网关——同时限定了它在风暴中的开销（每次重新登录约一次发往死亡 epoch 的 Kick）。
  - 放弃时，递增一个带 `target_lease=live|dead` 标签的指标。Reconcile 是兜底。
- **JWKS** *(R1-O10, R2-O7)*：
  - 启动时从账号服务加载 Ed25519 密钥集。失败则从挂载的 ConfigMap（`jwks-fallback`）加载，并按正常刷新周期继续重试账号服务。
  - Logic **只有在持有某一来源的密钥集后才报告 Ready**。
  - 每 5 分钟刷新。轮换期间接受所有已发布的密钥。刷新失败时保留最后一份有效密钥集并告警。Pod 使用回退密钥集期间持续告警。
  - **未知 `kid`** *(R4H-O10)*：`kid` 不在密钥集中的 JWT **立即被拒**为 `KEY_UNKNOWN`；Login 从不等待拉取（风暴中每秒 2 万次的校验在批次内进行）。若允许，这次拒绝会启动一次后台刷新：
    - **限速**：每个 **Logic Pod 每 30 秒最多一次**因未知 `kid` 触发的刷新，对所有 kid 全局生效（洪峰下每个 Pod 约每分钟 2 次拉取）；
    - **single-flight**：并发的未知 kid 共享同一次在途拉取；
    - **负缓存**：刷新后仍未知的 kid 被记为未知，直到下一次允许的刷新，因此洪峰直接在内存中被拒；
    - **客户端侧**：客户端 1–3 秒后重试（可重试拒绝），刷新落地后即成功。
  - **密钥轮换操作手册：** 新签名密钥必须在签发任何 token **至少一个刷新间隔（5 分钟）加余量之前**发布到 JWKS，这样每个 Pod 都已通过周期刷新持有它，未知 `kid` 只可能意味着操作手册被违反或 token 是伪造的 *(R4H-O10)*；它同样要在签发任何 token 之前发布到 `jwks-fallback` ConfigMap；因泄露而撤回的密钥也要从 ConfigMap 中删除。（过期的 ConfigMap 只在账号服务不可达时才有影响。）
- **Revoke** *(R1-O10, R2-O4, R3-OP2)*：`Revoke(uid, scope = all | slot, reason)`，由账号服务在封号、改密码、"退出所有设备"（scope=all）或"让这台设备下线"（scope=slot）时调用。两种 scope 处理方式相同：
  1. 用 Lua 脚本写入标记字段——scope=all 写 `*`，scope=slot 写 `appid:platform`——取 `revoked_before_ms = max(stored, new)`（幂等），然后执行 `WAIT 1 <timeout>`。如果没有从节点确认，返回可重试错误；账号服务会重试 `Revoke` 直到成功。**标记是同步写入的：必须在第 2 步之前、在 `Revoke` 返回成功之前得到确认**，这样丢失的 Kick 就无法让 Reconcile 复活会话。
  2. 写入确认后，删除受影响的 `sess:{uid}` 字段，并以原因 `REVOKED` 用加固 Kick 踢掉每条受影响的连接。

  客户端自己发出的 `Logout` 命令不写标记：网关会关闭那条连接，它不再是 ONLINE，Reconcile 无法恢复它。
- **生效的吊销标记**（针对某个槽位）*(R3-OP2)*：取 `rev:{uid}` 中的 `max(rev[*], rev[appid:platform])`；缺失的字段按 0 计。登录 Lua 和 Reconcile 规则 1 都使用它。
- **Reconcile 处理** *(R1-O3, R1-O8, R1-O10, R2-O1, R2-O4, R3-O4, R3-O6)*，对每个块中的条目，按以下顺序：
  1. `tok_iat <` 该条目所在槽位的生效吊销标记 *(R3-OP2)* → `not_owner`，原因 `REVOKED`；不写入。
  2. 字段正是这个 `gw|epoch|conn_id` → 刷新 `sess:{uid}` 的 TTL；确保 uid 在 `gwconns` 中。结果 `written`。
  3. 字段是**另一个**会话，且其 `gw|epoch` 的租约**存活** → 用登录表的排序规则（包括其回退窗口）比较两者 *(R2-O4, R3-O4)*：
     - 相同 device_id：若两者的 `login_ms` 相差不超过 `login_deadline`，`conn_seq` 更大的胜出；否则 `login_ms` 更晚的胜出（回退那一行）；
     - 不同 device_id：`login_ms` 更晚的胜出；
     - 两种情况下平局时，**已存的**会话胜出。

     结果：
     - 块中条目更新（只可能发生在写入丢失之后，例如 Redis 主从切换）→ compare-and-set 写入它，刷新 TTL，确保 uid 在 `gwconns` 中，并且**只有当 compare-and-set 确实写入时**，才以 `REPLACED`（相同 device_id）或 `KICKED_BY_OTHER_DEVICE`（不同 device_id）用加固 Kick 踢掉已存会话 *(R3-O6)*。
     - 已存会话更新 → `not_owner`，原因 `REPLACED`（相同 device_id）或 `KICKED_BY_OTHER_DEVICE`（不同 device_id）。
  4. 字段为空，或其 `gw|epoch` 没有存活租约 → compare-and-set 写入该条目（恢复），刷新 TTL，确保 uid 在 `gwconns` 中。

  由于租约 key 与 `sess:{uid}` 位于不同的 slot，Logic 在 Lua 之外检查租约存活，并把观察到的字段值传给一个 **compare-and-set Lua 脚本**，它只在字段仍是 Logic 观察到的值，**并且** Redis `TIME − sent_ms ≤ login_deadline` 时才写入 *(R2-O1)*。
  - **租约检查只是一次观察** *(R4M-2)*：在 compare-and-set 执行时它可能已经过时。如果规则 4 覆盖了一个其网关只是被饿死（租约 key 不存在，但网关存活）的条目，该网关的下一次续期会报告 `missing`（这是有保证的，因为它的租约 key 不存在），其触发式轮次会对它自己的更新会话应用规则 3：已存条目的网关存活，而块中条目更新，因此更新的会话被写回、旧的被踢，约 1 分钟内完成。观察时存活、随即过期的租约是无害的：规则 3 按排序决定，与存活性无关。租约 key 仍是判定存活的权威依据。
  - **截止时间的适用范围** *(R3-O6)*：截止时间只拒绝创建或替换字段的写入（规则 3 和 4）。迟到的 TTL 刷新和 `gwconns` SADD 无害，不拒绝。被拒绝的写入返回 `refused`；网关在本轮内重新入队。
  - 因字段已变化而失败的 compare-and-set 是一个没有写入的明确结果，报告为 `unchanged`（不是 `refused`，因此不重新入队），且不发送 Kick；由下一轮处理该条目。

  **删除阶段** *(R3-O1, R5S-O8)*：收到 `ReconcileEnd` 的 Logic 节点 SSCAN `gwconns:gw:{epoch}`。对每个 uid 读取 `sess:{uid}`，对每个指向本 `gw|epoch`、`conn_id ∉ tracked_conn_ids` 且 `login_ms < as_of_ms` 的字段执行 compare-and-delete。它还会把已没有任何字段的 uid 从 `gwconns` 中移除。为什么安全：conn_id 只有在其 Logout 栅栏满足后才离开跟踪集合，因此针对它的 Login 或 Reconcile 写入都不可能再落地；而 Login 在 `as_of_ms` 当时或之后才被收到的连接，会被 `login_ms` 检查排除。该阶段受可配置的速率限制（默认约每秒 1 万个 uid，与清扫器相同），20 万时每轮约 20 秒；幽灵会话的上界由周期决定，而不是秒级。`ReconcileEnd` 在该阶段完成时回复。若 Logic 节点在阶段中途宕掉，RPC 失败，网关的重试会重发相同 payload；compare-and-delete 是幂等的，因此部分完成的第一次执行是安全的。

  开销：每 10 分钟 20 万条，即每台网关约每秒 330 次 Reconcile 写入 Lua 调用，加上删除阶段约每秒 330 次读取（SSCAN + `sess:` 读取，只对幽灵会话执行 compare-and-delete）：周期式节奏下**每台网关约每秒 660 次 Redis 操作，6 台合计约每秒 4000 次** *(R5S-O8)*。触发式轮次每台网关约每秒 5000 次 *(R2-O8)*；无论哪种节奏，删除阶段都限制在约每秒 1 万个 uid。

## Redis 结构（Cluster；按用户的 key 使用 `{uid}` 哈希标签）

| Key | 类型 | 值 / TTL | 用途 |
|---|---|---|---|
| `sess:{uid}` | HASH | 字段 `appid:platform` → `gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat`；`login_ms` 是网关的 `recv_ms` *(R1-O4)*；key TTL 30 天，在登录和 Reconcile 时刷新 | 槽位归属记录 *(R4M-11)* |
| `rev:{uid}` | HASH | 字段 `*` → scope=all 的 `revoked_before_ms`；字段 `appid:platform` → 该槽位的 `revoked_before_ms` *(R3-OP2)*。每个字段按 `max(stored, new)` 写入并用 `WAIT` 确认 *(R2-O4)*；key TTL = `sess:{uid}` 的 TTL（30 天；不可单独配置），每次写标记时刷新 *(R4M-1)*。token 的有效期只约束登录：Reconcile 规则 1 检查的连接，其 token 可能早已远超任何 token 的有效期，因此标记必须存活得比"一条被吊销的连接在没有任何轮次处理它的情况下能存在的最长时间"更久，而不只是比 token 更久 | 吊销标记（用户级和槽位级） |
| `gwlease:gw:{epoch}` | STRING | `PX <lease TTL>`，每 10 秒用 `SET ... PX <TTL> GET` 续期（旧值为 nil 表示 `missing`）*(R3-O3)*。租约 TTL 可配置（默认 30 秒），必须满足 **TTL > 实测分片故障切换时间 + 一个续期间隔** *(R2-O5)* | 网关实例存活 |
| `gwepochs` | ZSET | 成员 `gw:epoch`，分数 = 最后一次续期的毫秒时间 | 供清扫器使用的候选 epoch 索引；**仅供参考**：它与租约 key 分开写入，因此过时的分数永远不能证明一个 epoch 已死亡 *(R4M-3)* |
| `gwconns:gw:{epoch}` | SET | uid 集合；由 Reconcile 删除阶段修剪。每个 epoch 一个 key，因此一台网关的所有 `SADD` 都落在同一分片；在风暴速率（约每秒 1 万）下可以接受 *(R2-O9)* | 清理线索（与 sess 不是原子的，正确性不依赖它） |
| `sweep:gw:{epoch}` | STRING | `SET NX PX 300000` | 清扫器认领 |

**一个会话有效**，当且仅当其字段存在，**并且** `EXISTS gwlease:{its gw}:{its epoch}`。

> 有效性规则在此定义，但面向使用方的高效读取路径推迟到子项目 2/3。它们**不得**实现为每个 epoch 一次 `HGETALL` + `EXISTS` *(R1-O12)*。

**复制** *(R2-O4)*：Redis Cluster 异步复制；一次故障切换可能丢失约 1 秒已确认的写入。`sess:` 和 `gwconns` 的丢失由 Reconcile 修复（规则 3 选出正确的赢家，规则 4 负责恢复）；租约的丢失由下一次续期弥补。`rev:{uid}` 是例外：它的丢失无法被修复，因此 Revoke 对两种 scope 的每次标记写入都用 `WAIT` 确认。

### 登录 Lua（单 slot，原子）

输入：新条目、`recv_ms`、token 的 `iat`、`login_deadline`（默认 5 秒）。步骤：

1. 读取 `rev:{uid}` 的 `*` 和 `appid:platform` 字段。若 `iat <` 两者的最大值（生效标记）→ `REVOKED`；不写入 *(R1-O10, R3-OP2)*。
2. 读取 Redis `TIME`。若 `now − recv_ms > login_deadline` → `EXPIRED`；不写入 *(R1-O4)*。

   **顺序** *(R4H-OP1)*：先检查吊销，再检查截止时间。两者都不写入，因此顺序永远不影响正确性；它只决定一个既被吊销又已超时的登录报告什么。先报 `REVOKED` 能立即给客户端最终答案（丢弃 token，显示登录页）；先报 `EXPIRED` 会让客户端 1–3 秒后带着已吊销的 token 重试，然后才看到 `REVOKED`。代价是在同一 slot 上多读两个 `rev:{uid}` 字段。Reconcile 规则 1 遵循同样的顺序（截止时间只拒绝写入，而规则 1 不写入）。
3. 将已存字段与新登录比较：

| 已存 vs 新登录 | 结果 |
|---|---|
| 空 | 写入；不踢人 |
| 相同 device_id，新 conn_seq > 已存 | 覆盖；以 `REPLACED` 踢掉旧连接 |
| 相同 device_id，新 conn_seq ≤ 已存，`recv_ms ≤ stored login_ms + login_deadline` | 以 `STALE` 拒绝（返回已存 conn_seq）；关闭；不写入 *(R1-O5)* |
| 相同 device_id，新 conn_seq ≤ 已存，`recv_ms > stored login_ms + login_deadline` | 计数器回退：覆盖；以 `REPLACED` 踢掉旧连接；递增 `conn_seq_regressed` *(R1-O5)* |
| 不同 device_id，`recv_ms ≥ stored login_ms` | 覆盖；以 `KICKED_BY_OTHER_DEVICE` 踢掉旧连接 |
| 不同 device_id，`recv_ms < stored login_ms` | 以 `SUPERSEDED` 拒绝；不写入，不踢人；网关以可重试错误关闭 *(R1-O4)* |

已存条目的租约是否存活不影响写入决策（Lua 无法读取位于另一 slot 的租约 key）。第一次 Kick 尝试总是发送（R1-O8, R2-O6）。

Reconcile 规则 3 使用相同的排序 *(R3-O4)*：比已存登录早超过 `login_deadline` 的更大 `conn_seq` 不可能由本脚本产生（`EXPIRED` 检查禁止了这种情况），因此带窗口的规则在它能到达的每个状态上都与本表一致。

**时钟契约** *(R1-O4, R4M-5)*：`|网关时钟 − Redis TIME| ≤ 1s`，由租约路径上的 `clock_skew_ms` 测量。正常的 NTP 偏差（几毫秒）远小于一个人切换设备所需的时间。保持 1 秒预算：更大的预算会推迟每一个带栅栏的 Logout，而且仍然覆盖不了更大的时钟跳变。违反契约只会降级，绝不会静默损坏状态：

| 偏差 | 影响 | 严重程度 |
|---|---|---|
| 网关比 Redis **快** 超过 1 秒 | 栅栏 (b)/(c) 在迟到的 Login 或 Reconcile 写入还可能落地之前就放行了 Logout → 留下一个已关闭连接的幽灵条目 | 有界：该 conn_id 已不再被跟踪，因此下一轮的删除阶段会移除它（≤ 1 个周期）。Logout 精确匹配，登录表仍对写入排序，因此不会覆盖更新的会话。 |
| 网关比 Redis **慢** 约 `login_deadline` − RPC 延迟以上 | 经该网关的每次 Login 都是 `EXPIRED` | 该网关无法完成登录。`EXPIRED` 是可重试拒绝，每次尝试使用新的源端口，因此很多客户端会落到别的网关，但这仍是整台网关的故障。 |
| 两台网关之间的偏差超过两台设备登录的间隔 | 同一槽位的竞争中错误的设备胜出（竞态 3、规则 3） | 出现一次多余的 `KICKED_BY_OTHER_DEVICE`；状态不会损坏 |

### Logout Lua

只有当字段仍匹配 `gw|epoch|conn_id` 时才 `HDEL`。由于 conn_id 永不复用，这解决了延迟断开竞态，包括同一网关上的情况。与该连接自身 Login 以及对它的任何 Reconcile 恢复之间的先后顺序，由网关的 Logout 栅栏保证。

### 清扫器（每个 Logic 节点中的 goroutine）

查找最后一次续期早于 **2 × 租约 TTL**（默认 60 秒）*(R2-O5)*、或已发送 `EpochEnded` 的 `gwepochs` 成员。用 `sweep:` SET NX 认领每一个，并**记录认领时该 epoch 的 `gwepochs` 分数** *(R3-O3)*。**然后检查 `EXISTS gwlease:gw:{epoch}`**：如果租约 key 存在，不论其分数如何，该 epoch 都是存活的（例如它的 `ZADD` 失败而 `SET` 成功）；释放认领（`DEL sweep:`）并跳过它 *(R4M-3)*。与有效性规则一样，租约 key 是判定存活的权威依据。执行 `SSCAN gwconns`，对每个 uid 运行一个 Lua 脚本，只删除 `gw|epoch` 与该死亡 epoch 匹配的字段。限速约每秒 1 万用户。

- **续期即中止** *(R3-O3, R4M-3)*：在每个限速批次之间重新读取该 epoch 的 `gwepochs` 分数并检查其租约 key。如果分数比认领时新，**或者租约 key 存在**，说明该 epoch 已续期：立即停止，不 `UNLINK` `gwconns`，不移除 ZSET 成员，`DEL` 掉 `sweep:gw:{epoch}` 认领，以免阻碍之后合法的清扫 *(R3-OP6)*，并计数 `sweep_aborted`。网关的那次续期已经报告了 `missing`（租约 key 曾不存在），因此其触发式 Reconcile 会恢复所有被删除的内容。由于每次清扫开始时租约 key 都不存在，结束一次错误清扫的那次续期总会报告 `missing` *(R4M-3)*。
- 完成后，`UNLINK` 该集合（在后台线程中释放，因此删除 20 万成员不会卡住分片），并移除 ZSET 成员 *(R2-O9)*。

**大规模过期保护闸** *(R1-O2, R2-O3, R2-O5, R3-O2)*：只有满足以下条件时才清扫一个过期 epoch：
- 至少有一个**其他** epoch 在 **1.5 × 续期间隔（15 秒）** 内续期过，**并且这一状态已连续保持一个租约 TTL（30 秒）**。每个清扫器在内存中维护一个 `renewing_since` 时间戳：条件为假时重置，首次变为真时设置。清扫器重启会重新开始计时，这是保守的。1.5× 的窗口使唯一幸存网关 10 秒的续期节奏（加上 RPC 延迟）不会重置计时；并且
- 清扫器自身访问 Redis 已连续健康一个完整的**租约 TTL**（默认 30 秒）。

已发送 `EpochEnded` 的 epoch 不受此限制，总是被清扫。保护闸阻止清扫时，会发出告警和指标，并在下一周期重新检查。该规则直接询问系统是否在明确地续期租约，因此能区分系统性故障（没有任何人续期）和真实的多网关死亡（其他网关仍在续期）。它**在续期恢复一个租约 TTL 后放行，届时每台健康网关都已续期过**（每台都有三次续期机会）*(R3-O2)*。跳过或推迟清扫永远不影响正确性——按有效性规则，死亡 epoch 的条目已经无效；清扫器只负责清理。

安全网：`sess:{uid}` 的 30 天 TTL，由登录和 Reconcile 刷新 *(R1-O3)*。

### 为什么不需要被舍弃的概念

- **按用户的登录限流：** 会阻挡真实的快速重连（Wi-Fi 切 4G），而且一次被拒的登录对 Redis 的开销与被接受的相同。防滥用转移到网关，不消耗 Redis。
- **按 IP 限流** *(R1-O11)*：移动端 IP 大多通过运营商级 NAT 共享，按 IP 限流会伤害正常用户，在重连风暴中尤甚。网关的握手限流是全局的。未来任何防滥用限流都以 device_id 或 user_id 为键（只有在 LoginReq 解密之后才可能）；子项目 1 不做设计。没有按 IP 限流，子项目 1 就不需要真实客户端 IP，因此 proxy protocol 推迟到 Web 阶段。
- **分布式锁：** 每次变更都是单个 slot 上的单个原子 Lua 脚本。
- **反向映射：** 网关知道每条连接属于哪个用户；清理使用 `gwconns`，Reconcile 让 Redis 与网关保持一致。
- **会话版本号：** `gw|epoch|conn_id` 三元组加上 `device_id`/`conn_seq`/`login_ms` 已经覆盖。

## 已处理的竞态

1. **延迟断开**（旧 G17 的登出晚于新 G42 的登录）：Logout 的 compare-and-delete 不匹配，什么也不发生。
2. **被弃用连接的延迟登录，同一设备**（S1 的登录在 S2 之后落地）：截止窗口内更旧的 conn_seq → `STALE`，S2 不受影响。晚于截止时间的登录本来就会 `EXPIRED`。
3. **同一槽位上另一台设备的延迟登录** *(R1-O4)*：其 `recv_ms` 早于已存的 `login_ms` → `SUPERSEDED`；更新的设备不会被踢。
4. **同名 Pod 的僵尸网关：** 租约按 epoch 区分，因此实例之间不冲突；送到错误实例的 Kick 因 epoch 不匹配而被忽略。
5. **Login 进行中连接关闭** *(R1-O9)*：Logout 栅栏把 Logout 推迟到收到明确的 Login 回复（栅栏 a），因此 Logout 总在写入之后落地。
6. **Login 没有明确回复，但 Redis 已被写入**（超时、RPC 错误、`MaxConnectionAge` 重置、Logic 崩溃）*(R1-O9)*：Lua 截止时间意味着 `recv_ms + login_deadline` 之后不可能再有写入；网关在发送 Logout 前等待 `recv_ms + login_deadline + 1s`（栅栏 b），因此 Logout 总在任何可能的写入之后落地。
7. **网关存活时 Logout 丢失：** 过期条目由下一轮结束时的删除阶段（`ReconcileEnd`）移除 *(R1-O3, R3-O1)*。清扫器只处理死亡的 epoch。
8. **Kick 丢失或提前到达** *(R1-O8)*：`AUTH_PENDING` 期间收到的 Kick 记为 kick-pending，在 Login 回复到达时执行；重试后仍丢失的 Kick 由下一轮 Reconcile 通过 `not_owner` 执行（≤ 约 10 分钟）。
9. **两台设备来回互踢：** 收到 `KICKED_BY_OTHER_DEVICE` 后客户端不得自动重连。
10. **两个客户端实例共用一个 device_id** *(R1-O6)*：它们互相替换一次，由于 `REPLACED` 不重连，循环停止。
11. **设备计数器回退**（重装、恢复备份）*(R1-O5)*：截止窗口外较小的 conn_seq 作为回退被接受；窗口内则返回 `STALE` 并附带已存计数器，让客户端重新同步。
12. **网关健康时会话被清扫** *(R1-O2, R2-O3, R3-O2, R3-O3)*：
    - 系统性故障（长时间 Redis 故障、Logic 饱和）使所有续期停止，因此保护闸保持关闭，什么也不清扫。
    - 网关与 Logic 之间的故障（Logic↔Redis 正常）同样使所有续期停止。恢复后，保护闸等待一个 TTL 的连续续期，因此健康网关在任何清扫之前都已续期。
    - 如果单台网关的租约被错误地饿死而其他网关仍在续期，它可能被清扫；它的租约回复随后报告 `missing`，触发式 Reconcile 在约 1 分钟内恢复其会话。
    - 如果被饿死的网关在其 epoch 被清扫期间续期，回复仍然报告 `missing`（租约 key 曾不存在），清扫器在下一个批次停止，触发式 Reconcile 恢复所有被删除的内容。在恢复之后才由最后一个在途批次删除的条目，由下一轮周期对账修复。
    - 如果一台存活网关续期时 `SET` 成功而 `ZADD gwepochs` 失败，它的分数会变旧；清扫器认领该 epoch 后发现租约 key 存在，于是跳过它 *(R4M-3)*。
13. **两个 loop 同时生成 conn_id：** 每个 loop 有自己的计数器，worker_id 前缀使 ID 互不冲突。
14. **Reconcile 轮次期间连接关闭** *(R2-O1)*：其 Logout 被推迟到包含它的块被确认（栅栏 c），或该块的截止时间已过，因此 Logout 总在任何恢复之后落地。在其块构建之前就关闭的连接根本不在本轮中。
15. **Redis 故障切换丢失写入，旧会话被恢复** *(R2-O4, R3-O4)*：如果字段回滚到旧会话而更新的连接仍存活，Reconcile 规则 3 按登录表的排序比较，写回更新的条目并踢掉旧连接。该排序包括回退窗口，因此通过计数器回退胜出的会话在回滚后仍然胜出。平局时已存会话胜出，因此两个存活会话永远不会互相覆盖并互相关闭。
16. **Reconcile 轮次前后 Login 在途或 Logout 丢失** *(R3-O1)*：Login 在途的连接是被跟踪的，因此在 `tracked_conn_ids` 中，删除阶段不会动它的字段；在 `as_of_ms` 之后收到的 Login 无法通过 `login_ms < as_of_ms` 检查。Logout 被丢弃的连接已离开跟踪集合，因此其幽灵条目在同一轮结束时被删除——不论 Logout 是在其块发送之前还是之后被丢弃的。
17. **槽位级登出的 Kick 丢失** *(R3-OP2)*：连接在其网关上仍是 ONLINE，而它的 `sess:` 字段已被删除。已确认的槽位级标记使下一轮 Reconcile 将该条目返回为 `not_owner`（`REVOKED`，规则 1），而不是恢复它（规则 4），因此该设备在一个周期内被关闭。该用户的其他槽位不受影响。
18. **Reconcile 块在途期间客户端登出** *(R4H-O8)*：Logout RPC 等待栅栏 (c)，因此 Logic 先处理该块（规则 2 刷新，或规则 4 恢复），然后 Logout 才删除字段；不会留下幽灵条目。

注：同一设备重连时，`conn_seq` 也会替换任何幽灵条目。Reconcile 负责处理再也不回来的设备。

## 一致性契约 *(R2-O8)*

使用方（子项目 2/3）在设计缓存和兜底时可以依赖的上界：

| 使用方可能看到的偏差 | 原因 | 上界 | 由谁修复 |
|---|---|---|---|
| 幽灵：条目有效，但连接已不存在 | Logout 被丢弃 | ≤ 1 个 Reconcile 周期（约 10 分钟 + 抖动）：在一轮结束前任何时刻丢弃的 Logout，都在该轮结束时修复 *(R3-O1)* | Reconcile 删除阶段（`ReconcileEnd`） |
| 被替换或吊销（任一 scope）后旧持有者仍连着 | Kick 丢失 | ≤ 1 个周期（对吊销而言：前提是在 30 天的标记 TTL 内有某一轮处理到该连接 *(R4M-1)*） | Reconcile `not_owner` / 规则 1 *(R3-OP2)* |
| 死亡网关的条目 | 网关死亡 | 租约 TTL（30 秒）后失效；2 × TTL，加上最多一个 TTL 的保护闸等待 *(R3-O2)*，再加清扫时间后删除 | 有效性规则，然后清扫器 |
| 存活连接在 Redis 中缺失 | 故障切换丢失写入、被误清扫，或基于过时的租约观察被覆盖 *(R4M-2)* | ≤ 1 个周期；收到 `missing` 租约回复后 ≤ 约 1 分钟（对过时观察而言，是归属网关的回复） | Reconcile 恢复（规则 4），或归属网关触发式轮次中的规则 3 |
| 健康会话被判为无效 | Logic 或租约分片故障超过 TTL | 与故障持续时间相同 | 租约续期恢复 *(R2-O5)* |

## 风暴、下线与故障

- 客户端 SDK：见 *客户端 SDK 规则*（失去已建立连接后 0–20 秒抖动，本地网络变化或 App 回到前台后 0–1 秒，收到 `Reconnect` 后 `[0, jitter_ms]`，握手前关闭 0.5–1.5 秒、最多连续 10 次，其他可重试拒绝（`Busy`、`EXPIRED`、`KEY_UNKNOWN`、`LoginResp` 之前被关闭）1–3 秒、最多连续 5 次 *(R5S-O4)*，指数退避，窗口可通过 `LoginResp` 调整 *(R5S-O6)*，10 秒 Pong 超时，遵守 `retry_after`）。
- 容量：N−1 台网关必须吸收一台死亡网关的连接，按 20 万运行点计算 *(R1-O1)*；单网关连接上限限制了它们吸收得有多不均匀 *(R2-O2)*。
- 同时失去多台网关（可用区）：降级，尽力而为，无 SLO *(R2-O3)*。
- 优雅下线：见 *网关*。

| 故障 | 行为 |
|---|---|
| Logic 宕机 | Login 得到可重试错误，客户端退避。Logout 进入有界重试队列 60 秒，之后丢弃；被丢弃的 Logout 在下一轮 Reconcile 结束时修复。宕机超过租约 TTL：所有会话都被判为无效，直到续期恢复（有意承担，见 *接口约束*）*(R2-O5)*。 |
| Logic 饱和 *(R1-O2)* | 租约 RPC 保有独立连接和工作池，续期继续。网关的登录队列上限回复 `Busy`（1–3 秒）。因截止时间被拒的 Reconcile 条目在本轮内重新入队（最多 3 次）*(R3-O6)*。 |
| 网关与 Logic 断开，Logic↔Redis 正常 *(R3-O2)* | 没有 epoch 续期，因此保护闸保持关闭。连通恢复后，保护闸只在一个 TTL 的连续续期后才放行，因此每台健康网关在任何清扫之前都已续期。 |
| 账号服务宕机时 Logic 冷启动 *(R2-O7)* | Pod 加载 `jwks-fallback`，变为 Ready 并接受 token；使用回退密钥期间持续告警。 |
| Redis 宕机 | 新登录失败；已有连接保持。Redis 恢复后租约 SET 恢复，条目重新有效。故障期间没有 epoch 续期，因此大规模过期保护闸阻止清扫，并在续期恢复一个 TTL 后放行；若仍有会话被清扫，在租约回复报告 `missing` 时由 Reconcile 恢复。 |
| Redis 分片故障切换 *(R2-O4, R2-O5, R5S-O1)* | 可能丢失最多约 1 秒已确认的写入：`sess:`/`gwconns` 的丢失由 Reconcile 修复，`rev:` 由 `WAIT` 保护。租约能存活，因为其 TTL 大于实测故障切换时间加一个续期间隔。故障分片上的登录在故障切换期间停滞或返回 `EXPIRED`；其他分片上的登录不受影响（按条目返回结果）。 |
| 可用区丢失（Redis）*(R5S-O9)* | 丢失可用区中的主节点故障切换到其位于存活可用区的副本（部署规则见 *部署要求*）。这些分片上的登录在故障切换期间停滞或返回 `EXPIRED`；其他分片不受影响。丢失的写入按分片故障切换的方式修复。对副本位于丢失可用区的分片，Revoke 可能返回可重试错误，直到重建出副本；账号服务持续重试。 |
| 网关进程崩溃 / Pod 被杀 | 客户端看到 RST/FIN，在 0–20 秒抖动内重连。租约在 TTL（30 秒）内过期；清扫器在 2 × TTL（60 秒）后清理，如果其保护闸计时刚开始，再加最多一个 TTL。每次重新登录有一次 Kick 尝试打到死亡的 epoch，不重试 *(R2-O6)*。 |
| 节点丢失 / 静默分区 | 客户端通过 Pong 超时发现（≤ 一个心跳间隔 + 10 秒），然后重连。服务端与崩溃情况相同。发往丢失 Pod 的 Kick 每次约 1 秒后超时，从不拖慢重新登录；溢出的被丢弃 *(R5S-O2)*。超过节点丢失阈值（2 分钟）后，节点被施加 `out-of-service` 污点，`gw-k` 在别处重建，约 2–3 分钟内恢复到 N 台 *(R5S-O3)*。 |
| 同时失去多台网关（可用区）*(R2-O3)* | 尽力而为：幸存网关填满到连接上限；多出的客户端在 accept 时被关闭，0.5–1.5 秒后重试（最多 10 次，之后退避），直到丢失的 Pod 被重新调度 *(R3-O5, R5S-O4)*。由于幸存网关持续续期，过期 epoch 会被清扫。 |
| Kick 无法到达网关 *(R2-O6)* | 目标租约存活时重试（约 30 秒内最多 3 次）；否则只尝试一次。之后由 Reconcile 兜底在一个周期内关闭连接。 |
| 握手洪峰 | 全局握手并发上限和令牌桶保护已建立的连接；多余的握手被关闭并计数。 |
| 慢速客户端 | `HANDSHAKE` / `AUTH_PENDING` 的 5 秒超时释放其占位。 |
| 坏帧 / 解密失败 / 帧过大 | 立即关闭。 |
| 全体用户重连 *(R5S-O6)* | 尽力而为，无 SLO（见 *成功标准*）。握手令牌桶、连接上限和登录队列上限以低成本关闭或拒绝多余部分；客户端按两个重试计数器重试，之后退避。预期恢复时间 ≈ 用户数 ÷ 准入的登录速率，再加退避长尾。 |
| 流哈希 LB 后的滚动重启 *(R5S-O4)* | 尚未重启的 Pod 填满到上限；被拒客户端每 0.5–1.5 秒做一次握手前重试，最多 10 次，落到有空位的 Pod 上。在最少连接 LB 上则是重启过的 Pod 被填充。 |
| 节点升级 / 自愿驱逐 *(R5S-O5)* | 网关 PDB 一次只让一台网关下线；后续驱逐要等它重新 Ready，且已有网关宕机时不会进行任何驱逐。Logic 永远不会低于 `minReplicas`。 |

## 对后续子项目的接口约束

- **所有权规则** *(R1-O8)*：一条连接只有在其 `gw|epoch|conn_id` 拥有 `sess:{uid}` 中自己的槽位时，才能以该用户身份行事（发消息、执行用户操作）。子项目 2 必须在发送路径上强制执行（缓存的所有权检查可以接受）。子项目 1 只负责记录这条规则，并提供它所依赖的数据（会话条目以及 Kick/Reconcile 投递）。
- **会话有效性读取路径** *(R1-O12)*：推迟到子项目 2/3；不得实现为每个 epoch 一次 `HGETALL` + `EXISTS`。
- **有效性依赖 Logic 和租约分片** *(R2-O5)*：如果 Logic，或存放某网关租约 key 的 Redis 分片，不可用时间超过租约 TTL，该网关的会话会被判为无效，尽管其连接是健康的。子项目 2/3 不得仅凭有效性触发大规模离线推送；缓解方案由它们设计。
- **一致性上界**：见 *一致性契约* *(R2-O8)*。
- 子项目 3 中的**惰性修复**（推送收到 "unknown conn" 时由 Logic 执行 compare-and-delete）是可选的；正确性已不再依赖它 *(R1-O3)*。

## 可观测性

Prometheus 指标：
- 每台网关按状态的连接数；等待 Logout 的已跟踪但已关闭条目；每秒握手数；**连接倾斜度**（每网关 ONLINE 数的最大值 / 平均值）*(R2-O2)*；
- 按结果分类的登录数：`ok`、`stale`、`replaced`、`kicked`、`superseded`、`expired`、`revoked`、`key_unknown` *(R4H-O10)*、`busy`、`error`；以及 `conn_seq_regressed`；
- 端到端登录延迟 p50/p99；gRPC 和 Redis 脚本延迟；
- **重新登录延迟**直方图：`offline_ms` +（登录成功时刻 − `recv_ms`），按 `reconnect_reason` 打标签 *(R1-O7)*；
- **死亡 epoch 交叉核对** *(R1-O7)*：当重新登录发现旧条目指向死亡 epoch 时，记录 `login_ms − 该 epoch 最后一次租约续期时间`（从网关死亡到重新登录的时间，±10 秒，无需客户端数据）；
- 租约续期失败；回复为 `missing` 的租约续期；每台网关的 `clock_skew_ms`，|偏差| > 500ms 时告警 *(R4M-5)*；
- 按原因分类的准入拒绝：`at_capacity` *(R2-O2)*、`handshake_rate`、`login_queue`、`pending_mem`、`handshake_timeout`、`auth_timeout`、`bad_version` *(R4H-O1)*、`noise_unknown_key` *(R4G-O6)*；
- Kick 尝试、重试和放弃，放弃按 `target_lease=live|dead` 打标签（告警只用 `live`）*(R2-O6)*；吊销导致的 Kick；Revoke `WAIT` 失败 *(R2-O4)*；
- Reconcile：刷新、恢复、`unchanged` *(R3-OP7)*、`refused` 及重新入队、达到重新入队上限 *(R3-O6)*、按原因分类的 `not_owner`、规则 3 覆盖（块中条目更新）*(R2-O4)* 的条目数；`ReconcileEnd` 删除数、`tracked_conn_ids` 大小、重试与丢弃 *(R3-O1, R3-OP4)*；被取消的轮次；按节奏分类的周期耗时 *(R2-O8)*；
- 清扫器进度；`sweep_aborted` *(R3-O3)*；**大规模过期保护闸拦截**（同时告警），包括 `renewing_since` 等待所占用的时间 *(R3-O2)*；
- JWKS 刷新失败和因未知 `kid` 触发的刷新；使用 `jwks-fallback` 的 Pod（同时告警）*(R2-O7)*；
- **Kick 队列** *(R5S-O2)*：每个目标 `gw|epoch` 的队列深度、`kick_dropped{target_lease=live|dead}`、尝试延迟；
- **Reconcile 删除阶段** *(R5S-O8)*：每轮耗时和扫描的 uid 数，按节奏分类；
- **节点丢失恢复** *(R5S-O3)*：从节点 NotReady 到替代网关 Ready 的时间，超过目标时告警；
- **限制** *(R5S-O7)*：实测的 `RLIMIT_NOFILE` 和 `nf_conntrack_max`（gauge），可读取时节点的 `nf_conntrack_count`（达到最大值 80% 时告警），以及自检失败时的 Readiness 失败原因；
- **空闲后重连率**（反映短于 150 秒的 NAT 超时）；Go GC 停顿和堆大小。

日志：带 `uid` 和 `conn_id` 的 `slog` 行；通过 gRPC metadata 传递请求 ID。

## 验证

### 测试执行模型 *(R4H-O11)*

| 层级 | 主机 | 运行时机 | 内容 |
|---|---|---|---|
| **1a** | 开发机（2 核 / 2GB / 32GB） | 每次改动 | 单元测试（带 `-race`）、Lua 测试、小规模压测探针（≤ 5000 连接） |
| **1b** | 开发机 | 每次提交 / 合并之前 | 在测试时间配置下运行的竞态集成测试套件 |
| **2** | 专用主机（12 核 / 26GB / 2TB） | 合并对网关、Logic 或 Lua 的改动之前；按需 | Redis 故障切换；压测 (a)、(b1)、(b2)、(b-stress)、(c)、风暴、洪峰、Logic 变慢；所有单机 SLO 测量 |
| **3** | 云集群（`target=cloud`），或 Tier 2 主机（`target=local`） | 发布之前 | 100 万滚动重启；NLB / 下线行为；真实网络上的多网关风暴 |

- **测试时间配置**（Tier 1b）：所有间隔和 TTL 都可配置。该配置把与时间相关的参数缩小约 10 倍，并保持其比例：租约 TTL 3 秒、每 1 秒续期、保护闸窗口、周期式 Reconcile，90 秒的故障变为 9 秒。`login_deadline` 和 1 秒的偏差余量**不**缩放；它们与真实的 RPC 延迟挂钩。token 有效期由 devtoken 按场景设置，`sess:`/`rev:` 的 TTL 保持 30 天。如果某个配置破坏了必需的比例（例如租约 TTL ≤ 故障切换时间 + 一个续期间隔），网关和 Logic 拒绝启动。
- **CPU 隔离**（Tier 2 和本地 Tier 3）：网关、loadgen、Logic + Redis 用 cgroups / `taskset` 绑定到互不重叠的核上（例如 6 / 4 / 2），因为 loadgen 的 Noise 握手开销与网关的大致相当。容量按**网关核数**报告，使 60 万上限可以换算到生产节点规格。主机准备为 60 万时的 loadgen 提供 ≥ 10 个源 IP（每个"源 IP → 目标"对约 6.4 万个端口），并设置 `nr_open`/ulimit 和 conntrack（调高，或在测试网卡上禁用）。
- **自动化约定**（Tier 2 和 3）：完全脚本化、无交互。
  - 每个场景一个入口，例如 `make test-tier2 SCENARIO=b1`、`make test-tier3 SCENARIO=rolling TARGET=local`；没有任何提示输入。
  - 幂等的主机准备（sysctl、ulimit、IP 别名、CPU 绑定）；清理步骤总会执行。
  - 机器可读的结果（JSON），按 SLO 阈值给出通过/失败，并返回对应的退出码；产物：pprof、指标快照、loadgen 延迟分布。
  - **主机保护**：Tier 2 和 3 的脚本检查 `nproc`、内存和显式的 `GOIM_TEST_TIER`，在规格不足的主机上拒绝运行。它们不得在开发机上运行。
- **Tier 3 目标**（可配置，因为目前还没有云集群）：`TARGET=cloud | local`。
  - **`cloud`**：在云上 Kubernetes 集群中、使用真实 NLB、以 100 万连接运行完整场景。
  - **`local`**：在 Tier 2 主机上运行同一场景，使用本地 Kubernetes（kind 或 k3s），从而覆盖 StatefulSet、就绪状态、`publishNotReadyAddresses` 和 `terminationGracePeriodSeconds`；用一个跟随 Kubernetes endpoints 的本地四层负载均衡器（例如 IPVS 模式的 kube-proxy）代替 NLB，使 NotReady 的 Pod 会被移出轮转 *(R4M-7)*。负载按 `TIER3_SCALE`（默认 0.15）缩放：总连接数 = 100 万 × scale，单网关上限 = 25 万 × scale（默认下 6 台网关、共 15 万连接、上限 3.75 万），从而保持 N−1 和连接上限的算术关系不变。所有不依赖真实 NLB 或云网络的断言都原样运行：没有网关超过上限；被拒客户端在 SLO 内登录到别处；正在下线的 Pod 上每个客户端都在连接关闭前收到 `Reconnect`；Kick 能到达正在下线的 Pod。
  - **本地无法覆盖**的部分，在 JSON 结果中如实列出（`"target": "local", "scale": …, "not_covered": [...]`）：真实 NLB 的 RST/FIN 透传、流哈希分布和摘除延迟；云网络延迟；100 万的绝对规模。在有云集群之前，`local` 运行就是被接受的 Tier 3 证据，并在发布说明中注明这些缺口。

- **单元测试**（Tier 1a）：
  - 帧编解码（不完整帧、一次到达多帧、模糊测试），针对测试客户端的 Noise 握手；被篡改的帧头导致 AEAD 失败，非零 flag 导致关闭，未知版本在握手前被关闭 *(R4H-O1)*；用两个已配置 key ID 中的任意一个都能完成握手，未知 key ID 在握手前被关闭并计数，被篡改的 `key_id` 因 prologue 而握手失败 *(R4G-O6)*；
  - 连接状态机：`AUTH_PENDING` 中的 kick-pending、两种超时（超出范围的值导致拒绝启动；排队时间超过 `AUTH_PENDING` 超时的 Login 不会因此被关闭 *(R4H-O7)*）、三个条件齐全的 Logout 栅栏、accept 时的连接上限（计 socket，不计等待 Logout 的条目），以及在 Logout 被确认或丢弃之前一直属于跟踪集合；
  - Reconcile 轮次控制：`refused` 条目最多重新入队 3 次，不再是 ONLINE 时跳过；被取消的轮次不再发送块，也不发送 `ReconcileEnd` *(R3-O6, R3-O1)*；每个块只包含同一个 loop 的 ID，各分区轮流取用；在 `-race` 下，一轮对账在各 loop 不断 accept 和关闭连接时运行，没有竞态报告，且每个跟踪片段都在 `as_of_ms` 之后取得 *(R4H-O9)*；
  - 按 loop 连接表的预分配：向按规则设定容量的表插入 `cap / numLoops` 个条目不会导致 map 扩容（用 `testing.AllocsPerRun` 检查）*(R4H-O4)*；
  - loopState *(R4H-O5)*：每个 loop 上的第一次 `OnOpen` 不分配连接表（`AllocsPerRun`）；超过 `numLoops` 的 loop 导致 panic；在租约和 Logic 连接就绪之前网关不会 Ready；
  - 计时器 *(R4H-O6)*：计时器绝不在截止时间之前触发（tick 边界向上取整）；心跳条目在有活动时重挂，并在最后一帧之后 310 秒触发；
  - 连接状态机在一个**伪造传输**上运行，证明核心不依赖 TCP 或 Noise *(R4G-O5)*；
  - conn_id 生成，以及 **`-race` 下的 gnet loop 归属测试**（对同一连接，`OnOpen`、`OnClose`、`Execute` 回调和时间轮到期回调都看到同一个 loop）；
  - 时钟偏差 *(R4M-5)*：由 `gw_now_ms` 和 Redis `TIME` 计算 `clock_skew_ms`，以及 500ms 告警阈值；
  - 清扫器保护闸：15 秒内没有其他 epoch 续期时 `renewing_since` 重置，带抖动的 10 秒节奏不会重置它 *(R3-O2)*；
  - JWKS 未知 `kid` *(R4H-O10)*：未知 kid 洪峰下每个 Pod 每 30 秒最多拉取一次，并发请求共享同一次拉取；
  - **SDK** *(R3-O5, R4H-O2, R4H-O10, R5S-O4)*：握手前关闭在 0.5–1.5 秒后重试，连续 10 次后回退到指数退避；`Busy`、`EXPIRED` 和 `KEY_UNKNOWN` 在 1–3 秒后重试，共用一个"连续 5 次"计数，之后回退；一种结果会清零另一个计数器，登录成功后两者都清零。登出、切换账号和关闭 App 都会取消尚未执行的重试。在 0–20 秒等待期间发生网络变化，1 秒内重连；在 `retry_after` 等待期间发生网络变化，不会缩短等待；收到 `Reconnect` 后，第一次尝试落在 `[0, jitter_ms]` 内，并携带下线原因 *(R4H-O3)*。握手之后没有收到 `LoginResp` 或 `Error` 就被关闭的连接，在 1–3 秒后重试，并计入 5 次 *(R4H-O7)*。每种抖动等待（0–20 秒、1–3 秒、0.5–1.5 秒、0–1 秒、`[0, jitter_ms]`）采样约 1 万次，都能通过均匀性分桶检查 *(R4G-O12)*。来自 `LoginResp` 的窗口会被使用，并在 App 重启后保留；超出范围的值被钳制；缺省字段保持当前值 *(R5S-O6)*；
  - **Logic 按条目返回 Login 结果** *(R5S-O1)*：一个条目的脚本被延迟时，其他条目的结果先于它到达；
  - **Kick** *(R5S-O2)*：对一个永不应答的目标（黑洞地址），Login 结果不等待 Kick 就返回，每次尝试在截止时间内结束，每目标队列满时丢弃并计数，而不是增长；
  - **删除阶段** *(R5S-O8)*：对 N 个 uid 的删除阶段耗时 ≥ N ÷ 速率，且删除的字段与不限速执行相同；中途被打断并重试的 `ReconcileEnd` 与一次不间断执行结果相同；
  - **限制自检** *(R5S-O7)*：`nofile` 或 `nf_conntrack_max` 低于上限所需时 Readiness 失败；配置 `conntrack: notrack` 时通过；conntrack 文件不存在时通过。
- **Lua 测试**（在 Docker 中运行真实 Redis；Tier 1a）：
  - 登录表每一行一个表驱动测试，包括 `SUPERSEDED`、`EXPIRED`、`STALE`（窗口内较小 conn_seq 被拒）和回退那一行（窗口外较小 conn_seq 被接受）；
  - `REVOKED`（token iat 早于标记时被拒）；吊销标记写入为 `max(stored, new)` 且幂等；生效标记是 `*` 和槽位字段的最大值，槽位标记只阻止该槽位的登录 *(R3-OP2)*；既被吊销又超过截止时间的登录返回 `REVOKED` *(R4H-OP1)*；每次写标记都把 `rev:` key 的 TTL 设为配置的 `sess:` TTL，重写时刷新 *(R4M-1)*；
  - logout 的 compare-and-delete；
  - Reconcile 的 compare-and-set（包括 `sent_ms + login_deadline` 之后的拒绝，以及比较失败时不写入）和 `ReconcileEnd` 删除（只删本 epoch 的字段、conn_id 未被跟踪、`login_ms < as_of_ms`）*(R3-O1)*；
  - 模拟写入丢失后的 Reconcile 规则 3：在更新的连接存活时手工恢复一个旧字段；Reconcile 保留更新的连接并踢掉旧连接 *(R2-O4)*；
  - 计数器回退情况下的 Reconcile 规则 3 *(R3-O4)*：一个计数器回退的会话（较小 conn_seq，登录时间比已存的晚超过 `login_deadline`），手工把它的写入回滚到旧的高 conn_seq 条目；Reconcile 保留回退后的会话并踢掉旧的。两台设备 `login_ms` 相同：已存会话胜出，不发生互踢；
  - 用 `SET ... GET` 续期租约：key 不存在 → `missing`，key 存在 → `live` *(R3-O3)*；
  - 清扫器只删除死亡 epoch 的字段；分数已过时但租约 key 存在的 epoch 会被清扫器跳过 *(R4M-3)*。
- **竞态集成测试**（`deploy/compose`：Redis、Logic、**4 台网关**和一个脚本化客户端；Tier 1b，使用测试时间配置）*(R3-O7)*：
  - 上文第 1–6、10、14、16 项竞态；第 6 项按两种顺序测试（Logout 先于 Login，Login 先于 Logout，通过注入 Logic 延迟）：栅栏之后没有幽灵；因 Logic 崩溃而没有明确回复的 Login：Logout 等待栅栏；
  - 第 14 项 *(R2-O1)*：连接在其块发送之后、Logic 处理之前关闭（注入 Logic 延迟）；Logout 之后没有残留幽灵；
  - 第 18 项 *(R4H-O8)*：客户端在其块于 Logic 中被延迟时发送 `Logout`；它立即收到 `LogoutResp`，且不留幽灵；
  - `ReconcileEnd` *(R3-O1)*：轮次开始时在途的 Login（注入 Logic 延迟）不会被该轮的删除阶段删除；在连接的块发送之后被丢弃的 Logout 在同一轮结束时修复；被取消的轮次不删除任何东西；失败一次的 `ReconcileEnd` 会被重试，其删除生效 *(R3-OP4)*；
  - Reconcile 拒绝 *(R3-O6)*：在一轮触发式对账的某个块上注入长于 `login_deadline` 的 Logic 延迟：其条目被拒绝、重新入队，并在同一轮内恢复；被拒绝的规则 3 写入不发送 Kick；
  - 网关崩溃 → 租约过期 → 清扫 → 条目消失；
  - 同时杀掉 2 台网关：其他网关续期期间两个 epoch 都被清扫 *(R2-O3)*；
  - 杀掉 3 台网关，剩下一台按 10 秒节奏续期：保护闸的 `renewing_since` 不被重置，三个 epoch 都在保护闸等待后被清扫 *(R3-O7, R3-O2)*；
  - Redis 暂停 90 秒：什么也不清扫；之后会话仍在，或由 Reconcile 恢复；
  - 网关→Logic 阻断 90 秒、Logic↔Redis 正常，然后恢复：什么也不清扫 *(R3-O2)*；
  - Logic 重启，续期在清扫器启动 30 秒以后才恢复：什么也不清扫 *(R3-O2)*；
  - 对存活网关强制清扫后，其会话由触发式轮次在约 1 分钟内恢复 *(R2-O8)*；
  - 清扫中止 *(R3-O3)*：开始清扫一台存活网关的 epoch（删除租约 key），让网关在清扫中途续期：回复为 `missing`，清扫器中止，不执行 `UNLINK`/`ZREM`，并删除其认领 key *(R3-OP6)*，会话在约 1 分钟内恢复。网关启动后的第一次租约调用报告 `missing`，不触发对账 *(R3-OP5)*。单纯租约过期、未被清扫后的续期为 `missing`，并触发一轮对账；
  - 时钟偏差 *(R4M-5)*，在网关中使用可注入的伪造时钟（偏差余量不随测试配置缩放）：把一台网关的时钟调快 2 秒：偏差告警触发，迟到的 Login 写入留下的幽灵由下一轮的删除阶段移除；把一台网关调慢 5 秒：它上面的 Login 都是 `EXPIRED`，脚本化客户端重试并通过另一台网关登录；
  - 过时的租约观察 *(R4M-2)*：在 G42 更新的会话 S2 已存储的情况下饿死 G42 的租约（删除 key）；投递 G17 上更旧的条目 S1，并丢弃其 Kick：规则 4 覆盖 S2，随后 G42 的触发式轮次在缩放后的时限内恢复 S2 并踢掉 S1；
  - 租约 key 是权威依据 *(R4M-3)*：让一台网关的 `ZADD gwepochs` 失败 9 秒（即配置中的 90 秒），而 `SET` 成功：什么也不清扫；在清扫中途删除租约 key 但不改分数，再重新加回：清扫器中止；已发送 `EpochEnded` 的 epoch 无需等待其 TTL 即被清扫；
  - Reconcile：幽灵条目在一个周期内被移除；缺失的条目被恢复；另一台网关上更新的会话永远不会被覆盖；被丢弃的 Logout 在一个周期内修复；
  - Kick：发往存活网关的 Kick 被丢弃后会重试，之后被下一轮 Reconcile 兜住 *(R2-O6)*；`AUTH_PENDING` 期间的 Kick 在 Login 回复返回时关闭连接；正在下线的网关仍能收到 Kick；
  - Revoke 关闭两个槽位上的连接；Kick 被丢弃的 Revoke 被下一轮 Reconcile 兜住；`WAIT` 失败的 Revoke 返回错误，重试是幂等的 *(R2-O4)*；
  - 吊销比 token 活得更久 *(R4M-1)*：devtoken 签发有效期 2 秒的 token（短于配置中 9 秒的故障，因此旧的"token 有效期 + 余量"TTL 会在故障期间过期）；把一台网关与 Logic 断开 9 秒，吊销它的一条连接并丢弃 Kick，恢复连通：触发式轮次以 `REVOKED` 关闭该连接，且不恢复它；
  - 槽位级 Revoke *(R3-OP2)*：Kick 被丢弃时，下一轮 Reconcile 通过 `not_owner`（`REVOKED`）关闭连接，且不恢复会话；同一用户的其他槽位不受影响；`WAIT` 失败的槽位级 Revoke 返回错误且不发送 Kick，重试是幂等的；
  - 密钥轮换：重叠期间两个 `kid` 都被接受；在 Logic 拉取到之前就开始签名的密钥会得到 `KEY_UNKNOWN`，后台刷新之后登录成功 *(R4H-O10)*；
  - JWKS *(R2-O7)*：账号服务宕机时 Logic 启动，从 `jwks-fallback` ConfigMap 变为 Ready 并接受 token；两个来源都没有密钥集的 Logic Pod 永远不会 Ready。
- **Redis 故障切换测试**（Tier 2）*(R2-O5)*：对存放租约 key 的分片做故障切换并测量耗时；在配置的 TTL 下租约必须存活。单分片故障切换期间，其他分片上 uid 的登录保持正常 p99 *(R5S-O1)*。另外同时故障切换一个可用区内的所有主节点（3 个主节点时为一个，6 个时为两个），连同该可用区内的副本；记录故障切换时间和受影响登录的比例；在配置的 TTL 下租约必须存活 *(R5S-O9)*。
- **压测**（`cmd/loadgen`，多源 IP；loadgen 记录每个客户端的发现时间和登录成功时间，直接计算 SLO 分布；除标明 Tier 3 的以外均为 Tier 2）：
  - (a) 单台网关 60 万连接（上限），配合 GOMEMLIMIT/GOGC 调优；
  - (b1) SIGKILL 一台 20 万连接的网关：99% 在 `T_death` 后 30 秒内重新登录（loadgen 记录 SIGKILL 时刻）*(R4M-17)*；发往它的 Kick 尝试约每次重新登录一次，不重试 *(R2-O6)*；记录每个 Redis 分片的 CPU 和 p99，并标出存放 `gwconns` key 的分片 *(R4M-9)*；
  - (b2) 对一台 20 万连接的网关执行 iptables DROP（静默节点丢失）：99% 在客户端发现后 30 秒内重新登录；Login p99 不受发往被丢弃网关的 Kick 影响，Logic 的 goroutine 数保持有界 *(R5S-O2)*；
  - (b-stress) 杀掉一台 60 万连接的网关：只测量，无 SLO；包含一次 60 万成员 `gwconns` 的 `SSCAN` + `UNLINK` *(R4M-9)*；
  - `gwconns` 分片负载 *(R4M-9)*：在连接上限处运行一轮触发式 Reconcile，记录每个 Redis 分片的 CPU 和 p99。Tier 2 的 Redis Cluster 在 Logic + Redis 的核上运行 ≥ 3 个主节点；由于共享核心，绝对数值会失真，因此通过标准是相对的：`gwconns` 所在分片的 p99 不超过其他分片 p99 的 2 倍；
  - (c) 150 秒心跳在 20 万和 60 万时的 CPU 开销，外加 **20 万下的 60 秒心跳**，以便在 `reconnect-after-idle` 迫使降低间隔时预先知道其代价 *(R4M-6)*；
  - Logic 固定在 `minReplicas`（不做 HPA 扩容）时的风暴：满足 SLO *(R2-O7)*；
  - **（Tier 3）** 总量 100 万时滚动重启所有网关（`TARGET=local`：100 万 × `TIER3_SCALE`）：没有网关超过上限，在上限处被拒的客户端在 SLO 内登录到别处（使用握手前 0.5–1.5 秒重试），且进入指数退避的客户端少于 1%；记录每一步各 Pod 的连接数，在最少连接 LB 上，重启过的 Pod 最终与均值相差不超过 10% *(R2-O2, R3-O5, R5S-O4)*。下线断言 *(R4M-7)*：在 NotReady 加上 LB 摘除延迟之后，正在下线的 Pod 不再接受新连接（`TARGET=local`：检查跟随 endpoints 的 LB，真实 NLB 的摘除延迟记为 `not_covered`）；正在下线的 Pod 上每个客户端都在连接关闭前收到 `Reconnect{jitter_ms}`；下线期间发往该 Pod 的 Kick 成功（`publishNotReadyAddresses`）；
  - 风暴：其他网关从不错过租约续期；
  - 握手洪峰：来自大量源的垃圾握手不影响心跳按时应答，合法的每秒 2 万风暴不会被令牌桶拒绝；
  - 慢速客户端洪峰：超时释放占位，合法客户端不会收到 `Busy`；
  - **全体用户重连** *(R5S-O6)*：一次性断开所有网关上的全部连接（重启本地 LB，或由 loadgen 在同一瞬间发 RST）；只测量，无 SLO：直到 99% 重新登录的时间，以及进入退避的客户端比例；已重连客户端的心跳按时应答，没有组件重启。再以所有客户端同时触发网络变化事件的方式重复一次；
  - **（Tier 3）** 节点丢失 *(R5S-O3)*：停掉网关所在节点（不只是 Pod）：超过阈值后节点被施加污点，`gw-k` 在目标时间内于另一节点 Ready（`TARGET=local`：停掉 k3s agent 节点上的 kubelet；若本地集群无法模拟，记为 `not_covered`）；
  - **（Tier 3）** 中断预算 *(R5S-O5)*：并行 `kubectl drain` 两个网关节点：第二次驱逐要等第一台网关重新 Ready，任何时刻都不会有两台网关不可用；drain Logic 节点永远不会使 Ready 的副本少于 `minReplicas`；
  - **（Tier 3 `cloud`）** LB 限制 *(R5S-O7)*：把 LB 的空闲超时和连接上限记录在 JSON 结果中，违反 *部署要求* 时本次运行失败；`TARGET=local` 将其列为 `not_covered`；
  - Logic 变慢：登录队列上限触发 `Busy`，客户端在 SLO 内重试成功；在按上文规则设定队列上限后，登录被拒为 `Busy` 而不是 `EXPIRED`（`expired` 保持接近零）*(R4H-O2)*。

## 不在范围内（后续子项目）

消息（seq、去重、MongoDB）· 推送链路（RocketMQ、APNs/FCM）· 社交动态 · Web 扫码登录 · 客户端自适应心跳 · 账号服务（它的事件只通过 `Revoke` 和 JWKS 到达我们）· 发送路径的所有权强制（子项目 2）· 会话有效性读取路径及其在故障期间租约被判无效时的缓解（子项目 2/3）· 网关间主动连接再均衡（子项目 5）· 可用区丢失的容量规划 · proxy protocol 与真实客户端 IP（Web 阶段）· 以 device_id/user_id 为键的防滥用限流。

## 待定问题

rev3 的待定问题 4（连接上限计什么）、5（Reconcile 轮次重叠）和 6（截止时间拒绝哪些 Reconcile 写入）已由第三轮评审确认，并已写入正文（*准入控制*、*Reconcile*、*Reconcile 处理*）。rev4 的待定问题 2（槽位级 Revoke）已由槽位级吊销标记解决，待定问题 4–7 已确认并写入正文（rev3 决议中的 *R3-OP2*、*R3-OP4*–*R3-OP7*）。

rev4 遗留的第一个待定问题（Reconcile 与租约检查的原子性）由 *R4M-2* 关闭：租约检查只是一次观察，基于过时观察做出的写入由归属网关的触发式轮次纠正（见 *Reconcile 处理*）。

rev4 遗留的另一个待定问题（登录 Lua 中 `REVOKED` 与 `EXPIRED` 的顺序）由用户决定 *(R4H-OP1)*：先检查吊销，理由见 *登录 Lua*。

rev5 没有待定问题。rev6 把两个选择留给计划，两者都记录在 rev5 决议中：Reconcile 块是否也按条目流式返回结果（非必需：慢分片在那里只会延迟块的了结并导致重新入队，*R5S-O1*），以及每目标 Kick 队列的大小和 SDK 钳制范围（*R5S-O2*、*R5S-O6*）。

本版没有遗留的待定问题。
