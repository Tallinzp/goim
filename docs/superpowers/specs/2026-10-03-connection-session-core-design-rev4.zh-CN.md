# 子项目 1：连接与会话核心 —— 设计（第 4 版）

> 本文是 `2026-10-03-connection-session-core-design-rev4.md` 的中文译本。两者不一致时，以英文版为准。标识符、命令、Redis key、指标名和引用标记（如 *(R1-O4)*）保持英文原样。

> 本版是 `2026-10-03-connection-session-core-design-rev3.md` 的第 4 版修订，rev3 保持不变。本版落实了 `docs/superpowers/objections/connection-session-core-design-rev3-dispositions.md` 中的全部决议（第三轮评审的 O1–O7，以及它对 rev3 待定问题 4–6 的澄清），以及同一文件中记录的、针对 rev4 自身待定问题的后续决议（*R3-OP2*、*R3-OP4*–*R3-OP7*）。每轮评审的异议都从 O1 开始编号，因此引用带前缀：*(R1-O4)* 指 `connection-session-core-design-dispositions.md`（第一轮评审），*(R2-O4)* 指 rev2 决议（第二轮评审），*(R3-O4)* 指 rev3 决议（第三轮评审）。本版需要自行解读的地方列在文末的 **待定问题** 中。

## 背景

我们要为一款类微信社交 App（Android、iOS、Web）构建 Go 语言的 IM 后端，目标约 100 万同时在线用户。用户此前用 C++ 做过一个支撑 260 万 TCP 连接（单网关 60 万）的 IM。两份早期草稿（`gpt_design.md`、`claude_design.md`，在 `4ce1fa6` 中加入，在 `05cd570` 中删除）在在线模型和范围上有分歧，并且都没有解决"延迟断开删除新会话"的竞态。

整个系统拆分为 5 个子项目（1 连接/会话核心，2 消息，3 推送链路，4 社交动态，5 扩容/加固）。**本文只覆盖子项目 1**：建连、加密握手、登录、心跳、踢下线、吊销、断开——在重连竞态下保持正确，并满足风暴恢复 SLO。子项目 1 交付基于 TCP 的 Android/iOS；Web 传输在此设计，之后再实现。

**成功标准**
- **容量上限** *(R1-O1)*：单台网关承载 60 万连接（测量 CPU / 内存 / GC）。这是压测基准，不是运行点。
- **风暴 SLO** *(R1-O1, R1-O7, R2-O3)*：覆盖**单台网关**丢失，按 20 万的运行点计算，按故障类别定义：

  | 故障类别 | 客户端如何发现 | SLO |
  |---|---|---|
  | 进程崩溃 / Pod 被杀（节点存活） | 内核 RST/FIN，经四层 NLB 透传 | 99% 在**网关死亡后** 30 秒内重新登录成功 |
  | 节点丢失 / 静默网络分区 | 客户端心跳（Pong 超时） | 99% 在**客户端发现故障后** 30 秒内重新登录成功。发现时间由设计保证有上界：最多一个心跳间隔加 Pong 超时。 |

  算术：20 万用户分散在 0–20 秒的重连抖动中，约每秒 1 万次登录，在每秒 2 万的峰值余量之内。

  同时失去多台网关（例如整个可用区）属于**降级、尽力而为、无 SLO** *(R2-O3)*：客户端可能被 `at_capacity` 拒绝，并持续重试，直到丢失的 Pod 被重新调度。
- **正确性：** 任何过期事件（延迟断开、同设备或其他设备的延迟登录、僵尸网关、延迟的 Reconcile 恢复、延迟的清扫）都不能删除或覆盖更新的会话；由自动化测试覆盖。

**密度** *(R1-O1, R2-O2, R2-O3)*：运行点为单网关最多 20 万连接。100 万用户至少需要 6 台网关（5 台各 20 万，加 1 台用于 N−1），稳态下每台约 16.7 万。在 Kubernetes 上 Pod 很便宜，真正的代价是爆炸半径；60 万仍作为效率基准。单网关硬连接上限（默认 25 万，见 *准入控制*）限制了倾斜：一台网关宕机时，其余五台最多承载 5 × 25 万 = 125 万 ≥ 100 万；整个可用区宕机（剩 4 台）时仍能勉强承载 100 万。

## 决定（已达成一致）

| 主题 | 决定 |
|---|---|
| 在线模型 | 每个 **槽位（slot）** 一个会话，slot = `userid + appid + platform`（`mobile` 涵盖 Android 和 iOS；另有 `pad`、`web`、`pc`）。同一槽位上的新登录替换旧登录。platform 会与 token 校验。不同平台是不同槽位，永不冲突。 |
| 客户端传输 | 移动端：通过 gnet 的原生 TCP。Web：基于 wss 的 WebSocket（TLS 在云 LB 终结），现在在传输接口之后完成设计，作为后期切片实现。 |
| 客户端加密 | 自定义应用层加密，使用 **Noise Protocol Framework 的 NK 模式**（`flynn/noise`）：服务端静态公钥固定在 App 中，带 key ID 以便轮换；每帧使用 ChaCha20-Poly1305。不做会话恢复票据（一次握手约 100µs，风暴中可忽略）。Web 依赖 wss 的 TLS，不使用 Noise。 |
| 内部 RPC | 双向 gRPC。网关→Logic：`dns:///` headless service + `round_robin`；Logic 服务端设置 `MaxConnectionAge` ≈5 分钟，**以及 `MaxConnectionAgeGrace` = 30 秒**（大于 6 秒的 Login 超时），使进行中的批次在 GOAWAY 后能完成 *(R1-O12)*。`GatewayLease` 使用独立的 gRPC 连接 *(R1-O2)*。Logic→网关：Kick 按 StatefulSet Pod DNS（`gw-17.gateway.im.svc`）拨号，无需注册中心；网关的 headless service 设置 `publishNotReadyAddresses: true`，使正在下线的 Pod 仍可被解析 *(R1-O8)*。 |
| 认证 | 独立的账号服务签发 Ed25519 签名的 JWT，作为**短期访问 token（例如 1 小时），只在 Login 时校验**。token 过期不影响已认证的连接；长连接的安全依靠吊销 *(R1-O10)*。Logic 用按 `kid` 索引的 JWKS 在本地校验 token（登录时不查数据库），冷启动时可回退到 ConfigMap *(R2-O7)*。`cmd/devtoken` 签发 token 并发布测试用 JWKS。 |
| 事实来源 | 网关是"有哪些连接"的事实来源。Redis 通过周期性反熵（`Reconcile` + `ReconcileEnd`）与之对齐 *(R1-O3, R3-O1)*。 |
| 部署 | 云上 Kubernetes。网关以 StatefulSet 运行在四层 NLB 之后，在 `topology.kubernetes.io/zone` 和 `kubernetes.io/hostname` 上设置 `topologySpreadConstraints`，`maxSkew: 1`，使单节点故障是真正的 N−1 事件 *(R2-O3)*。Logic 是带 HPA 的无状态 Deployment；它的 `minReplicas` 必须**在不扩容的情况下承受每秒 2 万次登录的峰值**，以压测测得为准。HPA 在此之上扩容；SLO 不依赖它 *(R2-O7)*。 |
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

## 客户端协议

- 外层帧：`[u32 length][payload]`，最大 64KB。网关缓冲不完整的帧，并拆分一起到达的多个帧。
- 第 1 帧：Noise NK 握手（携带 key ID）。此后每个 payload 都经过 AEAD 封装，每个方向各有一个 nonce 计数器。
- 明文：`Envelope{cmd, req_id, body}`。命令：`Login`、`LoginResp`（包含心跳间隔）、`Ping/Pong`、`Logout`、`Kick{reason}`（服务端发出）、`Reconnect{jitter}`（服务端发出）、`Error`、`Busy{retry_after}`。
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
  | `EXPIRED` | 登录到达 Redis 的时间比网关收到它晚了超过 `login_deadline` *(R1-O4)*。 | 重试。 |
  | `REVOKED` | token 签发时间早于用户的吊销标记 *(R1-O10)*。 | 丢弃 token，显示登录页。 |
- 踢下线原因。**任何一种都不会触发自动重连：**
  - `REPLACED`：同一设备，更新的连接；不向用户展示。客户端进入空闲，只在用户操作或 App 回到前台时重连。SDK 将其记录为异常，因为存活的客户端只有在另一个实例共用其 device_id 时才会看到它 *(R1-O6)*。
  - `KICKED_BY_OTHER_DEVICE`：向用户展示。
  - `REVOKED` *(R1-O10)*：客户端丢弃 token 并显示登录页。
- **device_id** *(R1-O6)* 标识一个持有连接的客户端实例。SDK 必须保证同一时刻每个 device_id 最多只有一条连接在用。移动端和桌面端：每次安装一个，外加进程级保护（例如 Android 上的单一连接服务）。

### 客户端 SDK 规则

- **conn_seq 持久化** *(R1-O5)*：在每次连接尝试**之前**持久化 `conn_seq`，存放位置和生命周期与 device_id 相同（例如两者都放在 iOS Keychain），这样清除一个就会清除另一个。
- **重连** *(R1-O7, R3-O5)*：
  - 意外失去一条**已建立的**连接后，首次尝试前随机等待 **0–20 秒**（为握手、登录和一次 Busy 重试留出 10 秒），之后指数退避（上限 5 分钟）。遵守 `retry_after`。
  - **服务端在 Noise 握手完成前关闭的 TCP 连接，按一次 `Busy` 重试处理：等待 1–3 秒（带抖动）。** 这覆盖 `at_capacity` 和 `handshake_rate` 拒绝，它们在任何分帧之前就关闭 socket，因此无法发送 `Busy{retry_after}`。**连续 5 次**这种关闭后，回退到指数退避（上限 5 分钟）。登录成功后计数清零。
  - 每次尝试使用新的源端口，使 NLB 的流哈希可以选中另一台网关 *(R2-O2)*。
- **服务端失联检测** *(R1-O7)*：发出 Ping 后 **10 秒**内没有收到任何帧，就关闭并重连。
- **Web**（现在设计，之后实现）*(R1-O6)*：每个浏览器 profile 一条连接。各标签页通过 `SharedWorker` 选举 leader，回退方案为 Web Locks 加 `BroadcastChannel`。leader 持有 wss 连接并向其他标签页转发消息；leader 标签页关闭时，由另一个标签页接管。device_id 和 conn_seq 存放在 localStorage。

## 网关

- gnet **≥ v2.8.0**（需要 `Conn.EventLoop()`）。不为每个连接开 goroutine。`OnTraffic` 从不阻塞；RPC 通过有界工作池执行。
- 连接状态：`HANDSHAKE → AUTH_PENDING → ONLINE → CLOSED`。
- **标识：** `gateway_id` = Pod 名；`epoch` = 启动时随机生成的 uint64；`conn_id = (worker_id << 48) | loopState.counter++`。
  - 已对照 gnet 的 `dev` 源码确认：`OnOpen` 总是在所属 loop 的 goroutine 上运行（两条 accept 路径都是）。loop 序号未导出，但 `Conn.EventLoop()` 给出了 loop 的身份。
  - `worker_id` 在 `OnOpen` 第一次见到某个 loop 时惰性分配（一个原子操作，大约执行 NumCPU 次），缓存在从 EventLoop 到 loopState 的 `sync.Map` 中。
  - 普通的 `counter++` 之所以安全，只因为只有 `OnOpen` 会访问它。计数器不导出，并有一个 `-race` 测试来保证这条规则。
- **无锁的按 loop 连接表：** `map[conn_id]*conn`，只由所属 loop 访问。来自 gRPC 的 Kick 读取 `conn_id >> 48`，拿到该 loop 的句柄，调用 `loop.Execute(...)` 查找连接、检查 epoch 和状态并执行动作。这同时满足了 gnet 要求 Close 在所属 loop 上执行的规则。
  - **被跟踪的连接** *(R3-O1)*：连接从 accept 起一直留在其 loop 的表中，直到它不再需要 Logout——即它从未发送过 Login 就关闭了，或它被栅栏延后的 Logout 已被确认或已从重试队列中丢弃。等待 Logout 栅栏的 `CLOSED` 条目仍被跟踪。`ReconcileEnd` 上报的就是这个跟踪集合。
- **Kick 处理** *(R1-O8)*：
  - `ONLINE`：发送 Kick 帧并关闭。
  - `AUTH_PENDING`（Login 进行中）：设置带原因的 kick-pending 标记。Login 回复到达时，发送 Kick 帧并关闭，而不是进入 `ONLINE`。
  - `HANDSHAKE`、`CLOSED`、未知 conn_id 或 epoch 不匹配：忽略。
- **心跳：** **默认 150 秒，由服务端在 `LoginResp` 中下发**。任何帧都会重置计时器。2 × 间隔 + 10 秒（= 310 秒）无响应即判定连接死亡，网关随即发送 Logout（受栅栏约束）。计时器由时间轮驱动。
- **超时** *(R1-O11)*：`HANDSHAKE` 5 秒（accept → 握手完成）；`AUTH_PENDING` 5 秒（握手完成 → 收到 LoginReq）。超时后静默关闭连接。两者都运行在时间轮上。
- **准入控制** *(R1-O11, R2-O2)*：
  - **连接上限** *(R2-O2, R3 澄清)*：单网关对打开的 **socket** 设硬上限，从 accept 到关闭计数，覆盖所有状态（可配置，默认 **25 万**；必须介于 20 万运行点和 60 万上限之间）。仅为等待 Logout 而保留在表中的已关闭条目不计入。达到上限时，网关在 accept 时、任何加密运算之前关闭新连接，并计一次 `at_capacity` 拒绝。实现上就是一个计数器加 accept 时的一次检查。上限以下的倾斜无害；主动再均衡不在范围内（子项目 5）。
  - **Noise 握手限流（单网关全局；绝不按 IP）：** 对同时进行的握手计算设并发上限（信号量，大小等于专用于握手的核数），再加一个全网关的每秒握手令牌桶，按实测 NK 开销（约 100µs）设定，避免握手拖垮已建立的连接。令牌桶必须至少允许每秒 2 万的风暴峰值并留有余量；在正常风暴中它起整形作用而非拒绝。任一耗尽时，网关不开始握手：关闭连接并计一次 `handshake_rate` 拒绝。客户端把这种关闭当作一次 `Busy` 重试（见 *客户端 SDK 规则*）*(R3-O5)*。
  - **登录队列上限：** 限制等待或正在调用 Logic RPC 的 Login 数量（反映 Logic 的背压）。达到上限时，回复 `Busy{retry_after}` 并关闭。
  - **仅为内存的待处理上限：** 对 `HANDSHAKE` + `AUTH_PENDING` 连接总数设一个较大的上限，只为保护网关内存。正常风暴中不得触发。
  - `Busy{retry_after}` 使用较短的值，**1–3 秒带抖动**，使被拒客户端仍能落在 SLO 内 *(R1-O7)*。
  - 每条连接只允许一次登录尝试（失败即关闭）。
  - 排队时间超过 `login_deadline`（从 `recv_ms` 算起）的 Login 在发送前丢弃；它们从未到达 Logic，因此不需要 Logout。
- **recv_ms** *(R1-O4)*：网关在收到 Login 帧时打上 `recv_ms`（挂钟时间，NTP 同步），并在 Login RPC 中传递。
- **Login 批处理：** 每个 RPC 最多 200 个登录或 5 毫秒。Login RPC 超时至少为 `login_deadline + 1s`（默认 6 秒），从 `recv_ms` 算起 *(R1-O9)*。
- **Logout 栅栏** *(R1-O9, R2-O1)*——网关不变式：只有当以下**两项**都满足时，网关才为一条连接发送 Logout。
  - **Login 条件**——满足其一：
    - (a) 它已有**明确的 Login 回复**（成功或拒绝），此时 Lua 写入要么已经发生，要么永远不会发生；或
    - (b) 已过了 `recv_ms + login_deadline + 1s`（1 秒用于吸收网关↔Redis 时钟偏差）。适用于**任何**没有明确回复的 Login：超时、RPC 错误、被 `MaxConnectionAge` 重置的 gRPC 连接，或 Logic 崩溃。
  - **Reconcile 条件** *(R2-O1)*：
    - (c) 没有任何包含该连接的 Reconcile 块尚未了结。一个块在收到明确回复，或已过 `sent_ms + login_deadline + 1s`（超时、RPC 错误、Logic 崩溃）时算了结，因为 Reconcile 的 compare-and-set Lua 在 `sent_ms + login_deadline` 之后拒绝写入。`refused` 条目结果也算明确回复 *(R3-O6)*。实现为每连接一个 `in_reconcile_chunk` 标记，构建块时设置，块了结时清除。

  在栅栏满足之前关闭的连接保留一个待发 Logout，栅栏满足时发出。Logout 经过有界重试队列（60 秒）；丢弃是安全的，因为下一轮 Reconcile 会修复 *(R1-O3, R3-O1)*。
- **租约** *(R1-O2, R3-O3)*：每 10 秒在**专用 gRPC 连接**上调用一次 `GatewayLease`，从不与 Login/Logout/Reconcile 合批或排在它们之后。回复说明租约是 **live** 还是 **missing**：只要续期时发现租约 key 不存在，就是 `missing`，不论 `gwepochs` 成员处于什么状态。续期本身会重新登记该 epoch（租约 key 和 `gwepochs` 分数）。收到 `missing` 时，网关立即启动一轮触发式 Reconcile。启动后的第一次租约调用总会发现 key 不存在（新 epoch）；网关忽略那一次的 `missing`，既不计数也不处理 *(R3-OP5)*。租约过期但从未被清扫的情况会多花一轮不必要的触发式对账，由于 TTL 的推导方式，这很少见 *(R2-O5)*。
- **Reconcile** *(R1-O3, R1-O8, R1-O10, R2-O1, R2-O8, R3-O1, R3-O6)*：网关把它的 ONLINE 集合——`(uid, appid:platform, conn_id, device_id, conn_seq, login_ms, tok_iat)`——分成限速的块发送，然后以 `ReconcileEnd` 结束本轮。
  - **实时块** *(R2-O1)*：每个块在发送的那一刻从按 loop 连接表中读取（通过 `loop.Execute`），而不是从本轮开始时冻结的快照中读取。每个块带 `sent_ms`（发送时的网关挂钟时间）。构建块时对其中每条连接设置 `in_reconcile_chunk`（见 Logout 栅栏）。
  - **逐条结果** *(R3-O6)*：Logic 对每条返回 `written`、`unchanged`（compare-and-set 输给了并发修改）*(R3-OP7)*、`refused`（超过截止时间）或 `not_owner`（附原因）。这四种都算 fence (c) 的明确回复。对每条 `not_owner`，网关发送带该原因的 Kick 帧并关闭连接。`refused` 条目的 conn_id 放回本轮队列，在其新块发送时从实时表中重新构建；如果连接已不是 ONLINE 则跳过。每条在一轮中最多重新入队 3 次，因此一轮总会结束；之后留给下一轮。
  - **轮次结束** *(R3-O1)*：当所有块（包括重新入队的条目）都已发送并了结，一轮即完成。网关随后收集它仍在**跟踪**的所有 conn_id（见 *按 loop 连接表*），不论状态，包括 Logout 尚未发出的已关闭连接，打上 `as_of_ms`（网关挂钟，与 `recv_ms` 同一时钟），并发送 `ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`。若 `ReconcileEnd` 失败，用相同 payload 通过与 Logout 相同的有界重试队列（60 秒）重试，之后丢弃；下一轮会补上遗漏 *(R3-OP4)*。延迟重试是安全的：在 `as_of_ms` 时不在列表中的 conn_id 已通过其 Logout 栅栏，且 conn_id 永不复用，所以之后不可能再出现针对它的写入。被丢弃的 `ReconcileEnd` 会让该轮的幽灵会话上界延长到两个周期。大小：8 字节 × 25 万（上限）≈ 2MB，低于 gRPC 默认的 4MB；在 60 万基准下需拆分列表或调高上限。
  - **两种节奏** *(R2-O8)*，均可配置：
    - **周期式**：约每 10 分钟一次，每台网关带随机抖动，分摊在整个周期内（20 万时约每秒 330 条）；
    - **由 `missing` 租约回复触发**：每台网关约每秒 5000 条（20 万约 40 秒；若 6 台网关同时进行，全集群约每秒 3 万次 Lua 调用）。
  - **同一时间只跑一轮** *(R3 澄清)*：每台网关最多同时运行一轮。周期轮进行中收到 `missing` 回复会取消它：被取消的轮次不再构建或发送任何块，也不发送 `ReconcileEnd`。触发式轮次在被取消轮次的在途块全部了结后（≤ `login_deadline + 1s`）开始，因此一条连接永远不会同时处于两个未了结的块中。
- **优雅下线：** 收到 SIGTERM 后，变为 NotReady 并停止接受连接，在约 60 秒内分批发送 `Reconnect{jitter}`，关闭剩余连接，然后调用 `EpochEnded`。`terminationGracePeriodSeconds` 必须长于下线窗口。正在下线的网关仍能收到 Kick（`publishNotReadyAddresses`）。

## Logic

- 无状态。每个节点都处理所有 RPC，并能踢任何网关上的连接。水平扩展（HPA 基于 CPU）；`minReplicas` 自身就能承受风暴峰值 *(R2-O7)*。
- **RPC** *(R1-O12, R3-O1)*：`Login`（批量）、`Logout`（批量）、`GatewayLease`（独立通道和**独立工作池**，使 Login 负载无法饿死它 *(R1-O2)*）、`EpochEnded`、`Reconcile`（批量或流式）、`ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`、`Revoke`。
- **Login：** 在本地校验 JWT（按 `kid` 查 JWKS），然后用 `recv_ms` 和 token 的 `iat` 执行登录 Lua 脚本（EVALSHA，管道化），再做后续处理：SADD 到 epoch 索引；如果结果需要踢人，使用下文的加固 Kick 发送。
- **GatewayLease** *(R3-O3)*：用 `SET gwlease:gw:{epoch} 1 PX <TTL> GET` 续期并更新 `gwepochs` 分数。旧值为 nil 表示 key 不存在：回复 `missing`；否则回复 `live`。
- **加固 Kick** *(R1-O8, R2-O6)*：
  - **第一次尝试总是发送**，即使目标的租约看起来已过期（网关上的 epoch 不匹配会使误发的 Kick 无害）。
  - **重试**（传输错误时，约 30 秒内最多 3 次）只在目标 `gw|epoch` 租约存活时进行。Logic 把租约状态缓存约 1 秒；epoch 只有寥寥几个。
  - Kick RPC 采用 **fail-fast**（不使用 `WaitForReady`）。
  - 放弃时，递增一个带 `target_lease=live|dead` 标签的指标。Reconcile 是兜底。
- **JWKS** *(R1-O10, R2-O7)*：
  - 启动时从账号服务加载 Ed25519 密钥集。失败则从挂载的 ConfigMap（`jwks-fallback`）加载，并按正常刷新周期继续重试账号服务。
  - Logic **只有在持有某一来源的密钥集后才报告 Ready**。
  - 每 5 分钟刷新。轮换期间接受所有已发布的密钥。刷新失败时保留最后一份有效密钥集并告警。Pod 使用回退密钥集期间持续告警。
  - 遇到未知 `kid` 的 JWT 最多触发一次限速的刷新，然后才拒绝。
  - **密钥轮换操作手册：** 新签名密钥在签发任何 token 之前先发布到 `jwks-fallback` ConfigMap；因泄露而撤回的密钥也要从 ConfigMap 中删除。（过期的 ConfigMap 只在账号服务不可达时才有影响。）
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
  - **截止时间的适用范围** *(R3-O6)*：截止时间只拒绝创建或替换字段的写入（规则 3 和 4）。迟到的 TTL 刷新和 `gwconns` SADD 无害，不拒绝。被拒绝的写入返回 `refused`；网关在本轮内重新入队。
  - 因字段已变化而失败的 compare-and-set 是一个没有写入的明确结果，报告为 `unchanged`（不是 `refused`，因此不重新入队），且不发送 Kick；由下一轮处理该条目。

  **删除阶段** *(R3-O1)*：收到 `ReconcileEnd` 的 Logic 节点 SSCAN `gwconns:gw:{epoch}`。对每个 uid 读取 `sess:{uid}`，对每个指向本 `gw|epoch`、`conn_id ∉ tracked_conn_ids` 且 `login_ms < as_of_ms` 的字段执行 compare-and-delete。它还会把已没有任何字段的 uid 从 `gwconns` 中移除。为什么安全：conn_id 只有在其 Logout 栅栏满足后才离开跟踪集合，因此针对它的 Login 或 Reconcile 写入都不可能再落地；而 Login 在 `as_of_ms` 当时或之后才被收到的连接，会被 `login_ms` 检查排除。

  开销：每 10 分钟 20 万条，即每台网关约每秒 330 次 Lua 调用，6 台合计约每秒 2000 次；触发式轮次每台网关约每秒 5000 次 *(R2-O8)*。

## Redis 结构（Cluster；按用户的 key 使用 `{uid}` 哈希标签）

| Key | 类型 | 值 / TTL | 用途 |
|---|---|---|---|
| `sess:{uid}` | HASH | 字段 `appid:platform` → `gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat`；`login_ms` 是网关的 `recv_ms` *(R1-O4)*；key TTL 30 天，在登录和 Reconcile 时刷新 | 在线标记（Redis 中的事实来源） |
| `rev:{uid}` | HASH | 字段 `*` → scope=all 的 `revoked_before_ms`；字段 `appid:platform` → 该槽位的 `revoked_before_ms` *(R3-OP2)*。每个字段按 `max(stored, new)` 写入并用 `WAIT` 确认 *(R2-O4)*；key TTL = 最长 token 有效期 + 余量 *(R1-O10)*，每次写标记时刷新 | 吊销标记（用户级和槽位级） |
| `gwlease:gw:{epoch}` | STRING | `PX <lease TTL>`，每 10 秒用 `SET ... PX <TTL> GET` 续期（旧值为 nil 表示 `missing`）*(R3-O3)*。租约 TTL 可配置（默认 30 秒），必须满足 **TTL > 实测分片故障切换时间 + 一个续期间隔** *(R2-O5)* | 网关实例存活 |
| `gwepochs` | ZSET | 成员 `gw:epoch`，分数 = 最后一次续期的毫秒时间 | epoch 登记表，供清扫器使用 |
| `gwconns:gw:{epoch}` | SET | uid 集合；由 Reconcile 删除阶段修剪。每个 epoch 一个 key，因此一台网关的所有 `SADD` 都落在同一分片；在风暴速率（约每秒 1 万）下可以接受 *(R2-O9)* | 清理线索（与 sess 不是原子的，正确性不依赖它） |
| `sweep:gw:{epoch}` | STRING | `SET NX PX 300000` | 清扫器认领 |

**一个会话有效**，当且仅当其字段存在，**并且** `EXISTS gwlease:{its gw}:{its epoch}`。

> 有效性规则在此定义，但面向使用方的高效读取路径推迟到子项目 2/3。它们**不得**实现为每个 epoch 一次 `HGETALL` + `EXISTS` *(R1-O12)*。

**复制** *(R2-O4)*：Redis Cluster 异步复制；一次故障切换可能丢失约 1 秒已确认的写入。`sess:` 和 `gwconns` 的丢失由 Reconcile 修复（规则 3 选出正确的赢家，规则 4 负责恢复）；租约的丢失由下一次续期弥补。`rev:{uid}` 是例外：它的丢失无法被修复，因此 Revoke 对两种 scope 的每次标记写入都用 `WAIT` 确认。

### 登录 Lua（单 slot，原子）

输入：新条目、`recv_ms`、token 的 `iat`、`login_deadline`（默认 5 秒）。步骤：

1. 读取 `rev:{uid}` 的 `*` 和 `appid:platform` 字段。若 `iat <` 两者的最大值（生效标记）→ `REVOKED`；不写入 *(R1-O10, R3-OP2)*。
2. 读取 Redis `TIME`。若 `now − recv_ms > login_deadline` → `EXPIRED`；不写入 *(R1-O4)*。
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

假设 *(R1-O4)*：网关之间的时钟偏差（NTP，几毫秒）远小于一个人切换设备所需的时间。

### Logout Lua

只有当字段仍匹配 `gw|epoch|conn_id` 时才 `HDEL`。由于 conn_id 永不复用，这解决了延迟断开竞态，包括同一网关上的情况。与该连接自身 Login 以及对它的任何 Reconcile 恢复之间的先后顺序，由网关的 Logout 栅栏保证。

### 清扫器（每个 Logic 节点中的 goroutine）

查找最后一次续期早于 **2 × 租约 TTL**（默认 60 秒）*(R2-O5)*、或已发送 `EpochEnded` 的 `gwepochs` 成员。用 `sweep:` SET NX 认领每一个，并**记录认领时该 epoch 的 `gwepochs` 分数** *(R3-O3)*。执行 `SSCAN gwconns`，对每个 uid 运行一个 Lua 脚本，只删除 `gw|epoch` 与该死亡 epoch 匹配的字段。限速约每秒 1 万用户。

- **续期即中止** *(R3-O3)*：在每个限速批次之间重新读取该 epoch 的 `gwepochs` 分数。如果比认领时的分数新，说明该 epoch 已续期：立即停止，不 `UNLINK` `gwconns`，不移除 ZSET 成员，`DEL` 掉 `sweep:gw:{epoch}` 认领，以免阻碍之后合法的清扫 *(R3-OP6)*，并计数 `sweep_aborted`。网关的那次续期已经报告了 `missing`（租约 key 曾不存在），因此其触发式 Reconcile 会恢复所有被删除的内容。
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
5. **Login 进行中连接关闭** *(R1-O9)*：Logout 栅栏把 Logout 推迟到收到明确的 Login 回复（fence a），因此 Logout 总在写入之后落地。
6. **Login 没有明确回复，但 Redis 已被写入**（超时、RPC 错误、`MaxConnectionAge` 重置、Logic 崩溃）*(R1-O9)*：Lua 截止时间意味着 `recv_ms + login_deadline` 之后不可能再有写入；网关在发送 Logout 前等待 `recv_ms + login_deadline + 1s`（fence b），因此 Logout 总在任何可能的写入之后落地。
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
13. **两个 loop 同时生成 conn_id：** 每个 loop 有自己的计数器，worker_id 前缀使 ID 互不冲突。
14. **Reconcile 轮次期间连接关闭** *(R2-O1)*：其 Logout 被推迟到包含它的块被确认（fence c），或该块的截止时间已过，因此 Logout 总在任何恢复之后落地。在其块构建之前就关闭的连接根本不在本轮中。
15. **Redis 故障切换丢失写入，旧会话被恢复** *(R2-O4, R3-O4)*：如果字段回滚到旧会话而更新的连接仍存活，Reconcile 规则 3 按登录表的排序比较，写回更新的条目并踢掉旧连接。该排序包括回退窗口，因此通过计数器回退胜出的会话在回滚后仍然胜出。平局时已存会话胜出，因此两个存活会话永远不会互相覆盖并互相关闭。
16. **Reconcile 轮次前后 Login 在途或 Logout 丢失** *(R3-O1)*：Login 在途的连接是被跟踪的，因此在 `tracked_conn_ids` 中，删除阶段不会动它的字段；在 `as_of_ms` 之后收到的 Login 无法通过 `login_ms < as_of_ms` 检查。Logout 被丢弃的连接已离开跟踪集合，因此其幽灵条目在同一轮结束时被删除——不论 Logout 是在其块发送之前还是之后被丢弃的。
17. **槽位级登出的 Kick 丢失** *(R3-OP2)*：连接在其网关上仍是 ONLINE，而它的 `sess:` 字段已被删除。已确认的槽位级标记使下一轮 Reconcile 将该条目返回为 `not_owner`（`REVOKED`，规则 1），而不是恢复它（规则 4），因此该设备在一个周期内被关闭。该用户的其他槽位不受影响。

注：同一设备重连时，`conn_seq` 也会替换任何幽灵条目。Reconcile 负责处理再也不回来的设备。

## 一致性契约 *(R2-O8)*

使用方（子项目 2/3）在设计缓存和兜底时可以依赖的上界：

| 使用方可能看到的偏差 | 原因 | 上界 | 由谁修复 |
|---|---|---|---|
| 幽灵：条目有效，但连接已不存在 | Logout 被丢弃 | ≤ 1 个 Reconcile 周期（约 10 分钟 + 抖动）：在一轮结束前任何时刻丢弃的 Logout，都在该轮结束时修复 *(R3-O1)* | Reconcile 删除阶段（`ReconcileEnd`） |
| 被替换或吊销（任一 scope）后旧持有者仍连着 | Kick 丢失 | ≤ 1 个周期 | Reconcile `not_owner` / 规则 1 *(R3-OP2)* |
| 死亡网关的条目 | 网关死亡 | 租约 TTL（30 秒）后失效；2 × TTL，加上最多一个 TTL 的保护闸等待 *(R3-O2)*，再加清扫时间后删除 | 有效性规则，然后清扫器 |
| 存活连接在 Redis 中缺失 | 故障切换丢失写入，或被误清扫 | ≤ 1 个周期；收到 `missing` 租约回复后 ≤ 约 1 分钟 | Reconcile 恢复 |
| 健康会话被判为无效 | Logic 或租约分片故障超过 TTL | 与故障持续时间相同 | 租约续期恢复 *(R2-O5)* |

## 风暴、下线与故障

- 客户端 SDK：见 *客户端 SDK 规则*（失去已建立连接后 0–20 秒抖动，握手前被关闭时 1–3 秒、最多连续 5 次，指数退避，10 秒 Pong 超时，遵守 `retry_after`）。
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
| Redis 分片故障切换 *(R2-O4, R2-O5)* | 可能丢失最多约 1 秒已确认的写入：`sess:`/`gwconns` 的丢失由 Reconcile 修复，`rev:` 由 `WAIT` 保护。租约能存活，因为其 TTL 大于实测故障切换时间加一个续期间隔。 |
| 网关进程崩溃 / Pod 被杀 | 客户端看到 RST/FIN，在 0–20 秒抖动内重连。租约在 TTL（30 秒）内过期；清扫器在 2 × TTL（60 秒）后清理，如果其保护闸计时刚开始，再加最多一个 TTL。每次重新登录有一次 Kick 尝试打到死亡的 epoch，不重试 *(R2-O6)*。 |
| 节点丢失 / 静默分区 | 客户端通过 Pong 超时发现（≤ 一个心跳间隔 + 10 秒），然后重连。服务端与崩溃情况相同。 |
| 同时失去多台网关（可用区）*(R2-O3)* | 尽力而为：幸存网关填满到连接上限；多出的客户端在 accept 时被关闭，1–3 秒后重试（最多 5 次，之后退避），直到丢失的 Pod 被重新调度 *(R3-O5)*。由于幸存网关持续续期，过期 epoch 会被清扫。 |
| Kick 无法到达网关 *(R2-O6)* | 目标租约存活时重试（约 30 秒内最多 3 次）；否则只尝试一次。之后由 Reconcile 兜底在一个周期内关闭连接。 |
| 握手洪峰 | 全局握手并发上限和令牌桶保护已建立的连接；多余的握手被关闭并计数。 |
| 慢速客户端 | `HANDSHAKE` / `AUTH_PENDING` 的 5 秒超时释放其占位。 |
| 坏帧 / 解密失败 / 帧过大 | 立即关闭。 |

## 对后续子项目的接口约束

- **所有权规则** *(R1-O8)*：一条连接只有在其 `gw|epoch|conn_id` 拥有 `sess:{uid}` 中自己的槽位时，才能以该用户身份行事（发消息、执行用户操作）。子项目 2 必须在发送路径上强制执行（缓存的所有权检查可以接受）。子项目 1 只负责记录这条规则，并提供它所依赖的数据（会话条目以及 Kick/Reconcile 投递）。
- **会话有效性读取路径** *(R1-O12)*：推迟到子项目 2/3；不得实现为每个 epoch 一次 `HGETALL` + `EXISTS`。
- **有效性依赖 Logic 和租约分片** *(R2-O5)*：如果 Logic，或存放某网关租约 key 的 Redis 分片，不可用时间超过租约 TTL，该网关的会话会被判为无效，尽管其连接是健康的。子项目 2/3 不得仅凭有效性触发大规模离线推送；缓解方案由它们设计。
- **一致性上界**：见 *一致性契约* *(R2-O8)*。
- 子项目 3 中的**惰性修复**（推送收到 "unknown conn" 时由 Logic 执行 compare-and-delete）是可选的；正确性已不再依赖它 *(R1-O3)*。

## 可观测性

Prometheus 指标：
- 每台网关按状态的连接数；等待 Logout 的已跟踪但已关闭条目；每秒握手数；**连接倾斜度**（每网关 ONLINE 数的最大值 / 平均值）*(R2-O2)*；
- 按结果分类的登录数：`ok`、`stale`、`replaced`、`kicked`、`superseded`、`expired`、`revoked`、`busy`、`error`；以及 `conn_seq_regressed`；
- 端到端登录延迟 p50/p99；gRPC 和 Redis 脚本延迟；
- **重新登录延迟**直方图：`offline_ms` +（登录成功时刻 − `recv_ms`），按 `reconnect_reason` 打标签 *(R1-O7)*；
- **死亡 epoch 交叉核对** *(R1-O7)*：当重新登录发现旧条目指向死亡 epoch 时，记录 `login_ms − 该 epoch 最后一次租约续期时间`（从网关死亡到重新登录的时间，±10 秒，无需客户端数据）；
- 租约续期失败；回复为 `missing` 的租约续期；
- 按原因分类的准入拒绝：`at_capacity` *(R2-O2)*、`handshake_rate`、`login_queue`、`pending_mem`、`handshake_timeout`、`auth_timeout`；
- Kick 尝试、重试和放弃，放弃按 `target_lease=live|dead` 打标签（告警只用 `live`）*(R2-O6)*；吊销导致的 Kick；Revoke `WAIT` 失败 *(R2-O4)*；
- Reconcile：刷新、恢复、`unchanged` *(R3-OP7)*、`refused` 及重新入队、达到重新入队上限 *(R3-O6)*、按原因分类的 `not_owner`、规则 3 覆盖（块中条目更新）*(R2-O4)* 的条目数；`ReconcileEnd` 删除数、`tracked_conn_ids` 大小、重试与丢弃 *(R3-O1, R3-OP4)*；被取消的轮次；按节奏分类的周期耗时 *(R2-O8)*；
- 清扫器进度；`sweep_aborted` *(R3-O3)*；**大规模过期保护闸拦截**（同时告警），包括 `renewing_since` 等待所占用的时间 *(R3-O2)*；
- JWKS 刷新失败和因未知 `kid` 触发的刷新；使用 `jwks-fallback` 的 Pod（同时告警）*(R2-O7)*；
- **空闲后重连率**（反映短于 150 秒的 NAT 超时）；Go GC 停顿和堆大小。

日志：带 `uid` 和 `conn_id` 的 `slog` 行；通过 gRPC metadata 传递请求 ID。

## 验证

- **单元测试：**
  - 帧编解码（不完整帧、一次到达多帧、模糊测试），针对测试客户端的 Noise 握手；
  - 连接状态机：`AUTH_PENDING` 中的 kick-pending、两种超时、三个条件齐全的 Logout 栅栏、accept 时的连接上限（计 socket，不计等待 Logout 的条目），以及在 Logout 被确认或丢弃之前一直属于跟踪集合；
  - Reconcile 轮次控制：`refused` 条目最多重新入队 3 次，不再是 ONLINE 时跳过；被取消的轮次不再发送块，也不发送 `ReconcileEnd` *(R3-O6, R3-O1)*；
  - conn_id 生成，以及 **`-race` 下的 gnet loop 归属测试**（对同一连接，`OnOpen`、`OnClose` 和 `Execute` 回调都看到同一个 loop）；
  - 清扫器保护闸：15 秒内没有其他 epoch 续期时 `renewing_since` 重置，带抖动的 10 秒节奏不会重置它 *(R3-O2)*；
  - **SDK** *(R3-O5)*：握手前被关闭时 1–3 秒后重试，最多连续 5 次，之后回退到指数退避；登录成功后计数清零。
- **Lua 测试**（在 Docker 中运行真实 Redis）：
  - 登录表每一行一个表驱动测试，包括 `SUPERSEDED`、`EXPIRED`、`STALE`（窗口内较小 conn_seq 被拒）和回退那一行（窗口外较小 conn_seq 被接受）；
  - `REVOKED`（token iat 早于标记时被拒）；吊销标记写入为 `max(stored, new)` 且幂等；生效标记是 `*` 和槽位字段的最大值，槽位标记只阻止该槽位的登录 *(R3-OP2)*；
  - logout 的 compare-and-delete；
  - Reconcile 的 compare-and-set（包括 `sent_ms + login_deadline` 之后的拒绝，以及比较失败时不写入）和 `ReconcileEnd` 删除（只删本 epoch 的字段、conn_id 未被跟踪、`login_ms < as_of_ms`）*(R3-O1)*；
  - 模拟写入丢失后的 Reconcile 规则 3：在更新的连接存活时手工恢复一个旧字段；Reconcile 保留更新的连接并踢掉旧连接 *(R2-O4)*；
  - 计数器回退情况下的 Reconcile 规则 3 *(R3-O4)*：一个计数器回退的会话（较小 conn_seq，登录时间比已存的晚超过 `login_deadline`），手工把它的写入回滚到旧的高 conn_seq 条目；Reconcile 保留回退后的会话并踢掉旧的。两台设备 `login_ms` 相同：已存会话胜出，不发生互踢；
  - 用 `SET ... GET` 续期租约：key 不存在 → `missing`，key 存在 → `live` *(R3-O3)*；
  - 清扫器只删除死亡 epoch 的字段。
- **竞态集成测试**（`deploy/compose`：Redis、Logic、**4 台网关**和一个脚本化客户端）*(R3-O7)*：
  - 上文第 1–6、10、14、16 项竞态；第 6 项按两种顺序测试（Logout 先于 Login，Login 先于 Logout，通过注入 Logic 延迟）：栅栏之后没有幽灵；因 Logic 崩溃而没有明确回复的 Login：Logout 等待栅栏；
  - 第 14 项 *(R2-O1)*：连接在其块发送之后、Logic 处理之前关闭（注入 Logic 延迟）；Logout 之后没有残留幽灵；
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
  - Reconcile：幽灵条目在一个周期内被移除；缺失的条目被恢复；另一台网关上更新的会话永远不会被覆盖；被丢弃的 Logout 在一个周期内修复；
  - Kick：发往存活网关的 Kick 被丢弃后会重试，之后被下一轮 Reconcile 兜住 *(R2-O6)*；`AUTH_PENDING` 期间的 Kick 在 Login 回复返回时关闭连接；正在下线的网关仍能收到 Kick；
  - Revoke 关闭两个槽位上的连接；Kick 被丢弃的 Revoke 被下一轮 Reconcile 兜住；`WAIT` 失败的 Revoke 返回错误，重试是幂等的 *(R2-O4)*；
  - 槽位级 Revoke *(R3-OP2)*：Kick 被丢弃时，下一轮 Reconcile 通过 `not_owner`（`REVOKED`）关闭连接，且不恢复会话；同一用户的其他槽位不受影响；`WAIT` 失败的槽位级 Revoke 返回错误且不发送 Kick，重试是幂等的；
  - 密钥轮换：重叠期间两个 `kid` 都被接受；
  - JWKS *(R2-O7)*：账号服务宕机时 Logic 启动，从 `jwks-fallback` ConfigMap 变为 Ready 并接受 token；两个来源都没有密钥集的 Logic Pod 永远不会 Ready。
- **Redis 故障切换测试** *(R2-O5)*：对存放租约 key 的分片做故障切换并测量耗时；在配置的 TTL 下租约必须存活。
- **压测**（`cmd/loadgen`，多源 IP；loadgen 记录每个客户端的发现时间和登录成功时间，直接计算 SLO 分布）：
  - (a) 单台网关 60 万连接（上限），配合 GOMEMLIMIT/GOGC 调优；
  - (b1) SIGKILL 一台 20 万连接的网关：99% 在死亡后 30 秒内重新登录；发往它的 Kick 尝试约每次重新登录一次，不重试 *(R2-O6)*；
  - (b2) 对一台 20 万连接的网关执行 iptables DROP（静默节点丢失）：99% 在客户端发现后 30 秒内重新登录；
  - (b-stress) 杀掉一台 60 万连接的网关：只测量，无 SLO；
  - (c) 150 秒心跳在 20 万和 60 万时的 CPU 开销；
  - Logic 固定在 `minReplicas`（不做 HPA 扩容）时的风暴：满足 SLO *(R2-O7)*；
  - 总量 100 万时滚动重启所有网关：没有网关超过上限，在上限处被拒的客户端在 SLO 内登录到别处（使用握手前 1–3 秒重试）*(R2-O2, R3-O5)*；
  - 风暴：其他网关从不错过租约续期；
  - 握手洪峰：来自大量源的垃圾握手不影响心跳按时应答，合法的每秒 2 万风暴不会被令牌桶拒绝；
  - 慢速客户端洪峰：超时释放占位，合法客户端不会收到 `Busy`；
  - Logic 变慢：登录队列上限触发 `Busy`，客户端在 SLO 内重试成功。

## 不在范围内（后续子项目）

消息（seq、去重、MongoDB）· 推送链路（RocketMQ、APNs/FCM）· 社交动态 · Web 扫码登录 · 客户端自适应心跳 · 账号服务（它的事件只通过 `Revoke` 和 JWKS 到达我们）· 发送路径的所有权强制（子项目 2）· 会话有效性读取路径及其在故障期间租约被判无效时的缓解（子项目 2/3）· 网关间主动连接再均衡（子项目 5）· 可用区丢失的容量规划 · proxy protocol 与真实客户端 IP（Web 阶段）· 以 device_id/user_id 为键的防滥用限流。

## 待定问题

rev3 的待定问题 4（连接上限计什么）、5（Reconcile 轮次重叠）和 6（截止时间拒绝哪些 Reconcile 写入）已由第三轮评审确认，并已写入正文（*准入控制*、*Reconcile*、*Reconcile 处理*）。rev4 的待定问题 2（槽位级 Revoke）已由槽位级吊销标记解决，待定问题 4–7 已确认并写入正文（rev3 决议中的 *R3-OP2*、*R3-OP4*–*R3-OP7*）。

仍保留、尚无评审决议：

1. **Reconcile 与租约检查的原子性。** R1 决议说 Reconcile Lua 在"已存 `gw|epoch` 没有存活租约"时写入，但租约 key 位于另一个 Redis slot。本版在 Logic 中检查存活性，并让 Lua 对观察到的字段值做 compare-and-set（见 *Reconcile 处理*）。
2. **登录 Lua 中 `REVOKED` 与 `EXPIRED` 的顺序：** 先检查吊销，依据是 R1-O10 所说的"先读取 `rev:{uid}`"。结果只在于客户端看到哪个错误码。
