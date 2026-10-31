# Polymarket GTD Bot — 部署指南

## 环境要求

- Python 3.11+
- Docker + Docker Compose（或 Coolify）
- Polymarket 账户 + pUSD 余额
- CLOB L2 API Key

---

## 一、API Key 配置

### 方式 A：直接使用已有 L2 API Key（推荐）

在 [.env.docker](.env.docker) 或项目根目录的 `.env` 中填入（变量名必须与代码一致）：

```bash
API_KEY=xxxxx
API_SECRET=xxxxx
API_PASSPHRASE=xxxxx
```

> 如果使用 Gnosis Safe（SIGNATURE_TYPE=2），还需确保 Safe 已审批 pUSD 给 CLOB Exchange，并设置 `FUNDER_ADDRESS=<Safe 地址>`。

### 方式 B：从私钥派生

```bash
WALLET_PRIVATE_KEY=0xYOUR_PRIVATE_KEY
WALLET_ADDRESS=0xYOUR_EOA_ADDRESS
FUNDER_ADDRESS=0x...        # SIG_TYPE=1/2/3 时必填（代理/Safe 地址）
SIGNATURE_TYPE=0            # 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE
```

bot 启动时会自动调用 `create_or_derive_clob_api_creds` 从私钥派生 L2 API Key。

### Gnosis Safe 注意事项

- `SIGNATURE_TYPE=2`
- pUSD 审批需通过 Safe Transaction 执行（多签）
- 确保 CLOB Exchange V2 (`0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`) 已被授权转移你的 pUSD

---

## 二、pUSD 余额

Polymarket CLOB 使用 pUSD（不是原生 USDC）作为抵押品：

1. 将 USDC 充值到 Polymarket 获取 pUSD
2. 或通过 Collateral Onramp 直接桥接
3. 建议余额：至少 $50（覆盖最多 4 层马丁 $16 + 手续费缓冲）

---

## 三、配置参数

```bash
# 马丁格尔仓位（美元名义）— 四层，总最大暴露 2+3+5+6 = 16 USD
TIER1_NOTIONAL=2.0   # 首单
TIER2_NOTIONAL=3.0   # 补仓1（下跌 20%）
TIER3_NOTIONAL=5.0   # 补仓2（下跌 40%）
TIER4_NOTIONAL=6.0   # 补仓3（下跌 60%）

# 策略阈值
ENTRY_PRICE_CAP=0.40     # T1 入场价格上限
TIER2_DROP_PCT=0.20      # T2 触发：T1 成交均价 × (1-20%)
TIER3_DROP_PCT=0.40      # T3 触发：T1 成交均价 × (1-40%)
TIER4_DROP_PCT=0.60      # T4 触发：T1 成交均价 × (1-60%)
TP_PROFIT_PCT=0.20       # 止盈目标：成本 + 20%
TP_DECREMENT_PCT=0.03    # 每补一档 -3% (N=1→17%, N=2→14%, N=3→11%)
TP_MIN_PCT=0.05          # 止盈下限 5%
BASELINE_DRIFT_LIMIT=0.0015  # BTC 现货偏移上限 0.15%

# T4 入场闸门（T4 是最大的一层，避免在趋势中越补越深）
T4_MIN_REMAINING_S=240   # 剩余 <240s 不补 T4；0=关闭
T4_STRIKE_GATE_ENABLED=true  # 只允许 BTC 仍接近开盘价时补 T4
T4_MAX_STRIKE_Z=2.0      # 不利方向 z 分数上限；越小越保守
T4_STRIKE_SIGMA_PCT_PER_MIN=0.05  # 60s 样本不足时的 1 分钟波动率下限
T4_MAX_GAP_BELOW_TRIGGER_PCT=0.05  # mid 已跌破触发价 >5% 时不挂单；0=关闭

# 订单参数
GTD_LIFETIME_S=300       # GTD 有效期（CLOB 硬下限 180，客户端会钳制到 >=181）
SELL_SLIPPAGE_PCT=0.02   # FAK 卖出滑点 2%
T1_ENTRY_WINDOW_S=300    # T1 入场时间窗口（窗口开启后 5 分钟内）
STARTUP_RECONCILE_CANCEL_ALL=true  # 启动时取消账户全部挂单；共享钱包请设为 false
```

---

## 四、本地运行

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 复制并编辑环境变量（项目根目录的 .env 会被自动读取）
cp .env.example .env
# 编辑 .env 填入 API Key / 策略参数

# 3. 启动（必须先设置 .env）
python -m src.main
# 数据库默认写入 ./data/polymarket.db（可用 POLYMARKET_DB_PATH 覆盖）
```

---

## 五、Docker 部署

`.env.docker` 只放**非敏感配置**（已随仓库提供，并会被打进镜像）；密钥通过
compose 的 `${VAR}` 插值注入，不要把密钥写进 `.env.docker`。

```bash
# 1. 准备密钥（.env 已被 .gitignore 忽略，也已被 .dockerignore 排除）
cp .env.example .env
# 编辑 .env：只填 API_KEY / API_SECRET / API_PASSPHRASE
# （或 WALLET_PRIVATE_KEY / SIGNATURE_TYPE / FUNDER_ADDRESS）

# 2. 构建并启动（compose 会自动读取 .env 做变量插值）
docker compose up -d --build

# 3. 查看日志
docker compose logs -f
```

---

## 六、Coolify 部署

### 1. 创建 Application 并授权仓库

- 仓库：`https://github.com/zhibite/polygtd`，分支：`main`
- 仓库是**私有**的，必须让 Coolify 有读取权限：
  - **推荐**：Coolify → Sources → Add → GitHub App → 安装并授权该仓库；
  - 或：使用只读 Deploy Key + SSH 仓库地址（见常见问题）。
- 构建方式：**Dockerfile**（推荐）。也支持 Docker Compose，此时
  `docker-compose.yml` 的 `env_file: .env.docker` 会生效。

### 2. 配置环境变量

**只有密钥是必须在这里设置的**（写入 Coolify → Environment Variables）：

```
API_KEY=xxxxx
API_SECRET=xxxxx
API_PASSPHRASE=xxxxx
```

私钥方式则改为：

```
WALLET_PRIVATE_KEY=0x...
SIGNATURE_TYPE=0
FUNDER_ADDRESS=0x...        # SIG_TYPE=1/2 时必填
```

其它非敏感配置已写在仓库的 `.env.docker` 中，并会随镜像一起加载。
如需覆盖某项策略参数，可以在 Coolify 面板再添加同名变量（进程环境变量
优先级最高）。

> 不要把密钥写进 `.env.docker`：它会被提交到 git，并被打进镜像。

### 3. 配置持久化

挂载 `/app/data` 目录保存数据库：

```
volumes:
  - polymarket_data:/app/data
```

### 4. 运行检查

- 内置 Web 看板（FastAPI），监听容器端口 **8010**
- 健康检查端点：`GET /api/health`（无需鉴权，可用于 Coolify HTTP 探针）
- 看板页面：`/`（控制台）、`/trades`（交易记录）、`/config`（策略配置）
- 启动命令：`python -m src.main`
- 日志存活判断：`docker logs` 中每 5 秒输出一行 `STATUS ...`
- 启动日志应出现 `web.started port=8010 auth=True/False`

### 5. Web 看板配置

| 变量 | 默认 | 说明 |
|------|------|------|
| `WEB_ENABLED` | `true` | 设为 `false` 关闭 HTTP 服务 |
| `WEB_HOST` | `0.0.0.0` | 监听地址 |
| `WEB_PORT` | `8010` | 监听端口（需与 Coolify 域名映射的端口一致） |
| `WEB_USERNAME` | `admin` | 登录用户名 |
| `WEB_PASSWORD` | 空 | 登录密码；设置后看板所有页面/API 都需要登录 |
| `WEB_SESSION_TTL_S` | `604800` | 登录会话有效期（秒），默认 7 天 |

登录说明：

- 访问 `/login` 输入用户名密码，成功后写入 `HttpOnly` + `SameSite=Lax`
  的会话 Cookie（HTTPS 下自动加 `Secure`）
- 会话保存在进程内存里，**重启/重新部署后需要重新登录**
- 连续输错 10 次密码，该 IP 会被锁定 5 分钟
- **`WEB_PASSWORD` 为空时**：只读看板保持公开（兼容旧行为），但
  `/config` 和 `/api/config` 永远要求登录；没有密码就登录不了，页面会提示
  去 Coolify 配置

`WEB_PASSWORD` 属于密钥，请放在 Coolify 面板的环境变量里，不要写进
`.env.docker`。看板数据来源：

- `/api/status`：进程内存中的实时状态（当前轮次、BTC 基准/漂移、持仓、分层、挂单）
- `/api/trades`、`/api/rounds`、`/api/fills`：`/app/data/polymarket.db` 中的
  `rounds` / `orders` / `fills` 表（由 `OrderManager` 实时写入）

Coolify 中将 `polygtd.bquant.com` 指向本服务的 **8010** 端口即可；
若仍出现 502，请检查 `docker logs` 是否有 `web.started`。

### 6. 策略配置页

`/config` 可以在线修改常用策略参数（入场价、分层金额/跌幅、止盈、风控、GTD 等），
保存后写入 `/app/data/runtime_config.json`（持久化卷），重启和重新部署后仍然生效。

页面顶部显示当前钱包状态（`GET /api/wallet`，15 秒缓存）：

- 可用余额（USDC/pUSD）与授权额度（`get_balance_allowance`）
- 钱包地址、签名类型（EOA / Proxy / Safe）
- 当前持仓代币数量（仅在持仓期间显示）
- 余额低于单轮最大名义金额（四层之和）时高亮提醒

| 标记 | 生效时机 |
|------|----------|
| 立即生效 | 下一次决策 tick 读取（入场价、预挂幅度、漂移上限、T1 时间窗、止盈参数、GTD 有效期等） |
| 下一轮生效 | 下个 15 分钟窗口开始时读取（分层金额、跨窗跳空阈值、晚启动宽限期） |
| 重启后生效 | 只在进程启动后的第一个窗口判定（晚启动跳过开关） |

- 「恢复默认」删除覆盖文件，回到 `.env` / `.env.docker` / 代码默认值
- 参数校验在前端和后端都做（范围 + 交叉校验：TP 下限 ≤ 止盈、T2<T3<T4 跌幅）
- **配置页必须登录**：即使 `WEB_PASSWORD` 为空，`/config` 和 `/api/config`
  也要求登录会话（无密码时无法登录，页面会提示先配置 `WEB_PASSWORD`）
- 登录入口：`/login`；导航栏右上角会显示当前用户和「退出」

### 7. 紧急停止 / 恢复运行

控制台顶部有「紧急停止」按钮（必须登录，公开看板下按钮会置灰）：

- 立即冻结开仓 / 补仓 / 止盈：`StrategyEngine._evaluate` 直接返回
- 撤销所有未成交挂单，并停止 TierWatcher
- `OrderManager` 在所有 `place_*` 入口再次拦截，停止瞬间已在途的动作不会下单
- **不会自动卖出已有持仓**——紧急停止只停止新订单，平仓由人工决定
- 状态写入 `/app/data/emergency_stop.json`，**重启/重新部署后仍保持停止**，
  必须从看板点「恢复运行」才恢复交易
- 日志：`killswitch.engaged` / `killswitch.released`，STATUS 行会变成
  `STATUS [STOPPED] ...`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/control` | 当前停止状态 |
| POST | `/api/control/stop` | 停止 + 撤销挂单 |
| POST | `/api/control/start` | 恢复运行 |

---

## 七、日志与监控

### 查看实时日志

```bash
# Docker
docker compose logs -f

# 本地
tail -f polymarket.log
```

### 日志关键词

| 关键词 | 含义 |
|--------|------|
| `engine.trigger_t1` | 触发 T1 首单 |
| `engine.t1_gate_closed` | T1 入场时间窗口已关闭（窗口开启超过 5 分钟） |
| `engine.trigger_tN` | 触发 T2/T3/T4 补仓 |
| `engine.trigger_tp` | 触发止盈 |
| `engine.tier_filled` | 某层订单成交 |
| `engine.tier_expired` | GTD 过期未成交 |
| `order_manager.placed` | 订单已挂出 |
| `order_manager.sell_filled` | 卖出已成交 |
| `position.fill_recorded` | 部分成交记录 |
| `gtd_expiry.detected` | GTD 过期检测到 |

### 状态打印（每 5 秒）

```
STATUS  binance=67350  drift=0.0001 [PASS] | next_in=180s
UP(last=0.35 bid=0.35)  DOWN(last=0.65 bid=0.65)
| tiers=[T1=✓ T2=○ T3=○ T4=○] qty=10.53 cost=$2.00
```

### T4 决策日志（JSONL）

T4 入场/出场的关键字段除了打印到 stdout，还会追加写入：

```
/app/data/t4_journal.jsonl        # 可用 T4_JOURNAL_PATH 覆盖；T4_JOURNAL_ENABLED=false 关闭
```

每行一个 JSON，包含 `event`、`timestamp`、`ts_ms`、`round_start_ts`、
`t4_z`、`t4_remaining_s`、`t4_sigma_1m_pct`、`mid`、`tier` 等字段。
覆盖的事件：

| 事件 | 内容 |
|------|------|
| `engine.trigger_tN`（`tier=4`） | T4 补仓决策，带 z / 剩余时间 / σ |
| `engine.tN_skip_t4_*` | T4 被时间 / 行权价距离 / gap 闸门拦下 |
| `engine.tier_filled`（`tier=4`） | T4 成交 |
| `engine.tp_*` / `position.tp_*` | 止盈触发与成交（出场） |
| `engine.*stop_loss*` | 止损触发与成交（出场） |
| `order_manager.fill_record`（T4/TP/SL） | 成交明细（价格、数量、手续费、maker/taker） |
| `round.resolved` | 回合最终 UP/DOWN 结果，可按 `round_start_ts` join |

导出与分析：

```bash
docker cp <container>:/app/data/t4_journal.jsonl ./
# 或
docker exec <container> cat /app/data/t4_journal.jsonl > t4_journal.jsonl
```

```python
import pandas as pd
df = pd.read_json("t4_journal.jsonl", lines=True)
t4 = df[df["event"] == "engine.trigger_tN"]
```

**持久化**：`/app/data` 目前没有挂载卷，Coolify 重新部署会连同 DB 一起清空。
要跨部署长期积累，请在 Coolify 的 Persistent Storage 里给应用加一个挂载
路径 `/app/data`（或用 bind mount 指到宿主机）。启动日志里的
`t4_journal.started persistent=False` 和 `t4_journal.ephemeral` 会提示这一点。

---

## 八、故障排查

### 0. Coolify 拉取仓库失败（exit code 128）

报错形如：

```
fatal: could not read Username for 'https://github.com'
```

说明仓库是私有的，而 Coolify 没有凭据。任选其一：
- Coolify → Sources → GitHub App，授权 `zhibite/polygtd`，然后在
  Application 的 Source 设置里切换为该 GitHub App；
- 或在 GitHub 仓库 Settings → Deploy keys 添加只读公钥，把仓库地址改成
  `git@github.com:zhibite/polygtd.git` 并在 Coolify 填入对应私钥。

### 1. CLOB 订单一直不成交

- 检查价格是否在有效范围（`0.01 ~ 0.99`）
- 检查 GTD 是否过期（`engine.tier_expired` 日志）
- 检查 pUSD 余额是否充足

### 2. 钱包余额充足但订单被拒

- **常见原因**：pUSD 未审批给 CLOB Exchange
  ```bash
  # 检查 pUSD 余额
  # Polygon RPC: https://polygon-bor.publicnode.com
  # pUSD 地址: 0xC011a7E12a19f7B1f670d46F03B3342E82DFB
  ```
- EOA 模式：检查钱包私钥正确
- Safe 模式：确认多签已执行 pUSD 授权交易

### 3. BTC 偏移超限导致 Gatekeeper 失效

- 检查 `BASELINE_DRIFT_LIMIT` 设置（当前 0.15%）
- 如果 BTC 波动剧烈，考虑放宽阈值

### 4. 重复下单同一层

- 检查 `engine.tier_filled` 日志
- 如果同一层出现多次 `fill_recorded`，可能是部分成交逻辑有问题

### 5. 仓位超出预期

- 检查 `stake_cap_remaining` 日志
- 确认 `total_max_notional = TIER1+TIER2+TIER3+TIER4`

---

## 九、安全注意事项

1. **不要将私钥提交到 Git** — 使用环境变量或 Coolify Secrets
2. **生产环境使用 SIGNATURE_TYPE=2（Safe）** — 多签保护
3. **设置合理的 `total_max_notional`** — 建议不超过账户余额的 20%
4. **监控日志** — `engine.tier_expired` 频繁出现说明策略有问题
5. **定期检查 pUSD 余额** — 马丁格尔可能快速消耗保证金

---

## 十、快速验证流程

```bash
# 1. 检查 API Key 是否有效
curl -X GET "https://clob.polymarket.com/balance" \
  -H "Poly-Address: $POLY_WALLET_ADDRESS" \
  -H "Poly-Signature: $SIGNATURE"

# 2. 检查 Polymarket 市场
curl "https://gamma-api.polymarket.com/events?slug=btc-updown-15m-$(date +%s -d '15 min' | xargs -I{} echo {})"

# 3. 启动 bot 观察日志
python -m src.main
```

---

## 十一、Coolify 部署检查清单

- [ ] 代码仓库已配置
- [ ] Dockerfile 构建成功
- [ ] 环境变量已填入 API Key
- [ ] `GAMMA_API`、`POLYMARKET_HOST`、`POLYGON_RPC_URL` 已配置
- [ ] 马丁格尔参数已调优
- [ ] 端口 8010 已暴露，`/api/health` 返回 200，看板 `/` 可访问
- [ ] 已设置 `WEB_PASSWORD`（公网域名必做）
- [ ] 数据目录 `/app/data` 已持久化（数据库路径 `/app/data/polymarket.db`）
- [ ] 日志正常输出（无 ERROR）
- [ ] 看到 `engine.trigger_t1` 或等待 baseline 的日志
