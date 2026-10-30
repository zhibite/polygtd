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

- 本服务**没有 HTTP 服务/健康检查端点**，不要配置 HTTP 探针
- 容器端口：8010（仅用于与外部系统对齐，程序本身不监听端口）
- 启动命令：`python -m src.main`
- 存活判断：`docker logs` 中每 5 秒输出一行 `STATUS ...`

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
- [ ] 端口 8010 已暴露（无 HTTP 服务，仅供编排系统占位）
- [ ] 数据目录 `/app/data` 已持久化（数据库路径 `/app/data/polymarket.db`）
- [ ] 日志正常输出（无 ERROR）
- [ ] 看到 `engine.trigger_t1` 或等待 baseline 的日志
