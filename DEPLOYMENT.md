# Polymarket GTD Bot — 部署指南

## 环境要求

- Python 3.11+
- Docker + Docker Compose（或 Coolify）
- Polymarket 账户 + pUSD 余额
- CLOB L2 API Key

---

## 一、API Key 配置

### 方式 A：直接使用已有 L2 API Key（推荐）

在 [.env.docker](.env.docker) 或 `.env` 中填入：

```bash
POLY_API_KEY=xxxxx
POLY_API_SECRET=xxxxx
POLY_API_PASSPHRASE=xxxxx
```

> 如果使用 Gnosis Safe（SIGNATURE_TYPE=2），还需确保 Safe 已审批 pUSD 给 CLOB Exchange。

### 方式 B：从私钥派生

```bash
POLY_PRIVATE_KEY=0xYOUR_PRIVATE_KEY
POLY_WALLET_ADDRESS=0xYOUR_EOA_ADDRESS
SIGNATURE_TYPE=0   # 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE
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
# 马丁格尔仓位（美元名义）
TIER1_NOTIONAL=2.0   # 首单
TIER2_NOTIONAL=4.0   # 补仓1
TIER3_NOTIONAL=8.0   # 补仓2
# 总最大暴露 = 14 USD (H-NEW-2026-07-11-4: 3-tier cap, removed T4)

# 策略阈值
# H-NEW-2026-07-12: entry cap 0.36 → 0.38, TP decrement 3% → 5%.
ENTRY_PRICE_CAP=0.38     # 入场价格上限
TIER2_DROP_PCT=0.30      # T2 触发：价格跌到基准 × 70%
TIER3_DROP_PCT=0.60      # T3 触发：价格跌到基准 × 40%
TP_PROFIT_PCT=0.20       # 止盈目标：成本 + 20%
TP_DECREMENT_PCT=0.05    # 止盈递减：每补一档 -5% (N=1→15%, N=2→10%)
BASELINE_DRIFT_LIMIT=0.0015  # BTC 现货偏移上限 0.15%

# 订单参数
GTD_LIFETIME_S=180       # GTD 有效期（必须 >= 180）
SELL_SLIPPAGE_PCT=0.02   # 卖出滑点 2%
T1_ENTRY_WINDOW_S=300    # T1 入场时间窗口（窗口开启后 5 分钟内）
```

---

## 四、本地运行

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 复制并编辑环境变量
cp .env.example .env
# 编辑 .env 填入 API Key

# 3. 启动（必须先设置 .env）
python -m src.main
```

---

## 五、Docker 部署

```bash
# 1. 构建
docker build -t polymarket-gtd .

# 2. 运行（编辑 .env.docker 填入凭证）
docker compose --env-file .env.docker up -d

# 3. 查看日志
docker compose logs -f
```

---

## 六、Coolify 部署

Coolify 原生支持 Docker Compose。部署步骤：

### 1. 上传代码

```bash
# 在 Coolify 中创建新 Application
# 仓库地址：https://github.com/YOUR_USER/polymarket-gtd
# 构建方式：Dockerfile
```

### 2. 配置环境变量

在 Coolify 的 Environment Variables 中添加：

```
POLY_API_KEY=xxxxx
POLY_API_SECRET=xxxxx
POLY_API_PASSPHRASE=xxxxx
GAMMA_API=https://gamma-api.polymarket.com
POLYMARKET_HOST=https://clob.polymarket.com
POLYGON_RPC_URL=https://polygon-bor.publicnode.com
TIER1_NOTIONAL=2.0
TIER2_NOTIONAL=4.0
TIER3_NOTIONAL=8.0
# H-NEW-2026-07-12: entry cap 0.38, TP decrement 0.05.
ENTRY_PRICE_CAP=0.38
TIER2_DROP_PCT=0.30
TIER3_DROP_PCT=0.60
TP_PROFIT_PCT=0.20
TP_DECREMENT_PCT=0.05
BASELINE_DRIFT_LIMIT=0.0015
T1_ENTRY_WINDOW_S=300
GTD_LIFETIME_S=180
SELL_SLIPPAGE_PCT=0.02
```

### 3. 配置持久化

挂载 `/app/data` 目录保存数据库：

```
volumes:
  - polymarket_data:/app/data
```

### 4. 健康检查

- 端口：8004（内部）
- HTTP 路径：`/` 或 `/health`
- 启动命令：`python -m src.main`

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
- [ ] 端口 8004 已暴露
- [ ] 数据目录 `/app/data` 已持久化
- [ ] 日志正常输出（无 ERROR）
- [ ] 看到 `engine.trigger_t1` 或等待 baseline 的日志
