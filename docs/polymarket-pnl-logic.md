# Polymarket UP/DOWN Token PnL 逻辑

## Token 机制

Polymarket 的每个预测市场有两种 outcome token：**UP** 和 **DOWN**（对应 YES/NO）。

- 持有 **UP token**，若市场最终 UP 胜出 → 每个 token 赎回 **$1.00**
- 持有 **DOWN token**，若市场最终 DOWN 胜出 → 每个 token 赎回 **$1.00**
- 无论哪种 token，输的一方价值归零（$0.00）

## 持仓期间的实时价值

在市场**未结算**期间，token 可以在 CLOB（订单簿）上随时买卖，实时价格反映市场当前认为的胜率：

| 场景 | 买入价（entry） | 当前市场价（current） | 持仓价值变化 |
|------|----------------|----------------------|-------------|
| 持有 UP token | $0.40 | $0.60 | $0.40 → $0.60，**盈利 +50%** |
| 持有 UP token | $0.60 | $0.40 | $0.60 → $0.40，**亏损 -33.3%** |
| 持有 DOWN token | $0.40 | $0.60 | $0.40 → $0.60，**盈利 +50%** |
| 持有 DOWN token | $0.60 | $0.40 | $0.60 → $0.40，**亏损 -33.3%** |

> **关键点**：DOWN token 和 UP token 的价值都等于各自的**当前市场价格**。两者都和"预测的方向"无关，涨的是价格，不是预测。

## 为什么不是"预测方向"？

容易混淆的地方：

```
错误理解：DOWN 方向，价格跌了 → 预测正确 → 盈利
```

这是错的。token 的盈亏和"预测对不对"无关，只和 token 的**当前市场价格**有关：

- 买入 DOWN @ $0.60，当前跌到 $0.40 → 你的资产从 0.60 跌到 0.40 → **亏损**
- 买入 DOWN @ $0.40，当前涨到 $0.60 → 你的资产从 0.40 涨到 0.60 → **盈利**

**Token 的价值 = 市场价格，不是"预测方向的概率"**。

## 最终结算 vs 持仓期

需要区分两个阶段：

### 持仓期（未结算）

PnL 按 **当前市场价格 vs 买入价** 计算：

```
PnL% = (current_price - entry_price) / entry_price * 100
```

无论 UP 还是 DOWN，公式完全相同。

### 结算期（已分出胜负）

- 胜方 token：每个赎回 $1.00
- 负方 token：每个归零 $0.00

```
UP 胜出：
  - 持有 UP token：exit = $1.00
  - 持有 DOWN token：exit = $0.00

DOWN 胜出：
  - 持有 UP token：exit = $0.00
  - 持有 DOWN token：exit = $1.00
```

## 代码实现

```python
def unrealized_pnl_pct(pos: OpenPosition, current_price: float) -> float:
    """持仓期 PnL：所有方向统一用 (current - entry) / entry"""
    if current_price <= 0 or pos.entry_price <= 0:
        return -100.0
    return (current_price - pos.entry_price) / pos.entry_price * 100


def closed_pnl(pos: OpenPosition, exit_price: float) -> float:
    """结算期 PnL：exit_price 由 settle_if_due 根据胜负结果确定（$1.00 或 $0.00）"""
    return (exit_price - pos.effective_entry_price) / pos.effective_entry_price * 100
```

## 常见误区

| 误区 | 正确理解 |
|------|---------|
| "DOWN 方向，价格跌了 = 盈利" | 错误。价格跌了 = 持仓价值缩水 = 亏损 |
| "UP token 和 DOWN token 盈亏方向相反" | 错误。两者都是市场价格多少，盈亏方向就多少 |
| "分方向用不同公式计算 PnL" | 不需要。统一公式对所有方向都正确 |
| "持有 DOWN token 等于做空市场概率" | 错误。DOWN token 不是做空，它是独立的 token，其价格就是它的价值 |

## 总结

Polymarket 的 UP/DOWN token 是两种**独立的资产**，每个都有独立的价格。

- **买入价**决定了你的成本
- **当前市场价**决定了你的持仓当前值多少
- **PnL = (当前值 - 成本) / 成本**，对 UP 和 DOWN 完全相同，不需要分方向处理
