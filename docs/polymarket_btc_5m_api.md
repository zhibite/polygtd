# Polymarket BTC Up/Down 价格数据采集（5分钟 & 15分钟）

> 支持 5 分钟和 15 分钟两种周期的 BTC/ETH Up/Down 二元预测市场。

## 数据来源

`future.news` API，通过 slug 从事件详情接口获取 token ID，再调用 K 线接口拉取秒级概率价格数据。数据格式与 BTC 市场完全一致。

## 一、事件详情接口（获取 Token ID）

通过 slug 查询事件信息，返回 `assetIds` 即为 UP/DOWN 的 token ID。

```
GET https://future.news/api/v1/event/polymarket/<SLUG>
```

### 请求头

```http
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36
Accept: application/json, text/plain, */*
Referer: https://future.news/event/<SLUG>
Origin: https://future.news
```

### Slug 格式

```
<ASSET>-updown-<PERIOD>-<START_EPOCH>
```

| 参数 | 说明 |
|------|------|
| `btc` / `eth` | 标的资产 |
| `updown` | Up/Down 市场 |
| `5m` 或 `15m` | 周期（分钟） |
| `<START_EPOCH>` | 该周期开始的 Unix 时间戳（秒） |

**示例：**
- `btc-updown-5m-1781131200` → BTC 5分钟，1781131200 秒开始
- `btc-updown-15m-1781134200` → BTC 15分钟，1781134200 秒开始
- `eth-updown-5m-1781131200` → ETH 5分钟，1781131200 秒开始
- `eth-updown-15m-1781131500` → ETH 15分钟（注意：15m 的 START_EPOCH 与同资产的 5m 不同，是接在 5m 结束后才开始）

### 响应示例

```json
{
  "code": 0,
  "data": {
    "slug": "btc-updown-5m-1781131200",
    "title": "Bitcoin Up or Down - June 10, 7:20PM-7:25PM ET",
    "endAt": 1781131500000,
    "volume": "28543.00",
    "status": "closed",
    "recurrence": "5m",
    "liveStartAt": 1781131200000,
    "marketInfo": [
      {
        "conditionId": "0xabc123...",
        "assetIds": [
          "<UP_TOKEN_ID>",
          "<DOWN_TOKEN_ID>"
        ],
        "outcomes": ["Up", "Down"],
        "resolvedAssetId": "<RESOLVED_TOKEN_ID>"
      }
    ],
    "seriesSlug": "btc-up-or-down-5m"
  }
}
```

**关键字段：**
- `marketInfo[].assetIds[0]` → UP Token ID
- `marketInfo[].assetIds[1]` → DOWN Token ID
- `marketInfo[].conditionId` → Polymarket 条件 ID（可用于官方 API）
- `marketInfo[].resolvedAssetId` → 结算方向（空字符串 = 未结算）

---

## 二、K 线接口（获取价格数据）

```
GET https://future.news/api/v1/token_kline/polymarket/<TOKEN_ID>?period=<PERIOD>
```

**注意：Token ID 和 period 必须匹配同一个市场，混用会返回空数据或错位数据。**

## 请求头（必需）

```http
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36
Accept: application/json, text/plain, */*
Referer: https://future.news/event/<SLUG>
Origin: https://future.news
```

## Token ID 对应关系

> Token ID 和 period 必须与 slug 匹配，混用会返回空数据或错位数据。

| 市场 | Slug | Token ID | 方向 |
|------|------|----------|------|
| BTC Up | `btc-updown-5m-1781131200` | `90135847675965642935072613122319692157829298927280507703323672779402330183073` | Up |
| BTC Down | `btc-updown-5m-1781131200` | `64559118937970596343376630736376171310028458155125566713201692421776955688450` | Down |
| BTC Up | `btc-updown-15m-1781134200` | `46741924611738407529671344188535500293783328690570275635942299049291965266080` | Up |
| BTC Down | `btc-updown-15m-1781134200` | `18445370705262595590564049767740436226447894086107363221515128870379462107418` | Down |
| ETH Up | `eth-updown-5m-1781131200` | `38754650506360235714412317441263856492793963023159983104003837969400228057271` | Up |
| ETH Down | `eth-updown-5m-1781131200` | `6021180289991336230105201327297303174613558749141880993541110747482936780942` | Down |
| ETH Up | `eth-updown-15m-1781131500` | `72797984730795867339002226924280146386968079459615698438351837427575319124419` | Up |
| ETH Down | `eth-updown-15m-1781131500` | `76382900628119918256426737905994598375654815734503509557143656346604413300192` | Down |

## 完整 URL 示例

```
# BTC 5m UP
https://future.news/api/v1/token_kline/polymarket/90135847675965642935072613122319692157829298927280507703323672779402330183073?period=5m

# BTC 5m DOWN
https://future.news/api/v1/token_kline/polymarket/64559118937970596343376630736376171310028458155125566713201692421776955688450?period=5m

# BTC 15m UP
https://future.news/api/v1/token_kline/polymarket/46741924611738407529671344188535500293783328690570275635942299049291965266080?period=15m

# BTC 15m DOWN
https://future.news/api/v1/token_kline/polymarket/18445370705262595590564049767740436226447894086107363221515128870379462107418?period=15m

# ETH 5m UP
https://future.news/api/v1/token_kline/polymarket/38754650506360235714412317441263856492793963023159983104003837969400228057271?period=5m

# ETH 5m DOWN
https://future.news/api/v1/token_kline/polymarket/6021180289991336230105201327297303174613558749141880993541110747482936780942?period=5m

# ETH 15m UP
https://future.news/api/v1/token_kline/polymarket/72797984730795867339002226924280146386968079459615698438351837427575319124419?period=15m

# ETH 15m DOWN
https://future.news/api/v1/token_kline/polymarket/76382900628119918256426737905994598375654815734503509557143656346604413300192?period=15m
```

## 参数说明

| 参数 | 是否必需 | 说明 |
|------|---------|------|
| `period=5m` / `period=15m` | **是** | K线周期，必须与 Token ID 所属市场匹配 |
| `device_id` | 否 | 设备UUID，格式 `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`，可随意生成 |
| `client_id` | 否 | `future_web_` + 日期版本号 |
| `app_ver` | 否 | 同上 |
| `fp_did` | 否 | Fingerprint ID，可填 `unknown` |
| `from_app` | 否 | `future-web` |
| `tz_name` | 否 | 时区，`Asia/Shanghai` |
| `tz_offset` | 否 | 时区偏移秒，`28800`（+8小时） |
| `app_lang` | 否 | 语言，`en-US` |
| `os` | 否 | 平台，`web` |
| `worker` | 否 | `0` |

**结论：只有 `period` 是必需的（且必须与 Token ID 匹配），其他所有参数均可省略。**

## 响应数据格式

```json
{
  "code": 0,
  "reason": "",
  "message": "success",
  "data": {
    "list": [
      {
        "time": 1781131218000,
        "price": "0.595000",
        "volume": "75.193798"
      },
      {
        "time": 1781131219000,
        "price": "0.585000",
        "volume": "5.999998"
      }
    ]
  }
}
```

## 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `time` | int64 | 时间戳（毫秒，13位），UTC+8 时区 |
| `price` | string | 概率价格，范围 0.0 ~ 1.0 |
| `volume` | string | 该秒成交量（对应仓位数） |

## 价格含义（Polymarket 二元市场机制）

| 价格 | 含义 |
|------|------|
| `price = 1.0` | 事件已发生，结果确定 |
| `price = 0.0` | 事件未发生，结果确定 |
| `0 < price < 1` | 事件进行中，市场当前判断的发生概率 |

## Event ID（Slug）结构

URL 中的 Event ID 格式为 `btc-updown-<PERIOD>-<START_EPOCH>`：
- `btc-updown` = BTC Up/Down 市场
- `5m` / `15m` = 市场周期
- `<START_EPOCH>` = 该周期开始的 Unix 时间戳（秒）

例如：`btc-updown-15m-1781134200` 表示从 `1781134200`（秒）开始的 15 分钟周期。

## 数据特征

- **频率**：秒级（每条记录间隔 1 秒）
- **5m 数量**：约 300 条/周期
- **15m 数量**：约 900 条/周期
- **互补性**：UP Token 价格 + DOWN Token 价格 = 1.0（始终成立）
- **结果归一**：事件结束后，胜出方向归 1.0，失败方向归 0.0
- **Volume 差异**：15m 市场有实际成交量数据，5m 市场 volume 通常为 0

## 测试代码

### 通过 Slug 获取 Token ID

```python
import urllib.request
import json

slug = "eth-updown-15m-1781131500"  # btc 或 eth，5m 或 15m

url = f"https://future.news/api/v1/event/polymarket/{slug}"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": f"https://future.news/event/{slug}",
    "Origin": "https://future.news",
}

req = urllib.request.Request(url, headers=headers)
resp = urllib.request.urlopen(req, timeout=10)
data = json.loads(resp.read().decode())

market_info = data["data"]["marketInfo"][0]
up_token   = market_info["assetIds"][0]
down_token = market_info["assetIds"][1]
condition_id = market_info["conditionId"]
print(f"UP: {up_token}")
print(f"DOWN: {down_token}")
print(f"ConditionID: {condition_id}")
```

### 拉取 K 线数据

```python
import urllib.request
import json

# 15m 示例
up_token = "46741924611738407529671344188535500293783328690570275635942299049291965266080"
period = "15m"  # 或 "5m"（必须与 Token ID 对应周期一致）

url = f"https://future.news/api/v1/token_kline/polymarket/{up_token}?period={period}"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://future.news/event/btc-updown-15m-1781134200",
    "Origin": "https://future.news",
}

req = urllib.request.Request(url, headers=headers)
resp = urllib.request.urlopen(req, timeout=10)
data = json.loads(resp.read().decode())
for item in data["data"]["list"]:
    print(item["time"], item["price"], item["volume"])
```

## 附：condition_id（官方 API 用）

> 以下为已验证的 condition_id（16进制格式，0x 前缀）。完整的 condition_id 需从事件详情接口获取。

- **BTC 5m**: 从事件接口获取 `marketInfo[].conditionId`
- **BTC 15m**: `0x3eac14ab18ce6d0de8b564e6f794f2ff0d27f03f4ad6b721bd4ea59c80887fb1`
- **ETH 5m**: `0x4c5cd5c6fdf65c3e69c51165a8b71d871ded3e007c2c329fc530e3a42184eb98`
- **ETH 15m**: 从事件接口获取 `marketInfo[].conditionId`
