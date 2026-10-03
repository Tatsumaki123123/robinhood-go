# Robinhood Chain Uniswap V4 线路配置说明

本文说明 RobinhoodGo 当前 execute 模块的线路配置和实际执行行为。配置只面向 Robinhood Chain 上的 EVM 钱包与 Uniswap V4，不包含其他链的账户、路由、手续费或钱包字段。

完整接口参数和响应格式请参阅：[EXECUTE_API.md](./EXECUTE_API.md)。

相关实现：

- 线路和交易接口：[internal/api/execute.go](../internal/api/execute.go)
- execute 持久化：[internal/store/execute.go](../internal/store/execute.go)
- V4 calldata、签名和交易：[internal/chain/trading.go](../internal/chain/trading.go)、[internal/chain/uniswap.go](../internal/chain/uniswap.go)
- 数据库表：[migrations/001_init.sql](../migrations/001_init.sql)

## 1. 执行模型

execute 是独立资金域，不依赖 `monitorUser`：

```text
ExecuteLine(line)
        |
        | 1:N
        v
ExecuteBatch(eid, boss)
        |
        +--> ExecuteWallet(index, address, encrypted private key)
        |
        +--> ExecuteToken(tid, V4 PoolKey, buy/sell route)
```

字段关系：

| 对象 | 作用 |
| --- | --- |
| `line` | 可复用的线路配置，保存 wallet 数量、充值金额、买入金额和提现地址 |
| `eid` | 一次执行批次；批次切换时会创建新的 boss |
| `boss` | 线路独立资金主钱包，负责给执行钱包充值和回收余额 |
| `wallet` | 执行实际买卖的 EVM 钱包，私钥加密保存 |
| `tid` | 批次内一个目标 token + Pool 的执行任务 |

线路只需要一个正整数 `line` 标识，不需要 `userId`。

## 2. 线路配置接口

execute 接口统一使用以下前缀：

```text
/api/v1/executerobin/*
```

### 2.1 创建或更新线路

```http
POST /api/v1/executerobin/updateLineData
```

请求体：

```json
{
  "line": 1001,
  "name": "v4-main-line",
  "data": {
    "walletConfig": [
      {
        "transferAmount": 0.01,
        "firstBuy": {
          "enable": true,
          "buyAmount": 0.002
        },
        "firstSell": {
          "enable": true,
          "sellRatio": 1
        }
      },
      {
        "transferAmount": 0.01,
        "firstBuy": {
          "enable": true,
          "buyAmount": 0.003
        },
        "firstSell": {
          "enable": true,
          "sellRatio": 1
        }
      }
    ],
    "withdrawAddress": "0x1111111111111111111111111111111111111111",
    "autoSwap": false
  }
}
```

`data` 可以传对象，也可以传 JSON 字符串。顶层字段 `walletConfig`、`withdrawAddress`、`autoSwap`、`name`、`sourceWeb`、`groupSort`、`minFollowStates`、`maxBuyTax`、`lineBots`、`enabled` 也会合并到线路配置中。

新建线路默认使用 AVE，默认 `walletConfig` 为一个钱包（`buyAmount=0.001`、`transferAmount=0.01`），并设置 `minFollowStates=2`、`maxBuyTax=1.5`、空的 `lineBots`。默认 `groupSort` 为 `sort_field=created_at`、`sort_order=desc`、`mcp_min=10000`、`mcp_max=50000`、`create_day=1`、`create_day_end=0`、`category=pump_out_new`、`duration=6`、空的 `address`。

如果配置 `sourceWeb: "ave"`，`POST /api/v1/executerobin/tokenList` 会按 `groupSort.category`、`sort_field`、`sort_order`、`mcp_min`、`mcp_max`、`create_day`、`create_day_end`、可选的 `holder_min` 和分页字段从 Robinhood AVE 获取 Uniswap V4 候选池，并排除当前批次已买入或已卖出的 token；未配置时只返回当前批次已保存的 execute token。

### 2.2 导入已有 boss

创建线路时服务会自动生成并保存 boss。 如果不希望使用自动生成的 boss，可以在更新线路时传入已有私钥：

```json
{
  "line": 1001,
  "bossPrivateKey": "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
```

服务会校验私钥并推导 `bossAddress`。私钥不会写入线路 JSON，也不会在接口响应中返回，只会使用 `PRIVATE_KEY_ENCRYPTION_KEY` 加密保存。旧线路如果没有 boss，下一次更新线路时会自动补齐。

## 3. boss 和批次生命周期

### 3.1 启动批次

```http
POST /api/v1/executerobin/start
```

```json
{"line": 1001}
```

行为：

1. 线路不存在或 `enabled=false` 时拒绝启动。
2. 创建线路时会先创建一条 active boss 批次；后续启动会先检查当前批次的 active 执行钱包余额，余额清零后生成新的 boss，并将旧 boss 的可用余额归集到新 boss。历史上没有 boss 批次的线路会在启动时补齐。
3. 如果线路已有活动 `eid`，会检查该批次的 active 执行钱包余额。
4. 仍有超过估算转账 gas 成本的原生币余额时，拒绝切换并要求先调用 `end`。
5. 没有可转出的余额后创建新的 boss 和新的 `eid`，并尝试把旧 boss 的可用余额归集到新 boss。

返回值包含 `eid`、`line`、`bossAddress`、`active` 和时间字段，不包含私钥。

### 3.2 生成执行钱包

```http
POST /api/v1/executerobin/generateWallets
```

```json
{"line": 1001}
```

钱包数量计算规则：

```text
wallets = max(len(config.walletConfig), 1)
```

`walletCount` 不再作为线路配置字段保存或读取。

每个新钱包生成独立 EVM 私钥并加密保存。`transferAmount` 是 JSON number，单位为 ETH；boss 会在生成后按钱包下标向该钱包充值。再次调用时，已有 active 钱包余额低于 `transferAmount` 会补足差额，余额达到或超过该金额时不转账。

### 3.3 结束和提现

```http
POST /api/v1/executerobin/end
{"eid": 1}
```

`end` 会逐个执行钱包归集可转出的原生币，扣除转账 gas 后发送到当前批次 boss。转账 gas limit 由 RPC 估算，最低按 30000 预留；余额不超过该 gas 成本的零头会保留在钱包中，并通过响应的 `dust` 数组返回，不影响接口成功。

```http
POST /api/v1/executerobin/withdraw
```

```json
{
  "eid": 1,
  "withdrawAddress": "0x1111111111111111111111111111111111111111",
  "amountRaw": "1000000000000000"
}
```

省略 `withdrawAddress` 时使用线路配置中的 `withdrawAddress`；可以传 `amount`（JSON number，单位 ETH）或 `amountRaw`；两者都省略时转出 boss 扣除 gas 后的可用余额。

## 4. `walletConfig` 字段

`walletConfig` 数组下标就是执行钱包的 `walletIndex`，不要在批次执行期间改变数组顺序。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `transferAmount` | number | 给该钱包充值的 ETH 数量，例如 `0.01` |
| `firstBuy` | object | `type=first` 的买入配置；必须设置 `enable: true` 才参加该阶段 |
| `secondBuy` | object | `type=second` 的买入配置；必须设置 `enable: true` 才参加该阶段 |
| `thirdBuy` | object | `type=third` 的买入配置；必须设置 `enable: true` 才参加该阶段 |
| `multiBuy` | object | `type=multi` 的买入配置；必须设置 `enable: true` 才参加该阶段 |
| `firstSell` | object | `type=first` 的卖出配置；必须设置 `enable: true` 才参加该阶段 |
| `secondSell` | object | `type=second` 的卖出配置；必须设置 `enable: true` 才参加该阶段 |
| `thirdSell` | object | `type=third` 的卖出配置；必须设置 `enable: true` 才参加该阶段 |
| `multiSell` | object | `type=multi` 的卖出配置；必须设置 `enable: true` 才参加该阶段 |

每个阶段对象中的 `buyAmount` 必须是 JSON number，单位为 ETH：

```json
{
  "firstBuy": {
    "enable": true,
    "buyAmount": 0.002
  }
}
```

调用 `buyToken` 时，服务只选择当前阶段 `enable=true` 的钱包。同一阶段配置了多个钱包时，这些钱包会在一次请求中并发提交买入交易；未配置该阶段的钱包不会参与。

阶段卖出配置使用 `sellRatio`：`0 < sellRatio <= 1` 表示比例，`1 < sellRatio <= 100` 表示百分比。例如：

```json
{
  "firstSell": {
    "enable": true,
    "sellRatio": 1
  }
}
```

传递 `type=first` 时只卖出配置了 `firstSell.enable=true` 的钱包；传递 `type=all` 时忽略阶段卖出配置，对所有 active 钱包按 100% 卖出。

## 5. 金额和单位

线路配置中的 `transferAmount` 和各阶段的 `buyAmount` 都必须是 JSON number，单位为 ETH，支持小数：

```json
{
  "transferAmount": 0.01,
  "firstBuy": {
    "enable": true,
    "buyAmount": 0.002
  }
}
```

这些配置字段不使用字符串，不使用 `Raw` 后缀。接口内部的 `amountInRaw`、`amountOutMinimumRaw` 和提现 `amountRaw` 仍是链上最小单位字段：

```text
1 native coin = 10^18 raw units
```

例如 `transferAmount: 0.01` 表示 `0.01 ETH`，而不是 `0.01 wei`。

## 6. Uniswap V4 PoolKey 配置

`checkToken` 可以直接提交完整 PoolKey，不需要依赖线路或监控用户数据：

```http
POST /api/v1/executerobin/checkToken
```

默认会拒绝其他 eid 已处于 `buy` 状态的同一 token；确需重新检查时可传 `forceCheck: true`。

```json
{
  "eid": 1,
  "tokenAddress": "0x2222222222222222222222222222222222222222",
  "quoteTokenAddress": "0x0000000000000000000000000000000000000000",
  "currency0": "0x0000000000000000000000000000000000000000",
  "currency1": "0x2222222222222222222222222222222222222222",
  "fee": 500,
  "tickSpacing": 10,
  "hooks": "0x3333333333333333333333333333333333333333",
  "poolId": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `tokenAddress` | 目标 ERC-20 token 地址 |
| `quoteTokenAddress` | 目标池报价币；原生币使用全零地址 |
| `currency0/currency1` | PoolKey 两个币种，必须按地址升序排列 |
| `fee` | V4 pool fee，整数形式，例如 `500` |
| `tickSpacing` | V4 tick spacing，整数形式，例如 `10` |
| `hooks` | V4 hooks 合约地址 |
| `poolId` | 可选；服务会使用 PoolKey 重新计算并校验 |
| `routeBuy` | 可选；显式买入路线，最多 3 跳 |
| `routeSell` | 可选；显式卖出路线，最多 3 跳 |

服务会校验：

- `tokenAddress` 和 `quoteTokenAddress` 必须属于 PoolKey；
- 地址必须是有效 EVM 地址；
- `poolId` 必须与 PoolKey 匹配；
- 每个 route hop 的 PoolKey、`poolId` 和前后币种必须连接；
- route hop 数量必须在 1 到 3 之间。

如果没有提交完整 PoolKey，服务会尝试从 AVE 的 V4 pair 数据推导 PoolKey。生产环境建议直接保存完整 PoolKey，减少对外部行情服务的依赖。

原生币可以使用：

```text
0x0000000000000000000000000000000000000000
```

服务也会把 AVE 常见的 native sentinel `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee` 归一化为全零地址。

## 7. 显式 route 格式

`routeBuy` 和 `routeSell` 是有序 hop 数组。每个 hop 使用以下字段：

```json
{
  "poolId": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "currency0": "0x0000000000000000000000000000000000000000",
  "currency1": "0x2222222222222222222222222222222222222222",
  "fee": 500,
  "tickSpacing": 10,
  "hooks": "0x3333333333333333333333333333333333333333",
  "tokenIn": "0x0000000000000000000000000000000000000000",
  "tokenOut": "0x2222222222222222222222222222222222222222",
  "hookData": "0x"
}
```

买入 route 的第一跳从原生币或 WETH 开始，最后一跳必须到目标 token。卖出 route 通常是买入 route 的反向顺序，最后一跳到原生币或 WETH。若不传 route，服务会为直接报价币、WETH、USDG 和报价币组合尝试自动发现 1--3 跳路线。

## 8. 买入接口

```http
POST /api/v1/executerobin/buyToken
```

请求金额优先级：

```text
amountInRaw -> ethAmount -> walletConfig[index].<type>Buy.buyAmount
```

正常使用时只传 `tid` 和 `type`，买入金额从对应钱包的阶段配置读取。`ethAmount` 是可选的 JSON number ETH 覆盖值；`amountInRaw` 和 `amountOutMinimumRaw` 仍使用链上最小单位。

示例：

```json
{
  "tid": 1,
  "type": "first",
  "amountInRaw": "2000000000000000",
  "amountOutMinimumRaw": "1000000000000000000"
}
```

可选 `walletAddress` 只执行一个钱包。`type` 可以是字符串，也可以是数组；数组会按传入顺序逐阶段执行：

```json
{
  "tid": 1,
  "type": ["first", "multi", "second"]
}
```

买入会通过 Universal Router 执行 V4 swap，原生币输入会自动设置交易 value；WETH 路线会自动处理 wrap。

## 9. 卖出接口

```http
POST /api/v1/executerobin/sellToken
```

按比例卖出：

```json
{
  "tid": 1,
  "type": "all",
  "percent": 100
}
```

规则：

- `amountInRaw` 已传入时，直接使用指定 token 数量；
- 未传 `amountInRaw` 时，读取每个钱包的 token 余额，再按 `percent` 计算；
- `percent` 默认 `100`，范围为 `1` 到 `100`；
- 卖出前会为目标 ERC-20 准备 Permit2/Router 授权；
- 卖出路径必须从当前任务代币开始，并以原生 ETH 或 WETH 结束；
- `type=all` 成功后，`ExecuteToken.status` 更新为 `sell`；其他阶段只更新交易记录和阶段状态。

`amountOutMinimumRaw` 是可选的链上最小单位，表示每个执行钱包各自的最低到账额。省略或传 `0` 时不限制最低到账额。当前服务没有内置 V4 流动性报价器；如需价格保护，调用方应根据卖出前的外部报价和可接受滑点传入正数。

## 10. 完整推荐配置

```json
{
  "line": 1001,
  "name": "robinhood-v4-two-wallets",
  "enabled": true,
  "data": {
    "walletConfig": [
      {
        "transferAmount": 0.01,
        "firstBuy": {
          "enable": true,
          "buyAmount": 0.002
        },
        "firstSell": {
          "enable": true,
          "sellRatio": 1
        }
      },
      {
        "transferAmount": 0.01,
        "firstBuy": {
          "enable": true,
          "buyAmount": 0.003
        },
        "firstSell": {
          "enable": true,
          "sellRatio": 1
        }
      }
    ],
    "withdrawAddress": "0x1111111111111111111111111111111111111111",
    "autoSwap": false
  }
}
```

推荐调用顺序：

```text
1. updateLineData
2. start
3. generateWallets
4. checkToken
5. buyToken
6. sellToken
7. end
8. withdraw
```

## 11. 自动开关

当前自动开关 `autoSwap` 只保存线路配置，不会启动一个独立的自动交易调度器；需要自动执行时，应由外部任务按上述接口顺序调用。

## 12. 运行前检查

execute 运行至少需要配置：

```text
RPC_HTTP_URL
DATABASE_URL
PRIVATE_KEY_ENCRYPTION_KEY
CHAIN_ID=4663
UNISWAP_V4_POOL_MANAGER
UNISWAP_V4_UNIVERSAL_ROUTER_ADDRESS
PERMIT2_ADDRESS
```

`RPC_HTTP_SEND_URL` 可以单独用于 nonce、gas 和交易提交。`TRADING_FIXED_GAS_PRICE_WEI` 配置后，交易会优先使用固定 gas price。实际启用线路前，应确认 boss 有足够原生币、每个执行钱包有足够交易 gas；如需卖出价格保护，应为每笔 V4 交易设置合理的 `amountOutMinimumRaw`。
