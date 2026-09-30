# RobinhoodGo Execute 接口说明

本文总结 RobinhoodGo 当前 execute 模块的 HTTP 接口。接口只面向 Robinhood Chain EVM 钱包和 Uniswap V4，不依赖 `monitorUser`。

## 1. 基础约定

### 1.1 URL 前缀

所有 execute 接口统一使用以下前缀：

```text
/api/v1/executerobin/*
```

Token 列表接口也使用同一前缀：

```text
/api/v1/executerobin/tokenList
```

`initTokenList` 是 `tokenList` 的兼容别名：

```text
POST /api/v1/executerobin/initTokenList
```

请求使用 `Content-Type: application/json`。

### 1.2 成功响应

成功响应统一使用以下结构：

```json
{
  "success": true,
  "statusCode": 200,
  "message": "Success",
  "data": {},
  "meta": {
    "timestamp": "2026-09-26T00:00:00.000Z",
    "path": "/api/v1/executerobin/start",
    "version": "1.0"
  }
}
```

`data` 的具体类型根据接口不同，可以是对象、数组、布尔值或交易结果。

失败响应示例：

```json
{
  "statusCode": 400,
  "timestamp": "2026-09-26T00:00:00.000Z",
  "message": "there are no execute wallets enabled for firstBuy",
  "path": "/api/v1/executerobin/buyToken"
}
```

### 1.3 ID 和金额

| 字段 | 含义 |
| --- | --- |
| `line` | 可复用线路 ID，正整数 |
| `eid` | 一次执行批次 ID |
| `tid` | 批次内 Token 任务 ID |
| `walletAddress` | 执行钱包 EVM 地址 |
| `amount`、`transferAmount`、`buyAmount`、`ethAmount` | JSON number，单位 ETH |
| `amountRaw`、`amountInRaw`、`amountOutMinimumRaw` | 链上最小单位，通常使用十进制字符串 |
| `percent` | 卖出百分比，范围 `1` 到 `100` |
| `sellRatio` | 配置中的卖出比例；`0 < x <= 1` 表示比例，`1 < x <= 100` 表示百分比 |

线路配置中的金额必须使用 JSON number，例如 `0.002`，不要把 `buyAmount` 写成字符串，也不要把 `buyAmount` 放在 `walletConfig` 顶层。详细配置见 [EXECUTE_LINE_CONFIG.md](./EXECUTE_LINE_CONFIG.md)。

## 2. 推荐调用顺序

```text
updateLineData
    -> start
    -> generateWallets
    -> checkToken
    -> buyToken
    -> sellToken
    -> end
    -> withdraw
```

`getWallets`、`getBoss`、`getBuyTokes`、`getTokenAccounts` 等查询接口可以在流程中随时调用。

## 3. 线路和批次接口

### 3.1 创建或更新线路

```http
POST /api/v1/executerobin/updateLineData
```

请求：

```json
{
  "line": 1001,
  "name": "v4-main-line",
  "enabled": true,
  "data": {
    "walletConfig": [
      {
        "transferAmount": 0.01,
        "firstBuy": { "enable": true, "buyAmount": 0.002 },
        "firstSell": { "enable": true, "sellRatio": 1 }
      },
      {
        "transferAmount": 0.01,
        "firstBuy": { "enable": true, "buyAmount": 0.003 },
        "firstSell": { "enable": true, "sellRatio": 1 }
      }
    ],
    "withdrawAddress": "0x1111111111111111111111111111111111111111",
    "autoSwap": false
  }
}
```

说明：

- `data` 可以是对象，也可以是 JSON 字符串。
- `walletConfig`、`autoSwap`、`name` 等字段也可以直接放在请求顶层。修改 `withdrawAddress` 时必须同时传入 `password`，且密码必须匹配 `ROBINHOOD_MONITOR_PRIVATE_KEY_EXPORT_PASSWORD`；也可以使用专用的 `updateWithdrawAddress` 接口。
- 创建线路时会自动生成 boss；也可以传 `bossPrivateKey` 导入已有 boss。私钥会加密保存，不会出现在响应中。

返回 `line`、`name`、`config`、`bossAddress`、`enabled` 和时间字段。

### 3.2 新增线路

```http
POST /api/v1/executerobin/addLine
```

请求体可为空，也可以传入线路名称：

```json
{ "name": "Line 2" }
```

服务会自动分配 `lineId`，按默认线路配置生成第一条 active boss 批次，并返回 `line`、`eid` 和 `bossAddress`。默认配置使用 `sourceWeb: "ave"`、`minFollowStates: 2`、`maxBuyTax: 1.5` 和 `groupSort` 的新池筛选条件；boss 私钥只会加密保存。

### 3.3 查询线路

```http
POST /api/v1/executerobin/getLines
```

请求体可以为空。返回线路数组，线路列表不会返回 boss 私钥。

### 3.4 修改线路提现地址

```http
POST /api/v1/executerobin/updateWithdrawAddress
```

```json
{
  "lineId": 1001,
  "withdrawAddress": "0x1111111111111111111111111111111111111111",
  "password": "your-export-password"
}
```

`password` 必须匹配 `ROBINHOOD_MONITOR_PRIVATE_KEY_EXPORT_PASSWORD`。也可以使用兼容路径 `/api/v1/executerobin/updateLineWithdrawAddress`。

### 3.5 删除线路

```http
POST /api/v1/executerobin/deleteLine
```

```json
{
  "lineId": 1001,
  "password": "your-export-password"
}
```

密码必须匹配服务端配置的 `ROBINHOOD_MONITOR_PRIVATE_KEY_EXPORT_PASSWORD`。删除前会检查线路 boss 和当前活动批次的 active 执行钱包原生币余额；任一余额大于 0 时返回失败。删除成功后会级联删除该线路的批次、钱包和 token。

### 3.6 启动执行批次

```http
POST /api/v1/executerobin/start
```

```json
{ "line": 1001 }
```

创建线路时会先创建一条 active boss 批次。后续调用 `start` 时，会先检查当前批次的 active 执行钱包余额；没有可转出的余额（允许不超过转账 gas 成本的零头）后生成新的 boss，保留旧批次并将旧 boss 的可用余额转给新 boss。历史上没有 boss 批次的线路会在首次启动时补齐。返回：

```json
{
  "eid": 1,
  "line": 1001,
  "active": true,
  "bossAddress": "0x...",
  "walletsExist": false,
  "status": "active"
}
```

### 3.7 生成执行钱包

```http
POST /api/v1/executerobin/generateWallets
```

```json
{ "line": 1001 }
```

也可以传 `{ "eid": 1 }`。钱包数量为 `max(walletConfig.length, 1)`。每个钱包生成独立私钥并由 boss 按 `transferAmount` 充值。重复调用时，active 钱包余额低于配置的 `transferAmount` 会由 boss 补足差额；余额达到或超过该金额时不转账。实际转账的钱包会在返回的 `wallets` 条目中包含 `funding` 交易结果。

### 3.8 结束批次并回收钱包余额

```http
POST /api/v1/executerobin/end
```

```json
{ "eid": 1 }
```

或者传 `{ "line": 1001 }`。接口会把执行钱包可转出的原生币扣除转账 gas 后归集到 boss，返回 `eid`、`recycled` 交易结果和 `dust` 零头数组。原生币转账的 gas limit 由 RPC 估算，最低按 30000 预留。余额为 0 的钱包跳过；余额大于 0 但不超过该 gas limit 乘以 gas 价格的，视为零头并在 `dust` 中返回 `address`、`balanceRaw`、`gasCostRaw`（单位均为 wei），接口仍返回成功。转账后若仍有超过 gas 成本的余额，则返回错误。

### 3.9 boss 提现

```http
POST /api/v1/executerobin/withdraw
```

```json
{
  "eid": 1,
  "withdrawAddress": "0x1111111111111111111111111111111111111111",
  "amount": 0.01
}
```

- `withdrawAddress` 省略时使用线路配置中的地址。
- 传 `amount` 时单位为 ETH。
- 也可以传 `amountRaw`。
- 两者都省略时，转出 boss 扣除 gas 后的可用余额。

## 4. 钱包查询和替换接口

### 4.1 查询执行钱包

```http
POST /api/v1/executerobin/getWallets
POST /api/v1/executerobin/getWalletBalances
```

请求：

```json
{ "eid": 1, "all": false }
```

`getWalletBalances` 是兼容别名。默认只返回 active 钱包；传 `all: true` 时包含 inactive 历史钱包。每个钱包返回 `address`、`index`、`active`、`balanceRaw` 和 ETH 单位的数字型 `balance`，并按 `index` 合并线路 `walletConfig[index]` 中的 `transferAmount`、买入/卖出阶段配置（例如 `firstBuy`、`firstSell`），不会返回私钥。

### 4.2 查询 boss

```http
POST /api/v1/executerobin/getBoss
```

```json
{ "eid": 1 }
```

返回批次信息、`bossAddress`、`balanceRaw`、ETH 单位的 `balance` 和 `tokenCount`。

### 4.3 导出 boss 私钥

```http
POST /api/v1/executerobin/getBossPrivateKey
```

请求需要传入 `eid` 或 `line`，以及服务端配置的
`ROBINHOOD_MONITOR_PRIVATE_KEY_EXPORT_PASSWORD`：

```json
{
  "eid": 1,
  "password": "your-export-password"
}
```

成功返回当前 active batch 的 `eid`、`line`、`bossAddress` 和 `privateKey`。
兼容路径为 `/api/v1/executerobin/exportBossPrivateKey`。

### 4.4 替换执行钱包

```http
POST /api/v1/executerobin/nextWallet
```

替换全部 active 钱包：

```json
{ "eid": 1 }
```

只替换一个钱包：

```json
{
  "eid": 1,
  "walletAddress": "0x旧钱包地址"
}
```

新钱包保留旧钱包的 `walletIndex`。旧钱包标记为 inactive，剩余 ETH 会先转移到新钱包；历史钱包记录不会覆盖删除。

## 5. Token 接口

### 5.1 获取候选 Token 列表

```http
POST /api/v1/executerobin/tokenList
```

```json
{ "line": 1001 }
```

当线路配置 `sourceWeb: "ave"` 时，接口从 Robinhood AVE 查询并只保留 Uniswap V4 候选池；可以通过 `groupSort` 配置分类、市值和创建时间。未配置 AVE 时，返回当前批次已保存的 execute token。

### 5.2 检查并创建 Token 任务

```http
POST /api/v1/executerobin/checkToken
```

最小请求：

```json
{
  "eid": 1,
  "tokenAddress": "0x2222222222222222222222222222222222222222",
  "quoteTokenAddress": "0x0000000000000000000000000000000000000000",
  "currency0": "0x0000000000000000000000000000000000000000",
  "currency1": "0x2222222222222222222222222222222222222222",
  "fee": 500,
  "tickSpacing": 10,
  "hooks": "0x3333333333333333333333333333333333333333"
}
```

可选字段：`poolId`、`routeBuy`、`routeSell`、`tokenName`、`tokenSymbol`、`quoteTokenSymbol`、`forceCheck`。

服务会重新计算并校验 PoolId，检查 PoolKey 地址和 route hop 是否连接。没有显式 route 时，会尝试发现 native ETH 到目标 token 的 1--3 跳 V4 路由。线路配置中的 `maxBuyTax` 会校验 AVE 的 `total_buy_tax`，`minFollowStates` 会校验 AVE 关注聚合数据中的 `all`。`lineBots` 的 ERC-20 余额总和按 token 的 `decimals` 换算后必须小于 100；规则不满足时不会创建 token 任务。成功返回的 `data` 是 ExecuteToken，包含 `tid`、`eid`、`poolId`、`routeBuy`、`routeSell`、`status` 等字段。

默认情况下，如果其他 eid 已经在买入同一 token，会返回冲突错误；确需重新检查时传 `forceCheck: true`。

### 5.3 查询待买入 Token

```http
POST /api/v1/executerobin/getBuyTokes
```

按批次查询：

```json
{ "eid": 1 }
```

返回 `{ "list": [...], "total": 1 }`，只包含 `pending` 和 `buy` 状态。按任务查询：

```json
{ "tid": 1 }
```

此时直接返回单个 ExecuteToken。

### 5.4 查询钱包 Token 余额

```http
POST /api/v1/executerobin/getTokenAccounts
```

```json
{
  "eid": 1,
  "tokenAddress": "0x2222222222222222222222222222222222222222"
}
```

也可以使用 `token` 代替 `tokenAddress`。返回 `tokenAccounts` 和 `lineBotsAccounts` 两个数组。执行钱包条目包含 `address`、`tokenBalance`；线路机器人条目还包含配置中的 `name`。`tokenBalance` 是按 ERC-20 `decimals` 换算后的数字，余额为零时返回 `0`。

### 5.5 删除待处理 Token

```http
POST /api/v1/executerobin/deleteToken
```

```json
{ "tid": 1 }
```

只允许删除 `pending` 状态任务。删除或完成全部卖出后可以再次调用 `checkToken`；同一批次的已卖出任务会重置为 `pending`，并重新出现在 AVE 候选列表。

## 6. 买入和卖出接口

### 6.1 买入

```http
POST /api/v1/executerobin/buyToken
```

普通阶段买入：

```json
{
  "tid": 1,
  "type": "first",
  "amountOutMinimumRaw": "0"
}
```

金额优先级：

```text
amountInRaw -> ethAmount -> walletConfig[index].<stage>Buy.buyAmount
```

正常使用只传 `tid` 和 `type`，每个钱包从自己的 `firstBuy`、`secondBuy`、`thirdBuy` 或 `multiBuy` 配置读取 `buyAmount`。只有 `enable: true` 的钱包参加该阶段；同一阶段配置多个钱包时会并发买入。

只执行一个钱包：

```json
{
  "tid": 1,
  "type": "first",
  "walletAddress": "0x..."
}
```

多个阶段按顺序执行：

```json
{ "tid": 1, "type": ["first", "multi", "second"] }
```

`type: "all"` 按 `first -> multi -> second -> third` 顺序执行已启用阶段；没有钱包启用的阶段会跳过。

### 6.2 卖出

```http
POST /api/v1/executerobin/sellToken
```

按阶段配置卖出：

```json
{
  "tid": 1,
  "type": "first",
  "percent": 100,
  "amountOutMinimumRaw": "0"
}
```

规则：

- 传 `amountInRaw` 时，使用指定的 token 原始数量。
- 未传 `amountInRaw` 时，读取每个钱包的 ERC-20 余额，再按 `percent` 或阶段 `sellRatio` 计算卖出量。
- `type=first/second/third/multi` 只选择对应 `*Sell.enable=true` 的钱包。
- `type=all` 忽略阶段卖出配置，对所有 active 钱包按 100% 计算。
- `type` 可以是数组，服务按数组顺序逐阶段执行。
- `type=all` 成功后任务状态更新为 `sell`；所有 active 钱包的 token 余额均为零时也返回成功，`results` 为 `[]`。

卖出前服务会准备目标 ERC-20 的 Permit2/Router 授权。

## 7. 兼容接口

### 7.1 自动交易开关

```http
POST /api/v1/executerobin/autoSwap
```

```json
{ "line": 1001, "enabled": true }
```

也兼容旧客户端字段：

```json
{ "line": 1001, "status": true }
```

当前接口只保存线路配置中的 `autoSwap`，不会自动启动调度器。

### 7.2 closeAllAccounts

```http
POST /api/v1/executerobin/closeAllAccounts
```

EVM/ERC-20 没有 Solana token account 关闭流程，因此接口保留为兼容 no-op，返回：

```json
{ "closed": 0, "unsupported": "EVM ERC-20 balances are not token accounts" }
```

## 8. 安全和运行要求

- boss 和执行钱包私钥只以加密形式存储，接口不返回私钥。
- 交易由当前钱包签名，通过 Uniswap V4 Universal Router 提交。
- 卖出会自动处理 Permit2/Router 授权。
- 生产调用建议传入非零 `amountOutMinimumRaw`，由调用方根据报价和滑点计算。
- 运行 execute 至少需要配置数据库、RPC、`PRIVATE_KEY_ENCRYPTION_KEY`、Robinhood Chain ID、V4 Pool Manager、Universal Router 和 Permit2 地址。

相关文档：

- [EXECUTE_WORKFLOW.md](./EXECUTE_WORKFLOW.md)：完整流程和状态变化
- [EXECUTE_LINE_CONFIG.md](./EXECUTE_LINE_CONFIG.md)：线路配置字段和示例
