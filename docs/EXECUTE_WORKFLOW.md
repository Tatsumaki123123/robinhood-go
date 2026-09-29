# Uniswap V4 execute workflow

RobinhoodGo 的 execute 流程保留 pump-service 的调用顺序，但只实现 EVM/Uniswap V4。execute 是独立资金域，不依赖 `monitorUser`：line 自己保存 boss 钱包，批次钱包由 boss 充值并在结束时归集回 boss。

接口参数总览请参阅：[EXECUTE_API.md](./EXECUTE_API.md)。

线路配置字段和示例请参阅：[EXECUTE_LINE_CONFIG.md](./EXECUTE_LINE_CONFIG.md)。

```text
updateLineData -> start -> generateWallets -> checkToken -> buyToken
                                      -> sellToken -> closeAllAccounts -> end -> withdraw
```

`line` 是可复用配置，`eid` 是一次执行批次，`execute_wallets` 保存批次钱包，`tid` 是批次内的 V4 代币任务。钱包私钥使用 `PRIVATE_KEY_ENCRYPTION_KEY` 加密，所有 HTTP 响应都不会返回私钥。

## 接口

所有 execute 接口统一挂在 `/api/v1/executerobin/` 下。

| 接口 | 请求重点 | 作用 |
| --- | --- | --- |
| `POST /api/v1/executerobin/updateLineData` | `line,data` | 创建或更新独立线路；`data` 可为对象或 JSON 字符串，常用字段是 `walletConfig`、`withdrawAddress`。可选 `bossPrivateKey` 用于导入已有 boss |
| `POST /api/v1/executerobin/updateWithdrawAddress` | `lineId,withdrawAddress,password` | 修改线路提现地址；密码必须匹配私钥导出密码 |
| `POST /api/v1/executerobin/addLine` | `name`（可选） | 自动分配下一个 lineId，创建线路和第一条 active boss 批次 |
| `POST /api/v1/executerobin/deleteLine` | `lineId,password` | 删除线路；boss 或活动批次的 active 钱包仍有原生币余额时拒绝删除 |
| `POST /api/v1/executerobin/start` | `line` | 校验线路钱包并创建活动 `eid`；上一批执行钱包仍有余额时拒绝切换 |
| `POST /api/v1/executerobin/generateWallets` | `line` 或 `eid` | 按 `walletConfig` 生成执行钱包；`transferAmount` 为 JSON number，单位是 ETH，并从 boss 钱包充值 |
| `POST /api/v1/executerobin/checkToken` | `eid`、`tokenAddress`、PoolKey | 校验 V4 PoolKey，保存 1--3 跳买入/卖出路线并创建 `tid` |
| `POST /api/v1/executerobin/buyToken` | `tid`、`type` | 按 `walletConfig[index].firstBuy/secondBuy/thirdBuy/multiBuy` 的 `enable` 和 `buyAmount` 选择钱包；同一阶段的多个钱包并发调用 Universal Router，也支持 `walletAddress` |
| `POST /api/v1/executerobin/sellToken` | `tid`、`type`、`percent`/`amountInRaw` | 按 `firstSell/secondSell/thirdSell/multiSell` 的 `enable` 和 `sellRatio` 选择钱包并发卖出；`type=all` 对所有 active 钱包按 100% 卖出并将任务置为 `sell` |
| `POST /api/v1/executerobin/end` | `eid` 或 `line` | 将执行钱包剩余原生币扣除 gas 后归集到 boss 钱包 |
| `POST /api/v1/executerobin/withdraw` | `amount`/`amountRaw`、`withdrawAddress` | 从 boss 钱包提现；`amount` 为 ETH number，省略金额时转出可用余额 |
| `POST /api/v1/executerobin/getWallets` | `eid` 或 `line` | 查询 active 批次钱包及 ETH balance（传 `all:true` 可包含 inactive 钱包，不含私钥） |
| `POST /api/v1/executerobin/getWalletBalances` | `eid` 或 `line` | `getWallets` 的余额查询兼容别名 |
| `POST /api/v1/executerobin/getBoss` | `eid` 或 `line` | 查询 boss 地址、ETH balance 和批次 token 数量 |
| `POST /api/v1/executerobin/getBossPrivateKey` | `eid` 或 `line`、`password` | 使用私钥导出密码导出当前 active boss 私钥 |
| `POST /api/v1/executerobin/getLines` | 无 | 查询 execute 线路列表，不返回 boss 私钥 |
| `POST /api/v1/executerobin/getTokenAccounts` | `eid` 或 `line`、`tokenAddress` | 查询执行钱包的 ERC-20 token balance |
| `POST /api/v1/executerobin/deleteToken` | `tid` | 只删除 `pending` 状态的 execute token |
| `POST /api/v1/executerobin/getBuyTokes` | `eid`、`line` 或 `tid` | 按批次返回 `pending/buy` token 列表及数量，传 `tid` 时返回单个任务 |
| `POST /api/v1/executerobin/tokenList` | `eid` 或 `line` | `sourceWeb=ave` 时查询并过滤 Robinhood AVE 的 V4 候选池，否则查询批次代币任务 |
| `POST /api/v1/executerobin/nextWallet` | `eid`、可选 `walletAddress` | 替换一个或全部 active 执行钱包；先把旧钱包可用 ETH 转给新钱包，原索引保持不变 |
| `POST /api/v1/executerobin/closeAllAccounts` | 任意 | EVM ERC-20 不需要关闭账户，该接口返回明确 no-op |

## checkToken 的 PoolKey

可以直接提交 `poolId,quoteTokenAddress,currency0,currency1,fee,tickSpacing,hooks`。服务会重新计算 `PoolId` 并拒绝不匹配的输入。也可以只提交 `tokenAddress` 和可选 `poolId`，服务会参考 `startAveToken` 的 AVE V4 pair 查询和 PoolKey 推导逻辑完成检查。AVE 返回的 native sentinel `0xeeee...` 会归一化为链上 native address `0x000...000`。

如果没有显式提交路线，服务会尝试发现并保存：

- native ETH -> 目标池报价币 -> token；
- native ETH -> WETH -> USDG -> 报价币 -> token；
- native ETH -> WETH -> 报价币 -> token。

每次 `checkToken` 会保存 `routeBuy` 和反向 `routeSell`，买卖接口不会接受未通过 PoolKey 校验的 hop。

## 买卖示例

```json
{
  "line": 1,
  "tokenAddress": "0x...",
  "quoteTokenAddress": "0x0000000000000000000000000000000000000000",
  "currency0": "0x0000000000000000000000000000000000000000",
  "currency1": "0x...",
  "fee": 500,
  "tickSpacing": 10,
  "hooks": "0x..."
}
```

`checkToken` 返回 `tid` 后：

```json
{"tid": 1, "type": "first"}
```

请求 `/api/v1/executerobin/buyToken` 时，服务会读取每个钱包 `walletConfig[index].firstBuy.buyAmount`；同一 `type` 下的多个钱包会并发提交。`type` 也可以是数组，服务会按数组顺序逐阶段执行。卖出时使用 `/api/v1/executerobin/sellToken`，可传 `percent: 100` 或明确的 `amountInRaw`，同样支持阶段数组。实际交易仍由现有 V4 `Trading.Swap` 负责签名、Permit2 授权、广播和回执等待。

创建线路时会生成并保存第一条 active boss 批次；如果线路通过 `bossPrivateKey` 导入了已有 boss，第一条批次会使用该钱包。后续 `start` 会先检查当前批次的 active 执行钱包余额，余额清零后创建下一条 boss 批次，保留历史记录并尝试将旧 boss 的可用原生币归集到新 boss。boss 和执行钱包的私钥只以加密形式存储。
