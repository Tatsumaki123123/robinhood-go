# RobinhoodGo 功能说明与实现规格

## 目标

RobinhoodGo 是原 NestJS/Prisma Robinhood 链上交易服务的 Go 后端替代实现。它保留
`/api/v1` HTTP 合约、统一响应格式、监控用户/代币/路由模型和 Docker 部署，同时将
请求处理、JSON-RPC、WebSocket 订阅和广播路径改为 Go 原生并发模型，以降低 WSS 事件
到策略处理的延迟。服务没有前端依赖。

## 技术方案

- **HTTP**：Fiber v2（fasthttp），默认 3000 端口，CORS/错误响应可在反向代理统一配置。
- **链连接**：HTTP JSON-RPC 用于查询和交易回执，gorilla/websocket 用于
  `eth_subscribe` 日志订阅；订阅断线自动重连。WSS 读协程先写入有序 ingress 队列，
  再交给策略 channel，避免数据库或 RPC 延迟阻塞 websocket；队列溢出计数可从
  `/health` 的 `wssDroppedEvents` 读取。
- **数据库**：PostgreSQL 16。`migrations/001_init.sql` 创建 AVE 配置、监控用户、监控
  代币、AVE 池快照、预验证路由、交易记录、待确认买单、文件和日志表。
- **缓存/锁**：Redis 连接已内置，策略进程可使用同一 Redis 部署扩展分布式锁和行情缓存。
- **链工具**：go-ethereum 用于地址/私钥、Universal Router/Permit2 ABI 编码和交易签名；私钥采用 AES-GCM 加密保存。
- **文件**：本地卷驱动已实现，S3/Wasabi/Azure 可在同一 `StorageDriver` 接口上替换。

### 高频路径优化

- WSS 使用有界 ingress/strategy 队列；读取循环不等待数据库或策略执行。队列溢出、WSS
  连接、消息数和 RPC HTTP 延迟通过 `/health` 暴露。
- Curve 与 Uniswap V4 事件在进入 token supply 查询和完整策略前进行本地 USD 名义价值
  快速过滤。默认只丢弃可可靠计算且小于 10 USD 的外部事件；己方成交回执先完成识别
  并始终进入记账。价格或 decimals 缺失、无效或过期时保守放行，native quote 缺失
  decimals 时按 18 位处理。
- WSS 消息 buffer、RPC 请求 buffer 和事件 word 解析均避免不必要的长期分配；HTTP RPC
  使用共享 Keep-Alive Transport、HTTP/2、多路连接上限和禁用压缩。
- 同一钱包的交易 nonce 在进程内按钱包串行分配；显式 nonce 用于替换交易，广播失败时
  会使本地 nonce cursor 失效并重新从 RPC 同步；nonce 已分配但广播前的 RPC、签名
  失败也会失效 cursor。
- WSS 断线或有界队列溢出后，Pons 曲线和 Uniswap V4 PoolManager 均由区块日志轮询补偿，
  使用交易 hash 与 log index 做幂等处理。

## API 合约

所有业务成功响应都是 `{success,statusCode,message,data,meta}`，资源错误返回
`{statusCode,timestamp,message,path}`。地址在写入和返回前统一小写，金额字段按字符串传输。

### 链与监听

`GET /api/v1/chain/network`、`/block/latest`、`/balance/:address`、
`/transactions/:hash` 提供网络、区块、余额和交易读取；
`/chain/pons-v2/listener` 返回两个监听开关和订阅数。启动时根据
`PONS_V2_DEPLOYMENT_LISTENER_ENABLED` 与 `UNISWAP_V4_SWAP_LISTENER_ENABLED` 建立 WSS
日志订阅，`/ws` 会向连接推送 `chain_event`。

### AVE 与监控用户

保留 `ave/getConfig`、`ave/updateConfig`，AVE 请求固定携带 `x-auth`；
`monitorUser` 全部八个接口均可用。新增用户省略 `userId` 时自动分配，自动生成钱包并
用 `PRIVATE_KEY_ENCRYPTION_KEY` 加密私钥；导出和删除都必须匹配
`ROBINHOOD_MONITOR_PRIVATE_KEY_EXPORT_PASSWORD`。`config` 支持旧字段以及新分组字段：
`tokenConfig`、`sellPolicy`、`scheduledSell`、`externalBuySell`、`profitSell`、
`lossSell`、`replacePending`、`listConfig`。AVE 持仓请求会按钱包地址转发。

### 监控代币与路由

`getList/getDisabledList/addToken/deleteToken/enableToken/disableToken/syncTokens` 覆盖
代币生命周期；代币保存完整 Pons V2 曲线和 Uniswap V4 PoolKey 定位字段。`getAveList`
与 `startAveToken` 用 AVE 列表/详情创建候选池。路由接口保存 1--3 跳有序 PoolKey JSON。

`startAveToken` 会在链上验证 Pons V2 `getLaunchedToken` 与目标 PoolKey，然后自动发现并持久化 native ETH 到目标池报价币的 V4 路由：native/WETH 直连、native→WETH→USDG、native→WETH→USDG→报价币，以及 native→WETH→报价币。每条买入路由同时保存反向卖出路由；路由不存在或校验失败时，代币会被禁用并返回错误。路由和代币接口支持启用列表与禁用列表。

### 交易

Pons V2 的 `quote/buy/sell` 与 Uniswap V4 的 `quote/buy/sell/route/quote/route/swap`
均已注册。V4 单跳和 1--3 跳 route 会生成 Universal Router `execute` calldata，支持
wrap/unwrap ETH、custom recipient 和 Permit2/ERC-20 allowance；交易会
签名、广播并等待 receipt。策略日志不会记录私钥；生产环境应由内部策略调用而不是把
私钥暴露给公网。

### 文件

`/files` 提供分页列表、详情、软删除、孤儿清理、单文件和多文件上传；静态文件映射到
`LOCAL_STORAGE_PATH`，默认 `/uploads`。

## 策略处理约定

策略配置支持 `minMcp`、`slippage`、`diffBuySecond`、亏损加仓限制、`replacePending`、
`tokenConfig`、`sellPolicy`、`scheduledSell`、`externalBuySell`、`profitSell`、`lossSell`
和 `listConfig`。事件使用 `sourceEventKey` 去重，并在同一用户/代币/曲线锁内串行
处理。`strategy_positions` 持久化剩余仓位、加权平均成本、首笔/上一笔买价、买入次数、
成功卖出次数、止盈档位、外部买单冷却和下一次定时卖出时间；`strategy_events` 防止 WSS
重连造成重复决策。外部买单需要同时满足金额、价格冲击和盈利条件，`buyAmountRatio`
按当前价格换算代币数量；阶梯止盈每次只推进一个档位；全仓计数阈值、止损和定时卖出
均按 Node.js 的优先级和冷却规则评估。V4 买入广播记录 nonce、两档 EIP-1559 费用和
替换次数，新的有效信号复用 nonce 并将费用提高 12.5%；Pons 曲线日志由区块轮询补齐，
V4 日志通过 WSS 接收。策略事件按用户/代币/池地址稳定分片到并行 worker，同一仓位仍按链上顺序串行处理。实际链上成交回执由交易执行器以 `own=true` 事件回写，只有该类
成功事件才更新仓位和卖出计数。

监控用户更新采用 Node.js 相同的合并语义，省略字段不会清空既有策略、钱包或账户开关；
用户查询返回归一化后的公开配置，私钥和数据库内部 id 永不出现在响应中。每一笔已接受的
V4 原始买单或替换单都写入 `strategy_buy_attempts`，回执按真正成交的交易哈希选择唯一赢家，
其余尝试标记为 replaced，服务重启后由回执协调器继续处理。持仓数量在数据库中按
`NUMERIC` 原始整数文本保存，避免 18 位代币数量经过 `float64` 后丢失最小单位。

## Docker 运行

```bash
cp .env.example .env
docker compose up -d --build
```

应用容器启动时自动执行 SQL 迁移；PostgreSQL 数据位于 `postgres` 卷，上传文件位于
`uploads` 卷。开发调试可直接运行 `go run ./cmd/server` 并把数据库和 Redis 指向本机映射端口。

## 配置清单

`PORT`、`DATABASE_URL`、`REDIS_URL`、`RPC_HTTP_URL`、`RPC_WS_URL`、`CHAIN_ID`、
`AVE_BASE_URL`、`AVE_X_AUTH`、`AVE_VISITOR_ID`（AVE 网页的浏览器指纹，用于自动刷新认证）、私钥密码、存储路径、监听开关、PoolManager、Universal Router、
Permit2、Pons Factory 和 Pons 外部 Hook 地址均从环境变量读取。性能参数包括
`STRATEGY_MIN_EVENT_USD`、`WSS_INGRESS_BUFFER`、`WSS_STRATEGY_BUFFER`、
`WSS_READ_LIMIT_BYTES`、`STRATEGY_WORKERS`、`STRATEGY_QUEUE_BUFFER` 和
`STRATEGY_SHARD_BACKLOG`；
`.env.example` 给出可直接用于 Compose 的默认值。
