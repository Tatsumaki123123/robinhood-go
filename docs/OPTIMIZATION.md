# 交易延迟优化运行手册

## 已实现路径

```text
Alchemy WSS
  -> 有界 ingress 队列
  -> 按事件分片的 strategy 队列
  -> 先识别己方成交，再做外部 USD 快速过滤
  -> 完整策略和数据库状态机
  -> 按钱包串行 nonce 分配
  -> Keep-Alive HTTP RPC
```

USD 过滤只在 quote amount、quote decimals 和有效且未过期的 USD 价格都可靠时执行。
低于 `STRATEGY_MIN_EVENT_USD` 的外部事件直接跳过；己方成交回执永远保留，缺少或
失效元数据的事件不会被误判为低价值。native quote 在缺少 decimals 时按 18 位处理。

部署/调试日志仍会发送给 API 事件订阅，但不会进入策略队列；策略队列只接收已解码的
Curve buy/sell 或 Uniswap V4 Swap 事件。

WSS 断线、队列溢出和重连间隙由区块轮询补偿：Pons 曲线按曲线地址读取买卖日志，
Uniswap V4 按 PoolManager 和 `V4SwapTopic` 读取 Swap 日志。两条路径都使用链上
transaction hash + log index 作为幂等键。

## 监控

查看 `/health`，重点关注：

- `wssIngressDepth`、`strategyChannelDepth`；
- `wssDroppedEvents`、`wssDroppedSubscriberEvents`；
- `wss.connections`、`wss.messages`、`wss.readErrors`；
- `rpcHTTP.avgLatencyMs`、`rpcHTTP.errors`；
- `strategy.lowValueDrops`、`strategy.filterUnknown`；
- `strategy.queueBacklog`、`strategy.queueDropped`、`strategy.avgQueueWaitMs`。

`wssDroppedEvents` 或 `strategy.queueDropped` 增长时，必须确认对应的区块补偿仍在运行；
V4 的 PoolManager 补偿和 Pons 曲线补偿都应保持 `rpcHTTP.errors` 为零或可解释的短暂峰值。

队列深度持续上升或 `queueDropped` 增长时，不要盲目增加 Worker。先收窄 Alchemy
订阅的 address/topic，确认策略和数据库没有慢查询，再按压测结果调整队列参数。

## 参数基线

生产环境默认值：

```text
STRATEGY_MIN_EVENT_USD=10
WSS_INGRESS_BUFFER=32768
WSS_STRATEGY_BUFFER=8192
WSS_READ_LIMIT_BYTES=1048576
STRATEGY_WORKERS=16
STRATEGY_QUEUE_BUFFER=512
STRATEGY_SHARD_BACKLOG=2048
```

`t3` 等突发型实例不适合作为稳定交易节点。优先使用计算型实例、ENA 增强网络和
专用 vCPU。只有在 p99 数据证明调度或 softirq 是瓶颈时，才使用 Docker 的
`cpuset`/`--cpuset-cpus`；不要在业务 goroutine 中使用 `runtime.LockOSThread`。

不要固定 Alchemy IP 到 hosts 文件。应保留 DNS 负载均衡和故障切换能力。

## GC 和验证

默认保持 Go GC 参数，先观察堆大小、分配率和 p99，再逐步测试 `GOGC` 与
`GOMEMLIMIT`。不要直接使用 `GOGC=500`。

链上发送验证应使用脱离生产钱包的回放数据和测试账户，覆盖正常流量、10 倍突发、
大量低价值事件、WSS 重连、RPC 429/5xx、nonce 竞争、广播前失败和发送响应丢失等场景。
验证目标是己方小额成交仍然记账、大额事件无误放行、低价值外部事件无误丢弃、区块
补偿不重复触发、队列有界、nonce 不冲突，而不只是平均耗时下降。
