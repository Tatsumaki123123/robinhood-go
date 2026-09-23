# RobinhoodGo

Go backend implementation of the Robinhood Node.js chain monitor and trading service.
The complete endpoint and strategy specification is in
[`docs/FUNCTIONAL_SPEC.md`](docs/FUNCTIONAL_SPEC.md).
The accepted BottomFishing configuration example is in
[`docs/STRATEGY_CONFIG.md`](docs/STRATEGY_CONFIG.md).

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

开发环境使用源码挂载和 `go run -mod=mod`，启动时自动补齐依赖并将 `go.mod`、`go.sum` 更新写回宿主机；请将这两个文件一起纳入版本控制。修改代码后需要重启应用容器才能重新编译：

```powershell
Copy-Item .env.dev.example .env.dev
docker compose -f docker-compose.dev.yml up --build
```

```powershell
docker compose -f docker-compose.dev.yml restart app
```

生产环境使用默认的 `docker-compose.yml`，构建出的应用镜像为 distroless 静态镜像：

```powershell
Copy-Item .env.example .env
docker compose -f docker-compose.yml up -d --build
```

开发和生产使用不同的 PostgreSQL、Redis、上传目录卷；如需清理对应环境数据，使用
`docker compose -f <compose-file> down -v`。

The API is available at `http://localhost:3000/api/v1`. PostgreSQL migrations run when
the application starts. Configure the wallet, RPC and contract settings before enabling
live trading. WebSocket chain events are available at `/ws`.

AVE 请求优先读取数据库 `ave_configs`（`id=1`）的 `x_auth`，未保存时使用
`AVE_X_AUTH`。token 为空、收到 HTTP 401/403 或 AVE 状态码 10000/10001 时，
应用会 GET `/v1api/v2/settings/serverTime`，按前端格式生成 RSA-OAEP/SHA-256
加密的 `request_id`，再 POST `/v1api/v1/captcha/requestToken`。成功取得 `data.id`
并保存到数据库后，使用新 token 请求原接口；已失败的原请求最多重试一次。
同一进程内的并发请求会复用已刷新的 token，其他业务错误不会触发刷新。

自动刷新需要在 `.env.dev` 或 `.env` 中配置 `AVE_VISITOR_ID`，填写 AVE 桌面网页
指纹库返回的 32 位十六进制 `visitorId`（在 `vemachine` 构造加密明文处查看）。
服务端使用 `web` 平台；该值不是 `request_id` 或 `data.id`，也不能从 `server_time`
推导。缺少指纹、刷新失败、需要验证码或数据库保存失败时会返回错误，不覆盖旧 token。
修改环境文件后用 `docker compose -f docker-compose.dev.yml up -d --force-recreate app`
重建开发容器以加载新配置。

服务启动后会参考 Node.js 服务执行两项后台同步：每小时从 AVE 刷新已启用
`monitorToken` 的行情和池数据；每分钟读取启用监控用户的钱包持仓，并将匹配的
AVE 持仓数量、平均成本和剩余持仓写回 `strategy_positions` 及最近一条
`monitor_records`。这些请求在后台执行，不会阻塞链上交易事件处理。

策略引擎会在监控用户和代币均启用且存在私钥时使用已缓存的 V4 路由广播买卖；V4 买入的
nonce、费用上限和替换次数写入 `strategy_pending_buys`，只有 own fill 事件才会更新持仓。
未配置可执行路由时会回退到 Pons V2 曲线交易。

## BottomFishing 配置

用户配置保存在 `monitorUser.updateUser` 的 `config` 字段中。下面的 JSONC 示例与
当前 Go 策略引擎的字段和判断顺序一致：

```jsonc
{
  "name": "大单抄底策略",
  "enabled": true,
  "minMcp": 100000, // 只限制买入事件的最低市值（USD）
  "slippage": 0.05, // 5%
  "diffBuySecond": 60, // 两次新买入的最小间隔；不延迟首次买入
  "maxLossBuyTimes": 5, // 价格低于本轮首笔买入价时的累计买入次数上限（含首笔）；0 禁止该加仓路径
  "minLossBuyRatio": 0.01, // 低于首笔价格时，还需较上一笔买入价下跌 1%；0 不增加该限制
  "replacePending": true,
  "pendingBuyTimeoutSecond": 2,
  "tokenConfig": [
    {
      "maxMcp": 1000000000,
      "buyUSD": 1000, // buyRatio 为 0 时使用；不是金额上限
      "buyRatio": 0.5, // 大于 0 时按触发卖单金额的 50% 买入
      "minSellRatio": 0.01 // 外部卖出造成的最低价格跌幅
    }
  ],
  "sellPolicy": {
    "fullSellAfterSellCount": 3, // 第 N 次有效卖出直接清仓；这里 N=3；0 关闭
    "resetSellCountOnBuy": true
  },
  "scheduledSell": {
    "enabled": true,
    "intervalSecond": 1250,
    "sellRatio": 0.2,
    "baseUSD": 150,
    "resetOnBuy": false // 补仓不重启首次建仓的定时节奏
  },
  "externalBuySell": {
    "enabled": true,
    "minBuyUSD": 100,
    "buyImpactRatio": 0.03,
    "needProfit": true,
    "profitRatio": 0.05,
    "sellRatio": 0.2,
    "buyAmountRatio": 0, // 0 使用 sellRatio；大于 0 按外部买单金额计算
    "cooldownSecond": 30 // 命中外部买单卖出条件时开始冷却
  },
  "profitSell": {
    "enabled": true,
    "levels": [
      { "profitRatio": 0.05, "sellRatio": 0.2 },
      { "profitRatio": 0.1, "sellRatio": 0.25 },
      { "profitRatio": 0.2, "sellRatio": 0.3 },
      { "profitRatio": 0.3, "sellRatio": 1.0 }
    ],
    "resetOnBuy": true
  },
  "lossSell": {
    "enabled": true,
    "triggerRatio": 0.15,
    "sellAll": true
  },
  "listConfig": {
    "source": "ave",
    "category": "pons_out_hot",
    "minMcp": 100000,
    "maxMcp": 1000000000,
    "createDay": 1,
    "from": "list",
    "userAddress": ""
  }
}
```

`listConfig.from` 默认为 `list`（省略或为空时沿用 AVE 列表接口）。设置为 `user` 时，
`/monitorToken/getAveList` 改为调用 AVE 钱包代币接口，并使用 `listConfig.userAddress` 作为钱包地址。

外部卖出事件（事件 `side=sell`）用于评估抄底买入；外部买入事件（事件
`side=buy`）用于评估提前卖出。`maxLossBuyTimes` 和 `minLossBuyRatio` 只在当前价低于
本轮首笔买入价时生效；当前价高于首笔价格时不拦截。卖出后本轮补仓计数会重置。
