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
the application starts. Set `UNISWAP_V4_TRADING_DRY_RUN=true` while configuring routes or
RPC credentials; set it to `false` only after the wallet, RPC and contract settings are
ready. WebSocket chain events are available at `/ws`.

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

策略引擎会在监控用户和代币均启用且存在私钥时使用已缓存的 V4 路由广播买卖；V4 买入的
nonce、费用上限和替换次数写入 `strategy_pending_buys`，只有 own fill 事件才会更新持仓。
未配置可执行路由时会回退到 Pons V2 曲线交易。
