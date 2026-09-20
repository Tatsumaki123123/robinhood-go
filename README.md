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

策略引擎会在监控用户和代币均启用且存在私钥时使用已缓存的 V4 路由广播买卖；V4 买入的
nonce、费用上限和替换次数写入 `strategy_pending_buys`，只有 own fill 事件才会更新持仓。
未配置可执行路由时会回退到 Pons V2 曲线交易。
