# BottomFishing strategy configuration

`monitorUser.updateUser` stores the following fields inside `config`. The Go
service accepts the same camel-case names as the Node service. Values are
validated as non-negative numbers; `tokenConfig` is selected by ascending
`maxMcp` and the first matching rule is used.

```json
{
  "name": "大单抄底策略",
  "enabled": true,
  "minMcp": 100000,
  "slippage": 0.05,
  "diffBuySecond": 60,
  "maxLossBuyTimes": 5,
  "minLossBuyRatio": 0.01,
  "replacePending": true,
  "tokenConfig": [{
    "maxMcp": 1000000000,
    "buyUSD": 1000,
    "buyRatio": 0.5,
    "minSellRatio": 0.01
  }],
  "sellPolicy": {
    "fullSellAfterSellCount": 3,
    "resetSellCountOnBuy": true
  },
  "scheduledSell": {
    "enabled": true,
    "intervalSecond": 1250,
    "sellRatio": 0.2,
    "baseUSD": 150,
    "resetOnBuy": false
  },
  "externalBuySell": {
    "enabled": true,
    "minBuyUSD": 100,
    "buyImpactRatio": 0.03,
    "needProfit": true,
    "profitRatio": 0.05,
    "sellRatio": 0.2,
    "buyAmountRatio": 0,
    "cooldownSecond": 30
  },
  "profitSell": {
    "enabled": true,
    "levels": [
      {"profitRatio": 0.05, "sellRatio": 0.2},
      {"profitRatio": 0.1, "sellRatio": 0.25},
      {"profitRatio": 0.2, "sellRatio": 0.3},
      {"profitRatio": 0.3, "sellRatio": 1}
    ],
    "resetOnBuy": true
  },
  "lossSell": {"enabled": true, "triggerRatio": 0.15, "sellAll": true},
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

`listConfig.from` defaults to `list` (or empty), which keeps the existing AVE list request.
Set it to `user` to make `/monitorToken/getAveList` use the AVE wallet token endpoint with
`listConfig.userAddress`.

The engine persists `strategy_positions`, `strategy_pending_buys` and
`strategy_events`. A successful own-wallet fill must be emitted with `own:true`
so weighted average cost, sell count, profit level and scheduled state advance
only after a confirmed transaction. External buy signals check amount, impact
and profit together; cooldown suppresses external, profit and loss decisions
until it expires, while scheduled sells continue independently.

`maxLossBuyTimes` counts the current holding cycle's buys, including the first
buy. A value of `0` disables the lower-than-first-price add-on path. The guard
is ignored when the current price is at or above the cycle's first buy price;
`minLossBuyRatio` only adds a required drop from the previous buy when that
lower-price path is active. A confirmed sell resets the current add-on cycle.
When `fullSellAfterSellCount` is `N`, the Nth valid sell is the full exit;
`0` disables this rule. `baseUSD` does not cap that forced scheduled exit.
