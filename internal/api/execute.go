package api

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
	"robinhood-go/internal/chain"
	secret "robinhood-go/internal/crypto"
	"robinhood-go/internal/httpx"
	"robinhood-go/internal/store"
)

// executeMu serializes lifecycle transitions (start, wallet generation and
// recycle) while individual wallet swaps remain independent.
var executeMu sync.Mutex

const executeZeroCurve = "0x0000000000000000000000000000000000000000"

func executeLineID(d map[string]any) int64 { return id(d["line"]) }

func (a *API) executeLine(ctx context.Context, lineID int64) (store.ExecuteLine, error) {
	if lineID <= 0 {
		return store.ExecuteLine{}, fmt.Errorf("line is required")
	}
	return a.Store.ExecuteLine(ctx, lineID)
}

func (a *API) executeBatch(ctx context.Context, d map[string]any) (store.ExecuteBatch, error) {
	if eid := id(d["eid"]); eid > 0 {
		return a.Store.ExecuteBatch(ctx, eid)
	}
	if eid := id(d["tokenEid"]); eid > 0 {
		return a.Store.ExecuteBatch(ctx, eid)
	}
	lineID := executeLineID(d)
	if lineID <= 0 {
		return store.ExecuteBatch{}, fmt.Errorf("line or eid is required")
	}
	return a.Store.ActiveExecuteBatch(ctx, lineID)
}

func executePrivateKey(address string, key *ecdsa.PrivateKey) (string, error) {
	if key == nil {
		return "", fmt.Errorf("private key is required")
	}
	if address != "" && !strings.EqualFold(address, gethcrypto.PubkeyToAddress(key.PublicKey).Hex()) {
		return "", fmt.Errorf("private key does not match wallet address")
	}
	return fmt.Sprintf("%x", gethcrypto.FromECDSA(key)), nil
}

func executeWalletView(w store.ExecuteWallet) map[string]any {
	return map[string]any{"id": w.ID, "eid": w.EID, "index": w.WalletIndex, "address": strings.ToLower(w.Address), "active": w.Active, "createdAt": w.CreatedAt, "updatedAt": w.UpdatedAt}
}

func executeBatchView(b store.ExecuteBatch) map[string]any {
	return map[string]any{"eid": b.EID, "line": b.LineID, "active": b.Active, "bossAddress": strings.ToLower(b.BossAddress), "walletsExist": b.WalletsExist, "status": b.Status, "createdAt": b.CreatedAt, "updatedAt": b.UpdatedAt}
}

func executeRoute(raw any) []map[string]any {
	out := []map[string]any{}
	switch values := raw.(type) {
	case []any:
		for _, value := range values {
			if item, ok := value.(map[string]any); ok {
				out = append(out, item)
			}
		}
	case []map[string]any:
		out = append(out, values...)
	}
	return out
}

func executeNativeAlias(v string) string {
	v = low(v)
	if v == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" || v == "eth" || v == "native" {
		return chain.NativeAddress
	}
	return v
}

func executePositiveRaw(v any) *big.Int {
	n := chain.ToBig(v)
	if n.Sign() > 0 {
		return n
	}
	text := strings.TrimSpace(str(v))
	if text == "" {
		return big.NewInt(0)
	}
	if raw := parseEtherRaw(text); raw != "" {
		return chain.ToBig(raw)
	}
	return big.NewInt(0)
}

func executeNativeAmount(v any) *big.Int {
	text := strings.TrimSpace(str(v))
	if text == "" {
		return big.NewInt(0)
	}
	raw := parseEtherRaw(text)
	if raw == "" {
		return big.NewInt(0)
	}
	return chain.ToBig(raw)
}

func executeNativeDisplay(raw *big.Int) string {
	if raw == nil {
		return "0"
	}
	return new(big.Rat).SetFrac(raw, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)).FloatString(18)
}

func executeBuyStage(v any) string {
	switch strings.ToLower(strings.TrimSpace(str(v))) {
	case "first", "firstbuy", "firstsell":
		return "first"
	case "second", "secondbuy", "secondsell":
		return "second"
	case "third", "thirdbuy", "thirdsell":
		return "third"
	case "multi", "multibuy", "multisell":
		return "multi"
	case "all":
		return "all"
	default:
		return strings.ToLower(strings.TrimSpace(str(v)))
	}
}

func executeConfigItems(cfg map[string]any) []map[string]any {
	result := []map[string]any{}
	items, _ := cfg["walletConfig"].([]any)
	for _, raw := range items {
		if item, ok := raw.(map[string]any); ok {
			result = append(result, item)
		}
	}
	return result
}

func executeConfigBuyAmount(item map[string]any, stage string) (*big.Int, bool) {
	stage = executeBuyStage(stage)
	if item == nil || stage == "" || stage == "all" {
		return big.NewInt(0), false
	}
	stageValue, ok := item[stage+"Buy"]
	if !ok {
		return big.NewInt(0), false
	}
	stageMap, ok := stageValue.(map[string]any)
	if !ok {
		return big.NewInt(0), false
	}
	enabled, ok := stageMap["enable"].(bool)
	if !ok || !enabled {
		return big.NewInt(0), false
	}
	return executeNativeAmount(stageMap["buyAmount"]), true
}

func executeConfigSellPercent(item map[string]any, stage string, fallback *big.Rat) (*big.Rat, bool) {
	stage = executeBuyStage(stage)
	if stage == "all" {
		return new(big.Rat).SetInt64(100), true
	}
	if item == nil {
		return nil, false
	}
	stageValue, ok := item[stage+"Sell"]
	if !ok {
		return nil, false
	}
	stageMap, ok := stageValue.(map[string]any)
	if !ok {
		return nil, false
	}
	enabled, ok := stageMap["enable"].(bool)
	if !ok || !enabled {
		return nil, false
	}
	value := stageMap["sellRatio"]
	if value == nil {
		return new(big.Rat).Set(fallback), true
	}
	ratio, ok := new(big.Rat).SetString(strings.TrimSpace(str(value)))
	if !ok || ratio.Sign() <= 0 {
		return nil, false
	}
	if ratio.Cmp(big.NewRat(1, 1)) <= 0 {
		ratio.Mul(ratio, big.NewRat(100, 1))
	}
	if ratio.Cmp(big.NewRat(100, 1)) > 0 {
		return nil, false
	}
	return ratio, true
}

func executePercentAmount(balance *big.Int, percent *big.Rat) *big.Int {
	if balance == nil || percent == nil || balance.Sign() <= 0 || percent.Sign() <= 0 {
		return big.NewInt(0)
	}
	numerator := new(big.Int).Mul(balance, percent.Num())
	denominator := new(big.Int).Mul(percent.Denom(), big.NewInt(100))
	return numerator.Div(numerator, denominator)
}

func (a *API) executeLineUpdate(c *fiber.Ctx) error {
	d := body(c)
	lineID := id(d["line"])
	if lineID <= 0 {
		return httpx.Error(c, 400, "line is required")
	}
	executeMu.Lock()
	defer executeMu.Unlock()
	line, lineErr := a.Store.ExecuteLine(c.Context(), lineID)
	if lineErr != nil && !errors.Is(lineErr, pgx.ErrNoRows) {
		return a.fail(c, lineErr)
	}
	hasLine := lineErr == nil
	cfg := map[string]any{}
	if hasLine {
		for k, v := range line.Config {
			cfg[k] = v
		}
	}
	if raw, ok := d["data"]; ok {
		switch value := raw.(type) {
		case map[string]any:
			for k, v := range value {
				cfg[k] = v
			}
		case string:
			var parsed map[string]any
			if err := json.Unmarshal([]byte(value), &parsed); err != nil {
				return httpx.Error(c, 400, "data must be a JSON object")
			}
			for k, v := range parsed {
				cfg[k] = v
			}
		}
	}
	for _, key := range []string{"walletConfig", "walletCount", "withdrawAddress", "autoSwap", "name"} {
		if value, ok := d[key]; ok {
			cfg[key] = value
		}
	}
	enabled := true
	if hasLine {
		enabled = line.Enabled
	}
	if value, ok := d["enabled"].(bool); ok {
		enabled = value
	}
	name := str(d["name"])
	if name == "" && hasLine {
		name = line.Name
	}
	bossAddress, bossPrivateKey := "", ""
	if hasLine {
		bossAddress, bossPrivateKey = line.BossAddress, line.BossPrivateKey
	}
	suppliedBossKey := strings.TrimSpace(str(d["bossPrivateKey"]))
	if suppliedBossKey == "" {
		suppliedBossKey = strings.TrimSpace(str(cfg["bossPrivateKey"]))
	}
	delete(cfg, "bossPrivateKey")
	if suppliedKey := suppliedBossKey; suppliedKey != "" {
		key, err := gethcrypto.HexToECDSA(strings.TrimPrefix(suppliedKey, "0x"))
		if err != nil {
			return httpx.Error(c, 400, "bossPrivateKey is invalid")
		}
		bossAddress = strings.ToLower(gethcrypto.PubkeyToAddress(key.PublicKey).Hex())
		bossPrivateKey, err = secret.Encrypt(strings.TrimPrefix(suppliedKey, "0x"), a.Cfg.EncryptionKey)
		if err != nil {
			return a.fail(c, err)
		}
	} else if strings.TrimSpace(bossAddress) == "" {
		// Every line has a stable boss from creation onward. Legacy lines with
		// no boss are repaired here and remain compatible with /start.
		key, err := gethcrypto.GenerateKey()
		if err != nil {
			return a.fail(c, err)
		}
		private, keyErr := executePrivateKey("", key)
		if keyErr != nil {
			return a.fail(c, keyErr)
		}
		bossAddress = strings.ToLower(gethcrypto.PubkeyToAddress(key.PublicKey).Hex())
		bossPrivateKey, err = secret.Encrypt(private, a.Cfg.EncryptionKey)
		if err != nil {
			return a.fail(c, err)
		}
	}
	updated, err := a.Store.UpsertExecuteLine(c.Context(), lineID, 0, name, cfg, enabled, bossAddress, bossPrivateKey)
	if err != nil {
		return a.fail(c, err)
	}
	if !hasLine {
		if _, batchErr := a.Store.CreateExecuteBatch(c.Context(), lineID, updated.BossAddress, updated.BossPrivateKey); batchErr != nil {
			_ = a.Store.DeleteExecuteLine(c.Context(), lineID)
			return a.fail(c, batchErr)
		}
	}
	return a.ok(c, map[string]any{"line": updated.LineID, "name": updated.Name, "config": cfg, "bossAddress": updated.BossAddress, "enabled": updated.Enabled, "createdAt": updated.CreatedAt, "updatedAt": updated.UpdatedAt})
}

func (a *API) executeLines(c *fiber.Ctx) error {
	lines, err := a.Store.ExecuteLines(c.Context())
	if err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, lines)
}

func (a *API) executeAddLine(c *fiber.Ctx) error {
	executeMu.Lock()
	defer executeMu.Unlock()

	lineID, err := a.Store.NextExecuteLineID(c.Context())
	if err != nil {
		return a.fail(c, err)
	}
	name := str(body(c)["name"])
	if name == "" {
		name = fmt.Sprintf("Line %d", lineID)
	}
	key, err := gethcrypto.GenerateKey()
	if err != nil {
		return a.fail(c, err)
	}
	private, err := executePrivateKey("", key)
	if err != nil {
		return a.fail(c, err)
	}
	address := strings.ToLower(gethcrypto.PubkeyToAddress(key.PublicKey).Hex())
	encrypted, err := secret.Encrypt(private, a.Cfg.EncryptionKey)
	if err != nil {
		return a.fail(c, err)
	}
	updated, err := a.Store.UpsertExecuteLine(c.Context(), lineID, 0, name, map[string]any{"name": name}, true, address, encrypted)
	if err != nil {
		return a.fail(c, err)
	}
	batch, err := a.Store.CreateExecuteBatch(c.Context(), lineID, address, encrypted)
	if err != nil {
		_ = a.Store.DeleteExecuteLine(c.Context(), lineID)
		return a.fail(c, err)
	}
	return a.ok(c, map[string]any{
		"line":        updated.LineID,
		"name":        updated.Name,
		"config":      updated.Config,
		"bossAddress": updated.BossAddress,
		"enabled":     updated.Enabled,
		"eid":         batch.EID,
		"active":      batch.Active,
		"createdAt":   updated.CreatedAt,
		"updatedAt":   updated.UpdatedAt,
	})
}

func (a *API) executeDeleteLine(c *fiber.Ctx) error {
	d := body(c)
	if str(d["password"]) != a.Cfg.ExportPassword {
		return httpx.Error(c, 400, "invalid password")
	}
	lineID := id(d["lineId"])
	if lineID <= 0 {
		lineID = id(d["line"])
	}
	if lineID <= 0 {
		return httpx.Error(c, 400, "lineId is required")
	}

	executeMu.Lock()
	defer executeMu.Unlock()

	line, err := a.Store.ExecuteLine(c.Context(), lineID)
	if err != nil {
		return httpx.Error(c, 404, "execute line not found")
	}
	batch, batchErr := a.Store.ActiveExecuteBatch(c.Context(), lineID)
	if batchErr != nil && !errors.Is(batchErr, pgx.ErrNoRows) {
		return a.fail(c, batchErr)
	}
	bossAddresses := map[string]struct{}{}
	if address := strings.TrimSpace(line.BossAddress); address != "" {
		bossAddresses[strings.ToLower(address)] = struct{}{}
	}
	if batchErr == nil {
		if address := strings.TrimSpace(batch.BossAddress); address != "" {
			bossAddresses[strings.ToLower(address)] = struct{}{}
		}
	}
	for address := range bossAddresses {
		balanceRaw, balanceErr := a.RPC.Balance(c.Context(), address)
		if balanceErr != nil {
			return a.fail(c, balanceErr)
		}
		if chain.ToBig(balanceRaw).Sign() > 0 {
			return httpx.Error(c, 409, "boss wallet still has native balance")
		}
	}
	if batchErr == nil {
		wallets, walletErr := a.Store.ExecuteWallets(c.Context(), batch.EID, true)
		if walletErr != nil {
			return a.fail(c, walletErr)
		}
		for _, wallet := range wallets {
			balanceRaw, balanceErr := a.RPC.Balance(c.Context(), wallet.Address)
			if balanceErr != nil {
				return a.fail(c, balanceErr)
			}
			if chain.ToBig(balanceRaw).Sign() > 0 {
				return httpx.Error(c, 409, "active execute wallet still has native balance")
			}
		}
	}

	if err = a.Store.DeleteExecuteLine(c.Context(), lineID); err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, map[string]any{"deleted": true, "line": lineID})
}

func (a *API) executeStart(c *fiber.Ctx) error {
	d := body(c)
	lineID := executeLineID(d)
	line, err := a.executeLine(c.Context(), lineID)
	if err != nil {
		return httpx.Error(c, 404, "execute line not found")
	}
	if !line.Enabled {
		return httpx.Error(c, 400, "execute line is disabled")
	}
	executeMu.Lock()
	defer executeMu.Unlock()
	previous, previousErr := a.Store.ActiveExecuteBatch(c.Context(), lineID)
	if previousErr != nil && !errors.Is(previousErr, pgx.ErrNoRows) {
		return a.fail(c, previousErr)
	}
	if previousErr == nil {
		wallets, walletErr := a.Store.ExecuteWallets(c.Context(), previous.EID, true)
		if walletErr != nil {
			return a.fail(c, walletErr)
		}
		for _, wallet := range wallets {
			balance, balanceErr := a.RPC.Balance(c.Context(), wallet.Address)
			if balanceErr != nil {
				return a.fail(c, balanceErr)
			}
			if chain.ToBig(balance).Sign() > 0 {
				return httpx.Error(c, 409, "active execute wallets still have native balance; call /api/v1/executerobin/end first")
			}
		}
	}
	var bossKey *ecdsa.PrivateKey
	if previousErr != nil && strings.TrimSpace(line.BossPrivateKey) != "" {
		stored, decryptErr := secret.Decrypt(line.BossPrivateKey, a.Cfg.EncryptionKey)
		if decryptErr != nil {
			return a.fail(c, decryptErr)
		}
		bossKey, err = gethcrypto.HexToECDSA(strings.TrimPrefix(stored, "0x"))
	} else {
		bossKey, err = gethcrypto.GenerateKey()
	}
	if err != nil {
		return a.fail(c, err)
	}
	bossPrivate, err := executePrivateKey("", bossKey)
	if err != nil {
		return a.fail(c, err)
	}
	bossAddress := strings.ToLower(gethcrypto.PubkeyToAddress(bossKey.PublicKey).Hex())
	encryptedBoss, err := secret.Encrypt(bossPrivate, a.Cfg.EncryptionKey)
	if err != nil {
		return a.fail(c, err)
	}
	if previousErr == nil && previous.PrivateKeyEnc != "" && !strings.EqualFold(previous.BossAddress, bossAddress) {
		oldPrivate, decryptErr := secret.Decrypt(previous.PrivateKeyEnc, a.Cfg.EncryptionKey)
		if decryptErr != nil {
			return a.fail(c, decryptErr)
		}
		if _, transferErr := a.Trading.SendNativeAll(c.Context(), oldPrivate, bossAddress); transferErr != nil && !strings.Contains(strings.ToLower(transferErr.Error()), "balance") {
			return a.fail(c, transferErr)
		}
	}
	if err = a.Store.SetExecuteLineBoss(c.Context(), lineID, bossAddress, encryptedBoss); err != nil {
		return a.fail(c, err)
	}
	batch, err := a.Store.CreateExecuteBatch(c.Context(), lineID, bossAddress, encryptedBoss)
	if err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, executeBatchView(batch))
}

func (a *API) executeGenerateWallets(c *fiber.Ctx) error {
	d := body(c)
	executeMu.Lock()
	defer executeMu.Unlock()
	batch, err := a.executeBatch(c.Context(), d)
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	line, err := a.Store.ExecuteLine(c.Context(), batch.LineID)
	if err != nil {
		return a.fail(c, err)
	}
	items := executeConfigItems(line.Config)
	count := int(id(line.Config["walletCount"]))
	if len(items) > count {
		count = len(items)
	}
	if count <= 0 {
		count = 1
	}
	existing, err := a.Store.ExecuteWallets(c.Context(), batch.EID, false)
	if err != nil {
		return a.fail(c, err)
	}
	byIndex := map[int]store.ExecuteWallet{}
	for _, wallet := range existing {
		byIndex[wallet.WalletIndex] = wallet
	}
	bossKey, err := secret.Decrypt(batch.PrivateKeyEnc, a.Cfg.EncryptionKey)
	if err != nil {
		return a.fail(c, err)
	}
	result := []map[string]any{}
	for index := 0; index < count; index++ {
		wallet, found := byIndex[index]
		if !found {
			key, keyErr := gethcrypto.GenerateKey()
			if keyErr != nil {
				return a.fail(c, keyErr)
			}
			private, privateErr := executePrivateKey("", key)
			if privateErr != nil {
				return a.fail(c, privateErr)
			}
			encrypted, encryptErr := secret.Encrypt(private, a.Cfg.EncryptionKey)
			if encryptErr != nil {
				return a.fail(c, encryptErr)
			}
			wallet = store.ExecuteWallet{EID: batch.EID, WalletIndex: index, Address: strings.ToLower(gethcrypto.PubkeyToAddress(key.PublicKey).Hex()), PrivateKeyEnc: encrypted, Active: true}
			wallet, err = a.Store.UpsertExecuteWallet(c.Context(), wallet)
			if err != nil {
				return a.fail(c, err)
			}
		}
		if wallet.Active {
			item := map[string]any{"wallet": executeWalletView(wallet)}
			if index < len(items) {
				amount := executePositiveRaw(items[index]["transferAmountRaw"])
				if amount.Sign() == 0 {
					amount = executeNativeAmount(items[index]["transferAmount"])
				}
				shouldFund := !found
				if found {
					balanceRaw, balanceErr := a.RPC.Balance(c.Context(), wallet.Address)
					if balanceErr != nil {
						return a.fail(c, balanceErr)
					}
					shouldFund = chain.ToBig(balanceRaw).Sign() == 0
				}
				if amount.Sign() > 0 && shouldFund {
					bossKeyHex := strings.TrimPrefix(bossKey, "0x")
					transfer, transferErr := a.Trading.SendNative(c.Context(), bossKeyHex, wallet.Address, amount)
					if transferErr != nil {
						return a.fail(c, transferErr)
					}
					item["funding"] = transfer
				}
			}
			result = append(result, item)
		}
	}
	if err = a.Store.SetExecuteBatchWalletsExist(c.Context(), batch.EID, true); err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, map[string]any{"eid": batch.EID, "wallets": result})
}

func (a *API) executeWallets(c *fiber.Ctx) error {
	d := body(c)
	batch, err := a.executeBatch(c.Context(), d)
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	activeOnly := true
	if all, ok := d["all"].(bool); ok && all {
		activeOnly = false
	}
	wallets, err := a.Store.ExecuteWallets(c.Context(), batch.EID, activeOnly)
	if err != nil {
		return a.fail(c, err)
	}
	result := make([]map[string]any, 0, len(wallets))
	for _, wallet := range wallets {
		view := executeWalletView(wallet)
		balanceRaw, balanceErr := a.RPC.Balance(c.Context(), wallet.Address)
		if balanceErr != nil {
			return a.fail(c, balanceErr)
		}
		balance := chain.ToBig(balanceRaw)
		view["balanceRaw"] = balance.String()
		view["balance"] = executeNativeDisplay(balance)
		result = append(result, view)
	}
	return a.ok(c, result)
}

func (a *API) executeBoss(c *fiber.Ctx) error {
	batch, err := a.executeBatch(c.Context(), body(c))
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	view := executeBatchView(batch)
	balanceRaw, balanceErr := a.RPC.Balance(c.Context(), batch.BossAddress)
	if balanceErr != nil {
		return a.fail(c, balanceErr)
	}
	balance := chain.ToBig(balanceRaw)
	view["balanceRaw"] = balance.String()
	view["balance"] = executeNativeDisplay(balance)
	if tokens, tokenErr := a.Store.ExecuteTokens(c.Context(), batch.EID); tokenErr == nil {
		view["tokenCount"] = len(tokens)
	}
	return a.ok(c, view)
}

func (a *API) resolveExecutePool(ctx context.Context, target string, d map[string]any) (routeHop, map[string]any, error) {
	target = low(target)
	quote := executeNativeAlias(str(d["quoteTokenAddress"]))
	c0, c1 := executeNativeAlias(str(d["currency0"])), executeNativeAlias(str(d["currency1"]))
	poolID := normalizePoolID(str(d["poolId"]))
	if poolID == "0x" {
		poolID = ""
	}
	if quote == "" && c0 != "" && c1 != "" {
		if strings.EqualFold(c0, target) {
			quote = c1
		} else if strings.EqualFold(c1, target) {
			quote = c0
		}
	}
	if quote != "" && c0 != "" && c1 != "" && str(d["hooks"]) != "" {
		if !((strings.EqualFold(c0, target) || strings.EqualFold(c1, target)) && (strings.EqualFold(c0, quote) || strings.EqualFold(c1, quote))) {
			return routeHop{}, nil, fmt.Errorf("tokenAddress and quoteTokenAddress must be PoolKey currencies")
		}
		if !common.IsHexAddress(c0) || !common.IsHexAddress(c1) || !common.IsHexAddress(str(d["hooks"])) {
			return routeHop{}, nil, fmt.Errorf("currency0, currency1 and hooks must be valid EVM addresses")
		}
		if c0 > c1 {
			c0, c1 = c1, c0
		}
		calculated, err := chain.PoolID(map[string]any{"currency0": c0, "currency1": c1, "fee": d["fee"], "tickSpacing": d["tickSpacing"], "hooks": d["hooks"]})
		if err != nil {
			return routeHop{}, nil, fmt.Errorf("invalid Uniswap V4 PoolKey: %w", err)
		}
		if poolID == "" {
			poolID = normalizePoolID(calculated)
		}
		if !strings.EqualFold(normalizePoolID(calculated), poolID) {
			return routeHop{}, nil, fmt.Errorf("poolId does not match the supplied Uniswap V4 PoolKey")
		}
		hop := routeHop{PoolID: poolID, Currency0: c0, Currency1: c1, Fee: id(d["fee"]), TickSpacing: id(d["tickSpacing"]), Hooks: low(str(d["hooks"])), TokenIn: quote, TokenOut: target, HookData: "0x"}
		return hop, map[string]any{"name": str(d["tokenName"]), "symbol": str(d["tokenSymbol"]), "quoteTokenSymbol": str(d["quoteTokenSymbol"])}, nil
	}
	if a.Cfg.AveBaseURL == "" {
		return routeHop{}, nil, fmt.Errorf("pool key is required when AVE is unavailable")
	}
	detail, err := a.aveJSON(ctx, "/v2api/token_info/v1/token/detail", map[string]string{"token_id": target + "-robinhood", "cache_use": "false"})
	if err != nil {
		return routeHop{}, nil, err
	}
	data, _ := detail["data"].(map[string]any)
	pairs, _ := data["pairs"].([]any)
	requested := normalizePoolID(str(d["poolId"]))
	for _, raw := range pairs {
		pair, ok := raw.(map[string]any)
		if !ok || !isV4Pair(pair) || requested != "" && !strings.EqualFold(normalizePoolID(str(pairValue(pair, "pair", "poolId"))), requested) {
			continue
		}
		left := pairAddress(pair, "token0_address", "token0Address", "currency0", "currency0_address")
		right := pairAddress(pair, "token1_address", "token1Address", "currency1", "currency1_address")
		if strings.EqualFold(left, target) {
			quote = executeNativeAlias(right)
		} else if strings.EqualFold(right, target) {
			quote = executeNativeAlias(left)
		} else {
			continue
		}
		hop, ok := a.resolveAveHop(ctx, pair, quote, target)
		if ok {
			return hop, pair, nil
		}
	}
	return routeHop{}, nil, fmt.Errorf("AVE has no verified Uniswap V4 pool for token %s", target)
}

func routeMap(h routeHop) map[string]any { return h.mapValue() }

func validateExecuteRoute(route []map[string]any) error {
	if len(route) < 1 || len(route) > 3 {
		return fmt.Errorf("Uniswap V4 route must contain between 1 and 3 hops")
	}
	current := ""
	for index, hop := range route {
		in, out := executeNativeAlias(str(hop["tokenIn"])), executeNativeAlias(str(hop["tokenOut"]))
		if in == "" && index == 0 {
			in = executeNativeAlias(str(hop["currencyIn"]))
		}
		if index > 0 && in == "" {
			in = current
		}
		if in == "" || out == "" || in == out {
			return fmt.Errorf("route hop %d is missing connected tokenIn/tokenOut", index+1)
		}
		if index > 0 && !strings.EqualFold(in, current) {
			return fmt.Errorf("route hop %d is not connected to the previous hop", index+1)
		}
		c0, c1 := executeNativeAlias(str(hop["currency0"])), executeNativeAlias(str(hop["currency1"]))
		if c0 == "" || c1 == "" {
			c0, c1 = in, out
		}
		if c0 > c1 {
			c0, c1 = c1, c0
		}
		hooks := low(str(hop["hooks"]))
		if hooks == "" {
			return fmt.Errorf("route hop %d is missing hooks", index+1)
		}
		if !common.IsHexAddress(c0) || !common.IsHexAddress(c1) || !common.IsHexAddress(hooks) {
			return fmt.Errorf("route hop %d has invalid EVM addresses", index+1)
		}
		poolID, err := chain.PoolID(map[string]any{"currency0": c0, "currency1": c1, "fee": hop["fee"], "tickSpacing": hop["tickSpacing"], "hooks": hooks})
		if err != nil {
			return fmt.Errorf("route hop %d has an invalid PoolKey: %w", index+1, err)
		}
		if supplied := normalizePoolID(str(hop["poolId"])); supplied != "" && supplied != "0x" && !strings.EqualFold(supplied, normalizePoolID(poolID)) {
			return fmt.Errorf("route hop %d poolId does not match its PoolKey", index+1)
		}
		current = out
	}
	return nil
}

func (a *API) executeCheckToken(c *fiber.Ctx) error {
	d := body(c)
	batch, err := a.executeBatch(c.Context(), d)
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	line, err := a.Store.ExecuteLine(c.Context(), batch.LineID)
	if err != nil {
		return a.fail(c, err)
	}
	if !line.Enabled {
		return httpx.Error(c, 400, "execute line is disabled")
	}
	target := low(str(d["tokenAddress"]))
	if target == "" {
		target = low(str(d["token"]))
	}
	if !common.IsHexAddress(target) || target == executeZeroCurve {
		return httpx.Error(c, 400, "tokenAddress must be a valid non-zero EVM address")
	}
	forceCheck, _ := d["forceCheck"].(bool)
	if !forceCheck {
		if _, activeErr := a.Store.ActiveExecuteTokenByAddress(c.Context(), batch.EID, target); activeErr == nil {
			return httpx.Error(c, 409, "another execute batch is already buying this token")
		}
	}
	targetHop, pair, err := a.resolveExecutePool(c.Context(), target, d)
	if err != nil {
		return a.fail(c, err)
	}
	quote := targetHop.TokenIn
	buyHops := executeRoute(d["routeBuy"])
	if len(buyHops) == 0 {
		discovered, ok := a.discoverNativeRoute(c.Context(), 0, target, targetHop.PoolID, quote, targetHop)
		if !ok {
			return httpx.Error(c, 400, "no executable Uniswap V4 route from native ETH to the token quote currency")
		}
		for _, hop := range discovered {
			buyHops = append(buyHops, routeMap(hop))
		}
	}
	sellHops := executeRoute(d["routeSell"])
	if len(sellHops) == 0 {
		reverse := make([]map[string]any, len(buyHops))
		for i := range buyHops {
			item := map[string]any{}
			for key, value := range buyHops[len(buyHops)-1-i] {
				item[key] = value
			}
			item["tokenIn"], item["tokenOut"] = item["tokenOut"], item["tokenIn"]
			reverse[i] = item
		}
		sellHops = reverse
	}
	if len(buyHops) < 1 || len(buyHops) > 3 || len(sellHops) < 1 || len(sellHops) > 3 {
		return httpx.Error(c, 400, "Uniswap V4 route must contain between 1 and 3 hops")
	}
	if err := validateExecuteRoute(buyHops); err != nil {
		return a.fail(c, err)
	}
	if err := validateExecuteRoute(sellHops); err != nil {
		return a.fail(c, err)
	}
	metadata := map[string]any{"name": str(d["tokenName"]), "symbol": str(d["tokenSymbol"]), "pair": pair}
	if metadata["name"] == "" {
		metadata["name"] = str(pairValue(pair, "token_name", "name"))
	}
	if metadata["symbol"] == "" {
		metadata["symbol"] = str(pairValue(pair, "token_symbol", "symbol"))
	}
	status := "pending"
	if current, currentErr := a.Store.ExecuteTokenByPool(c.Context(), batch.EID, target, targetHop.PoolID); currentErr == nil {
		status = current.Status
		if status == "sell" || status == "end" {
			status = "pending"
		}
	}
	token := store.ExecuteToken{EID: batch.EID, LineID: batch.LineID, TokenAddress: target, PoolID: targetHop.PoolID, QuoteTokenAddress: quote, Currency0: targetHop.Currency0, Currency1: targetHop.Currency1, Fee: int(targetHop.Fee), TickSpacing: int(targetHop.TickSpacing), Hooks: targetHop.Hooks, RouteBuy: buyHops, RouteSell: sellHops, AMM: "uniswapv4", Status: status, Metadata: metadata}
	tracked, err := a.Store.UpsertExecuteToken(c.Context(), token)
	if err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, tracked.Map())
}

func executeSwapRequest(token store.ExecuteToken, route []map[string]any, privateKey, recipient, side string, amount, minimum *big.Int) (map[string]any, error) {
	if len(route) == 0 || len(route) > 3 {
		return nil, fmt.Errorf("execute route must contain between 1 and 3 hops")
	}
	first := route[0]
	currencyIn := executeNativeAlias(str(first["tokenIn"]))
	if currencyIn == "" {
		if side == "buy" {
			currencyIn = chain.NativeAddress
		} else {
			currencyIn = token.TokenAddress
		}
	}
	path := make([]any, 0, len(route))
	for _, hop := range route {
		out := executeNativeAlias(str(hop["tokenOut"]))
		if out == "" {
			return nil, fmt.Errorf("execute route has no tokenOut")
		}
		path = append(path, map[string]any{"intermediateCurrency": out, "fee": hop["fee"], "tickSpacing": hop["tickSpacing"], "hooks": hop["hooks"], "hookData": hop["hookData"]})
	}
	lastOut := executeNativeAlias(str(route[len(route)-1]["tokenOut"]))
	return map[string]any{"currencyIn": currencyIn, "path": path, "amountInRaw": amount.String(), "amountOutMinimumRaw": minimum.String(), "recipient": recipient, "privateKey": privateKey, "side": side, "wrapNative": side == "buy" && currencyIn == chain.WrappedNativeAddress, "unwrapNative": side == "sell" && (lastOut == chain.NativeAddress || lastOut == chain.WrappedNativeAddress)}, nil
}

func (a *API) executeTrade(ctx context.Context, token store.ExecuteToken, wallet store.ExecuteWallet, side, stage string, amount, minimum *big.Int) (map[string]any, error) {
	privateKey, err := secret.Decrypt(wallet.PrivateKeyEnc, a.Cfg.EncryptionKey)
	if err != nil {
		return nil, err
	}
	if side == "sell" && !strings.EqualFold(token.TokenAddress, chain.NativeAddress) {
		if err = a.Trading.PrepareV4SellApproval(ctx, privateKey, token.TokenAddress); err != nil {
			return nil, err
		}
	}
	route := token.RouteBuy
	if side == "sell" {
		route = token.RouteSell
	}
	request, err := executeSwapRequest(token, route, privateKey, wallet.Address, side, amount, minimum)
	if err != nil {
		return nil, err
	}
	tradeID, err := a.Store.RecordExecuteTrade(ctx, token.TID, token.EID, wallet.Address, side, stage, amount.String(), minimum.String())
	if err != nil {
		return nil, err
	}
	result, err := a.Trading.Swap(ctx, request)
	hash := ""
	if result != nil {
		hash = str(result["transactionHash"])
	}
	if err != nil {
		_ = a.Store.FinishExecuteTrade(ctx, tradeID, "failed", hash, err.Error())
		return nil, err
	}
	_ = a.Store.FinishExecuteTrade(ctx, tradeID, "confirmed", hash, "")
	return result, nil
}

func (a *API) executeSelectedWallets(ctx context.Context, batch store.ExecuteBatch, address string) ([]store.ExecuteWallet, error) {
	wallets, err := a.Store.ExecuteWallets(ctx, batch.EID, true)
	if err != nil {
		return nil, err
	}
	if address == "" {
		return wallets, nil
	}
	for _, wallet := range wallets {
		if strings.EqualFold(wallet.Address, address) {
			return []store.ExecuteWallet{wallet}, nil
		}
	}
	return nil, fmt.Errorf("execute wallet not found")
}

func (a *API) executeBuyData(ctx context.Context, d map[string]any) (map[string]any, error) {
	tokenID := id(d["tid"])
	if tokenID <= 0 {
		return nil, fmt.Errorf("tid is required")
	}
	token, err := a.Store.ExecuteToken(ctx, tokenID)
	if err != nil {
		return nil, fmt.Errorf("execute token not found")
	}
	batch, err := a.Store.ExecuteBatch(ctx, token.EID)
	if err != nil {
		return nil, err
	}
	stage := executeBuyStage(d["type"])
	if stage == "" {
		stage = "first"
	}
	amount := executePositiveRaw(d["amountInRaw"])
	if amount.Sign() == 0 {
		amount = executeNativeAmount(d["ethAmount"])
	}
	minimum := executePositiveRaw(d["amountOutMinimumRaw"])
	line, err := a.Store.ExecuteLine(ctx, batch.LineID)
	if err != nil {
		return nil, err
	}
	configItems := executeConfigItems(line.Config)
	wallets, err := a.executeSelectedWallets(ctx, batch, low(str(d["walletAddress"])))
	if err != nil || len(wallets) == 0 {
		return nil, fmt.Errorf("there are no active execute wallets")
	}
	type buyJob struct {
		wallet store.ExecuteWallet
		amount *big.Int
	}
	jobs := make([]buyJob, 0, len(wallets))
	for _, wallet := range wallets {
		if wallet.WalletIndex < 0 || wallet.WalletIndex >= len(configItems) {
			continue
		}
		stageAmount, enabled := executeConfigBuyAmount(configItems[wallet.WalletIndex], stage)
		if !enabled {
			continue
		}
		walletAmount := new(big.Int).Set(amount)
		if walletAmount.Sign() == 0 {
			walletAmount = stageAmount
		}
		if walletAmount.Sign() <= 0 {
			return nil, fmt.Errorf("walletConfig[%d].%sBuy.buyAmount must be a positive ETH amount", wallet.WalletIndex, stage)
		}
		jobs = append(jobs, buyJob{wallet: wallet, amount: walletAmount})
	}
	if len(jobs) == 0 {
		return nil, fmt.Errorf("there are no execute wallets enabled for %sBuy", stage)
	}
	results := make([]any, len(jobs))
	errs := make(chan error, len(jobs))
	var wait sync.WaitGroup
	for index, job := range jobs {
		index, job := index, job
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, tradeErr := a.executeTrade(ctx, token, job.wallet, "buy", stage, job.amount, minimum)
			if tradeErr != nil {
				errs <- fmt.Errorf("wallet %s: %w", job.wallet.Address, tradeErr)
				return
			}
			results[index] = map[string]any{"walletAddress": job.wallet.Address, "result": result}
		}()
	}
	wait.Wait()
	close(errs)
	for tradeErr := range errs {
		if tradeErr != nil {
			return nil, tradeErr
		}
	}
	if err := a.Store.UpdateExecuteTokenStatus(ctx, token.TID, "buy", stage); err != nil {
		return nil, err
	}
	return map[string]any{"tid": token.TID, "status": "buy", "buyStatus": stage, "results": results}, nil
}

func (a *API) executeBuy(c *fiber.Ctx) error {
	d := body(c)
	if values, ok := d["type"].([]any); ok {
		results := []any{}
		for _, value := range values {
			if executeBuyStage(value) == "all" {
				for _, stage := range []string{"first", "multi", "second", "third"} {
					copyBody := map[string]any{}
					for key, item := range d {
						copyBody[key] = item
					}
					copyBody["type"] = stage
					result, err := a.executeBuyData(c.Context(), copyBody)
					if err != nil {
						if strings.HasPrefix(err.Error(), "there are no execute wallets enabled for ") {
							continue
						}
						return a.fail(c, err)
					}
					results = append(results, result)
				}
				continue
			}
			copyBody := map[string]any{}
			for key, item := range d {
				copyBody[key] = item
			}
			copyBody["type"] = value
			result, err := a.executeBuyData(c.Context(), copyBody)
			if err != nil {
				return a.fail(c, err)
			}
			results = append(results, result)
		}
		return a.ok(c, results)
	}
	if executeBuyStage(d["type"]) == "all" {
		results := []any{}
		for _, stage := range []string{"first", "multi", "second", "third"} {
			copyBody := map[string]any{}
			for key, item := range d {
				copyBody[key] = item
			}
			copyBody["type"] = stage
			result, err := a.executeBuyData(c.Context(), copyBody)
			if err != nil {
				if strings.HasPrefix(err.Error(), "there are no execute wallets enabled for ") {
					continue
				}
				return a.fail(c, err)
			}
			results = append(results, result)
		}
		if len(results) == 0 {
			return a.fail(c, fmt.Errorf("there are no execute wallets enabled for any buy stage"))
		}
		return a.ok(c, results)
	}
	result, err := a.executeBuyData(c.Context(), d)
	if err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, result)
}

func (a *API) executeSellData(ctx context.Context, d map[string]any) (map[string]any, error) {
	tokenID := id(d["tid"])
	token, err := a.Store.ExecuteToken(ctx, tokenID)
	if err != nil {
		return nil, fmt.Errorf("execute token not found")
	}
	batch, err := a.Store.ExecuteBatch(ctx, token.EID)
	if err != nil {
		return nil, err
	}
	stage := executeBuyStage(d["type"])
	if stage == "" {
		stage = "all"
	}
	percent := new(big.Rat).SetInt64(100)
	if text := strings.TrimSpace(str(d["percent"])); text != "" {
		parsed, parsedOK := new(big.Rat).SetString(text)
		if !parsedOK {
			return nil, fmt.Errorf("percent must be between 1 and 100")
		}
		percent = parsed
	}
	if percent.Sign() <= 0 || percent.Cmp(big.NewRat(100, 1)) > 0 {
		return nil, fmt.Errorf("percent must be between 1 and 100")
	}
	minimum := executePositiveRaw(d["amountOutMinimumRaw"])
	line, err := a.Store.ExecuteLine(ctx, batch.LineID)
	if err != nil {
		return nil, err
	}
	configItems := executeConfigItems(line.Config)
	wallets, err := a.executeSelectedWallets(ctx, batch, low(str(d["walletAddress"])))
	if err != nil || len(wallets) == 0 {
		return nil, fmt.Errorf("there are no active execute wallets")
	}
	type sellJob struct {
		wallet store.ExecuteWallet
		amount *big.Int
	}
	jobs := make([]sellJob, 0, len(wallets))
	for _, wallet := range wallets {
		var configItem map[string]any
		if wallet.WalletIndex >= 0 && wallet.WalletIndex < len(configItems) {
			configItem = configItems[wallet.WalletIndex]
		}
		if stage != "all" && configItem == nil {
			continue
		}
		walletPercent, enabled := executeConfigSellPercent(configItem, stage, percent)
		if !enabled {
			continue
		}
		amount := executePositiveRaw(d["amountInRaw"])
		if amount.Sign() == 0 {
			balance, balanceErr := a.RPC.ERC20Balance(ctx, token.TokenAddress, wallet.Address)
			if balanceErr != nil {
				return nil, balanceErr
			}
			amount = executePercentAmount(balance, walletPercent)
		}
		if amount.Sign() == 0 {
			continue
		}
		jobs = append(jobs, sellJob{wallet: wallet, amount: amount})
	}
	if len(jobs) == 0 {
		return nil, fmt.Errorf("there are no execute wallets enabled for %sSell", stage)
	}
	results := make([]any, len(jobs))
	errs := make(chan error, len(jobs))
	var wait sync.WaitGroup
	for index, job := range jobs {
		index, job := index, job
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, tradeErr := a.executeTrade(ctx, token, job.wallet, "sell", stage, job.amount, minimum)
			if tradeErr != nil {
				errs <- fmt.Errorf("wallet %s: %w", job.wallet.Address, tradeErr)
				return
			}
			results[index] = map[string]any{"walletAddress": job.wallet.Address, "result": result}
		}()
	}
	wait.Wait()
	close(errs)
	for tradeErr := range errs {
		if tradeErr != nil {
			return nil, tradeErr
		}
	}
	status := token.Status
	if stage == "all" {
		status = "sell"
	}
	if err := a.Store.UpdateExecuteTokenStatus(ctx, token.TID, status, token.BuyStatus); err != nil {
		return nil, err
	}
	return map[string]any{"tid": token.TID, "status": status, "results": results}, nil
}

func (a *API) executeSell(c *fiber.Ctx) error {
	d := body(c)
	if values, ok := d["type"].([]any); ok {
		results := make([]any, 0, len(values))
		for _, value := range values {
			copyBody := make(map[string]any, len(d)+1)
			for key, item := range d {
				copyBody[key] = item
			}
			copyBody["type"] = value
			result, err := a.executeSellData(c.Context(), copyBody)
			if err != nil {
				return a.fail(c, err)
			}
			results = append(results, result)
		}
		return a.ok(c, results)
	}
	result, err := a.executeSellData(c.Context(), d)
	if err != nil {
		if err.Error() == "execute token not found" {
			return httpx.Error(c, 404, err.Error())
		}
		return a.fail(c, err)
	}
	return a.ok(c, result)
}

func (a *API) executeTokenList(c *fiber.Ctx) error {
	d := body(c)
	batch, err := a.executeBatch(c.Context(), d)
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	line, lineErr := a.Store.ExecuteLine(c.Context(), batch.LineID)
	if lineErr != nil {
		return a.fail(c, lineErr)
	}
	if strings.EqualFold(str(line.Config["sourceWeb"]), "ave") {
		list, listErr := a.executeAveTokenList(c.Context(), line, batch.EID)
		if listErr != nil {
			return a.fail(c, listErr)
		}
		return a.ok(c, list)
	}
	tokens, err := a.Store.ExecuteTokens(c.Context(), batch.EID)
	if err != nil {
		return a.fail(c, err)
	}
	result := make([]map[string]any, 0, len(tokens))
	for _, token := range tokens {
		result = append(result, token.Map())
	}
	return a.ok(c, result)
}

func (a *API) executeAveTokenList(ctx context.Context, line store.ExecuteLine, eid int64) ([]map[string]any, error) {
	if a.Cfg.AveBaseURL == "" {
		return nil, fmt.Errorf("AVE service is unavailable")
	}
	group, _ := line.Config["groupSort"].(map[string]any)
	category := str(group["category"])
	if category == "" {
		category = "pons_out_hot"
	}
	params := map[string]string{
		"chain":         "robinhood",
		"category":      category,
		"pageNO":        "1",
		"pageSize":      "500",
		"sort":          "created_at",
		"sort_dir":      "desc",
		"marketcap_min": str(group["mcp_min"]),
		"marketcap_max": str(group["mcp_max"]),
	}
	if params["marketcap_min"] == "" {
		params["marketcap_min"] = "20000"
	}
	if params["marketcap_max"] == "" {
		params["marketcap_max"] = "200000"
	}
	if days := id(group["create_day"]); days > 0 {
		params["created_at_min"] = strconv.FormatInt(time.Now().Unix()-days*86400, 10)
	}
	payload, err := a.aveJSON(ctx, "/v1api/v4/tokens/treasure/list", params)
	if err != nil {
		return nil, err
	}
	data, _ := payload["data"].(map[string]any)
	items, _ := data["data"].([]any)
	existing, err := a.Store.ExecuteTokens(ctx, eid)
	if err != nil {
		return nil, err
	}
	excluded := map[string]bool{}
	for _, token := range existing {
		if token.Status == "buy" || token.Status == "sell" {
			excluded[strings.ToLower(token.TokenAddress)] = true
		}
	}
	result := make([]map[string]any, 0, len(items))
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok || !isV4Pair(item) {
			continue
		}
		address := strings.ToLower(str(item["target_token"]))
		if address == "" || excluded[address] {
			continue
		}
		result = append(result, aveListView(item))
	}
	return result, nil
}

func (a *API) executeBuyTokens(c *fiber.Ctx) error {
	d := body(c)
	if tokenID := id(d["tid"]); tokenID > 0 {
		token, err := a.Store.ExecuteToken(c.Context(), tokenID)
		if err != nil {
			return httpx.Error(c, 404, "execute token not found")
		}
		return a.ok(c, token.Map())
	}
	batch, err := a.executeBatch(c.Context(), d)
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	tokens, err := a.Store.ExecuteTokens(c.Context(), batch.EID)
	if err != nil {
		return a.fail(c, err)
	}
	list := make([]map[string]any, 0, len(tokens))
	for _, token := range tokens {
		if token.Status == "pending" || token.Status == "buy" {
			list = append(list, token.Map())
		}
	}
	return a.ok(c, map[string]any{"list": list, "total": len(list)})
}

func (a *API) executeTokenAccounts(c *fiber.Ctx) error {
	d := body(c)
	batch, err := a.executeBatch(c.Context(), d)
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	tokenAddress := low(str(d["tokenAddress"]))
	if tokenAddress == "" {
		tokenAddress = low(str(d["token"]))
	}
	if !common.IsHexAddress(tokenAddress) || tokenAddress == executeZeroCurve {
		return httpx.Error(c, 400, "tokenAddress is required")
	}
	wallets, err := a.Store.ExecuteWallets(c.Context(), batch.EID, true)
	if err != nil {
		return a.fail(c, err)
	}
	accounts := make([]map[string]any, 0, len(wallets))
	for _, wallet := range wallets {
		balance, balanceErr := a.RPC.ERC20Balance(c.Context(), tokenAddress, wallet.Address)
		if balanceErr != nil {
			return a.fail(c, balanceErr)
		}
		accounts = append(accounts, map[string]any{
			"address":        wallet.Address,
			"tokenAddress":   tokenAddress,
			"balanceRaw":     balance.String(),
			"walletIndex":    wallet.WalletIndex,
			"executeBatch":   batch.EID,
			"balanceNonZero": balance.Sign() > 0,
		})
	}
	return a.ok(c, map[string]any{"eid": batch.EID, "tokenAddress": tokenAddress, "wallets": accounts})
}

func (a *API) executeDeleteToken(c *fiber.Ctx) error {
	tokenID := id(body(c)["tid"])
	if tokenID <= 0 {
		return httpx.Error(c, 400, "tid is required")
	}
	if err := a.Store.DeletePendingExecuteToken(c.Context(), tokenID); err != nil {
		return httpx.Error(c, 400, "only pending execute tokens can be deleted")
	}
	return a.ok(c, true)
}

func (a *API) executeEnd(c *fiber.Ctx) error {
	d := body(c)
	executeMu.Lock()
	defer executeMu.Unlock()
	batch, err := a.executeBatch(c.Context(), d)
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	wallets, err := a.Store.ExecuteWallets(c.Context(), batch.EID, false)
	if err != nil {
		return a.fail(c, err)
	}
	results := []any{}
	for _, wallet := range wallets {
		if strings.EqualFold(wallet.Address, batch.BossAddress) {
			continue
		}
		privateKey, keyErr := secret.Decrypt(wallet.PrivateKeyEnc, a.Cfg.EncryptionKey)
		if keyErr != nil {
			return a.fail(c, keyErr)
		}
		result, transferErr := a.Trading.SendNativeAll(c.Context(), privateKey, batch.BossAddress)
		if transferErr != nil {
			// Empty wallets are already recycled; retain the other results.
			if strings.Contains(strings.ToLower(transferErr.Error()), "balance") {
				continue
			}
			return a.fail(c, transferErr)
		}
		results = append(results, result)
	}
	return a.ok(c, map[string]any{"eid": batch.EID, "recycled": results})
}

func (a *API) executeWithdraw(c *fiber.Ctx) error {
	d := body(c)
	batch, err := a.executeBatch(c.Context(), d)
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	line, err := a.Store.ExecuteLine(c.Context(), batch.LineID)
	if err != nil {
		return a.fail(c, err)
	}
	to := low(str(d["withdrawAddress"]))
	if to == "" {
		to = low(str(line.Config["withdrawAddress"]))
	}
	if !common.IsHexAddress(to) {
		return httpx.Error(c, 400, "withdrawAddress is required")
	}
	privateKey, err := secret.Decrypt(batch.PrivateKeyEnc, a.Cfg.EncryptionKey)
	if err != nil {
		return a.fail(c, err)
	}
	amount := executePositiveRaw(d["amountRaw"])
	if amount.Sign() == 0 {
		amount = executeNativeAmount(d["amount"])
	}
	var result map[string]any
	if amount.Sign() > 0 {
		result, err = a.Trading.SendNative(c.Context(), privateKey, to, amount)
	} else {
		result, err = a.Trading.SendNativeAll(c.Context(), privateKey, to)
	}
	if err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, result)
}

func (a *API) executeNextWallet(c *fiber.Ctx) error {
	d := body(c)
	executeMu.Lock()
	defer executeMu.Unlock()
	batch, err := a.executeBatch(c.Context(), d)
	if err != nil {
		return httpx.Error(c, 404, "active execute batch not found")
	}
	address := low(str(d["walletAddress"]))
	wallets, err := a.executeSelectedWallets(c.Context(), batch, address)
	if err != nil || len(wallets) == 0 {
		return httpx.Error(c, 404, "execute wallet not found")
	}
	result := []map[string]any{}
	for _, old := range wallets {
		oldPrivate, decryptErr := secret.Decrypt(old.PrivateKeyEnc, a.Cfg.EncryptionKey)
		if decryptErr != nil {
			return a.fail(c, decryptErr)
		}
		key, keyErr := gethcrypto.GenerateKey()
		if keyErr != nil {
			return a.fail(c, keyErr)
		}
		private, privateErr := executePrivateKey("", key)
		if privateErr != nil {
			return a.fail(c, privateErr)
		}
		encrypted, encryptErr := secret.Encrypt(private, a.Cfg.EncryptionKey)
		if encryptErr != nil {
			return a.fail(c, encryptErr)
		}
		newAddress := strings.ToLower(gethcrypto.PubkeyToAddress(key.PublicKey).Hex())
		funding, fundingErr := a.Trading.SendNativeAll(c.Context(), oldPrivate, newAddress)
		if fundingErr != nil && !strings.Contains(strings.ToLower(fundingErr.Error()), "balance") {
			return a.fail(c, fundingErr)
		}
		newWallet, insertErr := a.Store.InsertExecuteWallet(c.Context(), store.ExecuteWallet{EID: batch.EID, WalletIndex: old.WalletIndex, Address: newAddress, PrivateKeyEnc: encrypted, Active: true})
		if insertErr != nil {
			return a.fail(c, insertErr)
		}
		if deactivateErr := a.Store.DeactivateExecuteWallet(c.Context(), batch.EID, old.Address); deactivateErr != nil {
			return a.fail(c, deactivateErr)
		}
		item := map[string]any{"oldAddress": old.Address, "wallet": executeWalletView(newWallet)}
		if funding != nil {
			item["funding"] = funding
		}
		result = append(result, item)
	}
	return a.ok(c, result)
}

func (a *API) executeAutoSwap(c *fiber.Ctx) error {
	d := body(c)
	lineID := executeLineID(d)
	line, err := a.Store.ExecuteLine(c.Context(), lineID)
	if err != nil {
		return httpx.Error(c, 404, "execute line not found")
	}
	value, hasValue := d["enabled"].(bool)
	if !hasValue {
		value, hasValue = d["status"].(bool)
	}
	if hasValue {
		line.Config["autoSwap"] = value
	}
	updated, err := a.Store.UpsertExecuteLine(c.Context(), line.LineID, 0, line.Name, line.Config, line.Enabled, line.BossAddress, line.BossPrivateKey)
	if err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, map[string]any{"line": updated.LineID, "autoSwap": updated.Config["autoSwap"]})
}

func (a *API) executeCloseAccounts(c *fiber.Ctx) error {
	// EVM ERC-20 balances are held directly by the wallet and do not need an
	// account-close operation. Keep the endpoint for workflow compatibility.
	return a.ok(c, map[string]any{"closed": 0, "unsupported": "EVM ERC-20 balances are not token accounts"})
}

func (a *API) executeRegisterRoutes(app *fiber.App) {
	g := app.Group("/api/v1/executerobin")
	g.Post("/start", a.executeStart)
	g.Post("/addLine", a.executeAddLine)
	g.Post("/updateLineData", a.executeLineUpdate)
	g.Post("/deleteLine", a.executeDeleteLine)
	g.Post("/generateWallets", a.executeGenerateWallets)
	g.Post("/getWallets", a.executeWallets)
	g.Post("/getWalletBalances", a.executeWallets)
	g.Post("/getBoss", a.executeBoss)
	g.Post("/getLines", a.executeLines)
	g.Post("/checkToken", a.executeCheckToken)
	g.Post("/buyToken", a.executeBuy)
	g.Post("/sellToken", a.executeSell)
	g.Post("/end", a.executeEnd)
	g.Post("/withdraw", a.executeWithdraw)
	g.Post("/nextWallet", a.executeNextWallet)
	g.Post("/closeAllAccounts", a.executeCloseAccounts)
	g.Post("/autoSwap", a.executeAutoSwap)
	g.Post("/getTokenAccounts", a.executeTokenAccounts)
	g.Post("/deleteToken", a.executeDeleteToken)
	g.Post("/getBuyTokes", a.executeBuyTokens)
	g.Post("/tokenList", a.executeTokenList)
	g.Post("/initTokenList", a.executeTokenList)
}
