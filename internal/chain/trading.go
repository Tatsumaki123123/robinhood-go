package chain

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	"math/big"
	"strings"
	"time"
)

type Trading struct {
	RPC     *RPC
	ChainID int64
	DryRun  bool
	Router  string
	Permit2 string
}

// BroadcastOptions controls a transaction that is submitted but whose receipt
// is reconciled by the caller. It is used by the strategy replacement state
// machine: replacements reuse the same nonce and increase both EIP-1559 fee
// caps by the requested percentage.
type BroadcastOptions struct {
	Nonce        *uint64
	FeeBumpBps   uint64
	Gas          uint64
	MaxFeePerGas *big.Int
	TipPerGas    *big.Int
}

type BroadcastResult struct {
	TransactionHash string
	Nonce           uint64
	From            string
	MaxFeePerGas    *big.Int
	TipPerGas       *big.Int
}

func (t *Trading) Quote(ctx context.Context, d map[string]any) (map[string]any, error) {
	if curve := fmt.Sprint(d["curveAddress"]); curve != "" && curve != "<nil>" {
		if !common.IsHexAddress(curve) {
			return nil, fmt.Errorf("curveAddress must be a valid EVM address")
		}
		recipient := fmt.Sprint(d["recipient"])
		if recipient == "" || recipient == "<nil>" {
			recipient = "0x0000000000000000000000000000000000000000"
		}
		if !common.IsHexAddress(recipient) {
			return nil, fmt.Errorf("recipient must be a valid EVM address")
		}
		state, err := t.RPC.PonsState(ctx, curve, recipient)
		if err != nil {
			return nil, err
		}
		amount := ToBig(d["amountRaw"])
		if amount.Sign() <= 0 {
			return nil, fmt.Errorf("amount must be a positive integer in raw token units")
		}
		side := strings.ToLower(fmt.Sprint(d["side"]))
		slip := int64(100)
		if v, ok := d["slippageBps"]; ok {
			slip = ToBig(v).Int64()
		}
		if slip < 0 || slip > 5000 {
			return nil, fmt.Errorf("slippageBps must be between 0 and 5000")
		}
		var out, spent, refund *big.Int
		clamped := false
		if side == "buy" {
			out, spent, refund, clamped = PonsBuyQuote(state, amount)
		} else if side == "sell" {
			out = PonsSellQuote(state, amount)
			spent = amount
			refund = big.NewInt(0)
		} else {
			return nil, fmt.Errorf("side must be buy or sell")
		}
		min := new(big.Int).Div(new(big.Int).Mul(out, new(big.Int).Sub(big.NewInt(10000), big.NewInt(slip))), big.NewInt(10000))
		return map[string]any{"side": side, "curveAddress": strings.ToLower(curve), "recipient": strings.ToLower(recipient), "pairToken": strings.ToLower(state.PairToken.Hex()), "amountInRaw": amount.String(), "amountOutRaw": out.String(), "minimumAmountOutRaw": min.String(), "spentRaw": spent.String(), "refundRaw": refund.String(), "clamped": clamped, "slippageBps": slip, "feeBps": state.FeeBps.String(), "creatorTaxBps": state.CreatorTaxBps.String(), "snipeTaxBps": state.SnipeTaxBps.String()}, nil
	}
	// Route quotes use an ordered path of intermediate currencies and pool
	// parameters.  Preserve the Node response shape even when no on-chain
	// quoter is configured; calldata construction is delegated to callers.
	if d["path"] != nil {
		if _, ok := d["currencyIn"]; ok {
			prepared, err := encodeV4Route(d)
			if err != nil {
				return nil, err
			}
			if t.Router != "" {
				prepared["router"] = t.Router
			}
			prepared["dryRun"] = t.DryRun
			return prepared, nil
		}
		path, ok := d["path"].([]any)
		if !ok {
			if p, ok2 := d["path"].([]map[string]any); ok2 {
				path = make([]any, len(p))
				for i := range p {
					path[i] = p[i]
				}
			}
		}
		poolIDs := make([]string, 0, len(path))
		for _, raw := range path {
			h, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			pool := map[string]any{"currency0": d["currencyIn"], "currency1": h["intermediateCurrency"], "fee": h["fee"], "tickSpacing": h["tickSpacing"], "hooks": h["hooks"]}
			if id, e := PoolID(pool); e == nil {
				poolIDs = append(poolIDs, id)
			}
			d["currencyIn"] = h["intermediateCurrency"]
		}
		return map[string]any{"mode": "calldata-preview", "router": d["router"], "poolManager": d["poolManager"], "poolIds": poolIDs, "hopCount": len(poolIDs), "amountInRaw": ToBig(d["amountInRaw"]).String(), "amountOutMinimumRaw": ToBig(d["amountOutMinimumRaw"]).String(), "recipient": d["recipient"], "deadline": d["deadline"], "valueRaw": ToBig(d["valueRaw"]).String(), "calldata": d["calldata"], "approvals": d["approvals"], "dryRun": t.DryRun}, nil
	}
	if d["tokenIn"] != nil && d["tokenOut"] != nil && d["currency0"] != nil {
		prepared, err := encodeV4Single(d)
		if err != nil {
			return nil, err
		}
		prepared["mode"] = "calldata-preview"
		prepared["dryRun"] = t.DryRun
		return prepared, nil
	}
	// Uniswap V4 quotes are deterministic calldata previews.  Compute the
	// canonical PoolId from PoolKey fields when supplied so callers can use the
	// result exactly like the Node service even when no RPC quote provider is
	// configured.
	poolID, err := PoolID(d)
	if err != nil {
		return nil, err
	}
	amountIn := ToBig(d["amountInRaw"])
	minOut := ToBig(d["amountOutMinimumRaw"])
	return map[string]any{"mode": "calldata-preview", "router": d["router"], "poolManager": d["poolManager"], "poolId": poolID, "tokenIn": strings.ToLower(fmt.Sprint(d["tokenIn"])), "tokenOut": strings.ToLower(fmt.Sprint(d["tokenOut"])), "amountInRaw": amountIn.String(), "amountOutMinimumRaw": minOut.String(), "recipient": strings.ToLower(fmt.Sprint(d["recipient"])), "deadline": d["deadline"], "valueRaw": ToBig(d["valueRaw"]).String(), "calldata": d["calldata"], "approvals": d["approvals"], "dryRun": t.DryRun, "request": d, "quotedAt": time.Now().UTC()}, nil
}

// PoolID implements keccak256(abi.encode(PoolKey)) for the V4 static PoolKey.
func PoolID(d map[string]any) (string, error) {
	c0, c1 := common.HexToAddress(fmt.Sprint(d["currency0"])), common.HexToAddress(fmt.Sprint(d["currency1"]))
	hooks := common.HexToAddress(fmt.Sprint(d["hooks"]))
	if str := strings.TrimSpace(fmt.Sprint(d["currency0"])); str == "" || strings.TrimSpace(fmt.Sprint(d["currency1"])) == "" || strings.TrimSpace(fmt.Sprint(d["hooks"])) == "" {
		return "", fmt.Errorf("currency0, currency1 and hooks are required")
	}
	args := abi.Arguments{}
	for _, typ := range []string{"address", "address", "uint24", "int24", "address"} {
		t, err := abi.NewType(typ, "", nil)
		if err != nil {
			return "", err
		}
		args = append(args, abi.Argument{Type: t})
	}
	b, err := args.Pack(c0, c1, uint64(ToBig(d["fee"]).Uint64()), ToBig(d["tickSpacing"]).Int64(), hooks)
	if err != nil {
		return "", err
	}
	return crypto.Keccak256Hash(b).Hex(), nil
}
func (t *Trading) Swap(ctx context.Context, d map[string]any) (map[string]any, error) {
	if d["tokenIn"] != nil || d["currencyIn"] != nil {
		if d["recipient"] == nil && d["privateKey"] != nil {
			if key, ke := gethcrypto.HexToECDSA(strings.TrimPrefix(fmt.Sprint(d["privateKey"]), "0x")); ke == nil {
				d["recipient"] = gethcrypto.PubkeyToAddress(key.PublicKey).Hex()
			}
		}
		var prepared map[string]any
		var err error
		if d["path"] != nil {
			prepared, err = encodeV4Route(d)
		} else {
			prepared, err = encodeV4Single(d)
		}
		if err != nil {
			return nil, err
		}
		if t.Router != "" {
			prepared["router"] = t.Router
		}
		if t.DryRun {
			prepared["mode"] = "dry-run"
			prepared["transactionHash"] = nil
			return prepared, nil
		}
		if d["to"] == nil {
			d["to"] = prepared["router"]
		}
		if d["data"] == nil {
			d["data"] = prepared["calldata"]
		}
		if d["value"] == nil {
			d["value"] = prepared["valueRaw"]
		}
	}
	if t.DryRun {
		return map[string]any{"status": "simulated", "dryRun": true, "request": d}, nil
	}
	to, _ := d["to"].(string)
	keyText := strings.TrimPrefix(fmt.Sprint(d["privateKey"]), "0x")
	if to == "" || keyText == "" {
		return nil, fmt.Errorf("to and privateKey are required when dry-run is disabled")
	}
	key, err := gethcrypto.HexToECDSA(keyText)
	if err != nil {
		return nil, fmt.Errorf("invalid privateKey: %w", err)
	}
	from := gethcrypto.PubkeyToAddress(key.PublicKey).Hex()
	var approvalHashes []string
	preparedToken := fmt.Sprint(d["tokenIn"])
	if preparedToken == "" || preparedToken == "<nil>" {
		preparedToken = fmt.Sprint(d["currencyIn"])
	}
	if preparedToken != "" && preparedToken != "<nil>" && !strings.EqualFold(preparedToken, NativeAddress) && !boolValue(d["wrapNative"]) {
		if hashes, ae := t.ensureV4Approvals(ctx, key, from, preparedToken, ToBig(d["amountInRaw"])); ae != nil {
			return nil, ae
		} else {
			approvalHashes = hashes
		}
	}
	nonce := uint64(0)
	if d["nonce"] == nil {
		nonce, err = t.RPC.Nonce(ctx, from)
	} else {
		nonce = uint64(ToBig(d["nonce"]).Uint64())
	}
	if err != nil {
		return nil, err
	}
	value := ToBig(d["value"])
	gas := uint64(0)
	if d["gas"] != nil {
		gas = ToBig(d["gas"]).Uint64()
	}
	if gas == 0 {
		gas = 700000
	}
	dataHex := strings.TrimPrefix(fmt.Sprint(d["data"]), "0x")
	data, _ := hex.DecodeString(dataHex)
	chainID := big.NewInt(t.ChainID)
	if d["maxFeePerGas"] != nil || d["maxPriorityFeePerGas"] != nil {
		tx := types.NewTx(&types.DynamicFeeTx{ChainID: chainID, Nonce: nonce, To: ptrAddress(to), Value: value, Gas: gas, GasFeeCap: ToBig(d["maxFeePerGas"]), GasTipCap: ToBig(d["maxPriorityFeePerGas"]), Data: data})
		signed, err := types.SignTx(tx, types.LatestSignerForChainID(chainID), key)
		if err != nil {
			return nil, err
		}
		raw, _ := signed.MarshalBinary()
		hash, err := t.RPC.SendRaw(ctx, "0x"+hex.EncodeToString(raw))
		if err != nil {
			return nil, err
		}
		receipt, re := t.waitReceipt(ctx, hash)
		if re != nil {
			return nil, re
		}
		result := map[string]any{"mode": "live", "status": "confirmed", "transactionHash": hash, "hash": hash, "from": from, "nonce": nonce, "receipt": receipt, "approvalTransactionHashes": approvalHashes}
		for k, v := range receiptDetails(receipt, d) {
			result[k] = v
		}
		return result, nil
	}
	gasPrice := ToBig(d["gasPrice"])
	if gasPrice.Sign() == 0 {
		if gp, ge := t.RPC.GasPrice(ctx); ge == nil {
			gasPrice = gp
		}
	}
	tx := types.NewTx(&types.LegacyTx{Nonce: nonce, To: ptrAddress(to), Value: value, GasPrice: gasPrice, Gas: gas, Data: data})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(chainID), key)
	if err != nil {
		return nil, err
	}
	raw, _ := signed.MarshalBinary()
	hash, err := t.RPC.SendRaw(ctx, "0x"+hex.EncodeToString(raw))
	if err != nil {
		return nil, err
	}
	receipt, re := t.waitReceipt(ctx, hash)
	if re != nil {
		return nil, re
	}
	result := map[string]any{"mode": "live", "status": "confirmed", "transactionHash": hash, "hash": hash, "from": from, "nonce": nonce, "receipt": receipt, "approvalTransactionHashes": approvalHashes}
	for k, v := range receiptDetails(receipt, d) {
		result[k] = v
	}
	return result, nil
}

// BroadcastV4 prepares and sends a V4 swap without waiting for its receipt.
// The returned nonce/hash can be persisted and later reconciled with
// WaitReceipt. This keeps the WSS event loop free while preserving the Node
// service's pending replacement semantics.
func (t *Trading) BroadcastV4(ctx context.Context, d map[string]any, opts BroadcastOptions) (BroadcastResult, error) {
	if t.DryRun {
		return BroadcastResult{}, fmt.Errorf("cannot broadcast in dry-run mode")
	}
	keyText := strings.TrimPrefix(fmt.Sprint(d["privateKey"]), "0x")
	if keyText == "" {
		return BroadcastResult{}, fmt.Errorf("privateKey is required")
	}
	key, err := gethcrypto.HexToECDSA(keyText)
	if err != nil {
		return BroadcastResult{}, fmt.Errorf("invalid privateKey: %w", err)
	}
	from := gethcrypto.PubkeyToAddress(key.PublicKey).Hex()
	inputToken := fmt.Sprint(d["tokenIn"])
	if inputToken == "" || inputToken == "<nil>" {
		inputToken = fmt.Sprint(d["currencyIn"])
	}
	if inputToken != "" && inputToken != "<nil>" && !strings.EqualFold(inputToken, NativeAddress) && !boolValue(d["wrapNative"]) {
		if _, err = t.ensureV4Approvals(ctx, key, from, inputToken, ToBig(d["amountInRaw"])); err != nil {
			return BroadcastResult{}, err
		}
	}
	var prepared map[string]any
	if d["path"] != nil {
		prepared, err = encodeV4Route(d)
	} else {
		prepared, err = encodeV4Single(d)
	}
	if err != nil {
		return BroadcastResult{}, err
	}
	to := fmt.Sprint(d["to"])
	if to == "" || to == "<nil>" {
		to = t.Router
		if to == "" {
			to = fmt.Sprint(prepared["router"])
		}
	}
	dataHex := strings.TrimPrefix(fmt.Sprint(d["data"]), "0x")
	if dataHex == "" || dataHex == "<nil>" {
		dataHex = strings.TrimPrefix(fmt.Sprint(prepared["calldata"]), "0x")
	}
	data, err := hex.DecodeString(dataHex)
	if err != nil {
		return BroadcastResult{}, err
	}
	value := ToBig(d["value"])
	if value.Sign() == 0 {
		value = ToBig(prepared["valueRaw"])
	}
	nonce := uint64(0)
	if opts.Nonce != nil {
		nonce = *opts.Nonce
	} else if d["nonce"] != nil {
		nonce = ToBig(d["nonce"]).Uint64()
	} else {
		nonce, err = t.RPC.Nonce(ctx, from)
		if err != nil {
			return BroadcastResult{}, err
		}
	}
	gas := opts.Gas
	if gas == 0 {
		gas = ToBig(d["gas"]).Uint64()
	}
	if gas == 0 {
		gas = 700000
	}
	base := opts.MaxFeePerGas
	if base == nil {
		base, err = t.RPC.GasPrice(ctx)
		if err != nil {
			return BroadcastResult{}, err
		}
	}
	tip := opts.TipPerGas
	if tip == nil {
		tip = new(big.Int).Div(new(big.Int).Set(base), big.NewInt(10))
	}
	if opts.FeeBumpBps > 0 {
		mul := func(x *big.Int) *big.Int {
			return new(big.Int).Div(new(big.Int).Add(new(big.Int).Mul(x, new(big.Int).SetUint64(10000+opts.FeeBumpBps)), big.NewInt(9999)), big.NewInt(10000))
		}
		base, tip = mul(base), mul(tip)
	}
	chainID := big.NewInt(t.ChainID)
	tx := types.NewTx(&types.DynamicFeeTx{ChainID: chainID, Nonce: nonce, To: ptrAddress(to), Value: value, Gas: gas, GasFeeCap: base, GasTipCap: tip, Data: data})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(chainID), key)
	if err != nil {
		return BroadcastResult{}, err
	}
	raw, _ := signed.MarshalBinary()
	hash, err := t.RPC.SendRaw(ctx, "0x"+hex.EncodeToString(raw))
	if err != nil {
		return BroadcastResult{}, err
	}
	return BroadcastResult{TransactionHash: hash, Nonce: nonce, From: from, MaxFeePerGas: new(big.Int).Set(base), TipPerGas: new(big.Int).Set(tip)}, nil
}

func (t *Trading) ensureV4Approvals(ctx context.Context, key *ecdsa.PrivateKey, owner, token string, amount *big.Int) ([]string, error) {
	permit2Address := Permit2Address
	if t.Permit2 != "" {
		permit2Address = t.Permit2
	}
	routerAddress := UniversalRouterAddress
	if t.Router != "" {
		routerAddress = t.Router
	}
	permit2 := common.HexToAddress(permit2Address)
	tokenAddr := common.HexToAddress(token)
	ownerAddr := common.HexToAddress(owner)
	router := common.HexToAddress(routerAddress)
	hashes := []string{}
	allowData, _ := erc20ABI.Pack("allowance", ownerAddr, permit2)
	raw, e := t.RPC.Call(ctx, "eth_call", []any{map[string]any{"to": tokenAddr.Hex(), "data": "0x" + fmt.Sprintf("%x", allowData)}, "latest"})
	if e != nil {
		return nil, e
	}
	allowance := unpackBig(raw)
	if allowance.Cmp(amount) < 0 {
		data, _ := erc20ABI.Pack("approve", permit2, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)))
		h, e := t.sendContract(ctx, key, tokenAddr, data, big.NewInt(0))
		if e != nil {
			return nil, e
		}
		hashes = append(hashes, h)
		if _, e = t.waitReceipt(ctx, h); e != nil {
			return nil, e
		}
	}
	permitData, _ := permit2ABI.Pack("allowance", ownerAddr, tokenAddr, router)
	raw, e = t.RPC.Call(ctx, "eth_call", []any{map[string]any{"to": permit2.Hex(), "data": "0x" + fmt.Sprintf("%x", permitData)}, "latest"})
	if e != nil {
		return nil, e
	}
	permitAllowance := unpackBig(raw)
	if permitAllowance.Cmp(amount) < 0 {
		max160 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 160), big.NewInt(1))
		max48 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 48), big.NewInt(1))
		data, _ := permit2ABI.Pack("approve", tokenAddr, router, max160, max48)
		h, e := t.sendContract(ctx, key, permit2, data, big.NewInt(0))
		if e != nil {
			return nil, e
		}
		hashes = append(hashes, h)
		if _, e = t.waitReceipt(ctx, h); e != nil {
			return nil, e
		}
	}
	return hashes, nil
}
func unpackBig(raw json.RawMessage) *big.Int {
	s := strings.TrimPrefix(strings.Trim(string(raw), `"`), "0x")
	if len(s) > 64 {
		s = s[:64]
	}
	n := new(big.Int)
	if _, ok := n.SetString(s, 16); !ok {
		return big.NewInt(0)
	}
	return n
}
func (t *Trading) sendContract(ctx context.Context, key *ecdsa.PrivateKey, to common.Address, data []byte, value *big.Int) (string, error) {
	from := gethcrypto.PubkeyToAddress(key.PublicKey).Hex()
	nonce, e := t.RPC.Nonce(ctx, from)
	if e != nil {
		return "", e
	}
	gasPrice, e := t.RPC.GasPrice(ctx)
	if e != nil {
		gasPrice = big.NewInt(1)
	}
	tx := types.NewTx(&types.LegacyTx{Nonce: nonce, To: &to, Value: value, GasPrice: gasPrice, Gas: 120000, Data: data})
	signed, e := types.SignTx(tx, types.LatestSignerForChainID(big.NewInt(t.ChainID)), key)
	if e != nil {
		return "", e
	}
	raw, _ := signed.MarshalBinary()
	return t.RPC.SendRaw(ctx, "0x"+hex.EncodeToString(raw))
}
func (t *Trading) waitReceipt(ctx context.Context, hash string) (any, error) {
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("transaction receipt timeout: %s", hash)
		case <-tick.C:
			r, e := t.RPC.Receipt(ctx, hash)
			if e == nil && r != nil {
				if m, ok := r.(map[string]any); ok && strings.EqualFold(fmt.Sprint(m["status"]), "0x0") {
					return nil, fmt.Errorf("transaction reverted: %s", hash)
				}
				return r, nil
			}
		}
	}
}

// WaitReceipt is the public reconciliation half of BroadcastV4.
func (t *Trading) WaitReceipt(ctx context.Context, hash string) (any, error) {
	return t.waitReceipt(ctx, hash)
}

// receiptOutput extracts the actual output of the V4 PoolManager Swap event
// when it is present. Missing/unrelated logs intentionally return nil; callers
// must not treat a broadcast or an approval receipt as a token fill.
func receiptOutput(receipt any, tokenOut string, req map[string]any) any {
	return receiptDetails(receipt, req)["actualAmountOutRaw"]
}

func receiptDetails(receipt any, req map[string]any) map[string]any {
	out := map[string]any{}
	tokenOut := strings.ToLower(fmt.Sprint(req["tokenOut"]))
	m, ok := receipt.(map[string]any)
	if !ok {
		return out
	}
	logs, ok := m["logs"].([]any)
	if !ok {
		return out
	}
	currencies := []string{}
	if currencyIn := fmt.Sprint(req["currencyIn"]); currencyIn != "" && currencyIn != "<nil>" {
		currencies = append(currencies, strings.ToLower(currencyIn))
		if path, ok := req["path"].([]any); ok {
			for _, raw := range path {
				if h, ok := raw.(map[string]any); ok {
					currencies = append(currencies, strings.ToLower(fmt.Sprint(h["intermediateCurrency"])))
				}
			}
		}
	} else if in := fmt.Sprint(req["tokenIn"]); in != "" && in != "<nil>" {
		currencies = []string{strings.ToLower(in), strings.ToLower(fmt.Sprint(req["tokenOut"]))}
	}
	seen := 0
	for _, raw := range logs {
		lm, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		topics, ok := lm["topics"].([]any)
		if !ok || len(topics) == 0 {
			continue
		}
		if !strings.EqualFold(fmt.Sprint(topics[0]), V4SwapTopic) {
			continue
		}
		data := strings.TrimPrefix(fmt.Sprint(lm["data"]), "0x")
		if len(data) < 128 {
			continue
		}
		a0 := signedWord(data, 0)
		a1 := signedWord(data, 1)
		if len(currencies) >= 2 && seen < len(currencies)-1 {
			in, outCurrency := currencies[seen], currencies[seen+1]
			c0, c1 := in, outCurrency
			if c1 < c0 {
				c0, c1 = c1, c0
			}
			var inAmount, outAmount *big.Int
			if in == c0 {
				inAmount, outAmount = new(big.Int).Set(a0), new(big.Int).Set(a1)
			} else {
				inAmount, outAmount = new(big.Int).Set(a1), new(big.Int).Set(a0)
			}
			if inAmount.Sign() < 0 {
				inAmount.Neg(inAmount)
			}
			if outAmount.Sign() < 0 {
				outAmount.Neg(outAmount)
			}
			if seen == 0 {
				out["firstHopAmountInRaw"], out["firstHopAmountOutRaw"] = inAmount.String(), outAmount.String()
			}
			out["lastHopAmountInRaw"], out["lastHopAmountOutRaw"] = inAmount.String(), outAmount.String()
			out["actualAmountOutRaw"] = outAmount.String()
			seen++
			continue
		}
		if a0.Sign() < 0 {
			a0.Neg(a0)
		}
		if a1.Sign() < 0 {
			a1.Neg(a1)
		}
		if strings.EqualFold(tokenOut, strings.ToLower(fmt.Sprint(req["currency0"]))) {
			out["actualAmountOutRaw"] = a0.String()
		} else {
			out["actualAmountOutRaw"] = a1.String()
		}
		break
	}
	return out
}
func ptrAddress(s string) *common.Address { a := common.HexToAddress(s); return &a }
func ToBig(v any) *big.Int {
	if v == nil {
		return big.NewInt(0)
	}
	switch x := v.(type) {
	case *big.Int:
		if x == nil {
			return big.NewInt(0)
		}
		return new(big.Int).Set(x)
	case big.Int:
		return new(big.Int).Set(&x)
	case string:
		n := new(big.Int)
		if strings.HasPrefix(x, "0x") {
			if _, ok := n.SetString(x[2:], 16); !ok {
				return big.NewInt(0)
			}
		} else {
			if _, ok := n.SetString(x, 10); !ok {
				return big.NewInt(0)
			}
		}
		return n
	case json.Number:
		n := new(big.Int)
		if _, ok := n.SetString(string(x), 10); !ok {
			return big.NewInt(0)
		}
		return n
	case float64:
		return big.NewInt(int64(x))
	case int:
		return big.NewInt(int64(x))
	case int64:
		return big.NewInt(x)
	case uint:
		return new(big.Int).SetUint64(uint64(x))
	case uint32:
		return new(big.Int).SetUint64(uint64(x))
	case uint64:
		return new(big.Int).SetUint64(x)
	}
	return big.NewInt(0)
}
