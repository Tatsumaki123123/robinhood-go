package chain

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	"math/big"
	"reflect"
	"strings"
	"sync"
)

var ponsABI = mustABI(`[
 {"type":"function","name":"getReserves","stateMutability":"view","inputs":[],"outputs":[{"type":"uint256"},{"type":"uint256"}]},
 {"type":"function","name":"sellableTokens","stateMutability":"view","inputs":[],"outputs":[{"type":"uint256"}]},
 {"type":"function","name":"feeBps","stateMutability":"view","inputs":[],"outputs":[{"type":"uint256"}]},
 {"type":"function","name":"creatorTaxBps","stateMutability":"view","inputs":[],"outputs":[{"type":"uint256"}]},
 {"type":"function","name":"currentSnipeTaxBps","stateMutability":"view","inputs":[{"type":"address"}],"outputs":[{"type":"uint256"}]},
 {"type":"function","name":"pairToken","stateMutability":"view","inputs":[],"outputs":[{"type":"address"}]},
 {"type":"function","name":"buy","stateMutability":"payable","inputs":[{"type":"uint256"},{"type":"uint256"},{"type":"address"}],"outputs":[{"type":"uint256"}]},
 {"type":"function","name":"sell","stateMutability":"nonpayable","inputs":[{"type":"uint256"},{"type":"uint256"},{"type":"address"}],"outputs":[{"type":"uint256"}]}
]`)

var ponsFactoryABI = mustABI(`[
 {"type":"function","name":"getLaunchedToken","stateMutability":"view","inputs":[{"type":"address"}],"outputs":[{"type":"tuple","components":[{"name":"token","type":"address"},{"name":"curve","type":"address"},{"name":"deployer","type":"address"},{"name":"creatorFeeRecipient","type":"address"},{"name":"pairToken","type":"address"},{"name":"graduationThreshold","type":"uint256"},{"name":"poolFee","type":"uint24"},{"name":"tickSpacing","type":"int24"},{"name":"creatorTaxBps","type":"uint16"},{"name":"buybackEnabled","type":"bool"},{"name":"phase","type":"uint8"},{"name":"sweptQuote","type":"uint256"},{"name":"sweptTokens","type":"uint256"},{"name":"sweptAt","type":"uint256"},{"name":"exists","type":"bool"}]}]}
]`)

type LaunchedToken struct {
	Token, Curve, Deployer, CreatorFeeRecipient, PairToken   common.Address
	GraduationThreshold, PoolFee, TickSpacing, CreatorTaxBps *big.Int
	Exists                                                   bool
}

func (r *RPC) LaunchedToken(ctx context.Context, factory, token string) (LaunchedToken, error) {
	data, err := ponsFactoryABI.Pack("getLaunchedToken", common.HexToAddress(token))
	if err != nil {
		return LaunchedToken{}, err
	}
	raw, err := r.Call(ctx, "eth_call", []any{map[string]any{"to": common.HexToAddress(factory).Hex(), "data": "0x" + fmt.Sprintf("%x", data)}, "latest"})
	if err != nil {
		return LaunchedToken{}, err
	}
	b := common.FromHex(strings.Trim(string(raw), `"`))
	vals, err := ponsFactoryABI.Unpack("getLaunchedToken", b)
	if err != nil {
		return LaunchedToken{}, err
	}
	if len(vals) == 0 {
		return LaunchedToken{}, fmt.Errorf("empty getLaunchedToken result")
	}
	// go-ethereum returns a generated tuple struct; use reflection so this
	// remains compatible across go-ethereum minor versions.
	v := reflect.ValueOf(vals[0])
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	get := func(name string) reflect.Value {
		if v.Kind() == reflect.Struct {
			return v.FieldByName(name)
		}
		return reflect.Value{}
	}
	addr := func(name string) common.Address {
		x := get(name)
		if x.IsValid() {
			if a, ok := x.Interface().(common.Address); ok {
				return a
			}
		}
		return common.Address{}
	}
	num := func(name string) *big.Int {
		x := get(name)
		if x.IsValid() {
			if n, ok := x.Interface().(*big.Int); ok {
				return new(big.Int).Set(n)
			}
			if n, ok := x.Interface().(big.Int); ok {
				return new(big.Int).Set(&n)
			}
			switch x.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				return new(big.Int).SetUint64(x.Uint())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				return big.NewInt(x.Int())
			}
		}
		return big.NewInt(0)
	}
	exists := false
	x := get("Exists")
	if x.IsValid() && x.Kind() == reflect.Bool {
		exists = x.Bool()
	}
	return LaunchedToken{Token: addr("Token"), Curve: addr("Curve"), Deployer: addr("Deployer"), CreatorFeeRecipient: addr("CreatorFeeRecipient"), PairToken: addr("PairToken"), GraduationThreshold: num("GraduationThreshold"), PoolFee: num("PoolFee"), TickSpacing: num("TickSpacing"), CreatorTaxBps: num("CreatorTaxBps"), Exists: exists}, nil
}

func mustABI(s string) abi.ABI {
	a, err := abi.JSON(strings.NewReader(s))
	if err != nil {
		panic(err)
	}
	return a
}

type PonsState struct {
	QuoteReserve, TokenReserve, SellableTokens, FeeBps, CreatorTaxBps, SnipeTaxBps *big.Int
	PairToken                                                                      common.Address
}

type ponsPreparedTrade struct {
	curve, token, recipient string
	state                   PonsState
	amount, out, spent      *big.Int
	refund, minimum         *big.Int
	buy                     bool
	clamped                 bool
	data                    []byte
}

func (r *RPC) PonsState(ctx context.Context, curve, recipient string) (PonsState, error) {
	addr := common.HexToAddress(curve)
	who := common.HexToAddress(recipient)
	call := func(name string, args ...any) ([]any, error) {
		data, e := ponsABI.Pack(name, args...)
		if e != nil {
			return nil, e
		}
		raw, e := r.Call(ctx, "eth_call", []any{map[string]any{"to": addr.Hex(), "data": "0x" + fmt.Sprintf("%x", data)}, "latest"})
		if e != nil {
			return nil, e
		}
		b := strings.TrimPrefix(strings.Trim(string(raw), `"`), "0x")
		out, e := ponsABI.Unpack(name, common.FromHex("0x"+b))
		return out, e
	}
	// These reads are independent. Run them concurrently so the quote path is
	// bounded by one RPC round trip instead of six sequential round trips.
	type callSpec struct {
		name string
		args []any
	}
	type callResult struct {
		name string
		vals []any
		err  error
	}
	specs := []callSpec{
		{name: "getReserves"},
		{name: "sellableTokens"},
		{name: "feeBps"},
		{name: "creatorTaxBps"},
		{name: "currentSnipeTaxBps", args: []any{who}},
		{name: "pairToken"},
	}
	results := make(chan callResult, len(specs))
	var wg sync.WaitGroup
	for _, spec := range specs {
		spec := spec
		wg.Add(1)
		go func() {
			defer wg.Done()
			vals, err := call(spec.name, spec.args...)
			results <- callResult{name: spec.name, vals: vals, err: err}
		}()
	}
	wg.Wait()
	close(results)
	values := make(map[string][]any, len(specs))
	for result := range results {
		if result.err != nil {
			return PonsState{}, fmt.Errorf("pons %s: %w", result.name, result.err)
		}
		values[result.name] = result.vals
	}
	if len(values["getReserves"]) < 2 || len(values["sellableTokens"]) < 1 || len(values["feeBps"]) < 1 || len(values["creatorTaxBps"]) < 1 || len(values["currentSnipeTaxBps"]) < 1 || len(values["pairToken"]) < 1 {
		return PonsState{}, fmt.Errorf("pons state returned incomplete data")
	}
	rr := values["getReserves"]
	sell := values["sellableTokens"]
	fee := values["feeBps"]
	creator := values["creatorTaxBps"]
	snipe := values["currentSnipeTaxBps"]
	pair := values["pairToken"]
	asBig := func(v any) *big.Int {
		if x, ok := v.(*big.Int); ok {
			return x
		}
		return big.NewInt(0)
	}
	p := common.Address{}
	if x, ok := pair[0].(common.Address); ok {
		p = x
	}
	return PonsState{asBig(rr[0]), asBig(rr[1]), asBig(sell[0]), asBig(fee[0]), asBig(creator[0]), asBig(snipe[0]), p}, nil
}
func ponsAmountOut(in, resIn, resOut *big.Int) *big.Int {
	if in.Sign() <= 0 || resOut.Sign() <= 0 {
		return big.NewInt(0)
	}
	return new(big.Int).Div(new(big.Int).Mul(in, resOut), new(big.Int).Add(resIn, in))
}
func PonsBuyQuote(s PonsState, input *big.Int) (out, spent, refund *big.Int, clamped bool) {
	if input == nil || input.Sign() < 0 {
		return big.NewInt(0), big.NewInt(0), big.NewInt(0), false
	}
	bps := big.NewInt(10000)
	sn := new(big.Int).Set(s.SnipeTaxBps)
	max := new(big.Int).Sub(bps, new(big.Int).Add(s.FeeBps, s.CreatorTaxBps))
	max.Sub(max, big.NewInt(100))
	if max.Sign() < 0 {
		max.SetInt64(0)
	}
	if sn.Cmp(max) > 0 {
		sn = max
	}
	total := new(big.Int).Add(new(big.Int).Add(s.FeeBps, s.CreatorTaxBps), sn)
	net := new(big.Int).Sub(input, new(big.Int).Div(new(big.Int).Mul(input, total), bps))
	out = ponsAmountOut(net, s.QuoteReserve, s.TokenReserve)
	spent = new(big.Int).Set(input)
	if out.Cmp(s.SellableTokens) > 0 {
		clamped = true
		out = new(big.Int).Set(s.SellableTokens)
		n := new(big.Int).Add(new(big.Int).Div(new(big.Int).Mul(out, s.QuoteReserve), new(big.Int).Sub(s.TokenReserve, out)), big.NewInt(1))
		// The contract applies the taxes to the gross input.  Recover the
		// smallest gross amount that yields the required net input using
		// ceil(netInput * 10000 / (10000-totalBps)), matching the Node
		// implementation exactly.
		denom := new(big.Int).Sub(bps, total)
		spent = new(big.Int).Div(new(big.Int).Add(new(big.Int).Mul(n, bps), new(big.Int).Sub(denom, big.NewInt(1))), denom)
		if spent.Cmp(input) > 0 {
			spent = input
		}
	}
	refund = new(big.Int).Sub(input, spent)
	return
}
func PonsSellQuote(s PonsState, input *big.Int) *big.Int {
	gross := ponsAmountOut(input, s.TokenReserve, s.QuoteReserve)
	fee := new(big.Int).Div(new(big.Int).Mul(gross, s.FeeBps), big.NewInt(10000))
	tax := new(big.Int).Div(new(big.Int).Mul(gross, s.CreatorTaxBps), big.NewInt(10000))
	return new(big.Int).Sub(gross, new(big.Int).Add(fee, tax))
}
func (r *RPC) PackPonsBuy(input, minOut *big.Int, recipient string) ([]byte, error) {
	return ponsABI.Pack("buy", input, minOut, common.HexToAddress(recipient))
}
func (r *RPC) PackPonsSell(input, minOut *big.Int, recipient string) ([]byte, error) {
	return ponsABI.Pack("sell", input, minOut, common.HexToAddress(recipient))
}

func (t *Trading) preparePonsTrade(ctx context.Context, d map[string]any, buy bool) (ponsPreparedTrade, error) {
	curve := fmt.Sprint(d["curveAddress"])
	recipient := fmt.Sprint(d["recipient"])
	if recipient == "" || recipient == "<nil>" {
		recipient = fmt.Sprint(d["to"])
	}
	if recipient == "" || recipient == "<nil>" {
		return ponsPreparedTrade{}, fmt.Errorf("recipient is required")
	}
	amount := ToBig(d["amountRaw"])
	if amount.Sign() <= 0 {
		return ponsPreparedTrade{}, fmt.Errorf("amountRaw must be positive")
	}
	state, err := t.RPC.PonsState(ctx, curve, recipient)
	if err != nil {
		return ponsPreparedTrade{}, err
	}
	slip := ToBig(d["slippageBps"])
	if _, supplied := d["slippageBps"]; !supplied {
		slip = big.NewInt(100)
	}
	if slip.Sign() < 0 || slip.Cmp(big.NewInt(5000)) > 0 {
		return ponsPreparedTrade{}, fmt.Errorf("slippageBps must be between 0 and 5000")
	}
	var out, spent, refund *big.Int
	var clamped bool
	if buy {
		out, spent, refund, clamped = PonsBuyQuote(state, amount)
	} else {
		out, spent, refund = PonsSellQuote(state, amount), amount, big.NewInt(0)
	}
	min := new(big.Int).Div(new(big.Int).Mul(out, new(big.Int).Sub(big.NewInt(10000), slip)), big.NewInt(10000))
	var data []byte
	if buy {
		data, err = t.RPC.PackPonsBuy(amount, min, recipient)
	} else {
		data, err = t.RPC.PackPonsSell(amount, min, recipient)
	}
	if err != nil {
		return ponsPreparedTrade{}, err
	}
	return ponsPreparedTrade{curve: curve, token: fmt.Sprint(d["tokenAddress"]), recipient: recipient, state: state, amount: amount, out: out, spent: spent, refund: refund, minimum: min, buy: buy, clamped: clamped, data: data}, nil
}

func (t *Trading) broadcastPonsTrade(ctx context.Context, prepared ponsPreparedTrade, key *ecdsa.PrivateKey) (BroadcastResult, error) {
	from := gethcrypto.PubkeyToAddress(key.PublicKey).Hex()
	unlockWallet := t.lockWallet(from)
	defer unlockWallet()
	nonce, err := t.reserveNonce(ctx, from, nil)
	if err != nil {
		return BroadcastResult{}, err
	}
	gasPrice, err := t.gasPrice(ctx)
	if err != nil {
		t.invalidateNonce(from)
		return BroadcastResult{}, err
	}
	value := big.NewInt(0)
	if prepared.buy {
		value = new(big.Int).Set(prepared.amount)
	}
	tx := types.NewTx(&types.LegacyTx{Nonce: nonce, To: ptrAddress(prepared.curve), Value: value, GasPrice: gasPrice, Gas: 700000, Data: prepared.data})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(big.NewInt(t.ChainID)), key)
	if err != nil {
		t.invalidateNonce(from)
		return BroadcastResult{}, err
	}
	rawTx, err := signed.MarshalBinary()
	if err != nil {
		t.invalidateNonce(from)
		return BroadcastResult{}, err
	}
	hash, err := t.RPC.SendRaw(ctx, "0x"+hex.EncodeToString(rawTx))
	if err != nil {
		t.invalidateNonce(from)
		return BroadcastResult{}, err
	}
	return BroadcastResult{TransactionHash: hash, Nonce: nonce, From: from, MaxFeePerGas: new(big.Int).Set(gasPrice), TipPerGas: new(big.Int).Set(gasPrice)}, nil
}

// BroadcastPons signs and submits a curve buy/sell without waiting for a
// receipt. Strategy execution uses this fast path and reconciles the fill from
// the own transaction event, just like the V4 broadcast path.
func (t *Trading) BroadcastPons(ctx context.Context, d map[string]any, buy bool) (BroadcastResult, error) {
	prepared, err := t.preparePonsTrade(ctx, d, buy)
	if err != nil {
		return BroadcastResult{}, err
	}
	keyText := strings.TrimPrefix(fmt.Sprint(d["privateKey"]), "0x")
	if keyText == "" {
		return BroadcastResult{}, fmt.Errorf("privateKey is required")
	}
	key, err := gethcrypto.HexToECDSA(keyText)
	if err != nil {
		return BroadcastResult{}, err
	}
	return t.broadcastPonsTrade(ctx, prepared, key)
}

// PonsSwap signs and submits a curve buy/sell. It retains the API's confirmed
// response shape; strategy code should use BroadcastPons to avoid waiting.
func (t *Trading) PonsSwap(ctx context.Context, d map[string]any, buy bool) (map[string]any, error) {
	prepared, err := t.preparePonsTrade(ctx, d, buy)
	if err != nil {
		return nil, err
	}
	keyText := strings.TrimPrefix(fmt.Sprint(d["privateKey"]), "0x")
	if keyText == "" {
		return map[string]any{"mode": "calldata-preview", "side": map[bool]string{true: "buy", false: "sell"}[buy], "curveAddress": strings.ToLower(prepared.curve), "tokenAddress": strings.ToLower(prepared.token), "pairToken": strings.ToLower(prepared.state.PairToken.Hex()), "quoteInRaw": map[bool]string{true: prepared.amount.String(), false: ""}[buy], "tokensInRaw": map[bool]string{true: "", false: prepared.amount.String()}[buy], "expectedTokensOutRaw": map[bool]string{true: prepared.out.String(), false: ""}[buy], "expectedQuoteOutRaw": map[bool]string{true: "", false: prepared.out.String()}[buy], "minimumAmountOutRaw": prepared.minimum.String(), "spentRaw": prepared.spent.String(), "refundRaw": prepared.refund.String(), "clamped": prepared.clamped, "transactionHash": nil, "to": prepared.curve, "data": "0x" + hex.EncodeToString(prepared.data)}, nil
	}
	key, err := gethcrypto.HexToECDSA(keyText)
	if err != nil {
		return nil, err
	}
	broadcast, err := t.broadcastPonsTrade(ctx, prepared, key)
	if err != nil {
		return nil, err
	}
	receipt, err := t.waitReceipt(ctx, broadcast.TransactionHash)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"mode": "live", "status": "confirmed", "side": map[bool]string{true: "buy", false: "sell"}[buy], "curveAddress": strings.ToLower(prepared.curve), "tokenAddress": strings.ToLower(prepared.token), "pairToken": strings.ToLower(prepared.state.PairToken.Hex()), "transactionHash": broadcast.TransactionHash, "hash": broadcast.TransactionHash, "from": broadcast.From, "nonce": broadcast.Nonce, "expectedAmountOutRaw": prepared.out.String(), "minimumAmountOutRaw": prepared.minimum.String(), "receipt": receipt}
	if fill := ponsReceiptFill(receipt, prepared.curve, buy); fill != nil {
		result["actualAmountOutRaw"] = fill["actualAmountOutRaw"]
		result["actualQuoteAmountRaw"] = fill["quoteAmountRaw"]
		result["actualTokenAmountRaw"] = fill["tokenAmountRaw"]
	}
	if m, ok := receipt.(map[string]any); ok {
		result["gasUsed"] = m["gasUsed"]
		result["effectiveGasPrice"] = m["effectiveGasPrice"]
	}
	return result, nil
}

// PreparePonsSellApproval pre-authorizes the curve's pair token after a
// confirmed buy.
func (t *Trading) PreparePonsSellApproval(ctx context.Context, privateKey, owner, curve string) error {
	keyText := strings.TrimPrefix(strings.TrimSpace(privateKey), "0x")
	if keyText == "" {
		return fmt.Errorf("privateKey is required")
	}
	key, err := gethcrypto.HexToECDSA(keyText)
	if err != nil {
		return fmt.Errorf("invalid privateKey: %w", err)
	}
	state, err := t.RPC.PonsState(ctx, curve, owner)
	if err != nil {
		return err
	}
	if state.PairToken == (common.Address{}) {
		return fmt.Errorf("pons pair token is missing")
	}
	_, err = t.approvePonsMax(ctx, key, curve, state.PairToken)
	return err
}

func (t *Trading) approvePonsMax(ctx context.Context, key *ecdsa.PrivateKey, curve string, pairToken common.Address) (string, error) {
	approveData, _ := erc20ABI.Pack("approve", common.HexToAddress(curve), new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)))
	approval, err := t.sendContract(ctx, key, pairToken, approveData, big.NewInt(0))
	if err != nil {
		return "", err
	}
	if _, err = t.waitReceipt(ctx, approval); err != nil {
		return "", err
	}
	return approval, nil
}

// ponsReceiptFill extracts the event amounts from a confirmed curve receipt.
// Quote/token values are taken from the contract event, so accounting never
// relies on the pre-trade quote when a transaction was partially clamped.
func ponsReceiptFill(receipt any, curve string, buy bool) map[string]any {
	m, ok := receipt.(map[string]any)
	if !ok {
		return nil
	}
	logs, ok := m["logs"].([]any)
	if !ok {
		return nil
	}
	want := "buy"
	if !buy {
		want = "sell"
	}
	for _, raw := range logs {
		lm, ok := raw.(map[string]any)
		if !ok || !strings.EqualFold(fmt.Sprint(lm["address"]), curve) {
			continue
		}
		decoded := decodeChainEvent(lm)
		if fmt.Sprint(decoded["side"]) != want {
			continue
		}
		quote, token := fmt.Sprint(decoded["quoteAmountRaw"]), fmt.Sprint(decoded["tokenAmountRaw"])
		if quote == "" || token == "" {
			continue
		}
		out := token
		if !buy {
			out = quote
		}
		return map[string]any{"actualAmountOutRaw": out, "quoteAmountRaw": quote, "tokenAmountRaw": token}
	}
	return nil
}
