package chain

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

const (
	UniversalRouterAddress = "0x8876789976decbfcbbbe364623c63652db8c0904"
	AddressThis            = "0x0000000000000000000000000000000000000002"
	NativeAddress          = "0x0000000000000000000000000000000000000000"
	V4SwapCommand          = byte(0x10)
	WrapETHCommand         = byte(0x0b)
	UnwrapWETHCommand      = byte(0x0c)
	SwapExactInSingle      = byte(0x06)
	SwapExactIn            = byte(0x07)
	Settle                 = byte(0x0b)
	SettleAll              = byte(0x0c)
	Take                   = byte(0x0e)
	TakeAll                = byte(0x0f)
)

type V4PoolKey struct {
	Currency0   common.Address
	Currency1   common.Address
	Fee         *big.Int
	TickSpacing *big.Int
	Hooks       common.Address
}
type V4PathKey struct {
	IntermediateCurrency common.Address
	Fee                  *big.Int
	TickSpacing          *big.Int
	Hooks                common.Address
	HookData             []byte
}
type V4ExactInputSingle struct {
	PoolKey          V4PoolKey
	ZeroForOne       bool
	AmountIn         *big.Int
	AmountOutMinimum *big.Int
	MinHopPriceX36   *big.Int
	HookData         []byte
}
type V4ExactInput struct {
	CurrencyIn       common.Address
	Path             []V4PathKey
	MinHopPriceX36   []*big.Int
	AmountIn         *big.Int
	AmountOutMinimum *big.Int
}

var universalRouterABI = mustABI(`[ {"type":"function","name":"execute","stateMutability":"payable","inputs":[{"type":"bytes"},{"type":"bytes[]"},{"type":"uint256"}],"outputs":[]} ]`)
var erc20ABI = mustABI(`[ {"type":"function","name":"allowance","stateMutability":"view","inputs":[{"type":"address"},{"type":"address"}],"outputs":[{"type":"uint256"}]}, {"type":"function","name":"approve","stateMutability":"nonpayable","inputs":[{"type":"address"},{"type":"uint256"}],"outputs":[{"type":"bool"}]} ]`)
var permit2ABI = mustABI(`[ {"type":"function","name":"allowance","stateMutability":"view","inputs":[{"type":"address"},{"type":"address"},{"type":"address"}],"outputs":[{"type":"uint160"},{"type":"uint48"},{"type":"uint48"}]}, {"type":"function","name":"approve","stateMutability":"nonpayable","inputs":[{"type":"address"},{"type":"address"},{"type":"uint160"},{"type":"uint48"}],"outputs":[]} ]`)

const Permit2Address = "0x000000000022d473030f116ddee9f6b43ac78ba3"

func v4Types() (abi.Type, abi.Type, abi.Type, error) {
	poolComponents := []abi.ArgumentMarshaling{{Name: "currency0", Type: "address"}, {Name: "currency1", Type: "address"}, {Name: "fee", Type: "uint24"}, {Name: "tickSpacing", Type: "int24"}, {Name: "hooks", Type: "address"}}
	pool, err := abi.NewType("tuple", "", poolComponents)
	if err != nil {
		return abi.Type{}, abi.Type{}, abi.Type{}, err
	}
	single, err := abi.NewType("tuple", "", []abi.ArgumentMarshaling{{Name: "poolKey", Type: "tuple", Components: poolComponents}, {Name: "zeroForOne", Type: "bool"}, {Name: "amountIn", Type: "uint128"}, {Name: "amountOutMinimum", Type: "uint128"}, {Name: "minHopPriceX36", Type: "uint256"}, {Name: "hookData", Type: "bytes"}})
	if err != nil {
		return abi.Type{}, abi.Type{}, abi.Type{}, err
	}
	pathKey, err := abi.NewType("tuple", "", []abi.ArgumentMarshaling{{Name: "intermediateCurrency", Type: "address"}, {Name: "fee", Type: "uint24"}, {Name: "tickSpacing", Type: "int24"}, {Name: "hooks", Type: "address"}, {Name: "hookData", Type: "bytes"}})
	if err != nil {
		return abi.Type{}, abi.Type{}, abi.Type{}, err
	}
	_ = single
	return pool, pathKey, single, nil
}

func encodeV4Single(d map[string]any) (map[string]any, error) {
	_, _, singleType, err := v4Types()
	if err != nil {
		return nil, err
	}
	c0, c1, h := common.HexToAddress(fmt.Sprint(d["currency0"])), common.HexToAddress(fmt.Sprint(d["currency1"])), common.HexToAddress(fmt.Sprint(d["hooks"]))
	tokenIn, tokenOut := common.HexToAddress(fmt.Sprint(d["tokenIn"])), common.HexToAddress(fmt.Sprint(d["tokenOut"]))
	if tokenIn == tokenOut || (tokenIn != c0 && tokenIn != c1) || (tokenOut != c0 && tokenOut != c1) {
		return nil, fmt.Errorf("tokenIn and tokenOut must match the PoolKey currencies")
	}
	amount := ToBig(d["amountInRaw"])
	min := ToBig(d["amountOutMinimumRaw"])
	if amount.Sign() <= 0 {
		return nil, fmt.Errorf("amountInRaw must be greater than zero")
	}
	hookData := bytesValue(d["hookData"])
	recipient := common.HexToAddress(fmt.Sprint(d["recipient"]))
	if recipient == (common.Address{}) {
		recipient = common.HexToAddress(NativeAddress)
	}
	pool := V4PoolKey{c0, c1, ToBig(d["fee"]), ToBig(d["tickSpacing"]), h}
	single := V4ExactInputSingle{pool, tokenIn == c0, amount, min, big.NewInt(0), hookData}
	swapInput, err := abi.Arguments{{Type: singleType}}.Pack(single)
	if err != nil {
		return nil, err
	}
	inputCurrency := c0
	outputCurrency := c1
	if tokenIn != c0 {
		inputCurrency, outputCurrency = c1, c0
	}
	settle, _ := abi.Arguments{{Type: mustType("address")}, {Type: mustType("uint256")}}.Pack(inputCurrency, amount)
	custom := d["recipient"] != nil && strings.TrimSpace(fmt.Sprint(d["recipient"])) != ""
	var take []byte
	if custom {
		take, err = abi.Arguments{{Type: mustType("address")}, {Type: mustType("address")}, {Type: mustType("uint256")}}.Pack(outputCurrency, recipient, big.NewInt(0))
	} else {
		take, err = abi.Arguments{{Type: mustType("address")}, {Type: mustType("uint256")}}.Pack(outputCurrency, min)
	}
	if err != nil {
		return nil, err
	}
	actions := []byte{SwapExactInSingle, SettleAll, TakeAll}
	if custom {
		actions = []byte{SwapExactInSingle, SettleAll, Take}
	}
	v4Input, err := abi.Arguments{{Type: mustType("bytes")}, {Type: mustType("bytes[]")}}.Pack(actions, [][]byte{swapInput, settle, take})
	if err != nil {
		return nil, err
	}
	deadline := ToBig(d["deadline"])
	if deadline.Sign() == 0 {
		deadline = big.NewInt(time.Now().Unix() + 180)
	}
	calldata, err := universalRouterABI.Pack("execute", []byte{V4SwapCommand}, [][]byte{v4Input}, deadline)
	if err != nil {
		return nil, err
	}
	return map[string]any{"router": UniversalRouterAddress, "poolManager": d["poolManager"], "poolId": mustPoolID(pool), "tokenIn": strings.ToLower(tokenIn.Hex()), "tokenOut": strings.ToLower(tokenOut.Hex()), "amountInRaw": amount.String(), "amountOutMinimumRaw": min.String(), "recipient": strings.ToLower(recipient.Hex()), "valueRaw": map[bool]string{true: amount.String(), false: "0"}[inputCurrency == common.HexToAddress(NativeAddress)], "calldata": "0x" + fmt.Sprintf("%x", calldata), "inputs": []string{"0x" + fmt.Sprintf("%x", v4Input)}, "approvals": map[string]any{"required": inputCurrency != common.HexToAddress(NativeAddress), "transactionHashes": []string{}}}, nil
}

func encodeV4Route(d map[string]any) (map[string]any, error) {
	_, _, _, err := v4Types()
	if err != nil {
		return nil, err
	}
	exactType, err := abi.NewType("tuple", "", []abi.ArgumentMarshaling{{Name: "currencyIn", Type: "address"}, {Name: "path", Type: "tuple[]", Components: []abi.ArgumentMarshaling{{Name: "intermediateCurrency", Type: "address"}, {Name: "fee", Type: "uint24"}, {Name: "tickSpacing", Type: "int24"}, {Name: "hooks", Type: "address"}, {Name: "hookData", Type: "bytes"}}}, {Name: "minHopPriceX36", Type: "uint256[]"}, {Name: "amountIn", Type: "uint128"}, {Name: "amountOutMinimum", Type: "uint128"}})
	if err != nil {
		return nil, err
	}
	currency := common.HexToAddress(fmt.Sprint(d["currencyIn"]))
	amount := ToBig(d["amountInRaw"])
	min := ToBig(d["amountOutMinimumRaw"])
	if amount.Sign() <= 0 {
		return nil, fmt.Errorf("amountInRaw must be greater than zero")
	}
	pathRaw, ok := d["path"].([]any)
	if !ok || len(pathRaw) < 1 || len(pathRaw) > 3 {
		return nil, fmt.Errorf("a route must contain between 1 and 3 hops")
	}
	path := make([]V4PathKey, 0, len(pathRaw))
	poolIDs := make([]string, 0, len(pathRaw))
	current := currency
	for i, raw := range pathRaw {
		h, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid route hop %d", i+1)
		}
		intermediate := common.HexToAddress(fmt.Sprint(h["intermediateCurrency"]))
		if intermediate == current {
			return nil, fmt.Errorf("route hop %d has identical currencies", i+1)
		}
		c0, c1 := current, intermediate
		if strings.ToLower(c1.Hex()) < strings.ToLower(c0.Hex()) {
			c0, c1 = c1, c0
		}
		pool := V4PoolKey{c0, c1, ToBig(h["fee"]), ToBig(h["tickSpacing"]), common.HexToAddress(fmt.Sprint(h["hooks"]))}
		poolIDs = append(poolIDs, mustPoolID(pool))
		path = append(path, V4PathKey{intermediate, pool.Fee, pool.TickSpacing, pool.Hooks, bytesValue(h["hookData"])})
		current = intermediate
	}
	exact := V4ExactInput{currency, path, []*big.Int{}, amount, min}
	swapInput, err := abi.Arguments{{Type: exactType}}.Pack(exact)
	if err != nil {
		return nil, err
	}
	wrap := boolValue(d["wrapNative"])
	unwrap := boolValue(d["unwrapNative"])
	recipient := common.HexToAddress(fmt.Sprint(d["recipient"]))
	custom := d["recipient"] != nil && strings.TrimSpace(fmt.Sprint(d["recipient"])) != ""
	if recipient == (common.Address{}) {
		recipient = common.HexToAddress(NativeAddress)
	}
	var settle []byte
	if wrap {
		settle, err = abi.Arguments{{Type: mustType("address")}, {Type: mustType("uint256")}, {Type: mustType("bool")}}.Pack(currency, amount, false)
	} else {
		settle, err = abi.Arguments{{Type: mustType("address")}, {Type: mustType("uint256")}}.Pack(currency, amount)
	}
	if err != nil {
		return nil, err
	}
	var take []byte
	if unwrap {
		take, err = abi.Arguments{{Type: mustType("address")}, {Type: mustType("address")}, {Type: mustType("uint256")}}.Pack(current, common.HexToAddress(AddressThis), big.NewInt(0))
	} else if custom {
		take, err = abi.Arguments{{Type: mustType("address")}, {Type: mustType("address")}, {Type: mustType("uint256")}}.Pack(current, recipient, big.NewInt(0))
	} else {
		take, err = abi.Arguments{{Type: mustType("address")}, {Type: mustType("uint256")}}.Pack(current, min)
	}
	if err != nil {
		return nil, err
	}
	actions := []byte{SwapExactIn, SettleAll, TakeAll}
	if wrap {
		actions[1] = Settle
	}
	if unwrap || custom {
		actions[2] = Take
	}
	v4Input, err := abi.Arguments{{Type: mustType("bytes")}, {Type: mustType("bytes[]")}}.Pack(actions, [][]byte{swapInput, settle, take})
	if err != nil {
		return nil, err
	}
	commands := []byte{V4SwapCommand}
	inputs := [][]byte{v4Input}
	if wrap {
		commands = []byte{WrapETHCommand, V4SwapCommand}
		inputs = [][]byte{mustPackAddressAmount(common.HexToAddress(AddressThis), amount), v4Input}
	}
	if unwrap {
		commands = append(commands, UnwrapWETHCommand)
		inputs = append(inputs, mustPackAddressAmount(recipient, min))
	}
	deadline := ToBig(d["deadline"])
	if deadline.Sign() == 0 {
		deadline = big.NewInt(time.Now().Unix() + 180)
	}
	calldata, err := universalRouterABI.Pack("execute", commands, inputs, deadline)
	if err != nil {
		return nil, err
	}
	return map[string]any{"mode": "calldata-preview", "router": UniversalRouterAddress, "poolIds": poolIDs, "hopCount": len(poolIDs), "tokenIn": strings.ToLower(currency.Hex()), "tokenOut": strings.ToLower(current.Hex()), "amountInRaw": amount.String(), "amountOutMinimumRaw": min.String(), "recipient": strings.ToLower(recipient.Hex()), "deadline": deadline.String(), "valueRaw": map[bool]string{true: amount.String(), false: "0"}[wrap || currency == common.HexToAddress(NativeAddress)], "commands": "0x" + fmt.Sprintf("%x", commands), "calldata": "0x" + fmt.Sprintf("%x", calldata), "inputs": hexStrings(inputs), "approvals": map[string]any{"required": !wrap && currency != common.HexToAddress(NativeAddress), "transactionHashes": []string{}}}, nil
}

func boolValue(v any) bool { b, ok := v.(bool); return ok && b }
func mustPackAddressAmount(a common.Address, n *big.Int) []byte {
	b, _ := abi.Arguments{{Type: mustType("address")}, {Type: mustType("uint256")}}.Pack(a, n)
	return b
}
func hexStrings(v [][]byte) []string {
	out := make([]string, len(v))
	for i, b := range v {
		out[i] = "0x" + fmt.Sprintf("%x", b)
	}
	return out
}

func mustType(name string) abi.Type {
	t, e := abi.NewType(name, "", nil)
	if e != nil {
		panic(e)
	}
	return t
}
func bytesValue(v any) []byte {
	s := strings.TrimPrefix(fmt.Sprint(v), "0x")
	if s == "" || s == "<nil>" {
		return []byte{}
	}
	b, _ := hex.DecodeString(s)
	return b
}
func mustPoolID(p V4PoolKey) string {
	d := map[string]any{"currency0": p.Currency0.Hex(), "currency1": p.Currency1.Hex(), "fee": p.Fee, "tickSpacing": p.TickSpacing, "hooks": p.Hooks.Hex()}
	id, _ := PoolID(d)
	return id
}
