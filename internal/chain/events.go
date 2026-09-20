package chain

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

const (
	CurveBuyTopic  = "0xec36bf571f136799e8dc0b0b8bea4b04d8bd3d43de838aab0d5fc21d4cbfc455"
	CurveSellTopic = "0x8113d738abdcb6b38357e9d53a54a7157861a09031b453651f0fe7fe151f59df"
	V4SwapTopic    = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
)

func decodeChainEvent(log map[string]any) map[string]any {
	topics, ok := log["topics"].([]any)
	if !ok || len(topics) == 0 {
		return log
	}
	topic := strings.ToLower(toString(topics[0]))
	data := strings.TrimPrefix(strings.ToLower(toString(log["data"])), "0x")
	words := make([]*big.Int, 0, len(data)/64)
	for i := 0; i+64 <= len(data); i += 64 {
		n := new(big.Int)
		n.SetString(data[i:i+64], 16)
		words = append(words, n)
	}
	out := map[string]any{}
	for k, v := range log {
		out[k] = v
	}
	if len(words) >= 4 && topic == CurveBuyTopic {
		out["side"] = "buy"
		out["curveAddress"] = toString(log["address"])
		out["sender"] = topicAddress(topics, 1)
		out["recipient"] = topicAddress(topics, 2)
		out["quoteAmountRaw"] = words[0].String()
		out["tokenAmountRaw"] = words[1].String()
		out["quoteInRaw"] = words[0].String()
		out["tokensOutRaw"] = words[1].String()
	}
	if len(words) >= 4 && topic == CurveSellTopic {
		out["side"] = "sell"
		out["curveAddress"] = toString(log["address"])
		out["sender"] = topicAddress(topics, 1)
		out["recipient"] = topicAddress(topics, 2)
		out["tokenAmountRaw"] = words[0].String()
		out["quoteAmountRaw"] = words[1].String()
		out["tokensInRaw"] = words[0].String()
		out["quoteOutRaw"] = words[1].String()
	}
	if len(words) >= 6 && topic == V4SwapTopic {
		out["side"] = "buy"
		out["poolId"] = toStringAt(topics, 1)
		out["sender"] = topicAddress(topics, 2)
		out["amount0Raw"] = signedWord(data, 0).String()
		out["amount1Raw"] = signedWord(data, 1).String()
		out["sqrtPriceX96"] = words[2].String()
		out["liquidity"] = words[3].String()
		out["tick"] = signedWord(data, 4).String()
		out["fee"] = words[5].String()
	}
	if tx := toString(log["transactionHash"]); tx != "" {
		out["sourceEventKey"] = tx + ":" + toString(log["logIndex"])
	}
	return out
}

// DecodeChainEvent is used by the bottom-fishing block poller as well as the
// live WSS subscriber, keeping both transports on exactly the same decoder.
func DecodeChainEvent(log map[string]any) map[string]any { return decodeChainEvent(log) }
func toString(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}
func toStringAt(v []any, i int) string {
	if i >= len(v) {
		return ""
	}
	return toString(v[i])
}
func topicAddress(topics []any, i int) string {
	s := strings.TrimPrefix(toStringAt(topics, i), "0x")
	if len(s) < 40 {
		return ""
	}
	return "0x" + s[len(s)-40:]
}
func signedWord(data string, index int) *big.Int {
	if index < 0 || (index+1)*64 > len(data) {
		return big.NewInt(0)
	}
	b, _ := hex.DecodeString(data[index*64 : (index+1)*64])
	n := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
	}
	return n
}
