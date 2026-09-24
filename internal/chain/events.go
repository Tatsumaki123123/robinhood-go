package chain

import (
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
	return decodeChainEventMode(log, true)
}

// decodeChainEventInPlace is used only by the WSS reader, where the decoded
// result is immediately published and the raw map has no other owner. It
// avoids copying every log's map on the hottest path; block-poll callers keep
// the copy-preserving DecodeChainEvent behavior below.
func decodeChainEventInPlace(log map[string]any) map[string]any {
	return decodeChainEventMode(log, false)
}

func decodeChainEventMode(log map[string]any, copyLog bool) map[string]any {
	topics, ok := log["topics"].([]any)
	if !ok || len(topics) == 0 {
		return log
	}
	topic := strings.ToLower(toString(topics[0]))
	data := strings.TrimPrefix(strings.ToLower(toString(log["data"])), "0x")
	out := log
	if copyLog {
		out = make(map[string]any, len(log)+8)
		for k, v := range log {
			out[k] = v
		}
	}
	if len(data) >= 4*64 && topic == CurveBuyTopic {
		out["side"] = "buy"
		out["curveAddress"] = toString(log["address"])
		out["sender"] = topicAddress(topics, 1)
		out["recipient"] = topicAddress(topics, 2)
		quote := eventWord(data, 0)
		tokens := eventWord(data, 1)
		out["quoteAmountRaw"] = quote.String()
		out["tokenAmountRaw"] = tokens.String()
		out["quoteInRaw"] = quote.String()
		out["tokensOutRaw"] = tokens.String()
	}
	if len(data) >= 4*64 && topic == CurveSellTopic {
		out["side"] = "sell"
		out["curveAddress"] = toString(log["address"])
		out["sender"] = topicAddress(topics, 1)
		out["recipient"] = topicAddress(topics, 2)
		tokens := eventWord(data, 0)
		quote := eventWord(data, 1)
		out["tokenAmountRaw"] = tokens.String()
		out["quoteAmountRaw"] = quote.String()
		out["tokensInRaw"] = tokens.String()
		out["quoteOutRaw"] = quote.String()
	}
	if len(data) >= 6*64 && topic == V4SwapTopic {
		out["side"] = "buy"
		out["poolId"] = toStringAt(topics, 1)
		out["sender"] = topicAddress(topics, 2)
		out["amount0Raw"] = signedWord(data, 0).String()
		out["amount1Raw"] = signedWord(data, 1).String()
		out["sqrtPriceX96"] = eventWord(data, 2).String()
		out["liquidity"] = eventWord(data, 3).String()
		out["tick"] = signedWord(data, 4).String()
		out["fee"] = eventWord(data, 5).String()
	}
	if tx := toString(log["transactionHash"]); tx != "" {
		out["sourceEventKey"] = tx + ":" + toString(log["logIndex"])
	}
	return out
}

// DecodeChainEvent is used by the bottom-fishing block poller. The live WSS
// subscriber calls the in-place variant above, keeping both transports on the
// same decoder without copying the WSS map.
func DecodeChainEvent(log map[string]any) map[string]any { return decodeChainEvent(log) }

// StrategyEventFilter accepts only decoded swap events. Raw deployment logs
// remain available to API subscribers but do not consume the trading queue.
func StrategyEventFilter(event map[string]any) bool {
	if event == nil {
		return false
	}
	if pool := toString(event["poolId"]); pool != "" {
		return true
	}
	side := strings.ToLower(strings.TrimSpace(toString(event["side"])))
	return (side == "buy" || side == "sell") && toString(event["curveAddress"]) != ""
}

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

func eventWord(data string, index int) *big.Int {
	if index < 0 || (index+1)*64 > len(data) {
		return big.NewInt(0)
	}
	n := new(big.Int)
	if _, ok := n.SetString(data[index*64:(index+1)*64], 16); !ok {
		return big.NewInt(0)
	}
	return n
}

func signedWord(data string, index int) *big.Int {
	n := eventWord(data, index)
	if index >= 0 && (index+1)*64 <= len(data) {
		first := data[index*64]
		negative := (first >= '8' && first <= '9') || (first >= 'a' && first <= 'f') || (first >= 'A' && first <= 'F')
		if !negative {
			return n
		}
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	return n
}
