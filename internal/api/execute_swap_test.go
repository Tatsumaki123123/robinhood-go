package api

import (
	"math/big"
	"testing"

	"robinhood-go/internal/chain"
	"robinhood-go/internal/store"
)

func TestExecuteSwapRequestOnlyUnwrapsWrappedNative(t *testing.T) {
	token := store.ExecuteToken{TokenAddress: "0x2a00fc638ac8fbc7c7ba914a2018c07a3268d301"}
	for _, test := range []struct {
		name   string
		output string
		unwrap bool
	}{
		{name: "native ETH", output: chain.NativeAddress, unwrap: false},
		{name: "wrapped ETH", output: chain.WrappedNativeAddress, unwrap: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			route := []map[string]any{{"tokenIn": token.TokenAddress, "tokenOut": test.output, "fee": 10000, "tickSpacing": 200, "hooks": "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"}}
			request, err := executeSwapRequest(token, route, "key", "0xdb493c40480dc81c4b5a0fbf0f003bf9d77deac0", "sell", big.NewInt(100), big.NewInt(1))
			if err != nil {
				t.Fatal(err)
			}
			if request["unwrapNative"] != test.unwrap {
				t.Fatalf("unwrapNative = %v, want %v", request["unwrapNative"], test.unwrap)
			}
		})
	}
}

func TestExecuteSwapRequestRejectsUnprotectedSell(t *testing.T) {
	token := store.ExecuteToken{TokenAddress: "0x2a00fc638ac8fbc7c7ba914a2018c07a3268d301"}
	route := []map[string]any{{"tokenIn": token.TokenAddress, "tokenOut": chain.NativeAddress}}
	if _, err := executeSwapRequest(token, route, "key", "0xdb493c40480dc81c4b5a0fbf0f003bf9d77deac0", "sell", big.NewInt(100), big.NewInt(0)); err == nil {
		t.Fatal("sell without a minimum output must fail")
	}
	if _, err := executeSwapRequest(token, route, "key", "invalid", "sell", big.NewInt(100), big.NewInt(1)); err == nil {
		t.Fatal("sell with an invalid recipient must fail")
	}
	wrongInput := []map[string]any{{"tokenIn": chain.WrappedNativeAddress, "tokenOut": chain.NativeAddress}}
	if _, err := executeSwapRequest(token, wrongInput, "key", "0xdb493c40480dc81c4b5a0fbf0f003bf9d77deac0", "sell", big.NewInt(100), big.NewInt(1)); err == nil {
		t.Fatal("sell route for a different input token must fail")
	}
	wrongOutput := []map[string]any{{"tokenIn": token.TokenAddress, "tokenOut": "0x1111111111111111111111111111111111111111"}}
	if _, err := executeSwapRequest(token, wrongOutput, "key", "0xdb493c40480dc81c4b5a0fbf0f003bf9d77deac0", "sell", big.NewInt(100), big.NewInt(1)); err == nil {
		t.Fatal("sell route without ETH output must fail")
	}
}
