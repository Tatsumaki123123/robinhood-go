package chain

import (
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

func TestEncodeV4RoutePaysNativeOutputToRecipient(t *testing.T) {
	recipient := common.HexToAddress("0xdb493c40480dc81c4b5a0fbf0f003bf9d77deac0")
	request := map[string]any{
		"currencyIn": "0x2a00fc638ac8fbc7c7ba914a2018c07a3268d301",
		"path": []any{map[string]any{
			"intermediateCurrency": NativeAddress,
			"fee":                  10000, "tickSpacing": 200,
			"hooks": "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044",
		}},
		"amountInRaw": "1000000000000000000", "amountOutMinimumRaw": "1",
		"recipient": recipient.Hex(), "customRecipient": false, "unwrapNative": true,
	}
	prepared, err := encodeV4Route(request)
	if err != nil {
		t.Fatal(err)
	}
	if prepared["commands"] != "0x10" {
		t.Fatalf("native output must not include WETH unwrap command: %v", prepared["commands"])
	}
	inputs := prepared["inputs"].([]string)
	v4Args := abi.Arguments{{Type: mustType("bytes")}, {Type: mustType("bytes[]")}}
	decoded, err := v4Args.Unpack(common.FromHex(inputs[0]))
	if err != nil {
		t.Fatal(err)
	}
	actions := decoded[0].([]byte)
	if len(actions) != 3 || actions[2] != Take {
		t.Fatalf("unexpected V4 actions: %x", actions)
	}
	actionInputs := decoded[1].([][]byte)
	takeArgs := abi.Arguments{{Type: mustType("address")}, {Type: mustType("address")}, {Type: mustType("uint256")}}
	take, err := takeArgs.Unpack(actionInputs[2])
	if err != nil {
		t.Fatal(err)
	}
	if currency := take[0].(common.Address); currency != common.HexToAddress(NativeAddress) {
		t.Fatalf("TAKE currency = %s, want native ETH", currency)
	}
	if to := take[1].(common.Address); to != recipient {
		t.Fatalf("TAKE recipient = %s, want %s", to, recipient)
	}
}

func TestEncodeV4RouteUnwrapsWrappedOutputToRecipient(t *testing.T) {
	recipient := common.HexToAddress("0xdb493c40480dc81c4b5a0fbf0f003bf9d77deac0")
	request := map[string]any{
		"currencyIn": "0x2a00fc638ac8fbc7c7ba914a2018c07a3268d301",
		"path": []any{map[string]any{
			"intermediateCurrency": WrappedNativeAddress,
			"fee":                  10000, "tickSpacing": 200,
			"hooks": "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044",
		}},
		"amountInRaw": "1000000000000000000", "amountOutMinimumRaw": "1",
		"recipient": recipient.Hex(), "unwrapNative": true,
	}
	prepared, err := encodeV4Route(request)
	if err != nil {
		t.Fatal(err)
	}
	if prepared["commands"] != "0x100c" {
		t.Fatalf("wrapped output must be unwrapped: %v", prepared["commands"])
	}
	inputs := prepared["inputs"].([]string)
	decoded, err := (abi.Arguments{{Type: mustType("bytes")}, {Type: mustType("bytes[]")}}).Unpack(common.FromHex(inputs[0]))
	if err != nil {
		t.Fatal(err)
	}
	take, err := (abi.Arguments{{Type: mustType("address")}, {Type: mustType("address")}, {Type: mustType("uint256")}}).Unpack(decoded[1].([][]byte)[2])
	if err != nil {
		t.Fatal(err)
	}
	if take[0].(common.Address) != common.HexToAddress(WrappedNativeAddress) || take[1].(common.Address) != common.HexToAddress(AddressThis) {
		t.Fatalf("wrapped output must first be taken by router: %v", take)
	}
	unwrap, err := (abi.Arguments{{Type: mustType("address")}, {Type: mustType("uint256")}}).Unpack(common.FromHex(inputs[1]))
	if err != nil {
		t.Fatal(err)
	}
	if unwrap[0].(common.Address) != recipient {
		t.Fatalf("unwrap recipient = %s, want %s", unwrap[0], recipient)
	}
}

func TestEncodeV4RouteRejectsInvalidRecipient(t *testing.T) {
	request := map[string]any{
		"currencyIn": "0x2a00fc638ac8fbc7c7ba914a2018c07a3268d301",
		"path": []any{map[string]any{
			"intermediateCurrency": NativeAddress,
			"fee":                  10000, "tickSpacing": 200,
			"hooks": "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044",
		}},
		"amountInRaw": "1000000000000000000", "amountOutMinimumRaw": "1",
		"recipient": "invalid",
	}
	if _, err := encodeV4Route(request); err == nil {
		t.Fatal("invalid recipient must fail before broadcast")
	}
}
