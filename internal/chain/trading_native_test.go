package chain

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
)

func TestSendNativeAllUsesSignedGasPriceForAmount(t *testing.T) {
	key, err := gethcrypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	const balance = int64(5_000_000_000_000)
	const gasPrice = int64(60_000_000)
	gasPriceCalls := 0
	var sent *types.Transaction
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string   `json:"method"`
			Params []string `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode RPC request: %v", err)
			return
		}
		var result any
		switch req.Method {
		case "eth_gasPrice":
			gasPriceCalls++
			result = fmt.Sprintf("0x%x", gasPrice/int64(gasPriceCalls))
		case "eth_getBalance":
			result = fmt.Sprintf("0x%x", balance)
		case "eth_getTransactionCount":
			result = "0x0"
		case "eth_sendRawTransaction":
			if len(req.Params) != 1 {
				t.Errorf("unexpected send params: %v", req.Params)
				return
			}
			raw, err := hex.DecodeString(req.Params[0][2:])
			if err != nil {
				t.Errorf("decode transaction: %v", err)
				return
			}
			sent = new(types.Transaction)
			if err := sent.UnmarshalBinary(raw); err != nil {
				t.Errorf("unmarshal transaction: %v", err)
				return
			}
			result = "0xfeed"
		case "eth_getTransactionReceipt":
			result = map[string]any{"status": "0x1"}
		default:
			t.Errorf("unexpected RPC method: %s", req.Method)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	defer server.Close()

	trading := &Trading{RPC: New(server.URL, ""), ChainID: 1}
	_, err = trading.SendNativeAll(context.Background(), fmt.Sprintf("%x", gethcrypto.FromECDSA(key)), "0x0000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	if gasPriceCalls != 1 || sent == nil {
		t.Fatalf("expected one gas quote and a transaction, got %d quotes and transaction %v", gasPriceCalls, sent)
	}
	expected := big.NewInt(balance - 21000*gasPrice)
	if sent.Value().Cmp(expected) != 0 || sent.GasPrice().Cmp(big.NewInt(gasPrice)) != 0 {
		t.Fatalf("sweep value %s and gas price %s, want %s and %d", sent.Value(), sent.GasPrice(), expected, gasPrice)
	}
}

func TestSendNativeAllReportsDustBalance(t *testing.T) {
	key, err := gethcrypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode RPC request: %v", err)
			return
		}
		var result any
		switch req.Method {
		case "eth_gasPrice":
			result = "0x3938700" // 60,000,000 wei
		case "eth_getBalance":
			result = "0xb14d0e6380" // 761,502,000,000 wei
		default:
			t.Errorf("unexpected RPC method: %s", req.Method)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}))
	defer server.Close()

	trading := &Trading{RPC: New(server.URL, ""), ChainID: 1}
	_, err = trading.SendNativeAll(context.Background(), fmt.Sprintf("%x", gethcrypto.FromECDSA(key)), "0x0000000000000000000000000000000000000001")
	var insufficient *NativeSweepInsufficientError
	if !errors.As(err, &insufficient) {
		t.Fatalf("expected insufficient sweep balance, got %v", err)
	}
	if insufficient.Balance.Cmp(big.NewInt(761_502_000_000)) != 0 || insufficient.GasCost.Cmp(big.NewInt(1_260_000_000_000)) != 0 {
		t.Fatalf("unexpected balance %s or gas cost %s", insufficient.Balance, insufficient.GasCost)
	}
}
