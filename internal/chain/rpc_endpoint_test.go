package chain

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestRPCEndpointSeparation(t *testing.T) {
	var reads, sends atomic.Int64
	newServer := func(counter *atomic.Int64) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			counter.Add(1)
			var body struct {
				Method string `json:"method"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			result := any("0x0")
			if body.Method == "eth_sendRawTransaction" {
				result = "0xfeed"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
		}))
	}
	readServer := newServer(&reads)
	defer readServer.Close()
	sendServer := newServer(&sends)
	defer sendServer.Close()

	rpc := NewWithEndpoints(readServer.URL, sendServer.URL, "", Options{})
	if _, err := rpc.LatestBlock(context.Background()); err != nil {
		t.Fatalf("latest block through read endpoint: %v", err)
	}
	if _, err := rpc.SendRaw(context.Background(), "0x1234"); err != nil {
		t.Fatalf("raw transaction through send endpoint: %v", err)
	}
	if reads.Load() != 1 || sends.Load() != 1 {
		t.Fatalf("unexpected endpoint counts: reads=%d sends=%d", reads.Load(), sends.Load())
	}

	var fallbackCalls atomic.Int64
	fallbackServer := newServer(&fallbackCalls)
	defer fallbackServer.Close()
	fallback := NewWithEndpoints(fallbackServer.URL, "", "", Options{})
	if _, err := fallback.SendRaw(context.Background(), "0x1234"); err != nil {
		t.Fatalf("raw transaction through fallback endpoint: %v", err)
	}
	if fallbackCalls.Load() != 1 {
		t.Fatalf("expected empty send endpoint to fall back to read endpoint, got %d calls", fallbackCalls.Load())
	}
}

func TestEstimateNativeGasRetriesWithoutZeroGasPrice(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Method string           `json:"method"`
			Params []map[string]any `json:"params"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if body.Method != "eth_estimateGas" || len(body.Params) != 1 {
			t.Errorf("unexpected estimate request: %+v", body)
			return
		}
		calls++
		if calls == 1 {
			if body.Params[0]["gasPrice"] != "0x0" {
				t.Errorf("first estimate must omit gas charges: %+v", body.Params[0])
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32000, "message": "fee cap below base fee"}})
			return
		}
		if _, hasPrice := body.Params[0]["gasPrice"]; hasPrice {
			t.Errorf("retry retained gasPrice: %+v", body.Params[0])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": "0xcf08"})
	}))
	defer server.Close()

	gas, err := New(server.URL, "").EstimateNativeGas(context.Background(), "0x0000000000000000000000000000000000000001", "0x0000000000000000000000000000000000000002", big.NewInt(1))
	if err != nil || gas != 53000 || calls != 2 {
		t.Fatalf("estimate gas = %d, calls = %d, err = %v", gas, calls, err)
	}
}
