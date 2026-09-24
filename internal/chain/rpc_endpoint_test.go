package chain

import (
	"context"
	"encoding/json"
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
