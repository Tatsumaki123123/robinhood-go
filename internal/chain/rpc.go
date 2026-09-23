package chain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RPC struct {
	HTTP, WS                string
	id                      uint64
	client                  *http.Client
	Events                  chan map[string]any
	ingress                 chan map[string]any
	subMu                   sync.RWMutex
	subs                    map[chan map[string]any]struct{}
	publishedEvents         atomic.Uint64
	droppedEvents           atomic.Uint64
	droppedSubscriberEvents atomic.Uint64
}

func New(httpURL, wsURL string) *RPC {
	r := &RPC{HTTP: httpURL, WS: wsURL, client: &http.Client{Timeout: 15 * time.Second}, Events: make(chan map[string]any, 8192), ingress: make(chan map[string]any, 32768), subs: make(map[chan map[string]any]struct{})}
	// Keep the websocket reader independent from strategy/database latency.
	// The ingress queue absorbs short bursts and this ordered dispatcher is the
	// only writer of the public strategy stream, so events cannot overtake one
	// another when the strategy channel is temporarily full.
	go func() {
		for event := range r.ingress {
			r.Events <- event
		}
	}()
	return r
}

// AddSubscriber creates an independent event stream. The strategy keeps using
// Events while API/WebSocket consumers receive their own copy, so a slow UI
// client can no longer steal events from the trading engine.
func (r *RPC) AddSubscriber(buffer int) (<-chan map[string]any, func()) {
	if buffer < 1 {
		buffer = 128
	}
	ch := make(chan map[string]any, buffer)
	r.subMu.Lock()
	r.subs[ch] = struct{}{}
	r.subMu.Unlock()
	return ch, func() {
		r.subMu.Lock()
		if _, ok := r.subs[ch]; ok {
			delete(r.subs, ch)
			close(ch)
		}
		r.subMu.Unlock()
	}
}

func (r *RPC) publishEvent(event map[string]any) {
	if _, ok := event["receivedAt"]; !ok {
		event["receivedAt"] = time.Now().UTC()
	}
	target := r.ingress
	if target == nil {
		// Preserve the behavior for callers that construct RPC literals rather
		// than using New (for example small embedders and replay tools).
		target = r.Events
	}
	select {
	case target <- event:
		r.publishedEvents.Add(1)
	default:
		// The WSS reader must never block on strategy/database work.  Keep the
		// channel bounded for latency, but expose overflow so operators can
		// detect that polling/reconciliation needs to catch up instead of
		// silently treating a saturated stream as healthy.
		r.droppedEvents.Add(1)
	}
	r.subMu.RLock()
	defer r.subMu.RUnlock()
	for ch := range r.subs {
		copy := cloneEvent(event)
		select {
		case ch <- copy:
		default:
			r.droppedSubscriberEvents.Add(1)
		}
	}
}

// DroppedEvents reports events that could not be queued without blocking the
// WSS reader.  It is intentionally read-only and lock-free for health checks.
func (r *RPC) DroppedEvents() uint64 { return r.droppedEvents.Load() }

// DroppedSubscriberEvents reports events dropped only from API consumers. The
// strategy stream has its own counter exposed by DroppedEvents.
func (r *RPC) DroppedSubscriberEvents() uint64 { return r.droppedSubscriberEvents.Load() }

func (r *RPC) PublishedEvents() uint64 { return r.publishedEvents.Load() }

func (r *RPC) IngressDepth() int {
	if r.ingress == nil {
		return 0
	}
	return len(r.ingress)
}

func (r *RPC) StrategyDepth() int {
	if r.Events == nil {
		return 0
	}
	return len(r.Events)
}

func cloneEvent(event map[string]any) map[string]any {
	copy := make(map[string]any, len(event))
	for key, value := range event {
		copy[key] = value
	}
	if topics, ok := event["topics"].([]any); ok {
		copy["topics"] = append([]any(nil), topics...)
	}
	return copy
}
func (r *RPC) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if r.HTTP == "" {
		return nil, fmt.Errorf("RPC_HTTP_URL is not configured")
	}
	id := atomic.AddUint64(&r.id, 1)
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	req, e := http.NewRequestWithContext(ctx, "POST", r.HTTP, bytes.NewReader(body))
	if e != nil {
		return nil, e
	}
	req.Header.Set("content-type", "application/json")
	res, e := r.client.Do(req)
	if e != nil {
		return nil, e
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("rpc status %d: %s", res.StatusCode, b)
	}
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if e = json.Unmarshal(b, &out); e != nil {
		return nil, e
	}
	if out.Error != nil {
		return nil, fmt.Errorf("rpc %d: %s", out.Error.Code, out.Error.Message)
	}
	return out.Result, nil
}
func (r *RPC) LatestBlock(ctx context.Context) (string, error) {
	v, e := r.Call(ctx, "eth_blockNumber", []any{})
	var out string
	if e == nil {
		e = json.Unmarshal(v, &out)
	}
	return out, e
}
func (r *RPC) Balance(ctx context.Context, address string) (string, error) {
	v, e := r.Call(ctx, "eth_getBalance", []any{address, "latest"})
	var out string
	if e == nil {
		e = json.Unmarshal(v, &out)
	}
	return out, e
}
func (r *RPC) ERC20Balance(ctx context.Context, token, owner string) (*big.Int, error) {
	data := common.Hex2Bytes("70a08231")
	data = append(data, common.LeftPadBytes(common.HexToAddress(owner).Bytes(), 32)...)
	v, e := r.Call(ctx, "eth_call", []any{map[string]any{"to": common.HexToAddress(token).Hex(), "data": "0x" + fmt.Sprintf("%x", data)}, "latest"})
	if e != nil {
		return nil, e
	}
	s := strings.TrimPrefix(strings.Trim(string(v), `"`), "0x")
	n := new(big.Int)
	if _, ok := n.SetString(s, 16); !ok {
		return big.NewInt(0), nil
	}
	return n, nil
}

func (r *RPC) ERC20Decimals(ctx context.Context, token string) (int, error) {
	v, err := r.Call(ctx, "eth_call", []any{map[string]any{"to": common.HexToAddress(token).Hex(), "data": "0x313ce567"}, "latest"})
	if err != nil {
		return 0, err
	}
	s := strings.TrimPrefix(strings.Trim(string(v), `"`), "0x")
	n := new(big.Int)
	if _, ok := n.SetString(s, 16); !ok || n.Sign() < 0 || n.Int64() > 255 {
		return 0, fmt.Errorf("invalid ERC20 decimals")
	}
	return int(n.Int64()), nil
}

// ERC20TotalSupply reads the raw ERC20 totalSupply() value. Keeping this as a
// string avoids overflowing native integer types for tokens with large supply.
func (r *RPC) ERC20TotalSupply(ctx context.Context, token string) (string, error) {
	v, err := r.Call(ctx, "eth_call", []any{map[string]any{"to": common.HexToAddress(token).Hex(), "data": "0x18160ddd"}, "latest"})
	if err != nil {
		return "", err
	}
	s := strings.TrimPrefix(strings.Trim(string(v), `"`), "0x")
	n := new(big.Int)
	if _, ok := n.SetString(s, 16); !ok || n.Sign() < 0 {
		return "", fmt.Errorf("invalid ERC20 total supply")
	}
	return n.String(), nil
}
func (r *RPC) Tx(ctx context.Context, hash string) (any, error) {
	v, e := r.Call(ctx, "eth_getTransactionByHash", []any{hash})
	if e != nil {
		return nil, e
	}
	var tx any
	if e = json.Unmarshal(v, &tx); e != nil {
		return nil, e
	}
	receipt, e := r.Receipt(ctx, hash)
	if e != nil {
		return nil, e
	}
	return map[string]any{"transaction": tx, "receipt": receipt}, nil
}

func (r *RPC) TransactionFrom(ctx context.Context, hash string) (string, error) {
	v, e := r.Call(ctx, "eth_getTransactionByHash", []any{hash})
	if e != nil {
		return "", e
	}
	var tx struct {
		From string `json:"from"`
	}
	if e = json.Unmarshal(v, &tx); e != nil {
		return "", e
	}
	return strings.TrimSpace(tx.From), nil
}

func (r *RPC) Receipt(ctx context.Context, hash string) (any, error) {
	v, e := r.Call(ctx, "eth_getTransactionReceipt", []any{hash})
	if e != nil {
		return nil, e
	}
	var x any
	_ = json.Unmarshal(v, &x)
	return x, nil
}
func (r *RPC) Logs(ctx context.Context, filter map[string]any) (any, error) {
	v, e := r.Call(ctx, "eth_getLogs", []any{filter})
	if e != nil {
		return nil, e
	}
	var x any
	_ = json.Unmarshal(v, &x)
	return x, nil
}
func (r *RPC) SendRaw(ctx context.Context, raw string) (string, error) {
	v, e := r.Call(ctx, "eth_sendRawTransaction", []any{raw})
	var out string
	if e == nil {
		e = json.Unmarshal(v, &out)
	}
	return out, e
}
func (r *RPC) Nonce(ctx context.Context, address string) (uint64, error) {
	v, e := r.Call(ctx, "eth_getTransactionCount", []any{address, "pending"})
	if e != nil {
		return 0, e
	}
	return strconv.ParseUint(strings.TrimPrefix(strings.Trim(string(v), `"`), "0x"), 16, 64)
}
func (r *RPC) GasPrice(ctx context.Context) (*big.Int, error) {
	v, e := r.Call(ctx, "eth_gasPrice", []any{})
	if e != nil {
		return nil, e
	}
	s := strings.TrimPrefix(strings.Trim(string(v), `"`), "0x")
	n := new(big.Int)
	if _, ok := n.SetString(s, 16); !ok {
		return nil, fmt.Errorf("invalid gas price")
	}
	return n, nil
}

func (r *RPC) BaseFee(ctx context.Context) (*big.Int, error) {
	v, e := r.Call(ctx, "eth_getBlockByNumber", []any{"latest", false})
	if e != nil {
		return nil, e
	}
	var block struct {
		BaseFeePerGas string `json:"baseFeePerGas"`
	}
	if e := json.Unmarshal(v, &block); e != nil {
		return nil, e
	}
	if block.BaseFeePerGas == "" {
		return big.NewInt(0), nil
	}
	n := new(big.Int)
	if _, ok := n.SetString(strings.TrimPrefix(block.BaseFeePerGas, "0x"), 16); !ok {
		return nil, fmt.Errorf("invalid base fee")
	}
	return n, nil
}

func (r *RPC) Subscribe(ctx context.Context, params map[string]any) {
	if r.WS == "" {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			c, _, e := websocket.DefaultDialer.Dial(r.WS, nil)
			if e != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(250 * time.Millisecond):
				}
				continue
			}
			const readTimeout = 90 * time.Second
			_ = c.SetReadDeadline(time.Now().Add(readTimeout))
			c.SetPongHandler(func(string) error {
				return c.SetReadDeadline(time.Now().Add(readTimeout))
			})
			pingDone := make(chan struct{})
			connDone := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					_ = c.Close()
				case <-connDone:
				}
			}()
			go func() {
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						_ = c.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
					case <-pingDone:
						return
					case <-ctx.Done():
						return
					}
				}
			}()
			_ = c.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": []any{"logs", params}})
			for {
				var msg struct {
					Method string `json:"method"`
					Params struct {
						Result map[string]any `json:"result"`
					} `json:"params"`
				}
				if e = c.ReadJSON(&msg); e != nil {
					close(pingDone)
					close(connDone)
					_ = c.Close()
					select {
					case <-ctx.Done():
						return
					case <-time.After(250 * time.Millisecond):
					}
					break
				}
				if msg.Method == "eth_subscription" {
					msg.Params.Result = decodeChainEvent(msg.Params.Result)
					r.publishEvent(msg.Params.Result)
				}
			}
		}
	}()
}
func parseHex(v string) int64 {
	var n int64
	fmt.Sscanf(strings.TrimPrefix(v, "0x"), "%x", &n)
	return n
}
