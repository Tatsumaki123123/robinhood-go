package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func (a *API) StartEventBridge() {
	events, _ := a.RPC.AddSubscriber(512)
	go func() {
		for e := range events {
			if topics, ok := e["topics"].([]any); ok && len(topics) > 1 {
				if _, exists := e["poolId"]; !exists {
					e["poolId"] = fmt.Sprint(topics[1])
				}
			}
			// Keep the same bounded in-memory event views exposed by the Node
			// listener service.  Raw logs remain available over WebSocket.
			a.eventsMu.Lock()
			if strings.EqualFold(strings.TrimSpace(fmt.Sprint(e["address"])), strings.TrimSpace(a.Cfg.PoolManager)) {
				a.swaps = appendBounded(a.swaps, e, 500)
			} else {
				a.deployments = appendBounded(a.deployments, e, 500)
			}
			a.eventsMu.Unlock()
			b, _ := json.Marshal(map[string]any{"type": "chain_event", "data": e, "receivedAt": time.Now().UTC()})
			a.clientsMu.RLock()
			for c := range a.clients {
				_ = c.WriteMessage(1, b)
			}
			a.clientsMu.RUnlock()
		}
	}()
}

func appendBounded(items []map[string]any, item map[string]any, max int) []map[string]any {
	items = append(items, item)
	if len(items) > max {
		items = items[len(items)-max:]
	}
	return items
}
