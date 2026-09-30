package strategy

import "testing"

func TestNativeSellMinimumOutput(t *testing.T) {
	event := Event{QuoteAmountText: "1000000000000000000", TokenAmountText: "100000000000000000000"}
	if got := nativeSellMinimumOutput(event, "10000000000000000000", 0, 0.05, 1); got != "95000000000000000" {
		t.Fatalf("direct native sell minimum = %s", got)
	}
	if got := nativeSellMinimumOutput(event, "10000000000000000000", 0, 0.05, 2); got != "0" {
		t.Fatalf("multi-hop sell must require a separate quote, got %s", got)
	}
	if got := nativeSellMinimumOutput(Event{}, "10000000000000000000", 0, 0.05, 1); got != "0" {
		t.Fatalf("sell without a quote must fail closed, got %s", got)
	}
}
