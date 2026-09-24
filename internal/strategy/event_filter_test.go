package strategy

import (
	"testing"

	"robinhood-go/internal/chain"
	"robinhood-go/internal/store"
)

func TestDropLowValueEventCurveQuote(t *testing.T) {
	quoteDecimals := 6
	quotePrice := "1"
	e := &Engine{}
	token := store.Token{QuoteDecimals: &quoteDecimals, QuoteUSDPrice: &quotePrice}

	drop, known := e.dropLowValueEvent(Event{QuoteAmountText: "9999999"}, token)
	if !known || !drop {
		t.Fatalf("expected 9.999999 USD event to be dropped, known=%v drop=%v", known, drop)
	}

	drop, known = e.dropLowValueEvent(Event{QuoteAmountText: "10000000"}, token)
	if !known || drop {
		t.Fatalf("expected 10 USD event to pass, known=%v drop=%v", known, drop)
	}
}

func TestDropLowValueEventV4UsesQuoteSide(t *testing.T) {
	quoteDecimals := 6
	quotePrice := "1"
	e := &Engine{}
	token := store.Token{
		TokenAddress:  "0x00000000000000000000000000000000000000aa",
		Currency0:     "0x00000000000000000000000000000000000000aa",
		QuoteDecimals: &quoteDecimals,
		QuoteUSDPrice: &quotePrice,
	}

	drop, known := e.dropLowValueEvent(Event{
		PoolID:     "0xpool",
		Amount0Raw: "-1000000000000000000",
		Amount1Raw: "5000000",
	}, token)
	if !known || !drop {
		t.Fatalf("expected 5 USD V4 quote to be dropped, known=%v drop=%v", known, drop)
	}
}

func TestDropLowValueEventUnknownMetadataPasses(t *testing.T) {
	quoteDecimals := 6
	e := &Engine{}
	token := store.Token{QuoteDecimals: &quoteDecimals}

	drop, known := e.dropLowValueEvent(Event{QuoteAmountText: "1"}, token)
	if known || drop {
		t.Fatalf("expected unknown price to pass conservatively, known=%v drop=%v", known, drop)
	}
}

func TestDropLowValueEventInvalidPricePassesConservatively(t *testing.T) {
	quoteDecimals := 6
	zero := "0"
	e := &Engine{}
	token := store.Token{QuoteDecimals: &quoteDecimals, QuoteUSDPrice: &zero}

	drop, known := e.dropLowValueEvent(Event{QuoteAmountText: "1"}, token)
	if known || drop {
		t.Fatalf("expected zero quote price to be unknown, known=%v drop=%v", known, drop)
	}
}

func TestDropLowValueEventNativeDefaultsTo18Decimals(t *testing.T) {
	price := "2000"
	native := chain.NativeAddress
	e := &Engine{}
	token := store.Token{QuoteTokenAddress: native, QuoteUSDPrice: &price}

	drop, known := e.dropLowValueEvent(Event{QuoteAmountText: "4999999999999999"}, token)
	if !known || !drop {
		t.Fatalf("expected roughly 0.005 ETH to be dropped, known=%v drop=%v", known, drop)
	}
}

func TestDirectOwnEventMatchesSenderCaseInsensitively(t *testing.T) {
	user := store.User{UserID: 7, WalletAddress: "0xAbC"}
	if !directOwnEvent(Event{Sender: "0xabc"}, user) {
		t.Fatal("expected sender matching wallet to be recognized as own event")
	}
}
