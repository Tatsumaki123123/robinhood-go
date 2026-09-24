package chain

import (
	"math/big"
	"strings"
	"testing"
)

func TestSignedWordKeepsTwoComplementValue(t *testing.T) {
	if got := signedWord(strings.Repeat("f", 64), 0); got.Cmp(big.NewInt(-1)) != 0 {
		t.Fatalf("expected -1, got %s", got)
	}
	if got := signedWord(strings.Repeat("0", 63)+"1", 0); got.Cmp(big.NewInt(1)) != 0 {
		t.Fatalf("expected 1, got %s", got)
	}
}

func TestEventWordInvalidInputIsZero(t *testing.T) {
	if got := eventWord(strings.Repeat("z", 64), 0); got.Sign() != 0 {
		t.Fatalf("expected invalid word to decode as zero, got %s", got)
	}
}

func TestStrategyEventFilterRejectsRawDeploymentLog(t *testing.T) {
	if StrategyEventFilter(map[string]any{"address": "0xcurve", "topics": []any{"0xunknown"}}) {
		t.Fatal("raw deployment log must not consume the strategy queue")
	}
}

func TestStrategyEventFilterAcceptsDecodedV4Swap(t *testing.T) {
	if !StrategyEventFilter(map[string]any{"poolId": "0xpool", "side": "buy"}) {
		t.Fatal("decoded V4 swap must reach the strategy queue")
	}
}
