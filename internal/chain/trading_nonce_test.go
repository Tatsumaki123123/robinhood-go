package chain

import (
	"context"
	"testing"
)

func TestReserveNonceCursorAndInvalidation(t *testing.T) {
	trading := &Trading{}
	from := "0x0000000000000000000000000000000000000001"
	unlock := trading.lockWallet(from)
	defer unlock()

	state := &nonceCursor{next: 8, initialized: true}
	trading.nonceCursors.Store(from, state)
	if got, err := trading.reserveNonce(context.Background(), from, nil); err != nil || got != 8 {
		t.Fatalf("expected nonce 8, got %d, err=%v", got, err)
	}
	trading.invalidateNonce(from)
	if state.initialized {
		t.Fatal("expected nonce cursor to be invalidated")
	}
}

func TestReserveExplicitReplacementDoesNotMoveCursorBack(t *testing.T) {
	trading := &Trading{}
	from := "0x0000000000000000000000000000000000000002"
	unlock := trading.lockWallet(from)
	defer unlock()

	state := &nonceCursor{next: 12, initialized: true}
	trading.nonceCursors.Store(from, state)
	requested := uint64(7)
	got, err := trading.reserveNonce(context.Background(), from, &requested)
	if err != nil || got != requested {
		t.Fatalf("expected replacement nonce %d, got %d, err=%v", requested, got, err)
	}
	if state.next != 12 {
		t.Fatalf("replacement should not move cursor backwards, got next=%d", state.next)
	}
}
