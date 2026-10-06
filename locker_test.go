package keyedlocker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var bg = context.Background()

// waitFor polls cond without sleeping and fails after a watchdog timeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func waitersOf(l *Locker, key string) int {
	for _, s := range l.Snapshot() {
		if s.Key == key {
			return s.Waiters
		}
	}
	return 0
}

// recv reads one value from ch, failing the test on watchdog timeout.
func recv[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
		var zero T
		return zero
	}
}

func TestLockUnlock(t *testing.T) {
	l := New()
	defer l.Close()

	tok, err := l.Lock(bg, "a")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if tok == 0 {
		t.Fatal("token must be non-zero")
	}
	if err := l.Unlock(tok); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	tok2, err := l.Lock(bg, "a")
	if err != nil {
		t.Fatalf("re-Lock: %v", err)
	}
	if tok2 == tok {
		t.Fatal("tokens must be unique across acquisitions")
	}
	if err := l.Unlock(tok2); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if got := l.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0 after all unlocks", got)
	}
}

func TestTryLock(t *testing.T) {
	l := New()
	defer l.Close()

	tokA, err := l.TryLock("a")
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if _, err := l.TryLock("a"); !errors.Is(err, ErrLocked) {
		t.Fatalf("TryLock on held key = %v, want ErrLocked", err)
	}
	tokB, err := l.TryLock("b")
	if err != nil {
		t.Fatalf("TryLock on different key: %v", err)
	}

	if err := l.Unlock(tokA); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if _, err := l.TryLock("a"); err != nil {
		t.Fatalf("TryLock after Unlock: %v", err)
	}
	if err := l.Unlock(tokB); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestFIFOOrder(t *testing.T) {
	l := New()
	defer l.Close()

	hold, err := l.Lock(bg, "k")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	const n = 3
	order := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tok, err := l.Lock(bg, "k")
			if err != nil {
				t.Errorf("Lock: %v", err)
				return
			}
			order <- id
			_ = l.Unlock(tok)
		}(i)
		waitFor(t, fmt.Sprintf("waiter %d queued", i), func() bool {
			return waitersOf(l, "k") == i+1
		})
	}

	if err := l.Unlock(hold); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	wg.Wait()
	close(order)
	want := 0
	for got := range order {
		if got != want {
			t.Fatalf("acquisition order: got waiter %d, want waiter %d", got, want)
		}
		want++
	}
}

func TestDifferentKeysIndependent(t *testing.T) {
	l := New()
	defer l.Close()

	tokA, err := l.Lock(bg, "a")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	type result struct {
		tok Token
		err error
	}
	ch := make(chan result, 1)
	go func() {
		tok, err := l.Lock(bg, "b")
		ch <- result{tok, err}
	}()

	r := recv(t, "lock on different key", ch)
	if r.err != nil {
		t.Fatalf("Lock on different key: %v", r.err)
	}
	if got := l.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2 while both keys held", got)
	}
	if err := l.Unlock(tokA); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if err := l.Unlock(r.tok); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestLockCancel(t *testing.T) {
	l := New()
	defer l.Close()

	hold, err := l.Lock(bg, "k")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(bg)
	err1 := make(chan error, 1)
	go func() {
		_, err := l.Lock(ctx1, "k")
		err1 <- err
	}()
	tok2 := make(chan Token, 1)
	go func() {
		tok, err := l.Lock(bg, "k")
		if err == nil {
			tok2 <- tok
		}
	}()
	waitFor(t, "two waiters queued", func() bool { return waitersOf(l, "k") == 2 })

	cancel1()
	if err := recv(t, "canceled Lock", err1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Lock error = %v, want context.Canceled", err)
	}
	waitFor(t, "queue shrinks to surviving waiter", func() bool {
		return waitersOf(l, "k") == 1
	})

	// The surviving waiter must still receive the lock on release.
	if err := l.Unlock(hold); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	tok := recv(t, "surviving waiter acquires", tok2)
	if err := l.Unlock(tok); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	// An already-canceled context fails immediately, even on a free key.
	ctx3, cancel3 := context.WithCancel(bg)
	cancel3()
	if _, err := l.Lock(ctx3, "free"); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Lock error = %v, want context.Canceled", err)
	}
}

func TestInvalidToken(t *testing.T) {
	l := New()
	defer l.Close()

	for _, tok := range []Token{0, 42, 1 << 40} {
		if err := l.Unlock(tok); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("Unlock(%d) = %v, want ErrInvalidToken", tok, err)
		}
	}

	tok, err := l.Lock(bg, "a")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := l.Unlock(tok); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if err := l.Unlock(tok); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("reused token: Unlock = %v, want ErrInvalidToken", err)
	}
}

func TestSnapshotAndLen(t *testing.T) {
	l := New()
	defer l.Close()

	if got := l.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
	if snap := l.Snapshot(); len(snap) != 0 {
		t.Fatalf("Snapshot len = %d, want 0", len(snap))
	}

	tokB, err := l.Lock(bg, "b")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	tokA, err := l.Lock(bg, "a")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if got := l.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}

	snap := l.Snapshot()
	if len(snap) != 2 || snap[0].Key != "a" || snap[1].Key != "b" {
		t.Fatalf("Snapshot not sorted by key: %+v", snap)
	}
	if snap[0].Holder != tokA || snap[1].Holder != tokB {
		t.Fatalf("Snapshot holders = %v, %v; want %v, %v",
			snap[0].Holder, snap[1].Holder, tokA, tokB)
	}
	if snap[0].Waiters != 0 || snap[1].Waiters != 0 {
		t.Fatalf("Snapshot waiters = %d, %d; want 0, 0",
			snap[0].Waiters, snap[1].Waiters)
	}

	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() {
		_, _ = l.Lock(ctx, "a")
		close(done)
	}()
	waitFor(t, "waiter queued", func() bool { return waitersOf(l, "a") == 1 })
	if got := l.Len(); got != 2 {
		t.Fatalf("Len = %d with waiter queued, want 2", got)
	}
	cancel()
	recv(t, "canceled waiter exits", done)
	waitFor(t, "waiter removed", func() bool { return waitersOf(l, "a") == 0 })

	if err := l.Unlock(tokA); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if err := l.Unlock(tokB); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if got := l.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0 after all unlocks", got)
	}
}

func TestClose(t *testing.T) {
	l := New()

	hold, err := l.Lock(bg, "k")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := l.Lock(bg, "k")
		errCh <- err
	}()
	waitFor(t, "waiter queued", func() bool { return waitersOf(l, "k") == 1 })

	l.Close()
	if err := recv(t, "waiting Lock", errCh); !errors.Is(err, ErrClosed) {
		t.Fatalf("waiting Lock error = %v, want ErrClosed", err)
	}
	if _, err := l.Lock(bg, "k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Lock after Close = %v, want ErrClosed", err)
	}
	if _, err := l.TryLock("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("TryLock after Close = %v, want ErrClosed", err)
	}

	// Tokens held before Close remain unlockable.
	if err := l.Unlock(hold); err != nil {
		t.Fatalf("Unlock after Close: %v", err)
	}
	if got := l.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0 after final unlock", got)
	}

	l.Close() // idempotent, must not panic
}

func TestConcurrentExclusion(t *testing.T) {
	l := New()
	defer l.Close()

	const keys = 4
	const workers = 4
	const iters = 25

	var held [keys]int32
	counts := make([]int, keys) // only touched while holding the key
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				k := (id + i) % keys
				key := fmt.Sprintf("key-%d", k)
				tok, err := l.Lock(bg, key)
				if err != nil {
					t.Errorf("Lock: %v", err)
					return
				}
				if !atomic.CompareAndSwapInt32(&held[k], 0, 1) {
					t.Errorf("key %s held by two goroutines at once", key)
				}
				counts[k]++
				atomic.StoreInt32(&held[k], 0)
				if err := l.Unlock(tok); err != nil {
					t.Errorf("Unlock: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	total := 0
	for _, c := range counts {
		total += c
	}
	if total != workers*iters {
		t.Fatalf("total acquisitions = %d, want %d", total, workers*iters)
	}
	if got := l.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0 after all unlocks", got)
	}
}
