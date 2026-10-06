package keyedlocker_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"keyedlocker"
)

// waitFor polls cond until it holds, failing the test after a bounded
// deadline. It uses runtime.Gosched instead of sleeping.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// waitersOf reports the queued waiter count for key.
func waitersOf(lk *keyedlocker.Locker, key string) int {
	for _, s := range lk.Snapshot() {
		if s.Key == key {
			return s.Waiters
		}
	}
	return 0
}

func TestLockUnlock(t *testing.T) {
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	tok1, err := lk.Lock(ctx, "k")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if tok1 == 0 {
		t.Fatal("Lock returned zero token")
	}
	if _, err := lk.TryLock("k"); !errors.Is(err, keyedlocker.ErrLocked) {
		t.Fatalf("TryLock on held key = %v, want ErrLocked", err)
	}
	if err := lk.Unlock(tok1); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	tok2, err := lk.Lock(ctx, "k")
	if err != nil {
		t.Fatalf("re-Lock: %v", err)
	}
	if tok2 == 0 || tok2 == tok1 {
		t.Fatalf("tokens must be non-zero and unique, got %d then %d", tok1, tok2)
	}
	if err := lk.Unlock(tok2); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if lk.Len() != 0 {
		t.Fatalf("Len = %d, want 0 after all unlocks", lk.Len())
	}
}

func TestTryLock(t *testing.T) {
	lk := keyedlocker.New()
	defer lk.Close()

	tokA, err := lk.TryLock("a")
	if err != nil {
		t.Fatalf("TryLock(a): %v", err)
	}
	if tokA == 0 {
		t.Fatal("TryLock returned zero token")
	}
	if _, err := lk.TryLock("a"); !errors.Is(err, keyedlocker.ErrLocked) {
		t.Fatalf("TryLock(a) while held = %v, want ErrLocked", err)
	}
	// A different key is independent.
	tokB, err := lk.TryLock("b")
	if err != nil {
		t.Fatalf("TryLock(b) while a held: %v", err)
	}
	if err := lk.Unlock(tokA); err != nil {
		t.Fatalf("Unlock(a): %v", err)
	}
	// Available again after unlock.
	tokA2, err := lk.TryLock("a")
	if err != nil {
		t.Fatalf("TryLock(a) after unlock: %v", err)
	}
	if tokA2 == tokA {
		t.Fatal("token reused across acquisitions")
	}
	if err := lk.Unlock(tokA2); err != nil {
		t.Fatalf("Unlock(a2): %v", err)
	}
	if err := lk.Unlock(tokB); err != nil {
		t.Fatalf("Unlock(b): %v", err)
	}
}

func TestFIFOOrder(t *testing.T) {
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	hold, err := lk.Lock(ctx, "key")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	const waiters = 3
	var mu sync.Mutex
	order := make([]int, 0, waiters)
	var done atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tok, err := lk.Lock(ctx, "key")
			if err != nil {
				t.Errorf("waiter %d Lock: %v", id, err)
				return
			}
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			if err := lk.Unlock(tok); err != nil {
				t.Errorf("waiter %d Unlock: %v", id, err)
			}
			done.Add(1)
		}(i)
		// Ensure waiter i is queued before launching waiter i+1 so the
		// expected FIFO order is well defined.
		waitFor(t, fmt.Sprintf("waiter %d queued", i), func() bool {
			return waitersOf(lk, "key") == i+1
		})
	}

	if err := lk.Unlock(hold); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	waitFor(t, "all waiters served", func() bool { return done.Load() == waiters })
	wg.Wait()

	for i := 0; i < waiters; i++ {
		if order[i] != i {
			t.Fatalf("acquisition order = %v, want FIFO [0 1 2]", order)
		}
	}
}

func TestDifferentKeysIndependent(t *testing.T) {
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	tokA, err := lk.Lock(ctx, "a")
	if err != nil {
		t.Fatalf("Lock(a): %v", err)
	}

	type result struct {
		tok keyedlocker.Token
		err error
	}
	got := make(chan result, 1)
	go func() {
		tok, err := lk.Lock(ctx, "b")
		got <- result{tok, err}
	}()
	// Lock(b) must not wait for the holder of a.
	waitFor(t, "Lock(b) to return", func() bool { return len(got) == 1 })
	r := <-got
	if r.err != nil {
		t.Fatalf("Lock(b): %v", r.err)
	}
	if lk.Len() != 2 {
		t.Fatalf("Len = %d, want 2 while a and b are held", lk.Len())
	}
	if err := lk.Unlock(tokA); err != nil {
		t.Fatalf("Unlock(a): %v", err)
	}
	if err := lk.Unlock(r.tok); err != nil {
		t.Fatalf("Unlock(b): %v", err)
	}
}

func TestContextCancelRemovesOnlySelf(t *testing.T) {
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	hold, err := lk.Lock(ctx, "k")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(ctx)
	err1 := make(chan error, 1)
	go func() {
		_, err := lk.Lock(ctx1, "k")
		err1 <- err
	}()
	waitFor(t, "waiter 1 queued", func() bool { return waitersOf(lk, "k") == 1 })

	got2 := make(chan keyedlocker.Token, 1)
	err2 := make(chan error, 1)
	go func() {
		tok, err := lk.Lock(ctx, "k")
		if err != nil {
			err2 <- err
			return
		}
		got2 <- tok
	}()
	waitFor(t, "waiter 2 queued", func() bool { return waitersOf(lk, "k") == 2 })

	cancel1()
	waitFor(t, "waiter 1 to return", func() bool { return len(err1) == 1 })
	if err := <-err1; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter 1 err = %v, want context.Canceled", err)
	}
	// Only waiter 1 left the queue.
	waitFor(t, "queue to shrink to 1", func() bool { return waitersOf(lk, "k") == 1 })

	if err := lk.Unlock(hold); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	// Ownership passes to waiter 2, the next in line.
	waitFor(t, "waiter 2 to finish", func() bool { return len(got2)+len(err2) == 1 })
	select {
	case err := <-err2:
		t.Fatalf("waiter 2 err = %v", err)
	case tok := <-got2:
		if tok == 0 {
			t.Fatal("waiter 2 got zero token")
		}
		if err := lk.Unlock(tok); err != nil {
			t.Fatalf("waiter 2 Unlock: %v", err)
		}
	}
	if lk.Len() != 0 {
		t.Fatalf("Len = %d, want 0", lk.Len())
	}
}

func TestUnlockInvalidToken(t *testing.T) {
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	for _, tok := range []keyedlocker.Token{0, 1, 9999} {
		if err := lk.Unlock(tok); !errors.Is(err, keyedlocker.ErrUnknownToken) {
			t.Fatalf("Unlock(%d) = %v, want ErrUnknownToken", tok, err)
		}
	}

	tok, err := lk.Lock(ctx, "k")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := lk.Unlock(tok + 100); !errors.Is(err, keyedlocker.ErrUnknownToken) {
		t.Fatalf("Unlock(wrong token) = %v, want ErrUnknownToken", err)
	}
	// The real token still holds the lock.
	if _, err := lk.TryLock("k"); !errors.Is(err, keyedlocker.ErrLocked) {
		t.Fatalf("TryLock after wrong-token Unlock = %v, want ErrLocked", err)
	}
	if err := lk.Unlock(tok); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	// Reusing a released token fails too.
	if err := lk.Unlock(tok); !errors.Is(err, keyedlocker.ErrUnknownToken) {
		t.Fatalf("repeated Unlock = %v, want ErrUnknownToken", err)
	}
}

func TestSnapshotAndLen(t *testing.T) {
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	if lk.Len() != 0 {
		t.Fatalf("Len = %d, want 0", lk.Len())
	}
	if snap := lk.Snapshot(); len(snap) != 0 {
		t.Fatalf("Snapshot = %v, want empty", snap)
	}

	// Lock keys out of order to verify Snapshot sorts by key.
	tokB, err := lk.Lock(ctx, "b")
	if err != nil {
		t.Fatalf("Lock(b): %v", err)
	}
	tokA, err := lk.Lock(ctx, "a")
	if err != nil {
		t.Fatalf("Lock(a): %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := lk.Lock(ctx, "a")
			if err != nil {
				t.Errorf("queued Lock(a): %v", err)
				return
			}
			if err := lk.Unlock(tok); err != nil {
				t.Errorf("queued Unlock(a): %v", err)
			}
		}()
		waitFor(t, "waiter queued on a", func() bool { return waitersOf(lk, "a") == i+1 })
	}

	snap := lk.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot has %d entries, want 2", len(snap))
	}
	if snap[0].Key != "a" || snap[1].Key != "b" {
		t.Fatalf("Snapshot keys = [%s %s], want sorted [a b]", snap[0].Key, snap[1].Key)
	}
	if a := snap[0]; !a.Held || a.Token != tokA || a.Waiters != 2 {
		t.Fatalf("Snapshot[a] = %+v, want Held=true Token=%d Waiters=2", a, tokA)
	}
	if b := snap[1]; !b.Held || b.Token != tokB || b.Waiters != 0 {
		t.Fatalf("Snapshot[b] = %+v, want Held=true Token=%d Waiters=0", b, tokB)
	}
	if lk.Len() != 2 {
		t.Fatalf("Len = %d, want 2", lk.Len())
	}

	if err := lk.Unlock(tokB); err != nil {
		t.Fatalf("Unlock(b): %v", err)
	}
	if lk.Len() != 1 {
		t.Fatalf("Len = %d, want 1 after Unlock(b)", lk.Len())
	}

	// Releasing a hands ownership through the queue; each waiter unlocks.
	if err := lk.Unlock(tokA); err != nil {
		t.Fatalf("Unlock(a): %v", err)
	}
	wg.Wait()
	if lk.Len() != 0 {
		t.Fatalf("Len = %d, want 0 after queue drains", lk.Len())
	}
}

func TestClose(t *testing.T) {
	lk := keyedlocker.New()
	ctx := context.Background()

	holdA, err := lk.Lock(ctx, "a")
	if err != nil {
		t.Fatalf("Lock(a): %v", err)
	}
	holdB, err := lk.Lock(ctx, "b")
	if err != nil {
		t.Fatalf("Lock(b): %v", err)
	}

	errs := make(chan error, 2)
	go func() {
		_, err := lk.Lock(ctx, "a")
		errs <- err
	}()
	go func() {
		_, err := lk.Lock(ctx, "b")
		errs <- err
	}()
	waitFor(t, "waiters queued", func() bool {
		return waitersOf(lk, "a") == 1 && waitersOf(lk, "b") == 1
	})

	lk.Close()

	waitFor(t, "both waiters woken", func() bool { return len(errs) == 2 })
	for i := 0; i < 2; i++ {
		if err := <-errs; !errors.Is(err, keyedlocker.ErrClosed) {
			t.Fatalf("queued waiter err = %v, want ErrClosed", err)
		}
	}

	if _, err := lk.Lock(ctx, "a"); !errors.Is(err, keyedlocker.ErrClosed) {
		t.Fatalf("Lock after Close = %v, want ErrClosed", err)
	}
	if _, err := lk.TryLock("a"); !errors.Is(err, keyedlocker.ErrClosed) {
		t.Fatalf("TryLock after Close = %v, want ErrClosed", err)
	}

	// Tokens acquired before Close remain unlockable.
	if err := lk.Unlock(holdA); err != nil {
		t.Fatalf("Unlock(holdA) after Close: %v", err)
	}
	if err := lk.Unlock(holdB); err != nil {
		t.Fatalf("Unlock(holdB) after Close: %v", err)
	}
	if lk.Len() != 0 {
		t.Fatalf("Len = %d, want 0 after final unlocks", lk.Len())
	}

	// Close is idempotent; Snapshot and Len still work.
	lk.Close()
	if snap := lk.Snapshot(); len(snap) != 0 {
		t.Fatalf("Snapshot after Close = %v, want empty", snap)
	}
}

func TestConcurrentMutualExclusion(t *testing.T) {
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	const keys = 4
	const workers = 8
	const iters = 50

	var inside [keys]atomic.Int32
	var mu sync.Mutex
	seen := make(map[keyedlocker.Token]struct{}, workers*iters)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				k := (id + i) % keys
				key := fmt.Sprintf("key-%d", k)
				tok, err := lk.Lock(ctx, key)
				if err != nil {
					t.Errorf("Lock(%s): %v", key, err)
					return
				}
				if !inside[k].CompareAndSwap(0, 1) {
					t.Errorf("key %s held by two goroutines", key)
				}
				mu.Lock()
				if _, dup := seen[tok]; dup {
					t.Errorf("token %d issued twice", tok)
				}
				seen[tok] = struct{}{}
				mu.Unlock()
				inside[k].Store(0)
				if err := lk.Unlock(tok); err != nil {
					t.Errorf("Unlock(%s): %v", key, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if len(seen) != workers*iters {
		t.Fatalf("got %d unique tokens, want %d", len(seen), workers*iters)
	}
	if lk.Len() != 0 {
		t.Fatalf("Len = %d, want 0", lk.Len())
	}
}
