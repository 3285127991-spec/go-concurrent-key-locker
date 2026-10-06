// Command keyedlocker demonstrates the keyedlocker library: FIFO waiting
// on the same key, parallelism across different keys, context
// cancellation, and Close semantics.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"keyedlocker"
)

func main() {
	demoFIFO()
	demoParallelKeys()
	demoCancel()
	demoClose()
}

func waitersOf(lk *keyedlocker.Locker, key string) int {
	for _, s := range lk.Snapshot() {
		if s.Key == key {
			return s.Waiters
		}
	}
	return 0
}

// demoFIFO shows that waiters on one key are served in FIFO order.
func demoFIFO() {
	fmt.Println("== demo: same key is FIFO ==")
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tok, err := lk.Lock(ctx, "shared")
			if err != nil {
				fmt.Printf("  worker %d: %v\n", id, err)
				return
			}
			fmt.Printf("  worker %d acquired (token %d)\n", id, tok)
			time.Sleep(100 * time.Millisecond) // simulate work
			if err := lk.Unlock(tok); err != nil {
				fmt.Printf("  worker %d unlock: %v\n", id, err)
				return
			}
			fmt.Printf("  worker %d released\n", id)
		}(i)
		time.Sleep(30 * time.Millisecond) // stagger starts so queue order is deterministic
	}
	wg.Wait()
	fmt.Println()
}

// demoParallelKeys shows that different keys are held concurrently.
func demoParallelKeys() {
	fmt.Println("== demo: different keys run in parallel ==")
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	start := time.Now()
	var wg sync.WaitGroup
	for _, key := range []string{"a", "b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			tok, err := lk.Lock(ctx, key)
			if err != nil {
				fmt.Printf("  %s: %v\n", key, err)
				return
			}
			fmt.Printf("  %s acquired at %s\n", key, time.Since(start).Round(time.Millisecond))
			time.Sleep(150 * time.Millisecond) // simulate work
			if err := lk.Unlock(tok); err != nil {
				fmt.Printf("  %s unlock: %v\n", key, err)
			}
		}(key)
	}
	wg.Wait()
	fmt.Printf("  both held at once; total %s (not 2x150ms)\n\n", time.Since(start).Round(time.Millisecond))
}

// demoCancel shows a canceled waiter leaving the queue without affecting
// the waiter behind it.
func demoCancel() {
	fmt.Println("== demo: context cancellation removes only that waiter ==")
	lk := keyedlocker.New()
	defer lk.Close()
	ctx := context.Background()

	hold, err := lk.Lock(ctx, "file")
	if err != nil {
		fmt.Printf("  lock: %v\n", err)
		return
	}

	impatientCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	impatient := make(chan error, 1)
	go func() {
		_, err := lk.Lock(impatientCtx, "file")
		impatient <- err
	}()
	patient := make(chan keyedlocker.Token, 1)
	go func() {
		tok, err := lk.Lock(ctx, "file")
		if err == nil {
			patient <- tok
		}
	}()
	time.Sleep(20 * time.Millisecond) // let both waiters queue up
	fmt.Printf("  queued waiters: %d\n", waitersOf(lk, "file"))

	if err := <-impatient; errors.Is(err, context.DeadlineExceeded) {
		fmt.Println("  impatient waiter: deadline exceeded, left the queue")
	}
	if err := lk.Unlock(hold); err != nil {
		fmt.Printf("  unlock: %v\n", err)
	}
	tok := <-patient
	fmt.Printf("  patient waiter acquired next (token %d)\n", tok)
	if err := lk.Unlock(tok); err != nil {
		fmt.Printf("  unlock: %v\n", err)
	}
	fmt.Println()
}

// demoClose shows Close waking queued waiters and rejecting new locks
// while held tokens stay unlockable.
func demoClose() {
	fmt.Println("== demo: Close wakes waiters and rejects new locks ==")
	lk := keyedlocker.New()
	ctx := context.Background()

	hold, err := lk.Lock(ctx, "db")
	if err != nil {
		fmt.Printf("  lock: %v\n", err)
		return
	}
	woke := make(chan error, 1)
	go func() {
		_, err := lk.Lock(ctx, "db")
		woke <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the waiter queue up

	lk.Close()
	fmt.Printf("  queued waiter woke with: %v\n", <-woke)
	if _, err := lk.Lock(ctx, "db"); errors.Is(err, keyedlocker.ErrClosed) {
		fmt.Println("  new Lock rejected: ErrClosed")
	}
	if _, err := lk.TryLock("db"); errors.Is(err, keyedlocker.ErrClosed) {
		fmt.Println("  new TryLock rejected: ErrClosed")
	}
	if err := lk.Unlock(hold); err == nil {
		fmt.Println("  token held before Close still unlocked cleanly")
	}
}
