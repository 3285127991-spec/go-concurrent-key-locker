// Command keyedlocker demonstrates the keyedlocker library: FIFO waiting on
// the same key, parallel holders on different keys, context cancellation,
// and Close.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"keyedlocker"
)

func main() {
	demoFIFO()
	demoParallel()
	demoCancel()
	demoClose()
}

// waitWaiters blocks until key has at least n queued waiters.
func waitWaiters(l *keyedlocker.Locker, key string, n int) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, s := range l.Snapshot() {
			if s.Key == key && s.Waiters >= n {
				return
			}
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("timeout waiting for %d waiters on %q", n, key))
		}
		time.Sleep(time.Millisecond)
	}
}

func demoFIFO() {
	fmt.Println("== same key: FIFO order ==")
	l := keyedlocker.New()
	defer l.Close()
	ctx := context.Background()

	hold, _ := l.Lock(ctx, "alpha")
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tok, err := l.Lock(ctx, "alpha")
			if err != nil {
				fmt.Printf("waiter %d: %v\n", id, err)
				return
			}
			fmt.Printf("waiter %d acquired alpha (token %d)\n", id, tok)
			_ = l.Unlock(tok)
		}(i)
		waitWaiters(l, "alpha", i) // enqueue waiters in deterministic order
	}
	fmt.Println("main releases alpha")
	_ = l.Unlock(hold)
	wg.Wait()
	fmt.Println()
}

func demoParallel() {
	fmt.Println("== different keys: parallel holders ==")
	l := keyedlocker.New()
	defer l.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	acquired := make(chan string, 2)
	release := make(chan struct{})
	for _, key := range []string{"beta", "gamma"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			tok, _ := l.Lock(ctx, key)
			acquired <- key
			<-release
			_ = l.Unlock(tok)
		}(key)
	}
	first, second := <-acquired, <-acquired
	fmt.Printf("%s and %s are held at the same time\n", first, second)
	close(release)
	wg.Wait()
	fmt.Println()
}

func demoCancel() {
	fmt.Println("== cancel: only the canceled waiter leaves ==")
	l := keyedlocker.New()
	defer l.Close()
	ctx := context.Background()

	hold, _ := l.Lock(ctx, "delta")
	cctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := l.Lock(cctx, "delta")
		errCh <- err
	}()
	waitWaiters(l, "delta", 1)
	cancel()
	fmt.Println("canceled waiter got:", <-errCh)

	done := make(chan keyedlocker.Token, 1)
	go func() {
		tok, _ := l.Lock(ctx, "delta")
		done <- tok
	}()
	waitWaiters(l, "delta", 1)
	_ = l.Unlock(hold)
	tok := <-done
	fmt.Println("next waiter acquired delta after release")
	_ = l.Unlock(tok)
	fmt.Println()
}

func demoClose() {
	fmt.Println("== close: queued waiters wake up with ErrClosed ==")
	l := keyedlocker.New()
	ctx := context.Background()

	hold, _ := l.Lock(ctx, "epsilon")
	errCh := make(chan error, 1)
	go func() {
		_, err := l.Lock(ctx, "epsilon")
		errCh <- err
	}()
	waitWaiters(l, "epsilon", 1)

	l.Close()
	fmt.Println("waiting Lock returned:", <-errCh)
	if _, err := l.Lock(ctx, "epsilon"); err != nil {
		fmt.Println("new Lock after Close:", err)
	}
	if _, err := l.TryLock("epsilon"); err != nil {
		fmt.Println("TryLock after Close:", err)
	}
	fmt.Println("Unlock held token after Close:", l.Unlock(hold))
}
