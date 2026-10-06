// Package keyedlocker provides a concurrency-safe keyed mutual-exclusion
// locker. Different keys can be held in parallel, while the same key is
// held by at most one owner at a time and other callers wait in FIFO order.
package keyedlocker

import (
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrClosed is returned by Lock and TryLock after Close, and delivered
	// to waiters still queued when Close is called.
	ErrClosed = errors.New("keyedlocker: locker is closed")
	// ErrLocked is returned by TryLock when the key is currently held.
	ErrLocked = errors.New("keyedlocker: key is already locked")
	// ErrInvalidToken is returned by Unlock for zero, unknown, or
	// already-released tokens.
	ErrInvalidToken = errors.New("keyedlocker: invalid token")
)

// Token identifies one lock acquisition. It is unique and never zero.
type Token uint64

// KeySnapshot describes the lock state of a single key.
type KeySnapshot struct {
	Key     string
	Holder  Token // token of the current holder, never zero
	Waiters int   // number of waiters currently queued for the key
}

// Locker is a keyed mutual-exclusion locker. The zero value is not usable;
// create one with New.
type Locker struct {
	mu     sync.Mutex
	states map[string]*keyState // keys that currently have lock state
	owners map[Token]*keyState  // live holder token -> its key state
	last   uint64               // last issued token
	closed bool
}

// keyState tracks one key. A state exists in Locker.states only while the
// key is held (holder != 0); waiters may additionally be queued.
type keyState struct {
	key    string
	holder Token
	queue  []*waiter
}

// waiter is one queued Lock call. ch has capacity 1 and receives exactly
// one grant once done is set (under Locker.mu).
type waiter struct {
	ch   chan grant
	done bool
}

type grant struct {
	token Token
	err   error // ErrClosed when woken by Close
}

// New returns an empty Locker.
func New() *Locker {
	return &Locker{
		states: make(map[string]*keyState),
		owners: make(map[Token]*keyState),
	}
}

// nextToken issues a fresh non-zero token. Caller must hold l.mu.
func (l *Locker) nextToken() Token {
	l.last++
	return Token(l.last)
}

// acquireLocked takes key immediately. Caller must hold l.mu and must have
// verified the key has no state.
func (l *Locker) acquireLocked(key string) Token {
	tok := l.nextToken()
	st := &keyState{key: key, holder: tok}
	l.states[key] = st
	l.owners[tok] = st
	return tok
}

// Lock blocks until the caller holds key, ctx is canceled, or the locker is
// closed. On success it returns the unique non-zero token that must later be
// passed to Unlock. Waiters are served in FIFO order; a canceled waiter
// removes only itself and never disturbs the rest of the queue.
func (l *Locker) Lock(ctx context.Context, key string) (Token, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return 0, ErrClosed
	}
	if _, ok := l.states[key]; !ok {
		tok := l.acquireLocked(key)
		l.mu.Unlock()
		return tok, nil
	}
	st := l.states[key]
	w := &waiter{ch: make(chan grant, 1)}
	st.queue = append(st.queue, w)
	l.mu.Unlock()

	select {
	case g := <-w.ch:
		return g.token, g.err
	case <-ctx.Done():
	}

	l.mu.Lock()
	if w.done {
		// A grant (or Close) was already delivered concurrently with the
		// cancellation; consume it. If the lock was handed to us, pass
		// ownership on instead of leaking it.
		g := <-w.ch
		if g.err == nil {
			l.releaseLocked(g.token)
		}
		l.mu.Unlock()
		if g.err != nil {
			return 0, g.err
		}
		return 0, ctx.Err()
	}
	// Remove only ourselves from the queue.
	for i, q := range st.queue {
		if q == w {
			st.queue = append(st.queue[:i], st.queue[i+1:]...)
			break
		}
	}
	l.mu.Unlock()
	return 0, ctx.Err()
}

// TryLock acquires key without blocking. It returns ErrLocked if the key is
// currently held or has waiters, and ErrClosed after Close.
func (l *Locker) TryLock(key string) (Token, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	if _, ok := l.states[key]; ok {
		return 0, ErrLocked
	}
	return l.acquireLocked(key), nil
}

// Unlock releases the lock identified by token. Only the token of the
// current holder is accepted; zero, unknown, or already-released tokens
// return ErrInvalidToken. Ownership is handed directly to the next queued
// waiter, if any. Unlock remains valid for held tokens after Close.
func (l *Locker) Unlock(token Token) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.owners[token]; !ok {
		return ErrInvalidToken
	}
	l.releaseLocked(token)
	return nil
}

// releaseLocked releases the held token and either hands ownership to the
// next waiter or drops the key's state entirely. Caller must hold l.mu.
func (l *Locker) releaseLocked(token Token) {
	st := l.owners[token]
	delete(l.owners, token)
	if len(st.queue) > 0 {
		w := st.queue[0]
		st.queue = st.queue[1:]
		next := l.nextToken()
		st.holder = next
		l.owners[next] = st
		w.done = true
		w.ch <- grant{token: next}
		return
	}
	delete(l.states, st.key)
}

// Snapshot returns the current lock state of every key, sorted by key.
func (l *Locker) Snapshot() []KeySnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]KeySnapshot, 0, len(l.states))
	for _, st := range l.states {
		out = append(out, KeySnapshot{
			Key:     st.key,
			Holder:  st.holder,
			Waiters: len(st.queue),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Len returns the number of keys that currently have lock state.
func (l *Locker) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.states)
}

// Close wakes every queued waiter with ErrClosed and rejects all future
// Lock and TryLock calls. Tokens already held may still be unlocked.
// Close is idempotent.
func (l *Locker) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	for _, st := range l.states {
		for _, w := range st.queue {
			w.done = true
			w.ch <- grant{err: ErrClosed}
		}
		st.queue = nil
	}
}
