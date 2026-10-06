// Package keyedlocker provides a concurrency-safe keyed locker.
//
// Locks on different keys are fully independent and may be held in
// parallel. Locks on the same key are mutually exclusive: extra callers
// wait in a FIFO queue and ownership is handed directly from one holder
// to the next waiter.
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
	// ErrUnknownToken is returned by Unlock for zero, unknown, or
	// already-released tokens.
	ErrUnknownToken = errors.New("keyedlocker: unknown token")
)

// Token identifies one lock acquisition. It is unique per Locker and
// never zero for a successful acquisition.
type Token uint64

// KeySnapshot describes the lock state of one key.
type KeySnapshot struct {
	Key     string
	Held    bool
	Token   Token // current holder's token; 0 when not held
	Waiters int
}

// grant is the single message ever sent on a waiter's channel.
type grant struct {
	token Token
	err   error
}

// waiter is one queued Lock call.
type waiter struct {
	ch      chan grant // buffered (cap 1); exactly one send per waiter
	settled bool       // true once the grant has been queued on ch
}

// keyState is the per-key lock state.
type keyState struct {
	key   string
	held  bool
	token Token
	queue []*waiter
}

// Locker is a concurrency-safe keyed locker. Create one with New.
type Locker struct {
	mu      sync.Mutex
	closed  bool
	nextID  uint64
	byKey   map[string]*keyState
	byToken map[Token]*keyState
}

// New returns a ready-to-use Locker.
func New() *Locker {
	return &Locker{
		byKey:   make(map[string]*keyState),
		byToken: make(map[Token]*keyState),
	}
}

// Lock acquires the lock for key, waiting in FIFO order if another
// goroutine holds it. On success it returns a unique non-zero Token that
// must later be passed to Unlock.
//
// If ctx is canceled while waiting, only this caller leaves the queue and
// ctx.Err() is returned; other waiters are unaffected. If the locker is
// closed before or during the wait, ErrClosed is returned.
func (l *Locker) Lock(ctx context.Context, key string) (Token, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return 0, ErrClosed
	}
	ks := l.byKey[key]
	if ks == nil {
		ks = &keyState{key: key}
		l.byKey[key] = ks
	}
	if !ks.held && len(ks.queue) == 0 {
		tok := l.acquireLocked(ks)
		l.mu.Unlock()
		return tok, nil
	}
	w := &waiter{ch: make(chan grant, 1)}
	ks.queue = append(ks.queue, w)
	l.mu.Unlock()

	select {
	case g := <-w.ch:
		return g.token, g.err
	case <-ctx.Done():
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if w.settled {
		// The handoff raced with the cancellation. The grant is buffered
		// on w.ch; drain it and honor the delivered outcome.
		g := <-w.ch
		if g.err != nil {
			return 0, g.err
		}
		// Ownership was already transferred to us; pass it on to the
		// next waiter so the lock is neither leaked nor double-held.
		l.releaseLocked(ks)
		return 0, ctx.Err()
	}
	// Still queued: remove only this waiter.
	q := ks.queue
	for i, x := range q {
		if x == w {
			copy(q[i:], q[i+1:])
			q[len(q)-1] = nil
			ks.queue = q[:len(q)-1]
			break
		}
	}
	if !ks.held && len(ks.queue) == 0 {
		delete(l.byKey, key)
	}
	return 0, ctx.Err()
}

// TryLock acquires the lock for key only if it is immediately available.
// It returns ErrLocked if the key is held and ErrClosed after Close.
func (l *Locker) TryLock(key string) (Token, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	ks := l.byKey[key]
	if ks != nil && (ks.held || len(ks.queue) > 0) {
		return 0, ErrLocked
	}
	if ks == nil {
		ks = &keyState{key: key}
		l.byKey[key] = ks
	}
	return l.acquireLocked(ks), nil
}

// Unlock releases the acquisition identified by tok. It returns
// ErrUnknownToken for zero, unknown, or already-released tokens. Tokens
// acquired before Close remain unlockable afterwards.
func (l *Locker) Unlock(tok Token) error {
	if tok == 0 {
		return ErrUnknownToken
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ks, ok := l.byToken[tok]
	if !ok {
		return ErrUnknownToken
	}
	l.releaseLocked(ks)
	return nil
}

// Snapshot returns the state of every key that currently has lock state,
// sorted by key.
func (l *Locker) Snapshot() []KeySnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]KeySnapshot, 0, len(l.byKey))
	for key, ks := range l.byKey {
		out = append(out, KeySnapshot{
			Key:     key,
			Held:    ks.held,
			Token:   ks.token,
			Waiters: len(ks.queue),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Len returns the number of keys that currently have lock state (held or
// with queued waiters).
func (l *Locker) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.byKey)
}

// Close wakes every queued waiter with ErrClosed and rejects all future
// Lock and TryLock calls. Tokens already held remain unlockable. Close is
// idempotent.
func (l *Locker) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	for _, ks := range l.byKey {
		for _, w := range ks.queue {
			w.settled = true
			w.ch <- grant{err: ErrClosed}
		}
		ks.queue = nil
	}
}

// acquireLocked marks ks held under a fresh token. l.mu must be held.
func (l *Locker) acquireLocked(ks *keyState) Token {
	tok := l.nextTokenLocked()
	ks.held = true
	ks.token = tok
	l.byToken[tok] = ks
	return tok
}

// releaseLocked releases the hold on ks, handing ownership directly to the
// next queued waiter if any. l.mu must be held.
func (l *Locker) releaseLocked(ks *keyState) {
	delete(l.byToken, ks.token)
	if len(ks.queue) == 0 {
		ks.held = false
		ks.token = 0
		delete(l.byKey, ks.key)
		return
	}
	w := ks.queue[0]
	ks.queue = ks.queue[1:]
	tok := l.nextTokenLocked()
	ks.token = tok
	l.byToken[tok] = ks
	w.settled = true
	w.ch <- grant{token: tok}
}

// nextTokenLocked returns the next unique non-zero token. l.mu must be held.
func (l *Locker) nextTokenLocked() Token {
	l.nextID++
	return Token(l.nextID)
}
