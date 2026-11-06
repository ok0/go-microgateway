// Package state carries stateful components across configuration reloads. A component is keyed by kind, name and a fingerprint of its own definition, and a reload reuses it while that key is unchanged.
package state

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
)

// ErrTypeMismatch reports that a component stored under a kind and name holds a value of a type other than the one requested.
var ErrTypeMismatch = errors.New("state: component has a different type")

// ErrConflict reports that one transaction requested the same component with two different fingerprints.
var ErrConflict = errors.New("state: component requested with two fingerprints in one transaction")

// ErrDone reports a Get on a transaction that has already been committed or aborted.
var ErrDone = errors.New("state: transaction already finished")

type key struct{ kind, name string }

type entry struct {
	key         key
	fingerprint string
	value       any
	refs        int  // leases holding the entry; guarded by Registry.mu
	registered  bool // guarded by Registry.mu
}

// Registry holds the components of the current configuration. At most one transaction is open at a time.
type Registry struct {
	logger  *slog.Logger
	sem     chan struct{} // capacity 1; holds a token while a transaction is open
	mu      sync.Mutex
	entries map[key]*entry
}

// Tx is one build's view of the registry. It is used by one goroutine.
type Tx struct {
	r       *Registry
	used    map[key]*entry
	created []*entry
	done    bool
}

// Lease keeps the components used by one committed transaction open until it is released.
type Lease struct {
	r        *Registry
	entries  []*entry
	released atomic.Bool
}

// NewRegistry returns an empty Registry that logs component close failures to logger. A nil logger discards them.
func NewRegistry(logger *slog.Logger) *Registry {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Registry{
		logger:  logger,
		sem:     make(chan struct{}, 1),
		entries: map[key]*entry{},
	}
}

// Begin opens a transaction. It waits until no other transaction is open.
func (r *Registry) Begin() *Tx {
	r.sem <- struct{}{}
	return &Tx{r: r, used: map[key]*entry{}}
}

// Len returns the number of components the registry holds.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// Close removes every component from the registry. Components that no lease holds are closed now; the others are closed when their last lease is released.
func (r *Registry) Close() {
	var collected []*entry
	r.mu.Lock()
	for k, e := range r.entries {
		e.registered = false
		delete(r.entries, k)
		if e.refs == 0 {
			collected = append(collected, e)
		}
	}
	r.mu.Unlock()
	r.closeLogged(collected)
}

// Get returns the component of the given kind and name. It reuses the registered component when its fingerprint matches, and otherwise creates one with create. Within one transaction a component must keep the same fingerprint.
func Get[T any](tx *Tx, kind, name, fingerprint string, create func() (T, error)) (T, error) {
	var zero T
	if tx.done {
		return zero, ErrDone
	}
	k := key{kind, name}
	if e, ok := tx.used[k]; ok {
		if e.fingerprint != fingerprint {
			return zero, fmt.Errorf("%w: %s %q", ErrConflict, kind, name)
		}
		v, ok := e.value.(T)
		if !ok {
			return zero, fmt.Errorf("%w: %s %q holds %T", ErrTypeMismatch, kind, name, e.value)
		}
		return v, nil
	}
	tx.r.mu.Lock()
	e, ok := tx.r.entries[k]
	tx.r.mu.Unlock()
	if ok && e.fingerprint == fingerprint {
		v, ok := e.value.(T)
		if !ok {
			return zero, fmt.Errorf("%w: %s %q holds %T", ErrTypeMismatch, kind, name, e.value)
		}
		tx.used[k] = e
		return v, nil
	}
	v, err := create()
	if err != nil {
		return zero, err
	}
	ne := &entry{key: k, fingerprint: fingerprint, value: v}
	tx.created = append(tx.created, ne)
	tx.used[k] = ne
	return v, nil
}

// Commit publishes the transaction's components as the registry's current set and finishes the transaction. Components it replaces are closed unless a lease holds them. The returned Lease keeps the components this transaction used open until it is released.
func (tx *Tx) Commit() *Lease {
	if tx.done {
		panic("state: Commit on a finished transaction")
	}
	r := tx.r
	var collected []*entry
	r.mu.Lock()
	for k, e := range r.entries {
		if tx.used[k] != e {
			e.registered = false
			delete(r.entries, k)
			if e.refs == 0 {
				collected = append(collected, e)
			}
		}
	}
	for _, e := range tx.created {
		e.registered = true
		r.entries[e.key] = e
	}
	lease := &Lease{r: r, entries: make([]*entry, 0, len(tx.used))}
	for _, e := range tx.used {
		e.refs++
		lease.entries = append(lease.entries, e)
	}
	r.mu.Unlock()
	tx.done = true
	<-r.sem
	r.closeLogged(collected)
	return lease
}

// Abort finishes the transaction without publishing its components and closes the components it created. Abort after Commit or Abort does nothing.
func (tx *Tx) Abort() {
	if tx.done {
		return
	}
	tx.done = true
	<-tx.r.sem
	tx.r.closeLogged(tx.created)
}

// Release drops the lease's hold on its components and closes those that are no longer registered and no longer held. It returns the joined close errors. Release on a nil or already released lease does nothing.
func (l *Lease) Release() error {
	if l == nil {
		return nil
	}
	if !l.released.CompareAndSwap(false, true) {
		return nil
	}
	var collected []*entry
	l.r.mu.Lock()
	for _, e := range l.entries {
		e.refs--
		if e.refs == 0 && !e.registered {
			collected = append(collected, e)
		}
	}
	l.r.mu.Unlock()
	var errs []error
	for _, e := range collected {
		if err := closeEntry(e); err != nil {
			errs = append(errs, fmt.Errorf("%s %q: %w", e.key.kind, e.key.name, err))
		}
	}
	return errors.Join(errs...)
}

func closeEntry(e *entry) error {
	if c, ok := e.value.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (r *Registry) closeLogged(es []*entry) {
	for _, e := range es {
		if err := closeEntry(e); err != nil {
			r.logger.Warn("state component close failed", "kind", e.key.kind, "name", e.key.name, "error", err)
		}
	}
}
