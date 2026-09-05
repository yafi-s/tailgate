package hedge

import (
	"sync"
	"time"
)

// Each transition increments the generation. A slow result from an older
// generation cannot accidentally close a circuit opened by newer failures.
type breaker struct {
	mu         sync.Mutex
	generation uint64
	failures   int
	openUntil  time.Time
	probe      bool
}
type ticket struct {
	generation uint64
	probe      bool
}
type outcome int

const (
	neutral outcome = iota // caller cancellation: no evidence about backend health
	success
	failure
)

func (b *breaker) acquire(now time.Time) (ticket, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.openUntil.IsZero() {
		if now.Before(b.openUntil) || b.probe {
			return ticket{}, false
		}
		b.probe = true
		return ticket{b.generation, true}, true
	}
	return ticket{b.generation, false}, true
}
func (b *breaker) finish(t ticket, result outcome, threshold int, cooldown time.Duration, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t.generation != b.generation {
		return
	}
	if result == neutral {
		if t.probe {
			b.probe = false
		}
		return
	}
	if result == success {
		b.failures = 0
		if t.probe {
			b.openUntil = time.Time{}
			b.probe = false
			b.generation++
		}
		return
	}
	b.failures++
	if t.probe || b.failures >= threshold {
		b.openUntil = now.Add(cooldown)
		b.probe = false
		b.generation++
	}
}

// Credits bound aggregate amplification: hedges <= burst + fraction*admissions.
// Idle time earns nothing. Credits are spent only when an extra attempt starts.
type budget struct {
	mu                          sync.Mutex
	credits, fraction, capacity float64
}

func (b *budget) earn() {
	b.mu.Lock()
	b.credits = min(b.capacity, b.credits+b.fraction)
	b.mu.Unlock()
}
func (b *budget) take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fraction == 0 || b.credits < 1 {
		return false
	}
	b.credits--
	return true
}
