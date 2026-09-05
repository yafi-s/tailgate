package hedge

import (
	"testing"
	"time"
)

func TestCircuitGenerationAndSingleRecoveryProbe(t *testing.T) {
	var b breaker
	now := time.Unix(100, 0)
	old, _ := b.acquire(now)
	first, _ := b.acquire(now)
	b.finish(first, failure, 1, time.Second, now)
	b.finish(old, success, 1, time.Second, now) // stale success must not close
	if _, ok := b.acquire(now); ok {
		t.Fatal("open circuit admitted request")
	}
	probe, ok := b.acquire(now.Add(time.Second))
	if !ok {
		t.Fatal("no probe")
	}
	if _, ok := b.acquire(now.Add(time.Second)); ok {
		t.Fatal("multiple probes")
	}
	b.finish(probe, neutral, 1, time.Second, now)
	probe, ok = b.acquire(now.Add(time.Second))
	if !ok {
		t.Fatal("cancelled probe not released")
	}
	b.finish(probe, success, 1, time.Second, now.Add(time.Second))
	if _, ok := b.acquire(now.Add(time.Second)); !ok {
		t.Fatal("probe did not close circuit")
	}
}
func TestBudgetBound(t *testing.T) {
	b := budget{credits: 2, capacity: 2, fraction: .1}
	hedges := 0
	for admissions := 1; admissions <= 10000; admissions++ {
		b.earn()
		if b.take() {
			hedges++
		}
		if float64(hedges) > 2+float64(admissions)*.1 {
			t.Fatal("bound violated")
		}
	}
}
