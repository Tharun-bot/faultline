package ruleengine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Tharun-bot/faultline/core"
)

func newTestCacheDeps(t *testing.T) (*redis.Client, *Store, context.Context) {
	t.Helper()
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to connect/flush test redis: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })

	return rdb, NewStore(rdb), ctx
}

// TestCache_PubSub_PropagatesQuickly proves the fast path: a rule
// pushed via Store.SaveRule shows up in the Cache's Matcher well
// before the reconciliation interval would have caught it, proving
// pub/sub (not just periodic polling) is doing real work.
func TestCache_PubSub_PropagatesQuickly(t *testing.T) {
	rdb, store, ctx := newTestCacheDeps(t)

	// Reconcile interval deliberately long (10s) — if the rule shows up
	// well before that, we know pub/sub delivered it, not the reconciler.
	cache := NewCache(rdb, store, 10*time.Second, 20*time.Millisecond)

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go cache.Start(cacheCtx)

	// Give Start's initial synchronous load + subscriber goroutine a
	// moment to actually establish the subscription before we publish.
	time.Sleep(100 * time.Millisecond)

	rule := core.Rule{
		ID:          "fast-rule",
		Target:      core.Target{Service: "OrderService", Method: "Create", Client: "*"},
		FaultType:   core.FaultLatency,
		Params:      core.Params{LatencyMS: 100},
		Probability: 1.0,
		Active:      true,
	}
	if _, err := store.SaveRule(ctx, rule); err != nil {
		t.Fatalf("SaveRule failed: %v", err)
	}

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("rule did not propagate to Cache within 2s via pub/sub")
		case <-ticker.C:
			_, ok := cache.Matcher().Find(core.CallContext{
				Service: "OrderService", Method: "Create", Client: "anyone",
			})
			if ok {
				return // success — propagated well before the 10s reconcile interval
			}
		}
	}
}

// TestCache_Reconciliation_CatchesUpDespiteMissedPubSub simulates the
// failure mode pub/sub can't handle on its own: we save a rule
// DIRECTLY via the Store's Redis writes, bypassing any live subscriber
// by starting the Cache's subscription AFTER the write already
// happened (so the pub/sub message for it is already gone — pub/sub
// has no history/replay). Only the reconciliation loop can pick this
// up, since it does a full fetch from Redis rather than relying on
// having seen the announcement.
func TestCache_Reconciliation_CatchesUpDespiteMissedPubSub(t *testing.T) {
	rdb, store, ctx := newTestCacheDeps(t)

	rule := core.Rule{
		ID:          "missed-rule",
		Target:      core.Target{Service: "InventoryService", Method: "Check", Client: "*"},
		FaultType:   core.FaultError,
		Params:      core.Params{ErrorCode: "UNAVAILABLE"},
		Probability: 1.0,
		Active:      true,
	}
	// Save BEFORE the cache even exists — its pub/sub message has
	// nobody subscribed to receive it, so it's gone forever.
	if _, err := store.SaveRule(ctx, rule); err != nil {
		t.Fatalf("SaveRule failed: %v", err)
	}

	// Short reconcile interval so the test doesn't take long, but long
	// enough that if the rule appeared instantly we'd know something
	// other than reconciliation (i.e. a bug) explains it.
	cache := NewCache(rdb, store, 300*time.Millisecond, 20*time.Millisecond)

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go cache.Start(cacheCtx)

	// NOTE: Start() does a synchronous reconcileOnce BEFORE returning,
	// so actually this rule will appear almost immediately via that
	// initial load — which is correct and desired behavior (a freshly
	// booted agent should have current rules from second one). To
	// specifically test the PERIODIC reconcile loop's ability to catch
	// a change missed by pub/sub, we instead check a rule added AFTER
	// startup, via a direct Redis write that skips our own publish
	// path entirely (simulating a truly lost pub/sub message).
	time.Sleep(50 * time.Millisecond)

	lateRule := core.Rule{
		ID:          "late-rule",
		Target:      core.Target{Service: "PaymentService", Method: "Charge", Client: "*"},
		FaultType:   core.FaultLatency,
		Params:      core.Params{LatencyMS: 50},
		Probability: 1.0,
		Active:      true,
		Version:     1,
	}
	data, err := json.Marshal(lateRule)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	// Write directly via the raw client, deliberately bypassing
	// Store.SaveRule (and therefore its Publish call) to simulate a
	// pub/sub message that never arrived.
	if err := rdb.Set(ctx, ruleKey(lateRule.ID), data, 0).Err(); err != nil {
		t.Fatalf("direct redis set failed: %v", err)
	}
	if err := rdb.SAdd(ctx, ruleIndexKey, lateRule.ID).Err(); err != nil {
		t.Fatalf("direct redis sadd failed: %v", err)
	}

	// No pub/sub message was ever sent for late-rule. Only the
	// reconciliation loop (running every 300ms) can discover it.
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("reconciliation loop never picked up rule written without a pub/sub message")
		case <-ticker.C:
			_, ok := cache.Matcher().Find(core.CallContext{
				Service: "PaymentService", Method: "Charge", Client: "anyone",
			})
			if ok {
				return // success — only the periodic reconcile could have found this
			}
		}
	}
}
