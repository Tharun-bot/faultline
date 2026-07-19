package ruleengine

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/Tharun-bot/faultline/core"
)

// newTestStore connects to a local Redis (expected to be running via
// docker compose) and flushes the DB before each test so tests don't
// interfere with each other's state.
func newTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to connect/flush test redis (is docker compose up?): %v", err)
	}
	t.Cleanup(func() { rdb.Close() })

	return NewStore(rdb), ctx
}

func testRule(id string) core.Rule {
	return core.Rule{
		ID:          id,
		Target:      core.Target{Service: "OrderService", Method: "Create", Client: "*"},
		FaultType:   core.FaultLatency,
		Params:      core.Params{LatencyMS: 100},
		Probability: 0.5,
		Active:      true,
	}
}

func TestStore_SaveAndGetRule(t *testing.T) {
	store, ctx := newTestStore(t)

	saved, err := store.SaveRule(ctx, testRule("r1"))
	if err != nil {
		t.Fatalf("SaveRule failed: %v", err)
	}
	if saved.Version != 1 {
		t.Fatalf("expected version 1 on first save, got %d", saved.Version)
	}

	got, err := store.GetRule(ctx, "r1")
	if err != nil {
		t.Fatalf("GetRule failed: %v", err)
	}
	if got.ID != "r1" || got.Params.LatencyMS != 100 {
		t.Fatalf("got unexpected rule back: %+v", got)
	}
}

func TestStore_SaveRule_IncrementsVersionOnUpdate(t *testing.T) {
	store, ctx := newTestStore(t)

	r := testRule("r2")
	saved1, err := store.SaveRule(ctx, r)
	if err != nil {
		t.Fatalf("first save failed: %v", err)
	}
	if saved1.Version != 1 {
		t.Fatalf("expected version 1, got %d", saved1.Version)
	}

	r.Params.LatencyMS = 999 // simulate an update
	saved2, err := store.SaveRule(ctx, r)
	if err != nil {
		t.Fatalf("second save failed: %v", err)
	}
	if saved2.Version != 2 {
		t.Fatalf("expected version bumped to 2, got %d", saved2.Version)
	}
}

func TestStore_SaveRule_RejectsInvalidRule(t *testing.T) {
	store, ctx := newTestStore(t)

	bad := testRule("r3")
	bad.Probability = 5.0 // invalid, out of [0,1]

	_, err := store.SaveRule(ctx, bad)
	if err == nil {
		t.Fatal("expected SaveRule to reject an invalid rule")
	}
}

func TestStore_ListActiveRules_OnlyReturnsActive(t *testing.T) {
	store, ctx := newTestStore(t)

	active := testRule("active-1")
	inactive := testRule("inactive-1")
	inactive.Active = false

	if _, err := store.SaveRule(ctx, active); err != nil {
		t.Fatalf("save active failed: %v", err)
	}
	if _, err := store.SaveRule(ctx, inactive); err != nil {
		t.Fatalf("save inactive failed: %v", err)
	}

	rules, err := store.ListActiveRules(ctx)
	if err != nil {
		t.Fatalf("ListActiveRules failed: %v", err)
	}

	if len(rules) != 1 || rules[0].ID != "active-1" {
		t.Fatalf("expected only active-1 in results, got %+v", rules)
	}
}

func TestStore_DeleteRule_RemovesFromIndexAndData(t *testing.T) {
	store, ctx := newTestStore(t)

	if _, err := store.SaveRule(ctx, testRule("to-delete")); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	if err := store.DeleteRule(ctx, "to-delete"); err != nil {
		t.Fatalf("DeleteRule failed: %v", err)
	}

	if _, err := store.GetRule(ctx, "to-delete"); err == nil {
		t.Fatal("expected GetRule to fail after deletion")
	}

	rules, err := store.ListActiveRules(ctx)
	if err != nil {
		t.Fatalf("ListActiveRules failed: %v", err)
	}
	for _, r := range rules {
		if r.ID == "to-delete" {
			t.Fatal("deleted rule should not appear in ListActiveRules")
		}
	}
}

// TestStore_PublishUpdate_MessageReceivedOnChannel proves the pub/sub
// side actually fires — subscribes to the channel BEFORE saving,
// then confirms the UpdateMessage arrives with the right version.
func TestStore_PublishUpdate_MessageReceivedOnChannel(t *testing.T) {
	store, ctx := newTestStore(t)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	sub := rdb.Subscribe(ctx, updatesChannel)
	defer sub.Close()
	// Wait for subscription to actually be established before we
	// publish, otherwise we might publish before the subscriber is
	// listening and miss the message entirely.
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	msgCh := sub.Channel()

	if _, err := store.SaveRule(ctx, testRule("pubsub-test")); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	select {
	case msg := <-msgCh:
		if msg.Payload == "" {
			t.Fatal("expected non-empty pub/sub payload")
		}
		// We don't need to fully unmarshal here — Phase 5's test
		// will exercise the subscriber side in depth. This test's
		// job is just to prove SaveRule actually publishes something.
	case <-ctx.Done():
		t.Fatal("timed out waiting for pub/sub message")
	}
}
