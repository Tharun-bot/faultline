package ruleengine

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Tharun-bot/faultline/core"
)

// Cache is the agent-side component that keeps an in-memory Matcher
// fresh. It runs two background goroutines:
//   - subscribe(): reacts to pub/sub pointer messages, fast path
//   - reconcile(): periodic full resync, correctness backstop
//
// Interceptors (Phase 3/7/8) only ever call Cache.Matcher() to get the
// current *core.Matcher — they never see Redis, pub/sub, or locking.
type Cache struct {
	rdb   *redis.Client
	store *Store

	matcherPtr atomic.Pointer[core.Matcher]

	// rulesMu protects the plain map we keep alongside the Matcher.
	// We keep BOTH a map (for fast single-rule lookup/update by ID,
	// used when applying a pub/sub delta) and a Matcher (optimized for
	// Find() by CallContext) — updating one rebuilds the other. See
	// applyRuleUpdate for how they're kept in sync.
	rulesMu sync.Mutex
	rules   map[string]core.Rule

	reconcileInterval time.Duration
	maxJitter         time.Duration

	logger *slog.Logger
}

// NewCache constructs a Cache. reconcileInterval controls how often the
// full-resync backstop runs (e.g. 5-10s); maxJitter bounds the random
// delay before an agent fetches a rule after receiving a pub/sub
// pointer message (e.g. 50ms), to avoid a synchronized thundering herd
// of GETs against Redis when many agents receive the same message at
// the same instant.
func NewCache(rdb *redis.Client, store *Store, reconcileInterval, maxJitter time.Duration) *Cache {
	c := &Cache{
		rdb:               rdb,
		store:             store,
		rules:             make(map[string]core.Rule),
		reconcileInterval: reconcileInterval,
		maxJitter:         maxJitter,
		logger:            slog.Default(),
	}
	c.matcherPtr.Store(core.NewMatcher(nil))
	return c
}

// Matcher returns the current Matcher for interceptors to call Find on.
// This is lock-free and safe to call from any number of goroutines
// concurrently — that's the whole point of atomic.Pointer here.
func (c *Cache) Matcher() *core.Matcher {
	return c.matcherPtr.Load()
}

// Start launches the subscriber and reconciliation goroutines. It
// blocks until ctx is cancelled, so callers should run it in its own
// goroutine: `go cache.Start(ctx)`.
func (c *Cache) Start(ctx context.Context) {
	// Do one synchronous full load BEFORE returning control, so the
	// very first request the service handles already has whatever
	// rules existed at startup — without this, there'd be a window
	// right after boot where the Matcher is empty even though rules
	// exist in Redis.
	if err := c.reconcileOnce(ctx); err != nil {
		c.logger.Error("initial rule load failed", "err", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.subscribe(ctx)
	}()
	go func() {
		defer wg.Done()
		c.reconcileLoop(ctx)
	}()
	wg.Wait()
}

// subscribe listens on the pub/sub channel and reacts to each
// UpdateMessage by fetching just that one changed rule — NOT the full
// rule set. This is what keeps the pub/sub message itself tiny and
// keeps propagation fast: agents don't wait for a slow full resync,
// they react to a small pointer telling them exactly what changed.
func (c *Cache) subscribe(ctx context.Context) {
	sub := c.rdb.Subscribe(ctx, updatesChannel)
	defer sub.Close()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			c.handlePubSubMessage(ctx, msg.Payload)
		}
	}
}

func (c *Cache) handlePubSubMessage(ctx context.Context, payload string) {
	var upd UpdateMessage
	if err := json.Unmarshal([]byte(payload), &upd); err != nil {
		c.logger.Warn("failed to unmarshal pub/sub update message", "err", err)
		return
	}

	// Jitter: sleep a random duration up to maxJitter BEFORE fetching.
	// All agents receive this pub/sub message at roughly the same
	// instant. Without jitter, every agent in the fleet would issue its
	// GET to Redis in the same millisecond — a synchronized burst that
	// looks like a mini DDoS on your own rule store. Spreading the
	// fetch out over a random window turns one sharp spike into a flat
	// plateau of load.
	if c.maxJitter > 0 {
		jitter := time.Duration(rand.Int63n(int64(c.maxJitter)))
		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			return
		}
	}

	if upd.Deleted {
		c.removeRule(upd.RuleID)
		return
	}

	// Check our local version BEFORE fetching — if we already have
	// this version or newer (e.g. reconcile already caught it while we
	// were jittering), skip the redundant GET entirely.
	c.rulesMu.Lock()
	current, exists := c.rules[upd.RuleID]
	c.rulesMu.Unlock()
	if exists && current.Version >= upd.Version {
		return
	}

	rule, err := c.store.GetRule(ctx, upd.RuleID)
	if err != nil {
		c.logger.Warn("failed to fetch updated rule after pub/sub message",
			"rule_id", upd.RuleID, "err", err)
		// Not fatal — the reconciliation loop will catch this rule on
		// its next pass regardless of why this fetch failed.
		return
	}

	c.applyRule(rule)
}

// applyRule inserts or updates a single rule in the local map, then
// rebuilds the Matcher from the FULL current rule set. We always
// rebuild the whole Matcher rather than trying to mutate it in place,
// because core.Matcher's internal slice isn't designed for concurrent
// incremental mutation — rebuilding from the authoritative map is
// simple to reason about and, at the scale of dozens of rules, cheap
// enough to do on every single update.
func (c *Cache) applyRule(r core.Rule) {
	c.rulesMu.Lock()
	if !r.Active {
		// An inactive rule doesn't need to disappear from our map (we
		// still want to remember its version in case of stale-message
		// races), but the Matcher itself already filters inactive
		// rules at construction time — see core.NewMatcher.
		c.rules[r.ID] = r
	} else {
		c.rules[r.ID] = r
	}
	snapshot := c.snapshotLocked()
	c.rulesMu.Unlock()

	c.matcherPtr.Store(core.NewMatcher(snapshot))
}

// removeRule evicts a deleted rule from the local map entirely and
// rebuilds the Matcher.
func (c *Cache) removeRule(id string) {
	c.rulesMu.Lock()
	delete(c.rules, id)
	snapshot := c.snapshotLocked()
	c.rulesMu.Unlock()

	c.matcherPtr.Store(core.NewMatcher(snapshot))
}

// snapshotLocked returns a plain slice copy of the current rule map's
// values. MUST be called with rulesMu already held. Returning a copy
// (not ranging over the live map outside the lock) means the caller
// can safely pass this slice to core.NewMatcher after releasing the
// lock, without risking a concurrent map read/write.
func (c *Cache) snapshotLocked() []core.Rule {
	out := make([]core.Rule, 0, len(c.rules))
	for _, r := range c.rules {
		out = append(out, r)
	}
	return out
}

// reconcileLoop runs reconcileOnce on a fixed interval, forever, until
// ctx is cancelled. This is what bounds worst-case staleness: even if
// EVERY pub/sub message were somehow dropped, no agent could go stale
// for longer than reconcileInterval.
func (c *Cache) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(c.reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.reconcileOnce(ctx); err != nil {
				c.logger.Error("reconciliation failed", "err", err)
			}
		}
	}
}

// reconcileOnce does a full resync: fetch every currently-active rule
// from Redis (via Store.ListActiveRules) and REPLACE the entire local
// rule map with exactly that set. This is deliberately a full
// replace, not a merge — it means a rule that was deleted in Redis but
// somehow never triggered a "deleted" pub/sub message (e.g. that
// message was dropped) gets correctly evicted here too. A merge-only
// strategy could never fix that kind of drift.
func (c *Cache) reconcileOnce(ctx context.Context) error {
	rules, err := c.store.ListActiveRules(ctx)
	if err != nil {
		return err
	}

	fresh := make(map[string]core.Rule, len(rules))
	for _, r := range rules {
		fresh[r.ID] = r
	}

	c.rulesMu.Lock()
	c.rules = fresh
	snapshot := c.snapshotLocked()
	c.rulesMu.Unlock()

	c.matcherPtr.Store(core.NewMatcher(snapshot))
	return nil
}
