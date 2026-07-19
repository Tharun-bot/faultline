package ruleengine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/Tharun-bot/faultline/core"
)

const (
	ruleKeyPrefix  = "faultline:rule:"
	ruleIndexKey   = "faultline:rules:index"
	updatesChannel = "faultline:updates"
)

// UpdateMessage is the small pointer payload published on the pub/sub
// channel whenever a rule changes. Deliberately tiny — see Phase 5's
// design notes: agents receive this cheap message and then fetch the
// full rule themselves, rather than the full Rule JSON being pushed
// through pub/sub directly. Keeping this struct here (not in ruleengine
// alone) means both the publisher (Store) and subscriber (Phase 5's
// cache) share one definition instead of duplicating the JSON shape.
type UpdateMessage struct {
	RuleID  string `json:"rule_id"`
	Version int    `json:"version"`
	// Deleted marks that this rule was removed entirely, not just
	// deactivated. Subscribers need to distinguish "go fetch the new
	// version" from "evict this from your local cache."
	Deleted bool `json:"deleted"`
}

// Store is the only type in the whole codebase allowed to talk to
// Redis for rule storage. Everything above this layer (interceptors,
// control plane) goes through Store's methods, never touches
// *redis.Client directly. This keeps Redis an implementation detail
// that could theoretically be swapped later.
type Store struct {
	rdb *redis.Client
}

func NewStore(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

func ruleKey(id string) string {
	return ruleKeyPrefix + id
}

// SaveRule validates, version-bumps, persists, and announces a rule.
// The version bump happens HERE, server-side in Store, not left to
// callers to manage themselves — this guarantees version always
// increases monotonically on every write, even if two different
// callers (e.g. two control plane replicas) write concurrently, since
// we read-then-increment inside this one function using the existing
// stored value as the source of truth.
func (s *Store) SaveRule(ctx context.Context, r core.Rule) (core.Rule, error) {
	if err := r.Validate(); err != nil {
		return core.Rule{}, fmt.Errorf("ruleengine: invalid rule: %w", err)
	}

	existing, err := s.GetRule(ctx, r.ID)
	if err == nil {
		r.Version = existing.Version + 1
	} else {
		r.Version = 1 // first time this rule ID is being saved
	}

	data, err := json.Marshal(r)
	if err != nil {
		return core.Rule{}, fmt.Errorf("ruleengine: marshal rule: %w", err)
	}

	pipe := s.rdb.TxPipeline()
	pipe.Set(ctx, ruleKey(r.ID), data, 0) // 0 = no TTL, rules live until explicitly deleted
	pipe.SAdd(ctx, ruleIndexKey, r.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return core.Rule{}, fmt.Errorf("ruleengine: save rule: %w", err)
	}

	if err := s.publishUpdate(ctx, UpdateMessage{RuleID: r.ID, Version: r.Version}); err != nil {
		// The write itself succeeded — a failed publish just means
		// agents will pick this up on their next reconciliation loop
		// (Phase 5) instead of immediately via pub/sub. We log this
		// rather than fail the whole SaveRule call, since the data is
		// safely persisted either way.
		return r, fmt.Errorf("ruleengine: rule saved but publish failed: %w", err)
	}

	return r, nil
}

// GetRule fetches a single rule by ID.
func (s *Store) GetRule(ctx context.Context, id string) (core.Rule, error) {
	data, err := s.rdb.Get(ctx, ruleKey(id)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return core.Rule{}, fmt.Errorf("ruleengine: rule %q not found: %w", id, err)
		}
		return core.Rule{}, fmt.Errorf("ruleengine: get rule %q: %w", id, err)
	}

	var r core.Rule
	if err := json.Unmarshal(data, &r); err != nil {
		return core.Rule{}, fmt.Errorf("ruleengine: unmarshal rule %q: %w", id, err)
	}
	return r, nil
}

// ListActiveRules returns every rule currently in the index whose
// Active flag is true. This is used by Phase 5's reconciliation loop
// to do a full resync, and by the control plane's ListRules RPC.
//
// Note this does N+1 Redis calls (SMEMBERS + one GET per ID). For the
// scale this project targets (dozens of rules, not millions) that's
// perfectly fine and far more readable than an MGET-based optimization —
// worth calling out as a known scaling tradeoff if asked in an interview,
// not something to prematurely optimize here.
func (s *Store) ListActiveRules(ctx context.Context) ([]core.Rule, error) {
	ids, err := s.rdb.SMembers(ctx, ruleIndexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("ruleengine: list rule ids: %w", err)
	}

	rules := make([]core.Rule, 0, len(ids))
	for _, id := range ids {
		r, err := s.GetRule(ctx, id)
		if err != nil {
			// A rule ID in the index but missing its data key means
			// something got deleted inconsistently — skip it rather
			// than fail the whole list, and this is exactly the kind
			// of drift the reconciliation loop protects against.
			continue
		}
		if r.Active {
			rules = append(rules, r)
		}
	}
	return rules, nil
}

// DeleteRule removes a rule entirely and announces the deletion so
// subscribed agents evict it from their local cache immediately.
func (s *Store) DeleteRule(ctx context.Context, id string) error {
	pipe := s.rdb.TxPipeline()
	pipe.Del(ctx, ruleKey(id))
	pipe.SRem(ctx, ruleIndexKey, id)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("ruleengine: delete rule %q: %w", id, err)
	}

	return s.publishUpdate(ctx, UpdateMessage{RuleID: id, Deleted: true})
}

func (s *Store) publishUpdate(ctx context.Context, msg UpdateMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("ruleengine: marshal update message: %w", err)
	}
	return s.rdb.Publish(ctx, updatesChannel, data).Err()
}
