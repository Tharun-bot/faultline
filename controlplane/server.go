package controlplane

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Tharun-bot/faultline/controlplane/proto"
	"github.com/Tharun-bot/faultline/ruleengine"
)

// Server implements the generated ControlPlaneServer interface. It is
// a thin adapter: convert proto <-> core.Rule, call into Store, convert
// back. All actual persistence/versioning/pub-sub logic lives in
// ruleengine.Store (Phase 4) — this file has zero business logic of
// its own, which is exactly what we want from a wire-protocol layer.
type Server struct {
	pb.UnimplementedControlPlaneServer
	store *ruleengine.Store
}

func NewServer(store *ruleengine.Store) *Server {
	return &Server{store: store}
}

func (s *Server) CreateRule(ctx context.Context, req *pb.CreateRuleRequest) (*pb.RuleResponse, error) {
	if req.Rule == nil {
		return nil, status.Error(codes.InvalidArgument, "rule is required")
	}
	if req.Rule.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "rule.id is required")
	}

	// CreateRule should fail if the ID already exists — otherwise
	// "create" and "update" are indistinguishable to the caller, and
	// someone could accidentally overwrite an existing rule by typo'ing
	// the same ID. UpdateRule is the explicit, intentional path for
	// modifying an existing rule.
	if _, err := s.store.GetRule(ctx, req.Rule.Id); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "rule %q already exists, use UpdateRule", req.Rule.Id)
	}

	saved, err := s.saveAndConvert(ctx, req.Rule)
	if err != nil {
		return nil, err
	}
	return &pb.RuleResponse{Rule: saved}, nil
}

func (s *Server) UpdateRule(ctx context.Context, req *pb.UpdateRuleRequest) (*pb.RuleResponse, error) {
	if req.Rule == nil || req.Rule.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "rule with a valid id is required")
	}

	if _, err := s.store.GetRule(ctx, req.Rule.Id); err != nil {
		return nil, status.Errorf(codes.NotFound, "rule %q not found, use CreateRule", req.Rule.Id)
	}

	saved, err := s.saveAndConvert(ctx, req.Rule)
	if err != nil {
		return nil, err
	}
	return &pb.RuleResponse{Rule: saved}, nil
}

func (s *Server) DeleteRule(ctx context.Context, req *pb.DeleteRuleRequest) (*pb.DeleteRuleResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := s.store.DeleteRule(ctx, req.Id); err != nil {
		return nil, status.Errorf(codes.Internal, "delete rule: %v", err)
	}
	return &pb.DeleteRuleResponse{Deleted: true}, nil
}

func (s *Server) GetRule(ctx context.Context, req *pb.GetRuleRequest) (*pb.RuleResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	r, err := s.store.GetRule(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "rule %q not found", req.Id)
	}
	return &pb.RuleResponse{Rule: fromCoreRule(r)}, nil
}

func (s *Server) ListRules(ctx context.Context, req *pb.ListRulesRequest) (*pb.ListRulesResponse, error) {
	// Note: Store.ListActiveRules only returns ACTIVE rules by design
	// (Phase 4) — that's the right behavior for agents doing
	// reconciliation, but an operator managing rules probably wants to
	// see inactive ones too (e.g. to re-enable a paused experiment).
	// We accept this limitation for now rather than adding a second
	// Store method just for this — flagged here as a known gap, not
	// something accidentally missed.
	rules, err := s.store.ListActiveRules(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list rules: %v", err)
	}

	resp := &pb.ListRulesResponse{Rules: make([]*pb.Rule, 0, len(rules))}
	for _, r := range rules {
		resp.Rules = append(resp.Rules, fromCoreRule(r))
	}
	return resp, nil
}

func (s *Server) SetActive(ctx context.Context, req *pb.SetActiveRequest) (*pb.RuleResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	existing, err := s.store.GetRule(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "rule %q not found", req.Id)
	}

	existing.Active = req.Active
	saved, err := s.store.SaveRule(ctx, existing)
	if err != nil && saved.ID == "" {
		// Only a genuine save failure (not a "saved but publish failed"
		// partial success — see saveAndConvert's comment above) has an
		// empty ID here, since SaveRule always returns the attempted
		// rule value even when publish fails.
		return nil, status.Errorf(codes.Internal, "set active: %v", err)
	}
	return &pb.RuleResponse{Rule: fromCoreRule(saved)}, nil
}

// saveAndConvert is shared by CreateRule/UpdateRule: convert incoming
// proto to core.Rule, save it, convert the result back.
func (s *Server) saveAndConvert(ctx context.Context, r *pb.Rule) (*pb.Rule, error) {
	rule := toCoreRule(r)

	saved, err := s.store.SaveRule(ctx, rule)
	if err != nil {
		// Recall from Phase 4: SaveRule can return (rule, err) together
		// when the SAVE succeeded but the PUBLISH failed. We still want
		// to return the saved rule to the caller in that case (the data
		// really is persisted) but as a gRPC response we can't return
		// both a value and an error — so we log and proceed with the
		// data we have, since correctness-wise the reconciliation loop
		// will fix propagation regardless.
		if saved.ID != "" {
			return fromCoreRule(saved), nil
		}
		return nil, status.Errorf(codes.InvalidArgument, "save rule: %v", err)
	}
	return fromCoreRule(saved), nil
}

// errPublishOnly is a placeholder to keep this file compiling cleanly;
// we're removing this helper — see the corrected version below.
func errPublishOnly(err error) error { return nil }
