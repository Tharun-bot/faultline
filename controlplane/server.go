package controlplane

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Tharun-bot/faultline/controlplane/proto"
	"github.com/Tharun-bot/faultline/ruleengine"
)

// Server implements the generated ControlPlaneServer interface. It is
// a thin adapter: convert proto <-> core.Rule, call into Store, convert
// back. watcher may be nil (rollback tracking disabled) — checked
// before every use, same pattern as nil metrics elsewhere.
type Server struct {
	pb.UnimplementedControlPlaneServer
	store   *ruleengine.Store
	watcher *RollbackWatcher
}

func NewServer(store *ruleengine.Store, watcher *RollbackWatcher) *Server {
	return &Server{store: store, watcher: watcher}
}

func (s *Server) CreateRule(ctx context.Context, req *pb.CreateRuleRequest) (*pb.RuleResponse, error) {
	if req.Rule == nil {
		return nil, status.Error(codes.InvalidArgument, "rule is required")
	}
	if req.Rule.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "rule.id is required")
	}

	// CreateRule fails on an existing ID — "create" and "update" should
	// stay distinguishable to the caller.
	if _, err := s.store.GetRule(ctx, req.Rule.Id); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "rule %q already exists, use UpdateRule", req.Rule.Id)
	}

	saved, err := s.saveAndConvert(ctx, req.Rule)
	if err != nil {
		return nil, err
	}

	if s.watcher != nil && saved.Active {
		if err := s.watcher.StartExperiment(ctx, saved.Id); err != nil {
			slog.Error("failed to start experiment tracking", "rule_id", saved.Id, "err", err)
		}
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
	if s.watcher != nil {
		s.watcher.StopExperiment(req.Id)
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
	// Note: Store.ListActiveRules only returns ACTIVE rules by design —
	// good for agent reconciliation, but an operator managing rules
	// might want to see inactive ones too. Known scope cut, not an
	// oversight.
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
		// Only a genuine save failure has an empty ID here — SaveRule
		// always returns the attempted rule value even when the
		// publish-only step fails (see ruleengine.Store.SaveRule).
		return nil, status.Errorf(codes.Internal, "set active: %v", err)
	}

	if s.watcher != nil {
		if req.Active {
			if err := s.watcher.StartExperiment(ctx, req.Id); err != nil {
				slog.Error("failed to start experiment tracking", "rule_id", req.Id, "err", err)
			}
		} else {
			s.watcher.StopExperiment(req.Id)
		}
	}

	return &pb.RuleResponse{Rule: fromCoreRule(saved)}, nil
}

// saveAndConvert is shared by CreateRule/UpdateRule: convert incoming
// proto to core.Rule, save it, convert the result back.
func (s *Server) saveAndConvert(ctx context.Context, r *pb.Rule) (*pb.Rule, error) {
	rule := toCoreRule(r)

	saved, err := s.store.SaveRule(ctx, rule)
	if err != nil {
		// SaveRule can return (rule, err) together when the SAVE
		// succeeded but the PUBLISH failed — the data really is
		// persisted, so we still return it rather than erroring the
		// whole RPC; the reconciliation loop covers propagation.
		if saved.ID != "" {
			return fromCoreRule(saved), nil
		}
		return nil, status.Errorf(codes.InvalidArgument, "save rule: %v", err)
	}
	return fromCoreRule(saved), nil
}
