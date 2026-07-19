package controlplane_test

import (
	"context"
	"net"
	"testing"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Tharun-bot/faultline/controlplane"
	pb "github.com/Tharun-bot/faultline/controlplane/proto"
	"github.com/Tharun-bot/faultline/ruleengine"
)

func startTestControlPlane(t *testing.T) pb.ControlPlaneClient {
	t.Helper()
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to connect/flush test redis: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })

	store := ruleengine.NewStore(rdb)
	srv := controlplane.NewServer(store)

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterControlPlaneServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewControlPlaneClient(conn)
}

func sampleRule(id string) *pb.Rule {
	return &pb.Rule{
		Id:          id,
		Target:      &pb.Target{Service: "OrderService", Method: "Create", Client: "*"},
		FaultType:   "latency",
		Params:      &pb.Params{LatencyMs: 250},
		Probability: 0.5,
		Active:      true,
	}
}

func TestControlPlane_CreateThenGet(t *testing.T) {
	client := startTestControlPlane(t)
	ctx := context.Background()

	created, err := client.CreateRule(ctx, &pb.CreateRuleRequest{Rule: sampleRule("cp-1")})
	if err != nil {
		t.Fatalf("CreateRule failed: %v", err)
	}
	if created.Rule.Version != 1 {
		t.Fatalf("expected version 1, got %d", created.Rule.Version)
	}

	got, err := client.GetRule(ctx, &pb.GetRuleRequest{Id: "cp-1"})
	if err != nil {
		t.Fatalf("GetRule failed: %v", err)
	}
	if got.Rule.Params.LatencyMs != 250 {
		t.Fatalf("unexpected rule: %+v", got.Rule)
	}
}

func TestControlPlane_CreateRule_RejectsDuplicateID(t *testing.T) {
	client := startTestControlPlane(t)
	ctx := context.Background()

	if _, err := client.CreateRule(ctx, &pb.CreateRuleRequest{Rule: sampleRule("dup-1")}); err != nil {
		t.Fatalf("first create failed: %v", err)
	}

	_, err := client.CreateRule(ctx, &pb.CreateRuleRequest{Rule: sampleRule("dup-1")})
	if err == nil {
		t.Fatal("expected AlreadyExists error on duplicate create")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", st.Code())
	}
}

func TestControlPlane_UpdateRule_RejectsUnknownID(t *testing.T) {
	client := startTestControlPlane(t)
	ctx := context.Background()

	_, err := client.UpdateRule(ctx, &pb.UpdateRuleRequest{Rule: sampleRule("does-not-exist")})
	if err == nil {
		t.Fatal("expected NotFound error updating nonexistent rule")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", st.Code())
	}
}

func TestControlPlane_UpdateRule_BumpsVersion(t *testing.T) {
	client := startTestControlPlane(t)
	ctx := context.Background()

	if _, err := client.CreateRule(ctx, &pb.CreateRuleRequest{Rule: sampleRule("up-1")}); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	updated := sampleRule("up-1")
	updated.Params.LatencyMs = 999
	resp, err := client.UpdateRule(ctx, &pb.UpdateRuleRequest{Rule: updated})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if resp.Rule.Version != 2 {
		t.Fatalf("expected version 2 after update, got %d", resp.Rule.Version)
	}
	if resp.Rule.Params.LatencyMs != 999 {
		t.Fatalf("expected updated latency, got %d", resp.Rule.Params.LatencyMs)
	}
}

func TestControlPlane_SetActive_TogglesFlag(t *testing.T) {
	client := startTestControlPlane(t)
	ctx := context.Background()

	if _, err := client.CreateRule(ctx, &pb.CreateRuleRequest{Rule: sampleRule("act-1")}); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	resp, err := client.SetActive(ctx, &pb.SetActiveRequest{Id: "act-1", Active: false})
	if err != nil {
		t.Fatalf("SetActive failed: %v", err)
	}
	if resp.Rule.Active {
		t.Fatal("expected rule to be inactive after SetActive(false)")
	}
}

func TestControlPlane_DeleteRule_RemovesIt(t *testing.T) {
	client := startTestControlPlane(t)
	ctx := context.Background()

	if _, err := client.CreateRule(ctx, &pb.CreateRuleRequest{Rule: sampleRule("del-1")}); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	delResp, err := client.DeleteRule(ctx, &pb.DeleteRuleRequest{Id: "del-1"})
	if err != nil || !delResp.Deleted {
		t.Fatalf("DeleteRule failed: %v", err)
	}

	_, err = client.GetRule(ctx, &pb.GetRuleRequest{Id: "del-1"})
	if err == nil {
		t.Fatal("expected GetRule to fail after deletion")
	}
}

func TestControlPlane_ListRules_OnlyActive(t *testing.T) {
	client := startTestControlPlane(t)
	ctx := context.Background()

	active := sampleRule("list-active")
	inactive := sampleRule("list-inactive")
	inactive.Active = false

	if _, err := client.CreateRule(ctx, &pb.CreateRuleRequest{Rule: active}); err != nil {
		t.Fatalf("create active failed: %v", err)
	}
	if _, err := client.CreateRule(ctx, &pb.CreateRuleRequest{Rule: inactive}); err != nil {
		t.Fatalf("create inactive failed: %v", err)
	}

	resp, err := client.ListRules(ctx, &pb.ListRulesRequest{})
	if err != nil {
		t.Fatalf("ListRules failed: %v", err)
	}
	found := false
	for _, r := range resp.Rules {
		if r.Id == "list-inactive" {
			t.Fatal("inactive rule should not appear in ListRules")
		}
		if r.Id == "list-active" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected active rule to appear in ListRules")
	}
}
