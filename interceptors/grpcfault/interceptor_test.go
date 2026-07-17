package grpcfault_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Tharun-bot/faultline/cmd/toyservice/proto"
	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/interceptors/grpcfault"
)

// bufconn lets us run a real gRPC server + client in-process over an
// in-memory connection instead of binding a real TCP port. This makes
// the test fast, parallel-safe, and free of port-conflict flakiness —
// it's still a REAL gRPC call (real framing, real interceptors run),
// just without real sockets.
func startTestServer(t *testing.T, rules []core.Rule) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	source := grpcfault.NewStaticRuleSource(rules)

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(grpcfault.UnaryServerInterceptor(source)),
	)
	proto.RegisterOrderServiceServer(srv, &testOrderServer{})

	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}

// testOrderServer mirrors cmd/toyservice's real implementation, kept
// as a small local copy so this test package has no dependency on
// package main (which isn't importable anyway).
type testOrderServer struct {
	proto.UnimplementedOrderServiceServer
}

func (s *testOrderServer) Create(ctx context.Context, req *proto.CreateOrderRequest) (*proto.CreateOrderResponse, error) {
	return &proto.CreateOrderResponse{OrderId: "order-real", Status: "created"}, nil
}

func TestInterceptor_LatencyInjection_ActuallyDelaysCall(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "lat",
			Target:      core.Target{Service: "OrderService", Method: "Create", Client: "*"},
			FaultType:   core.FaultLatency,
			Params:      core.Params{LatencyMS: 200},
			Probability: 1.0,
			Active:      true,
		},
	}
	conn := startTestServer(t, rules)
	client := proto.NewOrderServiceClient(conn)

	start := time.Now()
	resp, err := client.Create(context.Background(), &proto.CreateOrderRequest{Item: "widget", Quantity: 1})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected success after delay, got error: %v", err)
	}
	if resp.OrderId != "order-real" {
		t.Fatalf("expected real response to still come through after latency, got %v", resp)
	}
	if elapsed < 180*time.Millisecond {
		t.Fatalf("expected call to take at least ~200ms due to injected latency, took %v", elapsed)
	}
}

func TestInterceptor_ErrorInjection_ShortCircuitsCall(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "err",
			Target:      core.Target{Service: "OrderService", Method: "Create", Client: "*"},
			FaultType:   core.FaultError,
			Params:      core.Params{ErrorCode: "UNAVAILABLE"},
			Probability: 1.0,
			Active:      true,
		},
	}
	conn := startTestServer(t, rules)
	client := proto.NewOrderServiceClient(conn)

	_, err := client.Create(context.Background(), &proto.CreateOrderRequest{Item: "widget", Quantity: 1})
	if err == nil {
		t.Fatal("expected injected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
}

func TestInterceptor_NoMatchingRule_PassesThroughUntouched(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "irrelevant",
			Target:      core.Target{Service: "SomeOtherService", Method: "Whatever", Client: "*"},
			FaultType:   core.FaultLatency,
			Params:      core.Params{LatencyMS: 5000},
			Probability: 1.0,
			Active:      true,
		},
	}
	conn := startTestServer(t, rules)
	client := proto.NewOrderServiceClient(conn)

	start := time.Now()
	resp, err := client.Create(context.Background(), &proto.CreateOrderRequest{Item: "widget", Quantity: 1})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if resp.OrderId != "order-real" {
		t.Fatalf("unexpected response: %v", resp)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("expected fast passthrough with no matching rule, took %v", elapsed)
	}
}
