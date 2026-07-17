package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Tharun-bot/faultline/cmd/toyservice/proto"
	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/interceptors/grpcfault"
)

// orderServer is the real (trivial) business logic. Faultline never
// touches this file's internals — it wraps around it via the interceptor.
type orderServer struct {
	proto.UnimplementedOrderServiceServer
}

func (s *orderServer) Create(ctx context.Context, req *proto.CreateOrderRequest) (*proto.CreateOrderResponse, error) {
	return &proto.CreateOrderResponse{
		OrderId: "order-123",
		Status:  fmt.Sprintf("created: %d x %s", req.Quantity, req.Item),
	}, nil
}

func main() {
	// Hardcoded static rules for Phase 3 — this is exactly what Phase
	// 4/5 will replace with live Redis-backed rules. Having this
	// hardcoded list here now makes it trivial to see the diff later:
	// we'll delete this slice and inject a Redis-backed RuleSource
	// implementing the same interface instead.
	rules := []core.Rule{
		{
			ID:          "demo-latency",
			Target:      core.Target{Service: "OrderService", Method: "Create", Client: "*"},
			FaultType:   core.FaultLatency,
			Params:      core.Params{LatencyMS: 500},
			Probability: 1.0, // always fire for this demo
			Active:      true,
		},
	}
	source := grpcfault.NewStaticRuleSource(rules)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpcfault.UnaryServerInterceptor(source)),
	)
	proto.RegisterOrderServiceServer(grpcServer, &orderServer{})
	reflection.Register(grpcServer)

	log.Println("toyservice listening on :50051 with Faultline interceptor active")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
