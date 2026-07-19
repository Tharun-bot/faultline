package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Tharun-bot/faultline/cmd/toyservice/proto"
	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/interceptors/grpcfault"
	"github.com/Tharun-bot/faultline/telemetry"
)

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
	ctx := context.Background()

	// "stdout" for now — prints spans to the terminal so you can see
	// this work with zero extra infrastructure. Phase 12 will switch
	// this to "otlp" pointed at a real collector feeding Jaeger.
	shutdown, err := telemetry.InitTracing(ctx, "toyservice", "stdout", "")
	if err != nil {
		log.Fatalf("failed to init tracing: %v", err)
	}
	defer shutdown(ctx)
	metrics := telemetry.NewMetrics(prometheus.DefaultRegisterer)

	rules := []core.Rule{
		{
			ID:          "demo-latency",
			Target:      core.Target{Service: "OrderService", Method: "Create", Client: "*"},
			FaultType:   core.FaultLatency,
			Params:      core.Params{LatencyMS: 500},
			Probability: 1.0,
			Active:      true,
		},
	}
	source := grpcfault.NewStaticRuleSource(rules)

	// Metrics served on a separate plain HTTP port — gRPC and HTTP
	// can't share one net.Listener directly without extra multiplexing
	// machinery (like cmux), and for a 30-hour project a second port
	// is a perfectly reasonable, simple choice worth naming as such if
	// asked in an interview.
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("metrics listening on :9090/metrics")
		log.Fatal(http.ListenAndServe(":9090", nil))
	}()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpcfault.UnaryServerInterceptor(source, metrics)),
	)
	proto.RegisterOrderServiceServer(grpcServer, &orderServer{})
	reflection.Register(grpcServer)

	log.Println("toyservice listening on :50051 with Faultline interceptor active")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
