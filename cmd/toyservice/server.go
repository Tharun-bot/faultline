package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Tharun-bot/faultline/cmd/toyservice/proto"
	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/interceptors/grpcfault"
	"github.com/Tharun-bot/faultline/ruleengine"
	"github.com/Tharun-bot/faultline/telemetry"
)

// orderServer is the real (trivial) business logic. Faultline never
// touches this file's internals — it wraps around it via interceptors.
type orderServer struct {
	proto.UnimplementedOrderServiceServer
}

func (s *orderServer) Create(ctx context.Context, req *proto.CreateOrderRequest) (*proto.CreateOrderResponse, error) {
	return &proto.CreateOrderResponse{
		OrderId: "order-123",
		Status:  fmt.Sprintf("created: %d x %s", req.Quantity, req.Item),
	}, nil
}

// outcomeRecordingInterceptor records success/error against
// ServiceMetrics for EVERY call, regardless of whether the error came
// from real business logic or from Faultline's injection. This models
// what a real service's own instrumentation would already do in
// production — it doesn't distinguish "why" a call failed, only "did
// it fail." That's the correct signal for the rollback watcher, which
// deliberately never looks at faultline_injections_total.
func outcomeRecordingInterceptor(svcMetrics *telemetry.ServiceMetrics) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			svcMetrics.RecordError()
		} else {
			svcMetrics.RecordSuccess()
		}
		return resp, err
	}
}

// cacheRuleSource adapts ruleengine.Cache to grpcfault.RuleSource,
// so the interceptor can be handed a live, Redis-backed rule set
// instead of the hardcoded slice earlier phases used.
type cacheRuleSource struct {
	cache *ruleengine.Cache
}

func (c *cacheRuleSource) Find(cc core.CallContext) (core.Rule, bool) {
	return c.cache.Matcher().Find(cc)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx := context.Background()

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	otlpEndpoint := getEnv("OTLP_ENDPOINT", "")

	// "stdout" exporter for pure local dev with zero extra infra;
	// switches to "otlp" automatically once OTLP_ENDPOINT is set (as
	// it is inside docker-compose), pointed at the collector.
	exporterKind := "stdout"
	if otlpEndpoint != "" {
		exporterKind = "otlp"
	}
	shutdown, err := telemetry.InitTracing(ctx, "toyservice", exporterKind, otlpEndpoint)
	if err != nil {
		log.Fatalf("failed to init tracing: %v", err)
	}
	defer shutdown(ctx)

	metrics := telemetry.NewMetrics(prometheus.DefaultRegisterer)
	svcMetrics := telemetry.NewServiceMetrics(prometheus.DefaultRegisterer)

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	store := ruleengine.NewStore(rdb)
	cache := ruleengine.NewCache(rdb, store, 10*time.Second, 50*time.Millisecond)
	go cache.Start(ctx)

	// Give the cache a moment to complete its initial synchronous load
	// before we start accepting traffic, so the very first request
	// isn't served against an empty rule set.
	time.Sleep(200 * time.Millisecond)

	source := &cacheRuleSource{cache: cache}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("metrics listening on :9090/metrics")
		log.Fatal(http.ListenAndServe(":9090", nil))
	}()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// outcomeRecordingInterceptor is OUTERMOST so it observes the FINAL
	// result after grpcfault has had a chance to inject a failure —
	// an injected error must count as a real error for the rollback
	// watcher's purposes, just like it would for any real caller.
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			outcomeRecordingInterceptor(svcMetrics),
			grpcfault.UnaryServerInterceptor(source, metrics),
		),
	)
	proto.RegisterOrderServiceServer(grpcServer, &orderServer{})
	reflection.Register(grpcServer)

	log.Println("toyservice listening on :50051 (rules loaded live from Redis)")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
