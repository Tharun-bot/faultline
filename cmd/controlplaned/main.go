package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Tharun-bot/faultline/controlplane"
	pb "github.com/Tharun-bot/faultline/controlplane/proto"
	"github.com/Tharun-bot/faultline/ruleengine"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx := context.Background()

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	prometheusURL := getEnv("PROMETHEUS_URL", "http://localhost:9090")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	store := ruleengine.NewStore(rdb)

	watcher, err := controlplane.NewRollbackWatcher(store, prometheusURL, 5*time.Second, 0.20)
	if err != nil {
		log.Fatalf("failed to create rollback watcher: %v", err)
	}
	go watcher.Run(ctx)

	srv := controlplane.NewServer(store, watcher)

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterControlPlaneServer(grpcServer, srv)
	reflection.Register(grpcServer)

	log.Println("control plane listening on :50052")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
