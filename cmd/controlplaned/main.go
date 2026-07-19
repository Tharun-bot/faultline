package main

import (
	"log"
	"net"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Tharun-bot/faultline/controlplane"
	pb "github.com/Tharun-bot/faultline/controlplane/proto"
	"github.com/Tharun-bot/faultline/ruleengine"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	store := ruleengine.NewStore(rdb)
	srv := controlplane.NewServer(store)

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterControlPlaneServer(grpcServer, srv)

	// Reflection lets tools like grpcurl discover the service's
	// methods and message shapes at runtime without needing the
	// .proto file locally. This is purely a dev/ops convenience — it's
	// common to disable this in production (it exposes your full API
	// surface to anyone who can reach the port), but for local manual
	// testing during development it's exactly what we want.
	reflection.Register(grpcServer)

	log.Println("control plane listening on :50052")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
