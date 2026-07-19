package kafkafault

import "context"

// Message is Faultline's own minimal representation of a Kafka
// message, decoupled from any specific Kafka client library. Only the
// fields fault injection actually needs to reason about are included.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}

// MessageHandler processes a single message — the natural shape for
// latency/error/corrupt/drop fault types, which each act on one
// message at a time.
type MessageHandler func(ctx context.Context, msg Message) error

// BatchResult describes the outcome of processing a batch of messages,
// used specifically for partial-failure injection. Index i in
// Succeeded corresponds to index i in the batch passed to the handler.
type BatchResult struct {
	Succeeded []bool
}

// BatchHandler processes a whole batch of messages at once — this is
// the shape partial-failure injection needs, since "succeed on the
// first K, fail the rest" is meaningless for a single message.
type BatchHandler func(ctx context.Context, msgs []Message) error
