package natsq

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/nbs-go/nlogger/v2"
)

// SubscriberHandlerFunc is a native NATS handler that includes context
type SubscriberHandlerFunc func(ctx context.Context, msg *nats.Msg)

type Handler interface {
	OnSubscribePanic(ctx context.Context, topic string, data []byte, errValue any, stack []byte)
	Publish(ctx context.Context, conn *nats.Conn, msg *nats.Msg) error
}

type noHandler struct {
	log nlogger.Logger
}

func (n *noHandler) OnSubscribePanic(_ context.Context, topic string, data []byte, errValue any, stack []byte) {
	n.log.Errorf("Panic occurred on handling subscription. Topic=%s Data=%s CausedBy=%v\n  Stack=%s", topic, data, errValue, stack)
}

func (n *noHandler) Publish(_ context.Context, conn *nats.Conn, msg *nats.Msg) error {
	return conn.PublishMsg(msg)
}
