package natsq

import (
	"context"
	"github.com/nats-io/nats.go"
)

// WorkerHandlerFunc is a default function for Worker type for handling subscription
// noRetry indicates function is done and no retry
type WorkerHandlerFunc func(ctx context.Context, topic string, data []byte) (noRetry bool)

func wrapWorker(handlerFunc WorkerHandlerFunc) SubscriberHandlerFunc {
	return func(ctx context.Context, msg *nats.Msg) {
		noRetry := handlerFunc(ctx, msg.Subject, msg.Data)

		if !noRetry {
			_ = msg.Nak()
			return
		}

		_ = msg.Ack()
	}
}
