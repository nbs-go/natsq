package natsq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nbs-go/errx"
	"github.com/nbs-go/nlogger/v2"
	logContext "github.com/nbs-go/nlogger/v2/context"
	logOption "github.com/nbs-go/nlogger/v2/option"
	"runtime/debug"
	"time"
)

// Worker is a Queue Messaging adapter implemented using NATS
type Worker struct {
	client    *nats.Conn
	host      string
	port      uint16
	authToken string
	log       nlogger.Logger
	subs      map[string]*nats.Subscription
	handler   Handler
}

func NewWorker(host string, port uint16, authToken string, args ...InitOptionSetter) *Worker {
	log := nlogger.Get().NewChild(logOption.WithNamespace("natsq"))

	// Evaluate options for args
	opt := evaluateInitOptions(log, args)

	return &Worker{
		host:      host,
		port:      port,
		authToken: authToken,
		log:       log,
		handler:   opt.h,
		subs:      make(map[string]*nats.Subscription),
	}
}

func (w *Worker) Client() (*nats.Conn, error) {
	if w.client != nil && w.client.IsConnected() {
		return w.client, nil
	}

	host := fmt.Sprintf("nats://%s:%d", w.host, w.port)
	w.log.Tracef("Initiating connection to server... Host=%s)", host)
	c, err := nats.Connect(host,
		nats.Token(w.authToken),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				w.log.Errorf("Disconnected. Reason=%s", err)
			} else {
				w.log.Tracef("Disconnected")
			}
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			w.log.Tracef("Reconnecting...")
		}),
	)
	if err != nil {
		return nil, errx.Trace(err)
	}
	// Set client
	w.log.Tracef("Connected")
	w.client = c
	return w.client, nil
}

func (w *Worker) Close() {
	// Start Unsubscribing
	for topic, subscription := range w.subs {
		err := subscription.Unsubscribe()
		if err != nil {
			w.log.Warnf("Failed to unsubscribe. Topic=%s, Error=%s", topic, err)
		} else {
			w.log.Tracef("Success Unsubscribed. Topic=%s", topic)
		}
	}
	// Closing client
	w.client.Close()
	// Reset client
	w.client = nil
	// Reset subscriptions mapping
	w.subs = make(map[string]*nats.Subscription)
	w.log.Tracef("Nats connection has been closed")
}

func (w *Worker) Publish(ctx context.Context, topic string, data any) (err error) {
	// Create publish id
	ctx, pubId := NewPublishId(ctx)

	// Set publishId to message Header
	h := make(nats.Header)
	h.Set(HeaderPublishId, pubId)

	// Serialize data to json
	d, err := json.Marshal(data)
	if err != nil {
		w.log.Error("Failed to serialize payload to json. Topic=%s Payload=%+v", logOption.Format(topic, data), logOption.Error(err))
		return errx.Trace(err)
	}

	// Init message
	msg := nats.Msg{
		Subject: topic,
		Header:  h,
		Data:    d,
	}

	// Init client
	conn, err := w.Client()
	if err != nil {
		w.log.Error("Failed to initiate NATS client", logOption.Error(err))
		return errx.Trace(err)
	}

	return w.handler.Publish(ctx, conn, &msg)
}

// PublishMsg publish message using native nats.Conn PublishMsg function
func (w *Worker) PublishMsg(msg *nats.Msg) (err error) {
	// Init client
	client, err := w.Client()
	if err != nil {
		w.log.Error("Failed to initiate NATS client", logOption.Error(err))
		return errx.Trace(err)
	}
	return client.PublishMsg(msg)
}

func (w *Worker) MustPublish(ctx context.Context, topic string, payload any) {
	err := w.Publish(ctx, topic, payload)
	if err != nil {
		panic(errx.Trace(err))
	}
}

func (w *Worker) MustSubscribe(topic string, handler WorkerHandlerFunc) {
	err := w.Subscribe(topic, handler)
	if err != nil {
		panic(errx.Trace(err))
	}
}

// Subscribe handles subscription using WorkerHandler that wraps into native NATS SubscriberHandler
func (w *Worker) Subscribe(topic string, handler WorkerHandlerFunc) error {
	return w.SubscribeMsg(topic, wrapWorker(handler))
}

// SubscribeMsg handles subscription using native NATS SubscriberHandler
func (w *Worker) SubscribeMsg(topic string, handler SubscriberHandlerFunc) error {
	// Init client
	_, err := w.Client()
	if err != nil {
		w.log.Error("Failed to initiate NATS client", logOption.Error(err))
		return errx.Trace(err)
	}

	// Start subscription
	sub, err := w.client.QueueSubscribe(topic, topic, func(msg *nats.Msg) {
		// Init context and set startedAt to context
		ctx := context.Background()
		startedAt := time.Now()
		ctx = context.WithValue(ctx, ContextStartedAt, startedAt)

		// Capture publishId from message header
		pubId := msg.Header.Get(HeaderPublishId)
		ctx = context.WithValue(ctx, ContextPublishId, pubId)
		w.log.Tracef("Received message for Subscription. Topic=%s PublishId=%s", topic, pubId)

		w.callSubHandler(handler, ctx, msg)
		w.log.Tracef("Handling subscription done. Topic=%s PublishId=%s TimeElapsed=%s", topic, pubId, time.Since(startedAt))
	})
	if err != nil {
		w.log.Error("Failed to Subscribe. Topic=%s", logOption.Format(topic), logOption.Error(err))
		return err
	}

	// Register subscriptions
	w.subs[topic] = sub
	w.log.Tracef("Subscription started. Topic=%s", topic)
	return nil
}

func (w *Worker) callSubHandler(h SubscriberHandlerFunc, ctx context.Context, msg *nats.Msg) {
	panicked := true

	// Recover from panic
	defer func() {
		if r := recover(); r != nil || panicked {
			w.handler.OnSubscribePanic(ctx, msg.Subject, msg.Data, r, debug.Stack())
		}
	}()

	// Handle request
	h(ctx, msg)

	panicked = false
}

// NewPublishId retrieve publishId from logging requestId or generates new one
// requestId is useful for tracing related log
func NewPublishId(ctx context.Context) (context.Context, string) {
	// If available, retrieve publishId from requestId in log
	v := ctx.Value(logContext.RequestIdKey)
	if sv, ok := v.(string); ok && sv != "" {
		return ctx, sv
	}

	// Else, create new PublishId
	pubId := uuid.NewString()
	// Set to requestId for logging and tracing
	ctx = context.WithValue(ctx, logContext.RequestIdKey, pubId)
	return ctx, pubId
}
