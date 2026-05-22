package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/natstrace"
	"order-service/config"
	eb "order-service/internal/presentation/event_broker"
)

type broker struct {
	conn *nats.Conn
	js   nats.JetStreamContext
	log  logging.Logger
}

// NewBroker connects to NATS JetStream and returns the runtime broker.
func NewBroker(cfg config.NATSConfig, log logging.Logger) (eb.EventBroker, error) {
	if log == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	opts := []nats.Option{nats.Name("order-service"), nats.MaxReconnects(-1)}
	if cfg.User != "" || cfg.Password != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	conn, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, WrapConnectToNATSError(err)
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, WrapConnectToNATSError(err)
	}
	return &broker{conn: conn, js: js, log: log.With(logging.String("module", "jetstream-broker"))}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	if _, err := b.js.PublishMsg(natstrace.NewMessage(ctx, subject, payload)); err != nil {
		return WrapPublishToNATSError(subject, err)
	}
	return nil
}

func (b *broker) Subscribe(ctx context.Context, subject, durable string, handler eb.MessageHandler) error {
	_, err := b.js.Subscribe(subject, func(msg *nats.Msg) {
		msgCtx := natstrace.ContextFromMessage(ctx, msg)
		if err := handler(msgCtx, msg.Subject, msg.Data); err != nil {
			_ = msg.Nak()
			b.log.Error("jetstream message handler failed", logging.Operation("nats.message.handle"), logging.String("subject", msg.Subject), logging.Err(err))
			return
		}
		_ = msg.Ack()
	}, nats.Durable(durable), nats.ManualAck(), nats.AckExplicit())
	if err != nil {
		return WrapSubscribeToNATSError(subject, err)
	}
	return nil
}

func (b *broker) Close() {
	if b.conn != nil {
		b.conn.Close()
	}
}
