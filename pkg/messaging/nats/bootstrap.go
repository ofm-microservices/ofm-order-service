package nats

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-service/config"
)

type bootstrapConn interface {
	JetStream() (jetStreamManager, error)
	Close()
}

type jetStreamManager interface {
	AddStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error)
	UpdateStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error)
}

type realBootstrapConn struct{ *nats.Conn }

func (c realBootstrapConn) JetStream() (jetStreamManager, error) { return c.Conn.JetStream() }

var connectBootstrap = func(cfg config.NATSConfig) (bootstrapConn, error) {
	nc, err := Connect(cfg)
	if err != nil {
		return nil, err
	}
	return realBootstrapConn{Conn: nc}, nil
}

// EnsureStream creates or updates the JetStream streams required by
// order-service.
func EnsureStream(cfg config.NATSConfig, log logging.Logger) error {
	if log == nil {
		return fmt.Errorf("logger is nil")
	}
	nc, err := connectBootstrap(cfg)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return WrapInitJetStreamContextError(err)
	}
	commands := &nats.StreamConfig{
		Name:      cfg.OrderCommandsStream,
		Subjects:  []string{cfg.OrderCreateSubject, cfg.OrderConfirmSubject, cfg.OrderFailSubject, cfg.OrderReleaseRequestSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  1,
		MaxAge:    7 * 24 * time.Hour,
	}
	events := &nats.StreamConfig{
		Name:      cfg.OrderEventsStream,
		Subjects:  []string{cfg.OrderCreateResultSubject, cfg.OrderPreviewProjectionSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  1,
		MaxAge:    7 * 24 * time.Hour,
	}
	if _, err := js.AddStream(commands); err != nil {
		if _, updateErr := js.UpdateStream(commands); updateErr != nil {
			return WrapEnsureStreamError(commands.Name, err, updateErr)
		}
	}
	if _, err := js.AddStream(events); err != nil {
		if _, updateErr := js.UpdateStream(events); updateErr != nil {
			return WrapEnsureStreamError(events.Name, err, updateErr)
		}
	}
	return nil
}

// Connect establishes the low-level NATS connection used by bootstrap code.
func Connect(cfg config.NATSConfig) (*nats.Conn, error) {
	opts := []nats.Option{nats.Name("order-service"), nats.MaxReconnects(-1)}
	if cfg.User != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, WrapConnectToNATSError(err)
	}
	return nc, nil
}
