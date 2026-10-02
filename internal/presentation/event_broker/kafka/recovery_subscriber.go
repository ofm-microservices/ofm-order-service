package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"order-service/config"
	app "order-service/internal/application"
)

// RecoverySubscriber consumes order-owned migration commands.
type RecoverySubscriber interface{ Subscribe(context.Context) error }

type recoverySubscriber struct {
	broker  app.EventBroker
	service app.Service
	cfg     config.KafkaConfig
	log     app.Logger
}

// NewRecoverySubscriber constructs the order recovery Kafka adapter.
func NewRecoverySubscriber(b app.EventBroker, service app.Service, cfg config.KafkaConfig, log app.Logger) (RecoverySubscriber, error) {
	if b == nil || service == nil || log == nil {
		return nil, errors.New("invalid order recovery subscriber dependency")
	}
	return &recoverySubscriber{broker: b, service: service, cfg: cfg, log: log.With(logging.String("module", "kafka-order-recovery-subscriber"))}, nil
}

func (s *recoverySubscriber) Subscribe(ctx context.Context) error {
	s.log.Info("starting order recovery Kafka consumer",
		logging.String("topic", s.cfg.RecoveryTopic),
		logging.String("group", s.cfg.RecoveryGroup),
		logging.String("brokers", strings.Join(s.cfg.Brokers, ",")),
	)
	err := s.broker.Subscribe(ctx, s.cfg.RecoveryTopic, s.cfg.RecoveryGroup, s.handle)
	if err != nil {
		s.log.Error("order recovery Kafka consumer stopped",
			logging.String("topic", s.cfg.RecoveryTopic),
			logging.String("group", s.cfg.RecoveryGroup),
			logging.Err(err),
		)
		return err
	}
	s.log.Info("order recovery Kafka consumer stopped cleanly",
		logging.String("topic", s.cfg.RecoveryTopic),
		logging.String("group", s.cfg.RecoveryGroup),
	)
	return nil
}

func (s *recoverySubscriber) handle(ctx context.Context, _ string, raw []byte) error {
	var command events.Envelope
	if err := json.Unmarshal(raw, &command); err != nil {
		return fmt.Errorf("decode order recovery command: %w", err)
	}
	if command.AggregateType != "order" && command.AggregateType != "orders" {
		return fmt.Errorf("unsupported order recovery aggregate_type=%q", command.AggregateType)
	}
	if !strings.EqualFold(command.Operation, "post") && !strings.EqualFold(command.Operation, "create") {
		return fmt.Errorf("unsupported order recovery operation=%s", command.Operation)
	}
	var p struct {
		SagaID              string `json:"saga_id"`
		OrderID             string `json:"order_id"`
		BuyerID             string `json:"buyer_id"`
		SellerID            string `json:"seller_id"`
		SellerUsername      string `json:"seller_username"`
		GigID               string `json:"gig_id"`
		GigTitle            string `json:"gig_title"`
		PictureFileID       string `json:"picture_file_id"`
		PackageID           string `json:"package_id"`
		PackageTier         string `json:"package_tier"`
		PackageDescription  string `json:"package_description"`
		PackageDeliveryDays int32  `json:"package_delivery_days"`
		PriceCents          int64  `json:"price_cents"`
		Currency            string `json:"currency"`
		IdempotencyKey      string `json:"idempotency_key"`
		RequestedAt         string `json:"requested_at"`
	}
	if err := json.Unmarshal(command.Payload, &p); err != nil {
		return fmt.Errorf("decode order recovery payload: %w", err)
	}
	if p.OrderID == "" {
		p.OrderID = command.AggregateID
	}
	if p.OrderID == "" {
		return resilience.Permanent(fmt.Errorf("order recovery command %s is missing order_id", command.CommandID))
	}
	if p.SagaID == "" {
		return resilience.Permanent(fmt.Errorf("order recovery command %s is missing saga_id", command.CommandID))
	}
	if p.IdempotencyKey == "" {
		p.IdempotencyKey = command.IdempotencyKey
	}
	for field, value := range map[string]string{
		"buyer_id":   p.BuyerID,
		"seller_id":  p.SellerID,
		"gig_id":     p.GigID,
		"package_id": p.PackageID,
	} {
		if strings.TrimSpace(value) == "" {
			return resilience.Permanent(fmt.Errorf("order recovery command %s is missing %s", command.CommandID, field))
		}
	}
	result, err := s.service.CreateDraftOrder(ctx, app.CreateDraftOrderCommand{SagaID: p.SagaID, OrderID: p.OrderID, BuyerID: p.BuyerID, SellerID: p.SellerID, SellerUsername: p.SellerUsername, GigID: p.GigID, GigTitle: p.GigTitle, PictureFileID: p.PictureFileID, PackageID: p.PackageID, PackageTier: p.PackageTier, PackageDescription: p.PackageDescription, PackageDeliveryDays: p.PackageDeliveryDays, PriceCents: p.PriceCents, Currency: p.Currency, IdempotencyKey: p.IdempotencyKey, RequestedAt: p.RequestedAt, SkipProjection: true})
	if err != nil {
		return err
	}
	body, err := json.Marshal(events.Envelope{EventID: command.EventID + ".completed", CommandID: command.CommandID, CorrelationID: command.CorrelationID, CausationID: command.EventID, IdempotencyKey: command.IdempotencyKey, TestRunID: command.TestRunID, EventType: "migration.recovery.completed", Operation: command.Operation, SchemaVersion: 1, AggregateType: "orders", AggregateID: result.OrderID, SourceService: "order-service-recovery", OccurredAt: time.Now().UTC(), Payload: mustJSON(result)})
	if err != nil {
		return err
	}
	if err := s.broker.Publish(context.WithoutCancel(ctx), s.cfg.RecoveryCompleted, body); err != nil {
		s.log.Error("order recovery completion publish failed", logging.String("topic", s.cfg.RecoveryCompleted), logging.String("command_id", command.CommandID), logging.Err(err))
		return err
	}
	s.log.Info("order recovery completed", logging.String("topic", s.cfg.RecoveryCompleted), logging.String("command_id", command.CommandID), logging.String("order_id", result.OrderID))
	return nil
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
