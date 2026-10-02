package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	commonevents "github.com/ofm-microservices/ofm-common/pkg/events"
	"github.com/ofm-microservices/ofm-common/pkg/idempotency"
	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	transportkafka "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	requestmetadata "github.com/ofm-microservices/ofm-common/pkg/observability/metadata"
	sharedmetrics "github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
	"order-service/config"
	eventbroker "order-service/internal/presentation/event_broker"
)

type broker struct {
	brokers           []string
	group, deadLetter string
	mu                sync.Mutex
	readers           []*kafka.Reader
	db                *sqlx.DB
}

// NewBroker constructs the Kafka broker used by order-service event adapters.
func NewBroker(cfg config.KafkaConfig) (eventbroker.EventBroker, error) {
	return newBroker(cfg, nil)
}

// NewBrokerWithDB enables durable event claims for order projections.
func NewBrokerWithDB(cfg config.KafkaConfig, db *sqlx.DB) (eventbroker.EventBroker, error) {
	return newBroker(cfg, db)
}
func newBroker(cfg config.KafkaConfig, db *sqlx.DB) (eventbroker.EventBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are empty")
	}
	return &broker{brokers: cfg.Brokers, group: cfg.GroupID, deadLetter: cfg.GroupID + ".dead-letter", db: db}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	enveloped, _, err := commonevents.Wrap(subject, payload)
	if err != nil {
		return err
	}
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, BatchSize: 100, BatchTimeout: 50 * time.Millisecond}
	defer w.Close()
	transportkafka.Published(subject, enveloped)
	return w.WriteMessages(ctx, kafka.Message{Value: enveloped, Headers: kafkaHeaders(ctx)})
}

func kafkaHeaders(ctx context.Context) []kafka.Header {
	headers := make([]kafka.Header, 0, 5)
	for key, value := range requestmetadata.OutgoingHeaders(ctx) {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return headers
}

func (b *broker) Subscribe(ctx context.Context, subject, durable string, handler eventbroker.MessageHandler) error {
	if strings.TrimSpace(durable) != "" {
		return b.runExplicitConsumerGroup(ctx, subject, durable, handler)
	}
	group := b.group
	go func() {
		_ = (resilience.KafkaRetryQueueConfig{Brokers: b.brokers, Group: group, MaxAttempts: resilience.DefaultRetryPolicy.MaxAttempts}).Run(ctx)
	}()
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: subject, GroupID: group, MinBytes: 1, MaxBytes: 10e6, MaxWait: 50 * time.Millisecond})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	defer r.Close()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		attempts := retryAttempt(msg.Headers)
		var event idempotency.Event
		if b.db != nil {
			event = idempotency.DecodeOrFingerprint(subject, msg.Value)
			claimed, claimErr := idempotency.ClaimDB(ctx, b.db, event)
			if claimErr != nil {
				return claimErr
			}
			if !claimed {
				continue
			}
		}
		payload, _, unwrapErr := commonevents.Unwrap(msg.Value)
		if unwrapErr != nil {
			return b.deadLetterMessage(ctx, subject, msg, attempts, unwrapErr)
		}
		transportkafka.Consumed(subject, msg.Partition, msg.Offset, attempts, payload)
		if strings.HasPrefix(subject, "migration.recovery.commands.") {
			payload = msg.Value
		}
		err = handler(kafkaprop.Context(ctx, msg.Headers), subject, payload)
		if err != nil {
			if b.db != nil {
				_ = idempotency.Release(ctx, b.db, event.EventID)
			}
			var permanent resilience.PermanentError
			if !errors.As(err, &permanent) && attempts < resilience.DefaultRetryPolicy.MaxAttempts {
				writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: resilience.RetryTopic(group), WriteTimeout: 5 * time.Second}
				queueErr := (resilience.KafkaRetryQueue{Writer: writer}).Enqueue(ctx, msg, subject, attempts+1, err)
				_ = writer.Close()
				if queueErr != nil {
					return queueErr
				}
				continue
			}
			return b.deadLetterMessage(ctx, subject, msg, attempts, err)
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func retryAttempt(headers []kafka.Header) int {
	for _, h := range headers {
		if h.Key == "x-ofm-retry-attempt" {
			if n, err := strconv.Atoi(string(h.Value)); err == nil && n > 0 {
				return n
			}
		}
	}
	return 1
}

func (b *broker) deadLetterMessage(ctx context.Context, subject string, msg kafka.Message, attempts int, cause error) error {
	payload, err := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", cause), Error: cause.Error(), FailedAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, WriteTimeout: 5 * time.Second}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
	cancel()
	_ = writer.Close()
	if err != nil {
		return err
	}
	sharedmetrics.IncKafkaDLQ(b.deadLetter)
	return nil
}

// runExplicitConsumerGroup keeps recovery partition readers separate from the
// kafka-go group Reader fetch path, which is unreliable with the local
// broker's advertised listener and topic metadata after broker restarts.
func (b *broker) runExplicitConsumerGroup(ctx context.Context, subject, groupID string, handler eventbroker.MessageHandler) error {
	cg, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		Brokers:     b.brokers,
		Dialer:      &kafka.Dialer{Timeout: 10 * time.Second, DualStack: true},
		Topics:      []string{subject},
		ID:          groupID,
		StartOffset: kafka.FirstOffset,
	})
	if err != nil {
		return fmt.Errorf("create Kafka consumer group topic=%s group=%s: %w", subject, groupID, err)
	}
	defer cg.Close()
	for {
		generation, err := cg.Next(ctx)
		if err != nil {
			return fmt.Errorf("get Kafka consumer generation topic=%s group=%s: %w", subject, groupID, err)
		}
		generation.Start(func(genCtx context.Context) {
			var workers sync.WaitGroup
			for _, assignment := range generation.Assignments[subject] {
				assignment := assignment
				workers.Add(1)
				go func() {
					defer workers.Done()
					reader := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: subject, Partition: assignment.ID, MinBytes: 1, MaxBytes: 16 << 20, MaxWait: 50 * time.Millisecond})
					defer reader.Close()
					if assignment.Offset >= 0 {
						_ = reader.SetOffset(assignment.Offset)
					}
					for genCtx.Err() == nil {
						message, fetchErr := reader.FetchMessage(genCtx)
						if fetchErr != nil {
							log.Printf("order recovery partition fetch failed topic=%s partition=%d: %v", subject, assignment.ID, fetchErr)
							return
						}
						event := idempotency.DecodeOrFingerprint(subject, message.Value)
						var envelope events.Envelope
						if jsonErr := json.Unmarshal(message.Value, &envelope); jsonErr == nil && envelope.CommandID != "" {
							event.EventID = envelope.CommandID
						}
						if b.db != nil {
							claimCtx, claimCancel := context.WithTimeout(context.WithoutCancel(genCtx), 30*time.Second)
							claimed, claimErr := idempotency.ClaimDB(claimCtx, b.db, event)
							claimCancel()
							if claimErr != nil {
								log.Printf("order recovery idempotency claim failed topic=%s offset=%d: %v", subject, message.Offset, claimErr)
								return
							}
							if !claimed {
								if commitErr := generation.CommitOffsets(map[string]map[int]int64{subject: {assignment.ID: message.Offset + 1}}); commitErr != nil {
									return
								}
								continue
							}
						}
						payload, _, unwrapErr := commonevents.Unwrap(message.Value)
						if unwrapErr != nil {
							log.Printf("order Kafka envelope decode failed topic=%s offset=%d: %v", subject, message.Offset, unwrapErr)
							return
						}
						if strings.HasPrefix(subject, "migration.recovery.commands.") {
							payload = message.Value
						}
						handlerErr := resilience.Retry(genCtx, resilience.RetryPolicy{MaxAttempts: 1}, func(attemptCtx context.Context, attempt int) error {
							attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(attemptCtx), 30*time.Second)
							defer cancel()
							if strings.HasPrefix(subject, "migration.recovery.commands.") {
								log.Printf("order recovery attempt topic=%s attempt=%d partition=%d offset=%d", subject, attempt, message.Partition, message.Offset)
							}
							return handler(kafkaprop.Context(attemptCtx, message.Headers), subject, payload)
						})
						if handlerErr != nil {
							if b.db != nil {
								_ = idempotency.Release(context.Background(), b.db, event.EventID)
							}
							payload, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: message.Key, OriginalValue: message.Value, OriginalTopic: subject, OriginalPartition: message.Partition, OriginalOffset: message.Offset, ErrorClass: fmt.Sprintf("%T", handlerErr), Error: handlerErr.Error(), FailedAt: time.Now().UTC()})
							if marshalErr == nil {
								writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
								writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, Balancer: &kafka.Hash{}, WriteTimeout: 5 * time.Second}
								dlqErr := writer.WriteMessages(writeCtx, kafka.Message{Key: message.Key, Value: payload, Headers: message.Headers})
								writeCancel()
								_ = writer.Close()
								if dlqErr != nil {
									log.Printf("order recovery DLQ publish failed topic=%s offset=%d: %v", b.deadLetter, message.Offset, dlqErr)
								}
							}
						}
						if commitErr := generation.CommitOffsets(map[string]map[int]int64{subject: {assignment.ID: message.Offset + 1}}); commitErr != nil {
							return
						}
					}
				}()
			}
			workers.Wait()
		})
	}
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
