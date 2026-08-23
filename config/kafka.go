package config

// KafkaConfig defines the Kafka broker and consumer group used by order-service.
type KafkaConfig struct {
	Brokers           []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"127.0.0.1:9092"`
	GroupID           string   `env:"KAFKA_ORDER_GROUP_ID" envDefault:"order-service"`
	CreateTopic       string   `env:"KAFKA_ORDER_CREATE_TOPIC" envDefault:"order.create"`
	CreateResultTopic string   `env:"KAFKA_ORDER_CREATE_RESULT_TOPIC" envDefault:"order.create.result"`
	ConfirmTopic      string   `env:"KAFKA_ORDER_CONFIRM_TOPIC" envDefault:"order.confirm"`
	FailTopic         string   `env:"KAFKA_ORDER_FAIL_TOPIC" envDefault:"order.fail"`
	PreviewTopic      string   `env:"KAFKA_ORDER_PREVIEW_TOPIC" envDefault:"order.projection.preview"`
	RequirementsTopic string   `env:"KAFKA_ORDER_REQUIREMENTS_TOPIC" envDefault:"order.projection.requirements"`
	DeliveryTopic     string   `env:"KAFKA_ORDER_DELIVERY_TOPIC" envDefault:"order.projection.delivery"`
}
