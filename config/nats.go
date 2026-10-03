package config

// NATSConfig defines NATS streams, subjects, and durable consumers used by
// order-service.
type NATSConfig struct {
	// URL is retained only for compatibility adapters; production uses Kafka.
	URL      string `env:"NATS_URL"`
	User     string `env:"NATS_USER"`
	Password string `env:"NATS_PASSWORD"`

	OrderCommandsStream string `env:"NATS_STREAM_ORDER_COMMANDS" envDefault:"ORDER_COMMANDS"`
	OrderEventsStream   string `env:"NATS_STREAM_ORDER_EVENTS" envDefault:"ORDER_EVENTS"`

	OrderCreateSubject                 string `env:"NATS_SUBJECT_ORDER_CREATE" envDefault:"order.create"`
	OrderCreateResultSubject           string `env:"NATS_SUBJECT_ORDER_CREATE_RESULT" envDefault:"order.create.result"`
	OrderPreviewProjectionSubject      string `env:"NATS_SUBJECT_ORDER_PREVIEW_PROJECTION_REQUESTED" envDefault:"order.projection.preview"`
	OrderRequirementsProjectionSubject string `env:"NATS_SUBJECT_ORDER_REQUIREMENTS_PROJECTION_REQUESTED" envDefault:"order.projection.requirements"`
	OrderDeliveryProjectionSubject     string `env:"NATS_SUBJECT_ORDER_DELIVERY_PROJECTION_REQUESTED" envDefault:"order.projection.delivery"`
	OrderFundedSubject                 string `env:"NATS_SUBJECT_ORDER_FUNDED" envDefault:"order.funded"`
	OrderConfirmSubject                string `env:"NATS_SUBJECT_ORDER_CONFIRM" envDefault:"order.confirm"`
	OrderFailSubject                   string `env:"NATS_SUBJECT_ORDER_FAIL" envDefault:"order.fail"`
	OrderReleaseRequestSubject         string `env:"NATS_SUBJECT_ORDER_RELEASE_REQUEST" envDefault:"order.release.request"`

	OrderCreateDurable                 string `env:"NATS_DURABLE_ORDER_CREATE" envDefault:"order_service_create"`
	OrderPreviewProjectionDurable      string `env:"NATS_DURABLE_ORDER_PREVIEW_PROJECTION" envDefault:"order_service_preview_projection"`
	OrderRequirementsProjectionDurable string `env:"NATS_DURABLE_ORDER_REQUIREMENTS_PROJECTION" envDefault:"order_service_requirements_projection"`
	OrderDeliveryProjectionDurable     string `env:"NATS_DURABLE_ORDER_DELIVERY_PROJECTION" envDefault:"order_service_delivery_projection"`
	OrderConfirmDurable                string `env:"NATS_DURABLE_ORDER_CONFIRM" envDefault:"order_service_confirm"`
	OrderFailDurable                   string `env:"NATS_DURABLE_ORDER_FAIL" envDefault:"order_service_fail"`
}
