package config

// NATSConfig defines NATS streams, subjects, and durable consumers used by
// order-service.
type NATSConfig struct {
	URL      string `env:"NATS_URL,required"`
	User     string `env:"NATS_USER"`
	Password string `env:"NATS_PASSWORD"`

	OrderCommandsStream string `env:"NATS_STREAM_ORDER_COMMANDS" envDefault:"ORDER_COMMANDS"`
	OrderEventsStream   string `env:"NATS_STREAM_ORDER_EVENTS" envDefault:"ORDER_EVENTS"`

	OrderCreateSubject       string `env:"NATS_SUBJECT_ORDER_CREATE" envDefault:"order.create"`
	OrderCreateResultSubject string `env:"NATS_SUBJECT_ORDER_CREATE_RESULT" envDefault:"order.create.result"`
	OrderConfirmSubject      string `env:"NATS_SUBJECT_ORDER_CONFIRM" envDefault:"order.confirm"`
	OrderFailSubject         string `env:"NATS_SUBJECT_ORDER_FAIL" envDefault:"order.fail"`

	OrderCreateDurable  string `env:"NATS_DURABLE_ORDER_CREATE" envDefault:"order_service_create"`
	OrderConfirmDurable string `env:"NATS_DURABLE_ORDER_CONFIRM" envDefault:"order_service_confirm"`
	OrderFailDurable    string `env:"NATS_DURABLE_ORDER_FAIL" envDefault:"order_service_fail"`
}
