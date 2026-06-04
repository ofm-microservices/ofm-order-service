package config

// UserServiceConfig defines the upstream user-service gRPC connection used by
// order-service to hydrate participant snapshots for order pages.
type UserServiceConfig struct {
	Address string `env:"USER_SERVICE_ADDRESS,required"`
}
