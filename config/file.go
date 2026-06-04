package config

// FileServiceConfig defines the upstream file-service gRPC connection used by
// order-service to resolve public picture URLs for order gig snapshots.
type FileServiceConfig struct {
	Address string `env:"FILE_SERVICE_ADDRESS"`
}
