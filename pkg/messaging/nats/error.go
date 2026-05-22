package nats

import "fmt"

// WrapConnectToNATSError annotates NATS connection failures.
func WrapConnectToNATSError(err error) error { return fmt.Errorf("connect nats: %w", err) }

// WrapInitJetStreamContextError annotates JetStream bootstrap failures.
func WrapInitJetStreamContextError(err error) error {
	return fmt.Errorf("init jetstream context: %w", err)
}

// WrapEnsureStreamError annotates JetStream stream creation failures.
func WrapEnsureStreamError(name string, addErr, updateErr error) error {
	return fmt.Errorf("ensure stream %s: add=%w update=%w", name, addErr, updateErr)
}
