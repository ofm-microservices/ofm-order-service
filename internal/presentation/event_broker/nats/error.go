package nats

import "fmt"

// WrapConnectToNATSError annotates connection failures.
func WrapConnectToNATSError(err error) error { return fmt.Errorf("connect nats: %w", err) }

// WrapPublishToNATSError annotates publication failures.
func WrapPublishToNATSError(subject string, err error) error {
	return fmt.Errorf("publish %s: %w", subject, err)
}

// WrapSubscribeToNATSError annotates subscription failures.
func WrapSubscribeToNATSError(subject string, err error) error {
	return fmt.Errorf("subscribe %s: %w", subject, err)
}
