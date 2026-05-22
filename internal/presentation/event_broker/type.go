package eventbroker

import app "order-service/internal/application"

// MessageHandler is the application-layer broker handler contract.
type MessageHandler = app.MessageHandler

// EventBroker is the application-layer publish contract.
type EventBroker = app.EventBroker
