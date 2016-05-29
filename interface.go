package pubsub

import (
	"time"

	"github.com/golang/protobuf/proto"
)

// Publisher is our generic publishing interface.
type Publisher interface {
	// Publish will publish a protobuf message.
	Publish(string, proto.Message) error
	// PublishRaw will publish a raw byte array as a message.
	PublishRaw(string, []byte) error
	// Stop will initiate a graceful shutdown of the publisher connection and
	// flush any remaining messages to be sent
	Stop() error
}

// Subscriber is our generic subscriber interface.
type Subscriber interface {
	// Start will return a channel of raw messages.
	Start() <-chan SubscriberMessage
	// Err will contain any errors returned from the consumer connection.
	Err() error
	// Stop will initiate a graceful shutdown of the subscriber connection.
	Stop() error
}

// SubscriberMessage is a struct to encapsulate subscriber messages and provide
// a mechanism for acknowledging messages _after_ they've been processed.
type SubscriberMessage interface {
	Message() []byte
	ExtendDoneDeadline(time.Duration) error
	Done() error
}
