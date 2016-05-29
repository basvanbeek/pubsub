package kafka

import (
	"time"

	"github.com/Shopify/sarama"
)

// subMessage is an SubscriberMessage implementation
// that will broadcast the message's offset when Done().
type subMessage struct {
	message         *sarama.ConsumerMessage
	broadcastOffset func(int64)
}

// Message will return the message payload.
func (m *subMessage) Message() []byte {
	return m.message.Value
}

// ExtendDoneDeadline has no effect on subMessage.
func (m *subMessage) ExtendDoneDeadline(time.Duration) error {
	return nil
}

// Done will emit the message's offset.
func (m *subMessage) Done() error {
	m.broadcastOffset(m.message.Offset)
	return nil
}
