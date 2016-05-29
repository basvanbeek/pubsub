package kafka

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/basvanbeek/pubsub"
	"github.com/go-kit/kit/log"
)

// subscriber is a subscriber implementation for Kafka.
type subscriber struct {
	consumer sarama.Consumer
	topic    string

	config *subscriberConfig

	kerr error

	stop chan chan error
}

// SubscriberOption allows for friendly APIs.
type SubscriberOption func(*subscriberConfig) error

type subscriberConfig struct {
	partition       int32
	offset          func() int64
	broadcastOffset func(int64)
	logger          log.Logger
}

// Partition sets the partition ID option.
func Partition(p int32) SubscriberOption {
	return func(sc *subscriberConfig) error {
		if p < 0 || p > 100 {
			return ErrInvalidPartitionValue
		}
		sc.partition = p
		return nil
	}
}

// Offset will start the consumer at the given offset.
func Offset(o int64) SubscriberOption {
	return func(sc *subscriberConfig) error {
		if o < -2 {
			return ErrInvalidOffset
		}
		sc.offset = func() int64 { return o }
		return nil
	}
}

// OffsetNewest will start the consumer at newly incoming messages.
func OffsetNewest() SubscriberOption {
	return Offset(-1)
}

// OffsetOldest will start the consumer at the earliest available message.
func OffsetOldest() SubscriberOption {
	return Offset(-2)
}

// OffsetCallback will start the consumer at the value returned by the callback
// function.
func OffsetCallback(cb func() int64) SubscriberOption {
	return func(sc *subscriberConfig) error {
		sc.offset = cb
		return nil
	}
}

// BroadcastOffset sets the callback which will receive the offset of the
// message that has had the Done() method called on itself.
func BroadcastOffset(cb func(int64)) SubscriberOption {
	return func(sc *subscriberConfig) error {
		sc.broadcastOffset = cb
		return nil
	}
}

// SubLogger option to set the logger to use
func SubLogger(logger log.Logger) SubscriberOption {
	return func(sc *subscriberConfig) error {
		if logger == nil {
			return ErrNoLogger
		}
		sc.logger = logger
		return nil
	}
}

// NewSubscriber will initiate a Kafka subscriber.
func NewSubscriber(
	broker, topic string,
	options ...SubscriberOption,
) (pubsub.Subscriber, error) {
	if len(strings.Trim(broker, " \t")) == 0 {
		return nil, ErrNoBrokers
	}
	brokerHosts := strings.Split(broker, ",")
	if len(topic) == 0 {
		return nil, ErrNoTopic
	}
	// set sensible defaults
	sc := &subscriberConfig{
		partition:       0,
		offset:          func() int64 { return sarama.OffsetNewest },
		broadcastOffset: func(int64) {},
		logger:          log.NewNopLogger(),
	}
	// parse optional parameters
	for _, option := range options {
		if err := option(sc); err != nil {
			return nil, err
		}
	}

	c := sarama.NewConfig()
	c.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokerHosts, c)
	if err != nil {
		return nil, err
	}
	s := &subscriber{
		consumer: consumer,
		topic:    topic,
		config:   sc,
		stop:     make(chan chan error, 1),
	}
	s.consumer = consumer
	return s, nil
}

// Start will start consuming message on the Kafka topic
// partition and emit any messages to the returned channel.
// On start up, it will call the offset func provider to the subscriber
// to lookup the offset to start at.
// If it encounters any issues, it will populate the Err() error
// and close the returned channel.
func (s *subscriber) Start() <-chan pubsub.SubscriberMessage {
	output := make(chan pubsub.SubscriberMessage)

	pCnsmr, err := s.consumer.ConsumePartition(
		s.topic,
		s.config.partition,
		s.config.offset(),
	)
	if err != nil {
		s.config.logger.Log(
			"msg", "unable to create partition consumer",
			"err", err,
		)
		close(output)
		return output
	}

	go func(
		s *subscriber,
		c sarama.PartitionConsumer,
		output chan pubsub.SubscriberMessage,
	) {
		defer close(output)
		var msg *sarama.ConsumerMessage
		errs := c.Errors()
		msgs := c.Messages()
		for {
			select {
			case exit := <-s.stop:
				exit <- c.Close()
				return
			case kerr := <-errs:
				s.kerr = kerr
				return
			case msg = <-msgs:
				output <- &subMessage{
					message:         msg,
					broadcastOffset: s.config.broadcastOffset,
				}
			}
		}
	}(s, pCnsmr, output)

	return output
}

// Stop will block until the consumer has stopped consuming messages and return
// any errors seen on consumer close.
func (s *subscriber) Stop() error {
	//	if
	exit := make(chan error)
	s.stop <- exit
	// close result from the partition consumer
	err := <-exit
	if err != nil {
		return err
	}
	return nil
}

// Err will contain any  errors that occurred during
// consumption. This method should be checked after
// a user encounters a closed channel.
func (s *subscriber) Err() error {
	return s.kerr
}

// GetKafkaPartitions is a helper function to look up which partitions are
// available via the given brokers for the given topic. This should be called
// only on startup.
func GetKafkaPartitions(brokers string, topic string) (partitions []int32, err error) {
	if len(strings.Trim(brokers, " \t")) == 0 {
		return nil, ErrNoBrokers
	}
	if len(topic) == 0 {
		return nil, ErrNoTopic
	}

	sc, err := sarama.NewConsumer(
		strings.Split(brokers, ","),
		sarama.NewConfig(),
	)
	if err != nil {
		return nil, err
	}

	defer func() {
		if cerr := sc.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	return sc.Partitions(topic)
}
