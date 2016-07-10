package kafka

import (
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/log"
	"github.com/golang/protobuf/proto"

	"github.com/basvanbeek/pubsub"
)

const (
	defaultSyncPublisher = false
	defaultRequiredAcks  = sarama.WaitForAll
)

type SuccessHandler func(*sarama.ProducerMessage)

// syncPublisher is a blocking publisher that provides an implementation
// for Kafka using the Shopify/sarama library.
type syncPublisher struct {
	publisher sarama.SyncProducer
	topic     string
	logger    log.Logger
}

// asyncPublisher is a non-blocking publisher that provides an
// implementation for Kafka using the Shopify/sarama library.
type asyncPublisher struct {
	publisher sarama.AsyncProducer
	wg        sync.WaitGroup
	topic     string
	logger    log.Logger
	successes SuccessHandler
}

// PublisherOption allows for functional options / friendly API's
type PublisherOption func(*publisherConfig) error

type publisherConfig struct {
	syncPublisher bool
	partitioner   sarama.Partitioner
	partition     int32
	topic         string
	ackMode       sarama.RequiredAcks
	successes     SuccessHandler
	logger        log.Logger
}

// SyncPublisher option to return a synchronous blocking publisher
func SyncPublisher() PublisherOption {
	return func(pc *publisherConfig) error {
		pc.syncPublisher = true
		return nil
	}
}

// AckNoResponse option to sets the ack mode to NoResponse.
func AckNoResponse() PublisherOption {
	return func(pc *publisherConfig) error {
		pc.ackMode = sarama.NoResponse
		return nil
	}
}

// AckWaitForLocal option to sets the ack mode to WaitForLocal.
func AckWaitForLocal() PublisherOption {
	return func(pc *publisherConfig) error {
		pc.ackMode = sarama.WaitForLocal
		return nil
	}
}

// AckWaitForAll option to sets the ack mode to WaitForAll.
func AckWaitForAll() PublisherOption {
	return func(pc *publisherConfig) error {
		pc.ackMode = sarama.WaitForAll
		return nil
	}
}

// HashPartitioner option to set the partition logic to use
func HashPartitioner() PublisherOption {
	return func(pc *publisherConfig) error {
		pc.partitioner = sarama.NewHashPartitioner(pc.topic)
		return nil
	}
}

// ManualPartitioner option to set the partition logic to use
func ManualPartitioner(partition int32) PublisherOption {
	return func(pc *publisherConfig) error {
		if partition < 0 || partition > 100 {
			return ErrInvalidPartitionValue
		}
		pc.partitioner = sarama.NewManualPartitioner(pc.topic)
		pc.partition = partition
		return nil
	}
}

// RandomPartitioner option to set the partition logic to use
func RandomPartitioner() PublisherOption {
	return func(pc *publisherConfig) error {
		pc.partitioner = sarama.NewRandomPartitioner(pc.topic)
		return nil
	}
}

// RoundRobinPartitioner option to set the partition logic to use
func RoundRobinPartitioner() PublisherOption {
	return func(pc *publisherConfig) error {
		pc.partitioner = sarama.NewRoundRobinPartitioner(pc.topic)
		return nil
	}
}

// Partitioner option to set a custom partitioner.
func Partitioner(partitioner sarama.PartitionerConstructor) PublisherOption {
	return func(pc *publisherConfig) error {
		pc.partitioner = partitioner(pc.topic)
		return nil
	}
}

// SuccessHandler option to set a success handler for use with async publisher.
func WithSuccessHandler(s SuccessHandler) PublisherOption {
	return func(pc *publisherConfig) error {
		if s == nil {
			return ErrNoSuccessHandler
		}
		pc.successes = s
		return nil
	}
}

// PubLogger option to set the logger to use
func PubLogger(logger log.Logger) PublisherOption {
	return func(pc *publisherConfig) error {
		if logger == nil {
			return ErrNoLogger
		}
		pc.logger = logger
		return nil
	}
}

// NewKafkaPublisher will initiate a new Kafka publisher.
func NewKafkaPublisher(
	broker, topic string,
	options ...PublisherOption,
) (pubsub.Publisher, error) {
	if len(strings.Trim(broker, " \t")) == 0 {
		return nil, ErrNoBrokers
	}
	brokerHosts := strings.Split(broker, ",")

	if len(topic) == 0 {
		return nil, ErrNoTopic
	}
	// set sensible defaults
	pc := &publisherConfig{
		syncPublisher: defaultSyncPublisher,
		ackMode:       defaultRequiredAcks,
		successes:     nil,
		logger:        log.NewNopLogger(),
		topic:         topic,
		partitioner:   sarama.NewManualPartitioner(topic),
		partition:     0,
	}
	// parse optional parameters
	for _, option := range options {
		if err := option(pc); err != nil {
			return nil, err
		}
	}
	// set-up sarama
	sconfig := sarama.NewConfig()
	sconfig.Producer.RequiredAcks = pc.ackMode

	var p pubsub.Publisher
	if pc.syncPublisher {
		publisher, err := sarama.NewSyncProducer(brokerHosts, sconfig)
		if err != nil {
			return nil, err
		}
		p = &syncPublisher{
			publisher: publisher,
			topic:     topic,
			logger:    pc.logger,
		}
	} else {
		if pc.successes != nil {
			sconfig.Producer.Return.Successes = true
		}
		publisher, err := sarama.NewAsyncProducer(brokerHosts, sconfig)
		if err != nil {
			return nil, err
		}
		ap := &asyncPublisher{
			publisher: publisher,
			topic:     topic,
			logger:    pc.logger,
		}

		ap.wg.Add(1)
		go ap.logErrors()
		if pc.successes != nil {
			ap.successes = pc.successes
			ap.wg.Add(1)
			go ap.handleSuccess()
		}
		p = ap
	}

	return p, nil
}

// Publish will marshal the proto message and emit it to the Kafka topic.
func (p *syncPublisher) Publish(key string, m proto.Message) error {
	mb, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return p.PublishRaw(key, mb)
}

// PublishRaw will emit the byte array to the Kafka topic.
func (p *syncPublisher) PublishRaw(key string, m []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(m),
	}
	// TODO: do something with this partition/offset values
	_, _, err := p.publisher.SendMessage(msg)
	return err
}

// Stop will close the pub connection.
func (p *syncPublisher) Stop() error {
	return p.publisher.Close()
}

// Publish will marshal the proto message and emit it to the Kafka topic.
func (p *asyncPublisher) Publish(key string, m proto.Message) error {
	mb, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return p.PublishRaw(key, mb)
}

// PublishRaw will emit the byte array to the Kafka topic.
func (p *asyncPublisher) PublishRaw(key string, m []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(m),
	}
	p.publisher.Input() <- msg
	return nil
}

// Stop will close the pub connection.
func (p *asyncPublisher) Stop() error {
	err := p.publisher.Close()
	p.wg.Wait()
	return err
}

// logErrors logs unsuccessful messages
func (p *asyncPublisher) logErrors() {
	defer p.wg.Done()
	for pe := range p.publisher.Errors() {
		p.logger.Log(
			"result", "failed to produce msg",
			"msg", pe.Msg,
			"err", pe.Err,
		)
	}
}

// handleSuccess triggers callback with successful message as its parameter.
func (p *asyncPublisher) handleSuccess() {
	defer p.wg.Done()
	for s := range p.publisher.Successes() {
		p.successes(s)
	}
}
