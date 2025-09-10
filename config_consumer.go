package nats_jetstream_flow

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/xid"
)

const (
	defaultAckWait           = time.Second * 30
	defaultMaxDeliver        = 5
	defaultDeliverPolicy     = jetstream.DeliverAllPolicy
	defaultAckPolicy         = jetstream.AckExplicitPolicy
	defaultInactiveThreshold = 7 * 24 * time.Hour
)

type ConsumerConfigOption func(*ConsumerConfig) error
type ConsumerConfigOptions []ConsumerConfigOption

func WithDurableName(name string) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		cfg.durableName = name
		return nil
	}
}

func WithConsumerName(name string) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		cfg.consumerName = name
		return nil
	}
}

func WithFilterSubjects(subjects []string) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		cfg.filterSubjects = subjects
		return nil
	}
}

func WithAckWait(td time.Duration) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		cfg.ackWait = td
		return nil
	}
}

func WithAckPolicy(p jetstream.AckPolicy) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		//if cfg.ackPolicy == nil {
		//	cfg.ackPolicy = new(jetstream.AckPolicy)
		//}
		cfg.ackPolicy = p
		return nil
	}
}

func WithMaxDeliver(n int) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		cfg.maxDeliver = n
		return nil
	}
}

func WithPullOptions(opts ...jetstream.PullConsumeOpt) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		cfg.pullOptions = append(cfg.pullOptions, opts...)
		return nil
	}
}

func WithDeliverPolicy(p jetstream.DeliverPolicy) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		//if cfg.deliverPolicy == nil {
		//	cfg.deliverPolicy = new(jetstream.DeliverPolicy)
		//}
		cfg.deliverPolicy = p
		return nil
	}
}

func WithBackOff(nn []int) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		var boff []time.Duration
		for _, n := range nn {
			boff = append(boff, time.Duration(n)*time.Millisecond)
		}
		cfg.backOff = boff
		return nil
	}
}

func WithInactiveThreshold(v time.Duration) ConsumerConfigOption {
	return func(cfg *ConsumerConfig) error {
		cfg.inactiveThreshold = v
		return nil
	}
}

type ConsumerConfig struct {
	streamName        string
	filterSubjects    []string
	consumerName      string
	durableName       string
	ackPolicy         jetstream.AckPolicy
	deliverPolicy     jetstream.DeliverPolicy
	pullOptions       []jetstream.PullConsumeOpt
	maxDeliver        int
	inactiveThreshold time.Duration

	backOff []time.Duration

	ackWait time.Duration
}

func NewConsumerConfig(streamName string, subjects []string, opts ...ConsumerConfigOption) (*ConsumerConfig, error) {
	cfg := &ConsumerConfig{
		streamName:        streamName,
		filterSubjects:    subjects,
		deliverPolicy:     defaultDeliverPolicy,
		ackWait:           defaultAckWait,
		ackPolicy:         defaultAckPolicy,
		maxDeliver:        defaultMaxDeliver,
		inactiveThreshold: defaultInactiveThreshold,
	}

	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	if cfg.consumerName == "" && cfg.durableName != "" {
		// Durable consumers MUST have a Name. If DurableName is set, ConsumerName must also be set.
		// Or you could make ConsumerName == DurableName if DurableName is set and ConsumerName is not.
		cfg.consumerName = cfg.durableName // Common pattern: durable name IS the consumer name
	}

	if cfg.consumerName == "" {
		// return nil, fmt.Errorf("consumer name is required (set DurableName or ConsumerName)")
		// auto-generated name for ephemeral consumers
		cfg.consumerName = fmt.Sprintf("ephemeral_consumers_%s", xid.New())
	}

	return cfg, nil
}

func (cfg ConsumerConfig) StreamName() string {
	return cfg.streamName
}

func (cfg ConsumerConfig) ConsumerName() string {
	return cfg.consumerName
}

func (cfg ConsumerConfig) DurableName() string {
	return cfg.durableName
}

func (cfg ConsumerConfig) FilterSubjects() []string {
	return cfg.filterSubjects
}

func (cfg ConsumerConfig) DeliverPolicy() jetstream.DeliverPolicy {
	return cfg.deliverPolicy
}

func (cfg ConsumerConfig) AckPolicy() jetstream.AckPolicy {
	return cfg.ackPolicy
}

func (cfg ConsumerConfig) AckWait() time.Duration {
	return cfg.ackWait
}

func (cfg ConsumerConfig) MaxDeliver() int {
	return cfg.maxDeliver
}

func (cfg ConsumerConfig) BackOff() []time.Duration {
	return cfg.backOff
}

func (cfg ConsumerConfig) PullOptions() []jetstream.PullConsumeOpt {
	return cfg.pullOptions
}

func (cfg ConsumerConfig) InactiveThreshold() time.Duration {
	return cfg.inactiveThreshold
}

var _ ConsumerConfiguration = (*ConsumerConfig)(nil)

type ConsumerConfiguration interface {
	StreamName() string
	ConsumerName() string
	DurableName() string
	FilterSubjects() []string
	DeliverPolicy() jetstream.DeliverPolicy
	AckPolicy() jetstream.AckPolicy
	AckWait() time.Duration
	MaxDeliver() int
	BackOff() []time.Duration
	PullOptions() []jetstream.PullConsumeOpt
	InactiveThreshold() time.Duration
}
