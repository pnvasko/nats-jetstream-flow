package nats_jetstream_flow

import (
	"context"
	"github.com/nats-io/nats.go/jetstream"
)

func CreateOrUpdateStream(ctx context.Context, js jetstream.JetStream, config StreamConfiguration) (jetstream.Stream, error) {
	streamConfig := jetstream.StreamConfig{
		Name:       config.StreamName(),
		Subjects:   config.Subjects(),
		MaxAge:     config.CleanupTTL(),
		Discard:    config.DiscardPolicy(),
		Retention:  config.RetentionPolicy(),
		Storage:    config.StreamStorageType(),
		Replicas:   config.StreamReplicas(),
		Duplicates: config.StreamDuplicatesWindow(),
	}

	if config.DiscardPolicy() == jetstream.DiscardNew && config.MaxMsgsPerSubject() > 0 {
		streamConfig.DiscardNewPerSubject = config.DiscardPolicyMsgsPerSubject()
		streamConfig.MaxMsgsPerSubject = config.MaxMsgsPerSubject()
	}

	return js.CreateOrUpdateStream(ctx, streamConfig)
}

func CreateOrUpdateConsumer(ctx context.Context, js jetstream.JetStream, config ConsumerConfiguration) (jetstream.Consumer, error) {
	consumerConfig := jetstream.ConsumerConfig{
		Name:           config.ConsumerName(),
		DeliverPolicy:  config.DeliverPolicy(),
		AckPolicy:      config.AckPolicy(),
		AckWait:        config.AckWait(),
		MaxDeliver:     config.MaxDeliver(),
		FilterSubjects: config.FilterSubjects(),
	}

	if config.DurableName() != "" {
		consumerConfig.Durable = config.DurableName()
	}

	if len(config.BackOff()) > 0 {
		consumerConfig.BackOff = config.BackOff()
	}

	return js.CreateOrUpdateConsumer(ctx, config.StreamName(), consumerConfig)
}
