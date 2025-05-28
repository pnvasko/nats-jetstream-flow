package handlers

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	stream "github.com/pnvasko/nats-jetstream-flow"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/coordination"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"github.com/rs/xid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type SinkHandlers[T any] struct {
	ctx     context.Context
	js      jetstream.JetStream
	counter coordination.Counter
	tracer  trace.Tracer
	logger  *common.Logger
}

func NewSinkHandlers[T any](ctx context.Context, js jetstream.JetStream, counter coordination.Counter, tracer trace.Tracer, logger *common.Logger) (*SinkHandlers[T], error) {
	sh := &SinkHandlers[T]{
		ctx:     ctx,
		js:      js,
		counter: counter,
		tracer:  tracer,
		logger:  logger,
	}

	return sh, nil
}

func (sh *SinkHandlers[T]) ProducerHandler(ctx context.Context, future *flow.Future[T]) error {
	producerSpanCtx, producerSpan := sh.tracer.Start(ctx, fmt.Sprintf("distributor_search_sink.message.producer"))
	var attrs []attribute.KeyValue
	defer func() {
		producerSpan.SetAttributes(attrs...)
		producerSpan.End()
	}()
	original := any(future.Original())
	search := proto.Search{}

	// msg, ok := original.(*stream.Message)
	switch original.(type) {
	case *stream.Message:
		originalMsg := original.(*stream.Message)
		err := search.UnmarshalVT(originalMsg.Payload())
		if err != nil {
			attrs = append(attrs, attribute.String("producer_function_test_attr", "test"))
			return stream.SetLogError(producerSpanCtx, "sink producer handler unmarshal payload failed", err, sh.logger)
		}
		subject, err := originalMsg.Subject()
		if err != nil {
			return stream.SetLogError(producerSpanCtx, "sink producer handler get subject failed", err, sh.logger)
		}
		attrs = append(attrs, attribute.String("subject", subject))
		uuid := xid.New().String()

		msg, err := originalMsg.Copy()
		if err != nil {
			return stream.SetLogError(producerSpanCtx, "map producer copy message failed", err, sh.logger)
		}
		msg.SetUuid(uuid)
		msg.SetSubject(subject)
		msg.SetContext(producerSpanCtx)
		natsMsgId := originalMsg.GetMessageMetadata(OriginalNatsMsgIdKey)
		msgData, err := stream.NewNatsMessage(msg)
		if err != nil {
			return stream.SetLogError(producerSpanCtx, "map producer new nats message failed", err, sh.logger)
		}
		//fmt.Printf("original DefaultNumDeliveredKey %+v\n", original.GetMessageMetadata(stream.DefaultNumDeliveredKey))
		numDelivered := msg.GetMessageMetadata(stream.DefaultNumDeliveredKey)
		debugFailed := msg.GetMessageMetadata(DebugFailedKey)
		if debugFailed == "true" && numDelivered == "1" {
			fmt.Printf("PublishMsgAsync numDelivered: %+v\n", numDelivered)
			fmt.Println("PublishMsgAsync msg debugFailedKey:", debugFailed)
			return stream.SetLogError(producerSpanCtx, "test failed PublishMsgAsync", fmt.Errorf("test error"), sh.logger)
		}
		ack, err := sh.js.PublishMsgAsync(msgData, jetstream.WithMsgID(natsMsgId))
		if err != nil {
			return stream.SetLogError(producerSpanCtx, "map producer publish error", err, sh.logger)
		}

		select {
		case <-ack.Ok():
			return nil
		case err := <-ack.Err():
			return stream.SetLogError(producerSpanCtx, "map producer ack message failed", err, sh.logger)
		case <-producerSpanCtx.Done():
			return producerSpanCtx.Err()
		}
	default:
		return fmt.Errorf("original message not support type: %+v\n", original)
	}
}

func (sh *SinkHandlers[T]) CompletionHandler(ctx context.Context, messages *flow.Messages) error {
	if err := sh.counter.Decr(ctx, &coordination.LabelParams{Label: LabelSearchNew}, 1); err != nil {
		sh.logger.Ctx(ctx).Sugar().Errorf("failed to increment counter: %s", err.Error())
		return err
	}

	return nil
}
