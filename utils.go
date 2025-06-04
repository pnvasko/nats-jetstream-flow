package nats_jetstream_flow

import (
	"context"
	"fmt"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"strconv"
)

func SetLogError(ctx context.Context, description string, err error, logger common.Logger, attrs ...attribute.KeyValue) error {
	recordError := fmt.Errorf("%s: %s", description, err.Error())
	logger.Ctx(ctx).Error(description, zap.Error(err))
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetStatus(codes.Error, recordError.Error())
	}
	return recordError
}

func Int64ToString(n int64) string {
	buf := make([]byte, 0, 20)
	buf = strconv.AppendInt(buf, n, 10)
	return string(buf)
}

func ParseStringToUInt64(d string) (n uint64) {
	// return strconv.ParseInt(s, 10, 64)

	if len(d) == 0 {
		return 0
	}

	// ASCII numbers 0-9
	const (
		asciiZero = 48
		asciiNine = 57
	)

	for _, dec := range d {
		if dec < asciiZero || dec > asciiNine {
			return 0
		}
		n = n*10 + uint64(dec) - asciiZero
	}
	return
}
