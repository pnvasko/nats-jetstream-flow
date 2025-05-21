package common

import (
	"github.com/uptrace/uptrace-go/uptrace"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func InitOpentelemetry(cfg OtlpConfig) error {
	var options []uptrace.Option
	options = append(options, uptrace.WithDSN(cfg.Dsn()))
	options = append(options, uptrace.WithTracingEnabled(true))
	options = append(options, uptrace.WithLoggingEnabled(true))
	options = append(options, uptrace.WithServiceName(cfg.ServiceName()),
		uptrace.WithDeploymentEnvironment(cfg.Environment()),
		uptrace.WithServiceVersion(cfg.Version()),
	)
	uptrace.ConfigureOpentelemetry(options...)
	var attributes []attribute.KeyValue
	attributes = append(attributes, semconv.FaaSTriggerKey.String(cfg.Key()))
	otel.SetTextMapPropagator(xray.Propagator{})
	return nil
}
