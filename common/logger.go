package common

import (
	"context"
	"github.com/go-logr/zapr"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"github.com/vmihailenco/taskq/v4"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

type Logger interface {
	Ctx(context.Context) otelzap.LoggerWithCtx
}

type LibLogger struct {
	*otelzap.Logger
}

func (log *LibLogger) Ctx(ctx context.Context) otelzap.LoggerWithCtx {
	return log.Logger.Ctx(ctx)
}

func (log *LibLogger) OtelZapLogger() *otelzap.Logger {
	return log.Logger
}

func (log *LibLogger) ZapLogger() *zap.Logger {
	return log.Logger.Logger
}

func NewLoggerWithParams(dsn, serviceName, environment, version, key string, debug bool) (*LibLogger, error) {
	cfg := DevOtlpConfig{
		debug:       true,
		dsn:         dsn,
		serviceName: serviceName,
		environment: environment,
		version:     version,
		key:         key,
	}
	return NewLibLogger(&cfg)
}

func NewLibLogger(cfg OtlpConfig) (*LibLogger, error) {
	zapConf := zap.NewProductionEncoderConfig()
	zapConf.EncodeTime = zapcore.ISO8601TimeEncoder

	var encoder zapcore.Encoder
	var defaultLogLevel zapcore.Level
	if cfg.Debug() {
		zapConf.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(zapConf)
		defaultLogLevel = zapcore.DebugLevel
	} else {
		encoder = zapcore.NewJSONEncoder(zapConf)
		defaultLogLevel = zapcore.InfoLevel
	}

	cores := []zapcore.Core{
		zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), defaultLogLevel),
	}
	core := zapcore.NewTee(cores...)

	zapLogger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))

	var options []otelzap.Option
	options = append(options, otelzap.WithMinLevel(defaultLogLevel))

	logger := &LibLogger{
		Logger: otelzap.New(zapLogger, options...),
	}
	zap.ReplaceGlobals(logger.ZapLogger())
	otelzap.ReplaceGlobals(logger.OtelZapLogger())

	zaprLogger := zapr.NewLogger(logger.ZapLogger())
	taskq.SetLogger(zaprLogger)

	return logger, nil
}
