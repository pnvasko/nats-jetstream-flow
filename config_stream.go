package nats_jetstream_flow

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultMaxMsgsPerSubject = 1
	defaultRetentionPolicy   = jetstream.LimitsPolicy
	defaultDiscardPolicy     = jetstream.DiscardOld // defaultDiscardPolicy     = jetstream.DiscardNew
	defaultStreamStorage     = jetstream.FileStorage
	defaultDuplicateWindow   = 2 * time.Minute
	defaultCleanupTTL        = 24 * time.Hour
)

type StreamConfigOption func(*StreamConfig) error
type StreamConfigOptions []StreamConfigOption

func WithStreamSource(s jetstream.Stream) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.streamInstance = s
		return nil
	}
}

func WithCleanupTtl(ttl time.Duration) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.cleanupTTL = ttl
		return nil
	}
}

func WithDiscardPolicy(p jetstream.DiscardPolicy) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.discardPolicy = p
		return nil
	}
}

func WithRetentionPolicy(p jetstream.RetentionPolicy) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.retentionPolicy = p
		return nil
	}
}

func WithStorageTypeStreamSource(st jetstream.StorageType) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.streamStorageType = st
		return nil
	}
}

func WithReplicasStreamSource(n int) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.streamReplicas = n
		return nil
	}
}

func WithDiscardPolicyMsgsPerSubject(v bool) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.discardPolicyMsgsPerSubject = v
		return nil
	}
}

func WithMaxMsgsPerSubject(n int64) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.maxMsgsPerSubject = n
		return nil
	}
}

func WithDuplicateWindow(td time.Duration) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.duplicateWindow = td
		return nil
	}
}

func WithAllowMsgTTL(v bool) StreamConfigOption {
	return func(cfg *StreamConfig) error {
		cfg.allowMsgTTL = v
		return nil
	}
}

type StreamConfig struct {
	streamName                  string
	subjects                    []string
	streamInstance              jetstream.Stream
	retentionPolicy             jetstream.RetentionPolicy
	discardPolicy               jetstream.DiscardPolicy
	streamStorageType           jetstream.StorageType
	maxMsgsPerSubject           int64
	discardPolicyMsgsPerSubject bool
	streamReplicas              int
	cleanupTTL                  time.Duration
	duplicateWindow             time.Duration
	allowMsgTTL                 bool
}

func NewStreamConfig(streamName string, subjects []string, opts ...StreamConfigOption) (*StreamConfig, error) {
	cfg := &StreamConfig{
		streamName:                  streamName,
		subjects:                    subjects,
		maxMsgsPerSubject:           defaultMaxMsgsPerSubject,
		retentionPolicy:             defaultRetentionPolicy,
		discardPolicy:               defaultDiscardPolicy,
		streamStorageType:           defaultStreamStorage,
		duplicateWindow:             defaultDuplicateWindow, // DuplicateWindow: 1 * time.Nanosecond, // practically disables deduplication window tracking
		streamReplicas:              1,
		discardPolicyMsgsPerSubject: true,
		cleanupTTL:                  defaultCleanupTTL,
		allowMsgTTL:                 false,
	}

	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func (sc StreamConfig) StreamName() string {
	return sc.streamName
}

func (sc StreamConfig) Subjects() []string {
	return sc.subjects
}

func (sc StreamConfig) StreamInstance() jetstream.Stream {
	return sc.streamInstance
}

func (sc StreamConfig) CleanupTTL() time.Duration {
	return sc.cleanupTTL
}

func (sc StreamConfig) MaxMsgsPerSubject() int64 {
	return sc.maxMsgsPerSubject
}

func (sc StreamConfig) DiscardPolicy() jetstream.DiscardPolicy {
	return sc.discardPolicy
}

func (sc StreamConfig) DiscardPolicyMsgsPerSubject() bool {
	return sc.discardPolicyMsgsPerSubject
}

func (sc StreamConfig) RetentionPolicy() jetstream.RetentionPolicy {
	return sc.retentionPolicy
}

func (sc StreamConfig) StreamStorageType() jetstream.StorageType {
	return sc.streamStorageType
}

func (sc StreamConfig) StreamReplicas() int {
	return sc.streamReplicas
}

func (sc StreamConfig) StreamDuplicatesWindow() time.Duration {
	return sc.duplicateWindow
}

func (sc StreamConfig) AllowMsgTTL() bool {
	return sc.allowMsgTTL
}

var _ StreamConfiguration = (*StreamConfig)(nil)

type StreamConfiguration interface {
	StreamName() string
	Subjects() []string
	StreamInstance() jetstream.Stream
	CleanupTTL() time.Duration
	MaxMsgsPerSubject() int64
	DiscardPolicy() jetstream.DiscardPolicy
	DiscardPolicyMsgsPerSubject() bool
	RetentionPolicy() jetstream.RetentionPolicy
	StreamStorageType() jetstream.StorageType
	StreamReplicas() int
	StreamDuplicatesWindow() time.Duration
	AllowMsgTTL() bool
}
