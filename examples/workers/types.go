package workers

import (
	"context"
)

type Worker interface {
	Run() error
	Close(ctx context.Context) error
	Done() <-chan struct{}
}
