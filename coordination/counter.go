package coordination

import "context"

type Counter interface {
	Get(context.Context, *LabelParams) (any, error)
	FullUpdate(context.Context, *LabelParams, any) error
	Incr(context.Context, *LabelParams, uint64) error
	Decr(context.Context, *LabelParams, uint64) error
	Failed(context.Context, *LabelParams, uint64) error
	Delete(context.Context, *LabelParams) error
}
