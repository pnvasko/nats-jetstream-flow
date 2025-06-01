package handlers

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/coordination"
	"github.com/pnvasko/nats-jetstream-flow/examples/models"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MetricsCollectionStoreType int

const (
	BaseStore MetricsCollectionStoreType = iota + 1
	MapStore
)

type WorkflowLabelScopeType uint

const (
	UserType WorkflowLabelScopeType = iota + 1
	BoardType
	ClientType
	SearchType
)

var workflowLabelScopeTypes = map[WorkflowLabelScopeType]string{
	UserType:   "user",
	BoardType:  "board",
	ClientType: "client",
	SearchType: "search",
}

func GetMetricRecordName(t WorkflowLabelScopeType) string {
	return workflowLabelScopeTypes[t]
}

type WorkflowLabelParams struct {
	Scope      string
	SearchId   int64
	BoardId    uint64
	ClientId   int64
	CompanyId  int64
	UserId     int64
	LabelScope WorkflowLabelScopeType
}

type MetricHandler struct {
	metricStoreType MetricsCollectionStoreType
	labelHandler    func(*strings.Builder, WorkflowLabelParams) error
}

func NewMetricHandler(t MetricsCollectionStoreType, labelHandler func(*strings.Builder, WorkflowLabelParams) error) *MetricHandler {
	return &MetricHandler{
		metricStoreType: t,
		labelHandler:    labelHandler,
	}
}

type MetricRecord struct {
	Name   string
	Type   MetricsCollectionStoreType
	Params WorkflowLabelParams
	Data   any
}

func NewBoardMetricRecord(name, scope string, boardId uint64) *MetricRecord {
	mr := &MetricRecord{
		Name: name,
		Type: BaseStore,
		Params: WorkflowLabelParams{
			Scope:   scope,
			BoardId: boardId,
		},
	}
	mr.Data = &models.BaseMetricInput{}
	return mr
}

func NewMetricRecord(name, scope string, msType MetricsCollectionStoreType, search *proto.Search) *MetricRecord {
	mr := &MetricRecord{
		Name: name,
		Type: msType,
		Params: WorkflowLabelParams{
			Scope:     scope,
			SearchId:  int64(search.Id),
			BoardId:   0,
			ClientId:  search.ClientId,
			CompanyId: search.ClientId,
			UserId:    search.UserId,
		},
	}

	switch mr.Type {
	case BaseStore:
		mr.Data = &models.BaseMetricInput{}
	case MapStore:
		mr.Data = &models.MapMetricInput{
			UpdateOverall: &models.BaseMetricInput{},
			UpdateBoards:  make(map[uint64]*models.BaseMetricInput),
		}
	}
	return mr
}

func (mr *MetricRecord) Set(counter int64, failedCounter uint64) error {
	var input *models.BaseMetricInput
	switch mr.Type {
	case BaseStore:
		input = mr.Data.(*models.BaseMetricInput)
	case MapStore:
		input = mr.Data.(*models.MapMetricInput).UpdateOverall
	}
	if input == nil {
		return fmt.Errorf("invalid metric store type encountered during update: %d for label %s", mr.Type, mr.Name)
	}
	return input.Set(counter, failedCounter)
}

func (mr *MetricRecord) WithMapMetricInput(id uint64) *models.BaseMetricInput {
	if mr.Type != MapStore {
		return nil
	}
	if mr.Data == nil {
		mr.Data = &models.MapMetricInput{
			UpdateOverall: &models.BaseMetricInput{
				Counter:       0,
				FailedCounter: 0,
			},
			UpdateBoards: make(map[uint64]*models.BaseMetricInput),
		}
	}
	if _, ok := mr.Data.(*models.MapMetricInput).UpdateBoards[id]; !ok {
		mr.Data.(*models.MapMetricInput).UpdateBoards[id] = &models.BaseMetricInput{
			Counter:       0,
			FailedCounter: 0,
		}
	}
	return mr.Data.(*models.MapMetricInput).UpdateBoards[id]
}

func (mr *MetricRecord) SetMapMetricBoardInput(id uint64, input *models.BaseMetricInput) error {
	if mr.Type != MapStore {
		return fmt.Errorf("invalid metric store type encountered during update: %d for label %s", mr.Type, mr.Name)
	}
	if mr.Data == nil {
		mr.Data = &models.MapMetricInput{
			UpdateOverall: &models.BaseMetricInput{
				Counter:       0,
				FailedCounter: 0,
			},
			UpdateBoards: make(map[uint64]*models.BaseMetricInput),
		}
	}
	if _, ok := mr.Data.(*models.MapMetricInput).UpdateBoards[id]; !ok {
		mr.Data.(*models.MapMetricInput).UpdateBoards[id] = &models.BaseMetricInput{
			Counter:       0,
			FailedCounter: 0,
		}
	}
	mr.Data.(*models.MapMetricInput).UpdateBoards[id].Counter = input.Counter
	mr.Data.(*models.MapMetricInput).UpdateBoards[id].FailedCounter = input.FailedCounter
	return nil
}

// func (m *internalMetricImpl) WithCounter(counter int64) InternalMetric {
//    m.pb.Counter = counter
//    return m
//}
//
//func (m *internalMetricImpl) WithGauge(gauge int64) InternalMetric {
//    m.pb.Gauge = gauge
//    return m
//}
//
//func (m *internalMetricImpl) WithTimestamp(ts time.Time) InternalMetric {
//    m.pb.Timestamp = ts.UnixNano() / int64(time.Millisecond) // Adjust if using seconds
//    return m
//}
//
//func (m *internalMetricImpl) WithMetadata(meta map[string]string) InternalMetric {
//    m.pb.Metadata = make(map[string]string, len(meta))
//    for k, v := range meta {
//        m.pb.Metadata[k] = v
//    }
//    return m
//}
//
//func (m *internalMetricImpl) Clone() InternalMetric {
//    return &internalMetricImpl{
//        pb: proto.Clone(m.pb).(*metricsv1.InternalMetricPB),
//    }
//}

// 			data := models.MapMetricInput{
//				UpdateOverall: &models.BaseMetricInput{
//					Counter:       1,
//					FailedCounter: 0,
//				},
//				UpdateBoards: make(map[uint64]*models.BaseMetricInput),
//			}
//			for _, b := range search.Boards {
//				boardId, err := models.GetBoardType(b)
//				if err != nil {
//					tc.logger.Ctx(mainCtx).Sugar().Errorf("%v", err)
//					continue
//				}
//				data.UpdateBoards[boardId] = &models.BaseMetricInput{
//					Counter:       1,
//					FailedCounter: 0,
//				}
//			}
//			mr.Data = &data

type BaseMetricsObjectStore = *coordination.ObjectStore[*models.BaseMetric, *models.BaseMetricInput]
type MapMetricsObjectStore = *coordination.ObjectStore[*models.MapMetric, *models.MapMetricInput]

type MetricsCollectorOption func(*MetricsCollector) error

func WithScope(name string) MetricsCollectorOption {
	return func(mc *MetricsCollector) error {
		mc.scope = name
		return nil
	}
}
func WithBucketName(bucketName string) MetricsCollectorOption {
	return func(mc *MetricsCollector) error {
		mc.bucketName = &bucketName
		return nil
	}
}

func WithBucketPrefix(bucketPrefix string) MetricsCollectorOption {
	return func(mc *MetricsCollector) error {
		mc.bucketPrefix = &bucketPrefix
		return nil
	}
}

func WithCleanupTTL(ttl time.Duration) MetricsCollectorOption {
	return func(mc *MetricsCollector) error {
		mc.cleanupTTL = &ttl
		return nil
	}
}

func WithMaxRetryAttempts(n int) MetricsCollectorOption {
	return func(mc *MetricsCollector) error {
		mc.maxRetryAttempts = &n
		return nil
	}
}

func WithRetryWait(retryWait time.Duration) MetricsCollectorOption {
	return func(mc *MetricsCollector) error {
		mc.retryWait = &retryWait
		return nil
	}
}

type MetricsCollector struct {
	mu        sync.Mutex
	closeOnce sync.Once

	scope    string
	store    *models.BaseMetricStore
	mapStore *models.MapMetricStore

	scopeLabelHandler func(*strings.Builder, WorkflowLabelParams) error
	metricHandlers    map[string]*MetricHandler

	cleanupTTL       *time.Duration
	bucketPrefix     *string
	bucketName       *string
	retryWait        *time.Duration
	maxRetryAttempts *int

	tracer trace.Tracer
	logger *common.Logger
}

func NewMetricsCollector(ctx context.Context,
	name, bucketName, scope string,
	handler func(*strings.Builder, WorkflowLabelParams) error,
	js jetstream.JetStream,
	tracer trace.Tracer,
	logger *common.Logger,
	opts ...MetricsCollectorOption,
) (*MetricsCollector, error) {
	var err error
	mc := &MetricsCollector{
		scope:             scope,
		scopeLabelHandler: handler,
		metricHandlers:    make(map[string]*MetricHandler),
		tracer:            tracer,
		logger:            logger,
	}
	// var optsSet []coordination.StoreOption[*WaitGroup]
	// var opts []coordination.StoreOption[MetricsObjectStore]
	// opts = append(opts, coordination.WithBucketName[MetricsObjectStore](bucketName))
	for _, opt := range opts {
		if err := opt(mc); err != nil {
			return nil, err
		}
	}

	baseStoreOpts := models.NewBaseMetricStoreOption(bucketName, scope)
	if mc.cleanupTTL != nil {
		baseStoreOpts = append(baseStoreOpts, coordination.WithCleanupTTL[models.BaseMetricStoreObject](*mc.cleanupTTL))
	}
	if mc.bucketName != nil {
		baseStoreOpts = append(baseStoreOpts, coordination.WithBucketName[models.BaseMetricStoreObject](*mc.bucketName))
	}
	if mc.bucketPrefix != nil {
		baseStoreOpts = append(baseStoreOpts, coordination.WithBucketPrefix[models.BaseMetricStoreObject](*mc.bucketPrefix))
	}
	if mc.retryWait != nil {
		baseStoreOpts = append(baseStoreOpts, coordination.WithRetryWait[models.BaseMetricStoreObject](*mc.retryWait))
	}
	if mc.maxRetryAttempts != nil {
		baseStoreOpts = append(baseStoreOpts, coordination.WithMaxRetryAttempts[models.BaseMetricStoreObject](*mc.maxRetryAttempts))
	}

	mc.store, err = models.NewBaseMetricStore(ctx, js, name, nil, tracer, logger, baseStoreOpts)
	if err != nil {
		return nil, err
	}

	mapStoreOpts := models.NewMapMetricStoreOption(bucketName, scope)
	if mc.cleanupTTL != nil {
		mapStoreOpts = append(mapStoreOpts, coordination.WithCleanupTTL[models.MapMetricStoreObject](*mc.cleanupTTL))
	}
	if mc.bucketName != nil {
		mapStoreOpts = append(mapStoreOpts, coordination.WithBucketName[models.MapMetricStoreObject](*mc.bucketName))
	}
	if mc.bucketPrefix != nil {
		mapStoreOpts = append(mapStoreOpts, coordination.WithBucketPrefix[models.MapMetricStoreObject](*mc.bucketPrefix))
	}
	if mc.retryWait != nil {
		mapStoreOpts = append(mapStoreOpts, coordination.WithRetryWait[models.MapMetricStoreObject](*mc.retryWait))
	}
	if mc.maxRetryAttempts != nil {
		mapStoreOpts = append(mapStoreOpts, coordination.WithMaxRetryAttempts[models.MapMetricStoreObject](*mc.maxRetryAttempts))
	}
	mc.mapStore, err = models.NewMapMetricStore(ctx, js, name, nil, tracer, logger, mapStoreOpts)
	if err != nil {
		return nil, err
	}

	return mc, nil
}

func (mc *MetricsCollector) InitMetric() error {
	if err := mc.AddMetric(GetMetricRecordName(UserType), NewMetricHandler(MapStore, func(labelBuilder *strings.Builder, params WorkflowLabelParams) error {
		if params.UserId == 0 {
			return fmt.Errorf("invalid user id: %d", params.UserId)
		}
		labelBuilder.WriteString("user.")
		labelBuilder.WriteString(strconv.FormatInt(params.UserId, 10))
		return nil
	})); err != nil {
		return err
	}

	if err := mc.AddMetric(GetMetricRecordName(ClientType), NewMetricHandler(MapStore, func(labelBuilder *strings.Builder, params WorkflowLabelParams) error {
		if params.ClientId == 0 {
			return fmt.Errorf("invalid client id: %d", params.ClientId)
		}
		labelBuilder.WriteString("client.")
		labelBuilder.WriteString(strconv.FormatInt(int64(params.ClientId), 10))
		return nil
	})); err != nil {
		return err
	}

	if err := mc.AddMetric(GetMetricRecordName(SearchType), NewMetricHandler(MapStore, func(labelBuilder *strings.Builder, params WorkflowLabelParams) error {
		if params.SearchId == 0 {
			return fmt.Errorf("invalid search id: %d", params.SearchId)
		}
		labelBuilder.WriteString("search.")
		labelBuilder.WriteString(strconv.FormatInt(params.SearchId, 10))
		return nil
	})); err != nil {
		return err
	}

	if err := mc.AddMetric(GetMetricRecordName(BoardType), NewMetricHandler(BaseStore, func(labelBuilder *strings.Builder, params WorkflowLabelParams) error {
		if params.BoardId == 0 {
			return fmt.Errorf("invalid board id: %d", params.BoardId)
		}
		labelBuilder.WriteString("board.")
		labelBuilder.WriteString(strconv.FormatInt(int64(params.BoardId), 10))
		return nil
	})); err != nil {
		return err
	}
	return nil
}

func (mc *MetricsCollector) ScatterGatherSearchMetrics(ctx context.Context, search *proto.Search) error {
	spanCtx, span := mc.tracer.Start(ctx, fmt.Sprintf("scatterGather.update.search.metrics"))
	var attrs []attribute.KeyValue
	defer func() {
		span.SetAttributes(attrs...)
		span.End()
	}()

	var mrs []*MetricRecord

	clientMr := NewMetricRecord(GetMetricRecordName(ClientType), mc.scope, MapStore, search)
	if err := clientMr.Set(1, 0); err != nil {
		return fmt.Errorf("failed to set client record: %v", err)
	}

	userMr := NewMetricRecord(GetMetricRecordName(UserType), mc.scope, MapStore, search)
	if err := userMr.Set(1, 0); err != nil {
		return fmt.Errorf("failed to set user metric record: %v", err)
	}

	searchMr := NewMetricRecord(GetMetricRecordName(SearchType), mc.scope, MapStore, search)
	if err := searchMr.Set(1, 0); err != nil {
		return fmt.Errorf("failed to set search metric record: %v", err)
	}

	for _, b := range search.Boards {
		boardId, err := models.GetBoardType(b)
		if err != nil {
			mc.logger.Ctx(ctx).Sugar().Errorf("%v", err)
			return err
		}

		boardMr := NewBoardMetricRecord(GetMetricRecordName(BoardType), mc.scope, boardId)
		if err := boardMr.Set(1, 0); err != nil {
			return fmt.Errorf("failed to set board metric record: %v", err)
		}

		if err := clientMr.WithMapMetricInput(boardId).Set(1, 0); err != nil {
			return fmt.Errorf("failed to set client map metric input: %v", err)
		}
		if err := userMr.WithMapMetricInput(boardId).Set(1, 0); err != nil {
			return fmt.Errorf("failed to set user map metric input: %v", err)
		}

		if err := searchMr.WithMapMetricInput(boardId).Set(1, 0); err != nil {
			return fmt.Errorf("failed to set search map metric input: %v", err)
		}

		mrs = append(mrs, boardMr)
	}

	mrs = append(mrs, clientMr, userMr, searchMr)

	if err := mc.Update(spanCtx, mrs); err != nil {
		mc.logger.Ctx(ctx).Sugar().Errorf("failed to update metric records: %v", err)
		return err
	}
	return nil
}

func (mc *MetricsCollector) AddMetric(name string, mh *MetricHandler) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, ok := mc.metricHandlers[name]; ok {
		return fmt.Errorf("metric name %s already exists", name)
	}
	mc.metricHandlers[name] = mh

	return nil
}

func (mc *MetricsCollector) GetFromBaseStore(ctx context.Context, label string) (any, error) {
	return mc.store.Get(ctx, &coordination.LabelParams{Label: label})
}

func (mc *MetricsCollector) GetFromMapStore(ctx context.Context, label string) (any, error) {
	return mc.mapStore.Get(ctx, &coordination.LabelParams{Label: label})
}

func (mc *MetricsCollector) Update(ctx context.Context, mrs []*MetricRecord) error {
	var mrsMap = make(map[string]*MetricRecord)
	for _, mr := range mrs {
		mh, ok := mc.metricHandlers[mr.Name]
		if !ok {
			return fmt.Errorf("metric handler not found for name: %s", mr.Name)
		}
		if mh.metricStoreType != mr.Type {
			return fmt.Errorf("metric type mismatch for %s: registered as %d, received %d", mr.Name, mh.metricStoreType, mr.Type)
		}
		if mh.labelHandler == nil {
			return fmt.Errorf("label handler not found: %s", mr.Name)
		}

		switch mr.Type {
		case BaseStore:
			_, ok := mr.Data.(*models.BaseMetricInput)
			if !ok {
				return fmt.Errorf("invalid data type for base metric %s: expected *models.BaseMetricInput, got %T", mr.Name, mr.Data)
			}
		case MapStore:
			_, ok := mr.Data.(*models.MapMetricInput)
			if !ok {
				return fmt.Errorf("invalid data type for map metric %s: expected *models.MapMetricInput, got %T", mr.Name, mr.Data)
			}
		default:
			return fmt.Errorf("unsupported [%v] metric store type for %s: %d", mr.Type, mr.Name, mh.metricStoreType)
		}

		var labelBuilder strings.Builder
		labelBuilder.Grow(64)

		err := mc.scopeLabelHandler(&labelBuilder, mr.Params)
		if err != nil {
			return fmt.Errorf("failed to build scope label for %s: %w", mr.Name, err)
		}

		err = mh.labelHandler(&labelBuilder, mr.Params)
		if err != nil {
			return fmt.Errorf("failed to build metric label for %s: %w", mr.Name, err)
		}
		label := labelBuilder.String()

		if label == "" {
			return fmt.Errorf("derived label is empty for metric %s after handlers processed", mr.Name)
		}
		if existingMr, ok := mrsMap[label]; ok {
			return fmt.Errorf("duplicate label found in batch: '%s' for metrics '%s' and '%s'", label, existingMr.Name, mr.Name)
		}
		mrsMap[label] = mr
	}

	var updateErrors []error
	for label, m := range mrsMap {
		var err error
		switch m.Type {
		case BaseStore:
			input := m.Data.(*models.BaseMetricInput)
			err = mc.store.Update(ctx, &coordination.LabelParams{Label: label}, input)
		case MapStore:
			input := m.Data.(*models.MapMetricInput)
			err = mc.mapStore.Update(ctx, &coordination.LabelParams{Label: label}, input)
		default:
			err = fmt.Errorf("invalid metric store type encountered during update: %d for label %s", m.Type, label)
		}

		if err != nil {
			updateErrors = append(updateErrors, err)
		}
	}

	if len(updateErrors) > 0 {
		return fmt.Errorf("batch update failed: %v", common.MultiError(updateErrors))
	}

	return nil
}

func (mc *MetricsCollector) WatchBaseStore(ctx context.Context, params *coordination.LabelParams, fn func(label string, obj any)) error {
	if err := mc.store.Watch(ctx, params, func(label string, metric *models.BaseMetric) {
		fn(label, metric)
	}); err != nil && !errors.Is(err, context.Canceled) {
		mc.logger.Ctx(ctx).Sugar().Errorf("failed to watch base metrics: %v", err)
		return err
	}
	return nil
}

func (mc *MetricsCollector) WatchMapStore(ctx context.Context, params *coordination.LabelParams, fn func(label string, obj any)) error {
	if err := mc.mapStore.Watch(ctx, params, func(label string, metric *models.MapMetric) {
		fn(label, metric)
	}); err != nil && !errors.Is(err, context.Canceled) {
		mc.logger.Ctx(ctx).Sugar().Errorf("failed to watch ,ap metrics: %v", err)
		return err
	}
	return nil
}

func (mc *MetricsCollector) Close(ctx context.Context) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.closeOnce.Do(func() {
		if mc.store != nil {
			mc.store.Close()
		}
		if mc.mapStore != nil {
			mc.mapStore.Close()
		}
	})
}
