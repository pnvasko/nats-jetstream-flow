package workers

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	stream "github.com/pnvasko/nats-jetstream-flow"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/examples/handlers"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"github.com/rs/xid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type CdcSource struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        common.SafeWaitGroup
	js        jetstream.JetStream

	tracer trace.Tracer
	logger *common.Logger
}

func NewCdcEmitter(ctx context.Context, js jetstream.JetStream, tracer trace.Tracer, logger *common.Logger) (*CdcSource, error) {
	cdc := &CdcSource{
		js:     js,
		tracer: tracer,
		logger: logger,
	}
	cdc.ctx, cdc.cancel = context.WithCancel(ctx)
	return cdc, nil
}

func (cdc *CdcSource) Run() error {
	ctxRun, runSpan := cdc.tracer.Start(cdc.ctx, "cdcSource.run")
	defer runSpan.End()

	allBoards := []string{"amazon", "ebay", "walmart", "shopify", "alibaba", "wish"}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	randomElement := func(slice []string) string {
		if len(slice) == 0 {
			return ""
		}
		return slice[rng.Intn(len(slice))]
	}

	countTicker := time.NewTicker(5 * 60 * time.Second)
	defer countTicker.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	i := 1
	var id int32 = 1
	for {
		select {
		case <-ctxRun.Done():
			return ctxRun.Err()
		case <-countTicker.C:
			fmt.Printf("cdc source ticker %d run %d\n", i, id)
			i++
		case <-ticker.C:
			complete := make(chan struct{})
			errCh := make(chan error)
			go func() {
				cdc.logger.Ctx(ctxRun).Sugar().Debugf("cdc source ticker run %d", id)
				cdcSpanCtx, cdcSpan := cdc.tracer.Start(context.Background(), fmt.Sprintf("cdc.message.producer"))
				var attrs []attribute.KeyValue
				defer func() {
					cdcSpan.SetAttributes(attrs...)
					cdcSpan.End()
				}()

				s := proto.Search{
					Id:       id,
					UserId:   1,
					ClientId: 1,
					Boards:   []string{},
				}
				totalBoards := rng.Intn(len(allBoards))
				if totalBoards == 0 {
					totalBoards = 1
				}
				boards := map[string]struct{}{}

				i := 0
				for {
					if i >= totalBoards {
						break
					}
					board := randomElement(allBoards)
					if _, ok := boards[board]; ok {
						continue
					}
					boards[board] = struct{}{}
					s.Boards = append(s.Boards, board)
					i++
				}

				data, err := s.MarshalVT()
				if err != nil {
					errCh <- stream.SetLogError(cdcSpanCtx, "marshal message failed", err, cdc.logger)
					return
				}
				msgId := xid.New().String()
				msg, err := stream.NewMessage(msgId, data)
				if err != nil {
					errCh <- stream.SetLogError(cdcSpanCtx, "create new stream message failed", err, cdc.logger)
					return
				}
				msg.SetSubject(fmt.Sprintf("%s.%d", DefaultSearchCDCSubject, id))
				msg.SetContext(cdcSpanCtx)
				if len(boards) >= 3 {
					msg.SetMessageMetadata(handlers.DebugFailedKey, "true")
				}
				msgData, err := stream.NewNatsMessage(msg)
				if err != nil {
					errCh <- stream.SetLogError(cdcSpanCtx, "create new nats message failed", err, cdc.logger)
					return

				}
				_, err = cdc.js.PublishMsgAsync(msgData)
				if err != nil {
					errCh <- stream.SetLogError(cdcSpanCtx, "publish message failed", err, cdc.logger)
					return
				}

				attrs = append(attrs, attribute.String("msg_id", msgId))
				attrs = append(attrs, attribute.String("msg_board", strings.Join(s.Boards, ",")))
				fmt.Printf("cdc source ticker search_id: %d; msg_id %s: len boards %d\n", id, msgId, len(s.Boards))

				id++
				if id > totalSearch {
					errCh <- nil
				}
				close(complete)
			}()

			select {
			case <-ctxRun.Done():
				return ctxRun.Err()
			case err := <-errCh:
				ticker.Stop()
				time.Sleep(5000 * time.Millisecond)
				return err
			case <-complete:

				continue
			}
		}
	}
}

func (cdc *CdcSource) Close(ctx context.Context) error {
	waitChan := make(chan struct{})
	go func() {
		cdc.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		cdc.closeOnce.Do(func() { cdc.cancel() })
		return ctx.Err()
	case <-waitChan:
		cdc.closeOnce.Do(func() { cdc.cancel() })
		return nil
	}
}

func (cdc *CdcSource) Done() <-chan struct{} {
	return cdc.ctx.Done()
}

var _ Worker = (*CdcSource)(nil)
