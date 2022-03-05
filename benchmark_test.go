package ticket_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/echlebek/ticket"
)

type BenchJobHandler[J Job] struct {
	Run bool
}

func (h *BenchJobHandler[J]) Handle(ctx context.Context, jobs []J) error {
	if len(jobs) > 0 {
		h.Run = true
	}
	return nil
}

func BenchmarkBatchServer(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := BenchJobHandler[Job]{}
	server := ticket.NewBatchServer[Job](ctx, 100, time.Millisecond, &handler)
	stubs := make([]*ticket.Stub, 100)
	var wg sync.WaitGroup
	wg.Add(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i % 100 == 0 && i != 0 {
			cstubs := make([]*ticket.Stub, 100)
			copy(cstubs, stubs)
			go func() {
				wg.Add(1)
				for i := range cstubs {
					_ = cstubs[i].Wait(ctx)
				}
				wg.Done()
			}()
		}
		stubs[i%100] = server.Accept(Job{})
	}
	wg.Done()
	wg.Wait()
}
