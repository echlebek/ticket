package ticket_test

import (
	"context"
	"testing"
	"time"

	"github.com/echlebek/ticket"
)

type JobWithDelay interface {
	Delay() time.Duration
}

type Job struct {
	delay time.Duration
}

func (j Job) Delay() time.Duration {
	return j.delay
}


type JobHandler[J JobWithDelay] struct {
	Run bool
}

func (h *JobHandler[J]) Handle(ctx context.Context, jobs []J) error {
	if len(jobs) == 0 {
		return nil
	}

	done := make(chan struct{})

	go func() {
		for _, j := range jobs {
			time.Sleep(j.Delay())
		}
		done <- struct{}{}
	}()

	select {
	case <- done:
		h.Run = true
	case <- ctx.Done():
	}

	return nil
}

func TestSmoke(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := JobHandler[Job]{}
	server := ticket.NewBatchServer[Job](ctx, 100, time.Millisecond, &handler)

	stub := server.Accept(Job{})
	if err := stub.Wait(); err != nil {
		t.Fatal(err)
	}

	if !handler.Run {
		t.Error("handler not run")
	}
}


func TestCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	handler := JobHandler[Job]{}
	server := ticket.NewBatchServer[Job](ctx, 100, time.Millisecond, &handler)

	stub := server.Accept(Job{delay: 10*time.Second})

	time.Sleep(200 * time.Millisecond)
	cancel()

	if err := stub.Wait(); err != nil {
		t.Fatal(err)
	}

	if handler.Run {
		t.Error("handler still ran")
	}
}
