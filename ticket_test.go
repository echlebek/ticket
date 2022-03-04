package ticket_test

import (
	"context"
	"testing"
	"time"

	"github.com/echlebek/ticket"
)

type Job struct{}

type JobHandler[Job any] struct {
	Run bool
}

func (j *JobHandler[Job]) Handle(ctx context.Context, jobs []Job) error {
	if len(jobs) > 0 {
		j.Run = true
	}
	return nil
}

func TestSmoke(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := JobHandler[Job]{}
	server := ticket.NewBatchServer[Job](ctx, 100, time.Millisecond, &handler)
	stub := server.Accept(Job{})
	if err := stub.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !handler.Run {
		t.Error("handler not run")
	}
}
