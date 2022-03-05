package ticket

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Stub represents a ticket for a Job. When a Job is Accepted, a Stub will be
// returned. The caller can wait for the Job to complete by calling Wait().
type Stub struct {
	err error
	wg sync.WaitGroup
}

// Wait waits for the Job associated with the ticket to be completed.
// The Job's error will be returned, or the context's error, if the server
// context is cancelled before the job completes.
func (t *Stub) Wait() error {
	t.wg.Wait()
	return t.err
}

type id int64

type acceptedJob[Job any] struct {
	ID  id
	Job Job
}

// Handler is used by the BatchServer to execute jobs in a batch. It must be
// provided by the library user.
type Handler[Job any] interface {
	// Handle handles a batch of Jobs, returning a non-nil error when the batch
	// could not be correctly processed, or when the provided context is
	// cancelled.
	Handle(context.Context, []Job) error
}

// BatchServer is a server that can accept individual jobs, but schedule
// them for execution as a batch.
type BatchServer[Job any] struct {
	count      int64
	bufferSize int
	timeout    time.Duration
	fifo       chan acceptedJob[Job]
	sig        chan struct{}
	tickets    map[id]*Stub
	tixMu      sync.Mutex
	handler    Handler[Job]
	cond       *sync.Cond
	lastSubmit time.Time
	lsMu       sync.Mutex
	jobs       []Job
	keys       []id
}

// NewBatchServer creates a new BatchServer with a maximum buffer size
// and a batch timeout duration. Batches will be submitted to the handler when
// the batch reaches capacity, or when the timeout occurs and there are new
// jobs in the buffer.
func NewBatchServer[Job any](ctx context.Context, size int, timeout time.Duration, handler Handler[Job]) *BatchServer[Job] {
	server := &BatchServer[Job]{
		bufferSize: size,
		timeout:    timeout,
		fifo:       make(chan acceptedJob[Job], size),
		sig:        make(chan struct{}, 1),
		handler:    handler,
		cond:       sync.NewCond(new(sync.Mutex)),
		lastSubmit: time.Now(),
		tickets:    make(map[id]*Stub),
		jobs:       make([]Job, 0, size),
		keys:       make([]id, 0, size),
	}
	go server.run(ctx)
	go server.watchdog(ctx)
	return server
}

func (b *BatchServer[Job]) timeSinceLastSubmit() time.Duration {
	b.lsMu.Lock()
	defer b.lsMu.Unlock()
	return time.Since(b.lastSubmit)
}

func (b *BatchServer[Job]) resetLastSubmit() {
	b.lsMu.Lock()
	defer b.lsMu.Unlock()
	b.lastSubmit = time.Now()
}

func (b *BatchServer[Job]) watchdog(ctx context.Context) {
	ticker := time.NewTicker(b.timeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if b.timeSinceLastSubmit() >= b.timeout {
				b.trySignal()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (b *BatchServer[Job]) doSubmit(ctx context.Context) {
	jobs := b.jobs
	keys := b.keys
	iter := true
	for iter {
		select {
		case j := <-b.fifo:
			jobs = append(jobs, j.Job)
			keys = append(keys, j.ID)
		default:
			iter = false
		}
	}
	b.cond.Broadcast()
	if len(jobs) == 0 {
		return
	}
	b.resetLastSubmit()
	err := b.handler.Handle(ctx, jobs)
	b.tixMu.Lock()
	defer b.tixMu.Unlock()
	for i, key := range keys {
		t, ok := b.tickets[key]
		if ok {
			delete(b.tickets, key)
			t.err = err
			t.wg.Done()
		}
		var zeroJob Job
		// avoid leaking jobs since the slice is reused
		jobs[i] = zeroJob
	}
}

func (b *BatchServer[Job]) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.sig:
			b.doSubmit(ctx)
		}
	}
}

// Accept accepts a new Job for processing. It will be queued in the batch
// until the batch can be processed by the Handler.
func (b *BatchServer[Job]) Accept(job Job) *Stub {
	id := id(atomic.AddInt64(&b.count, 1))
	for {
		select {
		case b.fifo <- acceptedJob[Job]{ID: id, Job: job}:
			return b.newTicket(id)
		default:
			b.trySignal()
			b.wait()
		}
	}
}

func (b *BatchServer[Job]) newTicket(id id) *Stub {
	ticket := &Stub{}
	ticket.wg.Add(1)

	b.tixMu.Lock()
	defer b.tixMu.Unlock()

	b.tickets[id] = ticket

	return ticket
}

func (b *BatchServer[Job]) wait() {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()
	b.cond.Wait()
}

func (b *BatchServer[Job]) trySignal() {
	select {
	case b.sig <- struct{}{}:
	default:
	}
}
