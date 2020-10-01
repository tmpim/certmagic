package certmagic

import (
	"log"
	"runtime"
	"sync"

	"go.uber.org/zap"
)

var jm = &jobManager{maxConcurrentJobs: 1000}

type jobManager struct {
	mu                sync.Mutex
	maxConcurrentJobs int
	activeWorkers     int
	queue             []namedJob
	names             map[string]struct{}
}

type namedJob struct {
	name   string
	job    func() error
	logger *zap.Logger
}

// Submit enqueues the given job with the given name. If name is non-empty
// and a job with the same name is already enqueued or running, this is a
// no-op. If name is empty, no duplicate prevention will occur. The job
// manager will then run this job as soon as it is able.
func (jm *jobManager) Submit(logger *zap.Logger, name string, job func() error) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	if jm.names == nil {
		jm.names = make(map[string]struct{})
	}
	if name != "" {
		// prevent duplicate jobs
		if _, ok := jm.names[name]; ok {
			return
		}
		jm.names[name] = struct{}{}
	}
	jm.queue = append(jm.queue, namedJob{name, job, logger})
	if jm.activeWorkers < jm.maxConcurrentJobs {
		jm.activeWorkers++
		go jm.worker()
	}
}

func (jm *jobManager) worker() {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, stackTraceBufferSize)
			buf = buf[:runtime.Stack(buf, false)]
			log.Printf("panic: certificate worker: %v\n%s", err, buf)
		}
	}()

	for {
		jm.mu.Lock()
		if len(jm.queue) == 0 {
			jm.activeWorkers--
			jm.mu.Unlock()
			return
		}
		next := jm.queue[0]
		jm.queue = jm.queue[1:]
		jm.mu.Unlock()
		if err := next.job(); err != nil {
			if next.logger != nil {
				next.logger.Error("job failed", zap.Error(err))
			}
		}
		if next.name != "" {
			jm.mu.Lock()
			delete(jm.names, next.name)
			jm.mu.Unlock()
		}
	}
}

// ErrNoRetry is an error type which signals
// to stop retries early.
type ErrNoRetry struct{ Err error }

// Unwrap makes it so that e wraps e.Err.
func (e ErrNoRetry) Unwrap() error { return e.Err }
func (e ErrNoRetry) Error() string { return e.Err.Error() }

type retryStateCtxKey struct{}

// AttemptsCtxKey is the context key for the value
// that holds the attempt counter. The value counts
// how many times the operation has been attempted.
// A value of 0 means first attempt.
var AttemptsCtxKey retryStateCtxKey
