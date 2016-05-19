package worker

import "golang.org/x/net/context"

// Worker defines something that can execute work
// for a given job removed from Resque.
type Worker interface {
	Work(queue string, args []interface{}, ctxt context.Context) error
}

// WorkerFunc implements Worker
// It is a useful wrapper for simplifying worker definition
// to that of just an anaonymous function.
type WorkerFunc func(queue string, args []interface{}, ctxt context.Context) error

func (w WorkerFunc) Work(queue string, args []interface{}, ctxt context.Context) error {
	return w(queue, args, ctxt)
}
