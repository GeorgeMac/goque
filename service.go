package worker

import (
	"fmt"
	"os"

	"golang.org/x/net/context"
)

type Service interface {
	Register(queue string, worker Worker)
	Start(root context.Context) (finish func())
}

type Queuer interface {
	RPush(queue, message string) error
	LPop(queue string) (message string, ok bool, err error)
}

type Setter interface {
	Add(set, value string) error
	Rem(set, value string) error
	Members(set string) ([]string, error)
}

type KeyValuer interface {
	Get(key string) (value string, ok bool, err error)
	Set(key, value string) error
	Del(key string) (value int64, err error)
}

type Incrementer interface {
	By(key string, value int64) error
}

type service struct {
	hostname    string
	concurrency int
	client      *Client
	workers     []func(string, context.Context) *worker
}

func New(client *Client, concurrency int) (Service, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return &service{
		client:      client,
		hostname:    hostname,
		concurrency: concurrency,
	}, nil
}

func (s *service) Register(queue string, w Worker) {
	s.workers = append(s.workers, func(id string, ctxt context.Context) *worker {
		return newWorker(s.client, w, s.hostname, id, queue)
	})
}

func (s *service) Start(root context.Context) func() {
	workers := make([]*worker, 0, len(s.workers))
	for _, w := range s.workers {
		for i := 0; i < s.concurrency; i++ {
			id := fmt.Sprintf("%d", i)
			worker := w(id, context.WithValue(root, "worker_id", id))
			worker.Start()
			workers = append(workers, worker)
		}
	}
	return func() {
		for _, worker := range workers {
			worker.Stop()
		}
	}
}
