package keys

import "fmt"

const (
	ResquePrefix        = "resque"
	WorkerPrefix        = ResquePrefix + ":worker"
	WorkersPrefix       = ResquePrefix + ":workers"
	QueuePrefix         = ResquePrefix + ":queue"
	FailedQueue         = ResquePrefix + ":failed"
	StatPrefix          = ResquePrefix + ":stat"
	StatProcessedPrefix = StatPrefix + ":processed"
	StatFailedPrefix    = StatPrefix + ":failed"
)

type Cache struct {
	host, id, queue string
	worker          *string
	workerPrefixed  *string
	workerStarted   *string
	workerProcessed *string
	workerFailed    *string
	queuePrefixed   *string
}

func New(host, id, queue string) *Cache {
	var (
		worker          string
		workerPrefixed  string
		workerStarted   string
		workerProcessed string
		workerFailed    string
		queuePrefixed   string
	)

	return &Cache{
		host:            host,
		id:              id,
		queue:           queue,
		worker:          &worker,
		workerPrefixed:  &workerPrefixed,
		workerStarted:   &workerStarted,
		workerProcessed: &workerProcessed,
		workerFailed:    &workerFailed,
		queuePrefixed:   &queuePrefixed,
	}
}

func (c *Cache) QueueName() string { return c.queue }

func (c *Cache) Queue() string {
	return getOrCreate(c.queuePrefixed, "%s:%s", QueuePrefix, c.QueueName())
}

func (c *Cache) WorkerName() string {
	return getOrCreate(c.worker, "%s:%s:%s", c.host, c.id, c.queue)
}

func (c *Cache) Worker() string {
	return getOrCreate(c.workerPrefixed, "%s:%s", WorkerPrefix, c.WorkerName())
}

func (c *Cache) WorkerStarted() string {
	return getOrCreate(c.workerStarted, "%s:%s", c.Worker(), "started")
}

func (c *Cache) StatWorkerProcessed() string {
	return getOrCreate(c.workerProcessed, "%s:%s", StatProcessedPrefix, c.WorkerName())
}

func (c *Cache) StatWorkerFailed() string {
	return getOrCreate(c.workerFailed, "%s:%s", StatFailedPrefix, c.WorkerName())
}

func getOrCreate(v *string, format string, a ...interface{}) string {
	if *v == "" {
		*v = fmt.Sprintf(format, a...)
	}

	return *v
}
