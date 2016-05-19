package goque

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"golang.org/x/net/context"

	"github.com/honeycomb-tv/assure/lib/worker/heartbeat"
	"github.com/honeycomb-tv/assure/lib/worker/keys"
	"github.com/honeycomb-tv/assure/lib/worker/util"
)

var now = time.Now

type worker struct {
	worker Worker

	ctxt context.Context

	setter      Setter
	queue       Queuer
	keyValuer   KeyValuer
	incrementer Incrementer

	heartbeat *heartbeat.Heartbeat
	keys      *keys.Cache

	popTicker *time.Ticker
	stopPop   chan struct{}
}

func newWorker(client *Client, wkr Worker, host, id, queue string) *worker {
	keys := keys.New(host, id, queue)
	return &worker{
		worker:      wkr,
		ctxt:        context.Background(),
		setter:      client,
		queue:       client,
		keyValuer:   client,
		incrementer: client,
		keys:        keys,
		heartbeat:   heartbeat.New(client, keys.Queue(), 1*time.Minute),
		popTicker:   time.NewTicker(5 * time.Second),
		stopPop:     make(chan struct{}),
	}
}

func (w *worker) Start() {
	// start a new heartbeat
	go w.heartbeat.Start()

	w.addWorker()

	// beginning attempting to pop
	go w.popJobs()
}

func (w *worker) addWorker() error {
	var found bool
	// SMEMBERS `resque:workers`
	members, err := w.setter.Members(keys.WorkersPrefix)
	if err != nil {
		return err
	}

	// check to see if worker already exists
	for _, member := range members {
		if member == w.keys.WorkerName() {
			found = true
			break
		}
	}

	if !found {
		// SADD `resque:workers` `<worker_name>`
		if err := w.setter.Add(keys.WorkersPrefix, w.keys.WorkerName()); err != nil {
			return err
		}
	}

	// SET `<worker>:started` <time.Now()>
	if err := w.keyValuer.Set(w.keys.WorkerStarted(), util.FormatTime(now())); err != nil {
		return err
	}

	return nil
}

func (w *worker) Stop() error {
	// stop popping, wait till ready
	w.stopPopping()

	errors := NewErrorSlice()
	// stop tracking worker in redis
	for _, finish := range []func() error{w.removeWorker, w.removeStats, w.heartbeat.Stop} {
		if err := finish(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

func (w *worker) removeWorker() error {
	// SREM `rescue:workers` `<host>:<port>:<queue>`
	if err := w.setter.Rem(keys.WorkersPrefix, w.keys.WorkerName()); err != nil {
		return err
	}

	// GET `resque:worker:<host>:<port>:<queue>`
	if _, ok, err := w.keyValuer.Get(w.keys.Worker()); err != nil {
		return err
	} else if ok {
		// We are shutting down, but a worker appears to be holding
		// on to a task!
		if err := w.removeWorkerKey(); err != nil {
			return err
		}
	}

	// DEL `resque:worker:<host>:<port>:<queue>:started`
	if _, err := w.keyValuer.Del(w.keys.WorkerStarted()); err != nil {
		return err
	}
	return nil
}

func (w *worker) removeWorkerKey() error {
	// DEL `resque:worker:<host>:<port>:<queue>`
	if _, err := w.keyValuer.Del(w.keys.Worker()); err != nil {
		return err
	}
	return nil
}

func (w *worker) removeStats() error {
	// DEL `resque:stat:processed:<host>:<port>:<queue>`
	if _, err := w.keyValuer.Del(w.keys.StatWorkerProcessed()); err != nil {
		return err
	}

	// DEL `resque:stat:failed:<host>:<port>:<queue>`
	if _, err := w.keyValuer.Del(w.keys.StatWorkerFailed()); err != nil {
		return err
	}

	return nil
}

func (w *worker) popJobs() {
	for {
		select {
		case <-w.stopPop:
			// shutting down worker
			return
		case <-w.popTicker.C:
			job, ok, err := w.queue.LPop(w.keys.Queue())
			if err != nil {
				log.Println("popJobs", err)
				//TODO: something with the error
			}

			if !ok {
				// nothing to see just yet
				continue
			}

			if err := w.keyValuer.Set(w.keys.Worker(), job); err != nil {
				//TODO: what now?
				log.Println("popJobs", err)
				continue
			}

			// do job
			if err := w.performJob(job); err != nil {
				//TODO: something? or remove error throw altogether?
				log.Println("popJobs", err)
			}
		}
	}
}

func (w *worker) stopPopping() {
	w.stopPop <- struct{}{}
}

type Job struct {
	Queue string    `json:"queue"`
	RunAt time.Time `json:run_at`
	Class string    `class`
	Args  []struct {
		JobClass  string        `json:"job_class"`
		JobId     string        `json:"job_id"`
		QueueName string        `json:"queue_name"`
		Arguments []interface{} `json:"arguments"`
		Locale    string        `json:"locale"`
	} `json:"args"`
}

func (w *worker) performJob(payload string) error {
	job := &Job{}
	if err := json.Unmarshal([]byte(payload), job); err != nil {
		return err
	}

	for _, innerJob := range job.Args {
		ctxt := context.WithValue(w.ctxt, "job_id", innerJob.JobId)
		if err := w.worker.Work(innerJob.QueueName, innerJob.Arguments, ctxt); err != nil {
			if err := w.failJob(job, err); err != nil {
				return err
			}
		} else {
			w.succeedJob(job)
		}
	}

	if err := w.incrementKeys(keys.StatProcessedPrefix, w.keys.StatWorkerProcessed()); err != nil {
		return err
	}

	// DEL `resque:worker:<host>:<port>:<queue>`
	return w.removeWorkerKey()
}

func (w *worker) succeedJob(job *Job) {
	// TODO: maybe some logging
}

type FailedJob struct {
	FailedAt  string `json:"failed_at"`
	Payload   Job    `json:"payload"`
	Exception string `json:"exception"`
	Error     string `json:"error"`
	Backtrace string `json:"backtrace"`
	Worker    string `json:"worker"`
	Queue     string `json:"queue"`
}

func (w *worker) failJob(job *Job, err error) error {
	failedJob := FailedJob{
		FailedAt:  util.FormatTime(now()),
		Payload:   *job,
		Exception: fmt.Sprintf("%T", err),
		Error:     err.Error(),
		Worker:    w.keys.Worker(),
		Queue:     job.Queue,
	}

	data, err := json.Marshal(&failedJob)
	if err != nil {
		return err
	}

	if err := w.queue.RPush(keys.FailedQueue, string(data)); err != nil {
		return err
	}

	return w.incrementKeys(keys.StatFailedPrefix, w.keys.StatWorkerFailed())
}

func (w *worker) incrementKeys(keys ...string) error {
	for _, key := range keys {
		if err := w.incrementer.By(key, 1); err != nil {
			return err
		}
	}
	return nil
}
