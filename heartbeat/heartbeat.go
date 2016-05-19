package heartbeat

import (
	"time"

	"github.com/honeycomb-tv/assure/lib/worker/keys"
	"github.com/honeycomb-tv/assure/lib/worker/util"
)

const HeartbeatKey = keys.WorkersPrefix + ":heartbeat"

var now = time.Now

type Hasher interface {
	HSet(hash, key, value string) error
	HDel(hash, key string) (value int64, err error)
}

type Heartbeat struct {
	now    func() string
	queue  string
	hash   Hasher
	ticker *time.Ticker
	stop   chan struct{}
}

func New(hash Hasher, queue string, duration time.Duration) *Heartbeat {
	ticker := time.NewTicker(duration)
	return &Heartbeat{
		now:    func() string { return util.FormatTime(now()) },
		queue:  queue,
		hash:   hash,
		ticker: ticker,
		stop:   make(chan struct{}),
	}
}

func (h *Heartbeat) Start() {
	h.hash.HSet(HeartbeatKey, h.queue, h.now())
	for {
		select {
		case <-h.stop:
			return
		case <-h.ticker.C:
			h.hash.HSet(HeartbeatKey, h.queue, h.now())
		}
	}
}

func (h *Heartbeat) Stop() error {
	// stop publishing hearbeats
	h.ticker.Stop()
	// ensure beat has finished
	h.stop <- struct{}{}
	// delete heartbeat key
	if _, err := h.hash.HDel(HeartbeatKey, h.queue); err != nil {
		return err
	}

	return nil
}
