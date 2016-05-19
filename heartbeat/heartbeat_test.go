package heartbeat

import (
	"testing"
	"time"
)

var expstr = `Expected "%+v", Got "%+v"\n`

func Test_New(t *testing.T) {
	h := New(&dummyHash{}, "queue_name", time.Hour)

	if h == nil {
		t.Fatalf(expstr, "a heartbeat to be returned", nil)
	}

	when := time.Date(2016, 1, 1, 1, 0, 0, 0, time.UTC)
	expected := "2016-01-01T01:00:00+00:00"
	// mock out time.Now
	now = func() time.Time { return when }

	if timestamp := h.now(); timestamp != expected {
		t.Errorf(expstr, expected, timestamp)
	}
}

func Test_Beat(t *testing.T) {
	ticks := make(chan time.Time)
	ticker := &time.Ticker{C: ticks}
	dummy := &dummyHash{mem: map[string]map[string]string{}}
	h := &Heartbeat{
		now:    func() string { return "time is nigh" },
		queue:  "localhost:12345:some_queue_name",
		hash:   dummy,
		ticker: ticker,
		stop:   make(chan struct{}),
	}

	go h.Start()

	ticks <- time.Time{}
	ticks <- time.Time{}
	ticks <- time.Time{}
	ticks <- time.Time{}
	ticks <- time.Time{}

	// just enough to finish last beat
	time.Sleep(500 * time.Millisecond)

	if dummy.calledSet != 5 {
		t.Errorf(expstr, 5, dummy.calledSet)
	}

	if value := dummy.mem["resque:workers:heartbeat"]["localhost:12345:some_queue_name"]; value != "time is nigh" {
		t.Errorf(expstr, "time is nigh", value)
	}

	if err := h.Stop(); err != nil {
		t.Errorf(expstr, nil, err)
	}

	if value, ok := dummy.mem["resque:workers:heartbeat"]["localhost:12345:some_queue_name"]; ok {
		t.Errorf(expstr, "hash key to be unset", value)
	}
}

type dummyHash struct {
	mem       map[string]map[string]string
	calledSet int
}

func (d *dummyHash) HSet(h, k, v string) error {
	d.calledSet++

	var (
		hash map[string]string
		ok   bool
	)

	if hash, ok = d.mem[h]; !ok {
		hash = map[string]string{}
		d.mem[h] = hash
	}
	hash[k] = v

	return nil
}

func (d *dummyHash) HDel(h, k string) (int64, error) {
	delete(d.mem[h], k)
	return 0, nil
}
