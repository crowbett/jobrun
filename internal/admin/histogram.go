package admin

import (
	"sort"
	"sync"
	"time"
)

// DurationObserver is what the worker calls when a handler finishes, so
// jobrun_job_duration_seconds can be exposed without the worker importing
// this package. *Histogram implements it; cmd/jobrund passes it to the pool.
type DurationObserver interface {
	ObserveDuration(queue, typ string, d time.Duration)
}

// DurationBuckets are the explicit upper bounds (seconds) of
// jobrun_job_duration_seconds; +Inf is implicit.
var DurationBuckets = durationBuckets[:]

const numBuckets = 14

var durationBuckets = [numBuckets]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

// SeriesKey labels one histogram series.
type SeriesKey struct {
	Queue string
	Type  string
}

// Histogram is a fixed-bucket cumulative histogram per (queue, type). It is
// written by handler goroutines and read by /metrics; both paths are cheap.
type Histogram struct {
	mu     sync.Mutex
	series map[SeriesKey]*series
}

type series struct {
	counts [numBuckets]uint64 // per-bucket (not cumulative) counts
	inf    uint64             // observations above the last bucket
	sum    float64
	count  uint64
}

// NewHistogram builds an empty histogram.
func NewHistogram() *Histogram {
	return &Histogram{series: make(map[SeriesKey]*series)}
}

// ObserveDuration records one job run.
func (h *Histogram) ObserveDuration(queue, typ string, d time.Duration) {
	secs := d.Seconds()
	if secs < 0 {
		secs = 0
	}
	k := SeriesKey{queue, typ}
	h.mu.Lock()
	s := h.series[k]
	if s == nil {
		s = &series{}
		h.series[k] = s
	}
	i := sort.SearchFloat64s(DurationBuckets, secs) // first bucket with le >= secs
	if i < len(DurationBuckets) {
		s.counts[i]++
	} else {
		s.inf++
	}
	s.sum += secs
	s.count++
	h.mu.Unlock()
}

// Series is a read-only snapshot of one (queue, type) histogram with
// cumulative bucket counts in DurationBuckets order.
type Series struct {
	Key        SeriesKey
	Cumulative []uint64 // len(DurationBuckets); the +Inf bucket equals Count
	Sum        float64
	Count      uint64
}

// Snapshot returns every series sorted by queue then type.
func (h *Histogram) Snapshot() []Series {
	h.mu.Lock()
	out := make([]Series, 0, len(h.series))
	for k, s := range h.series {
		cum := make([]uint64, len(DurationBuckets))
		var acc uint64
		for i, c := range s.counts {
			acc += c
			cum[i] = acc
		}
		out = append(out, Series{Key: k, Cumulative: cum, Sum: s.sum, Count: s.count})
	}
	h.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key.Queue != out[j].Key.Queue {
			return out[i].Key.Queue < out[j].Key.Queue
		}
		return out[i].Key.Type < out[j].Key.Type
	})
	return out
}
