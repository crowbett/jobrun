package admin

import (
	"bytes"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// ContentType is the Prometheus text exposition media type.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// Metrics serves the queue metrics in Prometheus text format 0.0.4, written by
// hand on a pooled buffer (no client library). cache may be nil (no depth
// series), counters may be nil (no counter series), hist may be nil (no
// histogram). build becomes the labels of jobrun_build_info; "go_version"
// defaults to runtime.Version() and "version" to "dev".
//
// Series: jobrun_queue_depth{queue,status}, jobrun_queue_oldest_age_seconds{queue}
// (oldest queued run_at), jobrun_jobs_total{queue,status}, jobrun_dead_total,
// jobrun_lease_expired_total, jobrun_scheduler_ticks_total,
// jobrun_job_duration_seconds{queue,type} (histogram), jobrun_depth_age_seconds,
// jobrun_build_info.
func Metrics(cache *DepthCache, counters func() Counters, hist *Histogram, build map[string]string) http.Handler {
	return newMetrics(cache, counters, hist, build, time.Now)
}

type metricsHandler struct {
	cache     *DepthCache
	counters  func() Counters
	hist      *Histogram
	buildInfo []byte // pre-rendered "jobrun_build_info{...} 1\n" block
	now       func() time.Time
}

func newMetrics(cache *DepthCache, counters func() Counters, hist *Histogram, build map[string]string, now func() time.Time) *metricsHandler {
	return &metricsHandler{cache: cache, counters: counters, hist: hist, buildInfo: renderBuildInfo(build), now: now}
}

var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func (h *metricsHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	b, _ := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufPool.Put(b)

	h.render(b)

	w.Header().Set("Content-Type", ContentType)
	w.Header().Set("Content-Length", strconv.Itoa(b.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b.Bytes())
}

// render writes the whole exposition into b.
func (h *metricsHandler) render(b *bytes.Buffer) {
	e := exposer{b: b}
	now := h.now()

	if h.cache != nil {
		rows, at := h.cache.Depth()
		if !at.IsZero() {
			h.renderDepth(&e, rows, now)
			e.gauge("jobrun_depth_age_seconds", "Seconds since the depth table was last refreshed.", max(0, now.Sub(at).Seconds()))
		}
	}
	if h.counters != nil {
		c := h.counters()
		keys := make([]JobKey, 0, len(c.Jobs))
		for k := range c.Jobs {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].Queue != keys[j].Queue {
				return keys[i].Queue < keys[j].Queue
			}
			return keys[i].Status < keys[j].Status
		})
		e.header("jobrun_jobs_total", "Job outcomes by queue and status: succeeded, dead, cancelled, retried.", "counter")
		for _, k := range keys {
			e.labelledU("jobrun_jobs_total", e.labels("queue", k.Queue, "status", k.Status), c.Jobs[k])
		}
		e.counter("jobrun_dead_total", "Jobs that reached the dead state (attempts exhausted or permanent failure).", c.Dead)
		e.counter("jobrun_lease_expired_total", "Leases the reaper found expired and requeued or killed.", c.LeaseExpired)
		e.counter("jobrun_lease_fenced_total", "Handlers cancelled because a heartbeat found their lease lost; never completed.", c.LeaseFenced)
		e.counter("jobrun_scheduler_ticks_total", "Scheduler ticks run by this process while it held the leader lock.", c.SchedulerTicks)
	}
	if h.hist != nil {
		h.renderHistogram(&e)
	}
	b.Write(h.buildInfo)
}

func (h *metricsHandler) renderDepth(e *exposer, rows []job.DepthRow, now time.Time) {
	sorted := make([]job.DepthRow, len(rows))
	copy(sorted, rows)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Queue != sorted[j].Queue {
			return sorted[i].Queue < sorted[j].Queue
		}
		return sorted[i].Status < sorted[j].Status
	})
	e.header("jobrun_queue_depth", "Jobs by queue and status.", "gauge")
	for _, r := range sorted {
		e.labelledI("jobrun_queue_depth", e.labels("queue", r.Queue, "status", r.Status.String()), r.N)
	}
	e.header("jobrun_queue_oldest_age_seconds", "Age of the oldest queued job's run_at per queue (0 when empty or not yet due).", "gauge")
	for _, r := range sorted {
		if r.Status != job.StatusQueued {
			continue
		}
		var age float64
		if r.N > 0 && !r.OldestRunAt.IsZero() {
			age = max(0, now.Sub(r.OldestRunAt).Seconds())
		}
		e.labelled("jobrun_queue_oldest_age_seconds", e.labels("queue", r.Queue), age)
	}
}

func (h *metricsHandler) renderHistogram(e *exposer) {
	const name = "jobrun_job_duration_seconds"
	e.header(name, "Handler run time per queue and type.", "histogram")
	for _, s := range h.hist.Snapshot() {
		for i, le := range DurationBuckets {
			e.labelledU(name+"_bucket", e.labels("queue", s.Key.Queue, "type", s.Key.Type, "le", strconv.FormatFloat(le, 'g', -1, 64)), s.Cumulative[i])
		}
		e.labelledU(name+"_bucket", e.labels("queue", s.Key.Queue, "type", s.Key.Type, "le", "+Inf"), s.Count)
		e.labelled(name+"_sum", e.labels("queue", s.Key.Queue, "type", s.Key.Type), s.Sum)
		e.labelledU(name+"_count", e.labels("queue", s.Key.Queue, "type", s.Key.Type), s.Count)
	}
}

// exposer is a tiny append-only writer for the text format.
type exposer struct {
	b   *bytes.Buffer
	tmp [32]byte
}

func (e *exposer) header(name, help, kind string) {
	e.b.WriteString("# HELP ")
	e.b.WriteString(name)
	e.b.WriteByte(' ')
	e.b.WriteString(help)
	e.b.WriteString("\n# TYPE ")
	e.b.WriteString(name)
	e.b.WriteByte(' ')
	e.b.WriteString(kind)
	e.b.WriteByte('\n')
}

func (e *exposer) float(v float64) {
	e.b.Write(strconv.AppendFloat(e.tmp[:0], v, 'g', -1, 64))
	e.b.WriteByte('\n')
}

func (e *exposer) uint(v uint64) {
	e.b.Write(strconv.AppendUint(e.tmp[:0], v, 10))
	e.b.WriteByte('\n')
}

func (e *exposer) gauge(name, help string, v float64) {
	e.header(name, help, "gauge")
	e.b.WriteString(name)
	e.b.WriteByte(' ')
	e.float(v)
}

func (e *exposer) counter(name, help string, v uint64) {
	e.header(name, help, "counter")
	e.b.WriteString(name)
	e.b.WriteByte(' ')
	e.uint(v)
}

// labelled writes `name<labels> v` where labels already carries the trailing space.
func (e *exposer) labelled(name, labels string, v float64) {
	e.b.WriteString(name)
	e.b.WriteString(labels)
	e.float(v)
}

func (e *exposer) labelledU(name, labels string, v uint64) {
	e.b.WriteString(name)
	e.b.WriteString(labels)
	e.uint(v)
}

func (e *exposer) labelledI(name, labels string, v int64) {
	e.b.WriteString(name)
	e.b.WriteString(labels)
	e.b.Write(strconv.AppendInt(e.tmp[:0], v, 10))
	e.b.WriteByte('\n')
}

// labels renders `{k1="v1",k2="v2"} ` from alternating key/value pairs with
// values escaped per the text format (backslash, double quote and newline).
func (e *exposer) labels(kv ...string) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(kv[i])
		sb.WriteString(`="`)
		writeEscaped(&sb, kv[i+1])
		sb.WriteByte('"')
	}
	sb.WriteString(`} `)
	return sb.String()
}

func writeEscaped(sb *strings.Builder, v string) {
	for i := 0; i < len(v); i++ {
		switch c := v[i]; c {
		case '\\':
			sb.WriteString(`\\`)
		case '"':
			sb.WriteString(`\"`)
		case '\n':
			sb.WriteString(`\n`)
		default:
			sb.WriteByte(c)
		}
	}
}

// renderBuildInfo pre-renders the constant jobrun_build_info block with labels
// in sorted key order.
func renderBuildInfo(info map[string]string) []byte {
	labels := make(map[string]string, len(info)+2)
	for k, v := range info {
		labels[k] = v
	}
	if _, ok := labels["go_version"]; !ok {
		labels["go_version"] = runtime.Version()
	}
	if _, ok := labels["version"]; !ok {
		labels["version"] = "dev"
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString("# HELP jobrun_build_info Build metadata; always 1.\n# TYPE jobrun_build_info gauge\njobrun_build_info{")
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteString(`="`)
		writeEscaped(&sb, labels[k])
		sb.WriteByte('"')
	}
	sb.WriteString("} 1\n")
	return []byte(sb.String())
}
