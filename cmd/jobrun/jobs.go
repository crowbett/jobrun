package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
)

// printJSON prints one message as indented protojson (the single-object format).
func printJSON(m proto.Message) error {
	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: false}.Marshal(m)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, string(b))
	return err
}

// table writes aligned rows; the first row is the header.
func table(rows [][]string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	for _, r := range rows {
		fmt.Fprintln(w, strings.Join(r, "\t"))
	}
	_ = w.Flush()
}

func fmtTS(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return "-"
	}
	return ts.AsTime().UTC().Format("2006-01-02T15:04:05.000Z")
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func (c *client) enqueue(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	queue := fs.String("queue", "default", "queue name")
	typ := fs.String("type", "", "job type (required; built-ins: noop, sleep, fail, echo)")
	payload := fs.String("payload", "{}", "JSON payload")
	key := fs.String("key", "", "idempotency key (unique per queue)")
	priority := fs.Int("priority", 0, "-100..100, higher first")
	delay := fs.Duration("delay", 0, "run after this long (database clock)")
	runAt := fs.String("run-at", "", "RFC3339 absolute run time (exclusive with --delay)")
	maxAttempts := fs.Int("max-attempts", 0, "1..100; 0 = server default (5)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *typ == "" {
		return errors.New("--type is required")
	}
	req := &jobrunv1.EnqueueRequest{
		Queue: *queue, Type: *typ, Payload: []byte(*payload),
		Priority:       int32(*priority),    //nolint:gosec // validated server-side
		MaxAttempts:    int32(*maxAttempts), //nolint:gosec // validated server-side
		IdempotencyKey: *key,
	}
	switch {
	case *runAt != "" && *delay != 0:
		return errors.New("--run-at and --delay are exclusive")
	case *runAt != "":
		t, err := time.Parse(time.RFC3339Nano, *runAt)
		if err != nil {
			return fmt.Errorf("--run-at: %w", err)
		}
		req.When = &jobrunv1.EnqueueRequest_RunAt{RunAt: timestamppb.New(t)}
	case *delay != 0:
		req.When = &jobrunv1.EnqueueRequest_Delay{Delay: durationpb.New(*delay)}
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.jobs.Enqueue(ctx, req)
	if err != nil {
		return err
	}
	if err := printJSON(resp.GetJob()); err != nil {
		return err
	}
	// Spelled out because protojson omits a false bool: a replay of an
	// idempotency key returns the stored job with created=false.
	fmt.Println("created:", resp.GetCreated())
	return nil
}

func (c *client) get(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	id := fs.String("id", "", "job id (required)")
	events := fs.Bool("events", false, "include the transition history")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.jobs.Get(ctx, &jobrunv1.GetRequest{Id: *id, IncludeEvents: *events})
	if err != nil {
		return err
	}
	if err := printJSON(resp.GetJob()); err != nil {
		return err
	}
	if *events {
		rows := [][]string{{"AT", "FROM", "TO", "ATTEMPT", "ERROR"}}
		for _, e := range resp.GetEvents() {
			rows = append(rows, []string{
				fmtTS(e.GetAt()), statusName(e.GetFromStatus()), statusName(e.GetToStatus()),
				strconv.Itoa(int(e.GetAttempt())), truncate(e.GetError(), 80),
			})
		}
		fmt.Println()
		table(rows)
	}
	return nil
}

func (c *client) list(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	queue := fs.String("queue", "", "queue (empty = all)")
	status := fs.String("status", "", "queued|leased|succeeded|failed|dead|cancelled (empty = all)")
	typ := fs.String("type", "", "job type (empty = all)")
	size := fs.Int("size", 50, "page size, 1..200")
	token := fs.String("token", "", "next_page_token from the previous page")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := parseStatus(*status)
	if err != nil {
		return err
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.jobs.List(ctx, &jobrunv1.ListRequest{
		Queue: *queue, Status: st, Type: *typ, PageToken: *token,
		PageSize: int32(*size), //nolint:gosec // validated server-side
	})
	if err != nil {
		return err
	}
	rows := [][]string{{"ID", "STATUS", "QUEUE", "TYPE", "ATT", "RUN_AT", "CREATED", "FINISHED", "ERROR"}}
	for _, j := range resp.GetJobs() {
		rows = append(rows, []string{
			j.GetId(), statusName(j.GetStatus()), j.GetQueue(), j.GetType(),
			fmt.Sprintf("%d/%d", j.GetAttempts(), j.GetMaxAttempts()), fmtTS(j.GetRunAt()), fmtTS(j.GetCreatedAt()),
			fmtTS(j.GetFinishedAt()), truncate(j.GetLastError(), 40),
		})
	}
	table(rows)
	if t := resp.GetNextPageToken(); t != "" {
		fmt.Println("next_page_token:", t)
	}
	return nil
}

func (c *client) cancel(args []string) error {
	fs := flag.NewFlagSet("cancel", flag.ContinueOnError)
	id := fs.String("id", "", "job id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.jobs.Cancel(ctx, &jobrunv1.CancelRequest{Id: *id})
	if err != nil {
		return err
	}
	return printJSON(resp.GetJob())
}

func (c *client) retry(args []string) error {
	fs := flag.NewFlagSet("retry", flag.ContinueOnError)
	id := fs.String("id", "", "job id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.jobs.Retry(ctx, &jobrunv1.RetryRequest{Id: *id})
	if err != nil {
		return err
	}
	return printJSON(resp.GetJob())
}

func (c *client) purge(args []string) error {
	fs := flag.NewFlagSet("purge", flag.ContinueOnError)
	queue := fs.String("queue", "", "queue (empty = all)")
	status := fs.String("status", "", "comma list of succeeded|cancelled|dead (empty = all three)")
	olderThan := fs.Duration("older-than", 0, "finished_at older than this (0 = any age)")
	limit := fs.Int("limit", 0, "max rows per call (0 = server default)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	req := &jobrunv1.PurgeRequest{Queue: *queue, Limit: int32(*limit)} //nolint:gosec // validated server-side
	for s := range strings.SplitSeq(*status, ",") {
		if s = strings.TrimSpace(s); s != "" {
			st, err := parseStatus(s)
			if err != nil {
				return err
			}
			req.Statuses = append(req.Statuses, st)
		}
	}
	if *olderThan > 0 {
		req.OlderThan = durationpb.New(*olderThan)
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.jobs.Purge(ctx, req)
	if err != nil {
		return err
	}
	fmt.Println("deleted:", resp.GetDeleted())
	return nil
}

func (c *client) stats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	queue := fs.String("queue", "", "queue (empty = all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.jobs.Stats(ctx, &jobrunv1.StatsRequest{Queue: *queue})
	if err != nil {
		return err
	}
	rows := [][]string{{"QUEUE", "STATUS", "COUNT", "OLDEST_RUN_AT"}}
	for _, d := range resp.GetDepths() {
		rows = append(rows, []string{d.GetQueue(), statusName(d.GetStatus()), strconv.FormatInt(d.GetCount(), 10), fmtTS(d.GetOldestRunAt())})
	}
	table(rows)
	fmt.Println("as_of:", fmtTS(resp.GetAsOf()))
	return nil
}
