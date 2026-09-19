package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
)

const scheduleUsage = `usage: jobrun schedule <create|list|get|update|delete> [flags]`

func (c *client) schedule(args []string) error {
	if len(args) == 0 {
		return errors.New(scheduleUsage)
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		return c.scheduleCreate(rest)
	case "list":
		return c.scheduleList(rest)
	case "get":
		return c.scheduleGet(rest)
	case "update":
		return c.scheduleUpdate(rest)
	case "delete":
		return c.scheduleDelete(rest)
	}
	return fmt.Errorf("unknown schedule command %q\n%s", sub, scheduleUsage)
}

func (c *client) scheduleCreate(args []string) error {
	fs := flag.NewFlagSet("schedule create", flag.ContinueOnError)
	name := fs.String("name", "", "unique schedule name (required)")
	cron := fs.String("cron", "", `5-field cron or @hourly/@daily/... (required), e.g. "*/1 * * * *"`)
	tz := fs.String("tz", "UTC", "IANA timezone")
	queue := fs.String("queue", "default", "template queue")
	typ := fs.String("type", "", "template job type (required)")
	payload := fs.String("payload", "{}", "template JSON payload")
	priority := fs.Int("priority", 0, "-100..100")
	maxAttempts := fs.Int("max-attempts", 0, "1..100; 0 = default")
	disabled := fs.Bool("disabled", false, "create the schedule paused")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *cron == "" || *typ == "" {
		return errors.New("--name, --cron and --type are required")
	}
	req := &jobrunv1.CreateScheduleRequest{
		Name: *name, Cron: *cron, Timezone: *tz,
		Template: &jobrunv1.JobTemplate{
			Queue: *queue, Type: *typ, Payload: []byte(*payload),
			Priority:    int32(*priority),    //nolint:gosec // validated server-side
			MaxAttempts: int32(*maxAttempts), //nolint:gosec // validated server-side
		},
	}
	if *disabled {
		req.Enabled = proto.Bool(false)
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.sched.CreateSchedule(ctx, req)
	if err != nil {
		return err
	}
	return printJSON(resp.GetSchedule())
}

func (c *client) scheduleList(args []string) error {
	fs := flag.NewFlagSet("schedule list", flag.ContinueOnError)
	size := fs.Int("size", 50, "page size, 1..200")
	token := fs.String("token", "", "next_page_token from the previous page")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.sched.ListSchedules(ctx, &jobrunv1.ListSchedulesRequest{PageToken: *token, PageSize: int32(*size)}) //nolint:gosec // validated server-side
	if err != nil {
		return err
	}
	rows := [][]string{{"ID", "NAME", "CRON", "TZ", "QUEUE", "TYPE", "ENABLED", "NEXT_RUN", "LAST_RUN"}}
	for _, s := range resp.GetSchedules() {
		rows = append(rows, []string{
			s.GetId(), s.GetName(), s.GetCron(), s.GetTimezone(), s.GetTemplate().GetQueue(),
			s.GetTemplate().GetType(), strconv.FormatBool(s.GetEnabled()), fmtTS(s.GetNextRun()), fmtTS(s.GetLastRun()),
		})
	}
	table(rows)
	if t := resp.GetNextPageToken(); t != "" {
		fmt.Println("next_page_token:", t)
	}
	return nil
}

func (c *client) scheduleGet(args []string) error {
	fs := flag.NewFlagSet("schedule get", flag.ContinueOnError)
	id := fs.String("id", "", "schedule id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.sched.GetSchedule(ctx, &jobrunv1.GetScheduleRequest{Id: *id})
	if err != nil {
		return err
	}
	return printJSON(resp.GetSchedule())
}

func (c *client) scheduleUpdate(args []string) error {
	fs := flag.NewFlagSet("schedule update", flag.ContinueOnError)
	id := fs.String("id", "", "schedule id (required)")
	cron := fs.String("cron", "", "new cron expression (unset = keep)")
	tz := fs.String("tz", "", "new IANA timezone (unset = keep)")
	enabled := fs.String("enabled", "", "true|false (unset = keep)")
	queue := fs.String("queue", "", "new template queue (with --type: replaces the template)")
	typ := fs.String("type", "", "new template type")
	payload := fs.String("payload", "{}", "new template JSON payload (with --type)")
	priority := fs.Int("priority", 0, "new template priority (with --type)")
	maxAttempts := fs.Int("max-attempts", 0, "new template max attempts (with --type)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	req := &jobrunv1.UpdateScheduleRequest{Id: *id}
	if *cron != "" {
		req.Cron = proto.String(*cron)
	}
	if *tz != "" {
		req.Timezone = proto.String(*tz)
	}
	switch *enabled {
	case "":
	case "true":
		req.Enabled = proto.Bool(true)
	case "false":
		req.Enabled = proto.Bool(false)
	default:
		return fmt.Errorf("--enabled must be true or false, got %q", *enabled)
	}
	if *typ != "" {
		q := *queue
		if q == "" {
			q = "default"
		}
		req.Template = &jobrunv1.JobTemplate{
			Queue: q, Type: *typ, Payload: []byte(*payload),
			Priority:    int32(*priority),    //nolint:gosec // validated server-side
			MaxAttempts: int32(*maxAttempts), //nolint:gosec // validated server-side
		}
	}
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.sched.UpdateSchedule(ctx, req)
	if err != nil {
		return err
	}
	return printJSON(resp.GetSchedule())
}

func (c *client) scheduleDelete(args []string) error {
	fs := flag.NewFlagSet("schedule delete", flag.ContinueOnError)
	id := fs.String("id", "", "schedule id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	ctx, cancel := c.call()
	defer cancel()
	if _, err := c.sched.DeleteSchedule(ctx, &jobrunv1.DeleteScheduleRequest{Id: *id}); err != nil {
		return err
	}
	fmt.Println("deleted:", *id)
	return nil
}
