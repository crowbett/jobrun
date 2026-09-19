package cronx

import (
	"testing"
	"time"
	_ "time/tzdata" // America/New_York etc. without relying on the OS zoneinfo
)

func loadLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", name, err)
	}
	return loc
}

// at builds a wall-clock time in loc; it must exist and be unambiguous.
func at(y int, m time.Month, d, h, minute int, loc *time.Location) time.Time {
	return time.Date(y, m, d, h, minute, 0, 0, loc)
}

func TestNextVectors(t *testing.T) {
	t.Parallel()
	ny := loadLoc(t, "America/New_York")
	kolkata := loadLoc(t, "Asia/Kolkata")
	utc := time.UTC

	// Instants around the 2026 DST transitions, written in UTC so they are unambiguous:
	// spring forward 2026-03-08 02:00 EST -> 03:00 EDT (07:00Z);
	// fall back      2026-11-01 02:00 EDT -> 01:00 EST (06:00Z).
	tests := []struct {
		name  string
		expr  string
		after time.Time
		loc   *time.Location
		want  time.Time // zero = no match within MaxLookahead
	}{
		// --- basics ---
		{"every minute, sub-minute after", "* * * * *", time.Date(2026, 9, 19, 10, 0, 30, 0, utc), utc, at(2026, 9, 19, 10, 1, utc)},
		{"every minute, strictly after", "* * * * *", at(2026, 9, 19, 10, 0, utc), utc, at(2026, 9, 19, 10, 1, utc)},
		{"exactly on the schedule is not a match", "0 10 * * *", at(2026, 9, 19, 10, 0, utc), utc, at(2026, 9, 20, 10, 0, utc)},
		{"nil loc is UTC", "0 0 1 * *", at(2026, 9, 19, 10, 0, utc), nil, at(2026, 10, 1, 0, 0, utc)},
		{"after in another zone", "0 9 * * *", at(2026, 9, 19, 14, 0, utc), ny, at(2026, 9, 20, 9, 0, ny)},
		{"fixed-offset zone", "0 12 * * *", at(2026, 9, 19, 12, 0, kolkata), kolkata, at(2026, 9, 20, 12, 0, kolkata)},
		{"year rollover", "0 0 1 1 *", at(2026, 9, 19, 0, 0, utc), utc, at(2027, 1, 1, 0, 0, utc)},

		// --- field forms ---
		{"*/15 first", "*/15 * * * *", at(2026, 9, 19, 10, 1, utc), utc, at(2026, 9, 19, 10, 15, utc)},
		{"*/15 wraps hour", "*/15 * * * *", at(2026, 9, 19, 10, 45, utc), utc, at(2026, 9, 19, 11, 0, utc)},
		{"range step 1-10/3", "1-10/3 * * * *", at(2026, 9, 19, 10, 4, utc), utc, at(2026, 9, 19, 10, 7, utc)},
		{"range step wraps", "1-10/3 * * * *", at(2026, 9, 19, 10, 10, utc), utc, at(2026, 9, 19, 11, 1, utc)},
		{"value step 5/10", "5/10 * * * *", at(2026, 9, 19, 10, 56, utc), utc, at(2026, 9, 19, 11, 5, utc)},
		{"list", "0 8,12,18 * * *", at(2026, 9, 19, 12, 0, utc), utc, at(2026, 9, 19, 18, 0, utc)},
		{"names", "0 9 * JAN-MAR MON-FRI", at(2026, 12, 25, 0, 0, utc), utc, at(2027, 1, 1, 9, 0, utc)},        // 2027-01-01 is a Friday
		{"names skip weekend", "0 9 * * MON-FRI", at(2026, 9, 18, 9, 0, utc), utc, at(2026, 9, 21, 9, 0, utc)}, // Fri 18th -> Mon 21st
		{"dow 7 is Sunday", "0 0 * * 7", at(2026, 9, 1, 0, 0, utc), utc, at(2026, 9, 6, 0, 0, utc)},            // Tue 1st -> Sun 6th
		{"dow 0 is Sunday", "0 0 * * 0", at(2026, 9, 1, 0, 0, utc), utc, at(2026, 9, 6, 0, 0, utc)},

		// --- macros ---
		{"@hourly", "@hourly", time.Date(2026, 9, 19, 10, 30, 15, 0, utc), utc, at(2026, 9, 19, 11, 0, utc)},
		{"@daily", "@daily", at(2026, 9, 19, 10, 30, utc), utc, at(2026, 9, 20, 0, 0, utc)},
		{"@weekly", "@weekly", at(2026, 9, 19, 10, 30, utc), utc, at(2026, 9, 20, 0, 0, utc)}, // Sat -> Sun
		{"@monthly", "@monthly", at(2026, 9, 19, 10, 30, utc), utc, at(2026, 10, 1, 0, 0, utc)},
		{"@yearly", "@yearly", at(2026, 9, 19, 10, 30, utc), utc, at(2027, 1, 1, 0, 0, utc)},

		// --- dom/dow rule ---
		{"dom OR dow: Friday first", "0 0 13 * 5", at(2026, 9, 1, 0, 0, utc), utc, at(2026, 9, 4, 0, 0, utc)},
		{"dom OR dow: 13th between Fridays", "0 0 13 * 5", at(2026, 9, 11, 0, 0, utc), utc, at(2026, 9, 13, 0, 0, utc)},
		{"dom OR dow: Friday after 13th", "0 0 13 * 5", at(2026, 9, 13, 0, 0, utc), utc, at(2026, 9, 18, 0, 0, utc)},
		{"dom only", "0 0 13 * *", at(2026, 9, 1, 0, 0, utc), utc, at(2026, 9, 13, 0, 0, utc)},
		{"dow only", "0 0 * * 5", at(2026, 9, 1, 0, 0, utc), utc, at(2026, 9, 4, 0, 0, utc)},
		{"*/2 dom is star: AND with dow", "0 0 */2 * 1", at(2026, 9, 1, 0, 0, utc), utc, at(2026, 9, 7, 0, 0, utc)}, // */2 dom = 1,3,..,31; Mondays 7,14,21,28 -> 7
		{"explicit full dom range is not star: OR", "0 0 1-31 * 1", at(2026, 9, 1, 0, 0, utc), utc, at(2026, 9, 2, 0, 0, utc)},

		// --- leap day and impossible dates ---
		{"leap day from 2026", "0 0 29 2 *", at(2026, 1, 1, 0, 0, utc), utc, at(2028, 2, 29, 0, 0, utc)},
		{"leap day from 2028-02-29", "0 0 29 2 *", at(2028, 2, 29, 0, 0, utc), utc, at(2032, 2, 29, 0, 0, utc)},
		{"leap day across 2100 exceeds lookahead", "0 0 29 2 *", at(2096, 3, 1, 0, 0, utc), utc, time.Time{}},
		{"Feb 30 never", "0 0 30 2 *", at(2026, 1, 1, 0, 0, utc), utc, time.Time{}},
		{"Feb 31 never", "0 0 31 2 *", at(2026, 1, 1, 0, 0, utc), utc, time.Time{}},
		{"April 31 never", "0 0 31 4 *", at(2026, 1, 1, 0, 0, utc), utc, time.Time{}},
		{"31st OR Sunday still fires", "0 0 31 4 0", at(2026, 4, 1, 0, 0, utc), utc, at(2026, 4, 5, 0, 0, utc)},

		// --- DST spring forward, America/New_York 2026-03-08 ---
		{"gap: */15 jumps 01:45 EST -> 03:00 EDT", "*/15 * * * *", time.Date(2026, 3, 8, 6, 45, 0, 0, utc), ny, time.Date(2026, 3, 8, 7, 0, 0, 0, utc)},
		{"gap: hourly 01:00 EST -> 03:00 EDT", "0 * * * *", time.Date(2026, 3, 8, 6, 0, 0, 0, utc), ny, time.Date(2026, 3, 8, 7, 0, 0, 0, utc)},
		{"gap: daily 02:30 skips to the next day", "30 2 * * *", at(2026, 3, 8, 0, 0, ny), ny, time.Date(2026, 3, 9, 6, 30, 0, 0, utc)},
		{"gap: daily 03:30 fires on the day", "30 3 * * *", at(2026, 3, 8, 0, 0, ny), ny, time.Date(2026, 3, 8, 7, 30, 0, 0, utc)},
		{"gap: daily 01:30 fires on the day", "30 1 * * *", at(2026, 3, 8, 0, 0, ny), ny, time.Date(2026, 3, 8, 6, 30, 0, 0, utc)},

		// --- DST fall back, America/New_York 2026-11-01 ---
		{"repeat: daily 01:30 fires once (EDT)", "30 1 * * *", at(2026, 11, 1, 0, 0, ny), ny, time.Date(2026, 11, 1, 5, 30, 0, 0, utc)},
		{"repeat: daily 01:30 next is the next day", "30 1 * * *", time.Date(2026, 11, 1, 5, 30, 0, 0, utc), ny, time.Date(2026, 11, 2, 6, 30, 0, 0, utc)},
		{"repeat: hourly 00:00 EDT -> 01:00 EDT", "0 * * * *", time.Date(2026, 11, 1, 4, 0, 0, 0, utc), ny, time.Date(2026, 11, 1, 5, 0, 0, 0, utc)},
		{"repeat: hourly 01:00 EDT -> 02:00 EST (second 01:00 skipped)", "0 * * * *", time.Date(2026, 11, 1, 5, 0, 0, 0, utc), ny, time.Date(2026, 11, 1, 7, 0, 0, 0, utc)},
		{"repeat: */30 from 01:30 EDT -> 02:00 EST", "*/30 * * * *", time.Date(2026, 11, 1, 5, 30, 0, 0, utc), ny, time.Date(2026, 11, 1, 7, 0, 0, 0, utc)},
		{"repeat: after inside the second pass", "*/15 * * * *", time.Date(2026, 11, 1, 6, 20, 0, 0, utc), ny, time.Date(2026, 11, 1, 7, 0, 0, 0, utc)},
		{"repeat: unaffected later time", "0 9 * * *", at(2026, 11, 1, 0, 0, ny), ny, time.Date(2026, 11, 1, 14, 0, 0, 0, utc)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := mustParse(t, tc.expr)
			got := s.Next(tc.after, tc.loc)
			if tc.want.IsZero() {
				if !got.IsZero() {
					t.Fatalf("Next = %v, want zero time", got)
				}
				return
			}
			if !got.Equal(tc.want) {
				t.Fatalf("Next(%v) = %v (%v), want %v (%v)", tc.after, got, got.UTC(), tc.want, tc.want.UTC())
			}
			wantLoc := tc.loc
			if wantLoc == nil {
				wantLoc = time.UTC
			}
			if got.Location() != wantLoc {
				t.Fatalf("Next returned location %v, want %v", got.Location(), wantLoc)
			}
			if !got.After(tc.after) {
				t.Fatalf("Next must be strictly after: %v <= %v", got, tc.after)
			}
			if got.Second() != 0 || got.Nanosecond() != 0 {
				t.Fatalf("Next must be on a whole minute: %v", got)
			}
		})
	}
}

// TestNextSequenceDST walks an hourly schedule through both 2026 New York
// transitions and checks the sequence of instants: the gap loses an hour, the
// repeat fires the wall-clock hour once.
func TestNextSequenceDST(t *testing.T) {
	t.Parallel()
	ny := loadLoc(t, "America/New_York")
	s := mustParse(t, "0 * * * *")

	walk := func(from time.Time, n int) []string {
		out := make([]string, 0, n)
		cur := from
		for i := 0; i < n; i++ {
			cur = s.Next(cur, ny)
			out = append(out, cur.Format("15:04 MST"))
		}
		return out
	}
	spring := walk(at(2026, 3, 8, 0, 0, ny), 4)
	if want := []string{"01:00 EST", "03:00 EDT", "04:00 EDT", "05:00 EDT"}; !equalStrings(spring, want) {
		t.Fatalf("spring forward sequence = %v, want %v", spring, want)
	}
	fall := walk(at(2026, 11, 1, 0, 0, ny), 4)
	if want := []string{"01:00 EDT", "02:00 EST", "03:00 EST", "04:00 EST"}; !equalStrings(fall, want) {
		t.Fatalf("fall back sequence = %v, want %v", fall, want)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestNextZeroSchedule(t *testing.T) {
	t.Parallel()
	if got := (Schedule{}).Next(time.Now(), time.UTC); !got.IsZero() {
		t.Fatalf("zero Schedule must never fire, got %v", got)
	}
}

func TestNextLookaheadBound(t *testing.T) {
	t.Parallel()
	// "0 0 29 2 *" from 2098-03-01: 2100 is not a leap year and 2104 is six
	// years out, so the bounded search must give up rather than spin.
	s := mustParse(t, "0 0 29 2 *")
	start := time.Now()
	got := s.Next(at(2098, 3, 1, 0, 0, time.UTC), time.UTC)
	if !got.IsZero() {
		t.Fatalf("expected zero, got %v", got)
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("bounded search took %v", d)
	}
	// From 2099-03-01, 2104-02-29 is inside the five-year window and is found.
	got = s.Next(at(2099, 3, 1, 0, 0, time.UTC), time.UTC)
	if want := at(2104, 2, 29, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("edge of window: got %v, want %v", got, want)
	}
}

func TestWeekdayMatchesTime(t *testing.T) {
	t.Parallel()
	d := time.Date(1999, 12, 25, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 366*30; i++ {
		if got, want := weekday(d.Year(), int(d.Month()), d.Day()), int(d.Weekday()); got != want {
			t.Fatalf("weekday(%v) = %d, want %d", d, got, want)
		}
		d = d.AddDate(0, 0, 1)
	}
}

func BenchmarkNext(b *testing.B) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		b.Fatal(err)
	}
	after := time.Date(2026, 9, 19, 10, 0, 30, 0, time.UTC)
	for _, expr := range []string{"* * * * *", "*/15 * * * *", "0 9 * * MON-FRI", "0 0 13 * 5", "@yearly", "0 0 29 2 *"} {
		s, err := Parse(expr)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(expr, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if s.Next(after, ny).IsZero() {
					b.Fatal("no match")
				}
			}
		})
	}
}

func BenchmarkParse(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Parse("0,30 9-17 * JAN-JUN MON-FRI"); err != nil {
			b.Fatal(err)
		}
	}
}
