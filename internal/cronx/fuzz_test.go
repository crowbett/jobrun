package cronx

import (
	"testing"
	"time"
)

// FuzzParse checks that Parse never panics, that every accepted expression
// round-trips through String, and that Next on the result terminates without
// panicking.
func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		"* * * * *", "*/15 * * * *", "0 0 1 1 *", "@daily", "@hourly", "@bogus",
		"0 9 * JAN-MAR MON-FRI", "0 0 13 * 5", "0 0 */2 * 1", "0 0 29 2 *", "5/10 * * * *",
		"1-10/3 * * * *", "0 0 * * 7", "59 23 31 12 6", "", "* * * *", "60 * * * *",
		"1,,2 * * * *", "*/0 * * * *", "-1 * * * *", "0 0 * * SAT-SUN", "\t0 0 * * *\n",
	} {
		f.Add(seed)
	}
	after := time.Date(2026, 9, 19, 10, 0, 30, 0, time.UTC)
	f.Fuzz(func(t *testing.T, expr string) {
		s, err := Parse(expr)
		if err != nil {
			if !s.IsZero() {
				t.Fatalf("Parse(%q) failed but returned %+v", expr, s)
			}
			return
		}
		if s.IsZero() {
			t.Fatalf("Parse(%q) succeeded with the zero Schedule", expr)
		}
		again, err := Parse(s.String())
		if err != nil {
			t.Fatalf("Parse(%q).String() = %q does not re-parse: %v", expr, s.String(), err)
		}
		if again != s {
			t.Fatalf("round trip mismatch for %q: %+v != %+v", expr, again, s)
		}
		if next := s.Next(after, time.UTC); !next.IsZero() && !next.After(after) {
			t.Fatalf("Next(%q) = %v is not after %v", expr, next, after)
		}
	})
}
