package cronx

import (
	"errors"
	"testing"
)

func mustParse(t *testing.T, expr string) Schedule {
	t.Helper()
	s, err := Parse(expr)
	if err != nil {
		t.Fatalf("Parse(%q): %v", expr, err)
	}
	return s
}

func bitsOf(vals ...int) uint64 {
	var b uint64
	for _, v := range vals {
		b |= 1 << uint(v)
	}
	return b
}

func TestParseAccepted(t *testing.T) {
	t.Parallel()
	every := func(lo, hi int) uint64 {
		var b uint64
		for v := lo; v <= hi; v++ {
			b |= 1 << uint(v)
		}
		return b
	}
	tests := []struct {
		expr string
		want Schedule
	}{
		{"* * * * *", Schedule{every(0, 59), uint32(every(0, 23)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(every(0, 6)), true, true, "* * * * *"}},
		{"@yearly", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(bitsOf(1)), uint16(bitsOf(1)), uint8(every(0, 6)), false, true, "0 0 1 1 *"}},
		{"@ANNUALLY", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(bitsOf(1)), uint16(bitsOf(1)), uint8(every(0, 6)), false, true, "0 0 1 1 *"}},
		{"@monthly", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(bitsOf(1)), uint16(every(1, 12)), uint8(every(0, 6)), false, true, "0 0 1 * *"}},
		{"@weekly", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(bitsOf(0)), true, false, "0 0 * * 0"}},
		{"@daily", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(every(0, 6)), true, true, "0 0 * * *"}},
		{"@midnight", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(every(0, 6)), true, true, "0 0 * * *"}},
		{"@hourly", Schedule{bitsOf(0), uint32(every(0, 23)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(every(0, 6)), true, true, "0 * * * *"}},
		{"  */15\t*  * * *  ", Schedule{bitsOf(0, 15, 30, 45), uint32(every(0, 23)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(every(0, 6)), true, true, "*/15 * * * *"}},
		{"1-10/3 * * * *", Schedule{bitsOf(1, 4, 7, 10), uint32(every(0, 23)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(every(0, 6)), true, true, "1-10/3 * * * *"}},
		{"5/10 * * * *", Schedule{bitsOf(5, 15, 25, 35, 45, 55), uint32(every(0, 23)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(every(0, 6)), true, true, "5/10 * * * *"}},
		{"0,30 9-17 * * 1-5", Schedule{bitsOf(0, 30), uint32(every(9, 17)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(every(1, 5)), true, false, "0,30 9-17 * * 1-5"}},
		{"0 9 * JAN-MAR MON-FRI", Schedule{bitsOf(0), uint32(bitsOf(9)), uint32(every(1, 31)), uint16(every(1, 3)), uint8(every(1, 5)), true, false, "0 9 * JAN-MAR MON-FRI"}},
		{"0 0 1 jan,Jul sun", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(bitsOf(1)), uint16(bitsOf(1, 7)), uint8(bitsOf(0)), false, false, "0 0 1 jan,Jul sun"}},
		{"0 0 * * 7", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(bitsOf(0)), true, false, "0 0 * * 7"}},
		{"0 0 * * 5-7", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(bitsOf(0, 5, 6)), true, false, "0 0 * * 5-7"}},
		{"0 0 */2 * 1", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(bitsOf(1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31)), uint16(every(1, 12)), uint8(bitsOf(1)), true, false, "0 0 */2 * 1"}},
		{"0 0 13 * 5", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(bitsOf(13)), uint16(every(1, 12)), uint8(bitsOf(5)), false, false, "0 0 13 * 5"}},
		{"0 0 29 2 *", Schedule{bitsOf(0), uint32(bitsOf(0)), uint32(bitsOf(29)), uint16(bitsOf(2)), uint8(every(0, 6)), false, true, "0 0 29 2 *"}},
		{"59 23 31 12 6", Schedule{bitsOf(59), uint32(bitsOf(23)), uint32(bitsOf(31)), uint16(bitsOf(12)), uint8(bitsOf(6)), false, false, "59 23 31 12 6"}},
		{"*/7 * * * */7", Schedule{bitsOf(0, 7, 14, 21, 28, 35, 42, 49, 56), uint32(every(0, 23)), uint32(every(1, 31)), uint16(every(1, 12)), uint8(bitsOf(0)), true, true, "*/7 * * * */7"}},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			t.Parallel()
			got := mustParse(t, tc.expr)
			if got != tc.want {
				t.Fatalf("Parse(%q)\n got %+v\nwant %+v", tc.expr, got, tc.want)
			}
			if got.IsZero() {
				t.Fatal("parsed schedule reports IsZero")
			}
			again := mustParse(t, got.String())
			if again != got {
				t.Fatalf("Parse(String()) round trip: %+v != %+v", again, got)
			}
		})
	}
}

func TestParseEquivalents(t *testing.T) {
	t.Parallel()
	same := func(a, b Schedule) bool {
		a.text, b.text = "", ""
		return a == b
	}
	pairs := [][2]string{
		{"0 0 * * 7", "0 0 * * 0"},
		{"0 0 * * SUN", "0 0 * * 0"},
		{"0 0 * * sun", "0 0 * * SUN"},
		{"0 0 1 JAN *", "0 0 1 1 *"},
		{"@daily", "0 0 * * *"},
		{"*/15 * * * *", "0,15,30,45 * * * *"},
		{"0-59 * * * *", "* * * * *"},
		{"0 0 * * 1-7", "0 0 * * 0-6"},
	}
	for _, p := range pairs {
		if !same(mustParse(t, p[0]), mustParse(t, p[1])) {
			t.Errorf("%q and %q should parse to the same bitsets", p[0], p[1])
		}
	}
	// Bitsets equal but star flags differ: the day rule must differ too.
	if same(mustParse(t, "0 0 * * *"), mustParse(t, "0 0 1-31 * 0-6")) {
		t.Error("star flags must distinguish '*' from an explicit full range")
	}
}

func TestParseRejected(t *testing.T) {
	t.Parallel()
	exprs := []string{
		"", "   ", "* * * *", "* * * * * *", "@bogus", "@", "@daily extra",
		"60 * * * *", "-1 * * * *", "+5 * * * *", "* 24 * * *", "* * 0 * *", "* * 32 * *",
		"* * * 0 *", "* * * 13 *", "* * * * 8", "*/0 * * * *", "*/60 * * * *", "* */24 * * *",
		"1/0 * * * *", "5-3 * * * *", "a * * * *", "* * * JANUARY *", "* * * * MONDAY",
		"1,,2 * * * *", ",1 * * * *", "1, * * * *", "1- * * * *", "-5 * * * *", "1-5- * * * *",
		"*/ * * * *", "/5 * * * *", "* * * * MON-", "* * * * -FRI", "1.5 * * * *",
		"０ * * * *", "* * * * SAT-SUN", "0 0 * * 7-8", "1-2-3 * * * *", "0000 * * * *",
	}
	for _, expr := range exprs {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()
			s, err := Parse(expr)
			if !errors.Is(err, ErrSyntax) {
				t.Fatalf("Parse(%q) = %+v, err %v; want ErrSyntax", expr, s, err)
			}
			if !s.IsZero() {
				t.Fatalf("failed parse must return the zero Schedule, got %+v", s)
			}
		})
	}
}

func TestZeroSchedule(t *testing.T) {
	t.Parallel()
	var z Schedule
	if !z.IsZero() || z.String() != "" {
		t.Fatal("zero Schedule must be IsZero with empty String")
	}
	if _, err := Parse(z.String()); !errors.Is(err, ErrSyntax) {
		t.Fatalf("Parse of the zero String must fail: %v", err)
	}
}
