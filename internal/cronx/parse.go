// Package cronx parses standard 5-field cron expressions and computes the next
// firing time in a named time zone. It has no dependencies and is the only cron
// code in jobrun (docs/PLAN.md, Risks: in-house over robfig/cron).
//
// Grammar (Vixie cron): five whitespace-separated fields
//
//	minute (0-59)  hour (0-23)  day-of-month (1-31)  month (1-12 or JAN-DEC)  day-of-week (0-7 or SUN-SAT; 7 = SUN)
//
// each a comma list of: "*", a value, a range "a-b", or any of those with a
// step "/n" (a value with a step means "a to max by n"). Names are
// case-insensitive three-letter abbreviations. The macros @yearly/@annually,
// @monthly, @weekly, @daily/@midnight and @hourly expand to their 5-field form.
//
// Day matching follows Vixie cron: when both day-of-month and day-of-week are
// restricted (neither starts with "*") a day matches if EITHER does; otherwise
// both must match (a "*" field matches every day, so this is the intuitive rule).
package cronx

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Schedule is a parsed cron expression. The zero value matches nothing; Parse
// is the only constructor. It is comparable and safe to copy.
type Schedule struct {
	minute uint64 // bit i set = minute i (0..59)
	hour   uint32 // 0..23
	dom    uint32 // 1..31
	month  uint16 // 1..12
	dow    uint8  // 0..6, Sunday = 0
	// domStar/dowStar record whether the field began with "*" (the Vixie rule
	// that decides OR vs AND day matching).
	domStar bool
	dowStar bool
	// text is the normalised 5-field expression; Parse(String()) == s.
	text string
}

// ErrSyntax wraps every parse failure.
var ErrSyntax = errors.New("cronx: invalid expression")

var macros = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

var (
	monthNames = map[string]int{
		"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
		"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
	}
	dowNames = map[string]int{"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}
)

type fieldSpec struct {
	name     string
	min, max int // accepted numeric range
	starMax  int // upper bound for "*"; differs from max only for dow (7 folds to 0)
	names    map[string]int
}

var (
	minuteSpec = fieldSpec{name: "minute", min: 0, max: 59, starMax: 59}
	hourSpec   = fieldSpec{name: "hour", min: 0, max: 23, starMax: 23}
	domSpec    = fieldSpec{name: "day-of-month", min: 1, max: 31, starMax: 31}
	monthSpec  = fieldSpec{name: "month", min: 1, max: 12, starMax: 12, names: monthNames}
	dowSpec    = fieldSpec{name: "day-of-week", min: 0, max: 7, starMax: 6, names: dowNames}
)

// Parse parses a 5-field cron expression or a @macro. Errors wrap ErrSyntax.
func Parse(expr string) (Schedule, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return Schedule{}, fmt.Errorf("%w: empty", ErrSyntax)
	}
	if expr[0] == '@' {
		expanded, ok := macros[strings.ToLower(expr)]
		if !ok {
			return Schedule{}, fmt.Errorf("%w: unknown macro %q", ErrSyntax, expr)
		}
		expr = expanded
	}
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return Schedule{}, fmt.Errorf("%w: want 5 fields, got %d", ErrSyntax, len(fields))
	}

	var s Schedule
	minute, _, err := parseField(fields[0], minuteSpec)
	if err != nil {
		return Schedule{}, err
	}
	hour, _, err := parseField(fields[1], hourSpec)
	if err != nil {
		return Schedule{}, err
	}
	dom, domStar, err := parseField(fields[2], domSpec)
	if err != nil {
		return Schedule{}, err
	}
	month, _, err := parseField(fields[3], monthSpec)
	if err != nil {
		return Schedule{}, err
	}
	dow, dowStar, err := parseField(fields[4], dowSpec)
	if err != nil {
		return Schedule{}, err
	}
	s.minute = minute
	s.hour = uint32(hour)   //nolint:gosec // bits 0..23
	s.dom = uint32(dom)     //nolint:gosec // bits 1..31
	s.month = uint16(month) //nolint:gosec // bits 1..12
	s.dow = uint8(dow)      //nolint:gosec // bits 0..6
	s.domStar, s.dowStar = domStar, dowStar
	s.text = strings.Join(fields, " ")
	return s, nil
}

// String returns the normalised 5-field expression ("" for the zero Schedule).
// Parse(s.String()) yields a Schedule equal to s.
func (s Schedule) String() string { return s.text }

// IsZero reports whether s came from the zero value rather than Parse.
func (s Schedule) IsZero() bool { return s.text == "" }

// parseField parses one comma-separated field into a bitset and reports
// whether any part began with "*".
func parseField(field string, spec fieldSpec) (bits uint64, star bool, err error) {
	for _, part := range strings.Split(field, ",") {
		b, st, err := parsePart(part, spec)
		if err != nil {
			return 0, false, err
		}
		bits |= b
		star = star || st
	}
	return bits, star, nil
}

func parsePart(part string, spec fieldSpec) (bits uint64, star bool, err error) {
	if part == "" {
		return 0, false, fmt.Errorf("%w: %s: empty list element", ErrSyntax, spec.name)
	}
	rangeText, stepText, hasStep := strings.Cut(part, "/")
	step := 1
	if hasStep {
		step, err = parseDigits(stepText)
		if err != nil || step < 1 || step > spec.max {
			return 0, false, fmt.Errorf("%w: %s: bad step %q", ErrSyntax, spec.name, stepText)
		}
	}

	var lo, hi int
	if rangeText == "*" {
		lo, hi, star = spec.min, spec.starMax, true
	} else {
		loText, hiText, isRange := strings.Cut(rangeText, "-")
		if lo, err = parseValue(loText, spec); err != nil {
			return 0, false, err
		}
		switch {
		case isRange:
			if hi, err = parseValue(hiText, spec); err != nil {
				return 0, false, err
			}
			if hi < lo {
				return 0, false, fmt.Errorf("%w: %s: range %q is descending", ErrSyntax, spec.name, rangeText)
			}
		case hasStep:
			hi = spec.max // "a/n" means a..max by n
		default:
			hi = lo
		}
	}
	for v := lo; v <= hi; v += step {
		bits |= 1 << uint(fold(v, spec))
	}
	return bits, star, nil
}

// fold maps day-of-week 7 onto 0; every other value is itself.
func fold(v int, spec fieldSpec) int {
	if spec.name == dowSpec.name && v == 7 {
		return 0
	}
	return v
}

func parseValue(tok string, spec fieldSpec) (int, error) {
	if tok == "" {
		return 0, fmt.Errorf("%w: %s: missing value", ErrSyntax, spec.name)
	}
	if spec.names != nil && isAlpha(tok) {
		v, ok := spec.names[strings.ToUpper(tok)]
		if !ok {
			return 0, fmt.Errorf("%w: %s: unknown name %q", ErrSyntax, spec.name, tok)
		}
		return v, nil
	}
	v, err := parseDigits(tok)
	if err != nil {
		return 0, fmt.Errorf("%w: %s: bad value %q", ErrSyntax, spec.name, tok)
	}
	if v < spec.min || v > spec.max {
		return 0, fmt.Errorf("%w: %s: %d out of range %d-%d", ErrSyntax, spec.name, v, spec.min, spec.max)
	}
	return v, nil
}

// parseDigits accepts unsigned decimal digits only (no sign, no whitespace).
func parseDigits(tok string) (int, error) {
	if tok == "" || len(tok) > 3 {
		return 0, ErrSyntax
	}
	for i := 0; i < len(tok); i++ {
		if tok[i] < '0' || tok[i] > '9' {
			return 0, ErrSyntax
		}
	}
	return strconv.Atoi(tok)
}

func isAlpha(tok string) bool {
	for i := 0; i < len(tok); i++ {
		c := tok[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return tok != ""
}
