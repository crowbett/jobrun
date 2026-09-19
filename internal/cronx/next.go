package cronx

import "time"

// MaxLookahead bounds the search: Next returns the zero time when nothing
// matches within this many years after `after` (e.g. "0 0 30 2 *").
const MaxLookahead = 5

// Next returns the first instant strictly after `after` that matches s, in loc
// (nil means UTC), or the zero time if none exists within MaxLookahead years.
//
// The schedule is evaluated on wall-clock time in loc, minute resolution:
//   - DST spring-forward gap: a wall-clock time that does not exist (02:30 on
//     the day clocks jump 02:00 -> 03:00) is skipped; the search continues with
//     the next matching wall-clock time.
//   - DST fall-back repeat: a wall-clock time that occurs twice fires once, on
//     its first occurrence; the repeated hour never fires again.
func (s Schedule) Next(after time.Time, loc *time.Location) time.Time {
	if s.IsZero() {
		return time.Time{}
	}
	if loc == nil {
		loc = time.UTC
	}
	// Start at the first whole minute strictly after `after`, using instant
	// arithmetic (not wall-clock arithmetic) so DST cannot shift the start.
	start := after.In(loc)
	start = start.Add(-time.Duration(start.Second())*time.Second - time.Duration(start.Nanosecond()))
	start = start.Add(time.Minute)
	startKey := key(start.Year(), int(start.Month()), start.Day(), start.Hour(), start.Minute())

	for year := start.Year(); year <= start.Year()+MaxLookahead; year++ {
		for month := 1; month <= 12; month++ {
			if s.month&(1<<uint(month)) == 0 || key(year, month, 31, 23, 59) < startKey {
				continue
			}
			for day, last := 1, daysIn(year, month); day <= last; day++ {
				if key(year, month, day, 23, 59) < startKey || !s.dayMatches(year, month, day) {
					continue
				}
				for hour := 0; hour < 24; hour++ {
					if s.hour&(1<<uint(hour)) == 0 || key(year, month, day, hour, 59) < startKey {
						continue
					}
					for minute := 0; minute < 60; minute++ {
						if s.minute&(1<<uint(minute)) == 0 || key(year, month, day, hour, minute) < startKey {
							continue
						}
						t := time.Date(year, time.Month(month), day, hour, minute, 0, 0, loc)
						// A wall-clock time inside a spring-forward gap normalises
						// to a different wall-clock time: skip it.
						if t.Hour() != hour || t.Minute() != minute || t.Day() != day {
							continue
						}
						// After a fall-back repeat time.Date resolves to the first
						// occurrence, which may be before `after`: that minute has
						// already fired once, so skip it.
						if !t.After(after) {
							continue
						}
						return t
					}
				}
			}
		}
	}
	return time.Time{}
}

// key orders wall-clock minutes for the pruning comparisons.
func key(year, month, day, hour, minute int) int64 {
	return ((((int64(year)*13+int64(month))*32+int64(day))*24+int64(hour))*60 + int64(minute))
}

func (s Schedule) dayMatches(year, month, day int) bool {
	domMatch := s.dom&(1<<uint(day)) != 0
	dowMatch := s.dow&(1<<uint(weekday(year, month, day))) != 0
	if s.domStar || s.dowStar {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}

// weekday is Zeller-style (Sakamoto) day-of-week, Sunday = 0; no time.Date so
// the inner loop stays allocation-free and DST-independent.
func weekday(year, month, day int) int {
	t := [...]int{0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4}
	if month < 3 {
		year--
	}
	return (year + year/4 - year/100 + year/400 + t[month-1] + day) % 7
}

func daysIn(year, month int) int {
	switch month {
	case 2:
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	default:
		return 31
	}
}
