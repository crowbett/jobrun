package job

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// FuzzNewJobValidate drives the enqueue validation with arbitrary payloads and
// idempotency keys: it must never panic, every accepted payload must be a JSON
// document Postgres jsonb can store (valid UTF-8, no \u0000, paired
// surrogates: encoding/json decodes it without producing U+FFFD), and every
// accepted key must be printable UTF-8.
func FuzzNewJobValidate(f *testing.F) {
	seeds := []struct {
		payload string
		key     string
	}{
		{"", ""},
		{"{}", "k"},
		{`{"to":"x"}`, "order-1"},
		{`{"a":"` + u("00e9") + `"}`, "é"},
		{`{"a":"` + u("D83D") + u("DE00") + `"}`, "emoji-😀"},
		{`{"a":"\u0000"}`, "a\x00b"},
		{`{"a":"\ud800"}`, "a\nb"},
		{`{"a":"\udc00"}`, "a\x7f"},
		{`{"a":"\ud83d` + u("0041") + `"}`, "a\xffb"},
		{`"\ud83d"`, strings.Repeat("k", MaxIdempotencyKeyLen)},
		{`"\ud83d\`, strings.Repeat("k", MaxIdempotencyKeyLen+1)},
		{`"\u"`, ""},
		{`"\u12"`, ""},
		{`"\\u0000"`, ""},
		{`"\\\u0000"`, ""},
		{`["` + u("d83d") + u("de00") + `","` + u("D83D") + u("DE00") + `"]`, ""},
		{"{\"a\":\"\xff\"}", ""},
		{`{"a":`, ""},
		{"[[[[[[[[]]]]]]]]", ""},
		{`{"a":1e400}`, ""},
		{`{"a":"\/\b\f\n\r\t\""}`, ""},
	}
	for _, s := range seeds {
		f.Add([]byte(s.payload), s.key)
	}
	f.Fuzz(func(t *testing.T, payload []byte, key string) {
		n := NewJob{Queue: "default", Type: "noop", Payload: payload, IdempotencyKey: key}
		err := n.Validate()
		if err != nil {
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Validate must wrap ErrInvalidArgument, got %v", err)
			}
			return
		}
		if len(payload) > MaxPayloadBytes || len(key) > MaxIdempotencyKeyLen {
			t.Fatalf("accepted over the cap: payload %d, key %d", len(payload), len(key))
		}
		if !utf8.ValidString(key) {
			t.Fatalf("accepted a non-UTF-8 key %q", key)
		}
		for i := 0; i < len(key); i++ {
			if key[i] < 0x20 || key[i] == 0x7f {
				t.Fatalf("accepted a control character in key %q", key)
			}
		}
		if len(payload) == 0 {
			return
		}
		if !utf8.Valid(payload) {
			t.Fatalf("accepted a non-UTF-8 payload %q", payload)
		}
		var v any
		dec := json.NewDecoder(bytes.NewReader(payload))
		dec.UseNumber() // jsonb stores 1e400 as numeric; float64 would overflow
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("accepted payload %q that does not decode: %v", payload, err)
		}
		// encoding/json turns every escape it tolerates but jsonb rejects
		// (\u0000 excepted, which it keeps as NUL) into U+FFFD or a NUL rune;
		// an accepted payload must contain neither after decoding.
		checkDecoded(t, payload, v)
	})
}

// u spells a JSON \u escape without writing one in the source (editor
// tooling decodes literal \uXXXX sequences).
func u(hex string) string { return "\\" + "u" + hex }

func checkDecoded(t *testing.T, payload []byte, v any) {
	t.Helper()
	switch x := v.(type) {
	case string:
		if strings.ContainsRune(x, 0) {
			t.Fatalf("accepted payload %q decodes to a NUL", payload)
		}
		if strings.ContainsRune(x, utf8.RuneError) && !strings.Contains(string(payload), string(utf8.RuneError)) &&
			!strings.Contains(strings.ToLower(string(payload)), u("fffd")) {
			t.Fatalf("accepted payload %q decodes to U+FFFD: an unpaired surrogate slipped through", payload)
		}
	case []any:
		for _, e := range x {
			checkDecoded(t, payload, e)
		}
	case map[string]any:
		for k, e := range x {
			checkDecoded(t, payload, k)
			checkDecoded(t, payload, e)
		}
	}
}

// FuzzValidateName pins the name grammar: accepted names are exactly
// 1..MaxNameLen bytes of [a-z0-9._-]; nothing panics.
func FuzzValidateName(f *testing.F) {
	for _, s := range []string{"", "default", "a.b-c_d9", "Default", "a b", "queué", strings.Repeat("a", MaxNameLen), strings.Repeat("a", MaxNameLen+1), "\x00", "a/b"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		err := ValidateName("queue", name)
		want := len(name) >= 1 && len(name) <= MaxNameLen
		for i := 0; i < len(name) && want; i++ {
			c := name[i]
			want = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'
		}
		if want && err != nil {
			t.Fatalf("ValidateName(%q) rejected a valid name: %v", name, err)
		}
		if !want && !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("ValidateName(%q) accepted an invalid name (err %v)", name, err)
		}
	})
}

// FuzzDecodeCursor pins the page token codec: never panics, rejects anything
// Encode did not produce, and round-trips what it accepts.
func FuzzDecodeCursor(f *testing.F) {
	c := Cursor{CreatedAt: time.Date(2026, 9, 19, 12, 0, 0, 123456789, time.UTC), ID: NewID()}
	for _, s := range []string{"", c.Encode(), "AAAA", "!!!!", strings.Repeat("A", 32), strings.Repeat("A", 33), "====", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, token string) {
		c, err := DecodeCursor(token)
		if err != nil {
			if !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("DecodeCursor(%q) error must wrap ErrInvalidCursor: %v", token, err)
			}
			if c != (Cursor{}) {
				t.Fatalf("DecodeCursor(%q) failed but returned %+v", token, c)
			}
			return
		}
		if token == "" {
			if !c.IsZero() {
				t.Fatalf("empty token must be the zero cursor, got %+v", c)
			}
			return
		}
		if c.IsZero() {
			t.Fatalf("DecodeCursor(%q) accepted the zero cursor", token)
		}
		again, err := DecodeCursor(c.Encode())
		if err != nil || again != c {
			t.Fatalf("round trip: %+v -> %q -> %+v (%v)", c, c.Encode(), again, err)
		}
	})
}

// FuzzSanitizeError pins the last_error sanitiser: output is at most
// MaxErrorLen bytes, valid UTF-8, NUL-free, and a prefix of the input when
// the input was already clean.
func FuzzSanitizeError(f *testing.F) {
	for _, s := range []string{"", "boom", "a\x00b", "a\xffb", strings.Repeat("é", MaxErrorLen), strings.Repeat("a", MaxErrorLen-1) + "😀"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		out := SanitizeError(in)
		if len(out) > MaxErrorLen {
			t.Fatalf("len %d > MaxErrorLen", len(out))
		}
		if !utf8.ValidString(out) {
			t.Fatalf("SanitizeError(%q) = %q is not valid UTF-8", in, out)
		}
		if strings.IndexByte(out, 0) >= 0 {
			t.Fatalf("SanitizeError(%q) = %q keeps a NUL", in, out)
		}
		if utf8.ValidString(in) && strings.IndexByte(in, 0) < 0 && !strings.HasPrefix(in, out) {
			t.Fatalf("SanitizeError(%q) = %q is not a prefix of clean input", in, out)
		}
	})
}

// FuzzScheduleValidate exercises the cron/timezone/template validation with
// arbitrary strings; it must never panic and every error wraps ErrInvalidArgument.
func FuzzScheduleValidate(f *testing.F) {
	for _, s := range [][2]string{
		{"0 2 * * *", "America/New_York"},
		{"@daily", ""},
		{"", "UTC"},
		{"* * * * *", "Mars/Olympus"},
		{strings.Repeat("*", MaxCronLen+1), ""},
		{"0 0 * * *", strings.Repeat("A", MaxTimezoneLen+1)},
		{"0 0 * * *", "../../etc/passwd"},
		{"0 0 * * *", "/etc/localtime"},
		{"0 0 * * *", "\x00"},
	} {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, cron, tz string) {
		s := Schedule{Name: "s", Cron: cron, Timezone: tz, Template: NewJob{Queue: "q", Type: "t"}}
		if err := s.Validate(); err != nil && !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("Schedule.Validate error must wrap ErrInvalidArgument: %v", err)
		}
	})
}
