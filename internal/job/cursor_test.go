package job

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		c    Cursor
	}{
		{"now", Cursor{CreatedAt: time.Now(), ID: NewID()}},
		{"micro precision (postgres)", Cursor{CreatedAt: time.Date(2026, 9, 19, 12, 0, 0, 123456000, time.UTC), ID: NewID()}},
		{"epoch id only", Cursor{ID: uuid.MustParse("0192f0c8-1234-7000-8000-000000000001")}},
		{"time only", Cursor{CreatedAt: time.Unix(1_700_000_000, 0)}},
		{"pre-1970", Cursor{CreatedAt: time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC), ID: NewID()}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tok := tc.c.Encode()
			if tok == "" {
				t.Fatal("non-zero cursor encoded to empty token")
			}
			got, err := DecodeCursor(tok)
			if err != nil {
				t.Fatalf("decode %q: %v", tok, err)
			}
			if !got.CreatedAt.Equal(tc.c.CreatedAt) || got.ID != tc.c.ID {
				t.Fatalf("round trip = %+v, want %+v", got, tc.c)
			}
			if got.CreatedAt.Location() != time.UTC {
				t.Fatalf("decoded time must be UTC, got %v", got.CreatedAt.Location())
			}
		})
	}
}

func TestCursorZero(t *testing.T) {
	t.Parallel()
	if tok := (Cursor{}).Encode(); tok != "" {
		t.Fatalf("zero cursor encoded to %q", tok)
	}
	c, err := DecodeCursor("")
	if err != nil || !c.IsZero() {
		t.Fatalf("DecodeCursor(\"\") = %+v, %v", c, err)
	}
}

func TestDecodeCursorGarbage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		token string
	}{
		{"not base64", "not a token!"},
		{"short", "AAAA"},
		{"std base64 of zeros", base64.StdEncoding.EncodeToString(make([]byte, cursorLen))},
		{"padded std base64", base64.StdEncoding.EncodeToString(make([]byte, cursorLen+2))},
		{"too long", base64.RawURLEncoding.EncodeToString(make([]byte, cursorLen+1))},
		{"too short", base64.RawURLEncoding.EncodeToString(make([]byte, cursorLen-1))},
		{"all zero bytes", base64.RawURLEncoding.EncodeToString(make([]byte, cursorLen))},
		{"whitespace", "   "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeCursor(tc.token)
			if !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("want ErrInvalidCursor, got %v", err)
			}
		})
	}
}
