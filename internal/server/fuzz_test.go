package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/crowbett/jobrun/internal/job"
)

// FuzzDecodeNameToken pins the ListSchedules page token: never panics,
// anything that is not base64url of a valid schedule name wraps
// job.ErrInvalidCursor, and every accepted token round-trips.
func FuzzDecodeNameToken(f *testing.F) {
	for _, s := range []string{
		"", encodeNameToken("nightly"), encodeNameToken("a.b-c_d9"), encodeNameToken("Nightly"),
		encodeNameToken(strings.Repeat("a", job.MaxNameLen)), encodeNameToken(strings.Repeat("a", job.MaxNameLen+1)),
		"!!!!", "====", "bmlnaHRseQ==", "bmlnaHRseQ", strings.Repeat("A", 1000),
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, token string) {
		name, err := decodeNameToken(token)
		if err != nil {
			if !errors.Is(err, job.ErrInvalidCursor) {
				t.Fatalf("decodeNameToken(%q) error must wrap ErrInvalidCursor: %v", token, err)
			}
			if name != "" {
				t.Fatalf("decodeNameToken(%q) failed but returned %q", token, name)
			}
			return
		}
		if token == "" {
			if name != "" {
				t.Fatalf("empty token must decode to \"\", got %q", name)
			}
			return
		}
		if err := job.ValidateName("page_token", name); err != nil {
			t.Fatalf("decodeNameToken(%q) accepted an invalid name %q: %v", token, name, err)
		}
		if again, err := decodeNameToken(encodeNameToken(name)); err != nil || again != name {
			t.Fatalf("round trip %q -> %q -> %q (%v)", token, name, again, err)
		}
	})
}
