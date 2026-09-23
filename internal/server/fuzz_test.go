package server

import (
	"errors"
	"strings"
	"testing"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
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

// FuzzWorkerLease pins the WorkerService identity inputs: a worker id and a
// lease from the wire never panic the validators; an accepted worker id is
// 1..MaxWorkerIDLen bytes of printable ASCII and becomes "remote:<id>"; an
// accepted lease has a positive token and a job id that round-trips.
func FuzzWorkerLease(f *testing.F) {
	f.Add("worker-a", "019210a0-1234-7abc-8def-0123456789ab", int64(1))
	f.Add("", "", int64(0))
	f.Add("a b", "nope", int64(-1))
	f.Add(strings.Repeat("w", MaxWorkerIDLen+1), "019210a0-1234-7abc-8def-0123456789ab", int64(1<<62))
	f.Add("\x7f\x00", "{019210a0-1234-7abc-8def-0123456789ab}", int64(3))
	f.Fuzz(func(t *testing.T, workerID, jobID string, token int64) {
		owner, err := ownerOf(workerID)
		if err == nil {
			if workerID == "" || len(workerID) > MaxWorkerIDLen || owner != RemoteOwnerPrefix+workerID {
				t.Fatalf("accepted worker id %q -> %q", workerID, owner)
			}
			for i := range len(workerID) {
				if c := workerID[i]; c <= ' ' || c > '~' {
					t.Fatalf("accepted worker id %q with byte %#x", workerID, c)
				}
			}
		}
		l, err := leaseFromProto(&jobrunv1.Lease{JobId: jobID, Token: token})
		if err != nil {
			return
		}
		if l.Token <= 0 || l.Token != token {
			t.Fatalf("accepted token %d -> %d", token, l.Token)
		}
		if again, err := parseID(l.ID.String()); err != nil || again != l.ID {
			t.Fatalf("job id %q -> %s does not round-trip", jobID, l.ID)
		}
	})
}
