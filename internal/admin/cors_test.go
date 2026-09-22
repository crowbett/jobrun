package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestAdminCORS pins the origin policy on both cross-origin-readable routes:
// no Access-Control-Allow-Origin unless the request's Origin is allowlisted,
// and never a wildcard.
func TestAdminCORS(t *testing.T) {
	t.Parallel()
	allow := []string{"http://localhost:3000", "HTTPS://Ops.Example.com/", "*", "null"}
	tests := []struct {
		name    string
		allowed []string
		origin  string // "" sends no Origin header
		want    string // expected Access-Control-Allow-Origin; "" means absent
	}{
		{name: "absent origin, default policy", allowed: nil, origin: "", want: ""},
		{name: "cross origin, default policy", allowed: nil, origin: "https://evil.example", want: ""},
		{name: "absent origin, allowlist set", allowed: allow, origin: "", want: ""},
		{name: "allowed origin echoed", allowed: allow, origin: "http://localhost:3000", want: "http://localhost:3000"},
		{name: "allowed origin, case and slash insensitive", allowed: allow, origin: "https://ops.example.com", want: "https://ops.example.com"},
		{name: "disallowed origin", allowed: allow, origin: "https://evil.example", want: ""},
		{name: "different port disallowed", allowed: allow, origin: "http://localhost:3001", want: ""},
		{name: "different scheme disallowed", allowed: allow, origin: "https://localhost:3000", want: ""},
		{name: "null origin never allowed", allowed: allow, origin: "null", want: ""},
		{name: "wildcard entry grants nothing", allowed: allow, origin: "https://anything.example", want: ""},
	}
	for _, path := range []string{"/admin/stats", "/admin/events"} {
		for _, tt := range tests {
			t.Run(path+"/"+tt.name, func(t *testing.T) {
				t.Parallel()
				d := testDeps(t)
				d.AllowedOrigins = tt.allowed
				srv := httptest.NewServer(Handler(d))
				t.Cleanup(srv.Close)

				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+path, http.NoBody)
				if err != nil {
					t.Fatal(err)
				}
				if tt.origin != "" {
					req.Header.Set("Origin", tt.origin)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("GET %s: %v", path, err)
				}
				defer resp.Body.Close() // the SSE stream ends with the server

				if resp.StatusCode != http.StatusOK {
					t.Fatalf("status = %d", resp.StatusCode)
				}
				got, present := resp.Header["Access-Control-Allow-Origin"]
				switch {
				case tt.want == "" && present:
					t.Errorf("Access-Control-Allow-Origin = %q, want absent", got)
				case tt.want != "" && resp.Header.Get("Access-Control-Allow-Origin") != tt.want:
					t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, tt.want)
				}
				if v := resp.Header.Values("Vary"); len(v) == 0 || v[0] != "Origin" {
					t.Errorf("Vary = %q, want Origin", v)
				}
			})
		}
	}
}
