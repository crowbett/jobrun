package admin

import (
	"net/http"
	"strings"
)

// allowOrigins wraps h with the admin surface's CORS policy, shared by
// /admin/stats and /admin/events. By default (an empty allowlist) no
// Access-Control-Allow-Origin header is ever sent, so only same-origin pages
// (the embedded /admin page) can read the responses: the admin surface has
// no auth, and a wildcard would let any site open in the operator's browser
// read queue names, job types and error strings from a loopback bind.
//
// A request whose Origin exactly matches (case-insensitively) an allowlisted
// origin gets that origin echoed back; anything else, including "null", gets
// no header. Vary: Origin is always set so a cache never replays one
// origin's answer to another.
func allowOrigins(origins []string, h http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(origins))
	for _, o := range origins {
		if o = normalizeOrigin(o); o != "" && o != "*" && o != "null" {
			allowed[o] = struct{}{}
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		if origin := r.Header.Get("Origin"); origin != "" {
			if _, ok := allowed[normalizeOrigin(origin)]; ok {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			}
		}
		h.ServeHTTP(w, r)
	})
}

func normalizeOrigin(o string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(o), "/"))
}
