package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminIsEmbeddedAndServable(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin", http.NoBody)
	http.ServeFileFS(rr, req, Admin, "admin.html")

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", ct)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"new EventSource('/admin/events')", "addEventListener('snapshot'", "addEventListener('job'",
		"<title>jobrun</title>", "MAX_ROWS = 200",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("admin.html lacks %q", want)
		}
	}
	if strings.Contains(body, "<script src=") || strings.Contains(body, "<link ") {
		t.Error("admin.html pulls an external dependency")
	}
	if n := strings.Count(body, "\n"); n > 220 {
		t.Errorf("admin.html is %d lines, want <= 220", n)
	}
}
