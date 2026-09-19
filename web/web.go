// Package web embeds the static admin page that subscribes to the
// /admin/events feed.
//
// Serve it with http.ServeFileFS(w, r, web.Admin, "admin.html"); internal/admin
// mounts it at GET /admin.
package web

import "embed"

// Admin holds admin.html, a dependency-free page that renders the queue depth
// snapshot, a live transition log and per-status counters from the SSE feed.
//
//go:embed admin.html
var Admin embed.FS
