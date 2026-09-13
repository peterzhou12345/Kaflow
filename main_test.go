package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPBoundary(t *testing.T) {
	a := newApp()
	defer a.close()
	h := a.handler("127.0.0.1:17893", "test-token")
	cases := []struct {
		name, method, path, body, token, host, origin string
		status                                        int
	}{
		{"unauthorized", "POST", "/api/metadata", "{}", "", "127.0.0.1:17893", "", 401},
		{"host rebinding", "POST", "/api/metadata", "{}", "test-token", "evil.test", "", 403},
		{"origin", "POST", "/api/metadata", "{}", "test-token", "127.0.0.1:17893", "https://evil.test", 403},
		{"wrong method", "GET", "/api/metadata", "{}", "test-token", "127.0.0.1:17893", "", 405},
		{"invalid session", "POST", "/api/metadata", "{}", "test-token", "127.0.0.1:17893", "", 400},
		{"unknown field", "POST", "/api/connect", `{"surprise":true}`, "test-token", "127.0.0.1:17893", "", 400},
		{"trailing JSON", "POST", "/api/connect", "{} {}", "test-token", "127.0.0.1:17893", "", 400},
		{"body limit", "POST", "/api/connect", `{"topic":"` + strings.Repeat("a", 2<<20) + `"}`, "test-token", "127.0.0.1:17893", "", 400},
		{"static", "GET", "/", "", "", "127.0.0.1:17893", "", 200},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
			r.Host = c.host
			r.Header.Set("Authorization", "Bearer "+c.token)
			if c.origin != "" {
				r.Header.Set("Origin", c.origin)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != c.status {
				t.Fatalf("got %d: %s", w.Code, w.Body.String())
			}
			if w.Header().Get("Content-Security-Policy") == "" {
				t.Fatal("missing CSP")
			}
			if c.path == "/" && !strings.Contains(w.Body.String(), "Kaflow") {
				t.Fatal("missing embedded UI")
			}
		})
	}
}
