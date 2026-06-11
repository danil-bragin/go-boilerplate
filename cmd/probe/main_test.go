package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProbeURL(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		args      []string
		adminAddr string
		want      string
	}{
		{
			name: "default when no arg and no env",
			want: "http://127.0.0.1:9090/livez",
		},
		{
			name: "explicit URL arg wins over env",
			args: []string{"http://127.0.0.1:8080/healthz"}, adminAddr: ":9999",
			want: "http://127.0.0.1:8080/healthz",
		},
		{
			name:      "ADMIN_HTTP_ADDR port-only form",
			adminAddr: ":9091",
			want:      "http://127.0.0.1:9091/livez",
		},
		{
			name:      "ADMIN_HTTP_ADDR host:port form uses port only",
			adminAddr: "0.0.0.0:9092",
			want:      "http://127.0.0.1:9092/livez",
		},
		{
			name:      "malformed ADMIN_HTTP_ADDR falls back to default",
			adminAddr: "not-an-addr",
			want:      "http://127.0.0.1:9090/livez",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := probeURL(tc.args, tc.adminAddr); got != tc.want {
				t.Fatalf("probeURL(%v, %q) = %q, want %q", tc.args, tc.adminAddr, got, tc.want)
			}
		})
	}
}

func TestRun_OK(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if code := run([]string{srv.URL}, "", time.Second); code != 0 {
		t.Fatalf("run on 200 = %d, want 0", code)
	}
}

func TestRun_ServerError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	if code := run([]string{srv.URL}, "", time.Second); code != 1 {
		t.Fatalf("run on 500 = %d, want 1", code)
	}
}

func TestRun_Timeout(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-block
	}))
	defer srv.Close()
	defer close(block)

	if code := run([]string{srv.URL}, "", 50*time.Millisecond); code != 1 {
		t.Fatalf("run on timeout = %d, want 1", code)
	}
}

func TestRun_BadURL(t *testing.T) {
	t.Parallel()
	if code := run([]string{"http://127.0.0.1:0/nope"}, "", 100*time.Millisecond); code != 1 {
		t.Fatalf("run on unreachable URL = %d, want 1", code)
	}
	if code := run([]string{"::not a url::"}, "", 100*time.Millisecond); code != 1 {
		t.Fatalf("run on malformed URL = %d, want 1", code)
	}
}
