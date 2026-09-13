package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"time"
)

//go:embed web/*
var assets embed.FS

func main() {
	addr := flag.String("addr", "127.0.0.1:17893", "loopback listen address (0 chooses a free port)")
	noOpen := flag.Bool("no-open", false, "do not open browser")
	flag.Parse()
	host, _, err := net.SplitHostPort(*addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		log.Fatal("listen address must be a loopback IP, e.g. 127.0.0.1:8080")
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	tokenBytes := make([]byte, 32)
	if _, err = rand.Read(tokenBytes); err != nil {
		log.Fatal(err)
	}
	token := hex.EncodeToString(tokenBytes)
	address := listener.Addr().String()
	app := newApp()
	defer app.close()
	server := &http.Server{Handler: app.handler(address, token), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 45 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	url := "http://" + address + "/#token=" + token
	fmt.Printf("\n  Kaflow · Kafka Client\n  %s\n  按 Ctrl+C 退出。\n\n", url)
	if !*noOpen && runtime.GOOS == "darwin" {
		if err := exec.Command("open", url).Run(); err != nil {
			log.Printf("请手动打开上方地址: %v", err)
		}
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func (a *app) handler(address, token string) http.Handler {
	static, _ := fs.Sub(assets, "web")
	files := http.FileServer(http.FS(static))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		w.Header().Set("Cache-Control", "no-store")
		if r.Host != address {
			http.Error(w, "invalid host", http.StatusForbidden)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			files.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			respond(w, 401, nil, fmt.Errorf("会话失效，请从启动地址重新打开"))
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+address {
			respond(w, 403, nil, fmt.Errorf("invalid origin"))
			return
		}
		if r.Method != "POST" {
			respond(w, 405, nil, fmt.Errorf("POST required"))
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
		defer r.Body.Close()
		var req request
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			respond(w, 400, nil, err)
			return
		}
		if err := dec.Decode(&struct{}{}); err != io.EOF {
			respond(w, 400, nil, fmt.Errorf("expected one JSON object"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
		defer cancel()
		data, err := a.execute(ctx, strings.TrimPrefix(r.URL.Path, "/api/"), req)
		if err != nil {
			respond(w, 400, nil, err)
			return
		}
		respond(w, 200, data, nil)
	})
}
func respond(w http.ResponseWriter, status int, data any, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"data": data})
}
