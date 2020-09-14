package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempts int = iota
	Retry
)

type Backend struct {
	URL          *url.URL
	alive        bool
	mux          *sync.Mutex
	ReverseProxy *httputil.ReverseProxy
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.alive = alive
}

func (b *Backend) IsAlive() (alive bool) {
	b.mux.Lock()
	defer b.mux.Lock()
	alive = b.alive
	return
}

type ServerPool struct {
	backends []*Backend
	current  uint64
}

func (s *ServerPool) AddBackend(backend *Backend) {
	s.backends = append(s.backends, backend)
}

func (s *ServerPool) MarkBackendStatus(url *url.URL, alive bool) {
	for _, backend := range s.backends {
		if backend.URL.String() == url.String() {
			backend.SetAlive(alive)
			break
		}
	}
}

func (s *ServerPool) NextIndent() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

func (s *ServerPool) GetNextPeer() *Backend {
	next := s.NextIndent()
	l := len(s.backends) + next
	for i := next; i < l; i++ {
		idx := i % len(s.backends)
		if s.backends[idx].IsAlive() {
			atomic.StoreUint64(&s.current, uint64(idx))
			return s.backends[idx]
		}
	}
	return nil
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 0
}

func isBackendAlive(u *url.URL) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	if err != nil {
		log.Println("Site unreachable, error : ", err)
		return false
	}
	_ = conn.Close()
	return true
}

func (s *ServerPool) HealthCheck() {
	for _, backend := range s.backends {
		status := "up"
		alive := isBackendAlive(backend.URL)
		backend.SetAlive(alive)
		if !alive {
			status = "down"
		}
		log.Printf("%s [%s]\n", backend.URL, status)
	}
}

func healthCheck() {
	t := time.NewTicker(time.Second * 20)
	for {
		select {
		case <-t.C:
			log.Println("Starting health check ...")
			serverPool.HealthCheck()
			log.Println("Health check completed!!!")
		}
	}
}

func lb(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) Max Attempts reached, teminating\n", r.RemoteAddr, r.URL)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}

	backend := serverPool.GetNextPeer()
	if backend != nil {
		backend.ReverseProxy.ServeHTTP(w, r)
		return
	}

	http.Error(w, "Service not available", http.StatusServiceUnavailable)
}

var serverPool ServerPool

func main() {
	var servers string
	var port int
	flag.StringVar(&servers, "backends", "", "Load balanced backedns, use comma to separate")
	flag.IntVar(&port, "port", 3030, "port to server")
	flag.Parse()

	if len(servers) == 0 {
		log.Fatal("Please provider one or more backedns to load balance")
	}

	hosts := strings.Split(servers, ",")
	for _, host := range hosts {
		serverUrl, err := url.Parse(host)
		if err != nil {
			log.Fatal(err)
		}

		// build backend
		proxy := httputil.NewSingleHostReverseProxy(serverUrl)
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("[%s] %s\n", serverUrl.Host, err.Error())
			retries := GetRetryFromContext(r)
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(r.Context(), Retry, retries+1)
					proxy.ServeHTTP(w, r.WithContext(ctx))
				}
			}

			// after 3 retries, mark this backend as down
			serverPool.MarkBackendStatus(serverUrl, false)

			attempts := GetAttemptsFromContext(r)
			log.Printf("%s(%s) Attempting retry %d\n", r.RemoteAddr, r.URL.Path, attempts)
			ctx := context.WithValue(r.Context(), Attempts, attempts+1)
			lb(w, r.WithContext(ctx))
		}

		serverPool.AddBackend(&Backend{
			URL:          serverUrl,
			alive:        true,
			mux:          new(sync.Mutex),
			ReverseProxy: nil,
		})
		log.Printf("add server : %s\n", serverUrl)
	}

	httpServer := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(lb),
	}

	go healthCheck()

	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
