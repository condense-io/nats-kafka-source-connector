// Package health exposes liveness/readiness endpoints for Kubernetes
// probes. Unlike a request/response service, a pull consumer's readiness
// doesn't gate any inbound traffic — it mainly protects rollout safety
// (don't call a new pod "up" mid-rollout if it can't yet reach NATS/Kafka)
// and feeds anything inspecting pod health.
package health

import (
	"context"
	"log"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

// Checker reports whether a dependency is currently usable.
type Checker func(ctx context.Context) error

type Server struct {
	httpServer *http.Server
}

// NewServer wires /healthz (always 200 once the process is up) and /readyz
// (200 only if every check succeeds) onto addr. When pprofEnabled is set it
// also exposes the Go profiling endpoints under /debug/pprof/ — see
// registerPprof for why that's opt-in.
func NewServer(addr string, pprofEnabled bool, checks ...Checker) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		for _, check := range checks {
			if err := check(ctx); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(err.Error()))
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})

	if pprofEnabled {
		registerPprof(mux)
	}

	return &Server{httpServer: &http.Server{Addr: addr, Handler: mux}}
}

// registerPprof exposes net/http/pprof on mux and turns on the block and
// mutex profilers.
//
// Opt-in rather than always on for two reasons. It's an attack surface: this
// server is fronted by a Service reachable from anywhere in the cluster, and
// the profiles expose process memory (message payloads included) while
// /debug/pprof/profile pins a goroutine for its whole duration. And the block
// and mutex profilers cost throughput on every contended lock and blocking
// operation — acceptable while investigating, not something to leave running.
//
// The rates below sample rather than record everything: one blocking event per
// ~10µs spent blocked, and 1 in 5 mutex contention events. Both are cheap
// enough to run under load while still resolving the two things worth ruling
// out here — time parked waiting on PubAcks, and contention between the
// per-partition publish goroutines.
func registerPprof(mux *http.ServeMux) {
	runtime.SetBlockProfileRate(10000)
	runtime.SetMutexProfileFraction(5)

	// pprof.Index serves the profile named in the path (heap, goroutine,
	// allocs, block, mutex, ...); the four below need their own routes.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	log.Println("health: pprof enabled at /debug/pprof/ (block and mutex profiling on)")
}

// Start runs the server until ctx is cancelled, then shuts it down.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("health: shutdown error: %v", err)
		}
		return nil
	}
}
