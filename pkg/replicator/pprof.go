package replicator

import (
	"fmt"
	"net/http"
	"net/http/pprof"
)

func (r *Replicator) startPprof() {
	defer r.logger.Sync()
	defer r.wg.Done()
	mux := http.NewServeMux()
	r.logger.Debugf("starting pprof server")

	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	r.pprofHttp = &http.Server{
		Addr:    fmt.Sprintf(":%d", r.cfg.PprofPort),
		Handler: mux,
	}

	err := r.pprofHttp.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		r.logger.Warnf("could not start pprof server: %v", err)
	}
}

func (r *Replicator) stopPprof() error {
	defer r.logger.Sync()

	r.logger.Debugf("stopping pprof server")
	return r.pprofHttp.Close()
}
