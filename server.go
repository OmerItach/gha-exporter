package main

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RunServer(cfg *CLI) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.Handle("GET /", http.RedirectHandler("/metrics", http.StatusPermanentRedirect))
	s := http.Server{
		Addr:    ":" + strconv.Itoa(cfg.Port),
		Handler: mux,
	}
	return s.ListenAndServe()
}
