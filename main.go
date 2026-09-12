package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BackendMetrics struct {
	Instance        string  `json:"instance"`
	CPU             float64 `json:"cpu"`
	Memory          float64 `json:"memory"`
	ActiveRequests  int64   `json:"activeRequests"`
	EWMALatencyMs   float64 `json:"ewmaLatencyMs"`
	Load1           float64 `json:"load1"`
	Uptime          int64   `json:"uptime"`
	Healthy         bool    `json:"healthy"`
	Score           float64 `json:"score"`
	LastCheckedUnix int64   `json:"lastCheckedUnix"`
}

type Backend struct {
	URL *url.URL

	Healthy atomic.Bool

	activeRequests atomic.Int64

	latencyMu sync.RWMutex
	ewmaMs    float64

	metricsMu sync.RWMutex
	metrics   BackendMetrics
}

type LBMetrics struct {
	totalRequests atomic.Uint64

	errorRequests atomic.Uint64

	latencyMu sync.Mutex
	latencies []time.Duration
}

type LoadBalancer struct {
	backends []*Backend

	threshold float64

	metrics LBMetrics

	client *http.Client
}

func (lb *LoadBalancer) backendScore(b *Backend) float64 {
	b.metricsMu.RLock()
	m := b.metrics
	b.metricsMu.RUnlock()

	/*
		Score combines:
		- CPU utilization
		- memory utilization
		- active requests
		- observed latency

		Lower score = better backend.
	*/

	cpuScore := m.CPU
	memoryScore := m.Memory

	activeScore := float64(m.ActiveRequests) * 10

	latencyScore := m.EWMALatencyMs / 10

	score := 0.45*cpuScore +
		0.25*memoryScore +
		0.20*activeScore +
		0.10*latencyScore

	return score
}

func (lb *LoadBalancer) chooseBackend() *Backend {
	var candidates []*Backend

	for _, b := range lb.backends {
		if !b.Healthy.Load() {
			continue
		}

		b.metricsMu.RLock()
		m := b.metrics
		b.metricsMu.RUnlock()

		/*
			Use CPU as the primary threshold because it directly
			represents system pressure.

			Backends above the configured threshold are avoided
			when another healthy backend is available.
		*/
		if m.CPU < lb.threshold {
			candidates = append(candidates, b)
		}
	}

	// If every healthy backend is above threshold, still use
	// the least-loaded healthy backend rather than failing requests.
	if len(candidates) == 0 {
		for _, b := range lb.backends {
			if b.Healthy.Load() {
				candidates = append(candidates, b)
			}
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return lb.backendScore(candidates[i]) < lb.backendScore(candidates[j])
	})

	return candidates[0]
}

func (lb *LoadBalancer) recordLatency(d time.Duration) {
	lb.metrics.latencyMu.Lock()
	defer lb.metrics.latencyMu.Unlock()

	lb.metrics.latencies = append(lb.metrics.latencies, d)

	// Keep the LB metrics bounded.
	if len(lb.metrics.latencies) > 10000 {
		lb.metrics.latencies = lb.metrics.latencies[len(lb.metrics.latencies)-10000:]
	}
}

func (lb *LoadBalancer) updateEWMA(b *Backend, latency time.Duration) {
	ms := float64(latency.Microseconds()) / 1000.0

	b.latencyMu.Lock()

	if b.ewmaMs == 0 {
		b.ewmaMs = ms
	} else {
		// Alpha = 0.2
		b.ewmaMs = 0.2*ms + 0.8*b.ewmaMs
	}

	b.latencyMu.Unlock()
}

func (lb *LoadBalancer) healthChecker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		lb.checkAllBackends()

		<-ticker.C
	}
}

func (lb *LoadBalancer) checkAllBackends() {
	for _, b := range lb.backends {
		go lb.checkBackend(b)
	}
}

func (lb *LoadBalancer) checkBackend(b *Backend) {
	start := time.Now()

	resp, err := lb.client.Get(b.URL.String() + "/metrics")

	healthy := false

	if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
		healthy = true

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr == nil {
			var m BackendMetrics

			if json.Unmarshal(body, &m) == nil {
				b.latencyMu.RLock()
				m.EWMALatencyMs = b.ewmaMs
				b.latencyMu.RUnlock()

				m.ActiveRequests = b.activeRequests.Load()
				m.Healthy = true
				m.Score = lb.backendScoreFromMetrics(m)
				m.LastCheckedUnix = time.Now().Unix()

				b.metricsMu.Lock()
				b.metrics = m
				b.metricsMu.Unlock()
			}
		}
	} else if resp != nil {
		resp.Body.Close()
	}

	b.Healthy.Store(healthy)

	if !healthy {
		b.metricsMu.Lock()
		b.metrics.Healthy = false
		b.metrics.LastCheckedUnix = time.Now().Unix()
		b.metricsMu.Unlock()

		log.Printf("Backend %s unhealthy", b.URL)
	}

	_ = start
}

func (lb *LoadBalancer) backendScoreFromMetrics(m BackendMetrics) float64 {
	return 0.45*m.CPU +
		0.25*m.Memory +
		0.20*float64(m.ActiveRequests)*10 +
		0.10*(m.EWMALatencyMs/10)
}

func percentile95(values []time.Duration) float64 {
	if len(values) == 0 {
		return 0
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})

	index := int(math.Ceil(float64(len(values))*0.95)) - 1

	if index < 0 {
		index = 0
	}

	if index >= len(values) {
		index = len(values) - 1
	}

	return float64(values[index].Microseconds()) / 1000.0
}

func main() {
	port := flag.Int(
		"port",
		3257,
		"Load balancer listening port",
	)

	rawBackends := flag.String(
		"backends",
		"https://172.17.0.59:3000,https://172.17.0.60:3000,https://172.17.0.61:3000",
		"Comma-separated backend URLs",
	)

	threshold := flag.Float64(
		"threshold",
		70,
		"CPU utilization threshold for preferred backends",
	)

	tlsEnabled := flag.Bool(
		"tls",
		true,
		"Enable HTTPS",
	)

	certFile := flag.String(
		"cert",
		"cert.pem",
		"TLS certificate",
	)

	keyFile := flag.String(
		"key",
		"key.pem",
		"TLS private key",
	)

	flag.Parse()

	lb := &LoadBalancer{
		threshold: *threshold,
		client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		},
	}

	for _, raw := range strings.Split(*rawBackends, ",") {
		raw = strings.TrimSpace(raw)

		u, err := url.Parse(raw)
		if err != nil {
			log.Fatalf("Invalid backend URL %q: %v", raw, err)
		}

		b := &Backend{
			URL: u,
		}

		b.Healthy.Store(false)

		lb.backends = append(lb.backends, b)
	}

	// Initial health check.
	lb.checkAllBackends()

	go lb.healthChecker()

	proxyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lb.metrics.totalRequests.Add(1)

		backend := lb.chooseBackend()

		if backend == nil {
			lb.metrics.errorRequests.Add(1)
			http.Error(
				w,
				"Service Unavailable: no healthy backends",
				http.StatusServiceUnavailable,
			)
			return
		}

		backend.activeRequests.Add(1)
		defer backend.activeRequests.Add(-1)

		start := time.Now()

		proxy := httputil.NewSingleHostReverseProxy(backend.URL)

		proxy.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}

		proxy.FlushInterval = -1

		originalDirector := proxy.Director

		proxy.Director = func(req *http.Request) {
			originalDirector(req)

			req.Header.Set(
				"X-Forwarded-Proto",
				"https",
			)

			req.Header.Set(
				"X-Forwarded-Host",
				r.Host,
			)

			req.Header.Set(
				"X-Load-Balancer",
				"dynamic-performance",
			)

			req.Header.Set(
				"X-Backend-Instance",
				backend.metrics.Instance,
			)
		}

		proxy.ErrorHandler = func(
			rw http.ResponseWriter,
			req *http.Request,
			err error,
		) {
			lb.metrics.errorRequests.Add(1)
			backend.Healthy.Store(false)

			log.Printf(
				"Proxy error for %s: %v",
				backend.URL,
				err,
			)

			http.Error(
				rw,
				"Backend unavailable",
				http.StatusBadGateway,
			)
		}

		proxy.ServeHTTP(w, r)

		latency := time.Since(start)

		lb.recordLatency(latency)
		lb.updateEWMA(backend, latency)
	})

	http.Handle("/", proxyHandler)

	http.HandleFunc("/lb/metrics", func(w http.ResponseWriter, r *http.Request) {
		lb.metrics.latencyMu.Lock()
		lats := append(
			[]time.Duration(nil),
			lb.metrics.latencies...,
		)
		lb.metrics.latencyMu.Unlock()

		p95 := percentile95(lats)

		backends := make([]map[string]interface{}, 0, len(lb.backends))

		for _, b := range lb.backends {
			b.metricsMu.RLock()
			m := b.metrics
			b.metricsMu.RUnlock()

			m.ActiveRequests = b.activeRequests.Load()
			m.Healthy = b.Healthy.Load()
			m.Score = lb.backendScore(b)

			backends = append(backends, map[string]interface{}{
				"url":             b.URL.String(),
				"instance":        m.Instance,
				"healthy":         m.Healthy,
				"cpu":             m.CPU,
				"memory":          m.Memory,
				"activeRequests":  m.ActiveRequests,
				"ewmaLatencyMs":   m.EWMALatencyMs,
				"load1":           m.Load1,
				"score":           m.Score,
				"lastCheckedUnix": m.LastCheckedUnix,
			})
		}

		response := map[string]interface{}{
			"loadBalancer": map[string]interface{}{
				"port":          *port,
				"threshold":     *threshold,
				"totalRequests": lb.metrics.totalRequests.Load(),
				"errorRequests": lb.metrics.errorRequests.Load(),
				"p95Ms":         p95,
				"backendCount":  len(lb.backends),
				"algorithm":     "dynamic-performance-score",
				"timestamp":     time.Now().UTC(),
			},
			"backends": backends,
		}

		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(response)
	})

	http.HandleFunc("/lb/health", func(w http.ResponseWriter, r *http.Request) {
		healthy := 0

		for _, b := range lb.backends {
			if b.Healthy.Load() {
				healthy++
			}
		}

		status := http.StatusOK

		if healthy == 0 {
			status = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          map[bool]string{true: "ok", false: "unavailable"}[healthy > 0],
			"healthyBackends": healthy,
			"totalBackends":   len(lb.backends),
			"threshold":       *threshold,
		})
	})

	addr := fmt.Sprintf("0.0.0.0:%d", *port)

	if *tlsEnabled {
		if _, err := os.Stat(*certFile); err != nil {
			log.Fatalf("TLS certificate not found: %s", *certFile)
		}

		if _, err := os.Stat(*keyFile); err != nil {
			log.Fatalf("TLS key not found: %s", *keyFile)
		}

		log.Printf(
			"Dynamic LB running on HTTPS %s (threshold=%.2f)",
			addr,
			*threshold,
		)

		log.Fatal(
			http.ListenAndServeTLS(
				addr,
				*certFile,
				*keyFile,
				nil,
			),
		)
	}

	log.Printf(
		"Dynamic LB running on HTTP %s (threshold=%.2f)",
		addr,
		*threshold,
	)

	log.Fatal(
		http.ListenAndServe(
			addr,
			nil,
		),
	)

	_ = strconv.Itoa
}
