package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/TarnishedGhost/LabGo4/httptools"
	"github.com/TarnishedGhost/LabGo4/signal"
)

var (
	serverPort  = flag.Int("port", 8090, "load balancer port")
	reqTimeout  = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	useHTTPS    = flag.Bool("https", false, "whether backends support HTTPs")
	enableTrace = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	requestTimeout = time.Duration(*reqTimeout) * time.Second
	backendServers = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	healthyServers    = make([]string, len(backendServers))
	serverLoadTracker = make(map[string]int64)
	mutex             sync.Mutex
	healthCheckImpl   HealthChecker
	requestHandler    RequestSender
)

type HealthChecker interface {
	Validate(string) bool
}

type BasicHealthChecker struct{}

func (hc *BasicHealthChecker) Validate(server string) bool {
	ctx, _ := context.WithTimeout(context.Background(), requestTimeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", getScheme(), server), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

type RequestSender interface {
	Dispatch(*http.Request) (*http.Response, error)
}

type BasicRequestSender struct{}

func (rs *BasicRequestSender) Dispatch(fwdRequest *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(fwdRequest)
}

func getScheme() string {
	if *useHTTPS {
		return "https"
	}
	return "http"
}

func relayRequest(target string, rw http.ResponseWriter, req *http.Request) error {
	ctx, cancel := context.WithTimeout(req.Context(), requestTimeout)
	defer cancel()
	clonedRequest := req.Clone(ctx)
	clonedRequest.RequestURI = ""
	clonedRequest.URL.Host = target
	clonedRequest.URL.Scheme = getScheme()
	clonedRequest.Host = target

	resp, err := requestHandler.Dispatch(clonedRequest)
	if err != nil {
		log.Printf("Failed to get response from %s: %s", target, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, value := range values {
			rw.Header().Add(key, value)
		}
	}

	if *enableTrace {
		rw.Header().Set("lb-from", target)
	}

	log.Println("relayed", resp.StatusCode, resp.Request.URL)
	rw.WriteHeader(resp.StatusCode)

	bytesCopied, err := io.Copy(rw, resp.Body)
	if err != nil {
		log.Printf("Failed to write response: %s", err)
		return err
	}

	mutex.Lock()
	serverLoadTracker[target] += bytesCopied
	mutex.Unlock()

	return nil
}

func selectServer() string {
	mutex.Lock()
	defer mutex.Unlock()

	var leastLoadedServer string
	var minLoad int64 = -1

	for _, server := range healthyServers {
		if load, exists := serverLoadTracker[server]; exists {
			if minLoad == -1 || load < minLoad {
				minLoad = load
				leastLoadedServer = server
			}
		} else {
			serverLoadTracker[server] = 0
			if minLoad == -1 {
				minLoad = 0
				leastLoadedServer = server
			}
		}
	}

	return leastLoadedServer
}

func checkHealth(servers []string) {
	healthStatusMap := make(map[string]bool)
	for _, server := range servers {
		healthStatusMap[server] = true
	}

	for i, server := range servers {
		go func(server string) {
			for range time.Tick(10 * time.Second) {
				isHealthy := healthCheckImpl.Validate(server)
				mutex.Lock()

				if isHealthy {
					healthStatusMap[server] = true
					healthyServers[i] = server
				} else {
					healthStatusMap[server] = false
					healthyServers[i] = ""
				}

				healthyServers = nil

				for _, server := range servers {
					if healthStatusMap[server] {
						healthyServers = append(healthyServers, server)
					}
				}

				mutex.Unlock()
				log.Println(server, isHealthy)
			}
		}(server)
	}
}

func main() {
	flag.Parse()

	healthCheckImpl = &BasicHealthChecker{}
	requestHandler = &BasicRequestSender{}

	checkHealth(backendServers)

	loadBalancer := httptools.CreateServer(*serverPort, http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		targetServer := selectServer()
		if targetServer == "" {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}

		err := relayRequest(targetServer, rw, req)
		if err != nil {
			return
		}
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *enableTrace)
	loadBalancer.Start()
	signal.WaitForTerminationSignal()
}
