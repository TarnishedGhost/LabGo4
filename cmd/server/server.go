package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/TarnishedGhost/LabGo4/httptools"
	"github.com/TarnishedGhost/LabGo4/signal"
)

type DatabaseResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type TimestampRequest struct {
	Value string `json:"value"`
}

var serverPort = flag.Int("port", 8080, "server port")

const responseDelayEnv = "CONF_RESPONSE_DELAY_SEC"
const healthFailureEnv = "CONF_HEALTH_FAILURE"
const databaseURL = "http://db:8083/db"

func main() {
	mux := http.NewServeMux()
	httpClient := http.DefaultClient

	mux.HandleFunc("/health", healthCheckHandler)
	report := make(Report)

	mux.HandleFunc("/api/v1/some-data", func(w http.ResponseWriter, r *http.Request) {
		queryKey := r.URL.Query().Get("key")
		resp, err := httpClient.Get(fmt.Sprintf("%s/%s", databaseURL, queryKey))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			w.WriteHeader(resp.StatusCode)
			return
		}

		responseDelay := os.Getenv(responseDelayEnv)
		if delaySec, err := strconv.Atoi(responseDelay); err == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		var dbResponse DatabaseResponse
		json.NewDecoder(resp.Body).Decode(&dbResponse)
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]string{"1", "2"})
	})

	mux.Handle("/report", report)

	server := httptools.CreateServer(*serverPort, mux)
	server.Start()

	buffer := new(bytes.Buffer)
	requestBody := TimestampRequest{Value: time.Now().Format(time.RFC3339)}
	json.NewEncoder(buffer).Encode(requestBody)

	resp, _ := httpClient.Post(fmt.Sprintf("%s/gophersengineers", databaseURL), "application/json", buffer)
	defer resp.Body.Close()

	signal.WaitForTerminationSignal()
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/plain")
	if os.Getenv(healthFailureEnv) == "true" {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("FAILURE"))
	} else {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}
}
