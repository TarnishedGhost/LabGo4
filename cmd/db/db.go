package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/TarnishedGhost/LabGo4/datastore"
	"github.com/TarnishedGhost/LabGo4/httptools"
	"github.com/TarnishedGhost/LabGo4/signal"
)

var serverPort = flag.Int("port", 8083, "server port")

type JsonResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type JsonRequest struct {
	Value string `json:"value"`
}

func main() {
	mux := new(http.ServeMux)
	tempDir, err := os.MkdirTemp("", "temp-dir")
	if err != nil {
		log.Fatal(err)
	}

	db, _ := datastore.NewDb(tempDir, 250)
	defer db.Close()

	mux.HandleFunc("/db/", func(responseWriter http.ResponseWriter, request *http.Request) {
		requestURL := request.URL.String()
		key := requestURL[4:]

		switch request.Method {
		case "GET":
			value, err := db.Get(key)
			if err != nil {
				responseWriter.WriteHeader(http.StatusNotFound)
				return
			}
			responseWriter.WriteHeader(http.StatusOK)
			responseWriter.Header().Set("content-type", "application/json")
			_ = json.NewEncoder(responseWriter).Encode(JsonResponse{
				Key:   key,
				Value: value,
			})
		case "POST":
			var requestBody JsonRequest

			err := json.NewDecoder(request.Body).Decode(&requestBody)
			if err != nil {
				responseWriter.WriteHeader(http.StatusBadRequest)
				return
			}

			err = db.Put(key, requestBody.Value)
			if err != nil {
				responseWriter.WriteHeader(http.StatusInternalServerError)
				return
			}
			responseWriter.WriteHeader(http.StatusCreated)
		default:
			responseWriter.WriteHeader(http.StatusBadRequest)
		}
	})

	server := httptools.CreateServer(*serverPort, mux)
	server.Start()
	signal.WaitForTerminationSignal()
}
