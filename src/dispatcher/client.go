// client.go
package main

import (
	"log"
	"time"
	"net/http"
	"strings"
)

func client(data MessageRecord) error {
	log.Println("Processing message", data.UUID)
		httpClient := &http.Client {
		Timeout: time.Duration(data.Timeout) * time.Millisecond,
	}
	
	var req *http.Request
	var err error
	
	if req, err = http.NewRequest(data.Method, data.Url, strings.NewReader(data.Body)); err != nil {
		log.Println("error", err)
		return err
	}
	
	for key, value := range data.Header {
		req.Header.Add(key, value)
	}
	
	var resp *http.Response
	if resp, err = httpClient.Do(req); err != nil {
		log.Println("Error in message", data.UUID, err)
		return err
	} 
	
	log.Printf("Message %s processed, status %d\n", data.UUID, resp.StatusCode)
	
	return nil
}
