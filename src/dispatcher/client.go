// client.go
package main

import (
	"log"
	"time"
	"net/http"
	"bytes"
)

func client(data *MessageRecord) error {
	log.Println("Processing message", data.ID)
	httpClient := &http.Client {
		Timeout: time.Duration(data.Timeout) * time.Millisecond,
	}
	
	var req *http.Request
	var err error
	
	if req, err = http.NewRequest(data.Method, data.Url, bytes.NewReader(data.Body)); err != nil {
		log.Println("error", err)
		return err
	}
	
	
	for key, value := range data.Header {
		for _, v := range value {
			req.Header.Add(key, v)
		}
	}
	
	var resp *http.Response
	if resp, err = httpClient.Do(req); err != nil {
		log.Println("Error in message", data.ID, err)
		return err
	}
	
	log.Printf("Message %s processed, status %d\n", data.ID, resp.StatusCode)
	resp.Body.Close()

	return nil
}
