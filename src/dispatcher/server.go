// server.go
package main

import (
	"net/http"
	"encoding/json"
	"io/ioutil"
	"log"
)

func server(url string, process func(MessageRecord) error) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			buf, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			} 
			
			message := MessageRecord{}
			if err = json.Unmarshal(buf, &message); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			
			
			if err = process(message); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			
			w.WriteHeader(202)
		default:
			w.WriteHeader(405)
		}
	})

	log.Fatal(http.ListenAndServe(url, nil))
}
