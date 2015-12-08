// server.go
package main

import (
	"net/http"
	"strconv"
	"io/ioutil"
	"log"
)

const XDispatcherOriginalHost = "X-Dispatcher-Original-Host"
const XDispatcherOriginalTimeout = "X-Dispatcher-Timeout"

func server(url string, process func(*MessageRecord) error) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		
		if host := r.Header.Get(XDispatcherOriginalHost); host == "" {
			w.WriteHeader(405)
		} else {
			buf, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			
			var timeout int
			if timeout, err = strconv.Atoi(r.Header.Get(XDispatcherOriginalTimeout)); err != nil {
				timeout = 0
			}
			
			r.Header.Del(XDispatcherOriginalHost)
			r.Header.Del(XDispatcherOriginalTimeout)
			
			message := MessageRecord {
				Url: host + r.URL.String(),
				Method: r.Method,
				Header: http.Header(r.Header),
				Body: buf,
				Timeout: timeout,
			}
			
			if err = process(&message); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}			
			
			w.WriteHeader(202)
		}
	})

	log.Fatal(http.ListenAndServe(url, nil))
}
