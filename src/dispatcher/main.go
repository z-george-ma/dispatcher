// main.go
package main

import (
	"log"
	"time"
	"os"
)

type MessageRecord struct {
	UUID string
	RetryCount int
	RetryTimestamp int64
	Url string
	Method string
	Header map[string]string
	Body string
	Timeout int	
	Error string
}

func main() {
	config := readConfig()	
	
	if _, err := os.Stat(config.Log + ".bak"); os.IsNotExist(err) {
		if _, err := os.Stat(config.Log); err == nil {
			if err := os.Rename(config.Log, config.Log + ".bak"); err != nil {
				log.Fatal(err)
			}
		}
	}
	
	transactionLog, err := NewTransactionLog(config.Log)
	
	if err != nil {
		log.Fatal(err)
	}
	
	pool := NewPool(config.Worker)
	var scheduler *Scheduler
	scheduler = NewScheduler(pool, func (data *MessageRecord) bool {
		if err := client(data); err == nil {
			if err := transactionLog.WriteAck(data.UUID); err != nil {
				log.Println("Failed to write to transaction log", err)
			}
			return true
		} else {
			data.RetryCount++
			data.RetryTimestamp = time.Now().Add(time.Duration(int64(time.Second) * int64(1 << uint(data.RetryCount)))).Unix()
			data.Error = err.Error()
			if err := transactionLog.WriteRetry(data); err != nil {
				log.Println("Failed to write to transaction log", err)
			}
			
			scheduler.Write(data)
			return false
		}
	})
	
	process := func (message *MessageRecord) error {
		if err = transactionLog.Write(message); err != nil {
			return err
		}
		
		scheduler.Write(message)
		return nil
	}
	
	if err := recover(config.Log + ".bak", process); err != nil {
		log.Fatal(err)
	}
	
	log.Println("Server started at", config.Listen)
	server(config.Listen, process)
}
