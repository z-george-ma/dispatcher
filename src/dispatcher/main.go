// main.go
package main

import (
	"log"
	"time"
	"os"
)

type MessageRecord struct {
	ID uint64
	RetryCount int
	RetryTimestamp int64
	Url string
	Method string
	Header map[string] []string
	Body []byte
	Timeout int	
	Error string
}

func main() {
	config := readConfig()
	var transactionLog *TransactionLog
	var err error
	var recoverFilePtr *os.File
	var scheduler *Scheduler
	
	pool := NewPool(config.Worker)
	scheduler = NewScheduler(pool, func (data *MessageRecord) bool {
		if err := client(data); err == nil {
			if err := transactionLog.WriteAck(data.ID); err != nil {
				log.Println("Failed to write to transaction log", err)
			}
			return true
		} else {
			data.Error = err.Error()
			data.RetryCount++

			if data.RetryCount >= config.RetryLimit {
				if err := transactionLog.WriteDeadLetter(data); err != nil {
					log.Println("Failed to write to transaction log", err)
				}
				return false
			}
			data.RetryTimestamp = time.Now().Add(time.Duration(int64(time.Second) * int64(1 << uint(data.RetryCount)))).Unix()
			
			if err := transactionLog.WriteRetry(data); err != nil {
				log.Println("Failed to write to transaction log", err)
			}
			
			scheduler.Write(data)
			return false
		}
	})
	
	if !config.EventSourcing {
		recoverFile := config.Log + ".bak"
		hasFile := false
		
		
		if _, err := os.Stat(recoverFile); os.IsNotExist(err) {
			if _, err := os.Stat(config.Log); err == nil {
				if err := os.Rename(config.Log, recoverFile); err != nil {
					log.Fatal(err)
				}
				
				hasFile = true
			}
		} else if err != nil {
			log.Fatal(err)
		} else {
			hasFile = true
		}
				
		if hasFile {
			if recoverFilePtr, err = os.OpenFile(recoverFile, os.O_RDONLY, 500); err != nil {
				log.Fatal(err)
			}
			
			var maxId uint64
			
			if maxId, err = getMessageId(recoverFilePtr); err != nil {
				log.Fatal(err)
			}
			
			if transactionLog, err = NewTransactionLog(config, maxId); err != nil {
				log.Fatal(err)
			}

			process := func (message *MessageRecord) error {				
				if err = transactionLog.Write(message); err != nil {
					return err
				}
				
				scheduler.Write(message)
				return nil
			}
			
			scheduler.Start()
			
			if err := recover(recoverFilePtr, process); err != nil {
				log.Fatal(err)
			}
			
			if err = recoverFilePtr.Close(); err != nil {
				log.Fatal(err)
			}
			if err = os.Remove(recoverFile); err != nil {
				log.Fatal(err)
			}			
		} else {
			if transactionLog, err = NewTransactionLog(config, 0); err != nil {
				log.Fatal(err)
			}
			scheduler.Start()
		}
	} else {

				
		if _, err := os.Stat(config.Log); err == nil {
			// do the recovery
			if recoverFilePtr, err = os.OpenFile(config.Log, os.O_RDONLY, 500); err != nil {
				log.Fatal(err)
			}
			
			var maxId uint64
			
			if maxId, err = getMessageId(recoverFilePtr); err != nil {
				log.Fatal(err)
			}
			
			if transactionLog, err = NewTransactionLog(config, maxId); err != nil {
				log.Fatal(err)
			}			
			
			process := func (message *MessageRecord) error {
				transactionLog.Reconstruct(message)
				scheduler.Write(message)
				return nil
			}
			
			if err := recover(recoverFilePtr, process); err != nil {
				log.Fatal(err)
			}
			
			if err = recoverFilePtr.Close(); err != nil {
				log.Fatal(err)
			}			
		} else {
			if transactionLog, err = NewTransactionLog(config, 0); err != nil {
				log.Fatal(err)
			}
		}

		scheduler.Start()
	}
	
	process := func (message *MessageRecord) error {
		if message.Timeout == 0 {
			message.Timeout = config.DefaultTimeout
		}
		
		if err = transactionLog.Write(message); err != nil {
			return err
		}
		
		scheduler.Write(message)
		return nil
	}
	
	log.Println("Server started at", config.Listen)
	server(config.Listen, process)
}
