// scheduler
package main

import (
	"time"
)

type Scheduler struct {
	current []MessageRecord
	deferred []MessageRecord
	currentLock chan bool
	deferredLock chan bool
	dataReady chan bool
	pool *Pool
	process func(MessageRecord) bool
}

func NewScheduler(pool *Pool, process func(MessageRecord) bool) *Scheduler {
	scheduler := Scheduler{
		currentLock: make(chan bool, 1),
		deferredLock: make(chan bool, 1),
		dataReady: make(chan bool, 1),
		pool: pool,
		process: process,
	}
	
	scheduler.currentLock <- true
	scheduler.deferredLock <- true
	go timeLoop(&scheduler)
	go jobLoop(&scheduler)
	return &scheduler
}

func signalDataReady(scheduler *Scheduler) {
	select {
	case scheduler.dataReady <- true:
		return
	default:
		return
	}
}

func timeLoop(scheduler *Scheduler) {
	c := time.Tick(time.Second)
	for _ = range c {
		go func() {
			msgs := getDeferred(scheduler)
			if (len(msgs) > 0) {
				addToCurrentBatch(scheduler, msgs)
				signalDataReady(scheduler)
			}				
		}()
	}
}

func jobLoop(scheduler *Scheduler) {
	for true {
		worker := <- scheduler.pool.Worker
		data, hasData := getCurrent(scheduler)
		for !hasData {
			<-scheduler.dataReady
			data, hasData = getCurrent(scheduler)
		}
		worker(func() bool { return scheduler.process(data) })
	}
}

func addToCurrent(scheduler *Scheduler, record MessageRecord) {
	<-scheduler.currentLock
	scheduler.current = append(scheduler.current, record)
	scheduler.currentLock <- true
}

func addToCurrentBatch(scheduler *Scheduler, records []MessageRecord) {
	<-scheduler.currentLock
	scheduler.current = append(scheduler.current, records...)
	scheduler.currentLock <- true
}

func getCurrent(scheduler *Scheduler) (MessageRecord, bool) {
	<-scheduler.currentLock
	if len(scheduler.current) > 0 {
		ret := scheduler.current[0]
		scheduler.current = scheduler.current[1:]
		scheduler.currentLock <- true
		return ret, true
	}
	
	scheduler.currentLock <- true
	return MessageRecord{}, false
}

func addToDeferred(scheduler *Scheduler, record MessageRecord) {
	<-scheduler.deferredLock
	scheduler.deferred = append(scheduler.deferred, record)
	scheduler.deferredLock <- true
}

func getDeferred(scheduler *Scheduler) []MessageRecord {
	now := time.Now().Unix()
	<-scheduler.deferredLock
	newDeferred := make([]MessageRecord, 0, len(scheduler.deferred))
	ret := make([]MessageRecord, 0, len(scheduler.deferred))
	
	for _, msg := range scheduler.deferred {
		if msg.RetryTimestamp <= now {
			ret = append(ret, msg)
		} else {
			newDeferred = append(newDeferred, msg)
		}
	}
	
	scheduler.deferred = newDeferred
	scheduler.deferredLock <- true
	return ret
}

func (scheduler *Scheduler) Write(record MessageRecord) {
	if record.RetryTimestamp == 0 {
		addToCurrent(scheduler, record)
		signalDataReady(scheduler)
	} else {
		addToDeferred(scheduler, record)
	}
}
