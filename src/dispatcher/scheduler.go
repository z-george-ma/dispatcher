// scheduler
package main

import (
	"time"
)

type Scheduler struct {
	current []*MessageRecord
	deferred []*MessageRecord
	start chan bool
	currentLock chan bool
	deferredLock chan bool
	dataReady chan bool
	pool *Pool
	process func(*MessageRecord) bool
}

func NewScheduler(pool *Pool, process func(*MessageRecord) bool) *Scheduler {
	scheduler := Scheduler{
		start: make(chan bool, 1),
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
		msgs := getDeferred(scheduler)
		if (len(msgs) > 0) {
			addToCurrentBatch(scheduler, msgs)
			signalDataReady(scheduler)
		}
	}
}

func jobLoop(scheduler *Scheduler) {
	<-scheduler.start
	for worker := range scheduler.pool.Worker {
		data := getCurrent(scheduler)
		for data == nil {
			<-scheduler.dataReady
			data = getCurrent(scheduler)
		}
		worker(scheduler.process, data)
	}
}

func addToCurrent(scheduler *Scheduler, record *MessageRecord) {
	<-scheduler.currentLock
	scheduler.current = append(scheduler.current, record)
	scheduler.currentLock <- true
}

func addToCurrentBatch(scheduler *Scheduler, records []*MessageRecord) {
	<-scheduler.currentLock
	scheduler.current = append(scheduler.current, records...)
	scheduler.currentLock <- true
}

func getCurrent(scheduler *Scheduler) *MessageRecord {
	<-scheduler.currentLock
	l := len(scheduler.current)
	if l > 0 {
		ret := scheduler.current[0]
		if l > 1 {
			copy(scheduler.current[0:], scheduler.current[1:])
		}
		scheduler.current[l - 1] = nil
		scheduler.current = scheduler.current[:l - 1]
		
		scheduler.currentLock <- true
		return ret
	}
	
	scheduler.currentLock <- true
	return nil
}

func addToDeferred(scheduler *Scheduler, record *MessageRecord) {
	<-scheduler.deferredLock
	scheduler.deferred = append(scheduler.deferred, record)
	scheduler.deferredLock <- true
}

func getDeferred(scheduler *Scheduler) []*MessageRecord {
	now := time.Now().Unix()
	<-scheduler.deferredLock
	newDeferred := scheduler.deferred[:0]
	ret := make([]*MessageRecord, 0, len(scheduler.deferred))
	
	for _, msg := range scheduler.deferred {
		if msg.RetryTimestamp <= now {
			ret = append(ret, msg)
		} else {
			newDeferred = append(newDeferred, msg)
		}
	}
	
	for i := len(newDeferred); i < len(scheduler.deferred); i++ {
		scheduler.deferred[i] = nil
	}
	
	scheduler.deferred = newDeferred
	scheduler.deferredLock <- true
	return ret
}

func (scheduler *Scheduler) Start() {
	scheduler.start <- true
}

func (scheduler *Scheduler) Write(record *MessageRecord) {
	if record.RetryTimestamp == 0 {
		addToCurrent(scheduler, record)
		signalDataReady(scheduler)
	} else {
		addToDeferred(scheduler, record)
	}
}
