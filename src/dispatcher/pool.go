// pool
package main

type worker_t struct {
	data *MessageRecord
	end chan bool
	f chan func(*MessageRecord) bool
}

type worker_result_t struct {
	worker *worker_t
	succeed bool
}

type Pool struct {
	lock chan bool
	sig_worker_result chan worker_result_t
	Worker chan func(func(*MessageRecord) bool, *MessageRecord)
}

func startWorker(worker *worker_t, done chan worker_result_t) {
	loop := true
	for(loop) {
		select {
		case <-worker.end:
			loop = false
		case f := <-worker.f:
			done <- worker_result_t {worker, f(worker.data)}
		}
	}
}

func NewPool(maxPoolSize int) *Pool {
	pool := Pool{
		make(chan bool, 1),
		make(chan worker_result_t, 1),
		make(chan func(func(*MessageRecord) bool, *MessageRecord), maxPoolSize),
	}

	for i:=0; i < maxPoolSize; i++ {
		worker := worker_t { nil, make(chan bool, 1), make(chan func(*MessageRecord) bool, 1)}
		go startWorker(&worker, pool.sig_worker_result)
		pool.Worker <- func(f func(*MessageRecord) bool, data *MessageRecord) { 
			worker.data = data 
			worker.f <- f 
		}
	}
	
	go func() {
		for result := range pool.sig_worker_result {
			pool.Worker <- func(f func(*MessageRecord) bool, data *MessageRecord) { 
				result.worker.data = data 
				result.worker.f <- f 
			}
		}
	}()
	
	return &pool
}
