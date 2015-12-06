// pool
package main

type worker_t struct {
	end chan bool
	f chan func() bool
}

type worker_result_t struct {
	worker *worker_t
	succeed bool
}

type Pool struct {
	lock chan bool
	sig_worker_result chan worker_result_t
	Worker chan func(func() bool)
}

func startWorker(worker *worker_t, done chan worker_result_t) {
	loop := true
	for(loop) {
		select {
		case <-worker.end:
			loop = false
		case f := <-worker.f:
			done <- worker_result_t {worker, f()}
		}
	}
}

func NewPool(maxPoolSize int) *Pool {
	pool := Pool{
		make(chan bool, 1),
		make(chan worker_result_t, 1),
		make(chan func(func() bool), maxPoolSize),
	}

	for i:=0; i < maxPoolSize; i++ {
		worker := worker_t { make(chan bool, 1), make(chan func() bool, 1)}
		go startWorker(&worker, pool.sig_worker_result)
		pool.Worker <- func(f func() bool) { worker.f <- f }
	}
	
	go func() {
		for result := range pool.sig_worker_result {
			pool.Worker <- func(f func() bool) { result.worker.f <- f }
		}
	}()
	
	return &pool
}
