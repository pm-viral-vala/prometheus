package session

import (
	"context"
	"fmt"
	"sync"
)

const (
	maxSample      = 20000
	maxThreads     = 10
	maxChannelSize = 100
)

type SessionClient struct {
	ctx       context.Context
	count     int64
	batchSize int64
	status    *bitmap
	//failed     bitmap
	sampleSize int
	threads    int
	running    bool
	ch         chan int
	callback   func(int, int) error
	wg         *sync.WaitGroup
}

func NewSessionClient(ctx context.Context, count int64, sampleSize int, threads int, cb func(int, int) error) *SessionClient {
	batchSize := (count / int64(sampleSize)) + 1

	sc := &SessionClient{
		ctx:        ctx,
		count:      count,
		sampleSize: sampleSize,
		batchSize:  batchSize,
		status:     newBitMap(batchSize),
		//failed:     newBitMap(batchSize),
		threads:  threads,
		running:  true,
		ch:       make(chan int, maxChannelSize),
		callback: cb,
		wg:       &sync.WaitGroup{},
	}

	sc.start()

	return sc
}

func (sc *SessionClient) start() {
	for i := 0; sc.running && i < sc.threads; i++ {
		sc.wg.Add(1)
		go sc.consumer()
	}
	go sc.producer()
}

func (sc *SessionClient) producer() {
	if sc.count > 0 {
		for i := 0; sc.running && i < int(sc.batchSize); i++ {
			if !sc.status.Get(i) {
				sc.ch <- i
			}
		}
	}
	if sc.running {
		close(sc.ch)
	}
}

func (sc *SessionClient) consumer() {
	defer sc.wg.Done()
	exit := false
	for !exit {
		select {
		case id, ok := <-sc.ch:
			if !ok {
				exit = true
			} else {
				if sc.callback != nil {
					start := id * sc.sampleSize
					end := start + sc.sampleSize
					if end > int(sc.count) {
						end = int(sc.count)
					}
					if err := sc.callback(start, end); err == nil {
						sc.status.Set(id, true)
					}
				}
			}
		case <-sc.ctx.Done():
			exit = true
		}
	}
}

func (sc *SessionClient) Wait() {
	sc.wg.Wait()
	fmt.Printf("\n[hackathon] wait done")

}

func (sc *SessionClient) SetCallback(cb func(int, int) error) {
	sc.callback = cb
}
