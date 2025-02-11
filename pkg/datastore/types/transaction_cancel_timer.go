package types

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type TransactionCancelTimer struct {
	delay     time.Duration
	done      chan struct{}
	doneMutex *sync.Mutex
	fnc       func()
}

func NewTransactionCancelTimer(delay time.Duration, f func()) *TransactionCancelTimer {
	return &TransactionCancelTimer{
		delay:     delay,
		fnc:       f,
		doneMutex: &sync.Mutex{},
	}
}

func (t *TransactionCancelTimer) Start() error {
	t.doneMutex.Lock()
	defer t.doneMutex.Unlock()
	if t.done != nil {
		return fmt.Errorf("TransactionCancelTimer already started")
	}
	t.done = make(chan struct{})

	go func() {
		timer := time.NewTimer(t.delay)
		log.Infof("TransactionCancelTimer started (%s)", t.delay.String())
		defer timer.Stop() // Ensure the timer is cleaned up

		select {
		case <-timer.C:
			// Timer fired, process TransactionCancel action
			log.Infof("TransactionCancelTimer triggered")
			if t.fnc != nil {
				t.fnc()
			}
		case <-t.done:
			// Stop the timer
			log.Infof("TransactionCancelTimer stopped")
			t.done = nil
		}
	}()

	return nil
}

func (t *TransactionCancelTimer) Stop() {
	t.doneMutex.Lock()
	defer t.doneMutex.Unlock()

	if t.done == nil {
		return
	}
	close(t.done)
}
