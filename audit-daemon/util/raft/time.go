package raft

import (
	"sync"
	"time"
)

var Now = NewNowTime()

// NowTime defines the current time.
type NowTime struct {
	sync.RWMutex
	now time.Time
}

// NewNowTime returns a new NowTime.
func NewNowTime() *NowTime {
	return &NowTime{
		now: time.Now(),
	}
}

// GetCurrentTime returns the current time.
func (t *NowTime) GetCurrentTime() (now time.Time) {
	t.RLock()
	now = t.now
	t.RUnlock()
	return
}
