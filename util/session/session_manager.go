package session

import (
	"sync"
	"time"
)

const (
	purgeInterval = 10 * time.Minute
)

type SessionManager[T any] struct {
	sessions map[string]*Session[T]
	lock     sync.RWMutex
}

func NewSessionManager[T any]() *SessionManager[T] {
	return &SessionManager[T]{
		sessions: make(map[string]*Session[T], 5),
		lock:     sync.RWMutex{},
	}
}

func (sm *SessionManager[T]) AddSession(data []T) *Session[T] {
	newSession := NewSession(data)
	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.sessions[newSession.ID()] = newSession
	return newSession
}

func (sm *SessionManager[T]) GetSession(id string) (ss *Session[T]) {
	sm.lock.RLock()
	defer sm.lock.RUnlock()
	return sm.sessions[id]
}

func (sm *SessionManager[T]) purge() {
	now := time.Now()
	newSessions := make(map[string]*Session[T], len(sm.sessions))

	sm.lock.Lock()
	defer sm.lock.Unlock()

	for id, session := range sm.sessions {
		if now.Before(session.ExpiryTime()) {
			newSessions[id] = session
		}
	}
	sm.sessions = newSessions
}
