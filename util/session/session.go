package session

import (
	"strconv"
	"time"
)

const (
	defaultExpiryTime = 5 * time.Minute
)

type SessionInfo struct {
	SessionID string `json:"sessionid"`
	Size      int    `json:"size"`
}

type Session[T any] struct {
	id         string
	expiryTime time.Time
	data       []T
}

func NewSession[T any](data []T) *Session[T] {
	t := time.Now().UnixMilli()

	return &Session[T]{
		id:         strconv.FormatInt(t, 10),
		expiryTime: time.Now().Add(defaultExpiryTime),
		data:       data,
	}
}

func (s *Session[T]) Data(i, j int) []T {
	if i >= 0 && i < j && i < len(s.data) {
		if j > len(s.data) {
			j = len(s.data)
		}
		return s.data[i:j]
	}
	return nil
}

func (s *Session[T]) Count() int {
	return len(s.data)
}

func (s *Session[T]) ID() string {
	return s.id
}

func (s *Session[T]) ExpiryTime() time.Time {
	return s.expiryTime
}
