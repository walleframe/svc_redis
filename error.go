package svc_redis

import "errors"

var (
	ErrLockFailed = errors.New("redis locker lock failed")
)
