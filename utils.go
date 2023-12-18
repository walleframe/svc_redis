package svc_redis

import (
	"context"
	"time"
)

// NOTE: copy from redis repo.

// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
// For example:
//
//	rdb.Set(ctx, key, value, redis.KeepTTL)
const KeepTTL = -1

func UsePrecise(dur time.Duration) bool {
	return dur < time.Second || dur%time.Second != 0
}

func FormatMs(ctx context.Context, dur time.Duration) int64 {
	if dur > 0 && dur < time.Millisecond {
		// internal.Logger.Printf(
		// 	ctx,
		// 	"specified duration is %s, but minimal supported value is %s - truncating to 1ms",
		// 	dur, time.Millisecond,
		// )
		return 1
	}
	return int64(dur / time.Millisecond)
}

func FormatSec(ctx context.Context, dur time.Duration) int64 {
	if dur > 0 && dur < time.Second {
		// internal.Logger.Printf(
		// 	ctx,
		// 	"specified duration is %s, but minimal supported value is %s - truncating to 1s",
		// 	dur, time.Second,
		// )
		return 1
	}
	return int64(dur / time.Second)
}
