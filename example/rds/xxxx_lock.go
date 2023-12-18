package rds

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/wtime"
)

type xXxxxLock struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.XxxxLock")
}

func XxxxLock(uid int64) *xXxxxLock {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xXxxxLock{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.XxxxLock"),
	}
}

// With reset redis client
func (x *xXxxxLock) With(rds redis.UniversalClient) *xXxxxLock {
	x.rds = rds
	return x
}

func (x *xXxxxLock) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis lock operation

func (x *xXxxxLock) Lock(ctx context.Context, expiration time.Duration) (lockID string, err error) {
	var cmd *redis.BoolCmd
	lockID = uuid.NewString()
	switch expiration {
	case 0:
		// Use old 'SETNX' to support old Redis versions.
		cmd = redis.NewBoolCmd(ctx, "setnx", x.key, lockID)
	case svc_redis.KeepTTL:
		cmd = redis.NewBoolCmd(ctx, "set", x.key, lockID, "keepttl", "nx")
	default:
		if svc_redis.UsePrecise(expiration) {
			cmd = redis.NewBoolCmd(ctx, "set", x.key, lockID, "px", svc_redis.FormatMs(ctx, expiration), "nx")
		} else {
			cmd = redis.NewBoolCmd(ctx, "set", x.key, lockID, "ex", svc_redis.FormatSec(ctx, expiration), "nx")
		}
	}
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	if !cmd.Val() {
		err = svc_redis.ErrLockFailed
	}
	return
}

func (x *xXxxxLock) UnLock(ctx context.Context, lockID string) (ok bool, err error) {
	cmd := redis.NewIntCmd(ctx, "evalsha", svc_redis.LockerScriptUnlock.Hash, "1", x.key, lockID)
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewIntCmd(ctx, "eval", svc_redis.LockerScriptUnlock.Script, "1", x.key, lockID)
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	ok = cmd.Val() > 0
	return
}

func (x *xXxxxLock) LockFunc(ctx context.Context, expiration time.Duration) (unlock func(ctx context.Context), err error) {
	lockID, err := x.Lock(ctx, expiration)
	if err != nil {
		return
	}
	unlock = func(ctx context.Context) {
		x.UnLock(ctx, lockID)
	}
	return
}
