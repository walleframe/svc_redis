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

type xXxxx2Lock struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Xxxx2Lock")
}

func Xxxx2Lock(uid int64) *xXxxx2Lock {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xXxxx2Lock{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Xxxx2Lock"),
	}
}

// With reset redis client
func (x *xXxxx2Lock) With(rds redis.UniversalClient) *xXxxx2Lock {
	x.rds = rds
	return x
}

func (x *xXxxx2Lock) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis lock operation

func (x *xXxxx2Lock) Lock(ctx context.Context, expiration time.Duration) (lockID string, err error) {
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

func (x *xXxxx2Lock) UnLock(ctx context.Context, lockID string) (ok bool, err error) {
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

func (x *xXxxx2Lock) LockFunc(ctx context.Context, expiration time.Duration) (unlock func(ctx context.Context), err error) {
	lockID, err := x.Lock(ctx, expiration)
	if err != nil {
		return
	}
	unlock = func(ctx context.Context) {
		x.UnLock(ctx, lockID)
	}
	return
}

// //////////////////////////////////////////////////////////
// redis keys operation
func (x *xXxxx2Lock) Del(ctx context.Context) (ok bool, err error) {
	n, err := x.rds.Del(ctx, x.key).Result()
	if err != nil {
		return
	}
	ok = n == 1
	return
}

func (x *xXxxx2Lock) Exists(ctx context.Context) (ok bool, err error) {
	n, err := x.rds.Exists(ctx, x.key).Result()
	if err != nil {
		return
	}
	ok = n == 1
	return
}

func (x *xXxxx2Lock) Expire(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.Expire(ctx, x.key, expire).Result()
}

func (x *xXxxx2Lock) ExpireNX(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.ExpireNX(ctx, x.key, expire).Result()
}

func (x *xXxxx2Lock) ExpireXX(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.ExpireLT(ctx, x.key, expire).Result()
}

func (x *xXxxx2Lock) ExpireGT(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.ExpireLT(ctx, x.key, expire).Result()
}

func (x *xXxxx2Lock) ExpireLT(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.ExpireLT(ctx, x.key, expire).Result()
}

func (x *xXxxx2Lock) ExpireAt(ctx context.Context, expire time.Time) (ok bool, err error) {
	return x.rds.ExpireAt(ctx, x.key, expire).Result()
}

func (x *xXxxx2Lock) TTL(ctx context.Context) (time.Duration, error) {
	return x.rds.TTL(ctx, x.key).Result()
}

func (x *xXxxx2Lock) PExpire(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.PExpire(ctx, x.key, expire).Result()
}

func (x *xXxxx2Lock) PExpireAt(ctx context.Context, expire time.Time) (ok bool, err error) {
	return x.rds.PExpireAt(ctx, x.key, expire).Result()
}

func (x *xXxxx2Lock) PExpireTime(ctx context.Context) (time.Duration, error) {
	return x.rds.PExpireTime(ctx, x.key).Result()
}

func (x *xXxxx2Lock) PTTL(ctx context.Context) (time.Duration, error) {
	return x.rds.PTTL(ctx, x.key).Result()
}

func (x *xXxxx2Lock) Persist(ctx context.Context) (ok bool, err error) {
	return x.rds.Persist(ctx, x.key).Result()
}

func (x *xXxxx2Lock) Rename(ctx context.Context, newKey string) (err error) {
	return x.rds.Rename(ctx, x.key, newKey).Err()
}

func (x *xXxxx2Lock) RenameNX(ctx context.Context, newKey string) (ok bool, err error) {
	return x.rds.RenameNX(ctx, x.key, newKey).Result()
}

func (x *xXxxx2Lock) Type(ctx context.Context) (string, error) {
	return x.rds.Type(ctx, x.key).Result()
}
