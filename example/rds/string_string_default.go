package rds

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/wtime"
)

type xStringStringDefaultRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.StringStringDefaultRedisOpt")
}

func StringStringDefaultRedisOpt(uid int64) *xStringStringDefaultRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xStringStringDefaultRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.StringStringDefaultRedisOpt"),
	}
}

// With reset redis client
func (x *xStringStringDefaultRedisOpt) With(rds redis.UniversalClient) *xStringStringDefaultRedisOpt {
	x.rds = rds
	return x
}

func (x *xStringStringDefaultRedisOpt) Key() string {
	return x.key
}

// //////////////////////////////////////////////////////////
// redis keys operation
func (x *xStringStringDefaultRedisOpt) Del(ctx context.Context) (ok bool, err error) {
	n, err := x.rds.Del(ctx, x.key).Result()
	if err != nil {
		return
	}
	ok = n == 1
	return
}

func (x *xStringStringDefaultRedisOpt) Exists(ctx context.Context) (ok bool, err error) {
	n, err := x.rds.Exists(ctx, x.key).Result()
	if err != nil {
		return
	}
	ok = n == 1
	return
}

func (x *xStringStringDefaultRedisOpt) Expire(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.Expire(ctx, x.key, expire).Result()
}

func (x *xStringStringDefaultRedisOpt) ExpireNX(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.ExpireNX(ctx, x.key, expire).Result()
}

func (x *xStringStringDefaultRedisOpt) ExpireXX(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.ExpireLT(ctx, x.key, expire).Result()
}

func (x *xStringStringDefaultRedisOpt) ExpireGT(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.ExpireLT(ctx, x.key, expire).Result()
}

func (x *xStringStringDefaultRedisOpt) ExpireLT(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.ExpireLT(ctx, x.key, expire).Result()
}

func (x *xStringStringDefaultRedisOpt) ExpireAt(ctx context.Context, expire time.Time) (ok bool, err error) {
	return x.rds.ExpireAt(ctx, x.key, expire).Result()
}

func (x *xStringStringDefaultRedisOpt) TTL(ctx context.Context) (time.Duration, error) {
	return x.rds.TTL(ctx, x.key).Result()
}

func (x *xStringStringDefaultRedisOpt) PExpire(ctx context.Context, expire time.Duration) (ok bool, err error) {
	return x.rds.PExpire(ctx, x.key, expire).Result()
}

func (x *xStringStringDefaultRedisOpt) PExpireAt(ctx context.Context, expire time.Time) (ok bool, err error) {
	return x.rds.PExpireAt(ctx, x.key, expire).Result()
}

func (x *xStringStringDefaultRedisOpt) PExpireTime(ctx context.Context) (time.Duration, error) {
	return x.rds.PExpireTime(ctx, x.key).Result()
}

func (x *xStringStringDefaultRedisOpt) PTTL(ctx context.Context) (time.Duration, error) {
	return x.rds.PTTL(ctx, x.key).Result()
}

func (x *xStringStringDefaultRedisOpt) Persist(ctx context.Context) (ok bool, err error) {
	return x.rds.Persist(ctx, x.key).Result()
}

func (x *xStringStringDefaultRedisOpt) Rename(ctx context.Context, newKey string) (err error) {
	return x.rds.Rename(ctx, x.key, newKey).Err()
}

func (x *xStringStringDefaultRedisOpt) RenameNX(ctx context.Context, newKey string) (ok bool, err error) {
	return x.rds.RenameNX(ctx, x.key, newKey).Result()
}

func (x *xStringStringDefaultRedisOpt) Type(ctx context.Context) (string, error) {
	return x.rds.Type(ctx, x.key).Result()
}
