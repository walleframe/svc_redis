package rds

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/wtime"
)

type xStringPbNokeyRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.StringPbNokeyRedisOpt")
}

func StringPbNokeyRedisOpt(uid int64) *xStringPbNokeyRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xStringPbNokeyRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.StringPbNokeyRedisOpt"),
	}
}

// With reset redis client
func (x *xStringPbNokeyRedisOpt) With(rds redis.UniversalClient) *xStringPbNokeyRedisOpt {
	x.rds = rds
	return x
}

func (x *xStringPbNokeyRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis string operation

func (x *xStringPbNokeyRedisOpt) GetRange(ctx context.Context, start, end int64) (string, error) {
	return x.rds.GetRange(ctx, x.key, start, end).Result()
}

func (x *xStringPbNokeyRedisOpt) SetRange(ctx context.Context, offset int64, value string) (int64, error) {
	return x.rds.SetRange(ctx, x.key, offset, value).Result()
}

func (x *xStringPbNokeyRedisOpt) Append(ctx context.Context, val string) (int64, error) {
	return x.rds.Append(ctx, x.key, val).Result()
}

func (x *xStringPbNokeyRedisOpt) StrLen(ctx context.Context) (int64, error) {
	return x.rds.StrLen(ctx, x.key).Result()
}

func (x *xStringPbNokeyRedisOpt) Get(ctx context.Context) (string, error) {
	return x.rds.Get(ctx, x.key).Result()
}

func (x *xStringPbNokeyRedisOpt) Set(ctx context.Context, data string, expire time.Duration) error {
	return x.rds.Set(ctx, x.key, data, expire).Err()
}

func (x *xStringPbNokeyRedisOpt) SetNX(ctx context.Context, data string, expire time.Duration) (bool, error) {
	return x.rds.SetNX(ctx, x.key, data, expire).Result()
}

func (x *xStringPbNokeyRedisOpt) SetEx(ctx context.Context, data string, expire time.Duration) error {
	return x.rds.SetEx(ctx, x.key, data, expire).Err()
}
