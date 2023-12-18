package rds

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/wtime"
)

type xStringF1Uint32RedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.StringF1Uint32RedisOpt")
}

func StringF1Uint32RedisOpt(uid int64) *xStringF1Uint32RedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xStringF1Uint32RedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.StringF1Uint32RedisOpt"),
	}
}

// With reset redis client
func (x *xStringF1Uint32RedisOpt) With(rds redis.UniversalClient) *xStringF1Uint32RedisOpt {
	x.rds = rds
	return x
}

func (x *xStringF1Uint32RedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis string operation

func (x *xStringF1Uint32RedisOpt) Incr(ctx context.Context) (uint32, error) {
	n, err := x.rds.Incr(ctx, x.key).Result()
	return uint32(n), err
}

func (x *xStringF1Uint32RedisOpt) IncrBy(ctx context.Context, val int) (uint32, error) {
	n, err := x.rds.IncrBy(ctx, x.key, int64(val)).Result()
	return uint32(n), err
}

func (x *xStringF1Uint32RedisOpt) Decr(ctx context.Context) (uint32, error) {
	n, err := x.rds.Decr(ctx, x.key).Result()
	return uint32(n), err
}

func (x *xStringF1Uint32RedisOpt) DecrBy(ctx context.Context, val int) (uint32, error) {
	n, err := x.rds.DecrBy(ctx, x.key, int64(val)).Result()
	return uint32(n), err
}

func (x *xStringF1Uint32RedisOpt) Get(ctx context.Context) (uint32, error) {
	data, err := x.rds.Get(ctx, x.key).Result()
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(data, 10, 64)
	if err != nil {
		return 0, err
	}
	return uint32(val), nil
}

func (x *xStringF1Uint32RedisOpt) Set(ctx context.Context, val uint32, expire time.Duration) error {
	return x.rds.Set(ctx, x.key, strconv.FormatUint(uint64(val), 10), expire).Err()
}

func (x *xStringF1Uint32RedisOpt) SetNX(ctx context.Context, val uint32, expire time.Duration) (bool, error) {
	return x.rds.SetNX(ctx, x.key, strconv.FormatUint(uint64(val), 10), expire).Result()
}

func (x *xStringF1Uint32RedisOpt) SetEx(ctx context.Context, val uint32, expire time.Duration) error {
	return x.rds.SetEx(ctx, x.key, strconv.FormatUint(uint64(val), 10), expire).Err()
}
