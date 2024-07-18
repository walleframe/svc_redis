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

type xStringStringNokeyRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.StringStringNokeyRedisOpt")
}

func StringStringNokeyRedisOpt(uid int64) *xStringStringNokeyRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xStringStringNokeyRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.StringStringNokeyRedisOpt"),
	}
}

// With reset redis client
func (x *xStringStringNokeyRedisOpt) With(rds redis.UniversalClient) *xStringStringNokeyRedisOpt {
	x.rds = rds
	return x
}

func (x *xStringStringNokeyRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis string operation

func (x *xStringStringNokeyRedisOpt) Incr(ctx context.Context) (int32, error) {
	n, err := x.rds.Incr(ctx, x.key).Result()
	return int32(n), err
}

func (x *xStringStringNokeyRedisOpt) IncrBy(ctx context.Context, val int) (_ int32, err error) {
	cmd := redis.NewIntCmd(ctx, "incrby", x.key, strconv.FormatInt(int64(val), 10))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int32(cmd.Val()), nil
}

func (x *xStringStringNokeyRedisOpt) Decr(ctx context.Context) (int32, error) {
	n, err := x.rds.Decr(ctx, x.key).Result()
	return int32(n), err
}

func (x *xStringStringNokeyRedisOpt) DecrBy(ctx context.Context, val int) (_ int32, err error) {
	cmd := redis.NewIntCmd(ctx, "decrby", x.key, strconv.FormatInt(int64(val), 10))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int32(cmd.Val()), nil
}

func (x *xStringStringNokeyRedisOpt) Get(ctx context.Context) (int32, error) {
	data, err := x.rds.Get(ctx, x.key).Result()
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		return 0, err
	}
	return int32(val), nil
}

func (x *xStringStringNokeyRedisOpt) Set(ctx context.Context, val int32, expire time.Duration) error {
	return x.rds.Set(ctx, x.key, strconv.FormatInt(int64(val), 10), expire).Err()
}

func (x *xStringStringNokeyRedisOpt) SetNX(ctx context.Context, val int32, expire time.Duration) (bool, error) {
	return x.rds.SetNX(ctx, x.key, strconv.FormatInt(int64(val), 10), expire).Result()
}

func (x *xStringStringNokeyRedisOpt) SetEx(ctx context.Context, val int32, expire time.Duration) error {
	return x.rds.SetEx(ctx, x.key, strconv.FormatInt(int64(val), 10), expire).Err()
}
