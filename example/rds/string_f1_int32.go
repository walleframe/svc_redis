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

type xStringF1Int32RedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.StringF1Int32RedisOpt")
}

func StringF1Int32RedisOpt(uid int64) *xStringF1Int32RedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xStringF1Int32RedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.StringF1Int32RedisOpt"),
	}
}

// With reset redis client
func (x *xStringF1Int32RedisOpt) With(rds redis.UniversalClient) *xStringF1Int32RedisOpt {
	x.rds = rds
	return x
}

func (x *xStringF1Int32RedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis string operation

func (x *xStringF1Int32RedisOpt) Incr(ctx context.Context) (int32, error) {
	n, err := x.rds.Incr(ctx, x.key).Result()
	return int32(n), err
}

func (x *xStringF1Int32RedisOpt) IncrBy(ctx context.Context, val int) (_ int32, err error) {
	cmd := redis.NewIntCmd(ctx, "incrby", x.key, strconv.FormatInt(int64(val), 10))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int32(cmd.Val()), nil
}

func (x *xStringF1Int32RedisOpt) Decr(ctx context.Context) (int32, error) {
	n, err := x.rds.Decr(ctx, x.key).Result()
	return int32(n), err
}

func (x *xStringF1Int32RedisOpt) DecrBy(ctx context.Context, val int) (_ int32, err error) {
	cmd := redis.NewIntCmd(ctx, "decrby", x.key, strconv.FormatInt(int64(val), 10))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int32(cmd.Val()), nil
}

func (x *xStringF1Int32RedisOpt) Get(ctx context.Context) (int32, error) {
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

func (x *xStringF1Int32RedisOpt) Set(ctx context.Context, val int32, expire time.Duration) error {
	return x.rds.Set(ctx, x.key, strconv.FormatInt(int64(val), 10), expire).Err()
}

func (x *xStringF1Int32RedisOpt) SetNX(ctx context.Context, val int32, expire time.Duration) (bool, error) {
	return x.rds.SetNX(ctx, x.key, strconv.FormatInt(int64(val), 10), expire).Result()
}

func (x *xStringF1Int32RedisOpt) SetEx(ctx context.Context, val int32, expire time.Duration) error {
	return x.rds.SetEx(ctx, x.key, strconv.FormatInt(int64(val), 10), expire).Err()
}
