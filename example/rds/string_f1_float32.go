package rds

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xStringF1Float32RedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.StringF1Float32RedisOpt")
}

func StringF1Float32RedisOpt(uid int64) *xStringF1Float32RedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xStringF1Float32RedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.StringF1Float32RedisOpt"),
	}
}

// With reset redis client
func (x *xStringF1Float32RedisOpt) With(rds redis.UniversalClient) *xStringF1Float32RedisOpt {
	x.rds = rds
	return x
}

func (x *xStringF1Float32RedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis string operation

func (x *xStringF1Float32RedisOpt) Get(ctx context.Context) (float32, error) {
	data, err := x.rds.Get(ctx, x.key).Result()
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseFloat(data, 64)
	if err != nil {
		return 0, err
	}
	return float32(val), nil
}

func (x *xStringF1Float32RedisOpt) IncrBy(ctx context.Context, val int) (_ float32, err error) {
	cmd := redis.NewFloatCmd(ctx, "incrbyfloat", x.key, strconv.FormatInt(int64(val), 10))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return float32(cmd.Val()), nil
}

func (x *xStringF1Float32RedisOpt) Set(ctx context.Context, val float32, expire time.Duration) error {
	return x.rds.Set(ctx, x.key, rdconv.Float64ToString(float64(val)), expire).Err()
}

func (x *xStringF1Float32RedisOpt) SetNX(ctx context.Context, val float32, expire time.Duration) (bool, error) {
	return x.rds.SetNX(ctx, x.key, rdconv.Float64ToString(float64(val)), expire).Result()
}

func (x *xStringF1Float32RedisOpt) SetEx(ctx context.Context, val float32, expire time.Duration) error {
	return x.rds.SetEx(ctx, x.key, rdconv.Float64ToString(float64(val)), expire).Err()
}
