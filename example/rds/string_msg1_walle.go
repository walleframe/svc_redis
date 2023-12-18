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

type xStringMsg1WalleRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.StringMsg1WalleRedisOpt")
}

func StringMsg1WalleRedisOpt(uid int64) *xStringMsg1WalleRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xStringMsg1WalleRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.StringMsg1WalleRedisOpt"),
	}
}

// With reset redis client
func (x *xStringMsg1WalleRedisOpt) With(rds redis.UniversalClient) *xStringMsg1WalleRedisOpt {
	x.rds = rds
	return x
}

func (x *xStringMsg1WalleRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis string operation

func (x *xStringMsg1WalleRedisOpt) Incr(ctx context.Context) (int32, error) {
	n, err := x.rds.Incr(ctx, x.key).Result()
	return int32(n), err
}

func (x *xStringMsg1WalleRedisOpt) IncrBy(ctx context.Context, val int) (int32, error) {
	n, err := x.rds.IncrBy(ctx, x.key, int64(val)).Result()
	return int32(n), err
}

func (x *xStringMsg1WalleRedisOpt) Decr(ctx context.Context) (int32, error) {
	n, err := x.rds.Decr(ctx, x.key).Result()
	return int32(n), err
}

func (x *xStringMsg1WalleRedisOpt) DecrBy(ctx context.Context, val int) (int32, error) {
	n, err := x.rds.DecrBy(ctx, x.key, int64(val)).Result()
	return int32(n), err
}

func (x *xStringMsg1WalleRedisOpt) Get(ctx context.Context) (int32, error) {
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

func (x *xStringMsg1WalleRedisOpt) Set(ctx context.Context, val int32, expire time.Duration) error {
	return x.rds.Set(ctx, x.key, strconv.FormatInt(int64(val), 10), expire).Err()
}

func (x *xStringMsg1WalleRedisOpt) SetNX(ctx context.Context, val int32, expire time.Duration) (bool, error) {
	return x.rds.SetNX(ctx, x.key, strconv.FormatInt(int64(val), 10), expire).Result()
}

func (x *xStringMsg1WalleRedisOpt) SetEx(ctx context.Context, val int32, expire time.Duration) error {
	return x.rds.SetEx(ctx, x.key, strconv.FormatInt(int64(val), 10), expire).Err()
}
