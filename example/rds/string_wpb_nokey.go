package rds

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/process/message"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/wtime"
)

type xStringWpbNokeyRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.StringWpbNokeyRedisOpt")
}

func StringWpbNokeyRedisOpt(uid int64) *xStringWpbNokeyRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xStringWpbNokeyRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.StringWpbNokeyRedisOpt"),
	}
}

// With reset redis client
func (x *xStringWpbNokeyRedisOpt) With(rds redis.UniversalClient) *xStringWpbNokeyRedisOpt {
	x.rds = rds
	return x
}

func (x *xStringWpbNokeyRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis string operation

func (x *xStringWpbNokeyRedisOpt) Set(ctx context.Context, pb message.Message, expire time.Duration) error {
	data, err := pb.MarshalObject()
	if err != nil {
		return err
	}
	return x.rds.Set(ctx, x.key, util.BytesToString(data), expire).Err()
}

func (x *xStringWpbNokeyRedisOpt) SetNX(ctx context.Context, pb message.Message, expire time.Duration) error {
	data, err := pb.MarshalObject()
	if err != nil {
		return err
	}
	return x.rds.SetNX(ctx, x.key, util.BytesToString(data), expire).Err()
}

func (x *xStringWpbNokeyRedisOpt) SetEx(ctx context.Context, pb message.Message, expire time.Duration) error {
	data, err := pb.MarshalObject()
	if err != nil {
		return err
	}
	return x.rds.SetEx(ctx, x.key, util.BytesToString(data), expire).Err()
}

func (x *xStringWpbNokeyRedisOpt) Get(ctx context.Context, pb message.Message) error {
	data, err := x.rds.Get(ctx, x.key).Result()
	if err != nil {
		return err
	}
	err = pb.UnmarshalObject(util.StringToBytes(data))
	if err != nil {
		return err
	}
	return nil
}
