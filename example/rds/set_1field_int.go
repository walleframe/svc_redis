package rds

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xSet1fieldIntRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Set1fieldIntRedisOpt")
}

func Set1fieldIntRedisOpt(uid int64) *xSet1fieldIntRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xSet1fieldIntRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Set1fieldIntRedisOpt"),
	}
}

// With reset redis client
func (x *xSet1fieldIntRedisOpt) With(rds redis.UniversalClient) *xSet1fieldIntRedisOpt {
	x.rds = rds
	return x
}

func (x *xSet1fieldIntRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis set operation

func (x *xSet1fieldIntRedisOpt) SAdd(ctx context.Context, val int64) (bool, error) {
	n, err := x.rds.SAdd(ctx, x.key, rdconv.Int64ToString(val)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xSet1fieldIntRedisOpt) SCard(ctx context.Context) (int64, error) {
	return x.rds.SCard(ctx, x.key).Result()
}

func (x *xSet1fieldIntRedisOpt) SRem(ctx context.Context, val int64) (bool, error) {
	n, err := x.rds.SRem(ctx, x.key, rdconv.Int64ToString(val)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xSet1fieldIntRedisOpt) SIsMember(ctx context.Context, val int64) (bool, error) {
	return x.rds.SIsMember(ctx, x.key, rdconv.Int64ToString(val)).Result()
}

func (x *xSet1fieldIntRedisOpt) SPop(ctx context.Context) (_ int64, err error) {
	v, err := x.rds.SPop(ctx, x.key).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(v)
}
func (x *xSet1fieldIntRedisOpt) SRandMember(ctx context.Context) (_ int64, err error) {
	v, err := x.rds.SRandMember(ctx, x.key).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(v)
}

func (x *xSet1fieldIntRedisOpt) SRandMemberN(ctx context.Context, count int) (vals []int64, err error) {
	ret, err := x.rds.SRandMemberN(ctx, x.key, int64(count)).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		val, err := rdconv.StringToInt64(v)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return
}

func (x *xSet1fieldIntRedisOpt) SMembers(ctx context.Context, count int) (vals []int64, err error) {
	ret, err := x.rds.SMembers(ctx, x.key).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		val, err := rdconv.StringToInt64(v)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return
}

func (x *xSet1fieldIntRedisOpt) SScan(ctx context.Context, match string, count int) (vals []int64, err error) {
	cursor := uint64(0)
	var ret []string
	for {
		ret, cursor, err = x.rds.SScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return nil, err
		}
		for _, v := range ret {
			val, err := rdconv.StringToInt64(v)
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
		}
		if cursor == 0 {
			break
		}
	}
	return
}

func (x *xSet1fieldIntRedisOpt) SScanRange(ctx context.Context, match string, count int, filter func(int64) bool) (err error) {
	cursor := uint64(0)
	var ret []string
	for {
		ret, cursor, err = x.rds.SScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return err
		}
		for _, v := range ret {
			val, err := rdconv.StringToInt64(v)
			if err != nil {
				return err
			}
			if !filter(val) {
				return nil
			}
		}
		if cursor == 0 {
			break
		}
	}
	return
}
