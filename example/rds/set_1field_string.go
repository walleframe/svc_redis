package rds

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/wtime"
)

type xSet1fieldStringRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Set1fieldStringRedisOpt")
}

func Set1fieldStringRedisOpt(uid int64) *xSet1fieldStringRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xSet1fieldStringRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Set1fieldStringRedisOpt"),
	}
}

// With reset redis client
func (x *xSet1fieldStringRedisOpt) With(rds redis.UniversalClient) *xSet1fieldStringRedisOpt {
	x.rds = rds
	return x
}

func (x *xSet1fieldStringRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis set operation

func (x *xSet1fieldStringRedisOpt) SAdd(ctx context.Context, val string) (bool, error) {
	n, err := x.rds.SAdd(ctx, x.key, val).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xSet1fieldStringRedisOpt) SCard(ctx context.Context) (int64, error) {
	return x.rds.SCard(ctx, x.key).Result()
}

func (x *xSet1fieldStringRedisOpt) SRem(ctx context.Context, val string) (bool, error) {
	n, err := x.rds.SRem(ctx, x.key, val).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xSet1fieldStringRedisOpt) SIsMember(ctx context.Context, val string) (bool, error) {
	return x.rds.SIsMember(ctx, x.key, val).Result()
}

func (x *xSet1fieldStringRedisOpt) SPop(ctx context.Context) (_ string, err error) {
	return x.rds.SPop(ctx, x.key).Result()
}
func (x *xSet1fieldStringRedisOpt) SRandMember(ctx context.Context) (_ string, err error) {
	return x.rds.SRandMember(ctx, x.key).Result()
}

func (x *xSet1fieldStringRedisOpt) SRandMemberN(ctx context.Context, count int) (vals []string, err error) {
	return x.rds.SRandMemberN(ctx, x.key, int64(count)).Result()
}

func (x *xSet1fieldStringRedisOpt) SMembers(ctx context.Context, count int) (vals []string, err error) {
	return x.rds.SMembers(ctx, x.key).Result()
}

func (x *xSet1fieldStringRedisOpt) SScan(ctx context.Context, match string, count int) (vals []string, err error) {
	cursor := uint64(0)
	var ret []string
	for {
		ret, cursor, err = x.rds.SScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return nil, err
		}
		vals = append(vals, ret...)
		if cursor == 0 {
			break
		}
	}
	return
}

func (x *xSet1fieldStringRedisOpt) SScanRange(ctx context.Context, match string, count int, filter func(string) bool) (err error) {
	cursor := uint64(0)
	var ret []string
	for {
		ret, cursor, err = x.rds.SScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return err
		}
		for _, v := range ret {
			if !filter(v) {
				return nil
			}
		}
		if cursor == 0 {
			break
		}
	}
	return
}
