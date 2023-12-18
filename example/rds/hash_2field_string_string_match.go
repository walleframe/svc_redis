package rds

import (
	"context"
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xHash2fieldStringStringMatchRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Hash2fieldStringStringMatchRedisOpt")
}

func Hash2fieldStringStringMatchRedisOpt(uid int64) *xHash2fieldStringStringMatchRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xHash2fieldStringStringMatchRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Hash2fieldStringStringMatchRedisOpt"),
	}
}

// With reset redis client
func (x *xHash2fieldStringStringMatchRedisOpt) With(rds redis.UniversalClient) *xHash2fieldStringStringMatchRedisOpt {
	x.rds = rds
	return x
}

func (x *xHash2fieldStringStringMatchRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

func MergeHash2fieldStringStringMatchRedisOptValue(a1 int32, a2 int8, a4 string) string {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteInt32(a1)
	buf.WriteByte(':')
	buf.WriteInt8(a2)
	buf.WriteByte(':')
	buf.WriteString(a4)
	return buf.String()
}
func SplitHash2fieldStringStringMatchRedisOptValue(val string) (a1 int32, a2 int8, a4 string, err error) {
	items := strings.Split(val, ":")
	if len(items) != 3 {
		err = errors.New("invalid Hash2fieldStringStringMatchRedisOpt field value")
		return
	}
	a1, err = rdconv.StringToInt32(items[0])
	if err != nil {
		return
	}
	a2, err = rdconv.StringToInt8(items[1])
	if err != nil {
		return
	}
	a4 = items[2]
	return
}

func (x *xHash2fieldStringStringMatchRedisOpt) GetField(ctx context.Context, field int64) (a1 int32, a2 int8, a4 string, err error) {
	v, err := x.rds.HGet(ctx, x.key, rdconv.Int64ToString(field)).Result()
	if err != nil {
		return
	}
	return SplitHash2fieldStringStringMatchRedisOptValue(v)
}
func (x *xHash2fieldStringStringMatchRedisOpt) SetField(ctx context.Context, field int64, a1 int32, a2 int8, a4 string) (err error) {
	num, err := x.rds.HSet(ctx, x.key, rdconv.Int64ToString(field), MergeHash2fieldStringStringMatchRedisOptValue(a1, a2, a4)).Result()
	if err != nil {
		return err
	}
	if num != 1 {
		return errors.New("set field failed")
	}
	return nil
}

func (x *xHash2fieldStringStringMatchRedisOpt) HKeys(ctx context.Context) (vals []int64, err error) {
	ret, err := x.rds.HKeys(ctx, x.key).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		key, err := rdconv.StringToInt64(v)
		if err != nil {
			return nil, err
		}
		vals = append(vals, key)
	}
	return
}

func (x *xHash2fieldStringStringMatchRedisOpt) HKeysRange(ctx context.Context, filter func(int64) bool) error {
	ret, err := x.rds.HKeys(ctx, x.key).Result()
	if err != nil {
		return err
	}
	for _, v := range ret {
		key, err := rdconv.StringToInt64(v)
		if err != nil {
			return err
		}
		if !filter(key) {
			return nil
		}
	}
	return nil
}

func (x *xHash2fieldStringStringMatchRedisOpt) HValsRange(ctx context.Context, filter func(a1 int32, a2 int8, a4 string) bool) (err error) {
	ret, err := x.rds.HVals(ctx, x.key).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		a1, a2, a4, err := SplitHash2fieldStringStringMatchRedisOptValue(v)
		if err != nil {
			return err
		}
		if !filter(a1, a2, a4) {
			return nil
		}
	}
	return nil
}

func (x *xHash2fieldStringStringMatchRedisOpt) HExists(ctx context.Context, field int64) (bool, error) {
	return x.rds.HExists(ctx, x.key, rdconv.Int64ToString(field)).Result()
}

func (x *xHash2fieldStringStringMatchRedisOpt) HDel(ctx context.Context, field int64) (bool, error) {
	n, err := x.rds.HDel(ctx, x.key, rdconv.Int64ToString(field)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xHash2fieldStringStringMatchRedisOpt) HLen(ctx context.Context) (count int64, err error) {
	return x.rds.HLen(ctx, x.key).Result()
}

func (x *xHash2fieldStringStringMatchRedisOpt) HRandField(ctx context.Context, count int) (vals []int64, err error) {
	ret, err := x.rds.HRandField(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		key, err := rdconv.StringToInt64(v)
		if err != nil {
			return nil, err
		}
		vals = append(vals, key)
	}
	return
}

func (x *xHash2fieldStringStringMatchRedisOpt) HRandFieldWithValuesRange(ctx context.Context, count int, filter func(field int64, a1 int32, a2 int8, a4 string) bool) (err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {

		key, err := rdconv.StringToInt64(v.Key)
		if err != nil {
			return err
		}

		a1, a2, a4, err := SplitHash2fieldStringStringMatchRedisOptValue(v.Value)
		if err != nil {
			return err
		}

		if !filter(key, a1, a2, a4) {
			return nil
		}

	}
	return
}

func (x *xHash2fieldStringStringMatchRedisOpt) HScanRange(ctx context.Context, match string, count int, filter func(field int64, a1 int32, a2 int8, a4 string) bool) (err error) {
	cursor := uint64(0)
	var kvs []string
	for {
		kvs, cursor, err = x.rds.HScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return err
		}
		for k := 0; k < len(kvs); k += 2 {

			key, err := rdconv.StringToInt64(kvs[k])
			if err != nil {
				return err
			}

			a1, a2, a4, err := SplitHash2fieldStringStringMatchRedisOptValue(kvs[k+1])
			if err != nil {
				return err
			}

			if !filter(key, a1, a2, a4) {
				return nil
			}
		}
		if cursor == 0 {
			break
		}
	}
	return
}
