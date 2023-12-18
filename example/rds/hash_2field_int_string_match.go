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

type xHash2fieldIntStringMatchRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Hash2fieldIntStringMatchRedisOpt")
}

func Hash2fieldIntStringMatchRedisOpt(uid int64) *xHash2fieldIntStringMatchRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xHash2fieldIntStringMatchRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Hash2fieldIntStringMatchRedisOpt"),
	}
}

// With reset redis client
func (x *xHash2fieldIntStringMatchRedisOpt) With(rds redis.UniversalClient) *xHash2fieldIntStringMatchRedisOpt {
	x.rds = rds
	return x
}

func (x *xHash2fieldIntStringMatchRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

func MergeHash2fieldIntStringMatchRedisOptValue(x1 int32, x2 int8, arg1 string) string {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteInt32(x1)
	buf.WriteByte(':')
	buf.WriteInt8(x2)
	buf.WriteByte(':')
	buf.WriteString(arg1)
	return buf.String()
}
func SplitHash2fieldIntStringMatchRedisOptValue(val string) (x1 int32, x2 int8, arg1 string, err error) {
	items := strings.Split(val, ":")
	if len(items) != 3 {
		err = errors.New("invalid Hash2fieldIntStringMatchRedisOpt field value")
		return
	}
	x1, err = rdconv.StringToInt32(items[0])
	if err != nil {
		return
	}
	x2, err = rdconv.StringToInt8(items[1])
	if err != nil {
		return
	}
	arg1 = items[2]
	return
}

func (x *xHash2fieldIntStringMatchRedisOpt) GetField(ctx context.Context, field int64) (x1 int32, x2 int8, arg1 string, err error) {
	v, err := x.rds.HGet(ctx, x.key, rdconv.Int64ToString(field)).Result()
	if err != nil {
		return
	}
	return SplitHash2fieldIntStringMatchRedisOptValue(v)
}
func (x *xHash2fieldIntStringMatchRedisOpt) SetField(ctx context.Context, field int64, x1 int32, x2 int8, arg1 string) (err error) {
	num, err := x.rds.HSet(ctx, x.key, rdconv.Int64ToString(field), MergeHash2fieldIntStringMatchRedisOptValue(x1, x2, arg1)).Result()
	if err != nil {
		return err
	}
	if num != 1 {
		return errors.New("set field failed")
	}
	return nil
}

func (x *xHash2fieldIntStringMatchRedisOpt) HKeys(ctx context.Context) (vals []int64, err error) {
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

func (x *xHash2fieldIntStringMatchRedisOpt) HKeysRange(ctx context.Context, filter func(int64) bool) error {
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

func (x *xHash2fieldIntStringMatchRedisOpt) HValsRange(ctx context.Context, filter func(x1 int32, x2 int8, arg1 string) bool) (err error) {
	ret, err := x.rds.HVals(ctx, x.key).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		x1, x2, arg1, err := SplitHash2fieldIntStringMatchRedisOptValue(v)
		if err != nil {
			return err
		}
		if !filter(x1, x2, arg1) {
			return nil
		}
	}
	return nil
}

func (x *xHash2fieldIntStringMatchRedisOpt) HExists(ctx context.Context, field int64) (bool, error) {
	return x.rds.HExists(ctx, x.key, rdconv.Int64ToString(field)).Result()
}

func (x *xHash2fieldIntStringMatchRedisOpt) HDel(ctx context.Context, field int64) (bool, error) {
	n, err := x.rds.HDel(ctx, x.key, rdconv.Int64ToString(field)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xHash2fieldIntStringMatchRedisOpt) HLen(ctx context.Context) (count int64, err error) {
	return x.rds.HLen(ctx, x.key).Result()
}

func (x *xHash2fieldIntStringMatchRedisOpt) HRandField(ctx context.Context, count int) (vals []int64, err error) {
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

func (x *xHash2fieldIntStringMatchRedisOpt) HRandFieldWithValuesRange(ctx context.Context, count int, filter func(field int64, x1 int32, x2 int8, arg1 string) bool) (err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {

		key, err := rdconv.StringToInt64(v.Key)
		if err != nil {
			return err
		}

		x1, x2, arg1, err := SplitHash2fieldIntStringMatchRedisOptValue(v.Value)
		if err != nil {
			return err
		}

		if !filter(key, x1, x2, arg1) {
			return nil
		}

	}
	return
}

func (x *xHash2fieldIntStringMatchRedisOpt) HScanRange(ctx context.Context, match string, count int, filter func(field int64, x1 int32, x2 int8, arg1 string) bool) (err error) {
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

			x1, x2, arg1, err := SplitHash2fieldIntStringMatchRedisOptValue(kvs[k+1])
			if err != nil {
				return err
			}

			if !filter(key, x1, x2, arg1) {
				return nil
			}
		}
		if cursor == 0 {
			break
		}
	}
	return
}
