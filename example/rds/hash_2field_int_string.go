package rds

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xHash2fieldIntStringRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Hash2fieldIntStringRedisOpt")
}

func Hash2fieldIntStringRedisOpt(uid int64) *xHash2fieldIntStringRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xHash2fieldIntStringRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Hash2fieldIntStringRedisOpt"),
	}
}

// With reset redis client
func (x *xHash2fieldIntStringRedisOpt) With(rds redis.UniversalClient) *xHash2fieldIntStringRedisOpt {
	x.rds = rds
	return x
}

func (x *xHash2fieldIntStringRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

func (x *xHash2fieldIntStringRedisOpt) GetField(ctx context.Context, field int64) (value string, err error) {
	return x.rds.HGet(ctx, x.key, rdconv.Int64ToString(field)).Result()
}
func (x *xHash2fieldIntStringRedisOpt) SetField(ctx context.Context, field int64, value string) (err error) {
	num, err := x.rds.HSet(ctx, x.key, rdconv.Int64ToString(field), value).Result()
	if err != nil {
		return err
	}
	if num != 1 {
		return errors.New("set field failed")
	}
	return nil
}

func (x *xHash2fieldIntStringRedisOpt) HKeys(ctx context.Context) (vals []int64, err error) {
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

func (x *xHash2fieldIntStringRedisOpt) HKeysRange(ctx context.Context, filter func(int64) bool) error {
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

func (x *xHash2fieldIntStringRedisOpt) HVals(ctx context.Context) (vals []string, err error) {
	return x.rds.HVals(ctx, x.key).Result()
}

func (x *xHash2fieldIntStringRedisOpt) HValsRange(ctx context.Context, filter func(string) bool) error {
	ret, err := x.rds.HVals(ctx, x.key).Result()
	if err != nil {
		return err
	}
	for _, v := range ret {

		if !filter(v) {
			return nil
		}
	}
	return nil
}

func (x *xHash2fieldIntStringRedisOpt) HExists(ctx context.Context, field int64) (bool, error) {
	return x.rds.HExists(ctx, x.key, rdconv.Int64ToString(field)).Result()
}

func (x *xHash2fieldIntStringRedisOpt) HDel(ctx context.Context, field int64) (bool, error) {
	n, err := x.rds.HDel(ctx, x.key, rdconv.Int64ToString(field)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xHash2fieldIntStringRedisOpt) HLen(ctx context.Context) (count int64, err error) {
	return x.rds.HLen(ctx, x.key).Result()
}

func (x *xHash2fieldIntStringRedisOpt) HRandField(ctx context.Context, count int) (vals []int64, err error) {
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

func (x *xHash2fieldIntStringRedisOpt) HRandFieldWithValues(ctx context.Context, count int) (vals map[int64]string, err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	vals = make(map[int64]string, len(ret))
	for _, v := range ret {
		key, err := rdconv.StringToInt64(v.Key)
		if err != nil {
			return nil, err
		}
		val := v.Value
		vals[key] = val
	}
	return
}

func (x *xHash2fieldIntStringRedisOpt) HRandFieldWithValuesRange(ctx context.Context, count int, filter func(field int64, value string) bool) (err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {

		key, err := rdconv.StringToInt64(v.Key)
		if err != nil {
			return err
		}

		val := v.Value

		if !filter(key, val) {
			return nil
		}

	}
	return
}

func (x *xHash2fieldIntStringRedisOpt) HScan(ctx context.Context, match string, count int) (vals map[int64]string, err error) {
	cursor := uint64(0)
	vals = make(map[int64]string)
	var kvs []string
	for {
		kvs, cursor, err = x.rds.HScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return nil, err
		}
		for k := 0; k < len(kvs); k += 2 {
			key, err := rdconv.StringToInt64(kvs[k])
			if err != nil {
				return nil, err
			}
			val := kvs[k+1]
			vals[key] = val
		}
		if cursor == 0 {
			break
		}
	}

	return
}

func (x *xHash2fieldIntStringRedisOpt) HScanRange(ctx context.Context, match string, count int, filter func(field int64, value string) bool) (err error) {
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

			val := kvs[k+1]

			if !filter(key, val) {
				return nil
			}
		}
		if cursor == 0 {
			break
		}
	}
	return
}
