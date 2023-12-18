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

type xHash2fieldIntFloatRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Hash2fieldIntFloatRedisOpt")
}

func Hash2fieldIntFloatRedisOpt(uid int64) *xHash2fieldIntFloatRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xHash2fieldIntFloatRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Hash2fieldIntFloatRedisOpt"),
	}
}

// With reset redis client
func (x *xHash2fieldIntFloatRedisOpt) With(rds redis.UniversalClient) *xHash2fieldIntFloatRedisOpt {
	x.rds = rds
	return x
}

func (x *xHash2fieldIntFloatRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

func (x *xHash2fieldIntFloatRedisOpt) GetField(ctx context.Context, field int64) (value float64, err error) {
	v, err := x.rds.HGet(ctx, x.key, rdconv.Int64ToString(field)).Result()
	if err != nil {
		return
	}
	return rdconv.StringToFloat64(v)
}
func (x *xHash2fieldIntFloatRedisOpt) SetField(ctx context.Context, field int64, value float64) (err error) {
	num, err := x.rds.HSet(ctx, x.key, rdconv.Int64ToString(field), rdconv.Float64ToString(value)).Result()
	if err != nil {
		return err
	}
	if num != 1 {
		return errors.New("set field failed")
	}
	return nil
}

func (x *xHash2fieldIntFloatRedisOpt) HKeys(ctx context.Context) (vals []int64, err error) {
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

func (x *xHash2fieldIntFloatRedisOpt) HKeysRange(ctx context.Context, filter func(int64) bool) error {
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

func (x *xHash2fieldIntFloatRedisOpt) HVals(ctx context.Context) (vals []float64, err error) {
	ret, err := x.rds.HVals(ctx, x.key).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		val, err := rdconv.StringToFloat64(v)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return
}

func (x *xHash2fieldIntFloatRedisOpt) HValsRange(ctx context.Context, filter func(float64) bool) error {
	ret, err := x.rds.HVals(ctx, x.key).Result()
	if err != nil {
		return err
	}
	for _, v := range ret {
		val, err := rdconv.StringToFloat64(v)
		if err != nil {
			return err
		}
		if !filter(val) {
			return nil
		}
	}
	return nil
}

func (x *xHash2fieldIntFloatRedisOpt) HExists(ctx context.Context, field int64) (bool, error) {
	return x.rds.HExists(ctx, x.key, rdconv.Int64ToString(field)).Result()
}

func (x *xHash2fieldIntFloatRedisOpt) HDel(ctx context.Context, field int64) (bool, error) {
	n, err := x.rds.HDel(ctx, x.key, rdconv.Int64ToString(field)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xHash2fieldIntFloatRedisOpt) HLen(ctx context.Context) (count int64, err error) {
	return x.rds.HLen(ctx, x.key).Result()
}

func (x *xHash2fieldIntFloatRedisOpt) HRandField(ctx context.Context, count int) (vals []int64, err error) {
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

func (x *xHash2fieldIntFloatRedisOpt) HRandFieldWithValues(ctx context.Context, count int) (vals map[int64]float64, err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	vals = make(map[int64]float64, len(ret))
	for _, v := range ret {
		key, err := rdconv.StringToInt64(v.Key)
		if err != nil {
			return nil, err
		}
		val, err := rdconv.StringToFloat64(v.Value)
		if err != nil {
			return nil, err
		}
		vals[key] = val
	}
	return
}

func (x *xHash2fieldIntFloatRedisOpt) HRandFieldWithValuesRange(ctx context.Context, count int, filter func(field int64, value float64) bool) (err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {

		key, err := rdconv.StringToInt64(v.Key)
		if err != nil {
			return err
		}

		val, err := rdconv.StringToFloat64(v.Value)
		if err != nil {
			return err
		}

		if !filter(key, val) {
			return nil
		}

	}
	return
}

func (x *xHash2fieldIntFloatRedisOpt) HScan(ctx context.Context, match string, count int) (vals map[int64]float64, err error) {
	cursor := uint64(0)
	vals = make(map[int64]float64)
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
			val, err := rdconv.StringToFloat64(kvs[k+1])
			if err != nil {
				return nil, err
			}
			vals[key] = val
		}
		if cursor == 0 {
			break
		}
	}

	return
}

func (x *xHash2fieldIntFloatRedisOpt) HScanRange(ctx context.Context, match string, count int, filter func(field int64, value float64) bool) (err error) {
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

			val, err := rdconv.StringToFloat64(kvs[k+1])
			if err != nil {
				return err
			}

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
