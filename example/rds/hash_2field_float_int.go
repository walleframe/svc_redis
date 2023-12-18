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

type xHash2fieldFloatIntRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Hash2fieldFloatIntRedisOpt")
}

func Hash2fieldFloatIntRedisOpt(uid int64) *xHash2fieldFloatIntRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xHash2fieldFloatIntRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Hash2fieldFloatIntRedisOpt"),
	}
}

// With reset redis client
func (x *xHash2fieldFloatIntRedisOpt) With(rds redis.UniversalClient) *xHash2fieldFloatIntRedisOpt {
	x.rds = rds
	return x
}

func (x *xHash2fieldFloatIntRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

func (x *xHash2fieldFloatIntRedisOpt) GetField(ctx context.Context, field float32) (value int64, err error) {
	v, err := x.rds.HGet(ctx, x.key, rdconv.Float32ToString(field)).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(v)
}
func (x *xHash2fieldFloatIntRedisOpt) SetField(ctx context.Context, field float32, value int64) (err error) {
	num, err := x.rds.HSet(ctx, x.key, rdconv.Float32ToString(field), rdconv.Int64ToString(value)).Result()
	if err != nil {
		return err
	}
	if num != 1 {
		return errors.New("set field failed")
	}
	return nil
}

func (x *xHash2fieldFloatIntRedisOpt) HKeys(ctx context.Context) (vals []float32, err error) {
	ret, err := x.rds.HKeys(ctx, x.key).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		key, err := rdconv.StringToFloat32(v)
		if err != nil {
			return nil, err
		}
		vals = append(vals, key)
	}
	return
}

func (x *xHash2fieldFloatIntRedisOpt) HKeysRange(ctx context.Context, filter func(float32) bool) error {
	ret, err := x.rds.HKeys(ctx, x.key).Result()
	if err != nil {
		return err
	}
	for _, v := range ret {
		key, err := rdconv.StringToFloat32(v)
		if err != nil {
			return err
		}
		if !filter(key) {
			return nil
		}
	}
	return nil
}

func (x *xHash2fieldFloatIntRedisOpt) HVals(ctx context.Context) (vals []int64, err error) {
	ret, err := x.rds.HVals(ctx, x.key).Result()
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

func (x *xHash2fieldFloatIntRedisOpt) HValsRange(ctx context.Context, filter func(int64) bool) error {
	ret, err := x.rds.HVals(ctx, x.key).Result()
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
	return nil
}

func (x *xHash2fieldFloatIntRedisOpt) HExists(ctx context.Context, field float32) (bool, error) {
	return x.rds.HExists(ctx, x.key, rdconv.Float32ToString(field)).Result()
}

func (x *xHash2fieldFloatIntRedisOpt) HDel(ctx context.Context, field float32) (bool, error) {
	n, err := x.rds.HDel(ctx, x.key, rdconv.Float32ToString(field)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xHash2fieldFloatIntRedisOpt) HLen(ctx context.Context) (count int64, err error) {
	return x.rds.HLen(ctx, x.key).Result()
}

func (x *xHash2fieldFloatIntRedisOpt) HRandField(ctx context.Context, count int) (vals []float32, err error) {
	ret, err := x.rds.HRandField(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		key, err := rdconv.StringToFloat32(v)
		if err != nil {
			return nil, err
		}
		vals = append(vals, key)
	}
	return
}

func (x *xHash2fieldFloatIntRedisOpt) HRandFieldWithValuesRange(ctx context.Context, count int, filter func(field float32, value int64) bool) (err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {

		key, err := rdconv.StringToFloat32(v.Key)
		if err != nil {
			return err
		}

		val, err := rdconv.StringToInt64(v.Value)
		if err != nil {
			return err
		}

		if !filter(key, val) {
			return nil
		}

	}
	return
}

func (x *xHash2fieldFloatIntRedisOpt) HScanRange(ctx context.Context, match string, count int, filter func(field float32, value int64) bool) (err error) {
	cursor := uint64(0)
	var kvs []string
	for {
		kvs, cursor, err = x.rds.HScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return err
		}
		for k := 0; k < len(kvs); k += 2 {

			key, err := rdconv.StringToFloat32(kvs[k])
			if err != nil {
				return err
			}

			val, err := rdconv.StringToInt64(kvs[k+1])
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
