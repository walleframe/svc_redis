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

type xHash2fieldStringStringPbRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Hash2fieldStringStringPbRedisOpt")
}

func Hash2fieldStringStringPbRedisOpt(uid int64) *xHash2fieldStringStringPbRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xHash2fieldStringStringPbRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Hash2fieldStringStringPbRedisOpt"),
	}
}

// With reset redis client
func (x *xHash2fieldStringStringPbRedisOpt) With(rds redis.UniversalClient) *xHash2fieldStringStringPbRedisOpt {
	x.rds = rds
	return x
}

func (x *xHash2fieldStringStringPbRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

func (x *xHash2fieldStringStringPbRedisOpt) GetField(ctx context.Context, field string) (value int64, err error) {
	v, err := x.rds.HGet(ctx, x.key, field).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(v)
}
func (x *xHash2fieldStringStringPbRedisOpt) SetField(ctx context.Context, field string, value int64) (err error) {
	num, err := x.rds.HSet(ctx, x.key, field, rdconv.Int64ToString(value)).Result()
	if err != nil {
		return err
	}
	if num != 1 {
		return errors.New("set field failed")
	}
	return nil
}

func (x *xHash2fieldStringStringPbRedisOpt) HKeys(ctx context.Context) (vals []string, err error) {
	return x.rds.HKeys(ctx, x.key).Result()
}

func (x *xHash2fieldStringStringPbRedisOpt) HKeysRange(ctx context.Context, filter func(string) bool) error {
	ret, err := x.rds.HKeys(ctx, x.key).Result()
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

func (x *xHash2fieldStringStringPbRedisOpt) HVals(ctx context.Context) (vals []int64, err error) {
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

func (x *xHash2fieldStringStringPbRedisOpt) HValsRange(ctx context.Context, filter func(int64) bool) error {
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

func (x *xHash2fieldStringStringPbRedisOpt) HExists(ctx context.Context, field string) (bool, error) {
	return x.rds.HExists(ctx, x.key, field).Result()
}

func (x *xHash2fieldStringStringPbRedisOpt) HDel(ctx context.Context, field string) (bool, error) {
	n, err := x.rds.HDel(ctx, x.key, field).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xHash2fieldStringStringPbRedisOpt) HLen(ctx context.Context) (count int64, err error) {
	return x.rds.HLen(ctx, x.key).Result()
}

func (x *xHash2fieldStringStringPbRedisOpt) HRandField(ctx context.Context, count int) (vals []string, err error) {
	return x.rds.HRandField(ctx, x.key, count).Result()
}

func (x *xHash2fieldStringStringPbRedisOpt) HRandFieldWithValues(ctx context.Context, count int) (vals map[string]int64, err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	vals = make(map[string]int64, len(ret))
	for _, v := range ret {
		key := v.Key
		val, err := rdconv.StringToInt64(v.Value)
		if err != nil {
			return nil, err
		}
		vals[key] = val
	}
	return
}

func (x *xHash2fieldStringStringPbRedisOpt) HRandFieldWithValuesRange(ctx context.Context, count int, filter func(field string, value int64) bool) (err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {

		key := v.Key

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

func (x *xHash2fieldStringStringPbRedisOpt) HScan(ctx context.Context, match string, count int) (vals map[string]int64, err error) {
	cursor := uint64(0)
	vals = make(map[string]int64)
	var kvs []string
	for {
		kvs, cursor, err = x.rds.HScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return nil, err
		}
		for k := 0; k < len(kvs); k += 2 {
			key := kvs[k]
			val, err := rdconv.StringToInt64(kvs[k+1])
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

func (x *xHash2fieldStringStringPbRedisOpt) HScanRange(ctx context.Context, match string, count int, filter func(field string, value int64) bool) (err error) {
	cursor := uint64(0)
	var kvs []string
	for {
		kvs, cursor, err = x.rds.HScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return err
		}
		for k := 0; k < len(kvs); k += 2 {

			key := kvs[k]

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
