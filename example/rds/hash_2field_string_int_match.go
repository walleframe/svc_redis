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

type xHash2fieldStringIntMatchRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Hash2fieldStringIntMatchRedisOpt")
}

func Hash2fieldStringIntMatchRedisOpt(uid int64) *xHash2fieldStringIntMatchRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xHash2fieldStringIntMatchRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Hash2fieldStringIntMatchRedisOpt"),
	}
}

// With reset redis client
func (x *xHash2fieldStringIntMatchRedisOpt) With(rds redis.UniversalClient) *xHash2fieldStringIntMatchRedisOpt {
	x.rds = rds
	return x
}

func (x *xHash2fieldStringIntMatchRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

func MergeHash2fieldStringIntMatchRedisOptField(x1 int32, x2 int8, arg1 string) string {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteInt32(x1)
	buf.WriteByte(':')
	buf.WriteInt8(x2)
	buf.WriteByte(':')
	buf.WriteString(arg1)
	return buf.String()
}
func SplitHash2fieldStringIntMatchRedisOptField(val string) (x1 int32, x2 int8, arg1 string, err error) {
	items := strings.Split(val, ":")
	if len(items) != 3 {
		err = errors.New("invalid Hash2fieldStringIntMatchRedisOpt field value")
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

func (x *xHash2fieldStringIntMatchRedisOpt) GetField(ctx context.Context, x1 int32, x2 int8, arg1 string) (value int64, err error) {
	v, err := x.rds.HGet(ctx, x.key, MergeHash2fieldStringIntMatchRedisOptField(x1, x2, arg1)).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(v)
}
func (x *xHash2fieldStringIntMatchRedisOpt) SetField(ctx context.Context, x1 int32, x2 int8, arg1 string, value int64) (err error) {
	num, err := x.rds.HSet(ctx, x.key, MergeHash2fieldStringIntMatchRedisOptField(x1, x2, arg1), rdconv.Int64ToString(value)).Result()
	if err != nil {
		return err
	}
	if num != 1 {
		return errors.New("set field failed")
	}
	return nil
}

func (x *xHash2fieldStringIntMatchRedisOpt) HKeysRange(ctx context.Context, filter func(x1 int32, x2 int8, arg1 string) bool) (err error) {
	ret, err := x.rds.HKeys(ctx, x.key).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		x1, x2, arg1, err := SplitHash2fieldStringIntMatchRedisOptField(v)
		if err != nil {
			return err
		}
		if !filter(x1, x2, arg1) {
			return nil
		}
	}
	return nil
}

func (x *xHash2fieldStringIntMatchRedisOpt) HVals(ctx context.Context) (vals []int64, err error) {
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

func (x *xHash2fieldStringIntMatchRedisOpt) HValsRange(ctx context.Context, filter func(int64) bool) error {
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

func (x *xHash2fieldStringIntMatchRedisOpt) HExists(ctx context.Context, x1 int32, x2 int8, arg1 string) (bool, error) {
	return x.rds.HExists(ctx, x.key, MergeHash2fieldStringIntMatchRedisOptField(x1, x2, arg1)).Result()
}

func (x *xHash2fieldStringIntMatchRedisOpt) HDel(ctx context.Context, x1 int32, x2 int8, arg1 string) (bool, error) {
	n, err := x.rds.HDel(ctx, x.key, MergeHash2fieldStringIntMatchRedisOptField(x1, x2, arg1)).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (x *xHash2fieldStringIntMatchRedisOpt) HLen(ctx context.Context) (count int64, err error) {
	return x.rds.HLen(ctx, x.key).Result()
}

func (x *xHash2fieldStringIntMatchRedisOpt) HRandFieldRange(ctx context.Context, count int, filter func(x1 int32, x2 int8, arg1 string) bool) (err error) {
	ret, err := x.rds.HRandField(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {
		x1, x2, arg1, err := SplitHash2fieldStringIntMatchRedisOptField(v)
		if err != nil {
			return err
		}
		if !filter(x1, x2, arg1) {
			return nil
		}
	}
	return
}

func (x *xHash2fieldStringIntMatchRedisOpt) HRandFieldWithValuesRange(ctx context.Context, count int, filter func(x1 int32, x2 int8, arg1 string, value int64) bool) (err error) {
	ret, err := x.rds.HRandFieldWithValues(ctx, x.key, count).Result()
	if err != nil {
		return
	}
	for _, v := range ret {

		x1, x2, arg1, err := SplitHash2fieldStringIntMatchRedisOptField(v.Key)
		if err != nil {
			return err
		}

		val, err := rdconv.StringToInt64(v.Value)
		if err != nil {
			return err
		}

		if !filter(x1, x2, arg1, val) {
			return nil
		}

	}
	return
}

func (x *xHash2fieldStringIntMatchRedisOpt) HScanRange(ctx context.Context, match string, count int, filter func(x1 int32, x2 int8, arg1 string, value int64) bool) (err error) {
	cursor := uint64(0)
	var kvs []string
	for {
		kvs, cursor, err = x.rds.HScan(ctx, x.key, cursor, match, int64(count)).Result()
		if err != nil {
			return err
		}
		for k := 0; k < len(kvs); k += 2 {
			x1, x2, arg1, err := SplitHash2fieldStringIntMatchRedisOptField(kvs[k])
			if err != nil {
				return err
			}

			val, err := rdconv.StringToInt64(kvs[k+1])
			if err != nil {
				return err
			}

			if !filter(x1, x2, arg1, val) {
				return nil
			}
		}
		if cursor == 0 {
			break
		}
	}
	return
}
