package rds

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/svc_redis/example/server"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xHash1fieldMsgRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Hash1fieldMsgRedisOpt")
}

func Hash1fieldMsgRedisOpt(uid int64) *xHash1fieldMsgRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xHash1fieldMsgRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Hash1fieldMsgRedisOpt"),
	}
}

// With reset redis client
func (x *xHash1fieldMsgRedisOpt) With(rds redis.UniversalClient) *xHash1fieldMsgRedisOpt {
	x.rds = rds
	return x
}

func (x *xHash1fieldMsgRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

const (
	_Hash1fieldMsgRedisOpt_Uid  = "uid"
	_Hash1fieldMsgRedisOpt_Name = "name"
)

func (x *xHash1fieldMsgRedisOpt) SetPlayer(ctx context.Context, obj *server.Player) (err error) {
	n, err := x.rds.HSet(ctx, x.key,
		_Hash1fieldMsgRedisOpt_Uid, rdconv.Int64ToString(obj.Uid),
		_Hash1fieldMsgRedisOpt_Name, rdconv.StringToString(obj.Name),
	).Result()
	if err != nil {
		return err
	}
	if n != 2 {
		return errors.New("set Player failed")
	}
	return
}

func (x *xHash1fieldMsgRedisOpt) GetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HGetAll(ctx, x.key).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if val, ok := ret[_Hash1fieldMsgRedisOpt_Uid]; ok {
		obj.Uid, err = rdconv.StringToInt64(val)
		if err != nil {
			return nil, fmt.Errorf("parse Hash1fieldMsgRedisOpt.Uid failed,%w", err)
		}
	}

	if val, ok := ret[_Hash1fieldMsgRedisOpt_Name]; ok {

		obj.Name = val
	}

	return obj, nil
}

func (x *xHash1fieldMsgRedisOpt) MGetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HMGet(ctx, x.key, _Hash1fieldMsgRedisOpt_Uid, _Hash1fieldMsgRedisOpt_Name).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if len(ret) > 0 && ret[0] != nil {
		obj.Uid, err = rdconv.AnyToInt64(ret[0])
		if err != nil {
			return nil, fmt.Errorf("parse Hash1fieldMsgRedisOpt.Uid failed,%w", err)
		}
	}

	if len(ret) > 1 && ret[1] != nil {
		obj.Name, err = rdconv.AnyToString(ret[1])
		if err != nil {
			return nil, fmt.Errorf("parse Hash1fieldMsgRedisOpt.Name failed,%w", err)
		}
	}

	return obj, nil
}

func (x *xHash1fieldMsgRedisOpt) GetUid(ctx context.Context) (_ int64, err error) {
	val, err := x.rds.HGet(ctx, x.key, _Hash1fieldMsgRedisOpt_Uid).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(val)

}
func (x *xHash1fieldMsgRedisOpt) SetUid(ctx context.Context, val int64) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _Hash1fieldMsgRedisOpt_Uid, rdconv.Int64ToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set Hash1fieldMsgRedisOpt.Uid failed")
	}
	return nil
}

func (x *xHash1fieldMsgRedisOpt) IncrByUid(ctx context.Context, incr int) (int64, error) {
	num, err := x.rds.HIncrBy(ctx, x.key, _Hash1fieldMsgRedisOpt_Uid, int64(incr)).Result()
	if err != nil {
		return 0, err
	}
	return int64(num), nil
}

func (x *xHash1fieldMsgRedisOpt) GetName(ctx context.Context) (_ string, err error) {
	return x.rds.HGet(ctx, x.key, _Hash1fieldMsgRedisOpt_Name).Result()

}
func (x *xHash1fieldMsgRedisOpt) SetName(ctx context.Context, val string) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _Hash1fieldMsgRedisOpt_Name, rdconv.StringToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set Hash1fieldMsgRedisOpt.Name failed")
	}
	return nil
}
