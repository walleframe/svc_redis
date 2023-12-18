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

type xScriptRetFloatRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.ScriptRetFloatRedisOpt")
}

func ScriptRetFloatRedisOpt(uid int64) *xScriptRetFloatRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xScriptRetFloatRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.ScriptRetFloatRedisOpt"),
	}
}

// With reset redis client
func (x *xScriptRetFloatRedisOpt) With(rds redis.UniversalClient) *xScriptRetFloatRedisOpt {
	x.rds = rds
	return x
}

func (x *xScriptRetFloatRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

const (
	_ScriptRetFloatRedisOpt_Uid  = "uid"
	_ScriptRetFloatRedisOpt_Name = "name"
)

func (x *xScriptRetFloatRedisOpt) SetPlayer(ctx context.Context, obj *server.Player) (err error) {
	n, err := x.rds.HSet(ctx, x.key,
		_ScriptRetFloatRedisOpt_Uid, rdconv.Int64ToString(obj.Uid),
		_ScriptRetFloatRedisOpt_Name, rdconv.StringToString(obj.Name),
	).Result()
	if err != nil {
		return err
	}
	if n != 2 {
		return errors.New("set Player failed")
	}
	return
}

func (x *xScriptRetFloatRedisOpt) GetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HGetAll(ctx, x.key).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if val, ok := ret[_ScriptRetFloatRedisOpt_Uid]; ok {
		obj.Uid, err = rdconv.StringToInt64(val)
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetFloatRedisOpt.Uid failed,%w", err)
		}
	}

	if val, ok := ret[_ScriptRetFloatRedisOpt_Name]; ok {

		obj.Name = val
	}

	return obj, nil
}

func (x *xScriptRetFloatRedisOpt) MGetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HMGet(ctx, x.key, _ScriptRetFloatRedisOpt_Uid, _ScriptRetFloatRedisOpt_Name).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if len(ret) > 0 && ret[0] != nil {
		obj.Uid, err = rdconv.AnyToInt64(ret[0])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetFloatRedisOpt.Uid failed,%w", err)
		}
	}

	if len(ret) > 1 && ret[1] != nil {
		obj.Name, err = rdconv.AnyToString(ret[1])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetFloatRedisOpt.Name failed,%w", err)
		}
	}

	return obj, nil
}

func (x *xScriptRetFloatRedisOpt) GetUid(ctx context.Context) (_ int64, err error) {
	val, err := x.rds.HGet(ctx, x.key, _ScriptRetFloatRedisOpt_Uid).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(val)

}
func (x *xScriptRetFloatRedisOpt) SetUid(ctx context.Context, val int64) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetFloatRedisOpt_Uid, rdconv.Int64ToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetFloatRedisOpt.Uid failed")
	}
	return nil
}

func (x *xScriptRetFloatRedisOpt) IncrByUid(ctx context.Context, incr int) (int64, error) {
	num, err := x.rds.HIncrBy(ctx, x.key, _ScriptRetFloatRedisOpt_Uid, int64(incr)).Result()
	if err != nil {
		return 0, err
	}
	return int64(num), nil
}

func (x *xScriptRetFloatRedisOpt) GetName(ctx context.Context) (_ string, err error) {
	return x.rds.HGet(ctx, x.key, _ScriptRetFloatRedisOpt_Name).Result()

}
func (x *xScriptRetFloatRedisOpt) SetName(ctx context.Context, val string) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetFloatRedisOpt_Name, rdconv.StringToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetFloatRedisOpt.Name failed")
	}
	return nil
}

var xScriptRetFloatRedisOptScriptName1Script = svc_redis.NewScript("xxx xxx x")

func (x *xScriptRetFloatRedisOpt) ScriptName1(ctx context.Context, uid int64, name string) (_ float32, err error) {
	cmd := redis.NewFloatCmd(ctx, "evalsha", xScriptRetFloatRedisOptScriptName1Script.Hash, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewFloatCmd(ctx, "eval", xScriptRetFloatRedisOptScriptName1Script.Script, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	return float32(cmd.Val()), nil
}
