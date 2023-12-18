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

type xScriptRetBoolRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.ScriptRetBoolRedisOpt")
}

func ScriptRetBoolRedisOpt(uid int64) *xScriptRetBoolRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xScriptRetBoolRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.ScriptRetBoolRedisOpt"),
	}
}

// With reset redis client
func (x *xScriptRetBoolRedisOpt) With(rds redis.UniversalClient) *xScriptRetBoolRedisOpt {
	x.rds = rds
	return x
}

func (x *xScriptRetBoolRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

const (
	_ScriptRetBoolRedisOpt_Uid  = "uid"
	_ScriptRetBoolRedisOpt_Name = "name"
)

func (x *xScriptRetBoolRedisOpt) SetPlayer(ctx context.Context, obj *server.Player) (err error) {
	n, err := x.rds.HSet(ctx, x.key,
		_ScriptRetBoolRedisOpt_Uid, rdconv.Int64ToString(obj.Uid),
		_ScriptRetBoolRedisOpt_Name, rdconv.StringToString(obj.Name),
	).Result()
	if err != nil {
		return err
	}
	if n != 2 {
		return errors.New("set Player failed")
	}
	return
}

func (x *xScriptRetBoolRedisOpt) GetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HGetAll(ctx, x.key).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if val, ok := ret[_ScriptRetBoolRedisOpt_Uid]; ok {
		obj.Uid, err = rdconv.StringToInt64(val)
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetBoolRedisOpt.Uid failed,%w", err)
		}
	}

	if val, ok := ret[_ScriptRetBoolRedisOpt_Name]; ok {

		obj.Name = val
	}

	return obj, nil
}

func (x *xScriptRetBoolRedisOpt) MGetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HMGet(ctx, x.key, _ScriptRetBoolRedisOpt_Uid, _ScriptRetBoolRedisOpt_Name).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if len(ret) > 0 && ret[0] != nil {
		obj.Uid, err = rdconv.AnyToInt64(ret[0])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetBoolRedisOpt.Uid failed,%w", err)
		}
	}

	if len(ret) > 1 && ret[1] != nil {
		obj.Name, err = rdconv.AnyToString(ret[1])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetBoolRedisOpt.Name failed,%w", err)
		}
	}

	return obj, nil
}

func (x *xScriptRetBoolRedisOpt) GetUid(ctx context.Context) (_ int64, err error) {
	val, err := x.rds.HGet(ctx, x.key, _ScriptRetBoolRedisOpt_Uid).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(val)

}
func (x *xScriptRetBoolRedisOpt) SetUid(ctx context.Context, val int64) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetBoolRedisOpt_Uid, rdconv.Int64ToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetBoolRedisOpt.Uid failed")
	}
	return nil
}

func (x *xScriptRetBoolRedisOpt) IncrByUid(ctx context.Context, incr int) (int64, error) {
	num, err := x.rds.HIncrBy(ctx, x.key, _ScriptRetBoolRedisOpt_Uid, int64(incr)).Result()
	if err != nil {
		return 0, err
	}
	return int64(num), nil
}

func (x *xScriptRetBoolRedisOpt) GetName(ctx context.Context) (_ string, err error) {
	return x.rds.HGet(ctx, x.key, _ScriptRetBoolRedisOpt_Name).Result()

}
func (x *xScriptRetBoolRedisOpt) SetName(ctx context.Context, val string) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetBoolRedisOpt_Name, rdconv.StringToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetBoolRedisOpt.Name failed")
	}
	return nil
}

var xScriptRetBoolRedisOptScriptName1Script = svc_redis.NewScript("xxx xxx x")

func (x *xScriptRetBoolRedisOpt) ScriptName1(ctx context.Context, uid int64, name string) (_ bool, err error) {
	cmd := redis.NewBoolCmd(ctx, "evalsha", xScriptRetBoolRedisOptScriptName1Script.Hash, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewBoolCmd(ctx, "eval", xScriptRetBoolRedisOptScriptName1Script.Script, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	return bool(cmd.Val()), nil
}
