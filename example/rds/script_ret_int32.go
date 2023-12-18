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

type xScriptRetInt32RedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.ScriptRetInt32RedisOpt")
}

func ScriptRetInt32RedisOpt(uid int64) *xScriptRetInt32RedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xScriptRetInt32RedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.ScriptRetInt32RedisOpt"),
	}
}

// With reset redis client
func (x *xScriptRetInt32RedisOpt) With(rds redis.UniversalClient) *xScriptRetInt32RedisOpt {
	x.rds = rds
	return x
}

func (x *xScriptRetInt32RedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

const (
	_ScriptRetInt32RedisOpt_Uid  = "uid"
	_ScriptRetInt32RedisOpt_Name = "name"
)

func (x *xScriptRetInt32RedisOpt) SetPlayer(ctx context.Context, obj *server.Player) (err error) {
	n, err := x.rds.HSet(ctx, x.key,
		_ScriptRetInt32RedisOpt_Uid, rdconv.Int64ToString(obj.Uid),
		_ScriptRetInt32RedisOpt_Name, rdconv.StringToString(obj.Name),
	).Result()
	if err != nil {
		return err
	}
	if n != 2 {
		return errors.New("set Player failed")
	}
	return
}

func (x *xScriptRetInt32RedisOpt) GetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HGetAll(ctx, x.key).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if val, ok := ret[_ScriptRetInt32RedisOpt_Uid]; ok {
		obj.Uid, err = rdconv.StringToInt64(val)
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetInt32RedisOpt.Uid failed,%w", err)
		}
	}

	if val, ok := ret[_ScriptRetInt32RedisOpt_Name]; ok {

		obj.Name = val
	}

	return obj, nil
}

func (x *xScriptRetInt32RedisOpt) MGetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HMGet(ctx, x.key, _ScriptRetInt32RedisOpt_Uid, _ScriptRetInt32RedisOpt_Name).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if len(ret) > 0 && ret[0] != nil {
		obj.Uid, err = rdconv.AnyToInt64(ret[0])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetInt32RedisOpt.Uid failed,%w", err)
		}
	}

	if len(ret) > 1 && ret[1] != nil {
		obj.Name, err = rdconv.AnyToString(ret[1])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetInt32RedisOpt.Name failed,%w", err)
		}
	}

	return obj, nil
}

func (x *xScriptRetInt32RedisOpt) GetUid(ctx context.Context) (_ int64, err error) {
	val, err := x.rds.HGet(ctx, x.key, _ScriptRetInt32RedisOpt_Uid).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(val)

}
func (x *xScriptRetInt32RedisOpt) SetUid(ctx context.Context, val int64) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetInt32RedisOpt_Uid, rdconv.Int64ToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetInt32RedisOpt.Uid failed")
	}
	return nil
}

func (x *xScriptRetInt32RedisOpt) IncrByUid(ctx context.Context, incr int) (int64, error) {
	num, err := x.rds.HIncrBy(ctx, x.key, _ScriptRetInt32RedisOpt_Uid, int64(incr)).Result()
	if err != nil {
		return 0, err
	}
	return int64(num), nil
}

func (x *xScriptRetInt32RedisOpt) GetName(ctx context.Context) (_ string, err error) {
	return x.rds.HGet(ctx, x.key, _ScriptRetInt32RedisOpt_Name).Result()

}
func (x *xScriptRetInt32RedisOpt) SetName(ctx context.Context, val string) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetInt32RedisOpt_Name, rdconv.StringToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetInt32RedisOpt.Name failed")
	}
	return nil
}

var xScriptRetInt32RedisOptScriptName1Script = svc_redis.NewScript("xxx xxx x")

func (x *xScriptRetInt32RedisOpt) ScriptName1(ctx context.Context, uid int64, name string) (_ int32, err error) {
	cmd := redis.NewIntCmd(ctx, "evalsha", xScriptRetInt32RedisOptScriptName1Script.Hash, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewIntCmd(ctx, "eval", xScriptRetInt32RedisOptScriptName1Script.Script, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	return int32(cmd.Val()), nil
}
