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

type xScriptRetStringRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.ScriptRetStringRedisOpt")
}

func ScriptRetStringRedisOpt(uid int64) *xScriptRetStringRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xScriptRetStringRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.ScriptRetStringRedisOpt"),
	}
}

// With reset redis client
func (x *xScriptRetStringRedisOpt) With(rds redis.UniversalClient) *xScriptRetStringRedisOpt {
	x.rds = rds
	return x
}

func (x *xScriptRetStringRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

const (
	_ScriptRetStringRedisOpt_Uid  = "uid"
	_ScriptRetStringRedisOpt_Name = "name"
)

func (x *xScriptRetStringRedisOpt) SetPlayer(ctx context.Context, obj *server.Player) (err error) {
	n, err := x.rds.HSet(ctx, x.key,
		_ScriptRetStringRedisOpt_Uid, rdconv.Int64ToString(obj.Uid),
		_ScriptRetStringRedisOpt_Name, rdconv.StringToString(obj.Name),
	).Result()
	if err != nil {
		return err
	}
	if n != 2 {
		return errors.New("set Player failed")
	}
	return
}

func (x *xScriptRetStringRedisOpt) GetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HGetAll(ctx, x.key).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if val, ok := ret[_ScriptRetStringRedisOpt_Uid]; ok {
		obj.Uid, err = rdconv.StringToInt64(val)
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetStringRedisOpt.Uid failed,%w", err)
		}
	}

	if val, ok := ret[_ScriptRetStringRedisOpt_Name]; ok {

		obj.Name = val
	}

	return obj, nil
}

func (x *xScriptRetStringRedisOpt) MGetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HMGet(ctx, x.key, _ScriptRetStringRedisOpt_Uid, _ScriptRetStringRedisOpt_Name).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if len(ret) > 0 && ret[0] != nil {
		obj.Uid, err = rdconv.AnyToInt64(ret[0])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetStringRedisOpt.Uid failed,%w", err)
		}
	}

	if len(ret) > 1 && ret[1] != nil {
		obj.Name, err = rdconv.AnyToString(ret[1])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetStringRedisOpt.Name failed,%w", err)
		}
	}

	return obj, nil
}

func (x *xScriptRetStringRedisOpt) GetUid(ctx context.Context) (_ int64, err error) {
	val, err := x.rds.HGet(ctx, x.key, _ScriptRetStringRedisOpt_Uid).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(val)

}
func (x *xScriptRetStringRedisOpt) SetUid(ctx context.Context, val int64) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetStringRedisOpt_Uid, rdconv.Int64ToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetStringRedisOpt.Uid failed")
	}
	return nil
}

func (x *xScriptRetStringRedisOpt) IncrByUid(ctx context.Context, incr int) (int64, error) {
	num, err := x.rds.HIncrBy(ctx, x.key, _ScriptRetStringRedisOpt_Uid, int64(incr)).Result()
	if err != nil {
		return 0, err
	}
	return int64(num), nil
}

func (x *xScriptRetStringRedisOpt) GetName(ctx context.Context) (_ string, err error) {
	return x.rds.HGet(ctx, x.key, _ScriptRetStringRedisOpt_Name).Result()

}
func (x *xScriptRetStringRedisOpt) SetName(ctx context.Context, val string) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetStringRedisOpt_Name, rdconv.StringToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetStringRedisOpt.Name failed")
	}
	return nil
}

var xScriptRetStringRedisOptScriptName1Script = svc_redis.NewScript("xxx xxx x")

func (x *xScriptRetStringRedisOpt) ScriptName1(ctx context.Context, uid int64, name string) (_ string, err error) {
	cmd := redis.NewStringCmd(ctx, "evalsha", xScriptRetStringRedisOptScriptName1Script.Hash, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewStringCmd(ctx, "eval", xScriptRetStringRedisOptScriptName1Script.Script, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	return string(cmd.Val()), nil
}
