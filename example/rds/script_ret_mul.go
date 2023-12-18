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

type xScriptRetMulRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.ScriptRetMulRedisOpt")
}

func ScriptRetMulRedisOpt(uid int64) *xScriptRetMulRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xScriptRetMulRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.ScriptRetMulRedisOpt"),
	}
}

// With reset redis client
func (x *xScriptRetMulRedisOpt) With(rds redis.UniversalClient) *xScriptRetMulRedisOpt {
	x.rds = rds
	return x
}

func (x *xScriptRetMulRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis hash operation

const (
	_ScriptRetMulRedisOpt_Uid  = "uid"
	_ScriptRetMulRedisOpt_Name = "name"
)

func (x *xScriptRetMulRedisOpt) SetPlayer(ctx context.Context, obj *server.Player) (err error) {
	n, err := x.rds.HSet(ctx, x.key,
		_ScriptRetMulRedisOpt_Uid, rdconv.Int64ToString(obj.Uid),
		_ScriptRetMulRedisOpt_Name, rdconv.StringToString(obj.Name),
	).Result()
	if err != nil {
		return err
	}
	if n != 2 {
		return errors.New("set Player failed")
	}
	return
}

func (x *xScriptRetMulRedisOpt) GetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HGetAll(ctx, x.key).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if val, ok := ret[_ScriptRetMulRedisOpt_Uid]; ok {
		obj.Uid, err = rdconv.StringToInt64(val)
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetMulRedisOpt.Uid failed,%w", err)
		}
	}

	if val, ok := ret[_ScriptRetMulRedisOpt_Name]; ok {

		obj.Name = val
	}

	return obj, nil
}

func (x *xScriptRetMulRedisOpt) MGetPlayer(ctx context.Context) (*server.Player, error) {
	ret, err := x.rds.HMGet(ctx, x.key, _ScriptRetMulRedisOpt_Uid, _ScriptRetMulRedisOpt_Name).Result()
	if err != nil {
		return nil, err
	}
	obj := &server.Player{}

	if len(ret) > 0 && ret[0] != nil {
		obj.Uid, err = rdconv.AnyToInt64(ret[0])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetMulRedisOpt.Uid failed,%w", err)
		}
	}

	if len(ret) > 1 && ret[1] != nil {
		obj.Name, err = rdconv.AnyToString(ret[1])
		if err != nil {
			return nil, fmt.Errorf("parse ScriptRetMulRedisOpt.Name failed,%w", err)
		}
	}

	return obj, nil
}

func (x *xScriptRetMulRedisOpt) GetUid(ctx context.Context) (_ int64, err error) {
	val, err := x.rds.HGet(ctx, x.key, _ScriptRetMulRedisOpt_Uid).Result()
	if err != nil {
		return
	}
	return rdconv.StringToInt64(val)

}
func (x *xScriptRetMulRedisOpt) SetUid(ctx context.Context, val int64) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetMulRedisOpt_Uid, rdconv.Int64ToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetMulRedisOpt.Uid failed")
	}
	return nil
}

func (x *xScriptRetMulRedisOpt) IncrByUid(ctx context.Context, incr int) (int64, error) {
	num, err := x.rds.HIncrBy(ctx, x.key, _ScriptRetMulRedisOpt_Uid, int64(incr)).Result()
	if err != nil {
		return 0, err
	}
	return int64(num), nil
}

func (x *xScriptRetMulRedisOpt) GetName(ctx context.Context) (_ string, err error) {
	return x.rds.HGet(ctx, x.key, _ScriptRetMulRedisOpt_Name).Result()

}
func (x *xScriptRetMulRedisOpt) SetName(ctx context.Context, val string) (err error) {
	n, err := x.rds.HSet(ctx, x.key, _ScriptRetMulRedisOpt_Name, rdconv.StringToString(val)).Result()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("set ScriptRetMulRedisOpt.Name failed")
	}
	return nil
}

var xScriptRetMulRedisOptScriptName1Script = svc_redis.NewScript("xxx xxx x")

func (x *xScriptRetMulRedisOpt) ScriptName1(ctx context.Context, uid int64, name string) (ret1 int32, ret2 float64, str string, vx int64, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "evalsha", xScriptRetMulRedisOptScriptName1Script.Hash, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewStringSliceCmd(ctx, "eval", xScriptRetMulRedisOptScriptName1Script.Script, "1", x.key, rdconv.Int64ToString(uid), rdconv.StringToString(name))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	vals := cmd.Val()
	if len(vals) != 4 {
		err = errors.New("script_name_1 return value count not equal 4")
		return
	}
	ret1, err = rdconv.StringToInt32(vals[0])
	if err != nil {
		return
	}
	ret2, err = rdconv.StringToFloat64(vals[1])
	if err != nil {
		return
	}
	str = vals[2]
	vx, err = rdconv.StringToInt64(vals[3])
	if err != nil {
		return
	}
	return
}
