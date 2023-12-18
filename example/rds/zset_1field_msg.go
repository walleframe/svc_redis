package rds

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/svc_redis/example/server"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xZset1fieldMsgRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Zset1fieldMsgRedisOpt")
}

func Zset1fieldMsgRedisOpt(uid int64) *xZset1fieldMsgRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xZset1fieldMsgRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Zset1fieldMsgRedisOpt"),
	}
}

// With reset redis client
func (x *xZset1fieldMsgRedisOpt) With(rds redis.UniversalClient) *xZset1fieldMsgRedisOpt {
	x.rds = rds
	return x
}

func (x *xZset1fieldMsgRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis zset operation

func (x *xZset1fieldMsgRedisOpt) ZCard(ctx context.Context) (int64, error) {
	cmd := redis.NewIntCmd(ctx, "zcard", x.key)
	x.rds.Process(ctx, cmd)
	return cmd.Result()
}

func (x *xZset1fieldMsgRedisOpt) ZAdd(ctx context.Context, mem *server.Player, score float64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, util.BytesToString(data), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZAddNX(ctx context.Context, mem *server.Player, score float64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "nx", util.BytesToString(data), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZAddXX(ctx context.Context, mem *server.Player, score float64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "xx", util.BytesToString(data), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZAddLT(ctx context.Context, mem *server.Player, score float64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "lt", util.BytesToString(data), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZAddGT(ctx context.Context, mem *server.Player, score float64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "gt", util.BytesToString(data), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZAdds(ctx context.Context, vals map[*server.Player]float64) error {
	args := make([]interface{}, 2, 2+len(vals)*2)
	args[0] = "zadd"
	args[1] = x.key
	for k, v := range vals {
		data, err := k.MarshalObject()
		if err != nil {
			return err
		}
		args = append(args, util.BytesToString(data))
		args = append(args, rdconv.Float64ToString(v))
	}
	cmd := redis.NewIntCmd(ctx, args...)
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZRem(ctx context.Context, mem *server.Player) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zrem", x.key, util.BytesToString(data))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZIncrBy(ctx context.Context, increment float64, mem *server.Player) (_ float64, err error) {
	data, err := mem.MarshalObject()
	if err != nil {
		return
	}
	cmd := redis.NewFloatCmd(ctx, "zincrby", x.key, rdconv.Float64ToString(increment), util.BytesToString(data))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return float64(cmd.Val()), nil
}

func (x *xZset1fieldMsgRedisOpt) ZScore(ctx context.Context, mem *server.Player) (_ float64, err error) {
	data, err := mem.MarshalObject()
	if err != nil {
		return
	}
	cmd := redis.NewFloatCmd(ctx, "zscore", x.key, util.BytesToString(data))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return float64(cmd.Val()), nil
}

func (x *xZset1fieldMsgRedisOpt) ZRank(ctx context.Context, mem *server.Player) (_ int64, err error) {
	data, err := mem.MarshalObject()
	if err != nil {
		return
	}
	cmd := redis.NewIntCmd(ctx, "zrank", x.key, util.BytesToString(data))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset1fieldMsgRedisOpt) ZRankWithScore(ctx context.Context, mem *server.Player) (rank int64, score float64, err error) {
	data, err := mem.MarshalObject()
	if err != nil {
		return
	}
	cmd := redis.NewRankWithScoreCmd(ctx, "zrank", x.key, util.BytesToString(data), "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = float64(cmd.Val().Score)
	return
}

func (x *xZset1fieldMsgRedisOpt) ZRevRank(ctx context.Context, mem *server.Player) (_ int64, err error) {
	data, err := mem.MarshalObject()
	if err != nil {
		return
	}
	cmd := redis.NewIntCmd(ctx, "zrevrank", x.key, util.BytesToString(data))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset1fieldMsgRedisOpt) ZRevRankWithScore(ctx context.Context, mem *server.Player) (rank int64, score float64, err error) {
	data, err := mem.MarshalObject()
	if err != nil {
		return
	}
	cmd := redis.NewRankWithScoreCmd(ctx, "zrevrank", x.key, util.BytesToString(data), "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = float64(cmd.Val().Score)
	return
}

func (x *xZset1fieldMsgRedisOpt) parseMemberSliceCmd(cmd *redis.StringSliceCmd) (vals []*server.Player, err error) {
	for _, v := range cmd.Val() {
		val := &server.Player{}
		err = val.UnmarshalObject(util.StringToBytes(v))
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return
}

func (x *xZset1fieldMsgRedisOpt) ZRange(ctx context.Context, start, stop int64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZRangeByScore(ctx context.Context, start, stop float64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZRevRange(ctx context.Context, start, stop int64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZRevRangeByScore(ctx context.Context, start, stop float64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) rangeMemberSliceCmd(cmd *redis.StringSliceCmd, f func(*server.Player) bool) (err error) {
	for _, v := range cmd.Val() {
		val := &server.Player{}
		err = val.UnmarshalObject(util.StringToBytes(v))
		if err != nil {
			return err
		}
		if !f(val) {
			return nil
		}
	}
	return
}

func (x *xZset1fieldMsgRedisOpt) ZRangeF(ctx context.Context, start, stop int64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) ZRangeByScoreF(ctx context.Context, start, stop float64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) ZRevRangeF(ctx context.Context, start, stop int64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) ZRevRangeByScoreF(ctx context.Context, start, stop float64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) parseZSliceCmd(cmd *redis.ZSliceCmd) (vals map[*server.Player]float64, err error) {
	vals = make(map[*server.Player]float64)
	for _, v := range cmd.Val() {
		str, err := rdconv.AnyToString(v.Member)
		if err != nil {
			return nil, err
		}
		val := &server.Player{}
		err = val.UnmarshalObject(util.StringToBytes(str))
		if err != nil {
			return nil, err
		}
		vals[val] = float64(v.Score)
	}
	return
}

func (x *xZset1fieldMsgRedisOpt) ZRangeWithScores(ctx context.Context, start, stop int64) (vals map[*server.Player]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZRangeByScoreWithScores(ctx context.Context, start, stop float64) (vals map[*server.Player]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}
func (x *xZset1fieldMsgRedisOpt) ZRevRangeWithScores(ctx context.Context, start, stop int64) (vals map[*server.Player]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZRevRangeByScoreWithScores(ctx context.Context, start, stop float64) (vals map[*server.Player]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) rangeZSliceCmd(cmd *redis.ZSliceCmd, f func(*server.Player, float64) bool) (err error) {
	for _, v := range cmd.Val() {
		str, err := rdconv.AnyToString(v.Member)
		if err != nil {
			return err
		}
		val := &server.Player{}
		err = val.UnmarshalObject(util.StringToBytes(str))
		if err != nil {
			return err
		}
		if !f(val, float64(v.Score)) {
			return nil
		}
	}
	return
}

func (x *xZset1fieldMsgRedisOpt) ZRangeWithScoresF(ctx context.Context, start, stop int64, f func(*server.Player, float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) ZRangeByScoreWithScoresF(ctx context.Context, start, stop float64, f func(*server.Player, float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}
func (x *xZset1fieldMsgRedisOpt) ZRevRangeWithScoresF(ctx context.Context, start, stop int64, f func(*server.Player, float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) ZRevRangeByScoreWithScoresF(ctx context.Context, start, stop float64, f func(*server.Player, float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) ZPopMin(ctx context.Context, count int64) (_ map[*server.Player]float64, err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZPopMinF(ctx context.Context, count int64, f func(*server.Player, float64) bool) (err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) ZPopMax(ctx context.Context, count int64) (_ map[*server.Player]float64, err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZPopMaxF(ctx context.Context, count int64, f func(*server.Player, float64) bool) (err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) ZPopGTScore(ctx context.Context, limitScore float64, count int64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "evalsha", svc_redis.ZPopMaxValue.Hash, "1", x.key, rdconv.Float64ToString(limitScore), rdconv.Int64ToString(count))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewStringSliceCmd(ctx, "eval", svc_redis.ZPopMaxValue.Script, "1", x.key, rdconv.Float64ToString(limitScore), rdconv.Int64ToString(count))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZPopGTScoreF(ctx context.Context, limitScore float64, count int64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "evalsha", svc_redis.ZPopMaxValue.Hash, "1", x.key, rdconv.Float64ToString(limitScore), rdconv.Int64ToString(count))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewStringSliceCmd(ctx, "eval", svc_redis.ZPopMaxValue.Script, "1", x.key, rdconv.Float64ToString(limitScore), rdconv.Int64ToString(count))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	//return cmd.Val(), nil
	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldMsgRedisOpt) ZPopGTScoreWithScores(ctx context.Context, limitScore float64, count int64) (vals map[*server.Player]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "evalsha", svc_redis.ZPopMaxValueWithScore.Hash, "1", x.key, rdconv.Float64ToString(limitScore), rdconv.Int64ToString(count))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewZSliceCmd(ctx, "eval", svc_redis.ZPopMaxValueWithScore.Script, "1", x.key, rdconv.Float64ToString(limitScore), rdconv.Int64ToString(count))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	//return cmd.Val(), nil
	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldMsgRedisOpt) ZPopGTScoreWithScoresF(ctx context.Context, limitScore float64, count int64, f func(*server.Player, float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "evalsha", svc_redis.ZPopMaxValueWithScore.Hash, "1", x.key, rdconv.Float64ToString(limitScore), rdconv.Int64ToString(count))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewZSliceCmd(ctx, "eval", svc_redis.ZPopMaxValueWithScore.Script, "1", x.key, rdconv.Float64ToString(limitScore), rdconv.Int64ToString(count))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	//return cmd.Val(), nil
	return x.rangeZSliceCmd(cmd, f)
}
