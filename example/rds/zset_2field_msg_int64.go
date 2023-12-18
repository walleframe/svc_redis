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

type xZset2fieldMsgInt64RedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Zset2fieldMsgInt64RedisOpt")
}

func Zset2fieldMsgInt64RedisOpt(uid int64) *xZset2fieldMsgInt64RedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xZset2fieldMsgInt64RedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Zset2fieldMsgInt64RedisOpt"),
	}
}

// With reset redis client
func (x *xZset2fieldMsgInt64RedisOpt) With(rds redis.UniversalClient) *xZset2fieldMsgInt64RedisOpt {
	x.rds = rds
	return x
}

func (x *xZset2fieldMsgInt64RedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis zset operation

func (x *xZset2fieldMsgInt64RedisOpt) ZCard(ctx context.Context) (int64, error) {
	cmd := redis.NewIntCmd(ctx, "zcard", x.key)
	x.rds.Process(ctx, cmd)
	return cmd.Result()
}

func (x *xZset2fieldMsgInt64RedisOpt) ZAdd(ctx context.Context, mem *server.Player, score int64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, util.BytesToString(data), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZAddNX(ctx context.Context, mem *server.Player, score int64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "nx", util.BytesToString(data), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZAddXX(ctx context.Context, mem *server.Player, score int64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "xx", util.BytesToString(data), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZAddLT(ctx context.Context, mem *server.Player, score int64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "lt", util.BytesToString(data), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZAddGT(ctx context.Context, mem *server.Player, score int64) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "gt", util.BytesToString(data), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZAdds(ctx context.Context, vals map[*server.Player]int64) error {
	args := make([]interface{}, 2, 2+len(vals)*2)
	args[0] = "zadd"
	args[1] = x.key
	for k, v := range vals {
		data, err := k.MarshalObject()
		if err != nil {
			return err
		}
		args = append(args, util.BytesToString(data))
		args = append(args, rdconv.Int64ToString(v))
	}
	cmd := redis.NewIntCmd(ctx, args...)
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRem(ctx context.Context, mem *server.Player) error {
	data, err := mem.MarshalObject()
	if err != nil {
		return err
	}
	cmd := redis.NewIntCmd(ctx, "zrem", x.key, util.BytesToString(data))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZIncrBy(ctx context.Context, increment int64, mem *server.Player) (_ int64, err error) {
	data, err := mem.MarshalObject()
	if err != nil {
		return
	}
	cmd := redis.NewIntCmd(ctx, "zincrby", x.key, rdconv.Int64ToString(increment), util.BytesToString(data))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int64(cmd.Val()), nil
}

func (x *xZset2fieldMsgInt64RedisOpt) ZScore(ctx context.Context, mem *server.Player) (_ int64, err error) {
	data, err := mem.MarshalObject()
	if err != nil {
		return
	}
	cmd := redis.NewIntCmd(ctx, "zscore", x.key, util.BytesToString(data))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int64(cmd.Val()), nil
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRank(ctx context.Context, mem *server.Player) (_ int64, err error) {
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

func (x *xZset2fieldMsgInt64RedisOpt) ZRankWithScore(ctx context.Context, mem *server.Player) (rank int64, score int64, err error) {
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
	score = int64(cmd.Val().Score)
	return
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRevRank(ctx context.Context, mem *server.Player) (_ int64, err error) {
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

func (x *xZset2fieldMsgInt64RedisOpt) ZRevRankWithScore(ctx context.Context, mem *server.Player) (rank int64, score int64, err error) {
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
	score = int64(cmd.Val().Score)
	return
}

func (x *xZset2fieldMsgInt64RedisOpt) parseMemberSliceCmd(cmd *redis.StringSliceCmd) (vals []*server.Player, err error) {
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

func (x *xZset2fieldMsgInt64RedisOpt) ZRange(ctx context.Context, start, stop int64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRangeByScore(ctx context.Context, start, stop int64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRevRange(ctx context.Context, start, stop int64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRevRangeByScore(ctx context.Context, start, stop int64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) rangeMemberSliceCmd(cmd *redis.StringSliceCmd, f func(*server.Player) bool) (err error) {
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

func (x *xZset2fieldMsgInt64RedisOpt) ZRangeF(ctx context.Context, start, stop int64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRangeByScoreF(ctx context.Context, start, stop int64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRevRangeF(ctx context.Context, start, stop int64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRevRangeByScoreF(ctx context.Context, start, stop int64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) parseZSliceCmd(cmd *redis.ZSliceCmd) (vals map[*server.Player]int64, err error) {
	vals = make(map[*server.Player]int64)
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
		vals[val] = int64(v.Score)
	}
	return
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRangeWithScores(ctx context.Context, start, stop int64) (vals map[*server.Player]int64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRangeByScoreWithScores(ctx context.Context, start, stop int64) (vals map[*server.Player]int64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}
func (x *xZset2fieldMsgInt64RedisOpt) ZRevRangeWithScores(ctx context.Context, start, stop int64) (vals map[*server.Player]int64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRevRangeByScoreWithScores(ctx context.Context, start, stop int64) (vals map[*server.Player]int64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) rangeZSliceCmd(cmd *redis.ZSliceCmd, f func(*server.Player, int64) bool) (err error) {
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
		if !f(val, int64(v.Score)) {
			return nil
		}
	}
	return
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRangeWithScoresF(ctx context.Context, start, stop int64, f func(*server.Player, int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRangeByScoreWithScoresF(ctx context.Context, start, stop int64, f func(*server.Player, int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}
func (x *xZset2fieldMsgInt64RedisOpt) ZRevRangeWithScoresF(ctx context.Context, start, stop int64, f func(*server.Player, int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZRevRangeByScoreWithScoresF(ctx context.Context, start, stop int64, f func(*server.Player, int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZPopMin(ctx context.Context, count int64) (_ map[*server.Player]int64, err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZPopMinF(ctx context.Context, count int64, f func(*server.Player, int64) bool) (err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZPopMax(ctx context.Context, count int64) (_ map[*server.Player]int64, err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZPopMaxF(ctx context.Context, count int64, f func(*server.Player, int64) bool) (err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZPopGTScore(ctx context.Context, limitScore int64, count int64) (vals []*server.Player, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "evalsha", svc_redis.ZPopMaxValue.Hash, "1", x.key, rdconv.Int64ToString(limitScore), rdconv.Int64ToString(count))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewStringSliceCmd(ctx, "eval", svc_redis.ZPopMaxValue.Script, "1", x.key, rdconv.Int64ToString(limitScore), rdconv.Int64ToString(count))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZPopGTScoreF(ctx context.Context, limitScore int64, count int64, f func(*server.Player) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "evalsha", svc_redis.ZPopMaxValue.Hash, "1", x.key, rdconv.Int64ToString(limitScore), rdconv.Int64ToString(count))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewStringSliceCmd(ctx, "eval", svc_redis.ZPopMaxValue.Script, "1", x.key, rdconv.Int64ToString(limitScore), rdconv.Int64ToString(count))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	//return cmd.Val(), nil
	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZPopGTScoreWithScores(ctx context.Context, limitScore int64, count int64) (vals map[*server.Player]int64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "evalsha", svc_redis.ZPopMaxValueWithScore.Hash, "1", x.key, rdconv.Int64ToString(limitScore), rdconv.Int64ToString(count))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewZSliceCmd(ctx, "eval", svc_redis.ZPopMaxValueWithScore.Script, "1", x.key, rdconv.Int64ToString(limitScore), rdconv.Int64ToString(count))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	//return cmd.Val(), nil
	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldMsgInt64RedisOpt) ZPopGTScoreWithScoresF(ctx context.Context, limitScore int64, count int64, f func(*server.Player, int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "evalsha", svc_redis.ZPopMaxValueWithScore.Hash, "1", x.key, rdconv.Int64ToString(limitScore), rdconv.Int64ToString(count))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		if !redis.HasErrorPrefix(err, "NOSCRIPT") {
			return
		}
		cmd = redis.NewZSliceCmd(ctx, "eval", svc_redis.ZPopMaxValueWithScore.Script, "1", x.key, rdconv.Int64ToString(limitScore), rdconv.Int64ToString(count))
		err = x.rds.Process(ctx, cmd)
		if err != nil {
			return
		}
	}
	//return cmd.Val(), nil
	return x.rangeZSliceCmd(cmd, f)
}
