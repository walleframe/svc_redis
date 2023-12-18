package rds

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xZset1fieldStringRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Zset1fieldStringRedisOpt")
}

func Zset1fieldStringRedisOpt(uid int64) *xZset1fieldStringRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xZset1fieldStringRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Zset1fieldStringRedisOpt"),
	}
}

// With reset redis client
func (x *xZset1fieldStringRedisOpt) With(rds redis.UniversalClient) *xZset1fieldStringRedisOpt {
	x.rds = rds
	return x
}

func (x *xZset1fieldStringRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis zset operation

func (x *xZset1fieldStringRedisOpt) ZCard(ctx context.Context) (int64, error) {
	cmd := redis.NewIntCmd(ctx, "zcard", x.key)
	x.rds.Process(ctx, cmd)
	return cmd.Result()
}

func (x *xZset1fieldStringRedisOpt) ZAdd(ctx context.Context, mem string, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, mem, rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldStringRedisOpt) ZAddNX(ctx context.Context, mem string, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "nx", mem, rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldStringRedisOpt) ZAddXX(ctx context.Context, mem string, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "xx", mem, rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldStringRedisOpt) ZAddLT(ctx context.Context, mem string, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "lt", mem, rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldStringRedisOpt) ZAddGT(ctx context.Context, mem string, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "gt", mem, rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldStringRedisOpt) ZAdds(ctx context.Context, vals map[string]float64) error {
	args := make([]interface{}, 2, 2+len(vals)*2)
	args[0] = "zadd"
	args[1] = x.key
	for k, v := range vals {
		args = append(args, k)
		args = append(args, rdconv.Float64ToString(v))
	}
	cmd := redis.NewIntCmd(ctx, args...)
	return x.rds.Process(ctx, cmd)
}
func (x *xZset1fieldStringRedisOpt) ZRem(ctx context.Context, mem string) error {
	cmd := redis.NewIntCmd(ctx, "zrem", x.key, mem)
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldStringRedisOpt) ZIncrBy(ctx context.Context, increment float64, mem string) (_ float64, err error) {
	cmd := redis.NewFloatCmd(ctx, "zincrby", x.key, rdconv.Float64ToString(increment), mem)
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return float64(cmd.Val()), nil
}

func (x *xZset1fieldStringRedisOpt) ZScore(ctx context.Context, mem string) (_ float64, err error) {
	cmd := redis.NewFloatCmd(ctx, "zscore", x.key, mem)
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return float64(cmd.Val()), nil
}

func (x *xZset1fieldStringRedisOpt) ZRank(ctx context.Context, mem string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zrank", x.key, mem)
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset1fieldStringRedisOpt) ZRankWithScore(ctx context.Context, mem string) (rank int64, score float64, err error) {
	cmd := redis.NewRankWithScoreCmd(ctx, "zrank", x.key, mem, "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = float64(cmd.Val().Score)
	return
}

func (x *xZset1fieldStringRedisOpt) ZRevRank(ctx context.Context, mem string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zrevrank", x.key, mem)
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset1fieldStringRedisOpt) ZRevRankWithScore(ctx context.Context, mem string) (rank int64, score float64, err error) {
	cmd := redis.NewRankWithScoreCmd(ctx, "zrevrank", x.key, mem, "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = float64(cmd.Val().Score)
	return
}

func (x *xZset1fieldStringRedisOpt) ZRange(ctx context.Context, start, stop int64) (vals []string, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return cmd.Result()
}

func (x *xZset1fieldStringRedisOpt) ZRangeByScore(ctx context.Context, start, stop float64) (vals []string, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return cmd.Result()
}

func (x *xZset1fieldStringRedisOpt) ZRevRange(ctx context.Context, start, stop int64) (vals []string, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return cmd.Result()
}

func (x *xZset1fieldStringRedisOpt) ZRevRangeByScore(ctx context.Context, start, stop float64) (vals []string, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return cmd.Result()
}

func (x *xZset1fieldStringRedisOpt) rangeMemberSliceCmd(cmd *redis.StringSliceCmd, f func(mem string) bool) (err error) {

	for _, v := range cmd.Val() {
		if !f(v) {
			return
		}
	}
	return
}

func (x *xZset1fieldStringRedisOpt) ZRangeF(ctx context.Context, start, stop int64, f func(mem string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldStringRedisOpt) ZRangeByScoreF(ctx context.Context, start, stop float64, f func(mem string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldStringRedisOpt) ZRevRangeF(ctx context.Context, start, stop int64, f func(mem string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldStringRedisOpt) ZRevRangeByScoreF(ctx context.Context, start, stop float64, f func(mem string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldStringRedisOpt) parseZSliceCmd(cmd *redis.ZSliceCmd) (vals map[string]float64, err error) {
	vals = make(map[string]float64)
	for _, v := range cmd.Val() {
		val, err := rdconv.AnyToString(v.Member)
		if err != nil {
			return nil, err
		}
		vals[val] = float64(v.Score)
	}
	return
}

func (x *xZset1fieldStringRedisOpt) ZRangeWithScores(ctx context.Context, start, stop int64) (vals map[string]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldStringRedisOpt) ZRangeByScoreWithScores(ctx context.Context, start, stop float64) (vals map[string]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}
func (x *xZset1fieldStringRedisOpt) ZRevRangeWithScores(ctx context.Context, start, stop int64) (vals map[string]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldStringRedisOpt) ZRevRangeByScoreWithScores(ctx context.Context, start, stop float64) (vals map[string]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldStringRedisOpt) rangeZSliceCmd(cmd *redis.ZSliceCmd, f func(mem string, score float64) bool) (err error) {

	for _, v := range cmd.Val() {
		if !f(v.Member.(string), float64(v.Score)) {
			return
		}
	}
	return
}

func (x *xZset1fieldStringRedisOpt) ZRangeWithScoresF(ctx context.Context, start, stop int64, f func(mem string, score float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldStringRedisOpt) ZRangeByScoreWithScoresF(ctx context.Context, start, stop float64, f func(mem string, score float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}
func (x *xZset1fieldStringRedisOpt) ZRevRangeWithScoresF(ctx context.Context, start, stop int64, f func(mem string, score float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldStringRedisOpt) ZRevRangeByScoreWithScoresF(ctx context.Context, start, stop float64, f func(mem string, score float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldStringRedisOpt) ZPopMin(ctx context.Context, count int64) (_ map[string]float64, err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldStringRedisOpt) ZPopMax(ctx context.Context, count int64) (_ map[string]float64, err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldStringRedisOpt) ZPopMinF(ctx context.Context, count int64, f func(mem string, score float64) bool) (err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldStringRedisOpt) ZPopMaxF(ctx context.Context, count int64, f func(mem string, score float64) bool) (err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldStringRedisOpt) ZPopGTScore(ctx context.Context, limitScore float64, count int64) (vals []string, err error) {
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
	return cmd.Result()
}

func (x *xZset1fieldStringRedisOpt) ZPopGTScoreF(ctx context.Context, limitScore float64, count int64, f func(mem string) bool) (err error) {
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

func (x *xZset1fieldStringRedisOpt) ZPopGTScoreWithScores(ctx context.Context, limitScore float64, count int64) (vals map[string]float64, err error) {
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

func (x *xZset1fieldStringRedisOpt) ZPopGTScoreWithScoresF(ctx context.Context, limitScore float64, count int64, f func(mem string, score float64) bool) (err error) {
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
	return x.rangeZSliceCmd(cmd, f)
}
