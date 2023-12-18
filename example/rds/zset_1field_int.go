package rds

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xZset1fieldIntRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Zset1fieldIntRedisOpt")
}

func Zset1fieldIntRedisOpt(uid int64) *xZset1fieldIntRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xZset1fieldIntRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Zset1fieldIntRedisOpt"),
	}
}

// With reset redis client
func (x *xZset1fieldIntRedisOpt) With(rds redis.UniversalClient) *xZset1fieldIntRedisOpt {
	x.rds = rds
	return x
}

func (x *xZset1fieldIntRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis zset operation

func (x *xZset1fieldIntRedisOpt) ZCard(ctx context.Context) (int64, error) {
	cmd := redis.NewIntCmd(ctx, "zcard", x.key)
	x.rds.Process(ctx, cmd)
	return cmd.Result()
}

func (x *xZset1fieldIntRedisOpt) ZAdd(ctx context.Context, mem int64, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, rdconv.Int64ToString(mem), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldIntRedisOpt) ZAddNX(ctx context.Context, mem int64, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "nx", rdconv.Int64ToString(mem), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldIntRedisOpt) ZAddXX(ctx context.Context, mem int64, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "xx", rdconv.Int64ToString(mem), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldIntRedisOpt) ZAddLT(ctx context.Context, mem int64, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "lt", rdconv.Int64ToString(mem), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldIntRedisOpt) ZAddGT(ctx context.Context, mem int64, score float64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "gt", rdconv.Int64ToString(mem), rdconv.Float64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldIntRedisOpt) ZAdds(ctx context.Context, vals map[int64]float64) error {
	args := make([]interface{}, 2, 2+len(vals)*2)
	args[0] = "zadd"
	args[1] = x.key
	for k, v := range vals {
		args = append(args, rdconv.Int64ToString(k))
		args = append(args, rdconv.Float64ToString(v))
	}
	cmd := redis.NewIntCmd(ctx, args...)
	return x.rds.Process(ctx, cmd)
}
func (x *xZset1fieldIntRedisOpt) ZRem(ctx context.Context, mem int64) error {
	cmd := redis.NewIntCmd(ctx, "zrem", x.key, rdconv.Int64ToString(mem))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset1fieldIntRedisOpt) ZIncrBy(ctx context.Context, increment float64, mem int64) (_ float64, err error) {
	cmd := redis.NewFloatCmd(ctx, "zincrby", x.key, rdconv.Float64ToString(increment), rdconv.Int64ToString(mem))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return float64(cmd.Val()), nil
}

func (x *xZset1fieldIntRedisOpt) ZScore(ctx context.Context, mem int64) (_ float64, err error) {
	cmd := redis.NewFloatCmd(ctx, "zscore", x.key, rdconv.Int64ToString(mem))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return float64(cmd.Val()), nil
}

func (x *xZset1fieldIntRedisOpt) ZRank(ctx context.Context, mem int64) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zrank", x.key, rdconv.Int64ToString(mem))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset1fieldIntRedisOpt) ZRankWithScore(ctx context.Context, mem int64) (rank int64, score float64, err error) {
	cmd := redis.NewRankWithScoreCmd(ctx, "zrank", x.key, rdconv.Int64ToString(mem), "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = float64(cmd.Val().Score)
	return
}

func (x *xZset1fieldIntRedisOpt) ZRevRank(ctx context.Context, mem int64) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zrevrank", x.key, rdconv.Int64ToString(mem))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset1fieldIntRedisOpt) ZRevRankWithScore(ctx context.Context, mem int64) (rank int64, score float64, err error) {
	cmd := redis.NewRankWithScoreCmd(ctx, "zrevrank", x.key, rdconv.Int64ToString(mem), "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = float64(cmd.Val().Score)
	return
}

func (x *xZset1fieldIntRedisOpt) parseMemberSliceCmd(cmd *redis.StringSliceCmd) (vals []int64, err error) {
	for _, v := range cmd.Val() {
		val, err := rdconv.StringToInt64(v)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return
}

func (x *xZset1fieldIntRedisOpt) ZRange(ctx context.Context, start, stop int64) (vals []int64, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset1fieldIntRedisOpt) ZRangeByScore(ctx context.Context, start, stop float64) (vals []int64, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset1fieldIntRedisOpt) ZRevRange(ctx context.Context, start, stop int64) (vals []int64, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset1fieldIntRedisOpt) ZRevRangeByScore(ctx context.Context, start, stop float64) (vals []int64, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseMemberSliceCmd(cmd)
}

func (x *xZset1fieldIntRedisOpt) rangeMemberSliceCmd(cmd *redis.StringSliceCmd, f func(mem int64) bool) (err error) {
	var mem int64

	for _, v := range cmd.Val() {
		mem, err = rdconv.StringToInt64(v)
		if err != nil {
			return
		}
		if !f(mem) {
			return
		}
	}
	return
}

func (x *xZset1fieldIntRedisOpt) ZRangeF(ctx context.Context, start, stop int64, f func(mem int64) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldIntRedisOpt) ZRangeByScoreF(ctx context.Context, start, stop float64, f func(mem int64) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldIntRedisOpt) ZRevRangeF(ctx context.Context, start, stop int64, f func(mem int64) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldIntRedisOpt) ZRevRangeByScoreF(ctx context.Context, start, stop float64, f func(mem int64) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset1fieldIntRedisOpt) parseZSliceCmd(cmd *redis.ZSliceCmd) (vals map[int64]float64, err error) {
	vals = make(map[int64]float64)
	for _, v := range cmd.Val() {
		val, err := rdconv.AnyToInt64(v.Member)
		if err != nil {
			return nil, err
		}
		vals[val] = float64(v.Score)
	}
	return
}

func (x *xZset1fieldIntRedisOpt) ZRangeWithScores(ctx context.Context, start, stop int64) (vals map[int64]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldIntRedisOpt) ZRangeByScoreWithScores(ctx context.Context, start, stop float64) (vals map[int64]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}
func (x *xZset1fieldIntRedisOpt) ZRevRangeWithScores(ctx context.Context, start, stop int64) (vals map[int64]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldIntRedisOpt) ZRevRangeByScoreWithScores(ctx context.Context, start, stop float64) (vals map[int64]float64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldIntRedisOpt) rangeZSliceCmd(cmd *redis.ZSliceCmd, f func(mem int64, score float64) bool) (err error) {
	var mem int64

	for _, v := range cmd.Val() {
		mem, err = rdconv.AnyToInt64(v.Member)
		if err != nil {
			return
		}
		if !f(mem, float64(v.Score)) {
			return
		}
	}
	return
}

func (x *xZset1fieldIntRedisOpt) ZRangeWithScoresF(ctx context.Context, start, stop int64, f func(mem int64, score float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldIntRedisOpt) ZRangeByScoreWithScoresF(ctx context.Context, start, stop float64, f func(mem int64, score float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(start), rdconv.Float64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}
func (x *xZset1fieldIntRedisOpt) ZRevRangeWithScoresF(ctx context.Context, start, stop int64, f func(mem int64, score float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldIntRedisOpt) ZRevRangeByScoreWithScoresF(ctx context.Context, start, stop float64, f func(mem int64, score float64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Float64ToString(stop), rdconv.Float64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldIntRedisOpt) ZPopMin(ctx context.Context, count int64) (_ map[int64]float64, err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldIntRedisOpt) ZPopMax(ctx context.Context, count int64) (_ map[int64]float64, err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset1fieldIntRedisOpt) ZPopMinF(ctx context.Context, count int64, f func(mem int64, score float64) bool) (err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldIntRedisOpt) ZPopMaxF(ctx context.Context, count int64, f func(mem int64, score float64) bool) (err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset1fieldIntRedisOpt) ZPopGTScore(ctx context.Context, limitScore float64, count int64) (vals []int64, err error) {
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

func (x *xZset1fieldIntRedisOpt) ZPopGTScoreF(ctx context.Context, limitScore float64, count int64, f func(mem int64) bool) (err error) {
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

func (x *xZset1fieldIntRedisOpt) ZPopGTScoreWithScores(ctx context.Context, limitScore float64, count int64) (vals map[int64]float64, err error) {
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

func (x *xZset1fieldIntRedisOpt) ZPopGTScoreWithScoresF(ctx context.Context, limitScore float64, count int64, f func(mem int64, score float64) bool) (err error) {
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
