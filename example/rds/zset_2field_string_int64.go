package rds

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xZset2fieldStringInt64RedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Zset2fieldStringInt64RedisOpt")
}

func Zset2fieldStringInt64RedisOpt(uid int64) *xZset2fieldStringInt64RedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xZset2fieldStringInt64RedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Zset2fieldStringInt64RedisOpt"),
	}
}

// With reset redis client
func (x *xZset2fieldStringInt64RedisOpt) With(rds redis.UniversalClient) *xZset2fieldStringInt64RedisOpt {
	x.rds = rds
	return x
}

func (x *xZset2fieldStringInt64RedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis zset operation

func (x *xZset2fieldStringInt64RedisOpt) ZCard(ctx context.Context) (int64, error) {
	cmd := redis.NewIntCmd(ctx, "zcard", x.key)
	x.rds.Process(ctx, cmd)
	return cmd.Result()
}

func (x *xZset2fieldStringInt64RedisOpt) ZAdd(ctx context.Context, mem string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, mem, rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZAddNX(ctx context.Context, mem string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "nx", mem, rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZAddXX(ctx context.Context, mem string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "xx", mem, rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZAddLT(ctx context.Context, mem string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "lt", mem, rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZAddGT(ctx context.Context, mem string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "gt", mem, rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZAdds(ctx context.Context, vals map[string]int64) error {
	args := make([]interface{}, 2, 2+len(vals)*2)
	args[0] = "zadd"
	args[1] = x.key
	for k, v := range vals {
		args = append(args, k)
		args = append(args, rdconv.Int64ToString(v))
	}
	cmd := redis.NewIntCmd(ctx, args...)
	return x.rds.Process(ctx, cmd)
}
func (x *xZset2fieldStringInt64RedisOpt) ZRem(ctx context.Context, mem string) error {
	cmd := redis.NewIntCmd(ctx, "zrem", x.key, mem)
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZIncrBy(ctx context.Context, increment int64, mem string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zincrby", x.key, rdconv.Int64ToString(increment), mem)
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int64(cmd.Val()), nil
}

func (x *xZset2fieldStringInt64RedisOpt) ZScore(ctx context.Context, mem string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zscore", x.key, mem)
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int64(cmd.Val()), nil
}

func (x *xZset2fieldStringInt64RedisOpt) ZRank(ctx context.Context, mem string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zrank", x.key, mem)
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset2fieldStringInt64RedisOpt) ZRankWithScore(ctx context.Context, mem string) (rank int64, score int64, err error) {
	cmd := redis.NewRankWithScoreCmd(ctx, "zrank", x.key, mem, "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = int64(cmd.Val().Score)
	return
}

func (x *xZset2fieldStringInt64RedisOpt) ZRevRank(ctx context.Context, mem string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zrevrank", x.key, mem)
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset2fieldStringInt64RedisOpt) ZRevRankWithScore(ctx context.Context, mem string) (rank int64, score int64, err error) {
	cmd := redis.NewRankWithScoreCmd(ctx, "zrevrank", x.key, mem, "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = int64(cmd.Val().Score)
	return
}

func (x *xZset2fieldStringInt64RedisOpt) ZRange(ctx context.Context, start, stop int64) (vals []string, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return cmd.Result()
}

func (x *xZset2fieldStringInt64RedisOpt) ZRangeByScore(ctx context.Context, start, stop int64) (vals []string, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return cmd.Result()
}

func (x *xZset2fieldStringInt64RedisOpt) ZRevRange(ctx context.Context, start, stop int64) (vals []string, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return cmd.Result()
}

func (x *xZset2fieldStringInt64RedisOpt) ZRevRangeByScore(ctx context.Context, start, stop int64) (vals []string, err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return cmd.Result()
}

func (x *xZset2fieldStringInt64RedisOpt) rangeMemberSliceCmd(cmd *redis.StringSliceCmd, f func(mem string) bool) (err error) {

	for _, v := range cmd.Val() {
		if !f(v) {
			return
		}
	}
	return
}

func (x *xZset2fieldStringInt64RedisOpt) ZRangeF(ctx context.Context, start, stop int64, f func(mem string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64RedisOpt) ZRangeByScoreF(ctx context.Context, start, stop int64, f func(mem string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64RedisOpt) ZRevRangeF(ctx context.Context, start, stop int64, f func(mem string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64RedisOpt) ZRevRangeByScoreF(ctx context.Context, start, stop int64, f func(mem string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64RedisOpt) parseZSliceCmd(cmd *redis.ZSliceCmd) (vals map[string]int64, err error) {
	vals = make(map[string]int64)
	for _, v := range cmd.Val() {
		val, err := rdconv.AnyToString(v.Member)
		if err != nil {
			return nil, err
		}
		vals[val] = int64(v.Score)
	}
	return
}

func (x *xZset2fieldStringInt64RedisOpt) ZRangeWithScores(ctx context.Context, start, stop int64) (vals map[string]int64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZRangeByScoreWithScores(ctx context.Context, start, stop int64) (vals map[string]int64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}
func (x *xZset2fieldStringInt64RedisOpt) ZRevRangeWithScores(ctx context.Context, start, stop int64) (vals map[string]int64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZRevRangeByScoreWithScores(ctx context.Context, start, stop int64) (vals map[string]int64, err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) rangeZSliceCmd(cmd *redis.ZSliceCmd, f func(mem string, score int64) bool) (err error) {

	for _, v := range cmd.Val() {
		if !f(v.Member.(string), int64(v.Score)) {
			return
		}
	}
	return
}

func (x *xZset2fieldStringInt64RedisOpt) ZRangeWithScoresF(ctx context.Context, start, stop int64, f func(mem string, score int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64RedisOpt) ZRangeByScoreWithScoresF(ctx context.Context, start, stop int64, f func(mem string, score int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}
func (x *xZset2fieldStringInt64RedisOpt) ZRevRangeWithScoresF(ctx context.Context, start, stop int64, f func(mem string, score int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64RedisOpt) ZRevRangeByScoreWithScoresF(ctx context.Context, start, stop int64, f func(mem string, score int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64RedisOpt) ZPopMin(ctx context.Context, count int64) (_ map[string]int64, err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZPopMax(ctx context.Context, count int64) (_ map[string]int64, err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return x.parseZSliceCmd(cmd)
}

func (x *xZset2fieldStringInt64RedisOpt) ZPopMinF(ctx context.Context, count int64, f func(mem string, score int64) bool) (err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64RedisOpt) ZPopMaxF(ctx context.Context, count int64, f func(mem string, score int64) bool) (err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64RedisOpt) ZPopGTScore(ctx context.Context, limitScore int64, count int64) (vals []string, err error) {
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
	return cmd.Result()
}

func (x *xZset2fieldStringInt64RedisOpt) ZPopGTScoreF(ctx context.Context, limitScore int64, count int64, f func(mem string) bool) (err error) {
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

func (x *xZset2fieldStringInt64RedisOpt) ZPopGTScoreWithScores(ctx context.Context, limitScore int64, count int64) (vals map[string]int64, err error) {
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

func (x *xZset2fieldStringInt64RedisOpt) ZPopGTScoreWithScoresF(ctx context.Context, limitScore int64, count int64, f func(mem string, score int64) bool) (err error) {
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
	return x.rangeZSliceCmd(cmd, f)
}
