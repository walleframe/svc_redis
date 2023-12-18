package rds

import (
	"context"
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/util/rdconv"
	"github.com/walleframe/walle/util/wtime"
)

type xZset2fieldStringInt64MatchRedisOpt struct {
	key string
	rds redis.UniversalClient
}

func init() {
	svc_redis.RegisterDBName(svc_redis.DBType, "rds.Zset2fieldStringInt64MatchRedisOpt")
}

func Zset2fieldStringInt64MatchRedisOpt(uid int64) *xZset2fieldStringInt64MatchRedisOpt {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteString("u:data")
	buf.WriteByte(':')
	buf.WriteInt64(uid)
	buf.WriteByte(':')
	buf.WriteInt64(wtime.DayStamp() + 8)
	return &xZset2fieldStringInt64MatchRedisOpt{
		key: buf.String(),
		rds: svc_redis.GetDBLink(svc_redis.DBType, "rds.Zset2fieldStringInt64MatchRedisOpt"),
	}
}

// With reset redis client
func (x *xZset2fieldStringInt64MatchRedisOpt) With(rds redis.UniversalClient) *xZset2fieldStringInt64MatchRedisOpt {
	x.rds = rds
	return x
}

func (x *xZset2fieldStringInt64MatchRedisOpt) Key() string {
	return x.key
}

////////////////////////////////////////////////////////////
// redis zset operation

func MergeZset2fieldStringInt64MatchRedisOptMember(x1 int32, x2 int8, arg1 string) string {
	buf := util.Builder{}
	buf.Grow(64)
	buf.WriteInt32(x1)
	buf.WriteByte(':')
	buf.WriteInt8(x2)
	buf.WriteByte(':')
	buf.WriteString(arg1)
	return buf.String()
}
func SplitZset2fieldStringInt64MatchRedisOptMember(val string) (x1 int32, x2 int8, arg1 string, err error) {
	items := strings.Split(val, ":")
	if len(items) != 3 {
		err = errors.New("invalid Zset2fieldStringInt64MatchRedisOpt mem value")
		return
	}
	x1, err = rdconv.StringToInt32(items[0])
	if err != nil {
		return
	}
	x2, err = rdconv.StringToInt8(items[1])
	if err != nil {
		return
	}
	arg1 = items[2]
	return
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZCard(ctx context.Context) (int64, error) {
	cmd := redis.NewIntCmd(ctx, "zcard", x.key)
	x.rds.Process(ctx, cmd)
	return cmd.Result()
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZAdd(ctx context.Context, x1 int32, x2 int8, arg1 string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZAddNX(ctx context.Context, x1 int32, x2 int8, arg1 string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "nx", MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZAddXX(ctx context.Context, x1 int32, x2 int8, arg1 string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "xx", MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZAddLT(ctx context.Context, x1 int32, x2 int8, arg1 string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "lt", MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZAddGT(ctx context.Context, x1 int32, x2 int8, arg1 string, score int64) error {
	cmd := redis.NewIntCmd(ctx, "zadd", x.key, "gt", MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1), rdconv.Int64ToString(score))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRem(ctx context.Context, x1 int32, x2 int8, arg1 string) error {
	cmd := redis.NewIntCmd(ctx, "zrem", x.key, MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1))
	return x.rds.Process(ctx, cmd)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZIncrBy(ctx context.Context, increment int64, x1 int32, x2 int8, arg1 string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zincrby", x.key, rdconv.Int64ToString(increment), MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int64(cmd.Val()), nil
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZScore(ctx context.Context, x1 int32, x2 int8, arg1 string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zscore", x.key, MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return int64(cmd.Val()), nil
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRank(ctx context.Context, x1 int32, x2 int8, arg1 string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zrank", x.key, MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRankWithScore(ctx context.Context, x1 int32, x2 int8, arg1 string) (rank int64, score int64, err error) {
	cmd := redis.NewRankWithScoreCmd(ctx, "zrank", x.key, MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1), "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = int64(cmd.Val().Score)
	return
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRevRank(ctx context.Context, x1 int32, x2 int8, arg1 string) (_ int64, err error) {
	cmd := redis.NewIntCmd(ctx, "zrevrank", x.key, MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	return cmd.Val(), nil
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRevRankWithScore(ctx context.Context, x1 int32, x2 int8, arg1 string) (rank int64, score int64, err error) {
	cmd := redis.NewRankWithScoreCmd(ctx, "zrevrank", x.key, MergeZset2fieldStringInt64MatchRedisOptMember(x1, x2, arg1), "withscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}
	rank = cmd.Val().Rank
	score = int64(cmd.Val().Score)
	return
}

func (x *xZset2fieldStringInt64MatchRedisOpt) rangeMemberSliceCmd(cmd *redis.StringSliceCmd, f func(x1 int32, x2 int8, arg1 string) bool) (err error) {
	var (
		x1   int32
		x2   int8
		arg1 string
	)
	for _, v := range cmd.Val() {
		x1, x2, arg1, err = SplitZset2fieldStringInt64MatchRedisOptMember(v)
		if err != nil {
			return
		}
		if !f(x1, x2, arg1) {
			return
		}
	}
	return
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRangeF(ctx context.Context, start, stop int64, f func(x1 int32, x2 int8, arg1 string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop))
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRangeByScoreF(ctx context.Context, start, stop int64, f func(x1 int32, x2 int8, arg1 string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRevRangeF(ctx context.Context, start, stop int64, f func(x1 int32, x2 int8, arg1 string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRevRangeByScoreF(ctx context.Context, start, stop int64, f func(x1 int32, x2 int8, arg1 string) bool) (err error) {
	cmd := redis.NewStringSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeMemberSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) rangeZSliceCmd(cmd *redis.ZSliceCmd, f func(x1 int32, x2 int8, arg1 string, score int64) bool) (err error) {
	var (
		x1   int32
		x2   int8
		arg1 string
	)
	for _, v := range cmd.Val() {
		x1, x2, arg1, err = SplitZset2fieldStringInt64MatchRedisOptMember(v.Member.(string))
		if err != nil {
			return
		}
		if !f(x1, x2, arg1, int64(v.Score)) {
			return
		}
	}
	return
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRangeWithScoresF(ctx context.Context, start, stop int64, f func(x1 int32, x2 int8, arg1 string, score int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRangeByScoreWithScoresF(ctx context.Context, start, stop int64, f func(x1 int32, x2 int8, arg1 string, score int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(start), rdconv.Int64ToString(stop), "byscore", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}
func (x *xZset2fieldStringInt64MatchRedisOpt) ZRevRangeWithScoresF(ctx context.Context, start, stop int64, f func(x1 int32, x2 int8, arg1 string, score int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZRevRangeByScoreWithScoresF(ctx context.Context, start, stop int64, f func(x1 int32, x2 int8, arg1 string, score int64) bool) (err error) {
	cmd := redis.NewZSliceCmd(ctx, "zrange", x.key, rdconv.Int64ToString(stop), rdconv.Int64ToString(start), "byscore", "rev", "withscores")
	err = x.rds.Process(ctx, cmd)
	if err != nil {
		return
	}

	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZPopMinF(ctx context.Context, count int64, f func(x1 int32, x2 int8, arg1 string, score int64) bool) (err error) {
	cmd := x.rds.ZPopMin(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZPopMaxF(ctx context.Context, count int64, f func(x1 int32, x2 int8, arg1 string, score int64) bool) (err error) {
	cmd := x.rds.ZPopMax(ctx, x.key, int64(count))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return x.rangeZSliceCmd(cmd, f)
}

func (x *xZset2fieldStringInt64MatchRedisOpt) ZPopGTScoreF(ctx context.Context, limitScore int64, count int64, f func(x1 int32, x2 int8, arg1 string) bool) (err error) {
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

func (x *xZset2fieldStringInt64MatchRedisOpt) ZPopGTScoreWithScoresF(ctx context.Context, limitScore int64, count int64, f func(x1 int32, x2 int8, arg1 string, score int64) bool) (err error) {
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
