package svc_redis

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
)

func RedisScriptSHA(src string) string {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	return hex.EncodeToString(h.Sum(nil))
}

type Script struct {
	Script string
	Hash   string
}

func NewScript(src string) *Script {
	return &Script{
		Script: src,
		Hash:   RedisScriptSHA(src),
	}
}

var (
	// ZPopMaxValue pop values of more then score
	ZPopMaxValue = NewScript(`local v = redis.call('ZREVRANGEBYSCORE',KEYS[1],'+inf','-inf','limit',0,ARGV[1]) if #v > 0 then redis.call('ZREM',KEYS[1], unpack(v)) end return v`)
	// ZPopMaxValueWithScore pop values of more then score withscores
	ZPopMaxValueWithScore = NewScript(`local v = redis.call('zrange',KEYS[1],'+inf',ARGV[1],'byscore','rev','limit',0,ARGV[2], 'withscores') for k = 1,#v,2 do redis.call('ZREM',KEYS[1], v[k]) end return v`)
	// LockerScriptUnlock redis locker unlock script
	LockerScriptUnlock = NewScript(`if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`)
)
