package svc_redis

import (
	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis/rediscfg"
	"github.com/walleframe/walle/app/bootstrap"
)

var DBType string = "redis"
var service = &RedisCacheService{
	// 默认配置
	globalConfig: rediscfg.NewConfig("redis.global"),
}

func init() {
	// register redis service
	bootstrap.RegisterServiceByPriority(40, service)
}

// RegisterDBName register redis cache config
var RegisterDBName func (dbType string, dbName string) = service.RegisterDBName

// GetDBLink get redis cache link
var GetDBLink func(dbType string, dbName string) redis.UniversalClient = service.GetDBLink
