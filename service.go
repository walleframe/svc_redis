package svc_redis

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/walleframe/svc_redis/rediscfg"
	"github.com/walleframe/walle/app"
)

var (
	_ app.Service = (*RedisCacheService)(nil)
)

type RedisCacheService struct {
	globalConfig *rediscfg.Config
	globalLink   redis.UniversalClient
}

func (svc *RedisCacheService) Name() string {
	return "redis-cache-service"
}

func (svc *RedisCacheService) Init(s app.Stoper) (err error) {
	// new redis link
	svc.globalLink = rediscfg.NewRedisClient(svc.globalConfig)
	// check redis link
	err = svc.globalLink.Ping(context.Background()).Err()
	if err != nil {
		return err
	}
	// register update config
	svc.globalConfig.AddNotifyFunc(service.updateConfigs)
	return
}

func (svc *RedisCacheService) Start(s app.Stoper) (err error) {
	return
}

func (svc *RedisCacheService) Stop() {
}

func (svc *RedisCacheService) Finish() {
}

// RegisterDBName register redis cache config
func (svc *RedisCacheService) RegisterDBName(dbType string, dbName string) {
	// TODO: enable to config individual configuration of parameters for each link
}

func (svc *RedisCacheService) GetDBLink(dbType string, dbName string) redis.UniversalClient {
	// TODO: enable to config individual configuration of parameters for each link
	return svc.globalLink
}

func (svc *RedisCacheService) updateConfigs(cfg *rediscfg.Config) {
	// svc.globalLink = rediscfg.NewRedisClient(cfg)
}
