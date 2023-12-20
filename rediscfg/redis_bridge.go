package rediscfg

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// redis config
//
//go:generate gogen cfggen -n Config -o gen_configs.go
func generateRedisConfig() interface{} {
	return map[string]interface{}{
		"Name": "",
		// redis addrs
		"Addrs":           []string{"127.0.0.1:6379"},
		"DB":              0,  // db index
		"Username":        "", // user name
		"Password":        "", // password
		"MaxRetries":      0,
		"MinRetryBackoff": time.Duration(0),
		"MaxRetryBackoff": time.Duration(0),
		// dail timeout
		"ConnDialTimeout": time.Duration(time.Second * 5),
		// read timeout
		"SocketReadTimeout": time.Duration(time.Second * 3),
		// write timeout
		"SocketWriteTimeout":    time.Duration(time.Second * 3),
		"ContextTimeoutEnabled": false,            // context time enable
		"PoolFIFO":              false,            // pool fifo, lifo default
		"ConnPoolSize":          0,                // connection pool size
		"PoolTimeout":           time.Duration(0), // pool timeout
		"MinIdleConns":          0,                // min idel count
		"MaxIdleConns":          0,                // max idle conn count
		"MaxActiveConns":        0,                // max active connection count
		"ConnMaxIdleTime":       time.Duration(0), // max idel time
		"ConnMaxLifetime":       time.Duration(0), // max life time
		"DisableIndentity":      false,

		"SentinelUsername":   "", // sentinel username
		"SentinelPassword":   "", // sentinel password
		"SentinelMasterName": "", // sentinel master name

		"Cluster":               false, // enable cluster mode
		"ClusterReadOnly":       false, // cluster read only
		"ClusterRouteRandomly":  false, // cluster route random
		"ClusterRouteByLatency": false, // cluster route by latency
	}
}

func NewRedisClient(cfg *Config) (cli redis.UniversalClient) {
	var opts = &redis.UniversalOptions{
		Addrs:                 cfg.Addrs,
		DB:                    cfg.DB,
		Username:              cfg.Username,
		Password:              cfg.Password,
		SentinelUsername:      cfg.SentinelUsername,
		SentinelPassword:      cfg.SentinelPassword,
		MaxRetries:            cfg.MaxRetries,
		MinRetryBackoff:       cfg.MinRetryBackoff,
		MaxRetryBackoff:       cfg.MaxRetryBackoff,
		DialTimeout:           cfg.ConnDialTimeout,
		ReadTimeout:           cfg.SocketReadTimeout,
		WriteTimeout:          cfg.SocketWriteTimeout,
		ContextTimeoutEnabled: cfg.ContextTimeoutEnabled,
		PoolFIFO:              cfg.PoolFIFO,
		PoolSize:              cfg.ConnPoolSize,
		PoolTimeout:           cfg.PoolTimeout,
		MinIdleConns:          cfg.MinIdleConns,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxActiveConns:        cfg.MaxActiveConns,
		ConnMaxIdleTime:       cfg.ConnMaxIdleTime,
		ConnMaxLifetime:       cfg.ConnMaxLifetime,
		ReadOnly:              cfg.ClusterReadOnly,
		RouteRandomly:         cfg.ClusterRouteRandomly,
		RouteByLatency:        cfg.ClusterRouteByLatency,
		MasterName:            cfg.SentinelMasterName,
		DisableIndentity:      cfg.DisableIndentity,
	}
	if cfg.Cluster {
		return redis.NewClusterClient(opts.Cluster())
	} else {
		return redis.NewUniversalClient(opts)
	}
}
