// Code generated by "gogen cfggen"; DO NOT EDIT.
// Exec: gogen cfggen -n Config -o gen_configs.go Version: 0.0.1
package rediscfg

import (
	"time"

	"github.com/walleframe/walle/services/configcentra"
)

var _ = generateRedisConfig()

// Config config generate by gogen cfggen.
type Config struct {
	Name string `json:"name,omitempty"`
	// redis addrs
	Addrs []string `json:"addrs,omitempty"`
	// db index
	DB int `json:"db,omitempty"`
	// user name
	Username string `json:"username,omitempty"`
	// password
	Password        string        `json:"password,omitempty"`
	MaxRetries      int           `json:"maxretries,omitempty"`
	MinRetryBackoff time.Duration `json:"minretrybackoff,omitempty"`
	MaxRetryBackoff time.Duration `json:"maxretrybackoff,omitempty"`
	// dail timeout
	ConnDialTimeout time.Duration `json:"conndialtimeout,omitempty"`
	// read timeout
	SocketReadTimeout time.Duration `json:"socketreadtimeout,omitempty"`
	// write timeout
	SocketWriteTimeout time.Duration `json:"socketwritetimeout,omitempty"`
	// context time enable
	ContextTimeoutEnabled bool `json:"contexttimeoutenabled,omitempty"`
	// pool fifo, lifo default
	PoolFIFO bool `json:"poolfifo,omitempty"`
	// connection pool size
	ConnPoolSize int `json:"connpoolsize,omitempty"`
	// pool timeout
	PoolTimeout time.Duration `json:"pooltimeout,omitempty"`
	// min idel count
	MinIdleConns int `json:"minidleconns,omitempty"`
	// max idle conn count
	MaxIdleConns int `json:"maxidleconns,omitempty"`
	// max active connection count
	MaxActiveConns int `json:"maxactiveconns,omitempty"`
	// max idel time
	ConnMaxIdleTime time.Duration `json:"connmaxidletime,omitempty"`
	// max life time
	ConnMaxLifetime  time.Duration `json:"connmaxlifetime,omitempty"`
	DisableIndentity bool          `json:"disableindentity,omitempty"`
	// sentinel username
	SentinelUsername string `json:"sentinelusername,omitempty"`
	// sentinel password
	SentinelPassword string `json:"sentinelpassword,omitempty"`
	// sentinel master name
	SentinelMasterName string `json:"sentinelmastername,omitempty"`
	// enable cluster mode
	Cluster bool `json:"cluster,omitempty"`
	// cluster read only
	ClusterReadOnly bool `json:"clusterreadonly,omitempty"`
	// cluster route random
	ClusterRouteRandomly bool `json:"clusterrouterandomly,omitempty"`
	// cluster route by latency
	ClusterRouteByLatency bool `json:"clusterroutebylatency,omitempty"`
	// config prefix string
	prefix string
	// update ntf funcs
	ntfFuncs []func(*Config)
}

var _ configcentra.ConfigValue = (*Config)(nil)

func NewConfig(prefix string) *Config {
	if prefix == "" {
		panic("config prefix invalid")
	}
	// new default config value
	cfg := NewDefaultConfig(prefix)
	// register value to config centra
	configcentra.RegisterConfig(cfg)
	return cfg
}

func NewDefaultConfig(prefix string) *Config {
	cfg := &Config{
		Name:                  "",
		Addrs:                 []string{"127.0.0.1:6379"},
		DB:                    0,
		Username:              "",
		Password:              "",
		MaxRetries:            0,
		MinRetryBackoff:       0,
		MaxRetryBackoff:       0,
		ConnDialTimeout:       time.Second * 5,
		SocketReadTimeout:     time.Second * 3,
		SocketWriteTimeout:    time.Second * 3,
		ContextTimeoutEnabled: false,
		PoolFIFO:              false,
		ConnPoolSize:          0,
		PoolTimeout:           0,
		MinIdleConns:          0,
		MaxIdleConns:          0,
		MaxActiveConns:        0,
		ConnMaxIdleTime:       0,
		ConnMaxLifetime:       0,
		DisableIndentity:      false,
		SentinelUsername:      "",
		SentinelPassword:      "",
		SentinelMasterName:    "",
		Cluster:               false,
		ClusterReadOnly:       false,
		ClusterRouteRandomly:  false,
		ClusterRouteByLatency: false,
		prefix:                prefix,
	}
	return cfg
}

// add notify func
func (cfg *Config) AddNotifyFunc(f func(*Config)) {
	cfg.ntfFuncs = append(cfg.ntfFuncs, f)
}

// impl configcentra.ConfigValue
func (cfg *Config) SetDefaultValue(cc configcentra.ConfigCentra) {
	if cc.UseObject() {
		cc.SetObject(cfg.prefix, "redis config", cfg)
		return
	}
	cc.SetDefault(cfg.prefix+".name", "", cfg.Name)
	cc.SetDefault(cfg.prefix+".addrs", "redis addrs", cfg.Addrs)
	cc.SetDefault(cfg.prefix+".db", "db index", cfg.DB)
	cc.SetDefault(cfg.prefix+".username", "user name", cfg.Username)
	cc.SetDefault(cfg.prefix+".password", "password", cfg.Password)
	cc.SetDefault(cfg.prefix+".maxretries", "", cfg.MaxRetries)
	cc.SetDefault(cfg.prefix+".minretrybackoff", "", cfg.MinRetryBackoff)
	cc.SetDefault(cfg.prefix+".maxretrybackoff", "", cfg.MaxRetryBackoff)
	cc.SetDefault(cfg.prefix+".conndialtimeout", "dail timeout", cfg.ConnDialTimeout)
	cc.SetDefault(cfg.prefix+".socketreadtimeout", "read timeout", cfg.SocketReadTimeout)
	cc.SetDefault(cfg.prefix+".socketwritetimeout", "write timeout", cfg.SocketWriteTimeout)
	cc.SetDefault(cfg.prefix+".contexttimeoutenabled", "context time enable", cfg.ContextTimeoutEnabled)
	cc.SetDefault(cfg.prefix+".poolfifo", "pool fifo, lifo default", cfg.PoolFIFO)
	cc.SetDefault(cfg.prefix+".connpoolsize", "connection pool size", cfg.ConnPoolSize)
	cc.SetDefault(cfg.prefix+".pooltimeout", "pool timeout", cfg.PoolTimeout)
	cc.SetDefault(cfg.prefix+".minidleconns", "min idel count", cfg.MinIdleConns)
	cc.SetDefault(cfg.prefix+".maxidleconns", "max idle conn count", cfg.MaxIdleConns)
	cc.SetDefault(cfg.prefix+".maxactiveconns", "max active connection count", cfg.MaxActiveConns)
	cc.SetDefault(cfg.prefix+".connmaxidletime", "max idel time", cfg.ConnMaxIdleTime)
	cc.SetDefault(cfg.prefix+".connmaxlifetime", "max life time", cfg.ConnMaxLifetime)
	cc.SetDefault(cfg.prefix+".disableindentity", "", cfg.DisableIndentity)
	cc.SetDefault(cfg.prefix+".sentinelusername", "sentinel username", cfg.SentinelUsername)
	cc.SetDefault(cfg.prefix+".sentinelpassword", "sentinel password", cfg.SentinelPassword)
	cc.SetDefault(cfg.prefix+".sentinelmastername", "sentinel master name", cfg.SentinelMasterName)
	cc.SetDefault(cfg.prefix+".cluster", "enable cluster mode", cfg.Cluster)
	cc.SetDefault(cfg.prefix+".clusterreadonly", "cluster read only", cfg.ClusterReadOnly)
	cc.SetDefault(cfg.prefix+".clusterrouterandomly", "cluster route random", cfg.ClusterRouteRandomly)
	cc.SetDefault(cfg.prefix+".clusterroutebylatency", "cluster route by latency", cfg.ClusterRouteByLatency)
}

// impl configcentra.ConfigValue
func (cfg *Config) RefreshValue(cc configcentra.ConfigCentra) error {
	if cc.UseObject() {
		return cc.GetObject(cfg.prefix, cfg)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".name")
		if err != nil {
			return err
		}
		cfg.Name = (string)(v)
	}
	{
		v, err := cc.GetStringSlice(cfg.prefix + ".addrs")
		if err != nil {
			return err
		}
		cfg.Addrs = ([]string)(v)
	}
	{
		v, err := cc.GetInt(cfg.prefix + ".db")
		if err != nil {
			return err
		}
		cfg.DB = (int)(v)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".username")
		if err != nil {
			return err
		}
		cfg.Username = (string)(v)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".password")
		if err != nil {
			return err
		}
		cfg.Password = (string)(v)
	}
	{
		v, err := cc.GetInt(cfg.prefix + ".maxretries")
		if err != nil {
			return err
		}
		cfg.MaxRetries = (int)(v)
	}
	{
		v, err := cc.GetDuration(cfg.prefix + ".minretrybackoff")
		if err != nil {
			return err
		}
		cfg.MinRetryBackoff = (time.Duration)(v)
	}
	{
		v, err := cc.GetDuration(cfg.prefix + ".maxretrybackoff")
		if err != nil {
			return err
		}
		cfg.MaxRetryBackoff = (time.Duration)(v)
	}
	{
		v, err := cc.GetDuration(cfg.prefix + ".conndialtimeout")
		if err != nil {
			return err
		}
		cfg.ConnDialTimeout = (time.Duration)(v)
	}
	{
		v, err := cc.GetDuration(cfg.prefix + ".socketreadtimeout")
		if err != nil {
			return err
		}
		cfg.SocketReadTimeout = (time.Duration)(v)
	}
	{
		v, err := cc.GetDuration(cfg.prefix + ".socketwritetimeout")
		if err != nil {
			return err
		}
		cfg.SocketWriteTimeout = (time.Duration)(v)
	}
	{
		v, err := cc.GetBool(cfg.prefix + ".contexttimeoutenabled")
		if err != nil {
			return err
		}
		cfg.ContextTimeoutEnabled = (bool)(v)
	}
	{
		v, err := cc.GetBool(cfg.prefix + ".poolfifo")
		if err != nil {
			return err
		}
		cfg.PoolFIFO = (bool)(v)
	}
	{
		v, err := cc.GetInt(cfg.prefix + ".connpoolsize")
		if err != nil {
			return err
		}
		cfg.ConnPoolSize = (int)(v)
	}
	{
		v, err := cc.GetDuration(cfg.prefix + ".pooltimeout")
		if err != nil {
			return err
		}
		cfg.PoolTimeout = (time.Duration)(v)
	}
	{
		v, err := cc.GetInt(cfg.prefix + ".minidleconns")
		if err != nil {
			return err
		}
		cfg.MinIdleConns = (int)(v)
	}
	{
		v, err := cc.GetInt(cfg.prefix + ".maxidleconns")
		if err != nil {
			return err
		}
		cfg.MaxIdleConns = (int)(v)
	}
	{
		v, err := cc.GetInt(cfg.prefix + ".maxactiveconns")
		if err != nil {
			return err
		}
		cfg.MaxActiveConns = (int)(v)
	}
	{
		v, err := cc.GetDuration(cfg.prefix + ".connmaxidletime")
		if err != nil {
			return err
		}
		cfg.ConnMaxIdleTime = (time.Duration)(v)
	}
	{
		v, err := cc.GetDuration(cfg.prefix + ".connmaxlifetime")
		if err != nil {
			return err
		}
		cfg.ConnMaxLifetime = (time.Duration)(v)
	}
	{
		v, err := cc.GetBool(cfg.prefix + ".disableindentity")
		if err != nil {
			return err
		}
		cfg.DisableIndentity = (bool)(v)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".sentinelusername")
		if err != nil {
			return err
		}
		cfg.SentinelUsername = (string)(v)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".sentinelpassword")
		if err != nil {
			return err
		}
		cfg.SentinelPassword = (string)(v)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".sentinelmastername")
		if err != nil {
			return err
		}
		cfg.SentinelMasterName = (string)(v)
	}
	{
		v, err := cc.GetBool(cfg.prefix + ".cluster")
		if err != nil {
			return err
		}
		cfg.Cluster = (bool)(v)
	}
	{
		v, err := cc.GetBool(cfg.prefix + ".clusterreadonly")
		if err != nil {
			return err
		}
		cfg.ClusterReadOnly = (bool)(v)
	}
	{
		v, err := cc.GetBool(cfg.prefix + ".clusterrouterandomly")
		if err != nil {
			return err
		}
		cfg.ClusterRouteRandomly = (bool)(v)
	}
	{
		v, err := cc.GetBool(cfg.prefix + ".clusterroutebylatency")
		if err != nil {
			return err
		}
		cfg.ClusterRouteByLatency = (bool)(v)
	}
	// notify update
	for _, ntf := range cfg.ntfFuncs {
		ntf(cfg)
	}
	return nil
}
