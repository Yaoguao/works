package config

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/spf13/viper"
)

type Config struct {
	Env             string          `mapstructure:"env"`
	HTTP            HTTP            `mapstructure:"http"`
	Postgres        Postgres        `mapstructure:"postgres"`
	Redis           Redis           `mapstructure:"redis"`
	ClickHouse      ClickHouse      `mapstructure:"clickhouse"`
	Kafka           Kafka           `mapstructure:"kafka"`
	Retry           Retry           `mapstructure:"retry"`
	Recommendations Recommendations `mapstructure:"recommendations"`
	ML              ML              `mapstructure:"ml"`
	Logging         Logging         `mapstructure:"logging"`
}

type HTTP struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	ReadTimeout  string `mapstructure:"read_timeout"`
	WriteTimeout string `mapstructure:"write_timeout"`
	IdleTimeout  string `mapstructure:"idle_timeout"`
}

type Postgres struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	Database     string `mapstructure:"database"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	SSLMode      string `mapstructure:"ssl_mode"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
	ConnLifetime string `mapstructure:"max_conn_lifetime"`
	ConnIdleTime string `mapstructure:"max_conn_idle_time"`
}

type Redis struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type ClickHouse struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Database string `mapstructure:"database"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
}

type Kafka struct {
	Brokers        []string `mapstructure:"brokers"`
	Topic          string   `mapstructure:"topic"`
	DLQTopic       string   `mapstructure:"dlq_topic"`
	GroupID        string   `mapstructure:"group_id"`
	MaxAttempts    int      `mapstructure:"max_attempts"`
	BaseRetryDelay string   `mapstructure:"base_retry_delay"`
	MaxRetryDelay  string   `mapstructure:"max_retry_delay"`
}

type Retry struct {
	Attempts int     `mapstructure:"attempts"`
	Delay    string  `mapstructure:"delay"`
	Backoff  float64 `mapstructure:"backoff"`
}

type Recommendations struct {
	Budget           string `mapstructure:"budget"`
	DefaultLimit     int    `mapstructure:"default_limit"`
	MaxLimit         int    `mapstructure:"max_limit"`
	MLTimeout        string `mapstructure:"ml_timeout"`
	PersonalCacheTTL string `mapstructure:"personal_cache_ttl"`
	FallbackCacheTTL string `mapstructure:"fallback_cache_ttl"`
	StaleCacheTTL    string `mapstructure:"stale_cache_ttl"`
	NegativeCacheTTL string `mapstructure:"negative_cache_ttl"`
	ExcludeLowMargin bool   `mapstructure:"exclude_low_margin"`
}

type ML struct {
	URL       string `mapstructure:"url"`
	SlowDelay string `mapstructure:"slow_delay"`
}

type Logging struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func (h HTTP) Addr() string {
	return net.JoinHostPort(h.Host, strconv.Itoa(h.Port))
}

func (r Redis) Addr() string {
	return net.JoinHostPort(r.Host, strconv.Itoa(r.Port))
}

func (p Postgres) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.User, p.Password, p.Host, p.Port, p.Database, p.SSLMode,
	)
}

func MustLoad() *Config {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		panic("CONFIG_PATH is not set")
	}

	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		panic("failed to read config: " + err.Error())
	}

	var cfg Config

	if err := viper.Unmarshal(&cfg); err != nil {
		panic("failed to unmarshal config: " + err.Error())
	}

	return &cfg
}
