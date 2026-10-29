package config

import (
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	Env        string     `mapstructure:"env"`
	HTTP       HTTP       `mapstructure:"http"`
	Postgres   Postgres   `mapstructure:"postgres"`
	Redis      Redis      `mapstructure:"redis"`
	MinIO      MinIO      `mapstructure:"minio"`
	ClickHouse ClickHouse `mapstructure:"clickhouse"`
	ML         ML         `mapstructure:"ml"`
	RabbitMQ   RabbitMQ   `mapstructure:"rabbitmq"`
	Logging    Logging    `mapstructure:"logging"`
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
	DB       int    `mapstructure:"migrations"`
}

type MinIO struct {
	Endpoint          string `mapstructure:"endpoint"`
	AccessKey         string `mapstructure:"access_key"`
	SecretKey         string `mapstructure:"secret_key"`
	UseSSL            bool   `mapstructure:"use_ssl"`
	PresignTTLSeconds int    `mapstructure:"presign_ttl_seconds"`
	BucketConsents    string `mapstructure:"bucket_consents"`
	BucketAttachments string `mapstructure:"bucket_attachments"`
}

type ClickHouse struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Database string `mapstructure:"database"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
}

type ML struct {
	Endpoint string `mapstructure:"endpoint"`
	Timeout  string `mapstructure:"timeout"`
}

type RabbitMQ struct {
	URL   string `mapstructure:"url"`
	Queue string `mapstructure:"queue"`
}

type Logging struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
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
