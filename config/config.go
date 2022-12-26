package config

import (
	"github.com/spf13/viper"
)

type AppConfiguration struct {
	Env      string
	Token    string
	Mongo    MongoConfigurationType
	RabbitMQ RabbitMQType
}

type MongoConfigurationType struct {
	URI        string
	Database   string
	Collection CollectionType
}

type CollectionType struct {
	DQBatch string
}

type RabbitMQType struct {
	Scheme   string
	Hostname string
	Port     int
	Username string
	Password string
	VHost    string
}

func GetConfig(env string) AppConfiguration {
	conf := AppConfiguration{}

	configEnv := "config-" + env

	viper.SetConfigName(configEnv)
	viper.SetConfigType("yml")
	viper.AddConfigPath("./config")

	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	if err := viper.Unmarshal(&conf); err != nil {
		panic(err)
	}

	return conf
}
