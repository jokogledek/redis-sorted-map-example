package config

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v2"
	"os"
)

func InitConfig() (cfg *Config, err error) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Info().Msgf("config load [OK]")
	cfg = &Config{}
	err = cfg.readFile()
	if err != nil {
		return
	}

	return
}

func (cfg *Config) readFile() (err error) {
	var (
		f *os.File
	)

	path := []string{
		"/config",
		"/etc/redis_bench/config",
		"files/config",
		"./files/config",
		"../files/config",
		"../../files/config",
	}

	for _, val := range path {
		f, err = os.Open(fmt.Sprintf(`%s/cfg.yml`, val))
		if err == nil {
			log.Info().Msgf("[config][init] load config file from %s", fmt.Sprintf(`%s/cfg.yml`, val))
			decoder := yaml.NewDecoder(f)
			err = decoder.Decode(cfg)
			break
		}
	}

	if err != nil {
		return
	}

	log.Info().Msg("[config][ReadConfig] Config load success")
	return
}
