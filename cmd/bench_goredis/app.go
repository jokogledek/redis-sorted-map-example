package main

import (
	"github.com/rs/zerolog/log"
	"github.com/ujunglangit-id/redis-sorted-map-example/pkg/bench/goredis"
	"github.com/ujunglangit-id/redis-sorted-map-example/pkg/lib"
	"github.com/ujunglangit-id/redis-sorted-map-example/pkg/model/config"
	"time"
)

func main() {
	cfg, err := config.InitConfig()
	if err != nil {
		log.Fatal().Msgf("failed load config %#v", err)
		return
	}
	bench := goredis.NewBench(cfg)
	defer func() {
		lib.PrintMemUsage()
	}()

	start := time.Now()
	bench.LoadCsv()
	log.Info().Msgf("load csv finished in %s", time.Since(start))

	start = time.Now()
	bench.InsertToRedis()
	log.Info().Msgf("insert into redis finished in %s", time.Since(start))

	start = time.Now()
	bench.UpdateRedis()
	log.Info().Msgf("redis update finished in %s", time.Since(start))
}
