package goredis

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"github.com/ujunglangit-id/redis-sorted-map-example/pkg/model/config"
	"github.com/ujunglangit-id/redis-sorted-map-example/pkg/model/types"
	"os"
	"strconv"
	"time"
)

type Bench struct {
	cfg            *config.Config
	PayloadData    []*types.Stocks
	RedisClient    *redis.Client
	BasketKey      string
	HashKey        string
	PipelineBuffer chan int
}

func NewBench(cfg *config.Config) *Bench {
	b := &Bench{
		cfg: cfg,
	}

	err := b.InitRedis()
	if err != nil {
		log.Fatal().Msgf("failed to init redis, %#v", err)
	}

	b.PipelineBuffer = make(chan int, cfg.Redis.Buffer)
	return b
}

func (b *Bench) InitRedis() (err error) {
	b.RedisClient = redis.NewClient(&redis.Options{
		Addr: b.cfg.Redis.Host,
	})
	//prepare timeseries key
	b.HashKey = b.cfg.Redis.Key + ":hash"
	b.BasketKey = b.cfg.Redis.Key + ":list"
	fmt.Printf("redis key : %s\n", b.HashKey)
	fmt.Printf("redis key : %s\n\n", b.BasketKey)
	return
}

func (b *Bench) LoadCsv() {
	// open file
	f, err := os.Open(b.cfg.Files.Csv)
	if err != nil {
		log.Error().Msgf("failed to open csv %#v", err)
		return
	}
	defer f.Close()

	//read csv values using csv.Reader
	csvReader := csv.NewReader(f)
	data, err := csvReader.ReadAll()
	if err != nil {
		log.Error().Msgf("failed to read csv %#v", err)
		return
	}

	for i, line := range data {
		if i > 0 {
			vol, _ := strconv.ParseFloat(line[6], 64)
			b.PayloadData = append(b.PayloadData, &types.Stocks{
				Ticker: line[0],
				Day:    line[1],
				Open:   line[2],
				High:   line[3],
				Low:    line[4],
				Close:  line[5],
				Volume: vol,
			})
		}
	}
	log.Info().Msgf("data length : %d", len(b.PayloadData))
}

func (b *Bench) InsertToRedis() {
	for _, v := range b.PayloadData {
		ctx := context.Background()
		score := time.Now().UnixNano()
		// Zadd
		err := b.RedisClient.HSet(ctx, b.HashKey, map[string]interface{}{
			v.Ticker: score,
		}).Err()
		if err != nil {
			log.Error().Msgf("err hset %#v", err)
			continue
		}
		err = b.RedisClient.ZAdd(ctx, b.BasketKey, &redis.Z{
			Score:  float64(score),
			Member: v.Volume,
		}).Err()

		if err != nil {
			log.Error().Msgf("err zadd %#v", err)
		}
	}
	//wg.Wait()
}

func (b *Bench) UpdateRedis() {
	for _, v := range b.PayloadData {
		ctx := context.Background()
		// Zadd
		score, err := b.RedisClient.HGet(ctx, b.HashKey, v.Ticker).Result()
		if err != nil {
			log.Error().Msgf("err hset %#v", err)
			continue
		}

		err = b.RedisClient.ZRemRangeByScore(ctx, b.BasketKey, score, score).Err()
		if err != nil {
			log.Error().Msgf("err ZRemRangeByScore %#v", err)
			continue
		}

		scoreFl, err := strconv.ParseFloat(score, 64)
		if err != nil {
			log.Error().Msgf("err strconv %#v", err)
			continue
		}
		err = b.RedisClient.ZAdd(ctx, b.BasketKey, &redis.Z{
			Score:  scoreFl,
			Member: v.Volume,
		}).Err()
		if err != nil {
			log.Error().Msgf("err zadd %#v", err)
		}
	}
	//wg.Wait()
}
