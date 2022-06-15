package lib

import (
	"github.com/rs/zerolog/log"
	"runtime"
)

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	log.Info().Msgf("Alloc : %v MiB", bToMb(m.Alloc))
	log.Info().Msgf("TotalAlloc : %v MiB", bToMb(m.TotalAlloc))
	log.Info().Msgf("Sys : %v MiB", bToMb(m.Sys))
	log.Info().Msgf("NumGC : %v", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
