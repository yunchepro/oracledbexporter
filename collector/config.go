package collector

import (
	"time"
)

// exporter default config

const (
	ScrapeIntervalTablespace = 3600 * time.Second
	ScrapeIntervalSnapshot   = 600 * time.Second
)
