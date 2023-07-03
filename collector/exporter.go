package collector

import (
	"context"
	// "database/sql"

	// "time"

	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"fmt"

	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

const (
	namespace = "oracle"
	exporter  = "exporter"
)

var (
	dbConnectStatusDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, exporter, "db_connect_status"),
		"Database Connect Status",
		[]string{"message"}, nil)
)

type Exporter struct {
	ctx      context.Context
	scrapers []Scraper
	dbclient *dbutil.OracleClient
	metrics  Metrics
}

func New(ctx context.Context, scrapers []Scraper, configFile string) *Exporter {
	metrics := NewMetrics()

	dbclient := dbutil.NewOracleClient(configFile)

	exporter := Exporter{
		ctx:      ctx,
		scrapers: scrapers,
		metrics:  metrics,
		dbclient: dbclient}

	return &exporter
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.metrics.Error.Desc()
	ch <- e.metrics.TotalScrapes.Desc()
	e.metrics.ScrapeErrors.Describe(ch)
	ch <- e.metrics.OracleUp.Desc()

}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	// scrape each cdb
	e.scrape(e.ctx, ch)

	ch <- e.metrics.Error
	ch <- e.metrics.OracleUp
	ch <- e.metrics.TotalScrapes
	e.metrics.ScrapeErrors.Collect(ch)
}

// case 1: version < 12c
// case 2: version >= 12c, with cdb and pdbs
// case 3: version >= 12c, with pdbs, but no cdb
// case 4: version >= 12c, no pdbs

func (e *Exporter) scrape(ctx context.Context, ch chan<- prometheus.Metric) {
	// var err error

	err := e.dbclient.Init()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Can not Init DB Connection")
		e.metrics.Error.Set(1)
		e.metrics.OracleUp.Set(0)
		ch <- prometheus.MustNewConstMetric(dbConnectStatusDesc, prometheus.GaugeValue, 1, fmt.Sprintf("%s", err))
		return
	}

	log.WithFields(log.Fields{"dbconfig": e.dbclient.C}).Debug("DB CONFIG")

	ch <- prometheus.MustNewConstMetric(dbConnectStatusDesc, prometheus.GaugeValue, 0, "OK")

	oracleInfo, err := getOracleInfoAll(ctx, e.dbclient)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Get Oracle Info has error")
		return
	}
	log.WithFields(log.Fields{"version": oracleInfo.Version,
		"dbid":         oracleInfo.Dbid,
		"instanceName": oracleInfo.InstanceName,
		"dbUniqueName": oracleInfo.DbUniqueName,
		"pdb":          oracleInfo.ConName,
		"databaseRole": oracleInfo.DatabaseRole}).Info("Scrape Oracle")

	e.scrapeOne(ctx, ch, oracleInfo)

	if oracleInfo.VersionNum >= 12 && oracleInfo.ConId == "1" {
		e.scrapePdbs(ctx, ch)
	}

	err = e.dbclient.CloseConnection()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Close Database Connection has error")
	}
}

func (e *Exporter) scrapePdbs(ctx context.Context, ch chan<- prometheus.Metric) {
	for _, pdb := range e.dbclient.C.Pdbs {
		err := e.dbclient.ReInitWithPdb(pdb)
		if err != nil {
			log.WithFields(log.Fields{"pdb": pdb, "error": err}).Error("Init With Pdb Failed")
			// try next pdb
			continue
		}

		oracleInfo, err := getOracleInfoAll(ctx, e.dbclient)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("Get Oracle Info has error")
			return
		}
		log.WithFields(log.Fields{"version": oracleInfo.Version,
			"dbid":         oracleInfo.Dbid,
			"instanceName": oracleInfo.InstanceName,
			"dbUniqueName": oracleInfo.DbUniqueName,
			"pdb":          oracleInfo.ConName,
			"databaseRole": oracleInfo.DatabaseRole}).Info("Scrape Oracle")

		oracleInfo.PdbFlag = true
		e.scrapeOne(ctx, ch, oracleInfo)

	}

}

func (e *Exporter) scrapeOne(ctx context.Context, ch chan<- prometheus.Metric, oracleInfo *InstanceInfoAll) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, scraper := range e.scrapers {
		// if version < scraper.Version() {
		// 	continue
		// }

		wg.Add(1)
		go func(scraper Scraper) {
			defer wg.Done()
			scraper.Scrape(ctx, e.dbclient, ch, oracleInfo)
		}(scraper)
	}
}

// Metrics represents exporter metrics which values can be carried between http requests.
type Metrics struct {
	TotalScrapes prometheus.Counter
	ScrapeErrors *prometheus.CounterVec
	Error        prometheus.Gauge
	OracleUp     prometheus.Gauge
}

// NewMetrics creates new Metrics instance.
func NewMetrics() Metrics {
	subsystem := exporter
	return Metrics{
		TotalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "scrapes_total",
			Help:      "Total number of times Oracle was scraped for metrics.",
		}),
		ScrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "scrape_errors_total",
			Help:      "Total number of times an error occurred scraping a Oracle.",
		}, []string{"collector"}),
		Error: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from Oracle resulted in an error (1 for error, 0 for success).",
		}),
		OracleUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the Oracle server is up.",
		}),
	}
}
