package collector

import (
	"context"
	// "database/sql"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleDbTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "time_model", "db_time"),
		"Oracle TIme Model",
		[]string{"con_id", "con_name"}, nil)

	oracleDbCpuDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "time_model", "db_cpu"),
		"Oracle Time Model",
		[]string{"con_id", "con_name"}, nil)

	oracleBackgroundCpuDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "time_model", "background_cpu"),
		"Oracle Time Model",
		[]string{"con_id", "con_name"}, nil)

	oracleTimeModelDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "time_model", "stat"),
		"Oracle Time Model",
		[]string{"stat_name", "con_id", "con_name"}, nil)
)

type ScrapeOracleTimeModel struct{}

func (ScrapeOracleTimeModel) Name() string {
	return "oracle_time_model"
}

func (ScrapeOracleTimeModel) Help() string {
	return "collect stats from v$time_module"

}

func (ScrapeOracleTimeModel) Version() float64 {
	return 10.2
}

// db time
// db cpu, background cpu
// elapsetime

func (ScrapeOracleTimeModel) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var sql string
	if ora.VersionNum < 12.0 {
		sql = `select stat_name, value, 0 as con_id
from v$sys_time_model
`
	} else {
		sql = `select stat_name, value, con_id
from v$sys_time_model
`
	}

	//where stat_name in ('DB time', 'DB CPU', 'background cpu time')

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("scrape oracle time model has error")
		return err
	}
	for _, r := range rows {
		stat_name := r[0].(string)
		val := r[1].(float64)
		conId := r[2].(float64)
		switch stat_name {
		case "DB time":
			ch <- prometheus.MustNewConstMetric(
				oracleDbTimeDesc, prometheus.CounterValue, val, formatFloat64(conId), ora.ConName)
		case "DB CPU":
			ch <- prometheus.MustNewConstMetric(
				oracleDbCpuDesc, prometheus.CounterValue, val, formatFloat64(conId), ora.ConName)
		case "background cpu time":
			ch <- prometheus.MustNewConstMetric(
				oracleBackgroundCpuDesc, prometheus.CounterValue, val, formatFloat64(conId), ora.ConName)
		default:
			ch <- prometheus.MustNewConstMetric(
				oracleTimeModelDesc, prometheus.CounterValue, val, stat_name, formatFloat64(conId), ora.ConName)
		}

	}
	return nil
}
