package collector

import (
	"context"
	// "database/sql"
	"fmt"

	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	params = []string{
		"sessions",
		"processes",
		"memory_target",
		"memory_max_target",
		"sga_target",
		"sga_max_size",
		"shared_pool_size",
		"db_cache_size",
		"large_pool_size",
		"java_pool_size",
		"streams_pool_size",
	}

	// oracleStatDesc = prometheus.NewDesc(
	// 	prometheus.BuildFQName(namespace, "stat", "stat"),
	// 	"Oracle Stats",
	// 	[]string{"name"}, nil)
)

type ScrapeOracleParameter struct{}

func (ScrapeOracleParameter) Name() string {
	return "oracle_parameter"
}

func (ScrapeOracleParameter) Help() string {
	return "collect stats from v$parameter"

}

func (ScrapeOracleParameter) Version() float64 {
	return 10.2
}

func (ScrapeOracleParameter) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	if ora.PdbFlag {
		return nil
	}
	sqltext := "select name, value from v$parameter where name in (%s)"
	sql := fmt.Sprintf(sqltext, formatInList(params))

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Query Error")
		return err
	}
	for _, r := range rows {
		param_name := r[0].(string)
		str_val := r[1].(string)
		val, err := strconv.ParseFloat(str_val, 64)
		if err != nil {
			log.WithFields(log.Fields{"str_val": str_val}).Info("can not parse float")
			continue
		}

		oracleParmsDesc := newDesc("param", param_name, "oracle param")

		ch <- prometheus.MustNewConstMetric(oracleParmsDesc, prometheus.UntypedValue, val)
	}
	return nil
}
