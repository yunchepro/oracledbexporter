package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

// sga, pga, shared pool, large pool
//
var (
	pga_stats = []string{}
)

type ScrapeMemoryInfo struct{}

func (ScrapeMemoryInfo) Name() string {
	return "oracle_memory_info"
}

func (ScrapeMemoryInfo) Help() string {
	return "collect stats from v$sgainfo, v$pgastat"

}

func (ScrapeMemoryInfo) Version() float64 {
	return 10.2
}

func (ScrapeMemoryInfo) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	if ora.PdbFlag {
		return nil
	}
	err := scrape_pga(ctx, dbcli, ch)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("scrape pga has error")
		return err
	}

	err = scrape_sga(ctx, dbcli, ch)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("scrape sga has error")
		return err
	}
	return nil
}

func scrape_pga(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric) error {
	sql := `select name, value from v$pgastat where unit is not null`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)

	if err != nil {
		return err
	}

	for _, r := range rows {

		stat_name := r[0].(string)
		val := r[1].(float64)
		desc := newDesc("pga", formatLabel(stat_name), "metric from v$pgastat")

		ch <- prometheus.MustNewConstMetric(
			desc, prometheus.GaugeValue, val,
		)
	}

	return nil
}

func scrape_sga(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric) error {
	sql := `select name, bytes from v$sgainfo`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)

	if err != nil {
		return err
	}

	for _, r := range rows {

		stat_name := r[0].(string)
		val := r[1].(float64)
		desc := newDesc("sga", formatLabel(stat_name), "metric from v$pgastat")

		ch <- prometheus.MustNewConstMetric(
			desc, prometheus.GaugeValue, val,
		)

		// if stat_name == '' {
		// 	sga_total_size += val
		// }

	}

	return nil
}

func newDesc(subsystem string, name string, help string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, name),
		help, nil, nil,
	)
}
