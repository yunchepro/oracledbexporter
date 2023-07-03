package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleRecoveryAreaDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "recovery_area", "stat"),
		"Oracle Recovery Area Stats",
		[]string{"name", "mode"}, nil)
)

type ScrapeOracleRecoveryAreaStat struct{}

func (ScrapeOracleRecoveryAreaStat) Name() string {
	return "oracle_recovery_area_stat"
}

func (ScrapeOracleRecoveryAreaStat) Help() string {
	return "collect stats from V$RECOVERY_FILE_DEST"
}

func (ScrapeOracleRecoveryAreaStat) Version() float64 {
	return 10.2
}

func (ScrapeOracleRecoveryAreaStat) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	if ora.PdbFlag {
		return nil
	}
	sql := `select
    substr(name,1,64) as name,
    space_limit as space_limit,
    space_used as space_used,
    space_reclaimable as space_reclaimable,
    number_of_files
from V$RECOVERY_FILE_DEST where space_limit > 0`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return err
	}
	for _, r := range rows {
		recoveryArea := r[0].(string)

		ch <- prometheus.MustNewConstMetric(
			oracleRecoveryAreaDesc, prometheus.GaugeValue, r[1].(float64),
			recoveryArea, "total")

		ch <- prometheus.MustNewConstMetric(
			oracleRecoveryAreaDesc, prometheus.GaugeValue, r[2].(float64),
			recoveryArea, "used")

		ch <- prometheus.MustNewConstMetric(
			oracleRecoveryAreaDesc, prometheus.GaugeValue, r[3].(float64),
			recoveryArea, "reclaimable")

		ch <- prometheus.MustNewConstMetric(
			oracleRecoveryAreaDesc, prometheus.GaugeValue, r[4].(float64),
			recoveryArea, "number_of_files")

		ch <- prometheus.MustNewConstMetric(
			oracleRecoveryAreaDesc, prometheus.GaugeValue, r[2].(float64)*100.0/r[1].(float64),
			recoveryArea, "used_pct")
	}
	return nil
}
