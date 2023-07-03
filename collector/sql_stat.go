package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleSqlStatDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "sql", "stat"),
		"Oracle SQL Stats",
		[]string{"sql_id", "sql_text", "executions", "fetches", "sorts", "buffer_gets", "rows_processed"}, nil)
)

type ScrapeOracleSqlStat struct{}

func (ScrapeOracleSqlStat) Name() string {
	return "oracle_sql_stat"
}

func (ScrapeOracleSqlStat) Help() string {
	return "collect stats from v$sql"

}

func (ScrapeOracleSqlStat) Version() float64 {
	return 10.2
}

func (ScrapeOracleSqlStat) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {

	sql := `select * from (select sql_id, sql_text, executions, fetches, sorts, buffer_gets, rows_processed 
from v$sql order by buffer_gets desc ) 
where rownum < 1`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return err
	}
	for _, r := range rows {
		ch <- prometheus.MustNewConstMetric(
			oracleSqlStatDesc, prometheus.GaugeValue, 1, r[0].(string),
			r[1].(string), formatFloat64(r[2].(float64)), formatFloat64(r[3].(float64)),
			formatFloat64(r[4].(float64)), formatFloat64(r[5].(float64)), formatFloat64(r[6].(float64)))

	}
	return nil
}
