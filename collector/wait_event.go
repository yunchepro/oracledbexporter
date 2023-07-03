package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleWaitTotalEventDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "wait", "total_event"),
		"Oracle Waits",
		[]string{"wait_class", "event", "con_id", "con_name"}, nil)

	oracleWaitTotalTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "wait", "total_time"),
		"Oracle Waited Time",
		[]string{"wait_class", "event", "con_id", "con_name"}, nil)
)

type ScrapeOracleWaitEvent struct{}

func (ScrapeOracleWaitEvent) Name() string {
	return "oracle_wait_event"
}

func (ScrapeOracleWaitEvent) Help() string {
	return "collect stats from v$sys_event"

}

func (ScrapeOracleWaitEvent) Version() float64 {
	return 10.2
}

func (ScrapeOracleWaitEvent) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var sql string
	if ora.VersionNum < 12.0 {
		sql = `select event, wait_class, total_waits, time_waited, 0 as con_id  
from v$system_event 
where wait_class in (
    'Application',
    'Commit',
    'Concurrency',
    'Configuration',
    'Network',
    'System I/O',
    'User I/O'
)`
	} else {
		sql = `select event, wait_class, total_waits, time_waited, con_id  
from v$system_event 
where wait_class in (
    'Application',
    'Commit',
    'Concurrency',
    'Configuration',
    'Network',
    'System I/O',
    'User I/O'
)`
	}

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return err
	}
	for _, r := range rows {
		event := r[0].(string)
		class := r[1].(string)
		conId := r[4].(float64)
		ch <- prometheus.MustNewConstMetric(
			oracleWaitTotalEventDesc, prometheus.CounterValue, r[2].(float64), class, event, formatFloat64(conId), ora.ConName)

		ch <- prometheus.MustNewConstMetric(
			oracleWaitTotalTimeDesc, prometheus.CounterValue, r[3].(float64), class, event, formatFloat64(conId), ora.ConName)
	}
	return nil
}
