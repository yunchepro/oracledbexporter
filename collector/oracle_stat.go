package collector

import (
	"context"
	// "database/sql"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	stats = []string{
		"sorts (memory)",
		"sorts (disk)",
		"sorts (rows)",
		"table scans (long tables)",
		"table scans (short tables)",
		"transaction rollbacks",
		"user commits",
		"redo synch time",
		"redo synch writes",
		"user calls",
		"SQL*Net roundtrips to/from client",
		"gc cr blocks served",
		"gc cr blocks received",
		"gc cr block receive time",
		"gc cr block send time",
		"gc current blocks served",
		"gc current blocks received",
		"gc current block receive time",
		"gc current block send time",
		"gcs messages sent",
		"ges messages sent",
		"db block changes",
		"redo writes",
		"physical read total bytes",
		"physical write total bytes",
		"session logical reads",
		"redo size",
		"leaf node splits",
		"branch node splits",
		"parse count (total)",
		"parse count (hard)",
		"parse count (failures)",
		"execute count",
		"bytes sent via SQL*Net to client",
		"bytes received via SQL*Net from client",
	}

	// oracleStatDesc = prometheus.NewDesc(
	// 	prometheus.BuildFQName(namespace, "stat", "stat"),
	// 	"Oracle Stats",
	// 	[]string{"name"}, nil)
)

type ScrapeOracleStat struct{}

func (ScrapeOracleStat) Name() string {
	return "oracle_stat"
}

func (ScrapeOracleStat) Help() string {
	return "collect stats from v$sysstat"

}

func (ScrapeOracleStat) Version() float64 {
	return 10.2
}

func (s ScrapeOracleStat) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var err error
	err = s.scrapeOracleStat(ctx, dbcli, ch, ora)
	if err != nil {
		return err
	}

	err = s.scrapeSessionNumber(ctx, dbcli, ch, ora)
	if err != nil {
		return err
	}

	err = s.scrapeProcessNumber(ctx, dbcli, ch, ora)
	if err != nil {
		return err
	}

	return nil
}

func (ScrapeOracleStat) scrapeOracleStat(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var sqltext string
	if ora.VersionNum < 12.0 {
		sqltext = "select /* oracle_exporter */ name, value, 0 as con_id from v$sysstat where name in (%s)"
	} else {
		sqltext = "select /* oracle_exporter */ name, value, con_id from v$sysstat where name in (%s)"
	}
	sql := fmt.Sprintf(sqltext, formatInList(stats))

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Query Error")
		return err
	}
	for _, r := range rows {
		val := r[1].(float64)
		stat := formatLabel(r[0].(string))
		conId := r[2].(float64)

		oracleStatDesc := prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "stat", stat),
			"Oracle Stats",
			[]string{"con_id", "con_name"}, nil)
		ch <- prometheus.MustNewConstMetric(oracleStatDesc, prometheus.CounterValue, val, formatFloat64(conId), ora.ConName)
	}
	return nil
}

func (ScrapeOracleStat) scrapeSessionNumber(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var sql string
	if ora.VersionNum < 12.0 {
		sql = `select count(*) as total_sessions, 
  sum(case when status = 'ACTIVE' and type = 'USER' then 1 else 0 end) as active_sessions,
  sum(case when taddr is not null and type = 'USER' then 1 else 0 end) as trans_sessions,
  sum(case when blocking_session is not null and type = 'USER' then 1 else 0 end) as blocking_sessions,
 0 as con_id from v$session`
	} else {
		if ora.PdbFlag {
			sql = `select count(*) as total_sessions, 
  sum(case when status = 'ACTIVE' and type = 'USER' then 1 else 0 end) as active_sessions,
  sum(case when taddr is not null and type = 'USER' then 1 else 0 end) as trans_sessions,
  sum(case when blocking_session is not null and type = 'USER' then 1 else 0 end) as blocking_sessions,
 con_id from v$session where con_id > 0
 group by con_id`
		} else {
			sql = `select count(*) as total_sessions, 
  sum(case when status = 'ACTIVE' and type = 'USER' then 1 else 0 end) as active_sessions,
  sum(case when taddr is not null and type = 'USER' then 1 else 0 end) as trans_sessions,
  sum(case when blocking_session is not null and type = 'USER' then 1 else 0 end) as blocking_sessions,
 con_id from v$session
 group by con_id`
		}
	}

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return err
	}

	for _, r := range rows {
		conId := r[4].(float64)

		var oracleStatDesc *prometheus.Desc

		oracleStatDesc = prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "stat_sessions", "total"),
			"Oracle Stats",
			[]string{"con_id", "con_name"}, nil)
		ch <- prometheus.MustNewConstMetric(oracleStatDesc, prometheus.GaugeValue, r[0].(float64), formatFloat64(conId), ora.ConName)

		oracleStatDesc = prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "stat_sessions", "active"),
			"Oracle Stats",
			[]string{"con_id", "con_name"}, nil)
		ch <- prometheus.MustNewConstMetric(oracleStatDesc, prometheus.GaugeValue, r[1].(float64), formatFloat64(conId), ora.ConName)

		oracleStatDesc = prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "stat_sessions", "with_trans"),
			"Oracle Stats",
			[]string{"con_id", "con_name"}, nil)
		ch <- prometheus.MustNewConstMetric(oracleStatDesc, prometheus.GaugeValue, r[2].(float64), formatFloat64(conId), ora.ConName)

		oracleStatDesc = prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "stat_sessions", "blocking"),
			"Oracle Stats",
			[]string{"con_id", "con_name"}, nil)
		ch <- prometheus.MustNewConstMetric(oracleStatDesc, prometheus.GaugeValue, r[3].(float64), formatFloat64(conId), ora.ConName)

	}

	return nil
}

func (ScrapeOracleStat) scrapeProcessNumber(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var sql string
	if ora.VersionNum < 12.0 {
		sql = `select count(*), 0 as con_id from v$process`
	} else {
		if ora.PdbFlag {
			sql = `select count(*), con_id from v$process where con_id > 0 group by con_id`
		} else {
			sql = `select count(*), con_id from v$process group by con_id`
		}
	}
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return err
	}

	for _, r := range rows {
		conId := r[1].(float64)
		oracleStatDesc := prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "stat", "process_count"),
			"Oracle Stats",
			[]string{"con_id", "con_name"}, nil)
		ch <- prometheus.MustNewConstMetric(oracleStatDesc, prometheus.GaugeValue, r[0].(float64), formatFloat64(conId), ora.ConName)
	}
	return nil
}
