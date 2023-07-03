package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleActiveTransactionDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "transaction", "duration"),
		"Oracle Active Session",
		[]string{"con_id", "sid", "serial", "session_status", "sql_id", "prev_sql_id", "start_time"}, nil)

	oracleActiveTransactionUndoBlkDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "transaction", "undo_block"),
		"Oracle Active Session",
		[]string{"con_id", "sid", "serial", "session_status", "sql_id", "prev_sql_id", "start_time"}, nil)

	oracleActiveTransactionUndoRecDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "transaction", "undo_record"),
		"Oracle Active Session",
		[]string{"con_id", "sid", "serial", "session_status", "sql_id", "prev_sql_id", "start_time"}, nil)
)

type ScrapeActiveTransactionStat struct{}

func (ScrapeActiveTransactionStat) Name() string {
	return "oracle_active_transaction"
}

func (ScrapeActiveTransactionStat) Help() string {
	return "collect active transaction from v$transaction"

}

func (ScrapeActiveTransactionStat) Version() float64 {
	return 10.2
}

func (s ScrapeActiveTransactionStat) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	err := s.scrapeActiveTransaction(ctx, dbcli, ch, ora)
	if err != nil {
		return err
	}

	return nil
}

func (ScrapeActiveTransactionStat) scrapeActiveTransaction(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var sql string
	if ora.VersionNum < 12.0 {
		sql = `select 0 as con_id, b.sid, 
    b.serial#, 
	b.status as session_status, 
	b.sql_id, 
	b.prev_sql_id,
    to_char(a.START_DATE, 'yyyy-mm-dd hh24:mi:ss') as start_time, 
	a.status as transaction_status, 
	(sysdate - a.start_date) * 86400 as duration,
	a.USED_UBLK, 
	a.USED_UREC
from v$transaction a, v$session b 
where a.addr = b.taddr
and a.status = 'ACTIVE'
and (sysdate - a.start_date) * 86400 >= 60`
	} else {
		sql = `select con_id, b.sid, 
    b.serial#, 
	b.status as session_status, 
	b.sql_id,
	b.prev_sql_id, 
    to_char(a.START_DATE, 'yyyy-mm-dd hh24:mi:ss') as start_time, 
	a.status as transaction_status, 
	(sysdate - a.start_date) * 86400 as duration,
	a.USED_UBLK, 
	a.USED_UREC
from v$transaction a, v$session b 
where a.addr = b.taddr
and a.status = 'ACTIVE'
and (sysdate - a.start_date) * 86400 >= 60
	`
	}

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Get Transaction has Error")
		return err
	}

	for _, r := range rows {
		conId := formatFloat64(r[0].(float64))
		sid := formatFloat64(r[1].(float64))
		serial := formatFloat64(r[2].(float64))
		sessionStatus := r[3].(string)
		sqlId := r[4].(string)
		prevSqlId := r[5].(string)
		startTime := r[6].(string)
		// transactionStatus := r[7].(string)
		duration := r[8].(float64)
		usedBlk := r[9].(float64)
		usedRec := r[10].(float64)
		ch <- prometheus.MustNewConstMetric(
			oracleActiveTransactionDurationDesc, prometheus.GaugeValue, duration,
			conId, sid, serial, sessionStatus, sqlId, prevSqlId, startTime,
		)
		ch <- prometheus.MustNewConstMetric(
			oracleActiveTransactionUndoBlkDesc, prometheus.GaugeValue, usedBlk,
			conId, sid, serial, sessionStatus, sqlId, prevSqlId, startTime,
		)
		ch <- prometheus.MustNewConstMetric(
			oracleActiveTransactionUndoRecDesc, prometheus.GaugeValue, usedRec,
			conId, sid, serial, sessionStatus, sqlId, prevSqlId, startTime,
		)
	}
	return nil
}
