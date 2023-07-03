package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleActiveSessionDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "session", "active"),
		"Oracle Active Session",
		[]string{"sid", "serial", "username", "sql_id", "sql_child_number", "program", "machine", "event", "sql_text", "con_id", "con_name"}, nil)

	oracleBlockingSessionDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "session", "blocking"),
		"Oracle Blocking Session",
		[]string{"sid", "serial", "logon_time", "status", "event", "p1", "p2", "p3", "username",
			"terminal", "program", "sql_id", "prev_sql_id", "blocking_session", "blocking_instance",
			"row_wait_obj", "sql_text", "prev_sql_text", "con_id", "con_name"}, nil)
)

type ScrapeBlockSessionStat struct{}

func (ScrapeBlockSessionStat) Name() string {
	return "oracle_active_session"
}

func (ScrapeBlockSessionStat) Help() string {
	return "collect active session from v$session"

}

func (ScrapeBlockSessionStat) Version() float64 {
	return 10.2
}

func (s ScrapeBlockSessionStat) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	err := s.scrapeActiveSession(ctx, dbcli, ch, ora)
	if err != nil {
		return err
	}

	err = s.scrapeBlockingSession(ctx, dbcli, ch, ora)
	if err != nil {
		return err
	}

	return nil
}

func (ScrapeBlockSessionStat) scrapeActiveSession(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var sql string
	if ora.VersionNum < 12.0 {
		sql = `select * from (
select
    last_call_et, a.sid, a.serial#, a.username, a.sql_id, a.sql_child_number, a.program, a.machine, a.event, b.sql_text, 0 as con_id
from v$session a, v$sql b
where a.status = 'ACTIVE'
  and a.sql_id = b.sql_id
  and rawtohex(sql_address) <> '00'
  and a.username is not null
  and a.type<>'BACKGROUND'
  and sid <> (select sid from v$mystat where rownum = 1)
  order by last_call_et desc) where rownum <= 30
	`
	} else {
		sql = `select * from (
select
    last_call_et, a.sid, a.serial#, a.username, a.sql_id, a.sql_child_number, a.program, a.machine, a.event, b.sql_text, a.con_id
from v$session a, v$sql b
where a.status = 'ACTIVE'
  and a.sql_id = b.sql_id
  and rawtohex(sql_address) <> '00'
  and a.username is not null
  and a.type<>'BACKGROUND'
  and sid <> (select sid from v$mystat where rownum = 1)
  order by last_call_et desc) where rownum <= 30
	`
	}

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Get Blocking Session has Error")
		return err
	}
	//cols 1,2,5 float64
	for _, r := range rows {
		ch <- prometheus.MustNewConstMetric(
			oracleActiveSessionDesc, prometheus.GaugeValue, r[0].(float64),
			formatFloat64(r[1].(float64)),
			formatFloat64(r[2].(float64)),
			r[3].(string), r[4].(string),
			formatFloat64(r[5].(float64)),
			r[6].(string),
			r[7].(string),
			r[8].(string),
			r[9].(string),
			formatFloat64(r[10].(float64)),
			ora.ConName,
		)
	}
	return nil
}

func (ScrapeBlockSessionStat) scrapeBlockingSession(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var sql string
	if ora.VersionNum < 12.0 {
		sql = `with sessions as (
select last_call_et,
  sid, serial# serial, to_char(logon_time, 'yyyy-mm-dd hh24:mi:ss') as logon_time, status, 
  event,p1, p2,p3,username, terminal, program, sql_id, prev_sql_id,
  blocking_session, blocking_instance, ROW_WAIT_OBJ# row_wait_obj, 0 as con_id
from v$session )
select a.*, b.sql_text, c.sql_text as prev_sql_text
from sessions a left join v$sql b 
on a.sql_id = b.sql_id
left join v$sql c
on a.prev_sql_id = c.sql_id
where a.sid in (select blocking_session from sessions)
     or blocking_session is not null
	`
	} else {
		sql = `with sessions as (
select last_call_et,
  sid, serial# serial, to_char(logon_time, 'yyyy-mm-dd hh24:mi:ss') as logon_time, status, 
  event,p1, p2,p3,username, terminal, program, sql_id, prev_sql_id,
  blocking_session, blocking_instance, ROW_WAIT_OBJ# row_wait_obj, con_id
from v$session )
select a.*, b.sql_text, c.sql_text as prev_sql_text
from sessions a left join v$sql b 
on a.sql_id = b.sql_id
left join v$sql c
on a.prev_sql_id = c.sql_id
where a.sid in (select blocking_session from sessions)
     or blocking_session is not null
	`
	}

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Get Blocking Session has Error")
		return err
	}
	//cols 1,2,14,15,16 float64
	for _, r := range rows {
		ch <- prometheus.MustNewConstMetric(
			oracleBlockingSessionDesc, prometheus.GaugeValue, r[0].(float64),
			formatFloat64(r[1].(float64)),
			formatFloat64(r[2].(float64)),
			r[3].(string),
			r[4].(string),
			r[5].(string),
			formatFloat64(r[6].(float64)),
			formatFloat64(r[7].(float64)),
			formatFloat64(r[8].(float64)),
			r[9].(string),
			r[10].(string),
			r[11].(string),
			r[12].(string),
			r[13].(string),
			formatFloat64(r[14].(float64)),
			formatFloat64(r[15].(float64)),
			formatFloat64(r[16].(float64)),
			r[18].(string),
			r[19].(string),
			formatFloat64(r[17].(float64)),
			ora.ConName,
		)

	}
	return nil
}
