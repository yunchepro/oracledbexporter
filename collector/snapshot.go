package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	snapshotSqlAllCols = []string{
		"snap_id", "begin_time", "end_time", "sql_id", "parsing_schema", "sql_text",
		"version_count", "executions", "sorts", "disk_reads", "buffer_gets", "cpu_time",
		"elapsed_time", "parse_calls", "rows_processed",
	}
	snapshotSqlLabelCols = []string{
		"snap_id", "begin_time", "end_time", "sql_id", "parsing_schema", "sql_text",
	}
	oracleSnapshotSqlStatDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "sql", "stat1"),
		"Oracle SQL Stats",
		snapshotSqlLabelCols, nil)

	oracleSnapshotSqlStatAllDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "sql", "snap"),
		"Oracle SQL Stats",
		snapshotSqlAllCols, nil)
)

type ScrapeOracleSnapshot struct {
	lastSnapshotId float64
	lastScrapeTime time.Time
}

type snapshot struct {
	dbid           string
	startupTime    string
	beginTime      string
	endTime        string
	snapId         string
	instanceNumber string
}

func (*ScrapeOracleSnapshot) Name() string {
	return "oracle_sql_snapshot"
}

func (*ScrapeOracleSnapshot) Help() string {
	return "collect SQL statistics from dba_hist_snapshot"

}

func (*ScrapeOracleSnapshot) Version() float64 {
	return 10.2
}

func (s *ScrapeOracleSnapshot) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	if ora.PdbFlag {
		return nil
	}
	duration := time.Since(s.lastScrapeTime)
	if duration < ScrapeIntervalSnapshot {
		log.WithFields(log.Fields{"last_scrape_time": s.lastScrapeTime, "scrape_interval": ScrapeIntervalSnapshot}).Info("skip scape")
		return nil
	}

	err := s.scrape(ctx, dbcli, ch)
	if err != nil {
		return err
	}

	s.lastScrapeTime = time.Now()
	return nil
}

func (s *ScrapeOracleSnapshot) scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric) error {

	// get snapshots list in last n hours, with snapshot id > last processed snapshot id
	// process each snapshot in order
	// record latest processed snapshot
	stats, err := loadContext()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Warning("can not read local stat file")
	}

	snapshots, err := getSnapshots(ctx, dbcli)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("get snapshot has error")
		return err
	}

	log.WithFields(log.Fields{"snapshot_id": s.lastSnapshotId}).Info("Cached snapshot Id")

	for _, s := range snapshots {
		if s.processed(stats) {
			log.WithFields(log.Fields{
				"dbid":           s.dbid,
				"instanceNumber": s.instanceNumber,
				"snapId":         s.snapId,
				"beginTime":      s.beginTime}).Info("snapshot alread processed")
			continue
		}

		err := s.scrapeOne(ctx, dbcli, ch)
		if err != nil {
			return err
		}

		s.markProcessed(stats)
		saveContext(stats)
	}

	return nil
}

func getSqlstat(ctx context.Context, dbcli *dbutil.OracleClient, dbid string, instanceNumber string, snapid string) {

}

func getSnapshots(ctx context.Context, dbcli *dbutil.OracleClient) ([]*snapshot, error) {
	sql := `SELECT to_char(dbid),
   to_char(sys_extract_utc(s.startup_time), 'yyyy-mm-dd hh24:mi:ss') snap_startup_time,
   to_char(sys_extract_utc(s.begin_interval_time), 'yyyy-mm-dd hh24:mi:ss') begin_interval_time,
   to_char(sys_extract_utc(s.end_interval_time), 'yyyy-mm-dd hh24:mi:ss') end_interval_time,
   to_char(s.snap_id), 
   to_char(s.instance_number)
from dba_hist_snapshot  s, v$instance b
where s.end_interval_time >= sysdate - interval '2' hour
and s.INSTANCE_NUMBER = b.INSTANCE_NUMBER`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	var ret []*snapshot

	for _, r := range rows {
		s := snapshot{dbid: r[0].(string),
			startupTime:    r[1].(string),
			beginTime:      r[2].(string),
			endTime:        r[3].(string),
			snapId:         r[4].(string),
			instanceNumber: r[5].(string)}

		ret = append(ret, &s)
	}
	return ret, nil

}

func (s *snapshot) processed(cache map[string]string) bool {
	key := s.dbid + "-" + s.instanceNumber + "-" + s.snapId
	if _, ok := cache[key]; ok {
		return true
	}

	return false
}

func (s *snapshot) markProcessed(cache map[string]string) {
	key := s.dbid + "-" + s.instanceNumber + "-" + s.snapId
	cache[key] = "Yes"
}

func (s *snapshot) scrapeOne(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric) error {
	sql := `select to_char(s.snap_id), 
    to_char(s.begin_interval_time, 'yyyy-mm-dd hh24:mi:ss'), 
    to_char(s.end_interval_time, 'yyyy-mm-dd hh24:mi:ss'),
    t.sql_id, 
    parsing_schema_name,
    to_char(substr(x.sql_text,1,4000)),
    t.version_count, t.executions_delta,
    round(sorts_delta/(decode(executions_delta,0,1,executions_delta)), 4),
    round(disk_reads_delta/(decode(executions_delta,0,1,executions_delta)), 4),
    round(buffer_gets_delta/(decode(executions_delta,0,1,executions_delta)), 4),
    round(cpu_time_delta/(decode(executions_delta,0,1,executions_delta))/1000, 4),
    round(elapsed_time_delta/(decode(executions_delta,0,1,executions_delta))/1000, 4),
    round(parse_calls_delta/(decode(executions_delta,0,1,executions_delta)), 4),
    round(rows_processed_delta/(decode(executions_delta,0,1,executions_delta)), 2)
from dba_hist_snapshot s, dba_hist_sqlstat t, dba_hist_sqltext x
 where s.dbid = :1
   and s.snap_id = :2
   and s.instance_number = :3
   and s.dbid = t.dbid
   and s.instance_number = t.instance_number
   and s.snap_id = t.snap_id
   and t.sql_id = x.sql_id
   and t.dbid = x.dbid
   and (t.buffer_gets_delta > 0 or t.executions_delta > 0)
`
	params := []interface{}{s.dbid, s.snapId, s.instanceNumber}
	rows, err := dbcli.FetchRowsWithContext(ctx, sql, params...)
	if err != nil {
		return err
	}

	for _, r := range rows {
		ch <- prometheus.MustNewConstMetric(oracleSnapshotSqlStatAllDesc, prometheus.GaugeValue, 1,
			r[0].(string), r[1].(string), r[2].(string), r[3].(string), r[4].(string), r[5].(string),
			formatFloat64(r[6].(float64)),
			formatFloat64(r[7].(float64)),
			formatFloat64(r[8].(float64)),
			formatFloat64(r[9].(float64)),
			formatFloat64(r[10].(float64)),
			formatFloat64(r[11].(float64)),
			formatFloat64(r[12].(float64)),
			formatFloat64(r[13].(float64)),
			formatFloat64(r[14].(float64)),
		)
	}

	return nil
}
