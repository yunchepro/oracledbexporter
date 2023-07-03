package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleBackupInfoDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "backupset", "size"),
		"Oracle Backupset Info",
		[]string{"bs_key", "recid", "stamp", "start_time", "completion_time", "backup_type", "con_id", "con_name"}, nil)
)

type ScrapeOracleBackupInfo struct {
	lastSnapshotId float64
	lastScrapeTime map[string]time.Time
}

func (*ScrapeOracleBackupInfo) Name() string {
	return "oracle_backup_set"
}

func (*ScrapeOracleBackupInfo) Help() string {
	return "collect backup set information"
}

func (*ScrapeOracleBackupInfo) Version() float64 {
	return 10.2
}

func (s *ScrapeOracleBackupInfo) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	if s.lastScrapeTime == nil {
		s.lastScrapeTime = make(map[string]time.Time)
	}

	if lastTime, ok := s.lastScrapeTime[ora.ConId]; ok {
		duration := time.Since(lastTime)

		if duration < ScrapeIntervalSnapshot {
			log.WithFields(log.Fields{"last_scrape_time": s.lastScrapeTime, "scrape_interval": ScrapeIntervalSnapshot}).Info("skip scape")
			return nil
		}
	}

	err := s.scrape(ctx, dbcli, ch, ora)
	if err != nil {
		return err
	}

	s.lastScrapeTime[ora.ConId] = time.Now()
	return nil
}

func (s *ScrapeOracleBackupInfo) scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	var sql string
	if ora.VersionNum < 12.0 {
		sql = `select bs_key, 
    recid, 
    stamp,
    to_char(start_time, 'yyyy-mm-dd hh24:mi:ss'),
    to_char(completion_time, 'yyyy-mm-dd hh24:mi:ss'),
    elapsed_seconds, 
    output_bytes,
    backup_type,
    0 as con_id
from v$backup_set_details`
	} else {
		sql = `select
    bs_key, 
    recid,
    stamp,
    to_char(start_time, 'yyyy-mm-dd hh24:mi:ss'),
    to_char(completion_time, 'yyyy-mm-dd hh24:mi:ss'),
    elapsed_seconds,
    output_bytes,
    backup_type,
    con_id
from v$backup_set_details`
	}

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return err
	}

	for _, r := range rows {
		ch <- prometheus.MustNewConstMetric(
			oracleBackupInfoDesc, prometheus.GaugeValue, r[6].(float64),
			formatFloat64(r[0].(float64)),
			formatFloat64(r[1].(float64)),
			formatFloat64(r[2].(float64)),
			r[3].(string),
			r[4].(string),
			r[7].(string),
			formatFloat64(r[8].(float64)),
			ora.ConName,
		)
	}
	return nil
}
