package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

// instance info
// uptime
// db status, instance status
var (
	instanceInfoCols = []string{"instance_number", "instance_name", "host_name",
		"version", "status", "parallel", "thread", "archiver",
		"instance_role", "database_status"}

	dbInfoCols = []string{"dbid", "db_name", "db_unique_name", "created",
		"log_mode", "open_mode", "protection_mode",
		"database_role", "platform_name", "con_id", "con_name"}

	allInfoCols = append(instanceInfoCols, dbInfoCols...)

	oracleInfoDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "instance", "info"),
		"Oracle Instance Info",
		allInfoCols, nil)
)

type ScrapeOracleInfo struct{}

func (ScrapeOracleInfo) Name() string {
	return "oracle_instance_info"
}

func (ScrapeOracleInfo) Help() string {
	return "collect stats from v$database, v$instance"

}

func (ScrapeOracleInfo) Version() float64 {
	return 10.2
}

func (ScrapeOracleInfo) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	if ora.PdbFlag {
		return nil
	}

	ch <- prometheus.MustNewConstMetric(
		oracleInfoDesc, prometheus.GaugeValue,
		ora.Uptime,
		ora.InstanceNumber,
		ora.InstanceName,
		ora.HostName,
		ora.Version,
		ora.Status,
		ora.Parallel,
		ora.ThreadNum,
		ora.Archiver,
		ora.InstanceRole,
		ora.DatabaseStatus,
		ora.Dbid,
		ora.DbName,
		ora.DbUniqueName,
		ora.Created,
		ora.LogMode,
		ora.OpenMode,
		ora.ProtectionMode,
		ora.DatabaseRole,
		ora.PlatformName,
		ora.ConId,
		ora.ConName,
	)

	return nil
}
