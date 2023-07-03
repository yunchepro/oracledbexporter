package collector

import (
	"context"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleOsStatCpuDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "osstat", "cpu_total"),
		"Oracle OS Stats Cpu Total",
		[]string{"mode"}, nil)

	regCpu = regexp.MustCompile(`(\w+)_time`)
)

type ScrapeOracleOsStat struct{}

func (ScrapeOracleOsStat) Name() string {
	return "oracle_os_stat"
}

func (ScrapeOracleOsStat) Help() string {
	return "collect stats from v$osstat"

}

func (ScrapeOracleOsStat) Version() float64 {
	return 10.2
}

func (ScrapeOracleOsStat) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {
	if ora.PdbFlag {
		return nil
	}
	sql := `select lower(stat_name) as stat_name, value from v$osstat
where stat_name in (
  'NUM_CPUS', 
  'IDLE_TIME', 
  'BUSY_TIME', 
  'USER_TIME', 
  'SYS_TIME', 
  'IOWAIT_TIME', 
  'NICE_TIME', 
  'LOAD',
  'PHYSICAL_MEMORY_BYTES',
  'NUM_CPU_CORES',
  'NUM_CPU_SOCKETS'
  )`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return err
	}
	for _, r := range rows {
		stat_name := r[0].(string)
		val := r[1].(float64)

		match := regCpu.FindStringSubmatch(stat_name)
		if match == nil {
			ch <- prometheus.MustNewConstMetric(
				newDesc("osstat", stat_name, "Metric from v$osstat"), prometheus.GaugeValue, val)

			continue
		}
		ch <- prometheus.MustNewConstMetric(
			oracleOsStatCpuDesc, prometheus.CounterValue, val, match[1])

	}
	return nil
}
