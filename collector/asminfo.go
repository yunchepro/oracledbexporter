package collector

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleAsmInfoDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "asm_diskgroup", "stat"),
		"Oracle Asm Diskgrup Stats",
		[]string{"group_name", "state", "type", "mode"}, nil)
)

type ScrapeOracleAsmStat struct{}

func (ScrapeOracleAsmStat) Name() string {
	return "oracle_asm_diskgroup"
}

func (ScrapeOracleAsmStat) Help() string {
	return "collect stats from v$asm_diskgroup_stat"
}

func (ScrapeOracleAsmStat) Version() float64 {
	return 10.2
}

func (ScrapeOracleAsmStat) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {

	diskGroups, err := getAsmDiskgroup(ctx, dbcli)
	if err != nil {
		return err
	}
	for _, r := range diskGroups {
		ch <- prometheus.MustNewConstMetric(
			oracleAsmInfoDesc, prometheus.GaugeValue, r.spaceTotal,
			r.groupName, r.state, r.groupType, "total")

		ch <- prometheus.MustNewConstMetric(
			oracleAsmInfoDesc, prometheus.GaugeValue, r.spaceFree,
			r.groupName, r.state, r.groupType, "free")

		ch <- prometheus.MustNewConstMetric(
			oracleAsmInfoDesc, prometheus.GaugeValue, r.spaceUsed,
			r.groupName, r.state, r.groupType, "used")

		ch <- prometheus.MustNewConstMetric(
			oracleAsmInfoDesc, prometheus.GaugeValue, r.spaceUsedPct,
			r.groupName, r.state, r.groupType, "used_pct")

		ch <- prometheus.MustNewConstMetric(
			oracleAsmInfoDesc, prometheus.GaugeValue, r.requiredMirrorFree,
			r.groupName, r.state, r.groupType, "required_mirror_free")

		ch <- prometheus.MustNewConstMetric(
			oracleAsmInfoDesc, prometheus.GaugeValue, r.useablFileMb,
			r.groupName, r.state, r.groupType, "useable_file_mb")

		ch <- prometheus.MustNewConstMetric(
			oracleAsmInfoDesc, prometheus.GaugeValue, r.offlineDisks,
			r.groupName, r.state, r.groupType, "offline_disks")
	}
	return nil
}

type AsmDiskgroupStat struct {
	groupName          string
	state              string
	groupType          string
	spaceTotal         float64
	spaceFree          float64
	spaceUsed          float64
	requiredMirrorFree float64
	useablFileMb       float64
	offlineDisks       float64
	spaceUsedPct       float64
}

func getAsmDiskgroup(ctx context.Context, dbcli *dbutil.OracleClient) ([]AsmDiskgroupStat, error) {
	sql := `select  name as group_name,
    state,
    type,
    total_mb as space_total,
    free_mb as space_free,
    total_mb - free_mb as space_used,
    required_mirror_free_mb,
    usable_file_mb,
    offline_disks
from v$asm_diskgroup_stat`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	var result []AsmDiskgroupStat

	for _, r := range rows {
		group := AsmDiskgroupStat{
			groupName:          r[0].(string),
			state:              r[1].(string),
			groupType:          r[2].(string),
			spaceTotal:         r[3].(float64),
			spaceFree:          r[4].(float64),
			spaceUsed:          r[5].(float64),
			requiredMirrorFree: r[6].(float64),
			useablFileMb:       r[7].(float64),
			offlineDisks:       r[8].(float64),
		}
		group.spaceUsedPct = 100.0 * group.spaceUsed / group.spaceTotal
		result = append(result, group)
	}
	return result, nil
}
