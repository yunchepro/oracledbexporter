package collector

import (
	"context"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	oracleTablespaceDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "tablespace", "stat"),
		"Oracle Tablespace Stats",
		[]string{"tablespace_name", "contents", "status", "mode", "con_id", "con_name"}, nil)
)

type TablespaceInfo struct {
	tablespaceName    string
	contents          string
	status            string
	spaceTotal        float64
	spaceExtensible   float64
	spaceFree         float64
	spaceUsed         float64
	usedPct           float64
	usedPctExtensible float64
	blockSize         float64
	recyclebinUsed    float64
}

type ScrapeOracleTablespaceStat struct{}

func (ScrapeOracleTablespaceStat) Name() string {
	return "oracle_tablespace"
}

func (ScrapeOracleTablespaceStat) Help() string {
	return "collect tablespace info from dba_tablespaces"

}

func (ScrapeOracleTablespaceStat) Version() float64 {
	return 10.2
}

func (ScrapeOracleTablespaceStat) Scrape(ctx context.Context, dbcli *dbutil.OracleClient, ch chan<- prometheus.Metric, ora *InstanceInfoAll) error {

	tbsInfo, err := getTbsSpaceInfo(ctx, dbcli)
	if err != nil {
		return err
	}
	for _, tbs := range tbsInfo {
		ch <- prometheus.MustNewConstMetric(
			oracleTablespaceDesc, prometheus.GaugeValue, tbs.spaceTotal,
			tbs.tablespaceName, tbs.contents, tbs.status, "total", ora.ConId, ora.ConName)

		ch <- prometheus.MustNewConstMetric(
			oracleTablespaceDesc, prometheus.GaugeValue, tbs.spaceExtensible,
			tbs.tablespaceName, tbs.contents, tbs.status, "extensible", ora.ConId, ora.ConName)

		ch <- prometheus.MustNewConstMetric(
			oracleTablespaceDesc, prometheus.GaugeValue, tbs.spaceUsed,
			tbs.tablespaceName, tbs.contents, tbs.status, "used", ora.ConId, ora.ConName)

		ch <- prometheus.MustNewConstMetric(
			oracleTablespaceDesc, prometheus.GaugeValue, tbs.usedPct,
			tbs.tablespaceName, tbs.contents, tbs.status, "used_pct", ora.ConId, ora.ConName)

		ch <- prometheus.MustNewConstMetric(
			oracleTablespaceDesc, prometheus.GaugeValue, tbs.usedPctExtensible,
			tbs.tablespaceName, tbs.contents, tbs.status, "used_pct_ext", ora.ConId, ora.ConName)

		ch <- prometheus.MustNewConstMetric(
			oracleTablespaceDesc, prometheus.GaugeValue, tbs.spaceFree,
			tbs.tablespaceName, tbs.contents, tbs.status, "free", ora.ConId, ora.ConName)

		ch <- prometheus.MustNewConstMetric(
			oracleTablespaceDesc, prometheus.GaugeValue, tbs.recyclebinUsed,
			tbs.tablespaceName, tbs.contents, tbs.status, "recyclebin_used", ora.ConId, ora.ConName)
	}
	return nil
}

func getTbsSpaceInfo(ctx context.Context, dbcli *dbutil.OracleClient) ([]*TablespaceInfo, error) {
	tbsList, err := getTbsMeta(ctx, dbcli)
	if err != nil {
		return nil, err
	}

	tbsUsedSpace, err := getTbsUsedSpace(ctx, dbcli)
	if err != nil {
		return nil, err
	}

	tbsFreeSpace, err := getTbsFreeSpace(ctx, dbcli)
	if err != nil {
		return nil, err
	}

	tempUsed, err := getTempTablespaceUsed(ctx, dbcli)
	if err != nil {
		return nil, err
	}

	recyclebinUsed, err := getTbsRecyclebinUsed(ctx, dbcli)
	if err != nil {
		return nil, err
	}

	for _, tbs := range tbsList {
		if usedSpace, ok := tbsUsedSpace[tbs.tablespaceName]; ok {
			tbs.spaceTotal = usedSpace[0]
			tbs.spaceExtensible = usedSpace[1]
		}

		if recyclebin, ok := recyclebinUsed[tbs.tablespaceName]; ok {
			tbs.recyclebinUsed = recyclebin * tbs.blockSize
		}

		if tbs.contents != "TEMPORARY" {
			if freeSpace, ok := tbsFreeSpace[tbs.tablespaceName]; ok {
				tbs.spaceFree = freeSpace
			}
			tbs.spaceUsed = tbs.spaceTotal - tbs.spaceFree
		} else {
			// temporary tablespace used should use v$sort_segment
			if usedBlks, ok := tempUsed[tbs.tablespaceName]; ok {
				tbs.spaceUsed = usedBlks * tbs.blockSize
				tbs.spaceFree = tbs.spaceTotal - tbs.spaceUsed
			}
		}

		tbs.usedPct = tbs.spaceUsed / tbs.spaceTotal * 100
		tbs.usedPctExtensible = tbs.spaceUsed / (tbs.spaceTotal + tbs.spaceExtensible) * 100
	}
	return tbsList, nil
}

func getTbsMeta(ctx context.Context, dbcli *dbutil.OracleClient) ([]*TablespaceInfo, error) {
	sql := `select tablespace_name, contents, status, block_size from dba_tablespaces`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	var tbsList []*TablespaceInfo
	for _, r := range rows {
		tbs := TablespaceInfo{}
		tbs.tablespaceName = r[0].(string)
		tbs.contents = r[1].(string)
		tbs.status = r[2].(string)
		tbs.blockSize = r[3].(float64)
		tbsList = append(tbsList, &tbs)
	}
	return tbsList, nil
}

func getTbsUsedSpace(ctx context.Context, dbcli *dbutil.OracleClient) (map[string][]float64, error) {
	sql := `select
  tablespace_name,
  sum(BYTES) as space_total,
  sum(case when AUTOEXTENSIBLE='YES' then maxbytes - bytes else 0 end) as  space_extensible,
  count(*) as num_files
from dba_data_files
where status = 'AVAILABLE'
group by tablespace_name
union all
select
    tablespace_name,
    sum(BYTES) as space_total,
    sum(case
    when AUTOEXTENSIBLE='YES' then maxbytes - bytes
    else 0 end) as space_extensible,
    count(*) as num_files
from DBA_TEMP_FILES
where status = 'ONLINE'
group by tablespace_name`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	tbsUsed := make(map[string][]float64)
	for _, r := range rows {
		tbsName := r[0].(string)
		spaceTotal := r[1].(float64)
		spaceExtensible := r[2].(float64)
		numFiles := r[3].(float64)
		tbsUsed[tbsName] = []float64{spaceTotal, spaceExtensible, numFiles}
	}
	return tbsUsed, nil
}

func getTbsFreeSpace(ctx context.Context, dbcli *dbutil.OracleClient) (map[string]float64, error) {
	result, err := getTbsFreeSpaceNonRecyclebin(ctx, dbcli)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Info("getTbsFreeSpace error")
		errmsg := err.Error()
		if strings.Contains(errmsg, "ORA-00942") || strings.Contains(errmsg, "ORA-01031") {
			log.Info("No dba_free_space_nonrecyclebin, fallback to dba_free_space")
			return getTbsFreeSpaceWithRecyclebin(ctx, dbcli)
		}
		return nil, err
	}

	return result, err
}

func getTbsFreeSpaceWithRecyclebin(ctx context.Context, dbcli *dbutil.OracleClient) (map[string]float64, error) {
	sql := `select tablespace_name, sum(bytes) as space_free
from dba_free_space
group by tablespace_name`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	tbsFree := make(map[string]float64)
	for _, r := range rows {
		tbsName := r[0].(string)
		spaceFree := r[1].(float64)
		tbsFree[tbsName] = spaceFree
	}
	return tbsFree, nil
}

func getTbsFreeSpaceNonRecyclebin(ctx context.Context, dbcli *dbutil.OracleClient) (map[string]float64, error) {
	sql := `select tablespace_name, sum(bytes) as space_free
from dba_free_space_nonrecyclebin
group by tablespace_name`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	tbsFree := make(map[string]float64)
	for _, r := range rows {
		tbsName := r[0].(string)
		spaceFree := r[1].(float64)
		tbsFree[tbsName] = spaceFree
	}
	return tbsFree, nil
}

func getTbsRecyclebinUsed(ctx context.Context, dbcli *dbutil.OracleClient) (map[string]float64, error) {
	sql := `select ts_name, sum(space) from dba_recyclebin group by ts_name`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	recyclebin := make(map[string]float64)
	for _, r := range rows {
		tbsName := r[0].(string)
		recyclebinUsed := r[1].(float64)
		recyclebin[tbsName] = recyclebinUsed
	}
	return recyclebin, nil
}

func getTempTablespaceUsed(ctx context.Context, dbcli *dbutil.OracleClient) (map[string]float64, error) {
	sql := `select tablespace_name, sum(used_blocks)
from V$SORT_SEGMENT
group by tablespace_name`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	tempSpaces := make(map[string]float64)
	for _, r := range rows {
		tbsName := r[0].(string)
		blocksUsed := r[1].(float64)
		tempSpaces[tbsName] = blocksUsed
	}
	return tempSpaces, nil
}
