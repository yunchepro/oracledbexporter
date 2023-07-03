package collector

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

type DbInfo struct {
	Dbid           string
	DbName         string
	DbUniqueName   string
	Created        string
	LogMode        string
	OpenMode       string
	ProtectionMode string
	DatabaseRole   string
	PlatformName   string
}

type InstanceInfo struct {
	InstanceNumber string
	InstanceName   string
	HostName       string
	Version        string
	Status         string
	Parallel       string
	ThreadNum      string
	Archiver       string
	StartupTime    string
	Uptime         float64
	InstanceRole   string
	DatabaseStatus string
	VersionNum     float64
}

type PdbInfo struct {
	ConName string
	ConId   string
}

type InstanceInfoAll struct {
	InstanceInfo
	DbInfo
	PdbInfo
	PdbFlag bool
}

func getOracleInfoAll(ctx context.Context, dbcli *dbutil.OracleClient) (*InstanceInfoAll, error) {
	instanceInfo, err := getInstanceInfo(ctx, dbcli)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Get Oracle Instance Info Error")
		return nil, err
	}

	dbInfo, err := getDbInfo(ctx, dbcli)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Get Oracle DB Info Error")
		return nil, err
	}

	pdbInfo := &PdbInfo{}

	if instanceInfo.VersionNum > 12.0 {
		pdbInfo, err = getPdbInfo(ctx, dbcli)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("Get Oracle PDB Info Error")
			return nil, err
		}
	}
	instanceInfoAll := InstanceInfoAll{*instanceInfo, *dbInfo, *pdbInfo, false}

	return &instanceInfoAll, nil
}

func getOracleInfo(ctx context.Context, dbcli *dbutil.OracleClient) (*DbInfo, *InstanceInfo, error) {
	instance_info, err := getInstanceInfo(ctx, dbcli)
	if err != nil {
		return nil, nil, err
	}

	db_info, err := getDbInfo(ctx, dbcli)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Get Oracle Info Error")
		return nil, nil, err
	}

	return db_info, instance_info, nil
}

func getDbInfo(ctx context.Context, dbcli *dbutil.OracleClient) (*DbInfo, error) {
	sql := `select /* dtagent */ to_char(dbid), name, db_unique_name, 
to_char(created, 'yyyy-mm-dd hh24:mi:ss') as created, log_mode, 
open_mode, protection_mode, database_role, platform_name 
from v$database`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	r := rows[0]

	dbinfo := DbInfo{}
	dbinfo.Dbid = r[0].(string)
	dbinfo.DbName = r[1].(string)
	dbinfo.DbUniqueName = r[2].(string)
	dbinfo.Created = r[3].(string)
	dbinfo.LogMode = r[4].(string)
	dbinfo.OpenMode = r[5].(string)
	dbinfo.ProtectionMode = r[6].(string)
	dbinfo.DatabaseRole = r[7].(string)
	dbinfo.PlatformName = r[8].(string)

	return &dbinfo, nil
}

func getInstanceInfo(ctx context.Context, dbcli *dbutil.OracleClient) (*InstanceInfo, error) {

	sql := `select to_char(instance_number), instance_name, host_name, version, status, 
parallel, to_char(thread#), archiver, to_char(startup_time, 'yyyy-mm-dd hh24:mi:ss') as startup_time, 
(sysdate - startup_time)*86400  as uptime, instance_role, database_status 
from v$instance`

	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	r := rows[0]
	info := InstanceInfo{}
	info.InstanceNumber = r[0].(string)
	info.InstanceName = r[1].(string)
	info.HostName = r[2].(string)
	info.Version = r[3].(string)
	info.Status = r[4].(string)
	info.Parallel = r[5].(string)
	info.ThreadNum = r[6].(string)
	info.Archiver = r[7].(string)
	info.StartupTime = r[8].(string)
	info.Uptime = r[9].(float64)
	info.InstanceRole = r[10].(string)
	info.DatabaseStatus = r[11].(string)

	versionNum, err := parseVersion(info.Version)
	if err != nil {
		parseErr := fmt.Errorf("can not parse oracle version (%s): %s", info.Version, err)
		return nil, parseErr
	}
	info.VersionNum = versionNum
	return &info, nil
}

func getPdbInfo(ctx context.Context, dbcli *dbutil.OracleClient) (*PdbInfo, error) {
	sql := `select sys_context('userenv', 'con_name'), sys_context('userenv', 'con_id')  from dual`
	rows, err := dbcli.FetchRowsWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	conName := rows[0][0].(string)
	conId := rows[0][1].(string)
	pdbInfo := PdbInfo{ConName: conName, ConId: conId}
	return &pdbInfo, nil
}
