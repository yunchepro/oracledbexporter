package main

import (
	"fmt"
	"os"

	"time"

	"context"
	"sync"

	"yunche.pro/dtsre/oracledb_exporter/dbutil"
)

var (
	configFile = "config.yaml"
)

func main() {

	dbclient := dbutil.NewOracleClient(configFile)

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {

		defer wg.Done()
		for i := 0; i < 100; i++ {
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			executeWithTimeout(ctx, dbclient, 9999999)
			fmt.Printf("==== %d =====\n", i)
			// time.Sleep(time.Millisecond)
		}
	}()

	// time.Sleep(time.Second)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			executeWithTimeout(ctx, dbclient, 10000001)
			fmt.Printf("***** %d *****\n", i+100)
			// time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

}

func executeWithTimeout(ctx context.Context, dbclient *dbutil.OracleClient, idx int) {
	// ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	// dbclient := dbutil.NewOracleClient()

	// sql := "select * from v$sqlarea where rownum < :1"
	sql := fmt.Sprintf("select /*  %d */ count(*) from dba_tables a, dba_tables b where rownum <= :1", idx)

	rows, err := dbclient.ExecuteQueryWithContext(ctx, sql, idx)
	if rows != nil {
		defer func() {
			fmt.Printf("close Rows\n")
			rows.Close()
		}()
	}
	if err != nil {
		fmt.Printf("Prepare Query(%s) Error: %s\n", sql, err)
		return
	}

	dbutil.DumpRows(rows)
}

func executePrepared() {
	dbclient := dbutil.NewOracleClient(configFile)
	// sql := "select sql_id, sql_fulltext from v$sql where sql_id like :1 and rownum < :2"
	// sql := "select sysdate, t.* from v$sql t where rownum  < :2"
	sql := "select * from v$sqlarea where rownum < :1"

	rows, err := dbclient.ExecuteQuery(sql, 10)
	if err != nil {
		fmt.Printf("Prepare Query Error: %s", err)
		return
	}

	dbutil.DumpRows(rows)

}

func executeQuery() {
	path := os.Getenv("DYLD_LIBRARY_PATH")
	fmt.Printf("Path: %s\n", path)
	cli := dbutil.NewOracleClient(configFile)
	db, err := cli.Connect()
	if err != nil {
		fmt.Printf("Connect error: %s\n", err)
		return
	}

	err = db.Ping()
	if err != nil {
		fmt.Printf("DB Ping Error: %s\n", err)
	}
	// sql := "select sysdate from dual"
	// sql := "select PCT_FREE from dba_tables where rownum < 2"
	// sql := "select SADDR from v$session where rownum < 2"
	sql := "select sql_fulltext from v$sql where rownum < 10"

	rows, err := db.Query(sql)
	if err != nil {
		fmt.Printf("Query Error: %s", err)
		return
	}

	columnTypes, _ := rows.ColumnTypes()
	for _, col_type := range columnTypes {
		fmt.Printf("Column Types:%s %v\n", col_type.Name(), col_type.ScanType())

	}

	var n []interface{}
	for ii := 0; ii < len(columnTypes); ii++ {
		// var v interface{}

		n = append(n, new(string))
	}

	for rows.Next() {
		err = rows.Scan(n...)
		if err != nil {
			fmt.Printf("Scan error: %s", err)
			return
		}
		for i, _ := range columnTypes {
			fmt.Printf("Col:%d %s %s %s,  ", i, columnTypes[i].Name(), columnTypes[i].ScanType(), columnTypes[i].DatabaseTypeName())
			cc := *n[i].(*string)
			fmt.Printf("Got Row C: %v,hex(%x)\n", cc, cc)
		}
	}
}
