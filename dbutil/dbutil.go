package dbutil

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"

	_ "github.com/godror/godror"
	"gopkg.in/yaml.v2"
)

type OracleConfig struct {
	Dsn         string
	Host        string
	Port        int
	Username    string
	Password    string
	ServiceName string
	Sid         string
	Pdbs        []string
	CurrentPdb  string
}

type OracleClient struct {
	configFile string
	C          OracleConfig
	dbconn     *sql.DB
}

type Row []interface{}

func NewOracleClient(configFile string) *OracleClient {

	cli := OracleClient{configFile: configFile}

	return &cli

}

func (c *OracleClient) Init() error {
	err := c.initConfig()
	if err != nil {
		return err
	}

	err = c.initConnection()
	if err != nil {
		return err
	}

	return err
}

func (c *OracleClient) ReInitWithPdb(pdb string) error {
	err := c.CloseConnection()
	if err != nil {
		return err
	}
	c.setCurrentPdb(pdb)
	err = c.initConnection()
	return err
}

func (c *OracleClient) CloseConnection() error {
	if c.dbconn != nil {
		err := c.dbconn.Close()
		return err
	}
	return nil
}

func (c *OracleClient) initConfig() error {
	buf, err := ioutil.ReadFile(c.configFile)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(buf, &c.C)
	return err
}

func (c *OracleClient) initConnection() error {
	conn, err := c.Connect()
	if err != nil {
		return err
	}

	err = conn.Ping()
	if err != nil {
		return err
	}

	c.dbconn = conn
	return nil
}

func (c *OracleClient) Connect() (*sql.DB, error) {
	// fmt.Printf("\nget new Connect\n")
	dsn := c.getDatasource()
	log.WithFields(log.Fields{"conn str": c.getConnectionStr(), "user": c.C.Username}).Info("Connect to Oracle")

	db, err := sql.Open("godror", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	return db, nil
}

func (c *OracleClient) getDatasource() string {
	connStr := c.getConnectionStr()
	dsn := fmt.Sprintf(`user="%s" password="%s" connectString="%s"`, c.C.Username, c.C.Password, connStr)
	return dsn
}

func (c *OracleClient) getConnectionStr() string {
	if c.C.CurrentPdb != "" {
		connStr := fmt.Sprintf("%s:%d/%s", c.C.Host, c.C.Port, c.C.CurrentPdb)
		return connStr
	}

	if c.C.Dsn != "" {
		return c.C.Dsn
	} else {
		connStr := fmt.Sprintf("%s:%d/%s", c.C.Host, c.C.Port, c.C.ServiceName)
		return connStr
	}

}

func (c *OracleClient) setCurrentPdb(pdb string) {
	c.C.CurrentPdb = pdb
}

func (c *OracleClient) ExecuteQuery(querytext string, params ...interface{}) (*sql.Rows, error) {
	ctx := context.Background()
	return c.ExecuteQueryWithContext(ctx, querytext, params...)
}

func (c *OracleClient) FetchRowsWithContext(ctx context.Context, querytext string, params ...interface{}) ([]Row, error) {
	rs, err := c.ExecuteQueryWithContext(ctx, querytext, params...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	rows, err := fetchRows(rs)
	return rows, err
}

func (c *OracleClient) ExecuteQueryWithContext(ctx context.Context, querytext string, params ...interface{}) (*sql.Rows, error) {
	if c.dbconn == nil {
		return nil, fmt.Errorf("DB Connection is Nil")
	}

	rows, err := c.dbconn.QueryContext(ctx, querytext, params...)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "query": querytext}).Warn("Execute Query")
	}
	return rows, err

}

func fetchRows(rows *sql.Rows) ([]Row, error) {
	var ret []Row

	columnTypes, _ := rows.ColumnTypes()
	// for _, col_type := range columnTypes {
	// 	fmt.Printf("Column Types:%s %v\n", col_type.Name(), col_type.ScanType())
	// }

	var n []interface{}
	for ii := 0; ii < len(columnTypes); ii++ {
		n = append(n, getField(columnTypes[ii].DatabaseTypeName()))
	}

	for rows.Next() {
		var r Row

		err := rows.Scan(n...)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("Scan Row Error")
			return nil, err
		}
		for i, _ := range columnTypes {
			log.WithFields(log.Fields{"column index": i,
				"column_name": columnTypes[i].Name(),
				"scan type":   columnTypes[i].ScanType(),
				"db type":     columnTypes[i].DatabaseTypeName()}).Debug("Col Info")
			vv := getFieldValue(n[i], columnTypes[i].DatabaseTypeName())
			// fmt.Printf("Got Row C: %v\n", vv)
			log.WithFields(log.Fields{"column": columnTypes[i].Name(), "value": vv}).Debug("Got Field")
			r = append(r, vv)
		}
		ret = append(ret, r)
	}
	return ret, nil
}

func DumpRows(rows *sql.Rows) {
	columnTypes, _ := rows.ColumnTypes()
	for _, col_type := range columnTypes {
		fmt.Printf("Column Types:%s %v\n", col_type.Name(), col_type.ScanType())
	}

	var n []interface{}
	for ii := 0; ii < len(columnTypes); ii++ {
		// var v interface{}

		n = append(n, getField(columnTypes[ii].DatabaseTypeName()))
	}

	for rows.Next() {
		err := rows.Scan(n...)
		if err != nil {
			fmt.Printf("Scan error: %s", err)
			return
		}
		for i, _ := range columnTypes {
			fmt.Printf("Col:%d %s %s %s,  ", i, columnTypes[i].Name(), columnTypes[i].ScanType(), columnTypes[i].DatabaseTypeName())
			vv := getFieldValue(n[i], columnTypes[i].DatabaseTypeName())
			fmt.Printf("Got Row C: %v\n", vv)

		}
	}
}

func getField(typename string) interface{} {
	switch typename {
	// case "VARCHAR2":
	// 	return new(sql.NullString)
	case "VARCHAR":
		return new(sql.NullString)
	case "CHAR":
		return new(sql.NullString)
	case "CLOB":
		return new(sql.NullString)
	case "NUMBER":
		return new(sql.NullFloat64)
	case "RAW":
		return new([]byte)
	case "TIMESTAMP":
		return new(sql.NullTime)
	case "DATE":
		return new(sql.NullTime)
	}
	return new(interface{})
}

func getFieldValue(val interface{}, typename string) interface{} {
	switch typename {
	// case "VARCHAR2":
	// 	return *val.(*sql.NullString)
	case "VARCHAR":
		return *val.(*sql.NullString)
	case "CHAR":
		return *val.(*sql.NullString)
	case "CLOB":
		return *val.(*sql.NullString)
	case "NUMBER":
		v1 := *val.(*sql.NullFloat64)
		var v2 interface{}
		if v1.Valid {
			v2 = v1.Float64
		} else {
			v2 = -1.0
		}
		return v2

	case "RAW":
		return fmt.Sprintf("%x", *val.(*[]byte))
	case "TIMESTAMP":
		return *val.(*sql.NullTime)
	case "DATE":
		return *val.(*sql.NullTime)
	}
	return *val.(*interface{})
}

// typename -> new value
func getScanValue(typename string) {

}

// value, typenamne -> interface{}
