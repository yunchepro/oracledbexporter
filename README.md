Oracle 10g, 11g, 12c, 19c, 21c

# 特性

* 支持10g以上版本
* 支持12c container db

# 使用说明

## 构建
```
export GOPROXY=https://goproxy.cn,direct
go build
```

## 启动exporter

```
./oracledb_exporter --help
usage: oracledb_exporter [<flags>]

Flags:
  --help                        Show context-sensitive help (also try --help-long and --help-man).
  --web.telemetry-path="/metrics"
                                Path under which to expose metrics.
  --web.listen-address=":9205"  Address to listen on for web interface and telemetry.
  --timeout-offset=0.25         Offset to subtract from timeout in seconds.
  --config="oracledb_exporter.yaml"
                                exporter config file

```

example:
```
./oracledb_exporter --config="oracledb_exporter.21c.yaml"
```

## exporter配置文件

```
host: 172.16.121.164
port: 1521
username: c##lazybug
password: lazybug21c
serviceName: orcl21c
pdbs:
  - orclpdb1
```

* host: oracle服务器IP
* port: oracle监听端口
* username: 监控账号名
* password: 监控账号密码
* serviceName: Oracle服务名。12C及以上版本请指定为CDB的服务名
* pdbs: 12C及以上版本，指定需要采集的PDB数据库列表。可登陆Oracle，通过show pdbs查看pdb列表



## 采集指标

* 实例信息(db, instance)
* oracle stats
* wait events
* block session
* tablespace
* parameters
* awr top sql
* backup


# 依赖

* golang 1.17
* oracle instant client 11g


## Oracle Instant Client安装（mac os）

1. 下载instantclient 11.2 ， 解压缩到如下路径

* instantclient-basic-macos.x64-11.2.0.4.0.zip: 必须
* instantclient-sqlplus-macos.x64-11.2.0.4.0.zip: 非必须，oracle sqlplus


```
/Users/lazybug/Downloads/instantclient_11_2
```

2. 设置环境变量

export DYLD_LIBRARY_PATH=/Users/lazybug/Downloads/instantclient_11_2/

## 常见问题

### 无法找到libclntsh.dylib

```
DB Ping Error: ORA-00000: DPI-1047: Cannot locate a 64-bit Oracle Client library: "dlopen(libclntsh.dylib, 0x0001): tried: 'libclntsh.dylib' (no such file), '/usr/local/lib/libclntsh.dylib' (no such file), '/usr/lib/libclntsh.dylib' (no such file), '/Users/lazybug/source/dtsre/oracledb_exporter/cmd/libclntsh.dylib' (no such file)". See https://oracle.github.io/odpi/doc/installation.html#macos for help
```

```
DB Ping Error: ORA-00000: DPI-1047: Cannot locate a 64-bit Oracle Client library: "dlopen(libclntsh.dylib, 0x0001): Library not loaded: /ade/dosulliv_ldapmac/oracle/ldap/lib/libnnz11.dylib
  Referenced from: /Users/lazybug/Downloads/instantclient_11_2/libclntsh.dylib.11.1
  Reason: tried: '/ade/dosulliv_ldapmac/oracle/ldap/lib/libnnz11.dylib' (no such file), '/usr/local/lib/libnnz11.dylib' (no such file), '/usr/lib/libnnz11.dylib' (no such file)". See https://oracle.github.io/odpi/doc/installation.html#macos for help成功: 进程退出代码 0.

```

添加软链接:

```
/usr/local/lib/libclntsh.dylib -> /Users/lazybug/Downloads/instantclient_11_2/libclntsh.dylib.11.1
```

### could not generate unique server group name
```
DB Ping Error: dpoPool_create user=scott extAuth=0: ORA-24408: could not generate unique server group name
```

在/etc/hosts加入域名绑定
```
/etc/hosts 
127.0.0.1 lazybug
```

### TNS:listener does not currently know of service requested 
```
DB Ping Error: dpoPool_create user=lazybug extAuth=0: ORA-12514: TNS:listener does not currently know of service requested in connect descriptor
```

### Warning Timezone
```
godror WARNING: discrepancy between DBTIMEZONE ("+00:00"=0) and SYSTIMESTAMP ("+08:00"=800) - set connection timezone, see https://github.com/godror/godror/blob/master/doc/timezone.md

```

### Cannot locate a 64-bit Oracle Client library
```
DB Ping Error: ORA-00000: DPI-1047: Cannot locate a 64-bit Oracle Client library: "dlopen(libclntsh.dylib, 0x0001): Library not loaded: /ade/dosulliv_ldapmac/oracle/ldap/lib/libnnz11.dylib
  Referenced from: /Users/lazybug/Downloads/instantclient_11_2/libclntsh.dylib.11.1
  Reason: tried: '/ade/dosulliv_ldapmac/oracle/ldap/lib/libnnz11.dylib' (no such file), '/usr/local/lib/libnnz11.dylib' (no such file), '/usr/lib/libnnz11.dylib' (no such file)". See https://oracle.github.io/odpi/doc/installation.html#macos for help

```

设置lib环境变量，mac下设置DYLD_LIBRARY_PATH， linux下设置LD_LIBRARY_PATH