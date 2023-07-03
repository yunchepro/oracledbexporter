# oracle collectors

# 普通指标
路径： /metrics

## 实例信息
                                                                                                                                                                                                                       * v$database
                                                                                                                                                                                                                   * v$instance
                                                                                                                                                                                                                   
* v$dataguard
* parameters
* uptime（instance.startup_time)

## 普通指标, 适合使用prometheus时序数据库存储
正常采集
* oracle_stat
* oracle_wait_event
* oracle_time_model
* v$osstat
* session number, process number, 


# 低频采集指标（小时）
路径：/lowfreq_metrics

## 空间
* tablespace
* undo tablespace, temp tablespace
* asm, datafile, flash area

* undostat
* temp segment usage / sort usage


# 高维度指标，适合以宽表的形式存储到clickhouse


* session active snapshot
* session blocking info, 



# high dimension & 低频采集（按oracle awr snapshot频率采集）
路径： /awr_snaps
* hist_sql_
* top_sql

# 其他
备份

# todo
支持12C CDB PDB

