-- =============================================================================
-- Demo 3: Schema-on-read — RegexSerDe parses Apache combined logs at read time
--
-- Run as user: admin  (kinit admin@REALM before Beeline / Hue)
--
-- Kerberos (edge node example):
--   kinit admin@YOUR.REALM
--   beeline -u "jdbc:hive2://<hs2-host>:10000/default;principal=hive/_HOST@YOUR.REALM"
--
-- Ranger (minimum for user admin):
--   1) HDFS policy: path /data/logs/httpd → Read, Execute (list)
--   2) Hive policy: database demo_logs → select, read, create (for DDL once)
--   3) Hive policy: table demo_logs.weblog → select, read
-- =============================================================================

CREATE DATABASE IF NOT EXISTS demo_logs
  COMMENT 'SOC PoC: log files on HDFS queried via Hive';

USE demo_logs;

-- Redefine safely when re-running demo DDL (column layout / serde change).
DROP TABLE IF EXISTS demo_logs.weblog;

-- Apache httpd "combined" log line, e.g.:
-- 68.152.252.15 - - [03/Oct/2026:22:17:07 +0000] "DELETE /images/logo.png HTTP/1.0" 301 656 "-" "Mozilla/5.0 ..."
CREATE EXTERNAL TABLE demo_logs.weblog (
  client_ip     STRING,
  ident         STRING,
  authuser      STRING,
  log_datetime  STRING,
  method        STRING,
  path          STRING,
  http_version  STRING,
  status        STRING,
  bytes         STRING,
  referer       STRING,
  user_agent    STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  'input.regex' = '^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] "(\\S+) (\\S+) (\\S+)" (\\d+) (\\d+) "(.*)" "(.*)"'
)
-- TextInputFormat + HiveIgnoreKeyTextOutputFormat (STORED AS TEXTFILE).
-- Do not use mapred TextInputFormat for OUTPUTFORMAT — Hive rejects it (Error 10055).
STORED AS TEXTFILE
LOCATION '/data/logs/httpd'
TBLPROPERTIES (
  'comment' = 'External table: RegexSerDe over raw httpd files (no ETL copy)',
  'external.table.purge' = 'false'
);
