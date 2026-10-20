-- =============================================================================
-- Demo: Schema-on-read — RegexSerDe for AWS VPC Flow Log (format version 2)
--
-- Run as user: admin  (kinit admin@REALM before Beeline / Hue)
--
-- Kerberos (edge node example):
--   kinit admin@YOUR.REALM
--   beeline -u "jdbc:hive2://<hs2-host>:10000/demo_logs;principal=hive/_HOST@YOUR.REALM"
--
-- Ranger (minimum for user admin):
--   1) HDFS: /data/logs/flowlog → Read, Execute
--   2) Hive: demo_logs.flowlog → Select, Read
--   (See hive/ranger_setup_notes.txt — flowlog section)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS demo_logs
  COMMENT 'SOC PoC: log files on HDFS queried via Hive';

USE demo_logs;

DROP TABLE IF EXISTS demo_logs.flowlog;

-- AWS VPC Flow Log v2 (space-separated), e.g.:
-- 2 123456789012 eni-0a1b2c3d4e5f67890 10.1.1.40 8.8.8.8 6883 443 6 47 275797 1791167698 1791167959 ACCEPT OK
CREATE EXTERNAL TABLE demo_logs.flowlog (
  version       STRING,
  account_id    STRING,
  interface_id  STRING,
  srcaddr       STRING,
  dstaddr       STRING,
  srcport       STRING,
  dstport       STRING,
  protocol      STRING,
  packets       STRING,
  bytes         STRING,
  start         STRING,
  end           STRING,
  action        STRING,
  log_status    STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  'input.regex' = '^(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+)'
)
STORED AS TEXTFILE
LOCATION '/data/logs/flowlog'
TBLPROPERTIES (
  'comment' = 'External table: RegexSerDe over NiFi VPC flow logs (v2)',
  'external.table.purge' = 'false'
);
