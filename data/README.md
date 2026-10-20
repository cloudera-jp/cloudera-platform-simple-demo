# サンプルデータについて

| パス | 内容 |
|------|------|
| `data/logs/httpd/` | Apache httpd combined 形式のアクセスログ（デモ用サンプル） |
| `data/logs/flowlog/` | AWS VPC Flow Log v2 形式（デモ用サンプル） |

VPC Flow Log シミュレータは、任意の IP 一覧を使ってリアルな通信パターンを生成します。

1. 社内 PoC 用の `cmdb_models_dev` を `data/cmdb_models_dev/` に配置する  
2. 以下を実行して NiFi 用 IP リストを生成する  

```bash
python3 nifi/scripts/extract_cmdb_nic_ips.py
# 出力: nifi/data/cmdb_nic_ips.txt
```

リポジトリには `nifi/data/cmdb_nic_ips.txt` の生成結果のみ同梱している場合があります（フロー単体の動作確認用）。

## 本番デモ環境でのデータの所在

デモ環境では NiFi が **5 分ごと** に HDFS へ書き込みます。

- Web アクセスログ: `/data/logs/httpd`
- VPC Flow Log: `/data/logs/flowlog`

Spark / Hive / Workbench アプリは上記 HDFS パス（または Hive 外部テーブル `demo_logs.weblog` / `demo_logs.flowlog`）を参照します。

ローカル検証では `spark/*.py --local` が `data/logs/httpd` を読み込みます。
