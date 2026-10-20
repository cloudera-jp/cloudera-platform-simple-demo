# デモ環境の構成と前提条件

本ドキュメントは、Cloudera Platfrom デモを実施する **Cloudera 環境** の想定と、デモ前に揃えておくべき設定をまとめたものです。

## プラットフォーム構成（概念）

デモスライド「デモ２：デモシナリオ」と同様に、次の 3 層を前提としています。

| 領域 | 役割 | 本デモでの利用 |
|------|------|----------------|
| **Cloudera Open Data Lakehouse** | Ozone/HDFS ストレージ、SDX（Ranger 等）、Runtime（Spark 等） | ログの永続化、権限、アクセス |
| **Cloudera Platform Gateway ノード** | CLI / `spark-submit` | デモ② PySpark |
| **Cloudera Data Services** | Data Warehouse（Hue / Virtual Warehouse）、AI Workbench | デモ③ Hive、デモ④〜⑥ Jupyter / Flask / FastAPI |

Data Engineering（CDE 等）のデモは **本リポジトリの範囲外** です（スライド上も「デモなし」）。

## 外部システム相当（ログシミュレータ）

本番の Web サーバや AWS VPC フローログの代わりに、**NiFi 上のデータフロー** がログを生成します（デモ①）。

| シミュレータ | 出力形式 | HDFS 先 |
|--------------|----------|---------|
| HTTPD Access Log Generator | Apache combined | `/data/logs/httpd` |
| VPC Flow Log Generator | AWS VPC Flow Log v2 | `/data/logs/flowlog` |

- **スケジュール**: 5 分ごと（CRON）
- **保持**: おおよそ 6 時間より古いファイルを HDFS 上で削除（フロー内 ListHDFS / DeleteHDFS）
- VPC Flow Log 側は NiFi フロー内で参照する **社内 IP アドレスリスト**（NiFi ノード上に配置）を用いて送信元・宛先を生成

## データレイク上のパスと Hive メタデータ

| HDFS パス | 内容 | Hive 外部テーブル |
|-----------|------|-------------------|
| `/data/logs/httpd` | `access.log_*` ファイル群 | `demo_logs.weblog` |
| `/data/logs/flowlog` | `vpc-flow-log_*` ファイル群 | `demo_logs.flowlog` |

デモ③以降（Hive SQL、Flask、FastAPI）は **httpd の `demo_logs.weblog`** を主に参照します。Flow Log は Hive サンプルクエリ（`hive/04_sample_queries_flowlog.sql`）で展示できます。

### スキーマオンリードの考え方

- ファイルは **テキストのまま** HDFS に置かれる
- **読み取り時** に RegexSerDe（Hive）または正規表現（Spark）で列に分解する
- 取り込みパイプラインで RDBMS や ORC へ変換する必要はない（デモのメッセージ）

## 認証・認可（Kerberos / Ranger）

デモ③〜⑥では、少なくとも次を想定しています。

### Kerberos

- デモ実行者ユーザー（例: `admin`）で `kinit` 済みであること
- Beeline / Hue / Workbench セッションがクラスタリソースにアクセスできること

Hive DDL の例（`hive/01_create_weblog_external_table.sql` 冒頭）:

```bash
kinit admin@YOUR.REALM
beeline -u "jdbc:hive2://<hs2-host>:10000/default;principal=hive/_HOST@YOUR.REALM"
```

### Ranger（最小限）

`hive/ranger_setup_notes.txt` に詳細があります。要点のみ:

1. **HDFS**: `/data/logs/httpd` , `/data/logs/flowlog` に対する Read / Execute（ディレクトリ一覧）
2. **Hive**: データベース `demo_logs`、テーブル `weblog` / `flowlog` に対する Select / Read
3. DDL 初回実行時は Create / Alter 等の追加権限

NiFi サービスユーザーが HDFS へ書き込めるポリシーは、デモ①の運用担当が別途設定します。

## NiFi（デモ①）

| 項目 | 値 |
|------|-----|
| フロー定義 JSON | `nifi/flows/httpd-access-log-generator.flow.json` , `nifi/flows/vpc-flow-log-generator.flow.json` |
| 再生成スクリプト | `nifi/flows/build_httpd_access_log_flow.py` , `nifi/flows/build_vpc_flow_log_flow.py` |
| NiFi バージョン | フロー定義は **2.3.x** 系を想定 |

**デプロイ手順（概要）**

1. NiFi UI でフロー JSON をインポート、または既存 Process Group を更新  
2. HDFS 書き込み先・Kerberos 主体・Controller Service（HDFS Client 等）を環境に合わせて設定  
3. フローを Start — 数分後に `hdfs dfs -ls /data/logs/httpd` 等でファイル増加を確認  

補助スクリプト（フロー外・開発用）: `nifi/scripts/generate_httpd.py` , `generate_vpc_flowlog.py`

## Gateway ノード（デモ②）

| 項目 | 内容 |
|------|------|
| 実行方法 | `spark-submit spark/0N_*.py` |
| デフォルト入力 | `hdfs:///data/logs/httpd/access.log_*` |
| ローカル検証 | `--local`（リポジトリ内 `data/logs/httpd`） |

共通ライブラリ: `spark/httpd_logs.py`（パース・CLI・SparkSession）

## Data Warehouse / Hue（デモ③）

| ファイル | 用途 |
|----------|------|
| `hive/01_create_weblog_external_table.sql` | `demo_logs.weblog` 作成 |
| `hive/02_sample_queries_weblog.sql` | Web ログの集計例 |
| `hive/03_create_flowlog_external_table.sql` | `demo_logs.flowlog` 作成 |
| `hive/04_sample_queries_flowlog.sql` | Flow Log の集計例 |

Virtual Warehouse または HiveServer2 経由の Beeline / **Hue** の SQL エディタで実行します。

## AI Workbench（デモ④〜⑥）

| デモ | 資産 | 備考 |
|------|------|------|
| ④ Jupyter | `jupyterlab/httpd_log_analysis.ipynb` | Spark カーネル、HDFS 直読み |
| ⑤ Flask | `flask/` + `.project-metadata.yaml` | Application としてデプロイ |
| ⑥ FastAPI | `fastapi/` + `.project-metadata.yaml` | Application、`/docs` で OpenAPI |

Workbench では Spark Data Lake 接続経由で **`demo_logs.weblog`** を参照します。デモ③の DDL を事前に実行しておく必要があります。

### Flask / FastAPI の配置パス

メタデータでは `/home/cdsw/demo_source/flask` , `fastapi` を想定しています。リポジトリを Workbench プロジェクトに同期したうえで、環境変数 `FLASK_APP_ROOT` / `FASTAPI_APP_ROOT` で上書き可能です。

### FastAPI の外部からの呼び出し

`fastapi/curl_demo.sh` は **Application の公開 HTTPS URL**（`WEBLOG_API_URL`）向けです。対話セッション内の `127.0.0.1:${CDSW_APP_PORT}` は Jupyter と競合し 404 になりやすい点に注意（スクリプト内コメント参照）。

## リポジトリ資産のクラスタへの反映

一般的な流れ:

1. 本プロジェクトを Gateway / Workbench / 運用端末へ配布（共有ストレージ、scp 等）  
2. NiFi フローをインポートし Start（デモ①）  
3. ログが溜まったらデモ②〜⑥を順に実施  

サンプルログのみ必要な場合は `data/logs/` を HDFS に `hdfs dfs -put` しても構いませんが、デモ①の「継続取り込み」のストーリーは NiFi Start が必要です。

## トラブルシューティング（よくある事象）

| 症状 | 確認ポイント |
|------|----------------|
| Hive で 0 行 | HDFS にファイルがあるか、`LOCATION` と Ranger HDFS 権限 |
| RegexSerDe で列が NULL | ログ行形式が combined / Flow Log v2 と一致しているか |
| Spark でパスが見つからない | `kinit`、glob `/data/logs/httpd/access.log_*` |
| Flask/FastAPI がテーブル不存在 | デモ③ DDL 未実行、または別カタログ |
| NiFi が書き込まない | フロー Stop/Invalid、HDFS CS の principal、ディレクトリ権限 |
