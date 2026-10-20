# デモシナリオ実施ガイド（①〜⑥）

各デモの **目的（聴衆へのメッセージ）**、**使用資産**、**推奨手順**、**話法のポイント** をまとめています。環境の前提は [demo-environment.md](demo-environment.md) を参照してください。

---

## デモ① — データ処理に最適化された Cloudera ストレージ

### メッセージ

随時発生するログファイルを **シンプルな操作で HDFS（Ozone/HDFS）に保存** し、CLI や GUI から参照できることを示す。

### 使用資産

| 種別 | パス |
|------|------|
| HTTPD ログフロー | `nifi/flows/httpd-access-log-generator.flow.json` |
| VPC Flow Log フロー | `nifi/flows/vpc-flow-log-generator.flow.json` |
| フロー再ビルド | `nifi/flows/build_httpd_access_log_flow.py` , `build_vpc_flow_log_flow.py` |

### 推奨手順

1. NiFi UI で両フロー（または Process Group）をインポート  
2. HDFS 出力先が `/data/logs/httpd` , `/data/logs/flowlog` であることを確認  
3. フローを **Start**  
4. **5 分待機** 後、Gateway または Hue File Browser / `hdfs dfs -ls` で新規ファイルを確認  
5. （任意）6 時間経過後の削除ロジックは ListHDFS + DeleteHDFS — 「ストレージのライフサイクルもフローで表現」と説明  

### デモの見せ方

- 左側スライドの「Web Log / Flow Log Simulator → 5 分ごと → ストレージ」を実画面で対応づける  
- **ETL 前の raw ファイル** がそのまま溜まっていることを強調（後続デモの前提）

---

## デモ② — raw データをスクリプトでフィルタ・集計

### メッセージ

ログを **データベースに登録しなくても**、業界標準の **PySpark** でフィルタ・集計できる。

### 使用資産

| スクリプト | 内容 |
|------------|------|
| `spark/01_overview.py` | 行数・ファイル数・先頭 10 行 |
| `spark/02_filter_errors.py` | HTTP 4xx / 5xx |
| `spark/03_filter_suspicious.py` | admin アカウント、curl、403 等 |
| `spark/04_aggregate_by_status.py` | ステータスコード別件数 |
| `spark/05_aggregate_by_method.py` | HTTP メソッド別件数 |
| `spark/06_aggregate_top_ips.py` | クライアント IP 上位 |
| `spark/07_aggregate_api_paths.py` | `/api*` , login/logout パス |
| `spark/analyze_httpd_logs.py` | 上記を一括実行（リハーサル用） |
| `spark/httpd_logs.py` | 共通パーサ・CLI |

### 推奨手順（プレゼン向けは 1 本ずつ）

```bash
kinit <user>@YOUR.REALM
cd spark
spark-submit 01_overview.py
spark-submit 02_filter_errors.py
# … 必要に応じて 03〜07
```

ローカルリハーサル:

```bash
spark-submit 01_overview.py --local
```

### 話法のポイント

- 読み込みパスは `hdfs:///data/logs/httpd/access.log_*`（デモ①の出力と一致）  
- 正規表現で **読み取り時に列化** — デモ③の Hive SerDe と同じ思想  
- Gateway ノード = 「データサイエンティストが使い慣れた spark-submit」

---

## デモ③ — スキーマオンリード（Schema-on-Read）

### メッセージ

HDFS 上の **生ログに後からスキーマを当て**、SQL で直接検索・集計できる（Virtual Warehouse / Hue）。

### 使用資産

| ファイル | 用途 |
|----------|------|
| `hive/01_create_weblog_external_table.sql` | 外部テーブル `demo_logs.weblog` |
| `hive/02_sample_queries_weblog.sql` | 件数、ステータス集計、エラー抽出 |
| `hive/03_create_flowlog_external_table.sql` | 外部テーブル `demo_logs.flowlog` |
| `hive/04_sample_queries_flowlog.sql` | アクション別、ポート別、IP フィルタ |
| `hive/ranger_setup_notes.txt` | Ranger ポリシー例 |

### 推奨手順

1. `kinit` 後、Beeline または Hue で `01_create_weblog_external_table.sql` を実行（初回のみ）  
2. `02_sample_queries_weblog.sql` を **1 クエリずつ** 実行し結果を説明  
3. （時間があれば）Flow Log 用 `03` + `04` — 特定プライベート IP（例: `10.1.1.40`）周辺の通信を SOC 文脈で触れる  

### 話法のポイント

- **EXTERNAL TABLE** + `LOCATION '/data/logs/httpd'` — データは HDFS に 1 副本のみ  
- RegexSerDe で combined ログをパース — 形式変更時は DDL の regex を更新する運用  
- デモ② Spark パースと **同じログ、別インターフェース（SQL）**

---

## デモ④ — 分析者向け統合分析環境（Jupyter）

### メッセージ

**Jupyter Notebook** 上で Python（PySpark）を柔軟に実行し、表・グラフをその場で共有できる。PyPI パッケージも利用可能。

### 使用資産

- `jupyterlab/httpd_log_analysis.ipynb`

### 推奨手順

1. AI Workbench でプロジェクトを開き、Notebook を起動  
2. 上からセルを順に実行  
   - Spark セッション確立  
   - HDFS `/data/logs/httpd/access.log_*` 読み込み・正規表現で列分解  
   - フィルタ・集計・可視化  
3. デモ②との違い: **対話的**、**グラフ**、**ナラティブ**（Markdown セルでシナリオを説明）

### 話法のポイント

- `spark-submit` 不要 — アナリスト向け UX  
- デモ③の Hive テーブルを使わず **直接 HDFS** でもよい（同一のスキーマオンリード）

---

## デモ⑤ — 簡易 Web アプリケーション（Flask）

### メッセージ

PyPI の **Flask** で、短いコードから **業務ユーザー向け Web UI** を Workbench 上に公開できる。

### 使用資産

| パス | 内容 |
|------|------|
| `flask/app.py` | Spark SQL で `demo_logs.weblog` を検索 |
| `flask/templates/index.html` | フィルタ UI |
| `flask/requirements.txt` | 依存パッケージ |
| `flask/.project-metadata.yaml` | Workbench Application 定義 |

### 推奨手順

1. デモ③で `demo_logs.weblog` が作成済みであることを確認  
2. Workbench で `install_dependencies.py` → Application **Start**（メタデータ参照）  
3. ブラウザで Application URL を開く  
4. ドロップダウン（method, status 等）と部分一致フィルタでログを絞り込み  

### 話法のポイント

- バックエンドは **Spark SQL** — データレイク上の同じテーブル  
- 利用者が「SQL を書かずに」疑わしいアクセスを探す UI 例

---

## デモ⑥ — 簡易 Web API（FastAPI）

### メッセージ

**FastAPI** で REST API を公開し、他システム（SOAR、ダッシュボード、curl）から集計結果を **JSON** で取得できる。

### 使用資産

| パス | 内容 |
|------|------|
| `fastapi/app.py` | `/v1/summary` , `/v1/stats/by-status` 等 |
| `fastapi/curl_demo.sh` | 公開 URL + API キー / Cookie での呼び出し例 |
| `fastapi/curl_with_api_key.example.sh` | API キー例 |
| `fastapi/.project-metadata.yaml` | Workbench Application 定義 |

### 主なエンドポイント

| メソッド | パス | 説明 |
|----------|------|------|
| GET | `/health` | ヘルスチェック |
| GET | `/v1/summary` | 総リクエスト数、ユニーク IP、エラー件数 |
| GET | `/v1/stats/by-status` | ステータス別件数（`min_status` クエリ可） |
| GET | `/v1/stats/by-method` | メソッド別件数 |
| GET | `/v1/stats/top-client-ips` | IP 上位（`errors_only` 可） |
| GET | `/docs` | Swagger UI |

### 推奨手順

1. Application を Start  
2. ブラウザで `/docs` を見せ、エンドポイントを 1〜2 個 Try it out  
3. ターミナルからは **公開 HTTPS URL** で `curl_demo.sh` を実行（`WEBLOG_API_URL` , `CAI_API_KEY` 等）  

### 話法のポイント

- デモ⑤（人間向け UI）と **同じデータセット** を機械可読 API で提供  
- SOC パイプライン（チケット起票、自動ブロック検討）への連携イメージ

---

## 推奨デモ順序と所要時間の目安

| 順序 | デモ | 目安 |
|------|------|------|
| 1 | ① NiFi Start + HDFS 確認 | 10 分（待ち時間含む） |
| 2 | ② PySpark 2〜3 本 | 15 分 |
| 3 | ③ Hive DDL + クエリ 2〜3 本 | 15 分 |
| 4 | ④ Jupyter 一部セル | 10 分 |
| 5 | ⑤ Flask UI | 10 分 |
| 6 | ⑥ FastAPI `/docs` + curl | 10 分 |

**短縮版**: ①は事前に NiFi を Start 済みにし、②は `01` + `04`、③は `02` の COUNT と status 集計のみ、④はグラフセル 1 つ、⑤⑥は各 1 画面。

---

## デモ間のストーリー

1. **①** ファイアウォール・Web サーバからログが **レイクに集約** される  
2. **②** データエンジニアが Spark で **異常 HTTP ステータス** を素早く確認  
3. **③** アナリストが Hue で **SQL により再現性のあるレポート** を作成  
4. **④** インシデント調査で Notebook を **チーム共有**  
5. **⑤** L1 オペレータが Web UI で **疑わしい IP / 403** を検索  
6. **⑥** システムが API で **Top エラー IP** を取得し playbook へ  

この流れが、添付スライド「Cloudera Base + Data Services」の各コンポーネント配置と一致します。
