#!/usr/bin/env bash
# Weblog Stats API — sample curl calls.
#
# 【重要】通常の Workbench ターミナルで 127.0.0.1:${CDSW_APP_PORT} を叩くと、
# Jupyter Server (404 HTML) に当たることがあります。Application 用 uvicorn とは
# 別プロセス・別セッションで動くためです。
#
# 認証（いずれか）:
#   A) Workbench API キー（推奨・自動化向け）Audiences = Application
#      CAI_API_KEY='...' WEBLOG_API_URL='https://demo-api-weblog....' ./curl_demo.sh
#   B) ブラウザ Cookie（手動デモ向け）
#      WEBLOG_API_COOKIE='...' WEBLOG_API_URL='https://...' ./curl_demo.sh
#
# localhost (127.0.0.1:${CDSW_APP_PORT}) は対話セッションでは Jupyter 404 になりやすい

set -euo pipefail

if [[ -z "${WEBLOG_API_URL:-}" ]]; then
  echo "ERROR: WEBLOG_API_URL を設定してください（Workbench Application の HTTPS URL）。" >&2
  echo "  例: WEBLOG_API_URL='https://demo-api-weblog....' WEBLOG_API_COOKIE='...' $0" >&2
  echo "  ※ 対話セッションから 127.0.0.1:${CDSW_APP_PORT:-?} へ curl すると Jupyter 404 になります。" >&2
  exit 1
fi

BASE_URL="${WEBLOG_API_URL%/}"
API_KEY="${CAI_API_KEY:-${WEBLOG_API_KEY:-}}"
CURL_OPTS=(-sS -L)
if [[ -n "${API_KEY}" ]]; then
  CURL_OPTS+=(-H "Authorization: Bearer ${API_KEY}")
elif [[ -n "${WEBLOG_API_COOKIE:-}" ]]; then
  CURL_OPTS+=(-H "Cookie: ${WEBLOG_API_COOKIE}")
else
  echo "WARNING: CAI_API_KEY / WEBLOG_API_COOKIE 未設定。302/ログイン HTML になる場合があります。" >&2
fi

echo "=== BASE_URL: ${BASE_URL} ==="
echo

echo "--- GET /health ---"
curl "${CURL_OPTS[@]}" "${BASE_URL}/health"
echo -e "\n"

echo "--- GET /v1/summary ---"
curl "${CURL_OPTS[@]}" "${BASE_URL}/v1/summary"
echo -e "\n"

echo "--- GET /v1/stats/by-status ---"
curl "${CURL_OPTS[@]}" "${BASE_URL}/v1/stats/by-status"
echo -e "\n"

echo "--- GET /v1/stats/by-status?min_status=400 ---"
curl "${CURL_OPTS[@]}" "${BASE_URL}/v1/stats/by-status?min_status=400"
echo -e "\n"

echo "--- GET /v1/stats/by-method ---"
curl "${CURL_OPTS[@]}" "${BASE_URL}/v1/stats/by-method"
echo -e "\n"

echo "--- GET /v1/stats/top-client-ips?limit=5 ---"
curl "${CURL_OPTS[@]}" "${BASE_URL}/v1/stats/top-client-ips?limit=5"
echo -e "\n"

echo "--- GET /v1/stats/top-client-ips?limit=5&errors_only=true ---"
curl "${CURL_OPTS[@]}" "${BASE_URL}/v1/stats/top-client-ips?limit=5&errors_only=true"
echo -e "\n"
