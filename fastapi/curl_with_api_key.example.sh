#!/usr/bin/env bash
# Cloudera AI Workbench API キーで Weblog Stats API を呼び出す最小例
#
# 前提:
#   User Settings > API Keys でキー作成時、Audiences に Application を含める
#   （API のみのキーは /api/v2 向けで、Application URL では使えない）
#
# ドキュメント: Cloudera AI API v2 — API key authentication
# https://docs.cloudera.com/machine-learning/cloud/api/topics/ml-api-v2.html

set -euo pipefail

export CAI_API_KEY='（User Settings で発行したキー）'
export WEBLOG_API_URL='https://demo-api-weblog.ml-62b2ed1f-392.apps.ecs-13-196-19-91.pvc.cloudera-labs.com'

curl -sS -L \
  -H "Authorization: Bearer ${CAI_API_KEY}" \
  "${WEBLOG_API_URL%/}/health"

echo
curl -sS -L \
  -H "Authorization: Bearer ${CAI_API_KEY}" \
  "${WEBLOG_API_URL%/}/v1/summary"

echo
