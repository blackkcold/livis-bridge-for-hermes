#!/usr/bin/env bash
# 模拟 `hermes -z <content> --usage-file <path>`：
#   - 读取最后一个参数（--usage-file 路径），写入假 usage JSON
#   - 根据内容关键词模拟: 延迟 / 失败 / 取消点
#
# 用法: fake_hermes.sh -z '<content>' [--usage-file <path>]
set -euo pipefail

USAGE_PATH=""
CONTENT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -z|--oneshot) CONTENT="$2"; shift 2 ;;
    --usage-file) USAGE_PATH="$2"; shift 2 ;;
    *) shift ;;
  esac
done

# 模拟延迟: 内容含 "SLOW" -> 2s
if [[ "$CONTENT" == *"SLOW"* ]]; then
  sleep 2
fi

# 模拟失败: 内容含 "FAIL" -> 非零退出
if [[ "$CONTENT" == *"FAIL"* ]]; then
  echo "fake error: simulated failure" >&2
  [ -n "$USAGE_PATH" ] && printf '{"completed": false, "failed": true}\n' > "$USAGE_PATH"
  exit 42
fi

# usage file（模拟真实 --usage-file 输出）
if [ -n "$USAGE_PATH" ]; then
  cat > "$USAGE_PATH" <<'JSON'
{"estimated_cost_usd": 0.001, "input_tokens": 100, "output_tokens": 20,
 "total_tokens": 120, "completed": true, "failed": false}
JSON
fi

echo "FAKE-HERMES-OK: ${CONTENT:0:60}"
