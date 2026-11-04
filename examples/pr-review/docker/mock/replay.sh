#!/bin/sh
# mock claude：吞掉 stdin（prompt），按录制会话回放 stream-json。
# 逐行带节奏输出（0.35s/行），模拟真实评审的 token 级直播效果。
cat > /dev/null
while IFS= read -r line; do
  printf '%s\n' "$line"
  sleep 0.35
done < /session.ndjson
