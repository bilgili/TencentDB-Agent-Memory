#!/usr/bin/env bash
# 一键拉起 **PostgreSQL 模式** 三件套（memory-core + memory-hub + proxy）。
# 试验特性：默认关闭；默认入口仍是 ./start-all.sh（sqlite）。
#
# 与 start-all.sh 的关系：
#   - start-all.sh 保持原逻辑不变 —— 默认 sqlite，数据落容器卷；
#   - 本脚本复用同一套流程，强制 MEMORY_CORE_STORE_MODE=postgres 并写入 .env：
#       · 数据面（L0/L1 记忆、profile、skill）走 Postgres + pgvector；
#       · 元数据（meta_* 团队/用户/agent/task）默认跟随落同一个 Postgres
#         （MEMORY_CORE_METADATA_BACKEND=auto）；
#       · memory-hub 里的 knowledge 同样落该 Postgres，使用独立 schema（knowledge）；
#       · .env 未设 POSTGRES_URL 时，自动在同网络起一个本地
#         pgvector/pgvector:0.8.0-pg17 容器（卷 postgres-local-data 持久化，
#         不映射宿主端口，脚本已为其执行 CREATE EXTENSION vector）。
#
# 用法（与 start-all.sh 完全一致）：
#   ./start-all-postgres.sh            # 交互式引导 LLM，通过后一键起
#   PULL=1 ./start-all-postgres.sh     # 先 docker pull 升级镜像
#
# 想回 sqlite：把 .env 里 MEMORY_CORE_STORE_MODE 注释掉或改为 sqlite，再 ./start-all.sh
# （若还设了 KNOWLEDGE_DB_URL，也一并注释掉）。切换后端不会迁移已有数据，
# 容器卷里的 SQLite 文件原样保留。

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_lib.sh
source "$SCRIPT_DIR/_lib.sh"

# .env 不存在时先从模板复制，才能把 STORE_MODE 写回去。
if [[ ! -f "$ENV_FILE" ]]; then
  info ".env 不存在，从 .env.example 复制一份"
  cp "$SCRIPT_DIR/.env.example" "$ENV_FILE"
fi

# 冲突检测：.env 里若**显式**设置了非 postgres 的 MEMORY_CORE_STORE_MODE，
# start-all.sh 里 source .env 会盖掉下面的 export，静默跑成别的模式 —— 提前拦下。
# （.env.example 里该行默认注释，标准用户不会触发。）
env_mode=$(grep -E '^[[:space:]]*MEMORY_CORE_STORE_MODE=' "$ENV_FILE" \
  | tail -n1 | cut -d= -f2- | tr -d '[:space:]"' || true)
if [[ -n "$env_mode" && "$env_mode" != "postgres" ]]; then
  echo "[error] .env 里显式设置了 MEMORY_CORE_STORE_MODE=${env_mode}，与本脚本冲突。" >&2
  echo "        二选一：" >&2
  echo "          ① 想用 postgres：注释掉 .env 里该行，重跑本脚本；" >&2
  echo "          ② 想用 ${env_mode}：直接 ./start-all.sh。" >&2
  exit 1
fi

set_env_value MEMORY_CORE_STORE_MODE postgres "$ENV_FILE"
export MEMORY_CORE_STORE_MODE=postgres
echo "[start-all-postgres] 已写入 MEMORY_CORE_STORE_MODE=postgres 到 .env（数据面 + 元数据 + knowledge 默认均落 Postgres）"

exec "$SCRIPT_DIR/start-all.sh" "$@"
