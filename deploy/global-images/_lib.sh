#!/usr/bin/env bash
# 通用工具函数：加载 .env、校验必填参数、等待容器 health、清理旧容器。
# 由 start-*.sh 通过 `source _lib.sh` 引入，不单独执行。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/.env}"

# 颜色
if [[ -t 1 ]]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YLW=$'\033[33m'; C_BLU=$'\033[34m'; C_RST=$'\033[0m'
else
  C_RED=""; C_GRN=""; C_YLW=""; C_BLU=""; C_RST=""
fi

info() { echo "${C_BLU}[$(date +%H:%M:%S)]${C_RST} $*"; }
ok()   { echo "${C_GRN}[ok]${C_RST} $*"; }
warn() { echo "${C_YLW}[warn]${C_RST} $*" >&2; }
die()  { echo "${C_RED}[error]${C_RST} $*" >&2; exit 1; }

# 加载 .env（未创建时给指引）
load_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    die ".env 不存在。先 cp .env.example .env 并填入 LLM 参数。"
  fi
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
}

# 校验一组必填变量；缺一个都不启动，一次性列出所有缺失项
require_vars() {
  local missing=()
  for var in "$@"; do
    local val="${!var:-}"
    if [[ -z "$val" || "$val" == "REPLACE_ME" ]]; then
      missing+=("$var")
    fi
  done
  if (( ${#missing[@]} > 0 )); then
    echo "${C_RED}[error]${C_RST} .env 中以下必填参数未设置或仍为 REPLACE_ME：" >&2
    for v in "${missing[@]}"; do echo "  - $v" >&2; done
    echo "" >&2
    echo "  编辑 $ENV_FILE 后重试。" >&2
    exit 1
  fi
}

# 找到可用 docker 命令（兼容 Homebrew 独立安装 + colima）
# 优先级：PATH 中的 docker → Homebrew apple silicon → Homebrew intel → /usr/local
# Homebrew Cellar 路径下按版本 glob，取最新（sort -V），避免硬编码具体小版本号。
find_docker() {
  if command -v docker >/dev/null 2>&1; then
    echo "docker"
    return
  fi
  local candidate
  for prefix in /opt/homebrew/Cellar/docker /usr/local/Cellar/docker; do
    if [[ -d "$prefix" ]]; then
      candidate=$(ls -1 "$prefix" 2>/dev/null | sort -V | tail -n1)
      if [[ -n "$candidate" && -x "$prefix/$candidate/bin/docker" ]]; then
        echo "$prefix/$candidate/bin/docker"
        return
      fi
    fi
  done
  for path in /opt/homebrew/bin/docker /usr/local/bin/docker; do
    if [[ -x "$path" ]]; then
      echo "$path"
      return
    fi
  done
  die "找不到 docker 命令。请先安装 Docker Desktop / OrbStack / colima + docker CLI。"
}

DOCKER="$(find_docker)"

# PULL=1 时拉取镜像最新版本。
# 默认关闭：docker run 在本地没有镜像时会自动拉，但本地已有同名 :latest 时会直接复用，
# 不会感知远端更新——想升级到最新 latest 就带 PULL=1。
pull_image() {
  local image="$1"
  [[ "${PULL:-0}" == "1" ]] || return 0
  info "拉取镜像 $image"
  $DOCKER pull "$image" || die "拉取 $image 失败。"
}

# 幂等移除同名容器
rm_container_if_exists() {
  local name="$1"
  if $DOCKER ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$name"; then
    info "移除已存在的容器 $name"
    $DOCKER rm -f "$name" >/dev/null
  fi
}

# 等待容器进入 healthy 状态（或没有 healthcheck 时等 running）
wait_healthy() {
  local name="$1"
  local timeout="${2:-90}"    # 秒
  local waited=0
  info "等待 $name 就绪（最长 ${timeout}s）..."
  while (( waited < timeout )); do
    local status health
    status="$($DOCKER inspect -f '{{.State.Status}}' "$name" 2>/dev/null || echo "missing")"
    health="$($DOCKER inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$name" 2>/dev/null || echo "unknown")"

    if [[ "$status" != "running" ]]; then
      warn "${name} 状态 ${status}，输出最近日志："
      $DOCKER logs --tail 30 "$name" 2>&1 || true
      die "${name} 未运行。"
    fi

    case "$health" in
      healthy) ok "$name healthy"; return 0 ;;
      unhealthy)
        warn "${name} unhealthy，日志："
        $DOCKER logs --tail 30 "$name" 2>&1 || true
        die "${name} 健康检查失败。"
        ;;
      none)
        # 镜像没有 healthcheck：容器 running 就当就绪
        ok "${name} running（无 healthcheck）"
        return 0
        ;;
    esac
    sleep 2
    waited=$((waited + 2))
  done
  warn "${name} 等待超时，最后日志："
  $DOCKER logs --tail 30 "$name" 2>&1 || true
  die "${name} 在 ${timeout}s 内未就绪。"
}

# 打印统一的服务地址表
print_endpoints() {
  echo ""
  echo "  ┌─────────────────────────────────────────────────────────┐"
  echo "  │ 服务地址                                                │"
  echo "  ├─────────────────────────────────────────────────────────┤"
  printf "  │ Panel UI       http://localhost:%-24s│\n" "${PANEL_PORT}/"
  printf "  │ Panel API      http://localhost:%-24s│\n" "${PANEL_PORT}/api/v1/"
  printf "  │ Knowledge API  http://localhost:%-24s│\n" "${KNOWLEDGE_PORT}/v3/"
  printf "  │ Knowledge Docs http://localhost:%-24s│\n" "${KNOWLEDGE_PORT}/docs"
  printf "  │ Memory Core     http://localhost:%-24s│\n" "${MEMORY_CORE_PORT}/"
  printf "  │ Proxy          http://localhost:%-24s│\n" "${PROXY_PORT}/"
  echo "  └─────────────────────────────────────────────────────────┘"
}

# ═══════════════════════════════════════════════════════════════
# PostgreSQL 后端（试验特性）：memory-core 与 memory-hub 共用同一个 Postgres
# ═══════════════════════════════════════════════════════════════

# redact_url <url>：把 URL 里的密码替换为 ****，用于日志输出。
redact_url() {
  printf '%s' "$1" | sed -E 's#(://[^:/@]+):[^@]*@#\1:****@#'
}

# ensure_postgres_url
#   保证变量 POSTGRES_URL 非空。
#     POSTGRES_URL 已设   → 用外部 Postgres（15+，带 pgvector >= 0.8.0），不起容器；
#     POSTGRES_URL 未设   → 在 tdai-memory-stack 网络里起一个 pgvector 容器
#                           （数据卷 postgres-local-data 持久化，不映射宿主端口）。
#   本地容器的超级用户由镜像创建，所以这里一次性执行 CREATE EXTENSION vector
#   （pgvector 0.8.0 不是 trusted 扩展，普通角色不能自己建）。
#   幂等：容器已在跑就只补一次 ready 检查。密码首次生成后写回 .env，
#   因为数据卷里的密码在首次 initdb 时就固定了，重建容器不能换密码。
ensure_postgres_url() {
  [[ -n "${POSTGRES_URL:-}" ]] && return 0

  local container="${POSTGRES_LOCAL_CONTAINER:-tdai-postgres-local}"
  local image="${POSTGRES_LOCAL_IMAGE:-pgvector/pgvector:0.8.0-pg17}"
  local volume="${POSTGRES_LOCAL_VOLUME:-postgres-local-data}"
  local network=tdai-memory-stack
  local db="${POSTGRES_LOCAL_DB:-tdai_memory}"
  local user=tdai

  if ! $DOCKER network inspect "$network" >/dev/null 2>&1; then
    info "创建 docker 网络 $network"
    $DOCKER network create "$network" >/dev/null
  fi

  if [[ -z "${POSTGRES_LOCAL_PASSWORD:-}" ]]; then
    if command -v openssl >/dev/null 2>&1; then
      POSTGRES_LOCAL_PASSWORD="$(openssl rand -hex 16)"
    else
      POSTGRES_LOCAL_PASSWORD="$(head -c 16 /dev/urandom | od -A n -t x1 | tr -d ' \n')"
    fi
    [[ -f "$ENV_FILE" ]] && set_env_value POSTGRES_LOCAL_PASSWORD "$POSTGRES_LOCAL_PASSWORD" "$ENV_FILE"
    info "已生成 POSTGRES_LOCAL_PASSWORD 并写回 ${ENV_FILE}"
  fi

  info "未设 POSTGRES_URL → 启动本地 Postgres（${image}）"
  if ! $DOCKER ps --format '{{.Names}}' 2>/dev/null | grep -qx "$container"; then
    rm_container_if_exists "$container"
    # 编码/区域固定为 UTF-8：全文检索的中日韩分词依赖 UTF-8 数据库。
    # healthcheck 走 TCP（-h 127.0.0.1）：initdb 期间的临时服务只听 unix socket，
    # 走 socket 会在真正的服务启动前误报 ready。
    $DOCKER run -d --name "$container" \
      --network "$network" \
      --network-alias tdai-postgres \
      -e POSTGRES_USER="$user" \
      -e POSTGRES_PASSWORD="$POSTGRES_LOCAL_PASSWORD" \
      -e POSTGRES_DB="$db" \
      -e POSTGRES_INITDB_ARGS="--encoding=UTF8 --locale=C.UTF-8" \
      -v "${volume}:/var/lib/postgresql/data" \
      --health-cmd "pg_isready -h 127.0.0.1 -U ${user} -d ${db}" \
      --health-interval 3s --health-timeout 3s --health-retries 20 --health-start-period 5s \
      "$image" >/dev/null
  fi
  wait_healthy "$container" 90

  $DOCKER exec "$container" psql -v ON_ERROR_STOP=1 -U "$user" -d "$db" \
    -c 'CREATE EXTENSION IF NOT EXISTS vector' >/dev/null \
    || die "CREATE EXTENSION vector 失败，docker logs $container 排查"
  ok "postgres 就绪（容器 ${container}，网络内别名 tdai-postgres，库 ${db}，已启用 pgvector）"
  POSTGRES_URL="postgres://${user}:${POSTGRES_LOCAL_PASSWORD}@tdai-postgres:5432/${db}"
}

# ═══════════════════════════════════════════════════════════════
# LLM 通路检查（与 verify.sh 同源逻辑；供 start-all.sh 交互式流程复用）
# ═══════════════════════════════════════════════════════════════

CURL="${CURL:-/usr/bin/curl}"
if [[ ! -x "$CURL" ]]; then
  if command -v curl >/dev/null 2>&1; then
    CURL="$(command -v curl)"
  else
    CURL="curl"
  fi
fi

# check_llm_openai <label> <base_url> <api_key> <model>
#   OpenAI 兼容：GET {base}/models 只验证 auth+URL，不消耗 token。返回 0 通过 / 1 失败。
check_llm_openai() {
  local label="$1" base="$2" key="$3" model="$4"
  base="${base%/}"
  base="${base%/messages}"
  base="${base%/chat/completions}"
  local url="${base}/models"
  local code body_file=/tmp/llm-check.$$
  code=$("$CURL" -sS --max-time 10 -o "$body_file" -w "%{http_code}" \
    -H "Authorization: Bearer $key" "$url" 2>/dev/null || echo "000")
  local rc=0
  if [[ "$code" == "200" ]]; then
    if grep -q "\"$model\"" "$body_file" 2>/dev/null; then
      ok "$label OpenAI 协议通路 OK（$model 在 /models 列表内）"
    else
      ok "$label OpenAI 协议通路 OK（未在 /models 里显式列出 ${model}，业务侧仍可能可用）"
    fi
  elif [[ "$code" == "401" || "$code" == "403" ]]; then
    warn "$label API key 无效（HTTP ${code}）：$url"
    head -c 200 "$body_file" >&2; echo >&2
    rc=1
  elif [[ "$code" == "404" ]]; then
    warn "$label GET /models 404 —— 该厂商可能没有该端点，改用 anthropic 协议检查"
    rm -f "$body_file"
    check_llm_anthropic "$label" "$base" "$key" "$model"
    return $?
  else
    warn "$label 无法访问 ${url}（HTTP=${code}）$(head -c 100 "$body_file" 2>/dev/null)"
    rc=1
  fi
  rm -f "$body_file"
  return $rc
}

# check_llm_anthropic <label> <base_url> <api_key> <model>
#   Anthropic：POST {base}/v1/messages 发 max_tokens=1，消耗 ≤ 10 token。返回 0/1。
check_llm_anthropic() {
  local label="$1" base="$2" key="$3" model="$4"
  base="${base%/}"
  local url
  if [[ "$base" == */messages ]]; then
    url="$base"
  elif [[ "$base" == */v1 ]]; then
    url="${base}/messages"
  else
    url="${base}/v1/messages"
  fi
  local code body_file=/tmp/llm-check.$$
  code=$("$CURL" -sS --max-time 15 -o "$body_file" -w "%{http_code}" \
    -X POST -H "Content-Type: application/json" \
    -H "x-api-key: $key" -H "Authorization: Bearer $key" \
    -H "anthropic-version: 2023-06-01" \
    -d "{\"model\":\"$model\",\"max_tokens\":1,\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}" \
    "$url" 2>/dev/null || echo "000")
  local rc=0
  case "$code" in
    200) ok "$label Anthropic 协议通路 OK（模型 $model 已应答）" ;;
    401|403)
      warn "$label API key 无效（HTTP ${code}）：$url"
      head -c 200 "$body_file" >&2; echo >&2
      rc=1 ;;
    404)
      warn "$label URL 不存在（HTTP 404）：$url —— 检查 BASE_URL"
      rc=1 ;;
    400)
      if grep -qE "model.*not.*found|invalid.*model|model_not_found" "$body_file" 2>/dev/null; then
        warn "$label 模型名 '$model' 无效（HTTP 400）"
        rc=1
      else
        warn "$label HTTP 400（可能是参数格式问题，非通路错）：$(head -c 150 "$body_file")"
        rc=0
      fi ;;
    *)
      warn "$label 无法访问 ${url}（HTTP=${code}）$(head -c 100 "$body_file" 2>/dev/null)"
      rc=1 ;;
  esac
  rm -f "$body_file"
  return $rc
}

# check_llm_group <label> <base_url> <api_key> <model> <protocol>
check_llm_group() {
  local label="$1" base="$2" key="$3" model="$4" proto="${5:-openai}"
  info "检查 $label 通路（协议=${proto}）..."
  case "$proto" in
    anthropic) check_llm_anthropic "$label" "$base" "$key" "$model" ;;
    *)         check_llm_openai    "$label" "$base" "$key" "$model" ;;
  esac
}

# ═══════════════════════════════════════════════════════════════
# 交互式输入辅助
# ═══════════════════════════════════════════════════════════════

# prompt_with_default <label> <default>
#   打印 "label [default]: "，读一行；空输入返回 default。结果输出到 stdout。
prompt_with_default() {
  local label="$1" default="${2:-}"
  # 提示走 stderr，结果走 stdout（供 $(...) 捕获，避免把提示文本也捕获进来）
  if [[ -n "$default" ]]; then
    printf '%s [%s]: ' "$label" "$default" >&2
  else
    printf '%s: ' "$label" >&2
  fi
  local input
  IFS= read -r input || { printf '\n' >&2; printf '%s' "$default"; return 0; }
  if [[ -z "$input" ]]; then
    printf '%s' "$default"
  else
    printf '%s' "$input"
  fi
}

# prompt_protocol <default>
#   让用户确认 LLM 协议（openai/anthropic），非法输入回退 openai。
prompt_protocol() {
  local default="${1:-openai}"
  printf 'memory 组 LLM 协议（openai/anthropic）[%s]: ' "$default" >&2
  local input
  IFS= read -r input || { printf '\n' >&2; printf '%s' "$default"; return 0; }
  input="${input:-$default}"
  case "$input" in
    openai|anthropic) printf '%s' "$input" ;;
    *) warn "未知协议 '$input'，回退到 openai"; printf 'openai' ;;
  esac
}

# prompt_confirm <question> <default_yes:0|1>
#   返回 0=是 / 1=否
prompt_confirm() {
  local question="$1" default_yes="${2:-0}"
  local hint
  if [[ "$default_yes" == "1" ]]; then hint="[Y/n]"; else hint="[y/N]"; fi
  printf '%s %s: ' "$question" "$hint" >&2
  local input
  IFS= read -r input || return 1
  case "$input" in
    [yY]|[yY][eE][sS]) return 0 ;;
    [nN]|[nN][oO])     return 1 ;;
    "")                [[ "$default_yes" == "1" ]] && return 0 || return 1 ;;
    *)                 return 1 ;;
  esac
}

# set_env_value <key> <value> <file>
#   就地更新/追加 .env 里的 KEY=VALUE。用 awk 原样输出，避免 sed/perl 转义坑。
set_env_value() {
  local key="$1" value="$2" file="$3"
  if grep -qE "^[[:space:]]*${key}=" "$file"; then
    local tmp="$file.tmp.$$"
    awk -v k="$key" -v v="$value" '
      $0 ~ ("^[[:space:]]*" k "=") { print k "=" v; next }
      { print }
    ' "$file" > "$tmp" && mv "$tmp" "$file"
  else
    printf '%s=%s\n' "$key" "$value" >> "$file"
  fi
}

# ensure_knowledge_service_key
#   Knowledge 服务间鉴权 key（Panel ↔ Knowledge 共用）。
#   为空或占位符时自动生成随机值并回写 .env（幂等：已生成则复用，重启不漂移）。
#   调用方随后把同一个值分两次注入 memory-hub 容器：
#     KNOWLEDGE_SERVICE_KEY → Knowledge 进程校验侧
#     KNOWLEDGE_AUTH_TOKEN  → Panel 进程调用侧
ensure_knowledge_service_key() {
  if [[ -z "${KNOWLEDGE_SERVICE_KEY:-}" || "${KNOWLEDGE_SERVICE_KEY}" == "REPLACE_ME" ]]; then
    local rand
    if command -v openssl >/dev/null 2>&1; then
      rand="$(openssl rand -hex 24)"
    else
      rand="$(head -c 24 /dev/urandom | od -A n -t x1 | tr -d ' \n')"
    fi
    KNOWLEDGE_SERVICE_KEY="ks-svc-${rand}"
    set_env_value KNOWLEDGE_SERVICE_KEY "$KNOWLEDGE_SERVICE_KEY" "$ENV_FILE"
    info "已生成 KNOWLEDGE_SERVICE_KEY 并写回 ${ENV_FILE}（Panel 与 Knowledge 共用）"
  fi
}

# interactive_llm_setup
#   交互式引导填写 LLM 两组（memory + proxy）→ 通路检查（可重试）→ 写回 .env。
#   会更新内存中的 MEMORY_LLM_* / PROXY_UPSTREAM_* 变量（已 export），并写回 .env 持久化。
interactive_llm_setup() {
  local base key model proto reuse_same

  echo ""
  info "═══ 交互式配置 LLM（回车 = 保留当前值） ═══════════════════"

  # ── memory 组 ──
  while true; do
    base=$(prompt_with_default "memory 组 LLM BASE_URL" "${MEMORY_LLM_BASE_URL:-}")
    key=$(prompt_with_default "memory 组 LLM API_KEY" "${MEMORY_LLM_API_KEY:-}")
    model=$(prompt_with_default "memory 组 LLM MODEL" "${MEMORY_LLM_MODEL:-}")
    proto=$(prompt_protocol "${MEMORY_LLM_PROTOCOL:-openai}")

    if check_llm_group "memory 组" "$base" "$key" "$model" "$proto"; then
      MEMORY_LLM_BASE_URL="$base"
      MEMORY_LLM_API_KEY="$key"
      MEMORY_LLM_MODEL="$model"
      MEMORY_LLM_PROTOCOL="$proto"
      break
    fi
    warn "memory 组 LLM 通路检查未通过。"
    prompt_confirm "是否重新输入？" 1 || die "用户放弃，退出。"
  done

  # ── proxy 组 ──
  # 默认复用（回车即复用）的情况：
  #   1) .env 里 proxy 组与刚填的 memory 组完全相同；
  #   2) proxy 组仍为空 / REPLACE_ME（首次使用尚未配置，默认复用更顺手）。
  reuse_same=0
  if [[ "${PROXY_UPSTREAM_URL:-}" == "$MEMORY_LLM_BASE_URL" && \
        "${PROXY_UPSTREAM_API_KEY:-}" == "$MEMORY_LLM_API_KEY" && \
        "${PROXY_UPSTREAM_MODEL:-}" == "$MEMORY_LLM_MODEL" ]]; then
    reuse_same=1
  elif [[ -z "${PROXY_UPSTREAM_URL:-}" || "${PROXY_UPSTREAM_URL:-}" == "REPLACE_ME" ]] && \
       [[ -z "${PROXY_UPSTREAM_API_KEY:-}" || "${PROXY_UPSTREAM_API_KEY:-}" == "REPLACE_ME" ]] && \
       [[ -z "${PROXY_UPSTREAM_MODEL:-}" || "${PROXY_UPSTREAM_MODEL:-}" == "REPLACE_ME" ]]; then
    reuse_same=1
  fi

  if prompt_confirm "proxy 组是否复用 memory 组的 LLM 配置？" "$reuse_same"; then
    PROXY_UPSTREAM_URL="$MEMORY_LLM_BASE_URL"
    PROXY_UPSTREAM_API_KEY="$MEMORY_LLM_API_KEY"
    PROXY_UPSTREAM_MODEL="$MEMORY_LLM_MODEL"
    ok "proxy 组复用 memory 组配置，跳过重复检查"
  else
    while true; do
      base=$(prompt_with_default "proxy 组 UPSTREAM_URL" "${PROXY_UPSTREAM_URL:-}")
      key=$(prompt_with_default "proxy 组 UPSTREAM_API_KEY" "${PROXY_UPSTREAM_API_KEY:-}")
      model=$(prompt_with_default "proxy 组 UPSTREAM_MODEL" "${PROXY_UPSTREAM_MODEL:-}")

      if check_llm_group "proxy 组" "$base" "$key" "$model" openai; then
        PROXY_UPSTREAM_URL="$base"
        PROXY_UPSTREAM_API_KEY="$key"
        PROXY_UPSTREAM_MODEL="$model"
        break
      fi
      warn "proxy 组 LLM 通路检查未通过。"
      prompt_confirm "是否重新输入？" 1 || die "用户放弃，退出。"
    done
  fi

  # ── 写回 .env ──
  info "写回 LLM 配置 → $ENV_FILE"
  set_env_value MEMORY_LLM_BASE_URL "$MEMORY_LLM_BASE_URL" "$ENV_FILE"
  set_env_value MEMORY_LLM_API_KEY "$MEMORY_LLM_API_KEY" "$ENV_FILE"
  set_env_value MEMORY_LLM_MODEL "$MEMORY_LLM_MODEL" "$ENV_FILE"
  set_env_value MEMORY_LLM_PROTOCOL "$MEMORY_LLM_PROTOCOL" "$ENV_FILE"
  set_env_value PROXY_UPSTREAM_URL "$PROXY_UPSTREAM_URL" "$ENV_FILE"
  set_env_value PROXY_UPSTREAM_API_KEY "$PROXY_UPSTREAM_API_KEY" "$ENV_FILE"
  set_env_value PROXY_UPSTREAM_MODEL "$PROXY_UPSTREAM_MODEL" "$ENV_FILE"
  ok "LLM 配置已保存到 $ENV_FILE"
}

# ═══════════════════════════════════════════════════════════════
# 端口预检
# ═══════════════════════════════════════════════════════════════

# port_in_use <port>
#   检测宿主机某端口是否处于 LISTEN 状态。返回 0=被占 / 1=空闲。
port_in_use() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
  elif command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE ":${port}$"
  else
    return 1  # 无检测工具时当作空闲，不阻塞启动
  fi
}

# tdai_self_ports
#   输出当前 running 的 tdai 三件套容器映射到宿主机的端口（空格分隔）。
#   这些端口是「自己人」占用，会在启动时被 rm_container_if_exists 重建，不算冲突。
tdai_self_ports() {
  local c p ports=""
  for c in tdai-proxy tdai-memory-hub tdai-memory-core; do
    if $DOCKER ps --format '{{.Names}}' 2>/dev/null | grep -qx "$c"; then
      p="$($DOCKER port "$c" 2>/dev/null | grep -oE '[0-9]+$' | sort -u | tr '\n' ' ' || true)"
      ports="$ports $p"
    fi
  done
  printf '%s' "$ports"
}

# check_ports
#   一次性检查 4 个目标端口是否被占用；被「外部进程」占用则报错退出。
#   会排除 tdai 自己容器占用的端口（那些会在启动时被重建）。
check_ports() {
  local self_ports port_var port conflict=0
  self_ports=" $(tdai_self_ports) "
  info "═══ 端口预检 ══════════════════════════════════════════"
  for port_var in MEMORY_CORE_PORT PANEL_PORT KNOWLEDGE_PORT PROXY_PORT; do
    port="${!port_var:-}"
    if [[ -z "$port" ]]; then continue; fi
    if [[ "$self_ports" == *" $port "* ]]; then
      info "端口 $port ($port_var) 由 tdai 旧容器占用（启动时会重建），跳过"
      continue
    fi
    if port_in_use "$port"; then
      echo "${C_RED}[error]${C_RST} 端口 $port ($port_var) 已被占用，请释放该端口或在 .env 改端口。" >&2
      conflict=1
    else
      ok "端口 $port ($port_var) 空闲"
    fi
  done
  (( conflict == 0 )) || die "存在端口冲突，请先释放端口后重试。"
}
