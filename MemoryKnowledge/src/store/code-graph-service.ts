/**
 * CodeGraphService — code-graph 资产的异步编排。
 *
 * 把 IKnowledgeStore（元数据/状态）+ BuildQueue（后台串行）+ 可注入的
 * worker（实际 git clone + codegraph 建图）粘合，实现：
 *   - create/sync 立即返回（fire-and-forget），管控轮询 status；
 *   - 状态机 pending → processing(cloning/indexing) → ready / failed(+sync_error)；
 *   - memory + team 隔离、幂等（同 memory+team+repo+branch 返回已存在）、硬删 + 四类资源清理。
 *
 * delete 语义（008 / 007 §5.5）：任何状态（含 pending/processing）均可删。
 * 用内存 cancelled 标记通知 in-flight worker 中止（不落库、不软删）；worker 在
 * 结束前的检查点发现被删则跳过 ready/回调并做幂等清理。清理覆盖四类资源：
 * instance pool（内存）→ 元数据行（硬删）→ 磁盘目录（rmSync），分步 try/catch
 * 保证任一步失败不影响其余（异常安全 + 幂等）。远端元数据上报本阶段不做。
 *
 * worker 注入便于单测（无需真实 git/codegraph）；生产实现见 router 装配处。
 * 物理目录：{dataRoot}/{service_id}/{team_id}/{code_graph_id}/（001 多租户）。
 */

import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { existsSync, readdirSync, rmSync, statSync } from "node:fs";

import type {
  AuditAction,
  CodeGraphRow,
  IKnowledgeStore,
  ListOpts,
  CountOpts,
} from "./types.js";
import { BuildQueue } from "./build-queue.js";
import type { ICredentialStore } from "../source-auth/types.js";
import { SSH_PROVIDER_ID } from "../code-source/providers/ssh.js";
import type { AuthMethodKind } from "../code-source/auth-methods/types.js";
import type { CredentialKind } from "../source-auth/types.js";

export interface CodeGraphBuildContext {
  codeGraphId: string;
  serviceId: string;
  teamId: string;
  repoUrl: string;
  branch: string;
  /** 该资产的本地工作目录（checkout + 索引落此）。 */
  dir: string;
  /** worker 可调用以更新细粒度内部状态（cloning → indexing）。 */
  setInternalStatus: (s: string) => void;
  /**
   * 资产归属用户（SSH clone 用它定位该用户的私钥）。
   * 取 `owner_user_id ?? user_id`；两者都缺时为空串（worker 据此报「未配置 SSH 公钥」）。
   */
  ownerUserId?: string;
}

export interface CodeGraphBuildResult {
  commitHash?: string;
  stats?: { files: number; nodes: number; edges: number };
}

export type CodeGraphWorker = (ctx: CodeGraphBuildContext) => Promise<CodeGraphBuildResult>;

/**
 * sync 结果（判别联合）：
 *   - ok       已入队重建；
 *   - not_found memory/team/id 不匹配；
 *   - busy     正在 pending/processing（并发拒绝，对应 HTTP 409），step 为内部阶段（可 null）。
 */
export type SyncResult =
  | { kind: "ok"; row: CodeGraphRow }
  | { kind: "not_found" }
  | { kind: "busy"; status: "pending" | "processing"; step: string | null };

/**
 * build 的选项（透传至 runBuild；不传则行为与 create/sync 完全一致）。
 */
export interface BuildOptions {
  /**
   * true = 跳过 fetch/clone，直接用该资源**已有目录**的内容建索引。
   *
   * 目录恒为 `{dataRoot}/{service_id}/{team_id}/{code_graph_id}`，
   * 不接受任何外部传入路径（无路径穿越面，见设计文档 §4）。
   */
  fromLocalDir?: boolean;
}

/**
 * build 结果：在 sync 的判别联合基础上多一个「目录缺失/为空」分支。
 *
 * 单独成成员而不是复用 not_found —— 两者的处置完全不同：
 * not_found 是资源不存在（404），empty_dir 是资源存在但没内容可建（400）。
 */
export type BuildResult = SyncResult | { kind: "empty_dir" };

export interface CodeGraphServiceLogger {
  info?: (msg: string) => void;
  warn?: (msg: string) => void;
  error?: (msg: string) => void;
}

export interface CodeGraphServiceOptions {
  store: IKnowledgeStore;
  /** knowledge 数据根目录；资产目录 = {dataRoot}/{service_id}/{team_id}/{code_graph_id}/。 */
  dataRoot: string;
  /** 常规 worker：拉取（clone/fetch）+ 建索引。create / sync 走它。 */
  worker: CodeGraphWorker;
  /**
   * 本地构建 worker：**只建索引，不拉取**。`POST /code-graph/build` 走它。
   *
   * 独立注入而非在 worker 里加分支 —— 保持线上 clone 链路的执行体单一语义，
   * 新增路径的回归风险不落在主链路上（设计文档 §3.1）。
   * 未注入时 build() 直接返回 empty_dir 之外的路径会失败，由装配层保证注入。
   */
  localWorker?: CodeGraphWorker;
  queue?: BuildQueue;
  logger?: CodeGraphServiceLogger;
  /** Callback config for TMC status notifications. Optional. */
  callbackConfig?: { tmcCallbackUrl: string };
  /**
   * 外部来源凭据存储（create 带 credential 时在入队前先落库）。
   * 未注入时 create 不接受 credential 参数（公开仓场景）。
   */
  credentialStore?: ICredentialStore;
  /**
   * 代码来源 provider 注册中心（用于读 provider 声明的认证方式）。
   * 只依赖 `authMethod.kind` 这一个字段，避免 store 层反向依赖 code-source 全量类型。
   */
  codeSourceRegistry?: {
    get(id: string): { authMethod: { kind: AuthMethodKind } } | undefined;
  };
  /**
   * 释放该 code-graph 占用的内存资源（instance pool + 关闭索引句柄）。
   * 注入而非直依赖 module，保持 store 层不反向依赖装配层。幂等：重复调用安全。
   * 由 module.ts 装配时提供（封装 instancePool.delete + closeIndex）。
   */
  releaseInstance?: (codeGraphId: string) => void;
}

export interface CreateCodeGraphParams {
  service_id: string;
  team_id: string;
  repo_url: string;
  branch: string;
  repo_name?: string;
  owner_user_id?: string;
  user_id?: string;
  agent_id?: string;
  task_id?: string;
  visibility?: string;
  /**
   * 外部来源凭据（私有仓）。与 create 同请求传入，KS 在**入队建图前**先落库，
   * 消除「create 立即 clone、凭据尚未写入」的时序窗口。
   */
  credential?: {
    provider_id: string;
    secret: string;
    username?: string;
  };
}

export class CodeGraphService {
  private readonly store: IKnowledgeStore;
  private readonly dataRoot: string;
  private readonly worker: CodeGraphWorker;
  private readonly localWorker?: CodeGraphWorker;
  private readonly queue: BuildQueue;
  private readonly logger?: CodeGraphServiceLogger;
  private readonly callbackConfig?: { tmcCallbackUrl: string };
  private readonly releaseInstance?: (codeGraphId: string) => void;
  private readonly credentialStore?: ICredentialStore;
  private readonly codeSourceRegistry?: CodeGraphServiceOptions["codeSourceRegistry"];
  /**
   * In-flight delete 标记：delete 命中一个正在排队/执行的资源时置位，
   * worker 在检查点读取以决定中止。仅内存态（同 id 由 SerialQueue 串行 +
   * Node 单线程，读写无并发）。清理收尾后移除。
   */
  private readonly cancelled = new Set<string>();

  constructor(opts: CodeGraphServiceOptions) {
    this.store = opts.store;
    this.dataRoot = opts.dataRoot;
    this.localWorker = opts.localWorker;
    this.worker = opts.worker;
    this.queue = opts.queue ?? new BuildQueue();
    this.logger = opts.logger;
    this.callbackConfig = opts.callbackConfig;
    this.releaseInstance = opts.releaseInstance;
    this.credentialStore = opts.credentialStore;
    this.codeSourceRegistry = opts.codeSourceRegistry;
  }

  dirFor(serviceId: string, teamId: string, codeGraphId: string): string {
    return join(this.dataRoot, serviceId, teamId, codeGraphId);
  }

  /**
   * 幂等创建并异步建图。
   * - 已存在（同 memory+team+repo+branch）→ 直接返回已有行，不重复建图。
   * - 新建 → 入库 pending + 后台建图。
   */
  async create(params: CreateCodeGraphParams): Promise<{ row: CodeGraphRow; existed: boolean }> {
    // A new code-graph is born pending with its first build run id (the insert is its claim).
    const runId = randomUUID();
    const { row, existed } = await this.store.createCodeGraph({ ...params, build_run_id: runId });
    if (!existed) {
      // ① 私有仓：入队前先落凭据（否则 worker 立刻 clone 时凭据还不存在 → 401）。
      //
      // 例外：SSH 来源不落凭据行 —— 它用「每用户私钥 + GIT_SSH_COMMAND」而非
      // URL 注入令牌。若落了凭据行，worker 会把 ssh:// 地址丢给 basic-auth
      // 拼装器拼成 https://... 直接炸掉。详见设计文档 §2.2 D2。
      const isSshSource = params.credential?.provider_id === SSH_PROVIDER_ID;
      if (params.credential && !isSshSource) {
        if (!this.credentialStore) {
          throw new Error("credential provided but credentialStore is not wired");
        }
        // 落库 kind 按 provider 的认证方式决定；未注入 registry 时兼容退化为 bearer。
        //
        // AuthMethodKind 比 CredentialKind 多一个 "ssh-key"（协议级、不落库）。
        // 此处显式收窄：非 bearer/basic 一律按 bearer 处理 —— 能走到这里的
        // 一定不是 ssh（上面已排除），收窄只为让类型系统满意，不改变运行时行为。
        const declared =
          this.codeSourceRegistry?.get(params.credential.provider_id)?.authMethod.kind ?? "bearer";
        const kind: CredentialKind = declared === "basic" ? "basic" : "bearer";
        await this.credentialStore.put(
          { type: "code-graph", serviceId: row.service_id, resourceId: row.code_graph_id },
          { kind, secret: params.credential.secret, username: params.credential.username },
          params.credential.provider_id,
          params.user_id,
        );
      }
      await this.audit(row, "create", `clone ${row.repo_url}@${row.branch}`, params.user_id);
      this.enqueueBuild(row, runId);
    }
    return { row, existed };
  }

  /** Persist service_url for a code-graph. Returns updated row or null. */
  async updateServiceUrl(serviceId: string, codeGraphId: string, serviceUrl: string): Promise<CodeGraphRow | null> {
    await this.store.updateCodeGraphStatus(serviceId, codeGraphId, { service_url: serviceUrl });
    return this.store.getCodeGraphById(serviceId, codeGraphId);
  }

  /** Update code-graph metadata (repo_name, summary). Returns updated row or null. */
  async updateMeta(serviceId: string, codeGraphId: string, patch: { repo_name?: string; summary?: string | null }): Promise<CodeGraphRow | null> {
    return this.store.updateCodeGraphMeta(serviceId, codeGraphId, patch);
  }

  /** 重新拉取 + 重建（管控显式触发）。memory/team 不匹配返回 not_found；pending/processing 返回 busy。 */
  async sync(serviceId: string, teamId: string, codeGraphId: string, requesterUserId?: string): Promise<SyncResult> {
    // busy 判定 + 置 pending（version+1, build_run_id）由 store 一步原子完成。
    // 并发拒绝：正在排队/执行中直接 busy，不覆盖状态、不重复入队、不写 audit。
    const runId = randomUUID();
    const claim = await this.store.claimCodeGraphBuild(serviceId, teamId, codeGraphId, { runId });
    if (claim.kind !== "claimed") return claim;
    await this.audit(claim.row, "ingest", "manual sync", requesterUserId);
    this.enqueueBuild(claim.row, runId);
    return { kind: "ok", row: claim.row };
  }

  /**
   * 本地目录构建：**不拉取**，直接用该资源已有目录的内容重建索引。
   *
   * 与 sync 的差别只有两点：
   *   1. 入队前多一道目录非空校验（缺失/为空 → empty_dir，不入队）
   *   2. 入队时带 `{ fromLocalDir: true }`，runBuild 据此换 localWorker
   *
   * 用途：① 目录内容已就位、只想重建索引；② 把 failed 的资源修复为 ready
   * （成功分支无条件写 ready + 清 sync_error，对原状态无要求，见设计文档 §3.3）。
   */
  async build(serviceId: string, teamId: string, codeGraphId: string, requesterUserId?: string): Promise<BuildResult> {
    // 先读一次决定响应优先级（not_found > busy > empty_dir，与改造前一致）；
    // 该读只是建议性的，真正的状态转换由下方 claim 原子完成。
    const row = await this.store.getCodeGraph(serviceId, teamId, codeGraphId);
    if (!row) return { kind: "not_found" };
    if (row.status === "pending" || row.status === "processing") {
      return { kind: "busy", status: row.status, step: row.internal_status };
    }
    // 目录校验前置：不存在/为空直接返回，避免入队后必然 failed 的难排查结果。
    if (!this.isNonEmptyDir(this.dirFor(serviceId, teamId, codeGraphId))) {
      return { kind: "empty_dir" };
    }
    const runId = randomUUID();
    const claim = await this.store.claimCodeGraphBuild(serviceId, teamId, codeGraphId, { runId });
    if (claim.kind !== "claimed") return claim;
    await this.audit(claim.row, "ingest", "build from local dir", requesterUserId);
    this.enqueueBuild(claim.row, runId, { fromLocalDir: true });
    return { kind: "ok", row: claim.row };
  }

  /** 目录存在且非空（用于 build 的前置校验）。 */
  private isNonEmptyDir(dir: string): boolean {
    try {
      if (!existsSync(dir) || !statSync(dir).isDirectory()) return false;
      return readdirSync(dir).length > 0;
    } catch {
      return false;
    }
  }

  async get(serviceId: string, teamId: string, codeGraphId: string): Promise<CodeGraphRow | null> {
    return this.store.getCodeGraph(serviceId, teamId, codeGraphId);
  }

  /** 按全局唯一 code_graph_id 查询（仍按 service_id 收敛防跨租户）。spec id-only 端点专用。 */
  async getById(serviceId: string, codeGraphId: string): Promise<CodeGraphRow | null> {
    return this.store.getCodeGraphById(serviceId, codeGraphId);
  }

  async list(serviceId: string, teamId: string, opts?: ListOpts): Promise<CodeGraphRow[]> {
    return this.store.listCodeGraphs(serviceId, teamId, opts);
  }

  async count(serviceId: string, teamId: string, opts?: CountOpts): Promise<number> {
    return this.store.countCodeGraphs(serviceId, teamId, opts);
  }

  /**
   * 删除 code-graph（008 / 007 §5.5）。任何状态均可删（含 pending/processing）。
   * memory/team 不匹配返回 false；否则硬删 + 四类资源清理，返回 true。
   *
   * 若资源正在排队/执行（pending/processing），先置 cancelled 标记通知 worker
   * 在检查点中止；随后立即硬删 + 清理（不等 worker）。worker 结束前重查发现
   * 已删则跳过 ready/回调并再做一次幂等清理，无残留。
   */
  async delete(serviceId: string, teamId: string, codeGraphId: string): Promise<boolean> {
    const row = await this.store.getCodeGraph(serviceId, teamId, codeGraphId);
    if (!row) return false;

    // 通知 in-flight worker 中止（pending 排队 or processing 执行中）。
    if (row.status === "pending" || row.status === "processing") {
      this.cancelled.add(codeGraphId);
    }

    await this.audit(row, "delete", null);
    await this.cleanupResources(serviceId, teamId, codeGraphId);

    // worker 若仍在跑，会在检查点看到行已被硬删（getById → null）而中止；
    // cancelled 标记留到 worker 结束由其自行清理（见 runBuild），此处不删标记，
    // 以覆盖“delete 先于 worker 检查点完成”的窗口。cleanup 幂等，重复无害。
    return true;
  }

  /**
   * 四类资源幂等清理（顺序：先释放内存/连接，再删盘）。
   * 每步独立 try/catch —— 任一步失败不影响其余，保证异常安全。
   *   1. instance pool（内存）：releaseInstance（pool.delete + closeIndex）
   *   2. 元数据行：硬删（命中 0 行也安全，支持 worker + delete 双重清理）
   *   3. 来源凭据行：删除（幂等；无行也安全）
   *   4. 磁盘目录：rmSync recursive+force（幂等）
   * 每步 await 完成后再走下一步（行删完才删盘）。
   * BuildQueue 排队任务由 runBuild 入口检查 cancelled/行存在性跳过，无需在此处理。
   */
  private async cleanupResources(serviceId: string, teamId: string, codeGraphId: string): Promise<void> {
    try {
      this.releaseInstance?.(codeGraphId);
    } catch (err) {
      this.logger?.warn?.(`[code-graph] release instance failed ${codeGraphId}: ${String(err)}`);
    }
    try {
      await this.store.deleteCodeGraph(serviceId, teamId, codeGraphId);
    } catch (err) {
      this.logger?.warn?.(`[code-graph] hard-delete row failed ${codeGraphId}: ${String(err)}`);
    }
    if (this.credentialStore) {
      try {
        await this.credentialStore.delete({ type: "code-graph", serviceId, resourceId: codeGraphId });
      } catch (err) {
        this.logger?.warn?.(`[code-graph] delete credential failed ${codeGraphId}: ${String(err)}`);
      }
    }
    try {
      rmSync(this.dirFor(serviceId, teamId, codeGraphId), { recursive: true, force: true });
    } catch (err) {
      this.logger?.warn?.(`[code-graph] rm dir failed ${codeGraphId}: ${String(err)}`);
    }
  }

  /**
   * worker 检查点：资源是否已被删除（cancelled 标记命中，或行已不在库）。
   * 双判据覆盖：①delete 发生在 worker 运行中（cancelled）；②delete 已完成
   * 且行被硬删（getById → null）。任一即视为已删。
   */
  private async isDeleted(serviceId: string, codeGraphId: string): Promise<boolean> {
    return this.cancelled.has(codeGraphId) || (await this.store.getCodeGraphById(serviceId, codeGraphId)) === null;
  }

  /** 写一条 code-graph 审计记录。失败不阻断主流程。 */
  private async audit(row: CodeGraphRow, action: AuditAction, detail: string | null, requesterUserId?: string): Promise<void> {
    try {
      await this.store.appendCodeGraphAudit({
        service_id: row.service_id,
        asset_id: row.code_graph_id,
        version: row.version,
        action,
        // 优先记录触发者（sync/create 的发起人），回退到行上的创建者。
        user_id: requesterUserId ?? row.user_id,
        agent_id: row.agent_id,
        detail,
      });
    } catch (err) {
      this.logger?.warn?.(`[code-graph] audit ${action} failed: ${String(err)}`);
    }
  }

  private enqueueBuild(row: CodeGraphRow, runId: string, opts?: BuildOptions): void {
    this.queue.enqueue(row.code_graph_id, () =>
      this.runBuild(
        row.service_id,
        row.code_graph_id,
        row.team_id,
        row.repo_url,
        row.branch,
        runId,
        row.owner_user_id ?? row.user_id ?? undefined,
        opts,
      ),
    );
  }

  private async runBuild(
    serviceId: string,
    codeGraphId: string,
    teamId: string,
    repoUrl: string,
    branch: string,
    runId: string,
    ownerUserId?: string,
    opts?: BuildOptions,
  ): Promise<void> {
    // 本次 build 的写栅栏：build_run_id ≠ runId（被删除 / 新 claim / 重启恢复）时写入匹配 0 行。
    const fence = { runId };
    // 入口检查点：pending 期间被删 → 直接跳过，不置 processing、不建图。
    if (await this.isDeleted(serviceId, codeGraphId)) {
      await this.finishCancelled(serviceId, teamId, codeGraphId);
      return;
    }
    // 本地构建没有 clone 步骤，写 "cloning" 会误导（前端/运维据此判断卡在哪一步）。
    const fromLocalDir = opts?.fromLocalDir === true;
    const started = await this.store.updateCodeGraphStatus(
      serviceId,
      codeGraphId,
      { status: "processing", internal_status: fromLocalDir ? "indexing" : "cloning", sync_error: null },
      fence,
    );
    if (!started) {
      this.logger?.info?.(`[code-graph] ${codeGraphId} build ${runId} superseded before start, skipped`);
      return;
    }
    // 进度写入串成本 build 的 promise 链；对 worker 保持 void、永不 reject（只记日志）。
    let progress: Promise<void> = Promise.resolve();
    const setInternalStatus = (s: string): void => {
      progress = progress
        .then(async () => {
          await this.store.updateCodeGraphStatus(
            serviceId,
            codeGraphId,
            { status: "processing", internal_status: s },
            fence,
          );
        })
        .catch((err: unknown) => {
          this.logger?.warn?.(`[code-graph] ${codeGraphId} progress write '${s}' failed: ${String(err)}`);
        });
    };
    // 本地构建换 worker；未注入 localWorker 时退化到常规 worker（保守：宁可走原路径也不静默跳过）。
    const worker = fromLocalDir ? (this.localWorker ?? this.worker) : this.worker;
    try {
      const result = await worker({
        codeGraphId,
        serviceId,
        teamId,
        repoUrl,
        branch,
        dir: this.dirFor(serviceId, teamId, codeGraphId),
        setInternalStatus,
        ownerUserId,
      });
      await progress;
      // 结束前检查点：processing 期间被删 → 跳过 ready/audit/回调，做幂等收尾清理。
      if (await this.isDeleted(serviceId, codeGraphId)) {
        await this.finishCancelled(serviceId, teamId, codeGraphId);
        return;
      }
      const wrote = await this.store.updateCodeGraphStatus(
        serviceId,
        codeGraphId,
        {
          status: "ready",
          internal_status: null,
          sync_error: null,
          commit_hash: result.commitHash ?? null,
          stats_json: result.stats ? JSON.stringify(result.stats) : null,
          last_sync_at: new Date().toISOString(),
          build_run_id: null,
        },
        fence,
      );
      if (!wrote) {
        this.logger?.info?.(`[code-graph] ${codeGraphId} build ${runId} superseded, ready write skipped`);
        return;
      }
      const synced = await this.store.getCodeGraphById(serviceId, codeGraphId);
      if (synced) {
        await this.audit(synced, "ready", result.stats ? JSON.stringify(result.stats) : null);
      }
      this.logger?.info?.(`[code-graph] ${codeGraphId} ready`);

      // Auto-generate summary + callback TMC
      await this.onBuildComplete(synced, "ready", null, result.stats ?? null);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      await progress;
      // worker 抛错，但若期间已被删，视为取消而非失败：跳过 failed 状态/回调，做清理。
      if (await this.isDeleted(serviceId, codeGraphId)) {
        await this.finishCancelled(serviceId, teamId, codeGraphId);
        return;
      }
      const wrote = await this.store.updateCodeGraphStatus(
        serviceId,
        codeGraphId,
        { status: "failed", internal_status: null, sync_error: msg.slice(0, 500), build_run_id: null },
        fence,
      );
      if (!wrote) {
        this.logger?.info?.(`[code-graph] ${codeGraphId} build ${runId} superseded, failed write skipped: ${msg}`);
        return;
      }
      const failed = await this.store.getCodeGraphById(serviceId, codeGraphId);
      if (failed) await this.audit(failed, "failed", msg.slice(0, 500));
      this.logger?.warn?.(`[code-graph] ${codeGraphId} failed: ${msg}`);

      // Callback TMC about failure
      await this.onBuildComplete(failed, "failed", msg, null);
    }
  }

  /**
   * worker 检查点判定“已删”后的收尾：幂等清理 worker 可能刚写下的盘/句柄，
   * 并移除 cancelled 标记（该 id 的 worker 到此结束，标记使命完成）。
   */
  private async finishCancelled(serviceId: string, teamId: string, codeGraphId: string): Promise<void> {
    await this.cleanupResources(serviceId, teamId, codeGraphId);
    this.cancelled.delete(codeGraphId);
    this.logger?.info?.(`[code-graph] ${codeGraphId} build aborted (deleted during processing)`);
  }

  /**
   * Post-build hook: generate summary (if synced) and callback TMC.
   * Never throws — runs after the main build is already committed.
   */
  private async onBuildComplete(
    row: CodeGraphRow | null,
    status: "ready" | "failed",
    errorMsg: string | null,
    stats: { files: number; nodes: number; edges: number } | null,
  ): Promise<void> {
    if (!row || !this.callbackConfig) return;

    let summary: string | null = null;

    if (status === "ready") {
      // Generate summary via template (no LLM for code-graph)
      const { generateCodeGraphSummary } = await import("../callback.js");
      summary = generateCodeGraphSummary(row.repo_name || row.repo_url, row.branch, stats);
      if (summary) {
        // Fence: only while no newer build owns the row (terminal write cleared build_run_id).
        await this.store.updateCodeGraphStatus(row.service_id, row.code_graph_id, { summary }, { runId: null });
      }
    }

    // Callback TMC
    const { callbackTMC } = await import("../callback.js");
    await callbackTMC(
      {
        knowledge_id: row.code_graph_id,
        service_id: row.service_id,
        type: "code-graph",
        status,
        summary,
        sync_error: errorMsg?.slice(0, 500) ?? null,
        timestamp: new Date().toISOString(),
      },
      this.callbackConfig,
    );
  }

  /** 等待后台任务完成（测试 / 停机）。 */
  async onIdle(codeGraphId?: string): Promise<void> {
    await this.queue.onIdle(codeGraphId);
  }
}
