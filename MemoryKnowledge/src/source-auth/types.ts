/**
 * 外部知识源认证 —— 通用类型契约（资源级绑定）。
 *
 * wiki 与 codegraph 共用一套：凭据挂在被访问的资源上（`code_graph_id` / `wiki_id`），
 * 不属人。设计文档：MemoryPanel/docs/design/2026-09-08-external-source-import.md §1.1 / §4.1。
 *
 * 存储在 KS，令牌用 base64 编入 `cred_secret`，使用时内存解码；
 * 明文永不出进程（路由层只回吐元数据）。
 */

/**
 * 凭据形态：
 *   - `bearer`：单一 token（PAT）—— 由平台协议提供占位用户名做 basic-auth 注入
 *   - `basic`：用户名 + 密码（用户自填 username）
 *
 * 新增方式先在 `code-source/auth-methods/` 加实现，再回来加联合成员。
 * 不预留未实现的成员（避免"看起来支持但实际走不通"的误导）。
 */
export type CredentialKind = "bearer" | "basic";

/**
 * 资源引用 —— 凭据的绑定单位。
 * 主键 (service_id, resource_type, resource_id)，与凭据表 PK 同构。
 */
export type ResourceRef =
  | { type: "code-graph"; serviceId: string; resourceId: string }   // resourceId = code_graph_id
  | { type: "wiki"; serviceId: string; resourceId: string };        // resourceId = wiki_id

export type ResourceType = ResourceRef["type"];

export interface SourceCredential {
  kind: CredentialKind;
  /** 长期令牌；basic 时为 password。 */
  secret: string;
  /** basic 认证的用户名（用户自填）；PAT 类由来源固定（见 CLONE_USERNAME）。 */
  username?: string;
  /** 协议特有字段（mcp_url / corpid / header 名）。 */
  extra?: Record<string, unknown>;
}

/** 凭据元数据（永不包含明文）。 */
export interface CredentialStatus {
  resource_type: ResourceType;
  resource_id: string;
  provider_id: string;
  cred_kind: CredentialKind;
  last_verified_at: string | null;
  updated_at: string;
}

/** 明文凭据 + 其 provider_id（同一行一次读出）。 */
export interface CredentialWithProvider {
  credential: SourceCredential;
  provider_id: string;
}

/** 资源级凭据存储。每个数据方法返回 Promise。 */
export interface ICredentialStore {
  /** 取明文凭据（仅 KS 进程内使用）。未配置 / 解码失败 → null（不抛异常）。 */
  get(ref: ResourceRef): Promise<SourceCredential | null>;
  /**
   * 一次读一行：明文凭据 + provider_id。替代 get + status 两次读
   * （两次读之间凭据可能被改）。未配置 / 解码失败 → null。
   */
  getWithProvider(ref: ResourceRef): Promise<CredentialWithProvider | null>;
  /** provider_id 记录协议来源（'gongfeng' / 'iwiki' 等），非键，仅供展示。 */
  put(ref: ResourceRef, cred: SourceCredential, providerId: string, updatedBy?: string): Promise<void>;
  /** 幂等：行不存在返回 false。 */
  delete(ref: ResourceRef): Promise<boolean>;
  /** 仅元数据，不回吐 secret。 */
  status(ref: ResourceRef): Promise<CredentialStatus | null>;
  /**
   * 列出**全服务**下某类资源的全部凭据行（仅 id + service，供后台调度器扫描）。
   * KS 内部方法，不对外暴露 HTTP 端点（跨资源枚举增加越权面）。
   */
  listAllByType(type: ResourceType): Promise<Array<{ serviceId: string; resourceId: string }>>;
  /** 记录一次校验结果（仅更新时间戳）。 */
  recordVerify(ref: ResourceRef): Promise<void>;
}

/** 密文行（落库形态）。 */
export interface CredentialRow {
  service_id: string;
  resource_type: ResourceType;
  resource_id: string;
  provider_id: string;
  cred_kind: CredentialKind;
  cred_secret: string;                // base64
  cred_username: string | null;       // 仅 basic
  cred_extra_json: string | null;
  last_verified_at: string | null;
  created_by: string | null;
  updated_by: string | null;
  created_at: string;
  updated_at: string;
}
