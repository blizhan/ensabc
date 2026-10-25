# Specification Quality Checklist: M1 单批次情景计算

**Purpose**: 检查 M1 规格的完整性、明确性和可验证性，作为技术规划输入。
**Created**: 2026-10-24
**Feature**: [spec.md](../spec.md)
**Review Ownership**: speckit-specify 规格质量自检。
**Marker Semantics**: `[x]` 表示规格质量已检查，不表示实现完成或运行验收通过。

## Content Quality

- [x] No implementation details (languages, frameworks, APIs) — 仅保留用户已确定的技术约束，不新增实现选择。
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (no implementation details)
- [x] All acceptance scenarios are defined
- [x] Edge cases are identified
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria
- [x] User scenarios cover primary flows
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] No implementation details leak into specification — 已声明的用户技术约束不视为新设计。

## Notes

- 第二轮审阅后，16 项规格质量检查全部通过，未留下待澄清标记。后续已完成 [技术计划](../plan.md)、设计契约与 [41 项实施任务](../tasks.md)，并修订一致性分析发现的 I1/U1/I2/I3/I4；下一步只读复核后进入实施。本清单仍仅评价规格质量，M1 尚未实施或执行验收。
- 第一轮发现“配置非整数”未区分计数参数与容差，以及停止参数有效范围不完整；已在 Edge Cases 与 FR-003 明确计数、容差、秩阈值的边界。FR-002 已明确按解释方差选择主成分及聚类目标，避免把任意线性投影或分组当作迁移实现。
- “成员平均场”“投影空间中的真实代表成员”“成员占比”在 FR-004–FR-005 分别定义；物理场和投影中心不混用，占比不宣称校准概率。
- FR-001–FR-006 对应 Story 1 / SC-001–SC-002；FR-007–FR-008 对应 Story 2 / SC-003；FR-009 对应 Story 2–3 / SC-004；FR-010、FR-013 对应 Story 1–3 / SC-005；FR-011–FR-012 对应 Story 3 / SC-001、SC-004；FR-014 对应 Story 3 / SC-006；FR-015、FR-017 对应 Story 4 / SC-008；FR-016 对应 Story 4 / SC-007。
- 共享初始化用于算法步骤对照，原 M0 随机初始化基线必须保留并独立对照。不能仅凭同一随机种子推定跨实现等价。
- 原生 Rust、独立 core、DuckDB SQL 入口及参考工具隔离是既有用户约束，集中列于 Assumptions；未选择数值依赖、SQL 类型签名、FFI 方式或代码目录。算法语义和数值容差是结果验收规则，不是新增技术栈选择。
- 真实路径默认沿用 M0 已验证样本；DuckOMO 的 OM 接入仍需单独实测。冻结载荷的首次恢复条件、再分发限制、目标平台及其他阶段的范围均已声明。
- Constitution 仍为未填写模板，不将示例视为正式原则；不以本次自检宣称已通过正式治理审查。
- `.specify/extensions.yml` 不存在，before_specify 和 after_specify hooks 均按规则跳过。当前 Git 分支仍为 `main`，活动特性路径已持久化到 `.specify/feature.json`。
