# Implementation Plan: M3 情景演化分析

**Branch**: `main` | **Date**: 2026-11-25 | **Spec**: [spec.md](spec.md)

**Feature directory**: `specs/004-m3-scenario-evolution`（`setup-plan.sh` 返回逻辑特性标识 `004-m3-scenario-evolution`，实际 Git 分支保持 `main`）。

**Input**: Feature specification from `/specs/004-m3-scenario-evolution/spec.md`（含 2026-11-24 三项已确认 Clarifications）

**Status**: Implementation Complete & Verified (Run ID: `m3-review-fixed-gate`). 全部 49 项实施任务（`T001`–`T049`）及 23 项代码审查加固项均已实现并通过正式门禁与无循环独立审计验证：`18/18` 项功能需求（`FR-001`–`FR-018`）与 `8/8` 项成功标准（`SC-001`–`SC-008`）100% 满足，`report-audit.json` `findings == []`（核验 210 项证据引用），M1（`m1-regress-fdea7f74`）与 M2（`m2-regress-83493031`）串行完整回归 0 退化通过，全新 clone 独立复现 `PASS`；全链路强制携带并核验非物理成员迁移、非物理天气系统生消、未经概率校准与非交易价值解释边界声明。

## Summary

在保持 M1 单批次情景计算（`ensabc-core::cluster`、`ens_cluster`）与 M2 跨批次一对一主干追踪（`ensabc-core::ensemble_dispersion`、`match_scenarios*`、`ens_dispersion`、`ens_match_scenarios` 及五张正式历史表）100% 不退化的前提下，交付 M3 情景演化分析完整闭环：
1. **多对多演化数学内核（`ensabc-core::evolution`）**：基于 Berge 最大基数匹配增广路定理确立四条结构不变量，采用“保留 M2 主干一对一 + 门槛内竞争落选节点单近邻可行归属（`nearest_feasible_v1`）”构建无平行边、零矛盾星形分解的多对多演化图，唯一划分五类基础事件（`continuation`、`split`、`merge`、`emergence`、`disappearance`）与缺口断链状态（`gap_interrupted`、`gap_restarted`）；采用仅依赖星形拓扑与占比的 `star_min_mass_proportional_v1` 计算算法归因流量与显式未归因残差；输出形态变化、成员占比变化与匹配不确定性三族正交指标及冻结 `m3.uncertainty.v1`（统一相对余量 `0.10`）高不确定性标记。
2. **DuckDB 扩展与不可变演化历史表（`ensabc-duckdb` + `validation/m3/sql/`）**：新增无状态 C API 标量函数 `ens_evolve_scenarios`（直接消费并严格复核 M2 `ens_match_scenarios` 返回结构）与 `ens_fsum`（暴露 Shewchuk 精确浮点求和供 SQL 聚合）；在 M2 五张表旁增量新增六张不可变演化历史表（`evolution_runs`、`evolution_nodes`、`evolution_transitions`、`evolution_edges`、`evolution_events`、`evolution_branch_summaries`），直接沿用 M2 `branch_id` 保证逐字符身份一致，通过两阶段 `TEMP` 暂存、单事务原子发布与 `E1`–`E8` 八条全局不变量守卫确保失败零残留与同版本冲突拒绝。
3. **真实 8 批案例、两层扰动与多维敏感性实验**：基于冻结的 WeatherBench2 8 批 2 米气温无损 OM 样本，输出主配置（`k=3, c=1.0`）完整多对多演化案例与局限说明；执行演化输入微扰（均值场 + 纯占比）与逐成员全流程微扰并核验四条件有理区间演化稳定性证书（稳健跃迁 0 翻转）；执行 18 组门槛扫描与 4 轴聚类配置敏感性对照（覆盖 `cluster_count`、`projection_dim`、`seed/restarts` 及自然触发单批算法 `failed` 的断链边界配置）。
4. **零依赖离线单文件可交互溯源可视化与双回归门禁**：仅使用锁定 Python 3.13 标准库从已发布 DuckDB 历史表渲染带严格离线 CSP、置顶非物理迁移免责横幅、内联 SVG 流图、内嵌规范溯源清单与原生 JS 点选检查抽屉的单文件 `.html`，并通过六重自动核验与 10 类恶意篡改反例自检确认 100% 图元可回溯；建立 `validation/m3/` 总门禁，覆盖断网无参考工具最小容器、七环节耗时/内存测量、M1 + M2 串行完整门禁回归、全新 clone 独立复现与无循环审计。

详细调研与设计权衡见 [research.md](research.md)。

## Technical Context

**Language/Version**: Rust 1.96.0（edition 2024）；Python 3.13.13（仅用于独立 `Fraction` 穷举 oracle、验证编排与离线单文件 HTML 生成/审计，不进入 core 或 DuckDB 扩展构建与运行依赖）。

**Primary Dependencies**:
- **Rust 生产依赖**：零新增第三方 crate。继续严格使用已锁定的 `nalgebra =0.35.0`、`rand =0.9.2`、`rand_chacha =0.9.0`（`ensabc-core`）与 `duckdb` / `libduckdb-sys =1.10505.0`（`ensabc-duckdb`, `loadable-extension`）。
- **Rust 开发依赖**：沿用 `Cargo.lock` 中的 `serde 1.0.229` 与 `serde_json 1.0.154`（开启 `features = ["std", "float_roundtrip"]`）。
- **外部 DuckDB 扩展（二进制隔离加载）**：复用已缓存校验的 DuckOMO `v0.3.5-rc.1`（目标）与 `v0.3.4`（对照）`duckdb-v1.5.5-linux_arm64` 官方发布包。
- **Python 验证环境**：零新增 Python 第三方包或锁文件，直接复用 M0/M1/M2 已锁定虚拟环境（`validation/m2/.venv` Python 3.13.13 标准库负责 HTML/SVG 生成与 XML/DOM 审计，`omfiles==1.2.0` 仍严格隔离在样本准备/读取校验子进程）。

**Storage**: 本地冻结 OM v3 文件（受限派生数据不入库）；DuckDB `v1.5.5` 数据库文件中的 11 张正式历史表（M2 原有 5 张 + M3 新增 6 张：`evolution_runs`、`evolution_nodes`、`evolution_transitions`、`evolution_edges`、`evolution_events`、`evolution_branch_summaries`）；离线单文件可交互演化图 `.html` 与门禁证据写入 `artifacts/m3/<run-id>/`（Git 忽略）。

**Testing**: `cargo clippy --locked --all-targets -- -D warnings`（含 `size_of::<CoreError>() <= 112` 守卫）；`cargo test --locked`；Python `Fraction` 独立穷举 oracle 对照（25 个固定对用例 + 4 类多批链 + 21 个固定/动态错误负例覆盖全部 17 类错误码 + 4,000 随机用例 + 2,000 次置换不变性 + 正交分离严格单调/精确零检验）；SQL 11 表事务原子性、`E1`–`E8` 守卫与 `>2048` 行跨向量块测试；WB2 8 批两层三档扰动与四条件稳定性证书核验、18 组门槛扫描与 4 轴聚类配置敏感性；HTML 六重自动溯源核验与篡改反例自检；`--network none` 无 Python 最小容器端到端回放；七环节资源测量；M1 + M2 串行完整门禁回归；全新 clone 独立复现；18 FR / 8 SC 防循环独立审计。

**Target Platform**: Linux aarch64 / Ubuntu 24.04、DuckDB CLI `v1.5.5`（`d8cdaa33fd`）、`C_STRUCT_UNSTABLE` / `linux_arm64`；生成的单文件交互 `.html` 支持任意现代浏览器离线打开（`default-src 'none'`）。

**Project Type**: 独立原生 Rust 核心计算库 + DuckDB C API 可加载扩展 + 纯 DuckDB SQL 不可变历史编排 + 零外链离线可交互可视化生成器 + Python 验证门禁工具链。

**Performance Goals**: 对真实 8 批演化全流程的 7 个环节（OM 读取、单批聚类+离散、M2 主干匹配、M3 演化计算、11 表原子发布、`m3_runner` 独立整链、交互 HTML 渲染与溯源校验）各执行至少 3 次测量，记录冷启动累积前缀子进程 `ru_maxrss` 峰值内存与已加载语句墙钟时间，如实标注 OS 峰值内存不可在单进程内拆分的边界；不设未经实测的吞吐承诺。

**Constraints**:
- 核心演化计算纯 Rust、无 I/O、无数据库/网络/Python 依赖，`CoreError` 大小维持 `<= 112` 字节；
- 100% 保留 M2 一对一主干边与持续身份 `branch_id` 逐字相等，不在 `match_scenarios` 上追加破坏 M2 兼容性的校验；
- 缺批或白名单单批算法失败处严格断链（`gap_interrupted` / `gap_restarted`），0 条跨缺口边，且与普通 `disappearance` / `emergence` 严格互斥；
- 可视化只允许从已发布 DuckDB 历史表渲染，缺溯源字段立即失败，0 外部网络依赖、0 孤立 SVG 图元；
- 派生气象载荷（`.om`、`.bin`、`.duckdb`、运行 `.html`）严禁入库；所有产出强制携带非物理成员迁移、非物理新生消亡、非概率校准与非交易价值声明。

**Scale/Scope**: 单模型（IFS ENS）、单变量（2m 气温 K）、单固定区域（3×3 欧洲格点，$p=9$）、单一共同有效时刻（`2020-01-05 00 UTC`）、4–8 个连续计划起报批次（首案例 8 批 × 50 扰动成员），单侧情景数支持 `1..=12`；主配置 `k=3, projection_dim=2, seed=3, restarts=10, c=1.0`，敏感性覆盖 18 组 `(k, c)` 与 4 轴聚类配置矩阵。

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` 仍为未填写的占位模板。沿用 M0/M1/M2 治理规则，不从占位注释推导虚构条款，以 `docs/ROADMAP.md`、用户已确认约束及 M3 `spec.md` 作为硬性门禁标准：

| Gate | Phase 0 前检查 | Phase 1 设计后复核 |
| --- | --- | --- |
| 原生 Rust core 独立于 DuckDB/Python/网络 | 必须遵守 | `evolve_scenarios*` 与 `evolve_chain` 为纯 Rust 无 I/O 函数；`m3_runner` 完全独立可跑；Rust 零新增生产依赖 |
| 不新增 GPL 依赖进入 MIT 仓库，零新增未锁定包 | 必须遵守 | Rust 与 Python 均零新增第三方依赖；`omfiles` 仍隔离于 M2 既有样本准备子进程；HTML 渲染与 XML 审计纯 Python 标准库实现 |
| M1 与 M2 已验收能力零退化，主干与身份 100% 对齐 | 必须遵守 | M1/M2 公共 API 与五张历史表零修改；`E1`–`E2` 守卫强制双向 `EXCEPT` 为空；门禁串行重跑完整 M1 与 M2 门禁 |
| 多对多规则无矛盾、三族指标正交、决胜不依赖输入顺序 | 必须遵守 | Berge 增广路四定理严格证明星形五分类互斥无重边；`attributed_flow` 不依赖连续距离，保证形态与占比变化严格正交 |
| 断网无 Python 最小容器可完成整条演化回放与持久化（FR-017） | 必须遵守 | 相邻演化（`ens_evolve_scenarios`）、精确求和（`ens_fsum`）、六表汇总与 `E1`–`E8` 原子发布全部由纯 DuckDB SQL + 原生扩展完成 |
| 可视化 100% 可回溯至正式历史表且离线自包含（FR-014） | 必须遵守 | 严格离线 CSP + 置顶免责横幅 + 带键内联 SVG + 规范溯源 JSON + 六重自动核验与 10 类恶意篡改自检 |
| 派生气象数据不入库，严守非物理迁移与非概率校准解释边界 | 必须遵守 | `.gitignore` 忽略 `/artifacts/m3/` 与 `/validation/m3/data/`；代码、表、报告与 HTML 强制内置并机器核验解释边界声明 |

## Project Structure

### Documentation (this feature)

```text
specs/004-m3-scenario-evolution/
├── spec.md                      # M3 特性规格说明（含 3 项已确认 Clarifications）
├── plan.md                      # 本实施计划（$speckit-plan 输出）
├── research.md                  # Phase 0 研究与技术决策 D1–D10
├── data-model.md                # Phase 1 核心数据结构、六张演化表与状态迁移
├── quickstart.md                # Phase 1 复跑与分步验证指南
├── checklists/
│   └── requirements.md          # 规格质量检查清单（16/16 通过）
├── contracts/
│   ├── core.md                  # Rust ensabc-core::evolution 接口与错误预算契约
│   ├── numerics.md              # 多对多星形定理、三族正交指标与四条件稳定性证书契约
│   ├── sql-history.md           # ens_evolve_scenarios / ens_fsum 与六张历史表契约
│   ├── visualization.md         # 离线单文件可交互 HTML 与六重溯源核验契约
│   └── validation-evidence.md   # 验证脚本、产物布局与 18 FR / 8 SC 追溯契约
└── tasks.md                     # Phase 2 任务拆解（由后续 $speckit-tasks 生成）
```

### Source Code (repository root)

```text
.gitignore                         # 增量补充 /artifacts/m3/ 与 /validation/m3/data/
Cargo.toml / Cargo.lock            # 保持生产依赖不变
crates/ensabc-core/
  src/
    lib.rs                         # 保持 M1/M2 导出不变；新增导出 evolution 模块与 M3 错误码变体
    preprocess.rs                  # M1 保持不变
    kmeans.rs                      # M1 保持不变
    summary.rs                     # M1 保持不变
    fsum.rs                        # M2 逻辑保持不变（复用 Shewchuk Expansion；仅 cargo fmt 格式同步）
    dispersion.rs                  # M2 保持不变
    matching.rs                    # M2 公共行为不变（向 crate 内部开放 canonical_scenario_order 等辅助）
    evolution.rs                   # 新增：主干核验、nearest_feasible_v1 次级边、三族正交指标与 evolve_chain
  examples/
    m1_runner.rs                   # M1 保持不变
    m2_runner.rs                   # M2 逻辑保持不变（仅 cargo fmt 格式同步）
    m3_runner.rs                   # 新增：独立 core 相邻对与多批整链演化 CLI
  tests/
    m2_matching_contract.rs        # M2 逻辑保持不变（仅 cargo fmt 格式同步）
    m3_evolution_contract.rs       # 新增：演化定理不变量、正交性、置换不变性、错误码与 CoreError 字节预算测试
crates/ensabc-duckdb/
  src/
    lib.rs                         # 保留既有 4 个函数，新增注册 ens_evolve_scenarios、ens_fsum 与 ens_diagnose_batch
    match_ext.rs                   # M2 保持不变（向 crate 内部共享 MatchResult 逻辑类型构造器）
    evolve_ext.rs                  # 新增：ens_evolve_scenarios、ens_fsum 与 ens_diagnose_batch C API 编解码与异常隔离
validation/m3/
  __init__.py
  README.md
  setup_env.sh
  build_extension.sh
  common.py
  oracle.py
  verify_evolution_core.py
  test_sql_history.py
  verify_sql_and_history.py
  test_stability.py
  run_case_and_sensitivity.py
  render_evolution_html.py
  verify_visualization_traceability.py
  test_evidence_and_viz.py
  verify_minimal_runtime.sh
  minimal_runtime.py
  measure_pipeline.py
  run_m1_regression.py
  run_m2_regression.py
  reproduce_independently.py
  traceability.py
  verify_report.py
  run_m3_gate.py
  fixtures/
    evolution_experiment_config.json
    synthetic_evolution_cases.json
  sql/
    schema.sql
    stage_and_publish.sql
    real_8run_evolution.sql
```

**Structure Decision**: 延续 M1/M2 经验证的双 crate 单扩展架构（`crates/ensabc-core` + `crates/ensabc-duckdb`，扩展二进制名保持 `ensabc_m0_extension.duckdb_extension`），以增量模块方式加入 `evolution.rs` 与 `evolve_ext.rs`，对 M0/M1/M2 原有源码与验证目录（`validation/m0..m2/`）零破坏性改动；M3 全部实验配置、SQL 编排、离线交互可视化与门禁审计集中在独立的 `validation/m3/` 目录。

## Implementation Order

1. **目录卫生与冻结配置基线**：更新 `.gitignore`（忽略 `/artifacts/m3/` 与 `/validation/m3/data/`），建立 `validation/m3/fixtures/`（`evolution_experiment_config.json`、`synthetic_evolution_cases.json`）、`setup_env.sh`、`build_extension.sh` 与 `common.py`。
2. **Rust Core 多对多演化内核与独立 Oracle 对照（US1、US2）**：在 `ensabc-core` 实现 `evolution.rs`、`examples/m3_runner.rs` 与 `tests/m3_evolution_contract.rs`；编写独立 `validation/m3/oracle.py` 与 `verify_evolution_core.py`，通过固定合成集、正交分离测试、4,000 随机用例、2,000 次置换不变性与全部错误负例。
3. **DuckDB C API 扩展与 11 表原子历史发布（US3）**：在 `ensabc-duckdb` 实现 `evolve_ext.rs`（`ens_evolve_scenarios`、`ens_fsum`）；编写 `validation/m3/sql/schema.sql`、`stage_and_publish.sql`、`real_8run_evolution.sql` 与 `verify_sql_and_history.py`，完成 C API 跨向量块、同会话恢复、`E1`–`E8` 守卫、缺口断链、三进程重跑、幂等/冲突与故障回滚验证。
4. **真实 8 批案例、两层扰动与门槛/聚类敏感性分析（US4）**：实现 `run_case_and_sensitivity.py`（含四条件有理区间演化稳定性证书检查），生成主案例演化报告、Level 1a/1b/2 扰动对照、18 组门槛扫描、4 轴聚类配置敏感性及 `limitations.json`。
5. **离线单文件可交互演化图与六重自动溯源核验（US5）**：实现 `render_evolution_html.py` 与 `verify_visualization_traceability.py`（含 `--self-check` 10 类篡改反例拦截），产出真实 8 批与合成全特性可交互 `.html` 并完成 100% 字段级溯源核对。
6. **最小断网容器、七环节测量、M1+M2 串行双回归、独立复现与总门禁（US5）**：实现 `verify_minimal_runtime.sh`、`measure_pipeline.py`、`run_m1_regression.py`、`run_m2_regression.py`、`reproduce_independently.py`、`traceability.py`、`verify_report.py` 与 `run_m3_gate.py`，执行完整门禁与无循环审计。

## Complexity Tracking

无违反项目约束或架构门禁的设计，无需豁免。
