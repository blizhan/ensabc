# Implementation Plan: M2 跨批次情景追踪

**Branch**: `main` | **Date**: 2026-10-29 | **Spec**: [spec.md](spec.md)

**Feature directory**: `specs/003-m2-cross-batch-tracking`（`setup-plan.sh` 返回逻辑标识 `003-m2-cross-batch-tracking`，实际 Git 分支保持 `main`）。

**Input**: `specs/003-m2-cross-batch-tracking/spec.md`（含 2026-10-29 五项 Clarifications）

**Status**: Implementation Complete & Verified (Run ID: `m2-review-v2-gate`). 全部 56 项实施任务（T001–T056）均已实现并通过正式门禁验证。21 项功能需求与 8 项成功准则 100% 满足，独立审计 `report-audit.json` 0 违规项。

## Analysis Revisions

I1–I5/U1–U2 已同步到 spec、模型、契约、任务和 quickstart：全体平方项一次精确累加；合法单批算法失败断链而其他错误阻止发布；同产物精确重复/独立重建语义复现分开；pair 与整链身份稳定分开且证书采用保守浮点界；权重缩放检验限定合法数值与决策余量；OM 验证阶段固定 cells→real→replay；绝对 MatchInput 移除可选尺度参数。56 项任务编号及未完成状态不变；文档复核不等于正式验收。

## Summary

在保持 M1 单批次情景计算（`ensabc-core::cluster` 与 DuckDB `ens_cluster`）不变的基础上，交付 M2 跨批次情景追踪闭环：
1. **数据与 DuckOMO 接入**：从 WeatherBench2 恢复同一有效时刻（`2020-01-05 00 UTC`）的连续 8 个 12 小时起报批次（每批 50 扰动成员 × 3×3 欧洲格点，2 米气温 K），由独立样本准备脚本使用 `omfiles 1.2.0` 的 `fpx_xor_2d` 无损编码写成每批一个 OM v3 文件；在 DuckDB 1.5.5 同一连接内加载最新 DuckOMO `v0.3.5-rc.1`（以 `v0.3.4` 为对照）和本项目扩展，验证源解码、独立 OM reader 与 DuckOMO 三路径 3,600 个值逐位一致。
2. **核心匹配算法**：在 `ensabc-core` 中新增单批集合离散 `ensemble_dispersion` 与两批情景匹配 `match_scenarios` / `match_scenarios_scaled`。距离采用物理均值场上的纬度余弦加权 RMSE（Shewchuk 精确浮点求和）；门槛由比例系数乘以两批集合离散算术平均换算；在门槛允许的可行边集上通过后批位掩码子集 DP 求解“先最多关联条数、再最小精确距离和”的一对一部分匹配，提供完全并列计数、单边可替代程度三态（`tied` / `same_count` / `count_drop`）和基于场内容的规范决胜。
3. **SQL 扩展、持续身份与历史表**：在 `ensabc-duckdb` 中新增无状态标量函数 `ens_dispersion` 与 `ens_match_scenarios`；通过两阶段 SQL（`TEMP` 表逐批故障隔离暂存 + 递归 CTE 确定性 `branch_id` 分配 + 单事务五张正式历史表原子发布与 4 条不动点守卫）保存回放版本、批次、情景节点、相邻比较与关联。
4. **验证与门禁**：建立 `validation/m2/`，覆盖 Python `Fraction` 独立穷举 oracle 对照、真实 8 批案例回放、比例系数（`0.5..2.0`）与情景数（`k=2,3,4`）敏感性、匹配级与成员级扰动稳定性证书、断网无参考工具最小容器回放、五环节资源测量、M1 原有门禁回归和全新 clone 独立复现。

详细研究结论见 [research.md](research.md)。

## Technical Context

**Language/Version**: Rust 1.96.0（edition 2024）；Python 3.13.13（仅用于样本准备、独立穷举 oracle 与验证编排，不进入 core 或扩展的构建与运行依赖）。

**Primary Dependencies**:
- **Rust 生产依赖**：不新增任何第三方生产 crate。继续使用 `nalgebra =0.35.0`、`rand =0.9.2`、`rand_chacha =0.9.0`（均 `default-features = false, features = ["std"]`）与 `duckdb` / `libduckdb-sys =1.10505.0`（`loadable-extension`）。匹配算法（位掩码 DP）与 Shewchuk 展开精确求和在 `ensabc-core` 内自研。
- **Rust 开发依赖**：以当前 Cargo.lock 的 `serde 1.0.229`、`serde_json 1.0.154` 为基线，`serde_json` 启用 `features = ["std", "float_roundtrip"]`，消除 17 位十进制浮点解析的 1 ulp 偏差。
- **外部 DuckDB 扩展（二进制加载）**：DuckOMO `v0.3.5-rc.1`（目标，SHA-256 `586bc49d…`）与 `v0.3.4`（对照，SHA-256 `554e36f6…`）的 `duckdb-v1.5.5-linux_arm64` 官方发布包。
- **Python 样本准备与验证环境**：复用 M0 数据锁文件（`numpy==2.5.4`、`numcodecs==0.17.0`），新增独立锁文件 `validation/m2/requirements-om.lock`（含 `omfiles==1.2.0`，`GPL-2.0-only`，仅在样本准备与独立读取校验子进程中使用，不链接、不随仓库分发）。

**Storage**: 本地冻结 OM v3 文件（每批一个，受限派生数据不入库，放在运行产物目录或外部缓存）；DuckDB 1.5.5 数据库文件中的五张历史表（`tracking_runs`、`replay_batches`、`scenario_nodes`、`pair_comparisons`、`scenario_links`）；门禁证据写 `artifacts/m2/<run-id>/` 的 JSON 与 SHA-256 引用。

**Testing**: `cargo test --locked -p ensabc-core` 单元测试；Python `Fraction` 穷举 oracle 对照（固定边界 + 4,000 随机用例 + 2,000 次置换不变性）；三路径 3,600 值逐位比较；双版本 DuckOMO 对照；SQL 事务回滚、幂等与冲突测试；真实 8 批敏感性与扰动证书检查；断网最小容器验证；五环节资源测量；M1 完整门禁回归；全新 clone 独立复现；21 FR / 8 SC 可执行追溯与无循环审计。

**Target Platform**: Linux aarch64 / Ubuntu 24.04、DuckDB CLI `v1.5.5`（`d8cdaa33fd`）、`C_STRUCT_UNSTABLE` / `linux_arm64`。本阶段仅验收本地冻结 OM 文件读取，不覆盖远程 HTTP/S3 读取、DuckDB 2.0、其他平台、WASM 或扩展签名。

**Project Type**: 独立 Rust 核心计算库 + DuckDB C API 可加载扩展 + DuckDB SQL 历史编排 + Python 验证工具链。

**Performance Goals**: 对真实 8 批回放的 5 个环节（OM 转换、OM 读取、单批聚类+离散、两批匹配、身份分配与历史保存）各执行至少 3 次测量，分别记录冷启动累积前缀进程的墙钟时间/峰值 RSS 与已加载语句耗时，明确标记不可分离的 OS 峰值内存边界；不设未经实验的硬性吞吐上限。

**Constraints**:
- 核心匹配与离散计算纯 Rust、无 I/O、无数据库或网络访问，单侧情景数支持 `1..=12`；
- 距离门槛必须先过滤可行边（`d <= τ`）再做字典序全局最优，严禁先全指派再删超距边；
- 加载 DuckOMO 时必须使用空 `HOME` 与隔离 `extension_directory` 并关闭 autoload/autoinstall，防止本机 `~/.duckdb/extensions/` 中的非发布构建混入；
- 回放版本不可变，发布在单事务中完成并经 4 条守卫检查，失败零残留；
- 派生气象数值与 OM 文件不提交入库；报告严禁将未关联表述为物理新生/消亡，严禁将成员占比称为校准概率。

**Scale/Scope**: 单模型（IFS ENS）、单变量（2m 气温 K）、单固定区域（3×3 欧洲格点，$p=9$）、单一共同有效时刻（`2020-01-05 00 UTC`）、连续 8 个 12h 起报批次（每批 $N=50$ 成员），主配置 $d=2, k=3, c=1.0$，敏感性覆盖 $k \in \{2,3,4\}$ 与 $c \in \{0.5, 0.75, 1.0, 1.25, 1.5, 2.0\}$。

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` 仍为未填写的占位模板。沿用 M0/M1 规则，不从模板注释推导额外条款，以用户已确认约束及 M2 `spec.md` 作为门禁标准：

| Gate | Phase 0 前检查 | Phase 1 设计后复核 |
| --- | --- | --- |
| 原生 Rust core 独立于 DuckDB/Python/网络 | 必须遵守 | `ensemble_dispersion` 与 `match_scenarios` 为纯 Rust 无 I/O 函数；`m2_runner` 脱离 DuckDB 独立可跑 |
| 不新增 GPL 依赖进入 MIT Rust workspace | 必须遵守 | `omfiles`（GPL-2.0-only）隔离在 `validation/m2/requirements-om.lock` 独立 Python 样本准备进程；Rust 零新增生产依赖 |
| 真实数据转 OM 无损且防本地扩展冒充 | 必须验证 | 采用 Float32 `fpx_xor_2d`，三路径 3,600 值逐位验证；SQL 强制隔离 `extension_directory` 并核对发布包 SHA-256 |
| 门槛参与全局优化、决胜不依赖编号、歧义显式标记 | 必须遵守 | 位掩码 DP + Shewchuk 精确和 + 场内容规范键决胜 + 三态可替代程度，已在原型上与穷举 oracle 4,008 用例零差异核对 |
| 最小无 Python 断网环境可完成整条回放（FR-018） | 必须遵守 | 身份分配与五表保存用纯 DuckDB SQL（递归 CTE + 事务）实现，最小容器仅需 DuckDB CLI + DuckOMO + 本项目扩展 |
| M1 已验收能力零退化、派生数据不入库 | 必须遵守 | M1 接口保持不变，门禁包含 `run_m1_regression.py`；OM 与载荷仅写 `artifacts/m2/` |

## Project Structure

### Documentation (this feature)

```text
specs/003-m2-cross-batch-tracking/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── tasks.md                     # T001–T056，按 US1–US5 组织，全部已完成
├── checklists/
│   └── requirements.md
└── contracts/
    ├── core.md
    ├── numerics.md
    ├── om-duckomo.md
    ├── sql-history.md
    └── validation-evidence.md
```

### Source Code (repository root)

```text
Cargo.toml / Cargo.lock
crates/ensabc-core/
  Cargo.toml                       # dev-dependencies serde_json 开启 float_roundtrip
  src/
    lib.rs                         # 导出 M1 既有 API + M2 dispersion/matching API 与错误码
    preprocess.rs                  # M1 保持不变
    kmeans.rs                      # M1 保持不变
    summary.rs                     # M1 保持不变
    fsum.rs                        # 新增：Shewchuk 精确浮点展开求和与比较
    dispersion.rs                  # 新增：单批集合加权 RMS 离散
    matching.rs                    # 新增：加权 RMSE、位掩码 DP 匹配、规范决胜与可替代程度
  examples/
    m1_runner.rs                   # M1 保持不变
    m2_runner.rs                   # 新增：独立 core 匹配/离散/整链 CLI
  tests/
    m2_matching_contract.rs        # 新增：core 匹配、并列、置换、缩放与错误码测试
crates/ensabc-duckdb/
  src/
    lib.rs                         # 注册 ens_m0_echo, ens_cluster, ens_dispersion, ens_match_scenarios
    # M1 当前实现在 lib.rs；不假设已有 cluster.rs/output.rs，不为 M2 全面拆分
    match_ext.rs                   # 新增：ens_dispersion 与 ens_match_scenarios C API 回调与解码
validation/m2/
  setup_env.sh
  build_extension.sh
  requirements-om.lock
  common.py
  prepare_om_sample.py
  verify_om_paths.py
  oracle.py
  verify_matching_core.py
  verify_sql_and_history.py
  run_case_and_sensitivity.py
  verify_minimal_runtime.sh
  measure_pipeline.py
  run_m1_regression.py
  reproduce_independently.py
  traceability.py
  verify_report.py
  run_m2_gate.py
  fixtures/
    wb2_8run_source.json
    spatial_weights_eu3x3.json
    tracking_experiment_config.json
    synthetic_match_cases.json
  sql/
    schema.sql
    stage_and_publish.sql
    real_8run_replay.sql
```

**Structure Decision**: 延续 M1 的双 crate 结构（`crates/ensabc-core` + `crates/ensabc-duckdb`），在其中增量添加 M2 模块与 SQL 标量入口，不拆分新 crate、不改动扩展二进制名称（仍为 `ensabc_m0_extension.duckdb_extension`）。验证脚本集中在独立的 `validation/m2/` 目录，复用 `validation/m0/` 与 `validation/m1/` 的锁定环境，不改动 M0/M1 原有脚本与判定标准。

## Implementation Order

1. **冻结配置与样本准备基础**：提交 `validation/m2/fixtures/`（8 批 WB2 源清单、9 点余弦权重、冻结实验参数、合成匹配用例）与 `requirements-om.lock`、`setup_env.sh`、`prepare_om_sample.py`。
2. **Rust Core 精确求和、离散与匹配**：在 `ensabc-core` 实现 `fsum.rs`、`dispersion.rs`、`matching.rs` 与 `examples/m2_runner.rs`（开启 `serde_json/float_roundtrip`），通过 `cargo test` 及 `verify_matching_core.py`（独立 `Fraction` 穷举 oracle 对照）。
3. **DuckDB SQL 扩展适配**：在 `ensabc-duckdb` 实现 `ens_dispersion` 与 `ens_match_scenarios`，验证 C API 嵌套类型、特殊 `NULL` 处理、跨向量批量边界与同连接错误恢复。
4. **DuckOMO 双版本接入与 SQL 历史表回放**：编写 `sql/schema.sql`、`sql/stage_and_publish.sql`、`verify_om_paths.py` 与 `verify_sql_and_history.py`，按 cells→real→replay 顺序完成三路径 3,600 值逐位验证、真实回放、双版本完整对照、多批链（含缺批/失败断链）、三进程重跑及事务原子性验证。
5. **真实 8 批案例研究与敏感性/扰动实验**：实现 `run_case_and_sensitivity.py`，产出 `k=3, c=1.0` 主回放报告、6 个比例系数与 `k=2,3,4` 敏感性报告、匹配级与成员级扰动及远离边界证书核验。
6. **最小运行隔离、五环节测量、M1 回归、独立复现与总门禁**：实现 `verify_minimal_runtime.sh`、`measure_pipeline.py`、`run_m1_regression.py`、`reproduce_independently.py`（父/子模式）、`traceability.py`、`verify_report.py` 与 `run_m2_gate.py`。

## Complexity Tracking

无违反项目约束的设计，无需豁免。
