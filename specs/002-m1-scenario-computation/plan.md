# Implementation Plan: M1 单批次情景计算

**Branch**: `main` | **Date**: 2026-10-24 | **Spec**: [spec.md](spec.md)

**Feature directory**: `specs/002-m1-scenario-computation`。setup-plan 返回的 BRANCH=`002-m1-scenario-computation` 是活动特性标识，实际 Git 分支保持 `main`。

**Input**: `specs/002-m1-scenario-computation/spec.md`

**Status**: Phase 0 研究、Phase 1 设计与 [41 项实施任务](tasks.md) 已完成；已修订分析问题 I1/U1/I2/I3/I4，明确分阶段调度、参考轨迹观察、初始化来源字段及 T034 独立复现父/子流程所有权。待只读复核后进入实施；原生 core、聚类 SQL 和正式 M1 验收尚未实现。研究/规划实验不等于 M1 通过。

## Summary

新增独立 Rust `ensabc-core`，在固定成员矩阵上执行标准化、PCA 与 Lloyd 聚类，输出成员归属、物理平均场、真实代表成员、成员占比和可核对诊断。在既有扩展新增 `ens_cluster` 类型化 SQL 入口，保留 `ens_m0_echo`。首个真实路径复用 M0 50×9 区域气温样本，数值和查询集成分别验证，生产 core 不依赖参考工具。

数值依赖选 `nalgebra` 的纯 Rust 有限迭代 SVD，局部实现标准化与窄 Lloyd 流程。原生固定种子的 k-means++ 使用明确 ChaCha8 随机规则；原 M0 基线符合性和共享中心算法诊断分别验收。新增 `validation/m1/` 验证与报告，直接复用合适的 M0 数据/参考 CLI，不更改其判定规则。具体依据见 [research.md](research.md)。

## Technical Context

**Language/Version**: Rust 1.96.0、edition 2024；Python 3.13.13 用于数据准备、独立参考及验证编排，不能进入 core 构建/运行依赖。

**Primary Dependencies**: `nalgebra =0.35.0`（default-features=false, std）；`rand =0.9.2`（default-features=false, std）、`rand_chacha =0.9.0`（default-features=false, std），固定 ChaCha8；DuckDB Rust `duckdb` / `libduckdb-sys =1.10505.0` 继续 loadable-extension、不启用 bundled。示例 runner 的 serde/serde_json 为开发依赖，以实际 Cargo.lock 固定。参考环境复用 NumPy 2.5.4、SciPy 1.18.1、scikit-learn 1.9.1。本轮只核对新增包元数据及 SVD 源码，完整 tree/编译/链接检查是实施门禁。

**Storage**: 输入沿用 JSON 清单及恢复载荷；调用方用 DuckDB 表保存任务上下文与嵌套结果。验收写 `artifacts/m1/<run-id>/` 的 JSON/日志与 SHA-256 引用。无常驻服务或额外数据库。真实派生载荷和结果不入库。

**Testing**: core 的数值/错误与参数边界测试；Python 原基线及共享中心对照；几何对齐比较器正反例；SQL 实际加载、类型、NULL/错误恢复、跨 task/member 边界、两连接真实重叠；最小隔离 core/SQL 执行；1/10/100 测量；有效源码在干净新 clone 复现；17 FR/8 SC 可执行追溯和独立审计。

**Target Platform**: Linux aarch64 / Ubuntu 24.04、DuckDB CLI 1.5.5（d8cdaa33fd）、C_STRUCT_UNSTABLE / linux_arm64。初版未覆盖其他宿主、x86_64/macOS/Windows/WASM 或签名发布。

**Project Type**: 独立计算库 + 既有 DuckDB 扩展 + 验证工具，无前端、服务或通用调度器。

**Performance Goals**: 固定真实输入的 1/10/100 个任务，至少 3 次测量/规模，记录冷启动及已加载时间、独立进程峰值 RSS 和测量边界；完整消费/核对结果。不设未经实验的吞吐或内存上限。完整诊断输出成本包含在测量中。

**Constraints**: 原生 Rust/Rust 包实现 core，不依赖 Python/CDO/wgrib/宿主数据库；无网络与历史副作用；每任务局部随机/拟合状态；非法输入、空簇、退化及未收敛明确失败；固定比较容差 `atol=1e-8, rtol=1e-6`，成员分区精确等价；不覆盖 M0 输入/配置/证据；不推断样本再分发许可。

**Scale/Scope**: 单模型/起报/有效时刻/变量/区域。核心参数支持 d=1..有效秩、k=2..可区分行数；首个验收配置 d=2、k=3，真实输入 n=50、p=9，10 重启。批量边界验证用超过 `duckdb_vector_size()` 的合法合成任务及成员列表。持久化与跨起报身份由调用方管理，M1 不实现追踪/校准。

## Constitution Check

**研究前检查**：`.specify/memory/constitution.md` 仍为未填写模板，没有已批准的治理条款。未把模板示例视为强制 TDD 或额外审批条件；不宣称通过正式 constitution 认证。以既有用户约束及 M1 spec 为 gate。

| Gate | 研究前判断 | Phase 1 后复核 |
| --- | --- | --- |
| 原生 core、无 DuckDB/参考运行依赖 | 设计必须遵守 | core API 及依赖明确隔离，符合设计约束 |
| SQL 类型/NULL/FFI 错误边界 | 复用 M0 路径研究 | special handling + 逐层解码 + owned 结果写回；实际验证留实施 |
| 原基线不覆盖、共享初始化不替代原基线 | 强制保留 | 两条独立比较及引用哈希明确 |
| 真实输入、数值错误及 17/8 追溯 | 不能用合成/echo 替代 | validation/evidence 定义完整，尚无执行通过声明 |
| 范围与数据许可 | 只沿用已验证来源、有限平台 | 不新增服务/跨批次，不提交真实派生数据 |

**本轮设计后复核**：数值 stage 不存在 shared→native 的反向依赖；参考只在隔离子进程观察真实内核并核验无扰动，未修改已安装参考源码；初始化来源的公开类型与 SQL 保持一致；T034→T037 只存在正向调用依赖，复现子集不生成整体通过报告。没有需豁免的已知约束冲突。新增纯 Rust数值包与 C API 结果写回的可运行性仍属实施门禁；文档设计通过不等于代码通过。正式 constitution 后续批准时须重新复核。

## Project Structure

### Documentation (this feature)

```text
specs/002-m1-scenario-computation/
  spec.md
  plan.md
  research.md
  data-model.md
  quickstart.md
  checklists/requirements.md
  tasks.md
  contracts/
    core.md
    numerics.md
    sql.md
    validation.md
    evidence.md
```

[tasks.md](tasks.md) 已生成 T001–T041，按四个用户故事组织；本轮只同步既有任务的契约参数和描述，不新增或执行实施任务。

### Source Code (repository root, implementation targets)

```text
Cargo.toml / Cargo.lock / rust-toolchain.toml
crates/ensabc-core/
  Cargo.toml
  src/lib.rs
  src/preprocess.rs
  src/kmeans.rs
  src/summary.rs
  examples/m1_runner.rs
  tests/                       # 公开 core 结果/错误契约
crates/ensabc-duckdb/
  src/lib.rs                   # 既有入口与 echo
  src/cluster.rs               # ens_cluster 注册/回调/解码
  src/output.rs                # 窄嵌套结果写入
validation/m1/
  setup_env.sh
  prepare_input.py
  run_reference.py
  verify_core.py
  compare_results.py
  build_extension.sh
  verify_extension.py
  verify_minimal_runtime.sh
  measure_tasks.py
  reproduce_independently.py
  common.py / schema.py / traceability.py / verify_report.py
  run_m1_gate.py
  fixtures/native_config.json
  fixtures/comparison_rules.json
  fixtures/synthetic_tasks.json
  examples/real_sample.sql
validation/m0/                 # 复用来源/参考 CLI，保留既有规则
artifacts/m1/<run-id>/          # 实施时加入 ignore，受限制真实派生证据不提交
```

**Structure Decision**: 新建一个实际计算 crate 并在现有扩展加 adapter；不预建空 crate、算法 trait 注册框架或通用验证平台。core 的语义校验单一维护，adapter 只处理 SQL 表示与定位。M1 验证复用 M0 来源/参考入口及锁文件，不把 M0 schema 的 14/6 常数泛化成框架。延后通用化，避免修改旧门禁时重解释旧证据。

## Phase 0: Research and Decision Resolution

初始未知项已由三个 Luna6 max 子代理及主代理查证解决：

1. **数值依赖与 SVD**：选择 nalgebra 0.35.0 std-only，MSRV 和有限迭代 API 已查证；标准化及初始化/Lloyd 语义写入数值契约。
2. **随机与基线**：原生单候选 D²/ChaCha8 不冒充 NumPy RNG；原参考最终结果门禁与显式共享中心步骤对照分别报告。Python 小实验确认 `[0,16,33]` 起点收敛但分区不同于原基线，不能替代原检查。
3. **SQL 返回**：直接 C API 的 STRUCT/LIST 分配与 validity 符号存在，返回 typed 嵌套结果；复用 special handling 语义，实际结果写回待实现验证。
4. **验收与复现**：M1 特定证据/断言和无循环审计；独立 PID RSS；新 clone 针对有效源码 snapshot，避免漏掉未提交文件或污染原仓库历史。

本轮分析修订另确认：reference/core/comparator 显式分阶段，未包装参考与透明内核观察的两种停止路径实测最终结果一致；RestartSummary 增加随机/显式模式有区别的 initial_member_indices。具体问题与决定见 [research.md 第 7 节](research.md#7-分析问题的设计修订)。

本计划没有未解决的产品澄清或待选择的技术分支。已识别的实现风险在 research 的门禁表保留；不能把未运行的实验填成 pass。

## Phase 1: Design and Contracts

- [data-model.md](data-model.md)：TaskContext、成员、配置、结果、诊断、错误与身份。
- [core.md](contracts/core.md)：`cluster(&TaskInput)` 公共 Rust seam、错误类别及开发 runner。
- [numerics.md](contracts/numerics.md)：标准化/秩/PCA、随机规则、共享中心、Lloyd、停止与比较。
- [sql.md](contracts/sql.md)：`ens_cluster` 精确类型、配置、返回矩阵、特殊 NULL、批量与写回。
- [validation.md](contracts/validation.md)：原基线、合成/错误、串并行、最小环境、资源和干净复现。
- [evidence.md](contracts/evidence.md)：CLI、产物、17 FR/8 SC 映射、引用哈希、无循环审计与状态。
- [quickstart.md](quickstart.md)：已存在的参考准备命令及实现后应可执行的验收步骤，明确区分当前可跑与未来入口。

设计后 constitution 与用户约束复核结果见上表。

## Implementation Order and Acceptance

以下为 [tasks.md](tasks.md) 的执行依赖依据，不是已实现清单：

1. 冻结 M1 配置/比较规则/合成 fixture 与 source inventory；复用 M0 输入及参考环境，确认许可/恢复边界，建立版本组合和样本身份记录。
2. 建立 core 与开发 runner，先覆盖标准化、秩、PCA，再加入初始化/Lloyd、编号规范化、物理均值及代表成员；完成 core 错误/参数边界。
3. 数值验证严格按 `run_reference --stage original → verify_core --stage native → compare_results --stage original → run_reference --stage shared → verify_core --stage shared → compare_results --stage shared` 执行。original 比较生成绑定双方投影身份的 Q，shared 参考只读这些前置结果；共享 native 不能反向生成 shared 参考所需输入。参考逐轮轨迹来自隔离真实内核观察，observer-check 须证明包装/未包装最终结果精确一致。先确认两条正确性检查，再接入 SQL；失败必须保留原因，不能修改原基线或临时改容差绕过。
4. 在既有扩展添加 typed SQL 输出，验证实际加载、类型和 M0 echo 回归；补齐完整非法恢复、跨行/成员批量、真实来源到保存结果及两连接重叠。
5. 最小运行隔离、1/10/100 资源记录和有效源码全新 clone 复现。T034 在 reproduce_independently.py 中实现独立父/子模式，子模式使用明确的 --output-dir/--source-manifest/--manifest/--payload/--config，直接编排既有 producer，并外置样本/输出/构建/cache；不依赖后续 gate/追溯/审计，T037 只调用其父模式。子集状态和保留证据必须与完整阶段判定区分；环境不足写 not_run，不能推断通过。
6. FR/SC 可执行 traceability → report → 独立 audit；任何必要 fail/not_run 阻止整体 passed。

core 与 adapter 类型构造的前期工作可在契约冻结后独立推进，但 SQL 数值验证依赖 core。报告/测量工具只在所消费的实际路径存在后验收，避免大量空测试桩。

## Complexity Tracking

没有已批准原则或用户约束的豁免。自有 Lloyd 的维护成本用于控制初始化/停止/空簇语义，SVD 复用成熟纯 Rust 包；typed C API 写回的 unsafe 代码局限在 adapter。不引入第三个生产 crate、JSON SQL 输出、常驻服务或跨批次状态层。
