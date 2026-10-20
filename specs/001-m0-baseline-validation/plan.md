# Implementation Plan: M0 可复现基线与接口验证

**Branch**: `main` | **Date**: 2026-10-20 | **Spec**: [spec.md](spec.md)

**Feature directory**: `specs/001-m0-baseline-validation`。setup-plan 返回的 BRANCH 是特性标识，实际 Git 分支仍为 `main`。

**Input**: `specs/001-m0-baseline-validation/spec.md`

## Summary

M0 冻结一份真实区域集合输入，形成三次独立进程的参考结果，验证 Rust DuckDB 扩展的数据往返，并生成逐项可复核证据。正式原生聚类属于 M1。研究和 spike 为技术选择提供证据，不能直接推出 M0 整体通过。

三个 Luna max worker 分别负责真实数据提取、ARM64 扩展、参考重复性。研究结论及实际执行边界见 [research.md](research.md)；操作入口见 [quickstart.md](quickstart.md)。

## Technical Context

**Language/Version**: Rust 1.96.0 / edition 2024；Python 3.13.13 仅用于数据准备和参考验证。正式构建锁定 Rust 工具链，精确 Python 依赖由 spike 的版本文件转为独立验证环境锁文件。

**Primary Dependencies**: DuckDB CLI 1.5.5 (`d8cdaa33fd`)；Rust `duckdb` / `libduckdb-sys` 锁定 `=1.10505.0`，不启用 bundled DuckDB。参考使用 NumPy、SciPy、scikit-learn；具体实测版本及数据读取依赖见 research 和各 spike 锁文件。M0 不引入生产数值库；M1 候选在 research 中记录。

**Storage**: 小型 JSON 样本、清单、配置和结果文件；内容以 SHA-256 绑定。不新增数据库服务。既有来源读取器仅属于准备环境，核心不绑定 OM、GRIB、Zarr 或 Icechunk。

**Testing**: 固定样本资格与离线恢复断言、三进程参考对照、比较器正反例、实际宿主 SQL 往返与错误恢复、跨批量及双连接并发、1/10/100 任务资源测量、无参考工具的最小环境执行。

**Target Platform**: 本机 Linux aarch64，固定 DuckDB 1.5.5。其他宿主版本、x86_64、macOS、Windows、WASM 和扩展签名发布未覆盖。

**Project Type**: 库项目的验证里程碑；一个最小 Rust 扩展与独立验证脚本，无服务端、前端或常驻调度器。

**Performance Goals**: 记录 1/10/100 个任务墙钟时间与峰值 RSS，验证完整结果及串并行一致性；不设未经测量的吞吐量承诺。

**Constraints**: 冻结验证气象样本总量 <=100 MiB；真实区域和完整产品成员不可被合成/均值/单点替代；原生 Rust 核心不得依赖 Python/CDO/wgrib；不关闭兼容性检查；不读取个人凭据。

**Scale/Scope**: 一个模型、一个起报、一个有效时刻、近地面气温区域场。参考 StandardScaler -> PCA(2) -> KMeans(3)，扩展只 echo，不执行正式聚类或概率校准。

**Frozen candidate**: WeatherBench2 IFS ENS，2020-01-01 00 UTC 起报 +24h，全部 50 个扰动成员、3×3 个点、K 单位。科学数据资格已有提取证据；该再加工归档的具体使用/分发许可尚未落实为来源许可证文件，正式样本门禁仍需该证据。当前文件仅本地研究保存，未提交或发布，不据公开可读性推断再分发授权。

## Constitution Check

前置检查：`.specify/memory/constitution.md` 仍为未填写模板，没有已批准的正式原则；不将模板示例当规则，也不声称获得正式 constitution 合规认证。

设计后复核：本计划遵守用户明确的 Rust 核心、参考工具隔离和 M0/M1 边界。采用已有读取库，不创建通用数据适配平台；扩展只验证身份和数值传递；所有验收结论必须有实际证据。没有相对现有明确要求的架构豁免。

治理缺口：后续批准 constitution 后应重新审查。本次规划无需以填写治理模板为前提；未知实验结果在 evidence 中保持 fail/not_run，不伪装成设计已解决的成功证明。

## Project Structure

### Documentation (this feature)

```text
specs/001-m0-baseline-validation/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── sql.md
│   ├── validation.md
│   └── evidence.md
├── checklists/requirements.md
└── spikes/
    ├── data/
    ├── baseline/
    └── extension/
```

本次保留可复跑 spike 和必要小型证据。`tasks.md` 由下一步 speckit-tasks 生成，不在本次规划中创建。

### Source Code (implementation target)

```text
Cargo.toml                         # 最小 workspace，实施时创建
Cargo.lock
rust-toolchain.toml
crates/ensabc-duckdb/               # M0 echo 与 SQL 边界校验
validation/m0/                     # 参考、数据准备与证据汇总脚本/锁文件
validation/m0/fixtures/            # 合成样本、可分发清单及恢复说明
artifacts/m0/<run-id>/             # 本地验收产物，实施时加入 ignore
ensabc/                            # 保留现有 Python 工具
tutorial/                          # 保留研究教程
```

**Structure Decision**: 先将已证实的最小 spike 整理为一个扩展 crate 和一组验证脚本，不复制成多层服务、通用执行框架或多个适配器。`ensabc-core` 在 M1 真正加入数值算法时建立；M0 不预建空 core crate。未来 core 不依赖 DuckDB，SQL 类型转换与错误映射留在扩展。

## Phase 0: Research and Spikes

三条独立工作线的最终决策记录在 [research.md](research.md)：

1. 来源资格、固定起报/有效时刻、完整成员、区域坐标、冻结/恢复和使用条件。
2. 精确扩展版本、真实加载、嵌套读写与 NULL/错误恢复。
3. 参考环境、显式算法配置、三进程重复性、比较器和 Rust 数值候选。

仅复用通过检查的实验部分。失败实验保留原因和替代路径；未执行的正式验收列入实现范围。来源使用限制与科学样本可用性分开记录，不推断公开下载等于自由再分发。

## Phase 1: Design

### 数据与参考计算

按 [data-model.md](data-model.md) 冻结样本清单、成员优先矩阵、配置及来源映射。真实样本和合成样本分别标识；下载脚本不是离线备份。提取器记录实际源坐标和成员标签，不硬编码控制成员存在。

参考流程显式固定 float64、StandardScaler、PCA full solver、KMeans 三簇及初始化参数。三次运行使用同一冻结输入和环境，但分别启动进程；保存标准化矩阵、投影、标签、中心、惯性和配置哈希。实际参数以研究选定配置为准，不能依赖库默认值漂移。

对照先按成员身份匹配，再处理簇编号置换和投影符号。固定容差、有效秩规则及退化判定，比较器需通过故意破坏分区和数值的反例。算法条件不足则失败，不自动减簇、减维或删除成员。

### SQL 边界

使用 [SQL 契约](contracts/sql.md) 的 `ens_m0_echo(members)`。`task_id` 在调用方保留；嵌套结构绑定成员 ID 与特征。函数不排序、不补值、不访问网络或历史，不把诊断值称为聚类结果。

按层检查 NULL 和列表 offset/length，注册 special NULL handling，保证所有非法输入被明确拒绝。每个错误后同连接执行有效任务；不能用新建连接替代恢复验证。跨批量案例使用随行变化输入避免常量折叠。

spike 的 `VArrowScalar` 已证明加载和合法嵌套往返，但锁定版本的高层注册器未提供 special NULL handling，顶层 NULL 实测被静默传播。正式实现先使用 `duckdb::ffi` C API 注册窄回调并设置该选项，捕获 panic/转为查询错误，再完成嵌套校验；不直接将当前 spike 当作完成的扩展移入产品目录。具体决策和证据见 research。

### 环境、测量与汇总

参考环境与扩展运行环境分离。准备/打包工具可以属于开发流程，但不进入 core 依赖或运行路径。最小环境必须实际加载和测试，单看链接库或清空 PATH 不足以通过。

实施时以 Linux ARM64 Ubuntu 24.04 最小容器作为隔离路径，记录基础镜像 digest，只复制匹配的 DuckDB CLI、扩展、SQL 用例与必要运行库，不安装参考环境。使用 CLI `.bail off` 在同一进程验证错误后的有效调用，并由外部验证驱动核对完整输出和预期错误；容器无网络执行。当前机器有 Docker 命令，但本轮未验证 daemon、镜像或该隔离运行，这一项仍是正式验收工作。

两个独立连接并发核对结果；固定数值输入以逐任务身份区分，测量 1/10/100 任务。记录冷启动/已加载范围、线程数、宿主和峰值 RSS 的测量边界。

按 [验证契约](contracts/validation.md) 执行，并输出 [证据报告](contracts/evidence.md)。FR-001 至 FR-014 全覆盖，任何必要项 fail/not_run 都不能给出整体 passed。

## Implementation Order and Acceptance

这是下一步任务分解的依赖顺序，不是已实现清单：

1. 先落实选定再加工来源的研究使用和验证者恢复条件，再把真实提取结果规范化为冻结样本，补齐 provenance、许可说明、恢复与哈希验证。若无法落实合规恢复，保留当前候选和失败证据，回到来源选择，不把它升级为正式通过样本。
2. 将参考 spike 接入该真实样本，建立三进程输出和比较器正反例。
3. 先修复 C API 注册与顶层 NULL 拒绝，证明同连接错误恢复，再整理 Rust echo 扩展，补齐全部非法输入、批量和并发案例。
4. 建立无参考工具运行环境，完成 1/10/100 任务资源记录。
5. 汇总所有 FR/SC 证据，在独立环境复跑并形成 M1 交接结论。

第 2、3 项在样本契约确定后可并行；第 4 项依赖可加载扩展；第 5 项必须消费实际执行结果。详细追踪见 [需求到证据表](contracts/evidence.md#需求到证据)。

## Complexity Tracking

无需豁免。新增复杂度仅来自规格要求的嵌套 SQL 边界、可恢复样本及可复核证据；不新增通用格式适配层、生产聚类、跨起报追踪或部署平台。
