# ensabc

集合预报数据处理与情景分析项目。当前仓库包含 Python 数据下载、索引解析、绘图工具，以及集合预报处理教程。

当前架构由独立的原生 Rust 计算核心（`crates/ensabc-core`）、DuckDB 扩展 SQL 接口（`crates/ensabc-duckdb`）与 DuckOMO 天气数据读取路径组成，后续将按路线图推进 M4 产品化与多变量/多区域扩展。

core 使用原生 Rust 或 Rust 包；CDO、wgrib/wgrib2 等仅作为可选验证工具。DuckOMO 是 OM 数据路径；M0–M3 采用 WeatherBench2 历史集合区域样本验证，core 不绑定文件格式。

> M0、M1、M2 与 M3 已全部完成。当前 Rust 部分包含独立的 `crates/ensabc-core`（纯 Rust 空间场聚类、单批集合离散、跨批次一对一精确主干匹配与多对多情景演化分析核心）以及 `crates/ensabc-duckdb`（DuckDB 扩展，提供 `ens_cluster`、`ens_dispersion`、`ens_match_scenarios`、`ens_evolve_scenarios`、`ens_fsum`、`ens_diagnose_batch` 标量函数与历史 `ens_m0_echo` 兼容接口）。

## 项目路线

见 [项目现状与完整技术路线](docs/ROADMAP.md)。该文档记录截至 2026-11-26 的阶段状态、已选定方向、M0-M4 阶段任务、验收条件和待验证问题，是后续工作的主记录；重启时的代码盘点保留为历史快照。

已完成 [M0 前置信息收集](docs/M0-RESEARCH.md)，形成 [M0 规格](specs/001-m0-baseline-validation/spec.md)、[技术计划](specs/001-m0-baseline-validation/plan.md) 和 [实施任务清单](specs/001-m0-baseline-validation/tasks.md)。规划期研究与 spike 见 [技术决策](specs/001-m0-baseline-validation/research.md)。

**M0 正式验收已于 2026-10-20 执行，总体判定 `passed`**：FR-001 至 FR-014
全部有实测证据。复跑入口见 [quickstart](specs/001-m0-baseline-validation/quickstart.md)，
证据位于 `artifacts/m0/<run-id>/`。

**M1「单批次情景计算」正式验收已于 2026-10-24 执行，总体判定 `passed`**：[规格](specs/002-m1-scenario-computation/spec.md)、[技术计划](specs/002-m1-scenario-computation/plan.md)、[研究决策](specs/002-m1-scenario-computation/research.md)、[数据模型](specs/002-m1-scenario-computation/data-model.md) 和 [复跑指南](specs/002-m1-scenario-computation/quickstart.md)。41 项实施任务（T001–T041）全部完成。完整门禁证据位于 `artifacts/m1/m1-gate-final/`，包含独立纯 Rust 核心、DuckDB `ens_cluster` 扩展、六阶段双路径数值对齐（Q 几何对齐 + 共享迭代轨迹一致性）、最小运行容器执行、任务性能测量与独立复现，17 项 FR 与 8 项 SC 覆盖率 100%。

**M2「跨批次情景追踪与 DuckOMO 集成」正式验收已于 2026-10-30 执行，总体判定 `passed`**：[规格](specs/003-m2-cross-batch-tracking/spec.md)、[技术计划](specs/003-m2-cross-batch-tracking/plan.md)、[研究决策](specs/003-m2-cross-batch-tracking/research.md)、[数据模型](specs/003-m2-cross-batch-tracking/data-model.md) 和 [复跑指南](specs/003-m2-cross-batch-tracking/quickstart.md)。56 项实施任务（T001–T056）全部完成。完整门禁证据位于 `artifacts/m2/m2-review-v2-gate/`，包含独立纯 Rust 集合离散与位掩码子集 DP 字典序匹配核心、DuckDB `ens_dispersion` 与 `ens_match_scenarios` 扩展、五表历史 DDL 与两阶段事务发布、真实 WeatherBench2 8 批 OM 样本三路径逐位 0 差异验证（Python omfiles vs DuckOMO v0.3.5-rc.1 vs v0.3.4）、敏感性与扰动证书分析、最小容器运行、流水线耗时/内存测量与独立复现，21 项 FR 与 8 项 SC 覆盖率 100%。

**M3「情景演化分析」正式验收与代码审查加固已于 2026-11-26 执行，总体判定 `passed`**：[规格](specs/004-m3-scenario-evolution/spec.md)、[技术计划](specs/004-m3-scenario-evolution/plan.md)、[研究决策](specs/004-m3-scenario-evolution/research.md)、[数据模型](specs/004-m3-scenario-evolution/data-model.md) 和 [复跑指南](specs/004-m3-scenario-evolution/quickstart.md)。49 项实施任务（T001–T049）与全部审查加固项均已完成。最新完整门禁证据位于 `artifacts/m3/m3-review-fixed-gate/`，包含基于 Berge 增广路四定理的“主干一对一 + `nearest_feasible_v1` 次级分裂/合并边”星形演化核心（`evolve_scenarios*`、`evolve_chain`、`m3_runner`）、形态变化/成员占比变化/匹配不确定性三族正交指标与 `m3.uncertainty.v1` 高不确定性标记、DuckDB `ens_evolve_scenarios` / `ens_fsum` / `ens_diagnose_batch` 扩展及 11 表（5 M2 + 6 M3）单事务 `E1`–`E8` 守卫发布、WeatherBench2 8 批主案例回放（`k=3, c=1.0`：24 节点、8 分支、16 主干边 + 2 次级分裂 + 1 次级合并 + 3 新生 + 4 消失）与两层扰动四条件稳定性证书验证（获证跃迁 0 翻转）、18 组门槛扫描与 4 轴聚类配置敏感性、零外链离线单文件可交互演化图 `.html` 及自动溯源核验（18/18 篡改反例拦截）、`--network none` 断网最小容器、七环节资源测量、M1 + M2 串行完整门禁回归（0 退化）、全新 clone 独立复现与无循环独立审计（`18/18` FR 与 `8/8` SC 覆盖率 100%，210 项证据引用核验通过，`findings == []`）。所有分裂/合并流量仅属算法归因，不代表集合成员跨起报发生物理迁移或天气系统物理生成消亡，`member_fraction` 未经概率校准且不构成预报技巧或交易价值依据。

## 当前代码

| 路径 | 内容 |
| --- | --- |
| `ensabc/fetch/` | HTTP/S3 下载与 GRIB 分段合并 |
| `ensabc/parse/` | ECMWF、GFS 索引解析与连续偏移分组 |
| `ensabc/util/plot.py` | 集合成员、聚类结果的邮票图 |
| `crates/ensabc-core/` | M1/M2/M3 独立纯 Rust 计算核心：标准化、PCA 降维、K-means、单批集合离散计算（Shewchuk 浮点累加）、两批情景位掩码 DP 字典序主干匹配，以及多对多星形演化拓扑、五类事件分类、三族正交指标与多批整链演化（`evolution.rs`） |
| `crates/ensabc-duckdb/` | DuckDB 扩展：`ens_cluster`、`ens_dispersion`、`ens_match_scenarios`、`ens_evolve_scenarios`、`ens_fsum`、`ens_diagnose_batch` 标量函数与历史 `ens_m0_echo` 兼容接口 |
| `validation/m0/` | M0 验证脚本、冻结样本清单、参考配置与门禁编排 |
| `validation/m1/` | M1 验证脚本、六阶段双路径对比、SQL 扩展测试、最小运行环境、性能测量与独立复现门禁编排 |
| `validation/m2/` | M2 验证脚本、OM 样本准备、纯 Rust 匹配 oracle 验证、五表 SQL 历史发布、DuckOMO 双版本回放、案例敏感性/扰动证书、最小隔离容器、性能评测与全流程门禁编排 |
| `validation/m3/` | M3 验证脚本、Python `Fraction` 独立演化与四条件证书 oracle、11 表 SQL 原子发布、真实 8 批两层扰动与多维敏感性、离线单文件可交互演化图生成与六重溯源核验、最小隔离容器、七环节性能评测、M1+M2 串行回归、全新 clone 独立复现与全流程门禁审计 |
| `tutorial/ensemble_fetch_convert.ipynb` | 集合数据获取与 NetCDF 转换 |
| `tutorial/ensemble_to_deterministic_product.ipynb` | 控制预报、均值、中位数、极值等示例 |
| `tutorial/ensemble_to_probability_product.ipynb` | 标准化、PCA、K-means 与情景地图 |

教程包含历史日期、本地数据路径及外部工具依赖，尚未验证全部原 notebook 能在新环境直接运行。M0、M1、M2 与 M3 已在此基础上完成了原生情景计算、历史查询扩展、跨批次追踪与多对多情景演化分析闭环。

M3 的验收命令（内部串行包含 M1 与 M2 完整门禁回归）：

```bash
bash validation/m3/setup_env.sh --check                                                # 核对锁定的环境、双版本 DuckOMO 与依赖工具链
validation/m2/.venv/bin/python validation/m3/run_m3_gate.py --run-id m3-acceptance    # 完整门禁，写出 artifacts/m3/m3-acceptance/
validation/m2/.venv/bin/python validation/m3/traceability.py --run-dir artifacts/m3/m3-acceptance --check
validation/m2/.venv/bin/python validation/m3/verify_report.py --run-dir artifacts/m3/m3-acceptance
```
