# ensabc

集合预报数据处理与情景分析项目。当前仓库包含 Python 数据下载、索引解析、绘图工具，以及集合预报处理教程。

下一阶段的方向是：使用独立的 Rust core 实现情景计算，通过 DuckDB extension 提供 SQL 接口，由 DuckOMO 提供天气数据。

core 使用原生 Rust 或 Rust 包；CDO、wgrib/wgrib2 等仅作为可选验证工具。DuckOMO 是 OM 数据路径；M0 采用 WeatherBench2 历史集合区域样本验证，core 不绑定文件格式。

> M0 与 M1 已全部完成。当前 Rust 部分包含独立的 `crates/ensabc-core`（纯 Rust 空间场聚类计算核心）以及 `crates/ensabc-duckdb`（DuckDB 扩展，提供 `ens_cluster` 标量函数与历史 `ens_m0_echo` 兼容接口）。

## 项目路线

见 [项目现状与完整技术路线](docs/ROADMAP.md)。该文档记录截至 2026-10-24 的阶段状态、已选定方向、M0-M4 阶段任务、验收条件和待验证问题，是后续工作的主记录；重启时的代码盘点保留为历史快照。

已完成 [M0 前置信息收集](docs/M0-RESEARCH.md)，形成 [M0 规格](specs/001-m0-baseline-validation/spec.md)、[技术计划](specs/001-m0-baseline-validation/plan.md) 和 [实施任务清单](specs/001-m0-baseline-validation/tasks.md)。规划期研究与 spike 见 [技术决策](specs/001-m0-baseline-validation/research.md)。

**M0 正式验收已于 2026-10-20 执行，总体判定 `passed`**：FR-001 至 FR-014
全部有实测证据。复跑入口见 [quickstart](specs/001-m0-baseline-validation/quickstart.md)，
证据位于 `artifacts/m0/<run-id>/`。

**M1「单批次情景计算」正式验收已于 2026-10-24 执行，总体判定 `passed`**：[规格](specs/002-m1-scenario-computation/spec.md)、[技术计划](specs/002-m1-scenario-computation/plan.md)、[研究决策](specs/002-m1-scenario-computation/research.md)、[数据模型](specs/002-m1-scenario-computation/data-model.md) 和 [复跑指南](specs/002-m1-scenario-computation/quickstart.md)。41 项实施任务（T001–T041）全部完成。完整门禁证据位于 `artifacts/m1/m1-gate-final/`，包含独立纯 Rust 核心、DuckDB `ens_cluster` 扩展、六阶段双路径数值对齐（Q 几何对齐 + 共享迭代轨迹一致性）、最小运行容器执行、任务性能测量与独立复现，17 项 FR 与 8 项 SC 覆盖率 100%。

## 当前代码

| 路径 | 内容 |
| --- | --- |
| `ensabc/fetch/` | HTTP/S3 下载与 GRIB 分段合并 |
| `ensabc/parse/` | ECMWF、GFS 索引解析与连续偏移分组 |
| `ensabc/util/plot.py` | 集合成员、聚类结果的邮票图 |
| `crates/ensabc-core/` | M1 独立纯 Rust 计算核心：标准化、PCA 降维、SVD 有效秩判定、K-means、真实代表成员选择及质量诊断 |
| `crates/ensabc-duckdb/` | DuckDB 扩展：`ens_cluster` 标量函数与历史 `ens_m0_echo` 兼容接口 |
| `validation/m0/` | M0 验证脚本、冻结样本清单、参考配置与门禁编排 |
| `validation/m1/` | M1 验证脚本、六阶段双路径对比、SQL 扩展测试、最小运行环境、性能测量与独立复现门禁编排 |
| `tutorial/ensemble_fetch_convert.ipynb` | 集合数据获取与 NetCDF 转换 |
| `tutorial/ensemble_to_deterministic_product.ipynb` | 控制预报、均值、中位数、极值等示例 |
| `tutorial/ensemble_to_probability_product.ipynb` | 标准化、PCA、K-means 与情景地图 |

教程包含历史日期、本地数据路径及外部工具依赖，尚未验证全部原 notebook 能在新环境直接运行。M0 与 M1 已在此基础上完成了原生情景计算与查询扩展。

M1 的验收命令：

```bash
bash validation/m1/setup_env.sh --check        # 核对锁定的环境与依赖工具链
python3 validation/m1/run_m1_gate.py --run-id m1-acceptance  # 完整门禁，写出 artifacts/m1/m1-acceptance/
python3 validation/m1/traceability.py --run-dir artifacts/m1/m1-acceptance
python3 validation/m1/verify_report.py --run-dir artifacts/m1/m1-acceptance
```
