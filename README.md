# ensabc

集合预报数据处理与情景分析项目。当前仓库包含 Python 数据下载、索引解析、绘图工具，以及集合预报处理教程。

下一阶段的方向是：使用独立的 Rust core 实现情景计算，通过 DuckDB extension 提供 SQL 接口，由 DuckOMO 提供天气数据。

core 使用原生 Rust 或 Rust 包；CDO、wgrib/wgrib2 等仅作为可选验证工具。DuckOMO 是 OM 数据路径；M0 采用 WeatherBench2 历史集合区域样本验证，core 不绑定文件格式。

> M0 的 Rust core 尚未实现：当前仓库只有 `crates/ensabc-duckdb` 这个诊断往返
> 扩展，用于验证 SQL 调用边界的安全性与可复现性。正式原生聚类属于 M1。

## 项目路线

见 [项目现状与完整技术路线](docs/ROADMAP.md)。该文档记录截至 2026-10-20 的代码状态、已选定方向、M0-M4 阶段任务、验收条件和待验证问题，是后续工作的主记录。

已完成 [M0 前置信息收集](docs/M0-RESEARCH.md)，形成 [M0 规格](specs/001-m0-baseline-validation/spec.md)、[技术计划](specs/001-m0-baseline-validation/plan.md) 和 [实施任务清单](specs/001-m0-baseline-validation/tasks.md)。规划期研究与 spike 见 [技术决策](specs/001-m0-baseline-validation/research.md)。

**M0 正式验收已于 2026-10-20 执行，总体判定 `passed`**：FR-001 至 FR-014
全部有实测证据。复跑入口见 [quickstart](specs/001-m0-baseline-validation/quickstart.md)，
证据位于 `artifacts/m0/<run-id>/`。已知限制：样本再加工衍生物的再分发许可未落实，
因此派生的数值载荷不提交到仓库，验证者从钉住的源 generation 重建；验收平台只覆盖
Linux aarch64 + DuckDB CLI 1.5.5。

## 当前代码

| 路径 | 内容 |
| --- | --- |
| `ensabc/fetch/` | HTTP/S3 下载与 GRIB 分段合并 |
| `ensabc/parse/` | ECMWF、GFS 索引解析与连续偏移分组 |
| `ensabc/util/plot.py` | 集合成员、聚类结果的邮票图 |
| `crates/ensabc-duckdb/` | M0 诊断扩展：`ens_m0_echo` 嵌套往返与逐层输入校验 |
| `validation/m0/` | M0 验证脚本、冻结样本清单、参考配置与门禁编排 |
| `tutorial/ensemble_fetch_convert.ipynb` | 集合数据获取与 NetCDF 转换 |
| `tutorial/ensemble_to_deterministic_product.ipynb` | 控制预报、均值、中位数、极值等示例 |
| `tutorial/ensemble_to_probability_product.ipynb` | 标准化、PCA、K-means 与情景地图 |

教程包含历史日期、本地数据路径及外部工具依赖。目前尚未验证它们能在新环境中直接运行；恢复可复现样本与环境是 M0 的工作。

M0 的验收命令：

```bash
bash validation/m0/setup_env.sh --check   # 核对锁定的环境
python3 validation/m0/run_m0_gate.py       # 完整门禁，写出 artifacts/m0/<run-id>/
python3 validation/m0/verify_report.py --run-dir artifacts/m0/<run-id>
python3 validation/m0/traceability.py --run-dir artifacts/m0/<run-id>
```
