# M3 验证套件（多对多情景演化分析）

本文档说明 Milestone 3（M3）情景演化分析（`004-m3-scenario-evolution`）的验证目录布局、环境与缓存复用边界、命令退出码、受限派生数据合规规则以及算法解释边界。

## 1. 运行产物布局与隔离规则

- **运行证据目录（`artifacts/m3/<run-id>/`）**：
  每次门禁（`validation/m3/run_m3_gate.py --run-id <run-id>`）或验证脚本执行均绑定独占的 `<run-id>` 目录（若目录已存在默认以退出码 `2` 拒绝覆盖），按固定子目录写出 `environment.json`、`source-manifest.json`、`execution-steps.json`、`build/`（`build_manifest.json`、`ensabc_m0_extension.duckdb_extension`、`m1_runner`、`m2_runner`、`m3_runner`）、`sample/`、`om-verification/`、`core/`、`sql-history/`（含 11 表 `evolution-runs.duckdb`）、`case-study/`、`visualization/`（`real-8run-main.html`、`synthetic-showcase.html`、`traceability-audit.json`、`tamper-self-check.json`）、`minimal-runtime.json`、`measurements.json`、`m1-regression/summary.json`、`m2-regression/summary.json`、`independent-reproduction.json`、`reproduction/` 以及无循环审计报告（`traceability.json`、`report.json`、`report-audit.json`）。该目录已被 `.gitignore` 忽略，严禁入库。
- **持久化数据库表结构（11 张正式表）**：
  `validation/m3/sql/schema.sql` 原样保留 M2 五张表（`tracking_runs`、`replay_batches`、`scenario_nodes`、`pair_comparisons`、`scenario_links`），并增量定义 M3 六张不可变演化表（`evolution_runs`、`evolution_nodes`、`evolution_transitions`、`evolution_edges`、`evolution_events`、`evolution_branch_summaries`），由 `validation/m3/sql/stage_and_publish.sql` 在单事务内原子发布并执行 `E1`–`E8` 守卫。
- **派生数据暂存目录（`validation/m3/data/`）**：
  供本地验证临时存放受限气象载荷或中间数据库，同样被 `.gitignore` 忽略，严禁入库。
- **构建与测试隔离**：
  - `validation/m3/build_extension.sh` 默认使用独立目标目录 `/tmp/ensabc-m3-extension-target`（环境变量 `ENSABC_M3_TARGET_DIR`，或通过 `--output-dir DIR` 指定），不与 M0/M1/M2 构建目录冲突。
  - M1 门禁（`validation/m1/run_m1_gate.py`）与 M2 门禁（`validation/m2/run_m2_gate.py`，内部包含 M1 回归）共享 `/tmp/ensabc-m1-extension-target`，因此 M3 中的 `run_m1_regression.py` 与 `run_m2_regression.py` **必须严格串行执行**。
  - 每次 DuckDB CLI 调用均使用独立临时 `HOME` 与 `extension_directory`，显式关闭 `autoinstall_known_extensions` 与 `autoload_known_extensions`，并在加载 DuckOMO 时核验官方发布包 SHA-256 与 `NOT_INSTALLED` 身份。

## 2. 缓存与锁定环境复用边界

M3 零新增第三方 Rust crate 或 Python 依赖包，显式复用已锁定并经哈希校验的本地资源：
1. **Python 验证环境**：统一使用 `validation/m2/.venv/bin/python`（Python `3.13.13`，含 `omfiles==1.2.0`、`numpy==2.5.4`、`numcodecs==0.17.0`）；HTML/SVG 生成与 DOM/XML 溯源核验仅使用 Python 3.13 标准库。
2. **M0 / M1 参考环境与样本载荷**：`setup_env.sh` 复用并核验 `/tmp/ensabc-m0-data/.venv`、`/tmp/ensabc-m0-baseline` 以及 `validation/m0/fixtures/real_ifs_ens_20200101_t2m_data.json`（缺失时自动按冻结清单恢复，供 M1 回归门禁使用）。
3. **双版本 DuckOMO 扩展缓存**：复用 `validation/m2/.cache/` 下经 `validation/m2/fixtures/duckomo_releases.json` 锁定的 `v0.3.5-rc.1`（目标版本）与 `v0.3.4`（对照基线）`duckdb-v1.5.5-linux_arm64` 预编译扩展。
4. **联网与离线边界**：除首次执行 `setup_env.sh` 或样本准备外，M3 核心验证、SQL 历史发布、案例敏感性分析、交互 HTML 渲染与溯源审计全部离线运行；`verify_minimal_runtime.sh` 在 `--network none` 且无 Python/参考工具的最小容器中执行端到端核验。

## 3. 命令退出码规范

`validation/m3/` 下所有 Shell 与 Python 验证脚本统一遵守以下退出码约定：
- **`0`（PASS）**：全部验证断言、哈希核对、守卫不变量与自检反例拦截均通过。
- **`1`（FAIL）**：数值对照失配、不变量守卫失败、非法输入未被拒绝、溯源字段缺失或审计发现异常。
- **`2`（MISSING_PREREQUISITE / USAGE_ERROR）**：缺少声明的前置条件（工具链版本不符、缓存/样本缺失）或命令行参数非法。

## 4. 受限派生数据合规与 GPL 隔离

- **禁止提交派生气象载荷与运行产物**：WeatherBench 2 IFS ENS 派生 `.om`、`.bin`、`real_ifs_ens_20200101_t2m_data.json`、运行期 `.duckdb` 数据库及渲染生成的 `.html` / `.json` 证据文件均不得进入版本库；Git 仅跟踪源码、SQL、冻结实验参数（`evolution_experiment_config.json`）、小型合成用例（`synthetic_evolution_cases.json`）与文档。
- **许可证隔离**：`omfiles`（GPL-2.0-only）严格限制在隔离的样本准备与三路径核对 Python 子进程中使用，绝不进入 `crates/ensabc-core` 或 `crates/ensabc-duckdb`。

## 5. 算法解释边界与非物理声明（FR-008）

M3 产出的多对多演化关系（`primary`、`secondary_split`、`secondary_merge`）、五类基础事件（`continuation`、`split`、`merge`、`emergence`、`disappearance`）与归因流量（`attributed_flow`）均严格受限于以下解释边界（内置标识 `algorithmic_attribution_only_not_physical_member_migration`）：
1. **非物理成员迁移**：分裂/合并连线与归因流量仅是基于情景物理平均场空间加权距离与集合成员占比的确定性算法归因，**不代表**真实集合预报成员跨起报批次发生物理迁移。
2. **非物理天气系统生消**：`emergence` 与 `disappearance` 仅表示门槛内无可行前驱或后继情景（缺批/单批算法失败则单独标记为 `gap_interrupted` / `gap_restarted`），**不代表**天气系统的物理新生（cyclogenesis）或物理消亡（cyclolysis）。
3. **非校准概率与非交易价值**：成员占比与演化指标不属于校准后的客观发生概率，不构成预报修正技巧证明或业务/交易决策依据。
