# M3 复跑与验收指南

**当前状态**：M3 全部 49 项实施任务（`T001`–`T049`）及全链路代码审查加固项已完成，最新正式全量门禁 `artifacts/m3/m3-review-fixed-gate/` 已于 2026-11-26 验证通过（`report.json` `overall_status == "passed"`，`traceability.json` `18/18 FR` 与 `8/8 SC` 全部 `pass`，无循环独立审计 `report-audit.json` `audit_status == "passed"`、210 项证据引用核验通过且 `findings == []`，M1 完整回归 `m1-regress-fdea7f74` 与 M2 完整回归 `m2-regress-83493031` 均 0 退化通过，全新 clone 独立复现 `PASS`）。演化关系中的分裂/合并与 `attributed_flow` 仅代表算法归因，不代表集合成员跨起报发生物理迁移，不代表天气系统物理新生或消亡，`member_fraction` 未经概率校准且不构成预报技巧或交易价值依据。

本指南说明如何在本地与全新检出环境中构建、执行并独立复核 M3 情景演化分析的全部验证场景。核心接口、数值公式、数据库表结构、可视化契约与证据映射分别见 [core.md](contracts/core.md)、[numerics.md](contracts/numerics.md)、[sql-history.md](contracts/sql-history.md)、[visualization.md](contracts/visualization.md) 与 [validation-evidence.md](contracts/validation-evidence.md)。

---

## 1. 前置条件与环境准备

- **目标平台**：Linux aarch64（Ubuntu 24.04）、Rust `1.96.0`（`--locked`）、DuckDB CLI `v1.5.5`（`d8cdaa33fd`）、Python `3.13.13`、Docker（用于 `--network none` 最小运行容器核验）。
- **零新增外部依赖**：M3 直接复用 M0/M1/M2 已锁定的 Python 虚拟环境与 DuckOMO 发布包缓存（`v0.3.5-rc.1` 目标版与 `v0.3.4` 对照版），离线可交互 `.html` 渲染与核验仅使用 Python 标准库。首次在空缓存机器运行需联网拉取锁定包与 WeatherBench2 源块；缓存齐备后可离线复用。

从仓库根目录检查或初始化环境：

```bash
bash validation/m3/setup_env.sh
bash validation/m3/setup_env.sh --check
```

---

## 2. 一键执行完整 M3 验收门禁（推荐）

正式判定 M3 是否完成必须通过完整门禁与无循环独立审计：

```bash
cargo fmt --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
validation/m2/.venv/bin/python validation/m3/run_m3_gate.py --run-id m3-acceptance
validation/m2/.venv/bin/python validation/m3/traceability.py --run-dir artifacts/m3/m3-acceptance --check
validation/m2/.venv/bin/python validation/m3/verify_report.py --run-dir artifacts/m3/m3-acceptance
validation/m2/.venv/bin/python validation/m3/verify_report.py --self-check
```

**预期通过标准**：
- `artifacts/m3/m3-acceptance/report.json` 中 `overall_status == "passed"`（全部 15 个前置必选门禁步骤 `exit_code == 0` 且 `m1_regression` 严格先于 `m2_regression`）；
- `artifacts/m3/m3-acceptance/traceability.json` 中 `status == "pass"`，100% 通过全部 `FR-001`–`FR-018`（18 项）与 `SC-001`–`SC-008`（8 项）；
- `artifacts/m3/m3-acceptance/report-audit.json` 中 `audit_status == "passed"` 且 `findings == []`；
- `m1-regression/summary.json`、`m2-regression/summary.json` 与 `independent-reproduction.json` 均显示 `PASS` / `passed`（0 退化）。

---

## 3. 分步手动验证流程

如需分环节单独调试或审查中间产物（以运行目录 `artifacts/m3/manual-check` 为例）：

### 3.1 构建扩展与准备冻结 OM 样本

```bash
bash validation/m3/build_extension.sh --output-dir "$PWD/artifacts/m3/manual-check/build"
validation/m2/.venv/bin/python validation/m2/prepare_om_sample.py \
  --cache-dir validation/m2/.cache \
  --out-dir "$PWD/artifacts/m3/manual-check/sample"
```

- 预期产出：`ensabc_m0_extension.duckdb_extension`、`m1_runner`、`m2_runner`、`m3_runner` 以及 8 个无损 `fpx_xor_2d` `.om` 样本文件。

### 3.2 验证 Rust Core 多对多演化、正交分离与置换不变性（US1、US2）

```bash
validation/m2/.venv/bin/python validation/m3/verify_evolution_core.py \
  --run-dir "$PWD/artifacts/m3/manual-check" \
  --runner "$PWD/artifacts/m3/manual-check/build/m3_runner"
```

- 预期结果：25 个固定合成对用例（覆盖全部 18 类拓扑/边界及三余量通道 `0.05R` vs `0.15R` 夹逼场景）、2 组正交分离序列、4 类多批整链、4,000 个随机种子用例与 Python `Fraction` 独立穷举参考 100% 0-ULP 逐字段一致；2,000 次随机编号置换与 `3×4` 全 144 排列零差异；纯形态漂移序列占比变化精确为 `+0.0`，纯占比转移序列形态变化精确为 `+0.0`；21 个非法输入、比例溢出与篡改主干负例（覆盖全部 17 类错误码）100% 拒绝且 `CoreError <= 112B` 守卫通过。

### 3.3 验证 DuckDB 扩展契约、不可变历史表与双版本 DuckOMO 回放（US3）

```bash
validation/m2/.venv/bin/python -m unittest validation.m3.test_sql_history
validation/m2/.venv/bin/python validation/m3/verify_sql_and_history.py \
  --scope all \
  --run-dir "$PWD/artifacts/m3/manual-check" \
  --sample-dir "$PWD/artifacts/m3/manual-check/sample" \
  --cache-dir validation/m2/.cache \
  --extension "$PWD/artifacts/m3/manual-check/build/ensabc_m0_extension.duckdb_extension"
```

- 预期结果：`ens_evolve_scenarios` 与 `ens_fsum` 通过 `>2048` 行跨向量块测试与同会话错误恢复；真实 8 批与合成含缺口多批链成功写入 11 张表（5 张 M2 表：`tracking_runs`、`replay_batches`、`scenario_nodes`、`pair_comparisons`、`scenario_links`；6 张 M3 表：`evolution_runs`、`evolution_nodes`、`evolution_transitions`、`evolution_edges`、`evolution_events`、`evolution_branch_summaries`）并通过 `E1`–`E8` 全部守卫；三独立进程重跑 100% 一致；同键异内容写入报 `EVOLUTION_RUN_VERSION_CONFLICT`；7 个钩子注入中途失败回滚后 0 残留。

### 3.4 真实 8 批案例回放、两层扰动与门槛/聚类敏感性分析（US4）

```bash
validation/m2/.venv/bin/python -m unittest validation.m3.test_stability
validation/m2/.venv/bin/python validation/m3/run_case_and_sensitivity.py \
  --run-dir "$PWD/artifacts/m3/manual-check" \
  --sample-dir "$PWD/artifacts/m3/manual-check/sample" \
  --cache-dir validation/m2/.cache \
  --extension "$PWD/artifacts/m3/manual-check/build/ensabc_m0_extension.duckdb_extension"
```

- 预期结果：产出主案例（`k=3, c=1.0`：24 节点、8 分支、16 主干边 + 2 次级分裂 + 1 次级合并 + 3 新生 + 4 消失，精准标出 `b0→b1`、`b3→b4`、`b6→b7` 高不确定性跃迁）、Level 1a/1b 与 Level 2 扰动核验（获四条件证书的稳健跃迁 **0 翻转**，Level 1b 纯占比微扰 **0 拓扑翻转**）、18 组门槛扫描、4 轴聚类配置对照（含单批算法 `failed` 自然断链记录）与局限说明 `case-study/limitations.json`。

### 3.5 渲染离线单文件可交互演化图并执行自动溯源审计（US5）

```bash
REAL_ERID="$(validation/m2/.venv/bin/python -c 'import json; print(json.load(open("artifacts/m3/manual-check/sql-history/real-evolution-replay.json"))["evolution_run_id"])')"
SYNTH_ERID="$(validation/m2/.venv/bin/python -c 'import json; print(json.load(open("artifacts/m3/manual-check/sql-history/synthetic-evolution-replay.json"))["showcase_evolution_run_id"])')"

validation/m2/.venv/bin/python validation/m3/render_evolution_html.py \
  --db "$PWD/artifacts/m3/manual-check/sql-history/evolution-runs.duckdb" \
  --evolution-run-id "$REAL_ERID" \
  --output "$PWD/artifacts/m3/manual-check/visualization/real-8run-main.html"

validation/m2/.venv/bin/python validation/m3/render_evolution_html.py \
  --db "$PWD/artifacts/m3/manual-check/sql-history/evolution-runs.duckdb" \
  --evolution-run-id "$SYNTH_ERID" \
  --output "$PWD/artifacts/m3/manual-check/visualization/synthetic-showcase.html"

validation/m2/.venv/bin/python validation/m3/verify_visualization_traceability.py \
  --html "$PWD/artifacts/m3/manual-check/visualization/real-8run-main.html" \
  --db "$PWD/artifacts/m3/manual-check/sql-history/evolution-runs.duckdb" \
  --evolution-run-id "$REAL_ERID" \
  --out-report "$PWD/artifacts/m3/manual-check/visualization/traceability-audit.json"

validation/m2/.venv/bin/python validation/m3/verify_visualization_traceability.py \
  --html "$PWD/artifacts/m3/manual-check/visualization/synthetic-showcase.html" \
  --db "$PWD/artifacts/m3/manual-check/sql-history/evolution-runs.duckdb" \
  --evolution-run-id "$SYNTH_ERID" \
  --out-report "$PWD/artifacts/m3/manual-check/visualization/synthetic-traceability-audit.json"

validation/m2/.venv/bin/python validation/m3/verify_visualization_traceability.py \
  --self-check \
  --out-report "$PWD/artifacts/m3/manual-check/visualization/tamper-self-check.json"

validation/m2/.venv/bin/python -m unittest validation.m3.test_evidence_and_viz
```

- 预期结果：生成的 `real-8run-main.html` 与 `synthetic-showcase.html` 可直接在浏览器离线打开点选查看任意节点与主/次连线的完整计算依据；自动核验确认 0 外部链接、0 孤立 SVG 图元、与 DuckDB 历史表 100% 逐字段一致，且 10 类篡改反例与缺溯源字段渲染全部被拒绝。

### 3.6 断网最小环境、七环节测量、M1/M2 串行回归与独立复现（US5）

```bash
bash validation/m3/verify_minimal_runtime.sh \
  --run-dir "$PWD/artifacts/m3/manual-check" \
  --extension "$PWD/artifacts/m3/manual-check/build/ensabc_m0_extension.duckdb_extension" \
  --runner "$PWD/artifacts/m3/manual-check/build/m3_runner" \
  --sample-dir "$PWD/artifacts/m3/manual-check/sample" \
  --cache-dir validation/m2/.cache

validation/m2/.venv/bin/python validation/m3/measure_pipeline.py \
  --run-dir "$PWD/artifacts/m3/manual-check" \
  --extension "$PWD/artifacts/m3/manual-check/build/ensabc_m0_extension.duckdb_extension" \
  --runner "$PWD/artifacts/m3/manual-check/build/m3_runner" \
  --sample-dir "$PWD/artifacts/m3/manual-check/sample" \
  --cache-dir validation/m2/.cache

validation/m2/.venv/bin/python validation/m3/run_m1_regression.py --run-dir "$PWD/artifacts/m3/manual-check"
validation/m2/.venv/bin/python validation/m3/run_m2_regression.py --run-dir "$PWD/artifacts/m3/manual-check"
validation/m2/.venv/bin/python validation/m3/reproduce_independently.py --self-check
validation/m2/.venv/bin/python validation/m3/reproduce_independently.py \
  --run-dir "$PWD/artifacts/m3/manual-check" \
  --sample-dir "$PWD/artifacts/m3/manual-check/sample" \
  --cache-dir validation/m2/.cache
validation/m2/.venv/bin/python validation/m3/traceability.py --run-dir "$PWD/artifacts/m3/manual-check"
```

> **注意**：`run_m1_regression.py` 与 `run_m2_regression.py` 必须按顺序串行执行，切勿后台并行启动；仓库不提交 `artifacts/m3/` 或任何 `.om` / `.duckdb` / `.html` 派生运行产物。
