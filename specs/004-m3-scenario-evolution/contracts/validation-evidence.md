# M3 验证协议与证据契约

版本：`m3.report.v1` / `m3.traceability.v1`。对应 `spec.md` 中 FR-001–FR-018（18 项功能需求）与 SC-001–SC-008（8 项成功标准）。

---

## 1. 验证命令清单（`validation/m3/`）

所有命令从仓库根目录执行，退码遵循统一约定：`0` 通过，`1` 校验或计算失败，`2` 缺少前置条件或参数错误。Python 脚本统一使用锁定的 `validation/m2/.venv/bin/python`（Python `3.13.13`），消除宿主未声明环境漂移。

| 脚本 | 参数与职责 |
| --- | --- |
| `setup_env.sh` | `[--check] [--json] [--cache-dir DIR] [--om-venv DIR]`；复用并核验 M0/M1 数据与参考 venv、M2 OM venv（`requirements-om.lock`）及 `validation/m2/.cache/` 下双版本 DuckOMO（`v0.3.5-rc.1` 与 `v0.3.4`）官方发布包 SHA-256；若缺少 M0 冻结样本载荷则自动恢复以备 M1 回归 |
| `build_extension.sh` | `[--output-dir DIR] [--target-dir DIR] [--check-tools]`；默认独立目标目录 `/tmp/ensabc-m3-extension-target`，以 `--release --locked` 构建 `m1_runner`、`m2_runner`、`m3_runner` 与 `ensabc_m0_extension.duckdb_extension`（含 `ens_evolve_scenarios` 与 `ens_fsum`），输出 `build_manifest.json` |
| `oracle.py` | 独立参考实现（不共享 `evolution.rs` 代码）：使用 `math.fsum` 与 `fractions.Fraction` 穷举求主干匹配、单近邻次级边、规范决胜、星形流向归因与三族正交指标，并提供向外舍入有理区间四条件演化稳定性证书判定器 |
| `verify_evolution_core.py` | `--run-dir DIR [--runner PATH]`；执行 25 个固定合成对用例（覆盖全部 18 类拓扑/边界及三通道 `0.05R` vs `0.15R` 夹逼场景）、2 组正交分离序列、4 类多批整链用例、21 个非法输入/溢出/主干冲突负例（覆盖全部 17 类错误码）、4,000 个随机种子用例（`seed=20261125`）、2,000 次随机行列/编号置换 + `3×4` 全 144 排列不变性测试及 `CoreError <= 112B` 核验（FR-002–FR-008、FR-012–FR-013、SC-001、SC-003、SC-004） |
| `test_sql_history.py` | 契约单测套件（`python -m unittest validation.m3.test_sql_history`）；覆盖 `ens_evolve_scenarios` / `ens_fsum` 的顶层与嵌套 `NULL` 拒绝、`>2048` 行跨向量块边界、篡改 `backbone_match` 报错、同会话 `valid -> invalid -> valid` 恢复、单批白名单算法失败断链与非白名单中止区分、`gap_interrupted` / `gap_restarted` 互斥、同版本幂等、`EVOLUTION_RUN_VERSION_CONFLICT` 拒绝及 `inject_m3_failure` 回滚零残留（FR-009–FR-012、SC-004） |
| `verify_sql_and_history.py` | `--run-dir DIR --scope synthetic\|real\|all --extension PATH [--sample-dir DIR --cache-dir DIR]`；验证 `ens_evolve_scenarios` / `ens_fsum` C API 契约、`>2048` 行跨 chunk 边界、同会话错误恢复、11 表（5 M2 + 6 M3）原子发布、`E1`–`E8` 八条守卫、缺批/单批算法失败断链（`gap_interrupted` / `gap_restarted`）、三独立进程 100% 一致重跑、同版本幂等与 `EVOLUTION_RUN_VERSION_CONFLICT` 拒绝、7 个钩子中途故障注入回滚 0 残留及双版本 DuckOMO 演化一致性（FR-009–FR-012、SC-002、SC-004） |
| `test_stability.py` | 契约单测套件（`python -m unittest validation.m3.test_stability`）；验证向外舍入 `Fraction` 有理区间四条件演化稳定性证书（有效门槛边界、主干同基数余量、`MatchResult.optima_count == 1`、次级竞争余量 $4\varepsilon_{\text{abs}}$）、$q^*=0$/无同基数替代边界及 `evaluation_mapping_ambiguous` 判定（FR-016、SC-005） |
| `run_case_and_sensitivity.py` | `--run-dir DIR [--extension PATH --sample-dir DIR --cache-dir DIR --db PATH]`；生成 WB2 8 批主配置演化报告（`k=3, c=1.0`）、Level 1a/1b 演化输入微扰与 Level 2 成员矩阵微扰报告（核验四条件证书稳健对 **0 翻转**，完整记录边界翻转及余量）、`c ∈ {0.5..2.0} × k ∈ {2,3,4}` 18 组门槛扫描、4 轴聚类配置敏感性对照（含自然触发单批 `failed` 的断链配置）及局限说明 `limitations.json`（FR-015–FR-016、SC-002、SC-005） |
| `render_evolution_html.py` | `--db PATH --evolution-run-id ID --output PATH`；从已发布 DuckDB 六表渲染零外链离线单文件可交互演化图（分别产出真实 8 批主视图 `real-8run-main.html` 与覆盖缺口断链/并列歧义/复合分裂合并的 `synthetic-showcase.html`）（FR-014、SC-006） |
| `verify_visualization_traceability.py` | `--html PATH --db PATH --evolution-run-id ID [--out-report PATH]` 及 `--self-check [--out-report PATH]`；执行六重自动溯源检查（CSP 零外链、SVG XML 零孤立图元、SVG↔JSON 双射、HTML↔DuckDB 100% 逐字段真值一致、免责声明审查）与 10 类恶意篡改反例自检（FR-014、SC-006） |
| `test_evidence_and_viz.py` | 契约单测套件（`python -m unittest validation.m3.test_evidence_and_viz`）；覆盖 10 类 HTML/SVG/JSON 篡改反例、缺溯源字段渲染拒绝、缺失门禁步骤、`../` 路径穿越、证据 SHA-256 失配、伪造 `tracking_run_key[:12]:` 前缀及违禁物理迁移/校准概率宣称拦截（FR-008、FR-014、FR-018、SC-006、SC-008） |
| `verify_minimal_runtime.sh` | `--run-dir DIR --extension PATH --runner PATH [--sample-dir DIR --cache-dir DIR --duckomo PATH --image NAME]`；在 `--network none` 且确认无 Python/R/CDO/wgrib 的 `ubuntu:24.04` 最小容器中，仅凭 `m3_runner` 与 `DuckDB CLI + DuckOMO + ensabc` 完成本地冻结 OM 8 批端到端演化回放、11 表发布及同会话错误恢复，并与宿主机结果逐字段比对（FR-017、SC-007） |
| `measure_pipeline.py` | `--run-dir DIR [--extension PATH --runner PATH --sample-dir DIR --cache-dir DIR --duckomo PATH]`；以 `REPEATS = 3` 测量七个环节（OM 读取前缀、单批聚类+离散前缀、M2 主干匹配前缀、M3 演化计算前缀、11 表原子发布全流程、`m3_runner` 独立整链、交互 HTML 渲染与核验）的墙钟时间与 `ru_maxrss` 峰值内存（FR-018、SC-008） |
| `run_m1_regression.py` / `run_m2_regression.py` | `--run-dir DIR [--cache-dir DIR]`；在当前 M3 代码基线上**严格串行**重跑完整 `validation/m1/run_m1_gate.py` 与完整 `validation/m2/run_m2_gate.py`（避免 `/tmp/ensabc-m1-extension-target` 并发冲突），断言两套门禁 `report`、`traceability`、`report-audit` 全部 `passed`（FR-011、FR-018、SC-008） |
| `reproduce_independently.py` | 父模式 `--run-dir DIR [--sample-dir DIR --cache-dir DIR --work-dir DIR]`、子模式 `--reproduction-child --output-dir DIR --source-manifest PATH --sample-dir DIR --cache-dir DIR` 与 `--self-check [--out-report PATH]`；在全新外部 clone 中叠加当前有效源码快照、从零构建二进制、仅复用显式缓存与冻结 OM 样本重跑确定性验证子集与可视化核验，核验真实 `run_key[:12]:` 前缀后按出生节点键比对全部节点、主/次边、事件、三族指标与可视化溯源清单 100% 一致（FR-018、SC-008） |
| `traceability.py` | `--run-dir DIR [--out PATH] [--check]`；对 FR-001–FR-018 与 SC-001–SC-008 执行全部可机读断言，生成带多文件 `evidence_refs`（路径 + SHA-256）的 `traceability.json`（`--check` 只读核对既有 `traceability.json` 不改写磁盘） |
| `verify_report.py` | `--run-dir DIR` 或 `--self-check [--out-report PATH]`；无循环独立审计：核对 `REQUIRED_STEPS` 退码为 0、拦截路径穿越与符号链接别名、核验源码/构建清单与证据 SHA-256、只读重跑 `TraceabilityEvaluator` 与 HTML 溯源断言、执行坏证据反例自检，生成 `report-audit.json` |
| `run_m3_gate.py` | `--run-id ID [--cache-dir DIR]`；按固定无环顺序执行全部门禁步骤并生成 `artifacts/m3/<run-id>/report.json` 与 `report-audit.json` |

---

## 2. 产物目录布局（`artifacts/m3/<run-id>/`，全部 Git 忽略）

```text
artifacts/m3/<run-id>/
  environment.json
  source-manifest.json
  execution-steps.json
  build/
    build_manifest.json
    ensabc_m0_extension.duckdb_extension
    m1_runner / m2_runner / m3_runner
  sample/                            # 本地无损 OM 样本副本（受限派生数据，不入库）
  om-verification/
    cell-matrix-manifest.json
    om-cells-report.json
    test-om-sample-report.json
    duckomo-version-comparison.json
  core/
    synthetic-oracle.json
    orthogonality-separation.json
    permutation-invariance.json
    error-cases.json
  sql-history/
    evolution-runs.duckdb
    sql-contract.json
    real-evolution-replay.json
    synthetic-evolution-replay.json
    three-process-repro.json
    transaction-atomicity.json
  case-study/
    main-evolution-k3-c1.0.json
    perturbation-evolution-level.json
    perturbation-member-level.json
    sensitivity-thresholds.json
    sensitivity-clustering-configs.json
    limitations.json
  visualization/
    real-8run-main.html
    synthetic-showcase.html
    real-traceability-audit.json
    synthetic-traceability-audit.json
    traceability-audit.json
    tamper-self-check.json
  minimal-runtime.json
  measurements.json
  m1-regression/
    summary.json
    report.json / traceability.json / report-audit.json
  m2-regression/
    summary.json
    report.json / traceability.json / report-audit.json
  independent-reproduction.json
  reproduction/
    effective-source-manifest.json
    self-check.json
    child/
  traceability.json
  report.json
  report-audit.json
```

---

## 3. FR-001–FR-018 与 SC-001–SC-008 可执行追溯映射表

| 需求 ID | 关键可执行断言与目标证据文件 | 关联 SC |
| --- | --- | --- |
| FR-001 | 4–8 批计划连续起报序列支持，模型/变量/单位/9 点顺序/`valid_time`/递增 `init_time`/时效恒等式沿用 M2 零放宽，负例 100% 拒绝（`om-verification/cell-matrix-manifest.json`, `om-verification/om-cells-report.json`, `om-verification/test-om-sample-report.json`, `case-study/main-evolution-k3-c1.0.json`） | SC-002, SC-004 |
| FR-002 | 主干 100% 保留 M2 最优匹配与持续身份；`nearest_feasible_v1` 仅为 `not_selected_in_global_optimum` 建立 `secondary_split`/`secondary_merge`；`star_min_mass_proportional_v1` 归因流量与显式未归因残差精确一致；`no_candidate` 0 边（`core/synthetic-oracle.json`, `sql-history/real-evolution-replay.json`） | SC-001, SC-002 |
| FR-003 | 五类基础事件（`continuation`, `split`, `merge`, `emergence`, `disappearance`）与缺口状态（`gap_interrupted`, `gap_restarted`）互斥完备；复合分裂+合并菱形用例零重边、零矛盾归类（`core/synthetic-oracle.json`, `sql-history/synthetic-evolution-replay.json`） | SC-001 |
| FR-004 | 2,000 次随机顺序/编号置换及 `3×4` 全 144 排列下非对称用例输出 100% 不变；主干与次级精确并列按 `canonical_scenario_order` 决胜且 `ambiguous_tie = true`（`core/permutation-invariance.json`, `core/synthetic-oracle.json`） | SC-001 |
| FR-005 | 三族演化指标（形态距离与相对离散比、占比净变/TV 重分配/归因流量、三通道余量与歧义标记）全部有限且与 Python `Fraction` oracle 逐位一致（`core/synthetic-oracle.json`, `core/orthogonality-separation.json`） | SC-001, SC-002, SC-003 |
| FR-006 | 输出连续三族指标 + 五类基础事件/缺口状态 + 冻结 `m3.uncertainty.v1`（`0.10`）高不确定性标记与 `uncertainty_reasons`；`0.05S` vs `0.15S` 跨边界用例精确命中（`core/synthetic-oracle.json`, `case-study/main-evolution-k3-c1.0.json`） | SC-002, SC-003 |
| FR-007 | 分支级（`evolution_branch_summaries`）与全链级（`evolution_runs.chain_summary`）生命周期、累计/峰值形态变化、初末/累计占比变化、分裂合并次数与高不确定性比例与 Rust `evolve_chain` 及 oracle 100% 一致（`sql-history/sql-contract.json`） | SC-001, SC-002 |
| FR-008 | 核心输出、历史表、案例报告与交互 HTML 100% 携带算法解释边界声明，全文扫描零物理成员迁移/物理新生消亡/校准概率/交易价值违禁宣称（`visualization/traceability-audit.json`, `case-study/limitations.json`） | SC-006 |
| FR-009 | 不可变 `evolution_run_id` 与 `content_sha256`；三独立进程重跑 100% 相同；同版本幂等、篡改内容报 `EVOLUTION_RUN_VERSION_CONFLICT`；改参数生成新版本且旧版本完整保留（`sql-history/three-process-repro.json`, `sql-history/transaction-atomicity.json`） | SC-004 |
| FR-010 | M3 六表与 M2 五表完整持久化；`E1`–`E8` 守卫验证 M2 主干与 `branch_id` 100% 一致、零跨缺口边；中途故障注入回滚后 0 残留（`sql-history/transaction-atomicity.json`, `sql-history/real-evolution-replay.json`） | SC-002, SC-004 |
| FR-011 | 无状态 `ens_evolve_scenarios`（及 `ens_fsum`、`ens_diagnose_batch`）直接计算相邻对演化与单批白名单诊断；`ens_cluster`、`ens_dispersion`、`ens_match_scenarios` 原有签名与输出 100% 不变（`sql-history/sql-contract.json`, `m1-regression/summary.json`, `m2-regression/summary.json`） | SC-002, SC-008 |
| FR-012 | 17 类非法输入/占比违约/主干不一致/溢出错误（21 个固定+动态负例）100% 明确拒绝并报告位置；同会话 `valid -> invalid -> valid` 100% 恢复；`>2048` 行含坏行查询整体失败（`core/error-cases.json`, `sql-history/sql-contract.json`） | SC-004 |
| FR-013 | 25 个固定合成对用例（覆盖全部 18 类拓扑/边界及三通道夹逼）+ 4 类多批链 + 4,000 随机用例与独立 `Fraction` oracle 100% 一致（`core/synthetic-oracle.json`, `core/orthogonality-separation.json`） | SC-001, SC-003 |
| FR-014 | 离线单文件可交互 `.html`（`real-8run-main.html`, `synthetic-showcase.html`）通过六重自动核验，100% 图元与 DuckDB 历史表逐字段一致，10 类篡改反例 100% 拦截（`visualization/traceability-audit.json`, `visualization/tamper-self-check.json`） | SC-006 |
| FR-015 | WB2 8 批主配置完整复现 24 节点、8 分支、16 主干边、3 次级边（1 合并 + 2 分裂）及全部五类事件与局限说明（`case-study/main-evolution-k3-c1.0.json`, `case-study/limitations.json`） | SC-002 |
| FR-016 | Level 1a/1b + Level 2 扰动回放（四条件证书对 **0 翻转**，边界翻转全带余量）、18 组门槛扫描及 4 轴聚类配置敏感性（含真实单批 `failed` 断链）全部完成（`case-study/perturbation-*.json`, `case-study/sensitivity-*.json`） | SC-005 |
| FR-017 | `--network none` 且无 Python/参考工具最小容器内完成 `m3_runner` 与 `DuckDB + DuckOMO + ensabc` 端到端 8 批演化发布及同会话错误恢复，与正常环境 100% 一致（`minimal-runtime.json`） | SC-007 |
| FR-018 | 复跑指南、七环节耗时与内存测量、局限说明、M1+M2 串行完整回归通过、全新 clone 独立复现通过、`report.json` + `traceability.json` + `report-audit.json` 全部 `passed`（`measurements.json`, `m1-regression/`, `m2-regression/`, `independent-reproduction.json`, `report-audit.json`） | SC-008 |
