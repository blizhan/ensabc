# Tasks: M3 情景演化分析

**Input**: `specs/004-m3-scenario-evolution/` 下的 [spec.md](spec.md)、[plan.md](plan.md)、[research.md](research.md)、[data-model.md](data-model.md)、[contracts/](contracts/) 与 [quickstart.md](quickstart.md)。

**Prerequisites**: M1 与 M2 已实现并通过正式门禁验证；本任务清单基于 2026-11-24 的 3 项规格澄清与 Phase 0/1 技术设计生成。`.specify/memory/constitution.md` 仍为未填写模板，不从占位注释推导虚构治理流程。

**Tests**: `spec.md` 中 `FR-012`–`FR-014`、`FR-016`–`FR-018` 与 `SC-001`–`SC-008` 明确要求独立 `Fraction` 参考对照、正交分离验证、编号置换不变性、SQL 事务冲突与故障回滚、四条件演化稳定性证书、离线可交互 HTML 六重溯源与 10 类篡改拦截、断网最小容器、M1+M2 双回归、全新 clone 独立复现及无循环审计，因此各用户故事均包含先行契约测试与独立核验任务。

**Organization**: 按用户故事优先级（`US1`–`US3` 为 P1，`US4`–`US5` 为 P2）分阶段组织，每个故事均可独立测试和验收。

## Format: `[ID] [P?] [Story] Description`

- 所有源码与脚本路径相对仓库根目录 `/home/blizhan/repo/github/ensabc`。
- **[P]**: 表示满足前置阶段后，与同阶段不触碰相同文件或共享构建目录的任务可并行开发；共享构建目录或回归目录的执行步骤必须串行。
- **[Story]**: 仅用户故事阶段（Phase 3–Phase 7）携带 `[US1]`–`[US5]` 标签；Setup（Phase 1）、Foundational（Phase 2）与 Polish（Phase 8）不携带故事标签。
- **严禁提交派生数据**: 仓库只提交源码、SQL、冻结实验参数、合成小型用例与说明文档；真实气象载荷（`.om`、`.bin`）、`.duckdb` 与运行产出的 `.html` / `.json` 统一写入被 `.gitignore` 忽略的 `artifacts/m3/<run-id>/`。

---

## Phase 1: Setup（共享基础设施与隔离边界）

**Purpose**: 建立独立的 `validation/m3/` 目录结构、Git 忽略防线、环境核验脚本、独立构建脚本与通用运行工具，不改动 M0/M1/M2 原有文件。

- [X] T001 更新 `.gitignore` 忽略 `/artifacts/m3/` 与 `/validation/m3/data/`，并创建 `validation/m3/README.md` 说明运行产物布局、缓存复用边界、命令退出码（`0`/`1`/`2`）、受限派生数据不入库规则及非物理迁移/非概率校准解释边界
- [X] T002 [P] 在 `validation/m3/setup_env.sh` 实现环境初始化与只读核验（`--check`），复用 M0/M1/M2 已锁定虚拟环境（`validation/m2/.venv` Python 3.13.13）及 `validation/m2/.cache/` 下双版本 DuckOMO（`v0.3.5-rc.1` 与 `v0.3.4`）发布包 SHA-256 校验，并检查/恢复 M0 样本载荷以备 M1 回归
- [X] T003 [P] 在 `validation/m3/build_extension.sh` 实现独立构建包装（默认目标目录 `/tmp/ensabc-m3-extension-target`，支持 `--output-dir`），以 `--release --locked` 构建 `m1_runner`、`m2_runner`、`m3_runner` 与 `ensabc_m0_extension.duckdb_extension` 并写出含源码和二进制哈希的 `build_manifest.json`
- [X] T004 在 `validation/m3/common.py` 实现独占运行目录管理、原子文件写入、`-0.0` 归一与有限浮点 round-trip JSON 序列化、有效源码清单采集、隔离 `HOME` / `extension_directory` 的 DuckDB CLI 调用封装，以及校验真实 `run_key[:12]:` 前缀后按 `(birth_ordinal, birth_canonical_rank)` 比较演化谱系的辅助函数（依赖 T001–T003）

---

## Phase 2: Foundational（阻塞所有故事的公共前置基础）

**Purpose**: 冻结全部实验与不确定性参数，向 crate 内部开放 M2 规范序与 DP 辅助，定义 M3 核心类型与错误码并锁定 `CoreError <= 112B` 预算。

**⚠️ CRITICAL**: 所有用户故事阶段必须在完成本阶段后开始。

- [X] T005 [P] 在 `validation/m3/fixtures/evolution_experiment_config.json` 冻结主配置（`k=3, projection_dim=2, seed=3, restarts=10, c=1.0`）、`nearest_feasible_v1`、`star_min_mass_proportional_v1`、`m3.uncertainty.v1` 三通道统一比例 `0.10`、`fraction_sum_tolerance = 1e-9`、Level 1a/1b/2 扰动参数与 `PCG64` 固定种子（`20261029` / `4242`）、18 组 `(k, c)` 门槛网格及 4 轴聚类配置敏感性矩阵
- [X] T006 [P] 在 `crates/ensabc-core/src/matching.rs` 将 `canonical_scenario_order` 及支持强制包含指定行/列的位掩码 DP 求解辅助开放为 `pub(crate)`，保持 M2 `match_scenarios` 与 `match_scenarios_scaled` 的公共签名、不校验占比行为及全部既有测试 100% 不变
- [X] T007 在 `crates/ensabc-core/src/lib.rs` 与新建的 `crates/ensabc-core/src/evolution.rs` 中定义版本/免责声明常量、6 个新增 `CoreErrorCode` 单元变体（`ZeroMemberScenario`、`InvalidMemberFraction`、`InvalidFractionSum`、`InvalidEvolutionConfig`、`InvalidPrimaryLink`、`BackboneInconsistency`，不向 `CoreError` 增加结构体字段）以及 `EvolutionConfig`、`BackboneAssertion`、`EvolutionInput`、`ScaledEvolutionInput`、`EvolutionResult`、`EvolutionChainInput`、`EvolutionChainResult` 完整类型骨架（依赖 T006）
- [X] T008 在 `crates/ensabc-core/tests/m3_evolution_contract.rs` 建立基础编译与字节预算守卫测试：断言 `std::mem::size_of::<CoreError>() <= 112`、核验 M1/M2 原有公共导出零破坏，并定义 M3 新增错误码与非法输入的预期失败断言（依赖 T007）

**Checkpoint**: 冻结配置、核心类型骨架与 `CoreError` 112 字节守卫就绪，可开始用户故事实现。

---

## Phase 3: User Story 1 - 识别与解释多对多演化事件 (Priority: P1) 🎯 MVP

**Goal**: 在 100% 保留 M2 一对一主干关联的前提下，基于 `nearest_feasible_v1` 与 `star_min_mass_proportional_v1` 构建无平行边、零矛盾星形分解的多对多演化图，唯一识别延续、分裂、合并、新生、消失五类基础事件并输出算法归因流量与未归因残差。

**Independent Test**: 脱离 DuckDB、OM 文件与历史表，直接通过 `m3_runner --mode pair` 对固定合成用例、4,000 个随机用例和 2,000 次编号置换与 Python `Fraction` 独立穷举参考逐项比对多对多边集、事件分类、流量归因与错误拒绝结果。

### Tests for User Story 1 ⚠️

> **NOTE: 先编写合成用例、独立 Oracle 与可失败契约测试，再实现 `evolution.rs` 生产逻辑**

- [X] T009 [P] [US1] 在 `validation/m3/fixtures/synthetic_evolution_cases.json` 定义拓扑与边界合成用例集：覆盖纯延续、纯分裂 `1→2`、纯合并 `2→1`、矩形 `2→3` 与 `3→2`、多候选单近邻合并、复合分裂+合并交叉菱形、新生与消失、全超门槛零关系、`d == τ` 与 `τ == 0` 边界、`1→3` 与 `4→1` 剧烈变簇、主干并列、次级精确距离并列、`-0.0` 特征归一及全部 16 类非法输入/篡改主干负例
- [X] T010 [P] [US1] 在 `validation/m3/oracle.py` 基于 Python 标准库 `math.fsum` 与 `fractions.Fraction` 独立实现主干穷举匹配、`nearest_feasible_v1` 单近邻次级边、`canonical_scenario_order` 决胜、五类星形连通分量划分及 `star_min_mass_proportional_v1` 算法归因流量与残差求解器（严禁调用或直译 Rust 实现）
- [X] T011 [US1] 在 `crates/ensabc-core/tests/m3_evolution_contract.rs` 编写 US1 拓扑契约测试：断言 Berge 四条结构不变量（未匹配侧零可行边、无平行重边、同一主干边分裂与合并互斥、星形五分类完备）、次级精确并列规范决胜、显式 `BackboneAssertion` 篡改拒绝与 `3×4` 全 144 排列不变性（依赖 T008、T009）

### Implementation for User Story 1

- [X] T012 [US1] 在 `crates/ensabc-core/src/evolution.rs` 实现输入形状/有限值校验、成员数与占比和容差校验（`ZeroMemberScenario` / `InvalidMemberFraction` / `InvalidFractionSum`）、`EvolutionConfig` 校验，以及 `BackboneAssertion` 对距离矩阵、可行矩阵、一对一约束与全局最优目标 `(q*, Σ*)` 的严格一致性核验（依赖 T011）
- [X] T013 [US1] 在 `crates/ensabc-core/src/evolution.rs` 实现 `nearest_feasible_v1` 次级分裂边（`secondary_split`）与次级合并边（`secondary_merge`）构建、规范场键并列决胜、星形连通分量五分类（`Continuation` / `Split` / `Merge` / `Disappearance` / `Emergence`）、节点角色赋予及固定 binary64 括号顺序的 `star_min_mass_proportional_v1` 归因流量、`edge_strength` 与节点未归因残差计算（依赖 T012）
- [X] T014 [US1] 在 `crates/ensabc-core/examples/m3_runner.rs` 实现 `--mode pair --input --output` 命令行程序（开启 `serde_json/float_roundtrip`、支持测试用非有限值令牌解析与临时文件原子重命名写出），串联 `evolve_scenarios` 与 `evolve_scenarios_scaled`（依赖 T013）
- [X] T015 [US1] 在 `validation/m3/verify_evolution_core.py` 实现 US1 对照核验流程：调用 `m3_runner --mode pair` 与 `validation/m3/oracle.py` 逐字段比对固定合成集、4,000 个随机种子用例（`seed=20261125`）、2,000 次随机顺序/编号置换及全部 16 类错误负例，写出 `core/synthetic-oracle.json`、`core/permutation-invariance.json` 与 `core/error-cases.json`（依赖 T010、T014）
- [X] T016 [US1] 执行 `cargo test --locked -p ensabc-core` 与 `validation/m3/verify_evolution_core.py`（拓扑与负例范围），确认全部多对多拓扑、流向归因与置换不变性测试 100% 通过（依赖 T015）

**Checkpoint**: 相邻两批多对多演化关系与五类事件分类可独立运行并通过穷举 Oracle 校验（MVP 就绪）。

---

## Phase 4: User Story 2 - 分离形态变化、占比变化与匹配不确定性 (Priority: P1)

**Goal**: 在单边、节点、分量、相邻跃迁、持续身份分支与整条回放链六个层级完整输出相互正交的形态变化、成员占比变化和匹配不确定性指标，并按冻结规则 `m3.uncertainty.v1`（`0.10`）输出高不确定性标记。

**Independent Test**: 通过受控正交分离序列（纯形态漂移 vs 纯占比转移）、`0.10` 边界夹逼用例及多批含缺口整链输入，独立运行 `m3_runner --mode pair|chain` 并与 `oracle.py` 逐位核对三族指标与分支/全链汇总。

### Tests for User Story 2 ⚠️

- [X] T017 [P] [US2] 在 `validation/m3/fixtures/synthetic_evolution_cases.json` 补充正交分离与多批链用例：包含 4 档纯形态递增漂移序列（占比恒定）、3 档纯占比重分配序列（物理均值场恒定）、三余量通道 `0.05R` vs `0.15R` 夹逼用例，以及 6 批全事件链与 8 批含 `missing`/`failed` 缺口的整链输入
- [X] T018 [P] [US2] 在 `validation/m3/oracle.py` 扩展三族正交指标计算（形态距离与离散比、主干分支 TV 重分配度与分量残差、有效门槛余量 `min_threshold_margin` 与全矩阵余量、定理 3 次级强制入主干晋升余量、次级竞争余量、`m3.uncertainty.v1` 乘法判定）及多批整链 `branch_id` 继承、缺口事件与分支/全链 `math.fsum` 汇总参考
- [X] T019 [US2] 在 `crates/ensabc-core/tests/m3_evolution_contract.rs` 补充 US2 正交性与整链契约测试：断言纯形态漂移下占比变化指标精确等于 `0.0` 且距离严格单调递增、纯占比转移下形态距离精确等于 `0.0` 且 TV 重分配精确等于理论值、定理 3 次级晋升余量恒有限非负，以及多批 `evolve_chain` 缺口断链与分支生命周期守恒（依赖 T016、T017）

### Implementation for User Story 2

- [X] T020 [US2] 在 `crates/ensabc-core/src/evolution.rs` 补全 `evolve_scenarios` 与 `evolve_scenarios_scaled` 的三族连续指标计算：接入强制行列位掩码 DP 求解次级晋升 `optimality_margin`、计算 `secondary_margin` 与有效门槛余量 `E_evol ∪ {d > τ}`、计算跃迁级 `transition_tv_redistribution` 与残差分解，并实现 `m3.uncertainty.v1` 高不确定性标志与 `uncertainty_reasons`（依赖 T019）
- [X] T021 [US2] 在 `crates/ensabc-core/src/evolution.rs` 实现纯内存多批整链编排 `evolve_chain(&EvolutionChainInput)`（支持 `4..=8` 计划批次、`Ok`/`Missing`/`Failed` 状态、与 M2 一致的确定性 `branch_id` 分配、`GapInterrupted`/`GapRestarted` 互斥事件、`EvolutionBranchSummary` 与 `EvolutionChainSummary`），并在 `crates/ensabc-core/examples/m3_runner.rs` 接入 `--mode chain`（依赖 T020）
- [X] T022 [US2] 在 `validation/m3/verify_evolution_core.py` 补全三族指标 0-ULP 对照、正交分离严格单调/精确零核验与 `--mode chain` 分支/全链汇总核验，输出 `core/orthogonality-separation.json`（依赖 T018、T021）
- [X] T023 [US2] 执行 `cargo clippy --locked --all-targets -- -D warnings`、`cargo test --locked -p ensabc-core` 与完整 `validation/m3/verify_evolution_core.py`，确认 US1 与 US2 全部核心定理、正交分离及整链汇总 100% 通过（依赖 T022）

**Checkpoint**: 三族正交演化指标、高不确定性判定与纯 Rust 多批整链计算全部通过独立 Oracle 验证。

---

## Phase 5: User Story 3 - 持久化不可变演化历史并与 M2 追踪保持一致 (Priority: P1)

**Goal**: 在 DuckDB 扩展中新增无状态标量函数 `ens_evolve_scenarios` 与 `ens_fsum`，在原样保留 M2 五张表的基础上通过两阶段暂存、单事务发布与 `E1`–`E8` 八条守卫原子持久化六张 M3 演化历史表，保证与 M2 主干及 `branch_id` 100% 逐字一致。

**Independent Test**: 在隔离 DuckDB 会话中运行合成多批链（含缺批/单批算法失败）与真实 8 批双版本 DuckOMO 回放，验证跨 `>2048` 行块处理、同会话错误恢复、`E1`–`E8` 守卫、三独立进程重跑一致、同版本幂等、异内容冲突拒绝及中途故障回滚 0 残留。

### Tests for User Story 3 ⚠️

- [X] T024 [P] [US3] 在 `validation/m3/test_sql_history.py` 编写 C API 与 SQL 历史契约单元测试：覆盖 `ens_evolve_scenarios` 与 `ens_fsum` 的顶层/嵌套 `NULL` 拒绝、`>2048` 行跨向量块边界、篡改 `backbone_match` 报错、同会话 `valid → invalid → valid` 恢复、白名单单批 `failed` 断链与非白名单错误阻止发布区分、`gap_interrupted`/`gap_restarted` 与新生消失互斥、同版本幂等、`EVOLUTION_RUN_VERSION_CONFLICT` 拒绝及 `inject_m3_failure` 各阶段回滚 0 残留（依赖 T004、T023）

### Implementation for User Story 3

- [X] T025 [US3] 在 `crates/ensabc-duckdb/src/match_ext.rs` 将 M2 `MatchResult` 逻辑类型构造器与输入读取辅助开放为 `pub(crate)`（保持 `ens_dispersion` 与 `ens_match_scenarios` 零改动），并在新建的 `crates/ensabc-duckdb/src/evolve_ext.rs` 与 `crates/ensabc-duckdb/src/lib.rs` 中实现并注册 `ens_fsum` 与 `ens_evolve_scenarios`（启用 `special_handling`、`catch_unwind` 及全 chunk 计算成功后再统一 `reserve_list` + 写向量）（依赖 T023）
- [X] T026 [P] [US3] 在 `validation/m3/sql/schema.sql` 定义完整数据库 Schema：原样包含 M2 五张表 DDL，并新增 `evolution_runs`、`evolution_nodes`、`evolution_transitions`、`evolution_edges`、`evolution_events`、`evolution_branch_summaries` 六张表的主外键、唯一约束与有限值 `CHECK` 约束（依赖 T024）
- [X] T027 [US3] 在 `validation/m3/sql/stage_and_publish.sql` 实现两阶段 SQL 编排：阶段 A 在 `TEMP` 表执行单批白名单容错、相邻 `ok` 批次匹配与递归 `branch_id` 继承、`ens_evolve_scenarios` 调用、含缺口全跃迁/事件展开、`ens_fsum` 分支与全链精确汇总及规范 `content_sha256` 计算；阶段 B 在单个 `BEGIN ... COMMIT` 事务内执行 M2+M3 幂等/冲突守卫、六表插入、`inject_m3_failure` 钩子及 `E1`–`E8` 八条全局不变量检查（依赖 T025、T026）
- [X] T028 [P] [US3] 在 `validation/m3/sql/real_8run_evolution.sql` 实现无 Python 依赖的真实 8 批端到端 SQL 脚本：设置隔离 `extension_directory` 并关闭自动加载，同连接加载 DuckOMO 与本项目扩展，执行 450 行/坐标/单位/时效恒等式准备门禁后调用阶段 A/B 完成 11 表发布（依赖 T027）
- [X] T029 [US3] 在 `validation/m3/verify_sql_and_history.py` 实现 `--scope synthetic|real|all` 验证脚本：串联 `validation/m2/verify_om_paths.py --stage cells` 三路径校验、合成与真实 8 批 SQL 历史发布、与 `m3_runner --mode chain` 全表逐字段核对、三个独立进程重跑比对、双版本 DuckOMO（`v0.3.5-rc.1` vs `v0.3.4`）演化一致性核验及全部冲突/回滚断言，输出 `sql-history/` 与 `om-verification/` 证据（依赖 T027、T028）
- [X] T030 [US3] 运行 `validation/m3/build_extension.sh`、`validation/m3/test_sql_history.py` 与 `validation/m3/verify_sql_and_history.py --scope all`，确认 11 表发布、`E1`–`E8` 守卫、双版本 DuckOMO 一致性与故障零残留全部通过（依赖 T029）

**Checkpoint**: US1 + US2 + US3 完成从真实本地 OM 样本到不可变多对多演化历史表的完整持久化闭环。

---

## Phase 6: User Story 4 - 真实案例回放、扰动回放与敏感性分析 (Priority: P2)

**Goal**: 在冻结的真实 8 批 WeatherBench2 集合样本上产出完整演化案例报告、两层三档扰动回放（配四条件保守有理区间稳定性证书）、18 组门槛比例系数扫描、4 轴聚类配置敏感性对照（含真实单批 `failed` 断链）及诚实的局限说明。

**Independent Test**: 消费冻结样本与 `evolution_experiment_config.json`，运行 `test_stability.py` 与 `run_case_and_sensitivity.py`，验证获四条件证书的稳健跃迁 0 翻转、全部边界翻转附带原始余量、全部敏感性版本独立入库且局限说明无违禁夸大宣称。

### Tests for User Story 4 ⚠️

- [X] T031 [P] [US4] 在 `validation/m3/oracle.py` 与新建的 `validation/m3/test_stability.py` 中实现四条件向外舍入 `Fraction` 有理区间演化稳定性证书判定器及正反例单测：覆盖有效门槛边界、主干同基数余量、无主干并列、次级竞争余量 $4\varepsilon_{\text{abs}}$、$q^*=0$/无替代、成员最大重叠映射歧义（`evaluation_mapping_ambiguous`）以及合成远离/接近边界两组均非空断言（依赖 T005、T023）

### Implementation for User Story 4

- [X] T032 [US4] 在 `validation/m3/run_case_and_sensitivity.py` 实现真实 8 批主配置（`k=3, c=1.0`）案例深度提取（输出 `case-study/main-evolution-k3-c1.0.json`，覆盖全部 7 次相邻跃迁的延续/合并/分裂/新生/消失与 `b0→b1`、`b3→b4`、`b6→b7` 高不确定性证据）及 `c ∈ {0.5, 0.75, 1.0, 1.25, 1.5, 2.0} × k ∈ {2, 3, 4}` 共 18 个独立演化版本的门槛扫描报告 `case-study/sensitivity-thresholds.json`（依赖 T030、T031）
- [X] T033 [US4] 在 `validation/m3/run_case_and_sensitivity.py` 实现主门槛 `c=1.0` 下的 4 轴聚类配置敏感性实验（`cluster_count ∈ {2,3,4,5}`、`projection_dim ∈ {1,2,3,4}`、`seed/restarts` 对照，以及自然触发白名单 `INSUFFICIENT_RANK`/`AMBIGUOUS_SUBSPACE`/`EMPTY_CLUSTER` 单批 `status='failed'` 断链的边界配置），全部发布为独立 `evolution_run_id` 并写出 `case-study/sensitivity-clustering-configs.json`（依赖 T032）
- [X] T034 [US4] 在 `validation/m3/run_case_and_sensitivity.py` 实现 Level 1a（均值场微扰 $\varepsilon \in \{0.005, 0.01, 0.02\}$，100 方向）、Level 1b（纯占比 $\pm 1$ 成员重分配）与 Level 2（逐成员高斯微扰 $\sigma \in \{0.0, 0.001, 0.01\}\text{ K}$，20 种子）扰动实验，调用四条件稳定性证书核验稳健组 **0 翻转**、记录全部边界翻转与基线余量，并写出 `case-study/perturbation-evolution-level.json`、`case-study/perturbation-member-level.json` 与 `case-study/limitations.json`（依赖 T033）
- [X] T035 [US4] 执行 `validation/m3/test_stability.py` 与完整 `validation/m3/run_case_and_sensitivity.py`，确认获证组零翻转、Level 1b 纯占比微扰零拓扑翻转、全部敏感性配置与真实退化断链记录完整（依赖 T034）

**Checkpoint**: 真实 8 批演化案例证据、扰动稳定性证明、门槛与聚类配置敏感性谱系及方法局限说明完备。

---

## Phase 7: User Story 5 - 可追溯可视化与独立复现验收 (Priority: P2)

**Goal**: 交付由已发布 DuckDB 历史表驱动的零外链离线单文件可交互演化图及六重自动溯源核验器，完成 `--network none` 无 Python 最小容器验证、七环节资源测量、M1 + M2 串行完整门禁回归、全新 clone 独立复现与 18 FR / 8 SC 无循环门禁审计。

**Independent Test**: 渲染真实 8 批与合成全特性 `.html` 并通过六重自动核验与 10 类恶意篡改自检；在全新检出目录按指南独立复现实验结果，且 M1、M2 原有门禁与 M3 独立审计全部 `passed`。

### Tests for User Story 5 ⚠️

- [X] T036 [P] [US5] 在 `validation/m3/test_evidence_and_viz.py` 编写可视化核验与门禁审计的可失败契约单测：覆盖 10 类 HTML/SVG/JSON 篡改反例、缺溯源字段渲染拒绝、缺失门禁步骤、`../` 路径穿越、证据 SHA-256 失配、伪造 `run_key[:12]:` 前缀及违禁物理迁移/校准概率宣称拦截（依赖 T004）

### Implementation for User Story 5

- [X] T037 [P] [US5] 在 `validation/m3/render_evolution_html.py` 仅使用锁定 Python 3.13 标准库实现离线单文件可交互演化图生成器：只从已发布 DuckDB M3 六张正式表读取指定 `evolution_run_id`（缺字段以 `MISSING_PROVENANCE_FIELD` 退码 `1` 原子失败），输出带 `default-src 'none'` CSP、常驻置顶 `#ensabc-disclaimer-banner`、良构带键内联 `<svg id="ensabc-evolution-svg">`、规范 `<script id="ensabc-evolution-provenance">`、原生 ES6 悬停/点击/键盘溯源抽屉 `#ensabc-inspector-drawer` 与静态 `<details id="ensabc-provenance-ledger">` 的单文件 `.html`（依赖 T030）
- [X] T038 [US5] 在 `validation/m3/verify_visualization_traceability.py` 实现六重自动溯源核验（CSP 与零外链扫描、SVG 严格 XML 与零孤立图元、SVG↔JSON 双向双射、HTML↔DuckDB 六表 100% 逐字段真值核对、免责横幅与违禁宣称审查）及 `--self-check` 10 类篡改反例自检；渲染并核验 `visualization/real-8run-main.html` 与覆盖缺口断链/并列歧义/复合分裂合并的 `visualization/synthetic-showcase.html`（依赖 T036、T037）
- [X] T039 [P] [US5] 在 `validation/m3/verify_minimal_runtime.sh` 与 `validation/m3/minimal_runtime.py` 实现 `--network none` 断网最小容器验证（独立目录 `/tmp/ensabc-m3-minimal-runtime`）：验证容器内 Python/R/CDO/wgrib 等 8 类参考工具全部缺席，仅凭 `m3_runner` 与 `DuckDB CLI + DuckOMO + ensabc` 从本地冻结 OM 完成 8 批演化 11 表发布与同会话错误恢复，并在宿主机与常规环境逐字段核对，写出 `minimal-runtime.json`（依赖 T021、T030）
- [X] T040 [P] [US5] 在 `validation/m3/measure_pipeline.py` 使用 `Popen + os.wait4`（`REPEATS = 3`）测量真实 8 批演化全流程七个环节（OM 读取前缀、单批聚类+离散前缀、M2 主干匹配前缀、M3 演化计算前缀、11 表原子发布全流程、`m3_runner` 独立整链、交互 HTML 渲染与核验）的墙钟时间与累积子进程峰值 RSS，如实标注单进程峰值内存不可拆分边界并写出 `measurements.json`（依赖 T030、T038）
- [X] T041 [P] [US5] 在 `validation/m3/run_m1_regression.py` 与 `validation/m3/run_m2_regression.py` 实现对完整 `validation/m1/run_m1_gate.py` 与完整 `validation/m2/run_m2_gate.py` 的串行回归封装（严禁两者并发触碰 `/tmp/ensabc-m1-extension-target`），复制并核验两套门禁的 `report.json`、`traceability.json`、`report-audit.json` 全部 `passed`（依赖 T030）
- [X] T042 [US5] 在 `validation/m3/reproduce_independently.py` 实现父模式、外部 `--reproduction-child` 子模式与 `--self-check`：在全新 `--no-hardlinks` clone 中叠加有效源码清单、从零重建 Rust 二进制、仅复用显式缓存与冻结 OM 样本重跑 core/SQL/敏感性/可视化核验子集，校验真实 `run_key[:12]:` 前缀后按出生节点键核对全部节点、主/次边、事件、三族指标与可视化溯源清单 100% 语义一致，写出 `independent-reproduction.json`（依赖 T035、T038）
- [X] T043 [US5] 在 `validation/m3/traceability.py` 实现覆盖 `FR-001`–`FR-018`（18 项）与 `SC-001`–`SC-008`（8 项）的可执行断言矩阵：独立重算关键指标与不变量、绑定多文件 `evidence_refs`（相对路径 + SHA-256），未执行前置输出 `not_run`，写出 `traceability.json`（依赖 T035、T038–T042）
- [X] T044 [US5] 在 `validation/m3/verify_report.py` 实现只读无循环独立审计器与 `--self-check`：校验 `REQUIRED_STEPS` 全部退码为 `0`、拦截 `../` 越界引用、重算全部 `evidence_refs` SHA-256、只读重跑 `TraceabilityEvaluator` 与 HTML 溯源核验、运行 T036 全部坏证据反例拦截，写出 `report-audit.json`（依赖 T036、T043）
- [X] T045 [US5] 在 `validation/m3/run_m3_gate.py` 实现 `--run-id ID [--cache-dir DIR]` 全流程门禁编排（环境核验 → 扩展与 runner 构建 → OM 样本准备与三路径核验 → Core 验证 → SQL 合成与真实双版本验证 → 真实案例/扰动/敏感性 → 离线交互 HTML 渲染与六重核验 → 断网最小容器 → 七环节测量 → M1 完整回归 → M2 完整回归 → 全新 clone 独立复现 → 需求追溯 → 汇总 `report.json` → 独立审计 `report-audit.json`），拒绝覆盖已有 `run-id` 目录（依赖 T044）

**Checkpoint**: 可追溯可视化与完整门禁工具链全部就绪，具备正式全量门禁执行条件。

---

## Phase 8: Polish & Cross-Cutting Concerns（文档对齐、正式门禁与收口）

**Purpose**: 在最终有效源码基线上跑通全部自检与正式完整门禁，校准复跑指南并更新项目路线图与规格状态。

- [X] T046 [P] 对照最终实现的 CLI 参数、SQL 表字段与产物文件名校准 `specs/004-m3-scenario-evolution/quickstart.md`、`specs/004-m3-scenario-evolution/contracts/validation-evidence.md` 与 `validation/m3/README.md`（依赖 T045）
- [X] T047 执行 `cargo fmt --check`、`cargo clippy --locked --all-targets -- -D warnings`、`cargo test --locked`、全部 M3 自检套件（`test_sql_history.py`、`test_stability.py`、`test_evidence_and_viz.py`、`verify_visualization_traceability.py --self-check`、`reproduce_independently.py --self-check`、`verify_report.py --self-check`）及全新 `--run-id` 的完整 `validation/m3/run_m3_gate.py`（依赖 T045、T046）
- [X] T048 独立运行 `validation/m3/traceability.py` 与 `validation/m3/verify_report.py` 复核 T047 的正式门禁目录，确认 `18/18 FR`、`8/8 SC`、M1 回归、M2 回归、`traceability.json`、`report.json` 与 `report-audit.json` 全部 `passed` 且 `findings == []`（依赖 T047）
- [X] T049 根据通过审计的正式门禁证据更新 `README.md`、`docs/ROADMAP.md`、`specs/004-m3-scenario-evolution/spec.md`、`specs/004-m3-scenario-evolution/plan.md` 与 `specs/004-m3-scenario-evolution/quickstart.md` 的完成状态与复跑说明，并核查 `git status` 与 `git diff` 确认零派生气象数据或运行产物入库（依赖 T048）

---

## Dependencies & Execution Order

### Phase & User Story Dependency Graph

```text
Phase 1: Setup (T001–T004)
          │
          ▼
Phase 2: Foundational (T005–T008)
          │
          ▼
Phase 3: US1 多对多拓扑与五类事件 [P1 MVP] (T009–T016)
          │
          ▼
Phase 4: US2 三族正交指标与整链汇总 [P1] (T017–T023)
          │
          ▼
Phase 5: US3 DuckDB 扩展与 11 表不可变历史 [P1] (T024–T030)
          ├───────────────────────────────────┐
          ▼                                   ▼
Phase 6: US4 真实案例/扰动/敏感性 [P2]     Phase 7: US5 可视化/最小容器/测量/双回归 [P2]
         (T031–T035)                         (T036–T041 可与 US4 并行推进；T042–T045 汇总)
          └───────────────┬───────────────────┘
                          ▼
Phase 8: Polish & Full Gate Verification (T046–T049)
```

### 关键顺序与并发约束

| 工作块 | 最早可启动前置 | 必须串行执行的约束点 |
| --- | --- | --- |
| US1 fixtures 与 `oracle.py`（`T009`, `T010`） | Phase 2 完成 | 与 `T011` 不同文件可并行；`T012 → T013 → T014` 同写 Rust core 必须串行 |
| US2 正交用例与 `oracle.py` 扩展（`T017`, `T018`） | `T016` 完成 | `T020 → T021` 同写 `crates/ensabc-core/src/evolution.rs` 必须串行 |
| US3 SQL DDL 与 C API 扩展（`T024`, `T025`, `T026`） | `T023` 完成 | `schema.sql`（`T026`）与 `evolve_ext.rs`（`T025`）不同文件可并行；`T027` 依赖两者汇合 |
| US4 稳定性证书与敏感性（`T031`–`T035`） | `T031` 可在 `T023` 后先行；`T032` 依赖 `T030` | `T032 → T033 → T034` 递进修改 `run_case_and_sensitivity.py`，保持串行 |
| US5 可视化、最小容器、测量与双回归（`T036`–`T041`） | `T030` 完成即可启动（无需等待 US4 跑完） | **严禁并行执行 `run_m1_regression.py` 与 `run_m2_regression.py`**（共享 `/tmp/ensabc-m1-extension-target`）；`measure_pipeline.py` 测量期间不得并行跑重负载编译或回归以免污染延迟与 RSS |
| US5 独立复现、追溯与总门禁（`T042`–`T045`） | `T035` 与 `T038`–`T041` 全部完成 | `T042 → T043 → T044 → T045` 按无循环证据链严格顺序完成 |

---

## Parallel Example: User Story 3 & User Story 5

```bash
# 示例 1：US2 验收通过（T023）后，在 US3 中并行推进 SQL 测试、C API 适配器与表结构 DDL（互不重叠文件）：
Task T024: "在 validation/m3/test_sql_history.py 编写 C API 与 SQL 历史契约单元测试"
Task T025: "在 crates/ensabc-duckdb/src/evolve_ext.rs 实现 ens_fsum 与 ens_evolve_scenarios"
Task T026: "在 validation/m3/sql/schema.sql 定义六张新增 M3 正式历史表 DDL"

# 示例 2：US3 历史表验收通过（T030）后，并行推进 US4 敏感性开发与 US5 独立验证模块开发（不同脚本文件）：
Task T032: "在 validation/m3/run_case_and_sensitivity.py 实现真实 8 批主配置案例与门槛扫描"
Task T037: "在 validation/m3/render_evolution_html.py 实现离线单文件可交互演化图生成器"
Task T039: "在 validation/m3/verify_minimal_runtime.sh 实现 --network none 最小容器验证"
```

---

## Requirement Coverage（生成期静态覆盖矩阵）

正式运行期可执行断言归口在 `T043`（`validation/m3/traceability.py`）与 `T044`（`validation/m3/verify_report.py`）：

| Requirement / Criterion | 覆盖的实施与验证任务 |
| --- | --- |
| `FR-001` | T005, T027–T030, T032 |
| `FR-002` | T006–T007, T009–T016, T027, T029 |
| `FR-003` | T009–T016, T021, T026–T030 |
| `FR-004` | T006, T009–T016 |
| `FR-005` | T017–T023, T025, T027 |
| `FR-006` | T005, T017–T023, T032 |
| `FR-007` | T018–T023, T025–T030 |
| `FR-008` | T001, T007, T013, T026–T027, T034, T037–T038 |
| `FR-009` | T004, T024, T027, T029–T030, T042 |
| `FR-010` | T024, T026–T030 |
| `FR-011` | T006–T008, T024–T025, T029–T030, T041 |
| `FR-012` | T007–T009, T011–T012, T015, T024–T025, T029 |
| `FR-013` | T009–T011, T015–T018, T022–T024, T029 |
| `FR-014` | T036–T038, T044 |
| `FR-015` | T028–T030, T032, T034–T035 |
| `FR-016` | T005, T031–T035 |
| `FR-017` | T007, T014, T021, T028, T039 |
| `FR-018` | T001–T003, T040–T049 |
| `SC-001` | T009–T023 |
| `SC-002` | T026–T030, T032, T035 |
| `SC-003` | T017–T023, T034–T035 |
| `SC-004` | T008, T012, T015, T024–T030 |
| `SC-005` | T031–T035 |
| `SC-006` | T036–T038, T044 |
| `SC-007` | T039 |
| `SC-008` | T040–T049 |

---

## Implementation Strategy

### MVP First（先交付核心算法闭环：US1 → US2）

1. 完成 **Phase 1（Setup）** 与 **Phase 2（Foundational）**，锁定参数与 `CoreError <= 112B` 边界。
2. 完成 **Phase 3（US1）**：交付多对多星形拓扑、五类事件分类、算法归因流量与独立穷举 Oracle 核验——此时无需数据库或天气样本即可独立演示和验证多对多演化核心规则。
3. 紧接完成 **Phase 4（US2）**：补齐三族正交指标、`m3.uncertainty.v1` 高不确定性判定与纯 Rust 整链汇总，验证形态与占比变化的严格正交分离。

### Incremental Delivery（数据库持久化 → 真实案例敏感性 → 可追溯可视化与双回归门禁）

1. **Phase 5（US3）**：打通 DuckDB `ens_evolve_scenarios` / `ens_fsum` 与 11 表原子历史发布，在真实 8 批 OM 样本上验证与 M2 主干和持续身份 100% 对齐。
2. **Phase 6（US4）**：在冻结真实样本上运行两层扰动（含四条件稳定性证书）、18 组门槛扫描与 4 轴聚类配置敏感性，形成完整案例证据与局限边界。
3. **Phase 7 & Phase 8（US5 + Polish）**：交付离线单文件可交互溯源演化图与六重自动核验，完成断网最小容器、七环节资源测量、M1+M2 串行完整回归、全新 clone 独立复现与无循环审计，全部通过后正式收口 M3。
