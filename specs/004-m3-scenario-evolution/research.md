# M3 技术研究与决策

日期：2026-11-25。范围为技术规划（Phase 0），不是 M3 生产实现或正式门禁验收。四个并行只读 Swarm 调研成员（`core-math-auditor`、`duckdb-sql-auditor`、`case-sensitivity-auditor`、`viz-gate-auditor`）分别完成了核心数学与次级边定理证明、DuckDB C API 扩展与不可变历史表设计、WeatherBench2 真实 8 批案例逐跃迁与敏感性标定、以及零依赖离线可交互溯源可视化与双回归门禁架构调查。项目仓库在调查阶段保持只读，以下汇总全部已解决的技术决策（全部待明确技术问题已闭环）。

---

## 1. 多对多演化拓扑与流向解释（FR-002、FR-003、FR-004、FR-008）

### D1：基于最大基数增广路定理的主干 + 单近邻次级边模型（`nearest_feasible_v1`）

- **Decision**：在 100% 保留 M2 门槛内字典序全局最优一对一主干匹配 $M^*$（先最大关联数 $|M|$、再最小 Shewchuk 精确距离和 $\Sigma(M)$、按 `mean_features` 规范键决胜）的前提下，对每个“门槛内存在可行候选但未入选全局最优”（`UnmatchedScenario.reason: String == "not_selected_in_global_optimum"`）的节点采用**单近邻可行次级归属规则**（`secondary_rule = "nearest_feasible_v1"`）：
  - 对后批竞争落选节点 $j \in U_{\text{curr}}^{\text{comp}}$，在可行前驱集 $F_{\text{in}}(j) = \{i : d_{i,j} \le \tau\} \ne \emptyset$ 中选最小距离 $d_{(1)}(j) = \min_{i \in F_{\text{in}}(j)} d_{i,j}$；若存在精确距离并列（$|T_{\text{in}}(j)| > 1$），按前批 `canonical_scenario_order` 最小序号决胜，生成唯一一条 `secondary_split` 边 $(i^*(j), j)$，并将该边标为 `ambiguous_tie = true`、`secondary_margin = 0.0`。
  - 对前批竞争落选节点 $i \in U_{\text{prev}}^{\text{comp}}$，对称地在可行后继集 $F_{\text{out}}(i) = \{j : d_{i,j} \le \tau\} \ne \emptyset$ 中取最小距离（并列按后批 `canonical_scenario_order` 最小序号决胜），生成唯一一条 `secondary_merge` 边 $(i, j^*(i))$。
  - 对门槛内度为 0（`UnmatchedScenario.reason: String == "no_candidate_within_threshold"`）的节点 $U^{\text{none}}$，不建立任何演化边，分别归类为 `disappearance`（前批）与 `emergence`（后批）。
- **Rationale**：
  - **结构定理保证零矛盾星形分解（Berge 增广路定理推论）**：
    1. *未匹配侧零可行边定理*：因 $M^*$ 在 $G_\tau$ 中达到最大基数，任何 $i \notin V_{\text{prev}}(M^*)$ 与 $j \notin V_{\text{curr}}(M^*)$ 必有 $d_{i,j} > \tau$（否则 $M^* \cup \{(i,j)\}$ 基数为 $|M^*|+1$）。故 $E_{\text{split}}$ 的起点必在 $V_{\text{prev}}(M^*)$，$E_{\text{merge}}$ 的终点必在 $V_{\text{curr}}(M^*)$，三类边集 $E_{\text{primary}}, E_{\text{split}}, E_{\text{merge}}$ 两两互不相交，绝无平行重边。
    2. *单条主干边分裂与合并互斥定理*：对任一主干边 $(i, j) \in M^*$，不可能同时存在 $c' \in U_{\text{curr}}^{\text{comp}}$ 使 $d_{i,c'} \le \tau$ 且存在 $p' \in U_{\text{prev}}^{\text{comp}}$ 使 $d_{p',j} \le \tau$——否则 $p' - j - i - c'$ 构成长度为 3 的增广路，与 $|M^*|$ 最大矛盾。
    3. *星形连通分量唯一分类推论*：结合单近邻次级边使每个 $u \in U^{\text{comp}}$ 恰有度 1，演化图 $G_{\text{evol}}$ 的每个连通分量 $C = (P_C, Q_C)$ 严格互斥地落入五类基础事件之一：`continuation`（$1 \to 1$）、`split`（$1 \to 1+k$）、`merge`（$1+k \to 1$）、`disappearance`（$1 \to 0$）、`emergence`（$0 \to 1$）。即使同一相邻跃迁中在不同主干分支上同时发生分裂与合并（含交叉/菱形可行图），也绝不会对任何节点产生自相矛盾的角色归属（完美解决 Edge Case 5 与 FR-003）。
  - **真实 WB2 实证优势**：在 `b0→b1` 中，未入选节点 `b0.c2`（占比 `0.24`）到 `b1.c1`（`0.4546 K`，占比从 `0.42` 增至 `0.62`）和 `b1.c2`（`0.5321 K`，占比从 `0.34` 降至 `0.22`）均在 $\tau = 0.6639\text{ K}$ 内。`nearest_feasible_v1` 将 `{b0.c0, b0.c2} → b1.c1`（`0.42 + 0.24 = 0.66 → 0.62`）准确识别为合并，并用 `secondary_margin = 0.0774 K`（`0.1166S`）暴露对 `b1.c2` 的竞争余量；而全可行连边会把 `b0.c2` 同时连向 `b1.c1` 和 `b1.c2`，造成伪分裂+伪合并纠缠。
- **Alternatives considered**：
  - *全门槛内可行二部图（all-feasible-within-$\tau$）*：在 $c \ge 1.25$ 时把已一对一延续的独立主干粘连成稠密交叉网，破坏星形可解释性且丧失次级竞争余量定义。
  - *次级容差带多近邻（$d \le d_{(1)} + \delta$）*：额外引入新的未标定超参数 $\delta$，且在容差内触发多出度时破坏星形互斥性质。

### D2：星形最小质量按比例算法流向归因（`star_min_mass_proportional_v1`）

- **Decision**：采用仅依赖星形拓扑与节点 `member_fraction` 的算法流向归因公式（`flow_rule = "star_min_mass_proportional_v1"`），同步输出几何贴近强度 `edge_strength` 与显式节点未归因残差 `unattributed_outflow_fraction` / `unattributed_inflow_fraction`：
  - `edge_strength`：$\tau > 0$ 时为 $\operatorname{clamp}_{[0,1]}(1.0 \ominus (d_{i,j} \oslash \tau))$，$\tau = 0$ 时为 `1.0`；
  - 对每个连通分量 $C = (P_C, Q_C)$，记前驱占比精确展开和 $P = \operatorname{fsum}_{i \in P_C} p_i$，后继占比精确展开和 $Q = \operatorname{fsum}_{j \in Q_C} q_j$：
    - `continuation`（$\{i\} \to \{j\}$）：$f(i,j) = \min(p_i, q_j)$；
    - `split`（$\{i\} \to Q_C, |Q_C| \ge 2$）：若精确比较 $\sum p \ge \sum q$，则 $f(i,j) = q_j$；否则 $f(i,j) = \min\bigl(q_j, (p_i \otimes q_j) \oslash Q\bigr)$；
    - `merge`（$P_C \to \{j\}, |P_C| \ge 2$）：若精确比较 $\sum q \ge \sum p$，则 $f(i,j) = p_i$；否则 $f(i,j) = \min\bigl(p_i, (q_j \otimes p_i) \oslash P\bigr)$；
    - `disappearance` / `emergence`：无边，整块占比计入 `unattributed_outflow` / `unattributed_inflow`。
- **Rationale**：
  - 严格满足 $0 < f(i,j) \le \min(p_i, q_j)$，在离线交互流图中每个节点的入边/出边带宽之和绝不越出节点自身高度；
  - **`SC-003` 正交性保证**：`attributed_flow` 完全不依赖连续距离 $d_{i,j}$（仅依赖拓扑连通分量与占比），因此在“纯形态漂移（拓扑与占比不变）”用例下，所有占比与流量指标精确恒定、零交叉污染；
  - 未平衡的跨批占比涨跌（例如 WB2 `b3→b4` 分裂 `0.32 → 0.42 + 0.16` 净增 `+0.26`）通过 `unattributed_inflow_fraction` 诚实暴露，不强行伪造跨无关分支的“质量守恒搬运”，符合 FR-008 非物理成员迁移原则。
- **Alternatives considered**：
  - *按距离倒数加权分摊占比*：当物理均值场微移而成员占比不变时，`attributed_flow` 会随 $d_{i,j}$ 改变，破坏形态变化与占比变化的正交性（违反 US2 / SC-003）。
  - *全局最优传输（Sinkhorn / EMD）*：迫使无关主干分支之间产生虚假跨流，且违反已确认的 Q1 决定。

---

## 2. 三类正交演化指标与高不确定性标定（FR-005–FR-007）

### D3：形态变化、占比变化与匹配不确定性的正交公式体系

- **Decision**：将演化指标严格划分为互不混杂的三族，全部多元素求和统一采用单层平铺 Shewchuk `Expansion` 一次舍入（与 Python `math.fsum` / `Fraction` 逐位一致）：
  1. **形态变化族（Morphology）**：单边加权 RMSE $d_{i,j}$ 与相对离散比 $d_{i,j} / S$；跃迁级主干总距离、主干均值/最大距离、全边最大距离；分支级沿主干延续链的累计距离 $\operatorname{fsum}_t d_t$、累计相对离散漂移 $\operatorname{fsum}_t (d_t / S_t)$、单步峰值及首尾端到端距离 $d(\mu_{\text{birth}}, \mu_{\text{end}})$。
  2. **成员占比变化族（Member Fraction）**：单边 `edge_delta_fraction`（$q_j \ominus p_i$）与 `attributed_flow`；节点主干净变 `primary_delta_fraction` 与未归因残差；**跃迁级主干分支总变差重分配度** $\text{TV}_{\text{branch}} = 0.5 \otimes \operatorname{fsum}_{b \in B_{\text{prev}} \cup B_{\text{curr}}} |q(b) \ominus p(b)| \in [0, 1]$（未在当批出现的分支占比记 0）及连通分量净残差 $\text{TV}_{\text{component}} = 0.5 \otimes \operatorname{fsum}_C |\Delta f_C|$；分支级初末占比、净变化 $f_{\text{end}} \ominus f_{\text{birth}}$、累计绝对变动 $\operatorname{fsum}_t |f_t \ominus f_{t-1}|$ 与极值。
  3. **匹配与演化不确定性族（Uncertainty）**：
     - *有效门槛余量（Active Threshold Margin）*：由定理 4，已可行但未入选 $E_{\text{evol}}$ 的交叉格 $(i,j) \in E_\tau \setminus E_{\text{evol}}$ 向上越过 $\tau$ 不会改变 $M^*$ 或次级边，因此单边门槛余量为 $\tau \ominus d_e \ge 0$，`no_candidate` 节点门槛余量为 $d_{\min}(u) \ominus \tau > 0$，跃迁有效门槛余量为 $\min_{(i,j) \in E_{\text{evol}} \cup \{d > \tau\}} |d_{i,j} \ominus \tau|$（同时保留全矩阵 `raw_matrix_threshold_margin` 与 M2 对照）；
     - *主干竞争/次级晋升余量（Optimality Margin）*：主干边沿用 M2 单边禁用重求差距（`Tied` 为 `0.0`，`SameCount` 为 $\operatorname{round}(\Sigma' - \Sigma^*)$，`CountDrop` 为无同基数替代）；次级边依据**定理 3（次级节点强制入主干必保最大基数 $|M^*|$）**，由 `solve_dp` 强制匹配该节点重求得到精确晋升代价差 $\Delta\Sigma_{\text{prom}}(u) = \operatorname{round}(\Sigma_{\text{force}}(u) - \Sigma^*) \ge 0$；
     - *次级竞争余量（Secondary Margin）*：当 $|F(u)| \ge 2$ 时为次近与最近可行候选的距离差 $d_{(2)}(u) \ominus d_{(1)}(u) \ge 0$（$|F(u)| = 1$ 时标为无次级竞争者）；
     - *精确并列标记（`ambiguous_tie`）*：主干 `Tied`（Rust `MatchResult.optima_count > 1` / SQL `optimal_solution_count > 1` 且可替换）或次级 `matched_in_some_optimum || secondary_margin == 0.0`。
- **Rationale**：
  - $\text{TV}_{\text{branch}}$ 在纯形态漂移下精确为 `0.0`，在两分支纯占比转移 $\pm \delta$ 下精确等于 $|\delta|$；形态距离完全不读取 `member_fraction`，从而在数学定义层 100% 满足 `SC-003`。
  - 区分 `active_threshold_margin` 与 `raw_matrix_threshold_margin` 消除了 WB2 `b2→b3`（`d(1,1)=0.9798S`）、`b4→b5`（`d(2,0)=0.9825S`）、`b5→b6`（`d(1,0)=0.9638S`）上“无关非选中可行格贴近 $\tau$”造成的假阳性告警。
- **Alternatives considered**：
  - *将形态距离与占比差加权合成为综合演化指数*：无法解释变化来源，且引入任意量纲混合权重，直接违反 US2。

### D4：冻结的高不确定性判定规则（`m3.uncertainty.v1`）

- **Decision**：在比例模式下令参考尺度 $R = S$（绝对模式下 $\tau > 0$ 时 $R = \tau$），冻结三个通道统一采用无量纲相对余量比例 **`0.10`（10%）**，以纯乘法比较避免除零：
  - `high_uncertainty = true` 当且仅当满足任一条件：
    1. `ambiguous_tie == true`；
    2. $\tau == 0.0$（零门槛精确重合边界）或有效门槛余量 $\Delta_\tau \le 0.10 \otimes R$（含 $d == \tau$ 边界）；
    3. 存在同基数主干替代/次级晋升余量且 $\Delta\Sigma \le 0.10 \otimes R$；
    4. 存在次级竞争候选且 $\Delta d_{\text{sec}} \le 0.10 \otimes R$。
  - 同时输出触发原因数组 `uncertainty_reasons`（`ambiguous_tie` / `near_threshold` / `near_primary_switch` / `near_secondary_switch`）及全部原始连续余量。
- **Rationale**：
  - **理论与实证双重锚定（零事后挑参）**：在 M2 扰动幅度 $\varepsilon = 0.01 S$ 下，主干切换三角不等式上界 $4 q^* \varepsilon$ 对 $q^* \in \{2, 3\}$ 恰好落在 $[0.08 S, 0.12 S]$，中点即为 **`0.10 S`**；而在 WB2 真实样本中，敏感跃迁的主干余量 `{0.0372S, 0.0489S}` 与稳健跃迁 `{0.2026S..0.8219S}` 之间存在 **4.1 倍自然间隙**，敏感门槛余量 `{0.0279S, 0.0525S}` 与稳健余量 `>= 0.1605S` 之间存在 **3.1 倍自然间隙**。统一取 `0.10` 无需分通道凑数，在主案例中精准标出 `b0→b1`（近门槛新生）、`b3→b4`（近并列分裂）和 `b6→b7`（近门槛延续 + 近并列分裂）3 个边界敏感跃迁。
- **Alternatives considered**：
  - *三个通道分别设置不同经验阈值（如 `0.05 / 0.08 / 0.12`）*：增加自由度且有针对 8 批样本过拟合之嫌，拒绝。

---

## 3. Rust Core 实现边界、DuckDB 扩展与历史持久化（FR-009–FR-012、FR-017）

### D5：`ensabc-core` 增量模块与 `CoreError` 112 字节预算守卫

- **Decision**：保持 `crates/ensabc-core` 零新增生产依赖，保持 M1（`cluster`）与 M2（`ensemble_dispersion`, `match_scenarios`, `match_scenarios_scaled`）签名和行为 100% 不变（尤其不在 `match_scenarios` 中追加占比校验，以免影响传占位占比的既有 M2 调用）。新增 `src/evolution.rs` 与 `examples/m3_runner.rs`：
  - 暴露纯函数 `evolve_scenarios(&EvolutionInput)`、`evolve_scenarios_scaled(&ScaledEvolutionInput)` 与纯内存整链编排函数 `evolve_chain(&EvolutionChainInput)`；
  - `EvolutionInput` 支持可选 `backbone: Option<BackboneAssertion>`：传入 `Some` 时在 core 内严格复核维度、`cluster_id`、加权 RMSE 矩阵、可行性与全局最优目标一致性，不一致报 `BackboneInconsistency` 或 `InvalidPrimaryLink`（满足 Edge Case 1 与 FR-011）；传入 `None` 时直接调用 `match_scenarios`。
  - **错误结构体大小硬约束**：当前 64 位下 `size_of::<CoreError>() == 112`（`Result<T, CoreError>` 为 `120B`），距离 `clippy::result_large_err` 默认上限 `128B` 仅剩 `8B`。M3 仅在 1 字节枚举 `CoreErrorCode` 中新增单元变体（`ZeroMemberScenario`、`InvalidMemberFraction`、`InvalidFractionSum`、`BackboneInconsistency`、`InvalidPrimaryLink`、`InvalidEvolutionConfig`），复用已有 `prev_index: Option<u32>`、`curr_index: Option<u32>` 与 `feature_index: Option<usize>`，确保 `CoreError` 维持 `112B`、零 Clippy 警告。
  - **成员占比校验口径**：要求 `member_count >= 1`、$0 < \text{member\_fraction} \le 1$ 有限、同批 $|\operatorname{fsum}(\text{member\_fraction}) - 1.0| \le \text{fraction\_sum\_tolerance}$（默认 `1e-9`），不强制 `member_fraction == member_count / sum(member_count)`，从而支持连续占比合成正交测试（FR-013 / SC-003）。
- **Rationale**：单一维护校验与数学逻辑，确保 `m3_runner` 与 `ens_evolve_scenarios` 逐位同源，同时避免破坏 M2 审查确立的 `CoreError` 内存布局。
- **Alternatives considered**：
  - *用 `Box<CoreError>` 扩容错误结构体*：会改变 M1/M2 已发布的公共返回签名 `Result<..., CoreError>`，违反 FR-011。

### D6：DuckDB 标量函数 `ens_evolve_scenarios`、`ens_fsum` 与 6 张不可变历史表

- **Decision**：
  - 在 `crates/ensabc-duckdb` 中保留 `ens_m0_echo`、`ens_cluster`、`ens_dispersion`、`ens_match_scenarios` 完全不变，新增 `src/evolve_ext.rs` 注册两个 C API 无状态标量函数（均开启 `duckdb_scalar_function_set_special_handling` 显式拒绝 `NULL`，遵循先全 chunk 计算成功再一次性 `reserve_list` + 写向量的安全模式）：
    1. `ens_evolve_scenarios(prev_scenarios, curr_scenarios, weights, backbone_match, config) -> STRUCT(...)`：直接复用 M2 `ens_match_scenarios` 的返回类型作为第 4 参数 `backbone_match`，零结构体拆包开销；所有输出字段采用非 `NULL` 有限值 + `has_* BOOLEAN` 约定；
    2. `ens_fsum(values DOUBLE[]) -> DOUBLE`：将 `ensabc_core::fsum::Expansion` 暴露给 SQL，使纯 SQL 的分支级与全链级 `GROUP BY` 聚合（`ens_fsum(list(x ORDER BY prev_ordinal))`）与 Rust `evolve_chain` 及 Python `math.fsum` 达到 **0 ULP** 一致。
  - **不可变演化历史表（M2 五表原样保留 + 新增 M3 六表）**：
    - 保留 `tracking_runs`、`replay_batches`、`scenario_nodes`、`pair_comparisons`、`scenario_links` 零修改；
    - 新增 `evolution_runs`、`evolution_nodes`、`evolution_transitions`、`evolution_edges`、`evolution_events`、`evolution_branch_summaries`，通过 `tracking_run_key REFERENCES tracking_runs(run_key)` 关联上游 M2 版本；
    - **持续身份逐字对齐规则**：`evolution_nodes.branch_id` 直接继承 M2 `scenario_nodes.branch_id`（`<tracking_run_key[:12]>:b<ord:02d>:k<rank:02d>`），不替换为 `evolution_run_id` 前缀，保证与 M2 身份链 100% 逐字符相等（US3.1）；
    - **缺口跃迁显式建行**：`evolution_transitions` 为全部 `B - 1` 个相邻计划对建行，缺批/单批算法失败处记 `transition_status = 'gap'`、`chain_broken = true`，并在 `evolution_events` 中仅生成 `'gap_interrupted'` / `'gap_restarted'`，与正常 `'disappearance'` / `'emergence'` 严格互斥（Edge Case 7）；
    - **两阶段暂存 + 单事务发布 + 8 条不动点守卫（`E1`–`E8`）**：阶段 A 在 `TEMP` 表完成单批白名单容错、匹配、演化与汇总；阶段 B 在单个 `BEGIN ... COMMIT` 事务内执行 `EVOLUTION_RUN_VERSION_CONFLICT` 幂等/冲突检查、写入六表，并在提交前执行 `E1`–`E8` 守卫（M2 节点双向 `EXCEPT` 为空、M2 主干边双向 `EXCEPT` 为空、次级边可行性与落选角色唯一、零跨缺口边与分支连续性、缺口与新生消失互斥、节点度数守恒、分支生命周期守恒、全有限值与免责声明存在），任一失败立即 `error()` 回滚至 0 残留。
- **Rationale**：100% 依赖纯 DuckDB SQL + 原生扩展即可在 `--network none` 且无 Python 的最小容器中跑通从本地 `.om` 读取到 11 表（5 M2 + 6 M3）原子发布与同会话恢复的全链路（FR-017 / SC-007）。
- **Alternatives considered**：
  - *修改或扩充 M2 原有五张表列定义*：侵入 M2 既有 schema 并可能引起回归耦合；分表外键叠加既保持 M2 100% 不变，又支持同一 `tracking_run_key` 挂载多组演化规则对照版本。
  - *在 SQL 中用内置 `SUM(DOUBLE)` 汇总分支指标*：DuckDB `SUM` 为普通浮点累加，在多步链上可能与 Shewchuk `fsum` 产生 1 ulp 偏差，增加 25 行 `ens_fsum` 彻底根除差异。

---

## 4. 真实案例扰动、配置敏感性与稳定性证书（FR-015、FR-016）

### D7：两层三档扰动方案与四条件演化稳定性证书

- **Decision**：在 `validation/m3/fixtures/evolution_experiment_config.json` 中事先冻结全部扰动与敏感性参数：
  - **Level 1a（演化输入层形态微扰）**：保持 $S, \tau$ 与 `member_fraction` 不变，对各情景 `mean_features` 施加加权 RMS 范数为 $\varepsilon \cdot S$ 的随机方向微扰，$\varepsilon \in \{0.005, 0.01, 0.02\}$，每对批次 100 个固定种子（`PCG64(20261029)`）；
  - **Level 1b（演化输入层纯占比微扰）**：保持 `mean_features`、$S, \tau$ 不变，对 WB2 各批施加 $\pm 1$ 成员（$\pm 0.02$ 占比）成对重分配，实证检验主干/次级拓扑与事件分类 **0 翻转**；
  - **Level 2（逐成员原始场全流程微扰）**：对 $8 \times 50 \times 9$ 成员场加独立 $\mathcal{N}(0, \sigma^2)$，$\sigma \in \{0.0, 0.001, 0.01\}\text{ K}$，每档 20 个固定种子（`PCG64(4242)`），按成员最大重叠映射基线情景（并列单独记 `evaluation_mapping_ambiguous`）；
  - **M3 四条件保守有理区间稳定性证书**：在向外舍入 `Fraction` 区间下同时满足：
    1. *有效门槛边界不变*：对所有 $(i,j) \in E_{\text{evol}} \cup \{(i,j) : d_{i,j} > \tau\}$，$\operatorname{lower}(|d_{i,j} - \tau|) > \operatorname{upper}(2\varepsilon_{\text{abs}} + c\Delta S)$；
    2. *主干最优组合不变*：对所有 `SameCount` 主干边，$\operatorname{lower}(\Delta\Sigma_e) > \operatorname{upper}(4 q^* \varepsilon_{\text{abs}})$；
    3. *无主干精确并列*：`MatchResult.optima_count == 1`（映射为 SQL `optimal_solution_count == 1`）；
    4. *次级单近邻归属不变*：无次级距离并列，且对每个 $|F(u)| \ge 2$ 的 $u \in U^{\text{comp}}$，$\operatorname{lower}(d_{(2)}(u) - d_{(1)}(u)) > \operatorname{upper}(4\varepsilon_{\text{abs}})$。
    满足证书的受扰对要求映射后主干边、次级边与事件分类翻转数 **严格为 0**；未获证书的接近边界受扰对完整记录发生的事件翻转（如 `split ↔ emergence`、主/次子分支互换）及基线余量。
- **Rationale**：定理 4 严格证明仅需守卫 $E_{\text{evol}} \cup \{d > \tau\}$ 即可保证 $E_{\text{evol}} \subseteq E'_{\tau'} \subseteq E_\tau$，结合条件 2–4 可完整推出 $G'_{\text{evol}} = G_{\text{evol}}$；在 $\varepsilon = 0.01 S$ 下 WB2 共有 **5/7** 对相邻跃迁（`b0→b1, b1→b2, b2→b3, b4→b5, b5→b6`）获得数学稳定性证明，仅 `b3→b4` 与 `b6→b7` 两个真实近并列分裂留作诚实记录的边界翻转样本。
- **Alternatives considered**：
  - *仅沿用 M2 三条件证书*：无法约束次级分裂/合并边在多个可行候选间的归属切换（例如合成多候选合并用例），补齐条件 4 才对多对多演化完备。

### D8：门槛网格与 M1 实际聚类配置轴敏感性矩阵

- **Decision**：
  - **门槛比例系数扫描（FR-016.2）**：在 `c ∈ {0.5, 0.75, 1.0, 1.25, 1.5, 2.0}` × `k ∈ {2, 3, 4}`（共 18 个独立演化版本）上全量回放。在 `k=3` 下实证覆盖从 `c=0.5` 严重欠连（0 分裂/0 合并、17 消失/17 新生）、`c ∈ [0.75, 1.5]` 核心分裂（`b3→b4`, `b6→b7`）跨 2 倍门槛区间稳健保持、到 `c=2.0` 全 3-to-3 饱和强连（0 分裂/0 合并）的完整谱系，如实暴露最大基数目标在大门槛下掩盖分裂/合并的固有局限。
  - **聚类配置敏感性矩阵（FR-016.3，对齐 M1 实际 `ScenarioConfig` 字段）**：核查 `crates/ensabc-core/src/lib.rs` 与 `preprocess.rs` 确认，M1 实际可配置字段为 `cluster_count`、`projection_dim`（非 `target_dim`）、`initialization`、`seed`、`restarts`、`initial_centers`、`stopping_rule`、`tolerance`（即 `ScenarioConfig.tolerance`，非 `convergence_tolerance`）、`max_iterations`、`rank_threshold`（M1 固定执行总体 z-score 标准化，无 `standardize` 布尔开关，且 M3 不得修改 M1 接口）。因此在主门槛 `c=1.0` 下冻结四组正交聚类对照轴（每组均发布为独立 `evolution_run_id`）：
    1. *情景数轴*：`cluster_count ∈ {2, 3, 4, 5}`（`projection_dim=2, seed=3, restarts=10`）；
    2. *投影维度轴*：`projection_dim ∈ {1, 2, 3, 4}`（`cluster_count=3, seed=3, restarts=10`，在 $N=50, p=9$ 上合法满秩区间内考察降维截断对演化拓扑的影响）；
    3. *随机初始化与重启轴*：`(seed=0, restarts=10)`、`(seed=42, restarts=10)`、`(seed=3, restarts=1)`（`cluster_count=3, projection_dim=2`）；
    4. *单批算法不可计算断链边界轴*：`(cluster_count=3, projection_dim=3, rank_threshold=0.35)` 与 `(cluster_count=8, projection_dim=2, seed=3, restarts=10)`，在真实数据上自然触发单批算法失败白名单（`INSUFFICIENT_RANK`、`AMBIGUOUS_SUBSPACE`、`EMPTY_CLUSTER`、`NOT_CONVERGED`、`INSUFFICIENT_DISTINCT_VECTORS`、`INSUFFICIENT_PROJECTED_VECTORS`、`SVD_NOT_CONVERGED`、`ZERO_DISPERSION`）中的退化单批 `status='failed'` 断链并记录 `gap_interrupted` / `gap_restarted`（不靠人工删表模拟）。
- **Rationale**：100% 忠实于 M1 真实代码契约，不虚构不存在的聚类开关，同时同时覆盖常规参数敏感性与真实单批退化断链行为。
- **Alternatives considered**：
  - *为 M3 在 M1 `ScenarioConfig` 中新增 `standardize: bool`*：会修改 M1 已冻结的公共结构体与 SQL `ens_cluster` 契约，违反 FR-011 与 SC-008，拒绝。

---

## 5. 零依赖离线可交互溯源可视化与双回归门禁（FR-014、FR-018）

### D9：零新增依赖的离线单文件可交互 HTML/SVG 与六重机器核验

- **Decision**：
  - **不新增任何第三方 Python 包或前端框架**：生成器 `validation/m3/render_evolution_html.py` 与核验器 `validation/m3/verify_visualization_traceability.py` 仅使用锁定的 Python `3.13.13` 标准库（`json`, `html`, `html.parser`, `xml.etree.ElementTree`, `hashlib`, `math`, `re`）及隔离调用的 DuckDB CLI `v1.5.5`。
  - **单一事实源与渲染前置门禁**：`render_evolution_html.py` 只从已发布的 DuckDB M3 六张正式历史表读取数据；若目标 `evolution_run_id` 不存在或任一必需溯源字段缺失/非有限，直接以 `MISSING_PROVENANCE_FIELD`（退码 `1`）拒绝且不写半成品文件（满足 Edge Case 10）。
  - **离线单文件 HTML 结构（`m3.visualization.v1`）**：
    1. 顶部 `<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline';">`，物理阻断任何外网脚本、样式、字体或图片请求；
    2. 置顶常驻不可折叠免责横幅 `#ensabc-disclaimer-banner`（声明非物理成员迁移、非物理新生消亡、非校准概率/预报技巧/交易价值）；
    3. 严格良构的内联 `<svg id="ensabc-evolution-svg">`，按批次列排列节点（高度正比于 `member_fraction`）、用实线彩带绘制 `primary` 边、虚线次级描边绘制 `secondary_split`/`secondary_merge` 边、双层高亮描边标注 `high_uncertainty`/`ambiguous_tie`、竖向屏障标注 `missing`/`failed` 断链缺口；所有非装饰图形均带 `data-node-id`、`data-edge-id` 或 `data-batch-ordinal`；
    4. 内嵌规范化溯源清单 `<script id="ensabc-evolution-provenance" type="application/json">` + 原生 ES6 悬停提示与点击/键盘抽屉面板 `#ensabc-inspector-drawer`（展示完整版本、批次、身份、角色、事件、距离、离散比、占比与流量、三类余量、歧义/高不确定性标记与触发规则）+ `<details id="ensabc-provenance-ledger">` 静态兜底核对表。
  - **六重自动核验与 10 类篡改反例自检**：`verify_visualization_traceability.py` 自动执行 ① CSP 与零外链扫描、② SVG 严格 XML 解析与零孤立图形元素检查、③ SVG DOM 与内嵌 JSON 双向双射核对、④ HTML 与 DuckDB 六张正式历史表 100% 逐字段核对、⑤ 免责声明与违禁宣称检查，并通过 `--self-check` 对 10 类恶意篡改（删节点、塞孤立 `<path>`、改距离、**同时篡改 SVG+JSON 企图骗过内部一致性检查**、主次角色互换、跨缺口塞边、注入外链 CDN、隐藏免责声明、删余量字段、错绑 `evolution_run_id`）逐一断言退码为 `1`。
- **Rationale**：既提供流畅直观的多起报演化流图与点选钻取体验，又用“DOM ↔ 内嵌 JSON ↔ DuckDB 正式表”三角闭环杜绝任何静态幻觉或孤立图形。
- **Alternatives considered**：
  - *引入 D3 / Plotly / ECharts 打包脚本*：引入数百 KB 第三方压缩 JS，难以做严格 XML 孤立图元审计，且增加许可证与供应链审计负担。

### D10：M1 + M2 串行双回归、七阶段资源测量与防循环门禁审计

- **Decision**：
  - 建立独立的 `validation/m3/` 验证目录与 `/tmp/ensabc-m3-*` 临时构建/容器命名空间；在 `.gitignore` 中新增 `/artifacts/m3/` 与 `/validation/m3/data/`，严防派生 `.om`、`.duckdb`、`.html` 入库。
  - **M1 与 M2 串行双回归（SC-008）**：因 `validation/m1/run_m1_gate.py` 与 `validation/m2/run_m2_gate.py`（内部第 13 步也会调用 M1 回归）共享 `/tmp/ensabc-m1-extension-target` 构建目录，M3 门禁固定**串行**执行 `run_m1_regression.py` 与 `run_m2_regression.py`，分别核验两者的 `report.json`、`traceability.json`、`report-audit.json` 全部 `passed`。
  - **七阶段资源测量（`measure_pipeline.py`）**：使用 `Popen + os.wait4`（`REPEATS = 3`）测量 ① OM 读取前缀、② 单批聚类+离散前缀、③ M2 主干匹配前缀、④ M3 演化计算前缀（`ens_evolve_scenarios`）、⑤ M2+M3 十一表原子发布全流程、⑥ `m3_runner` 纯 Rust 独立整链、⑦ 离线交互 HTML 渲染与溯源校验的墙钟时间与峰值 RSS，诚实标注单进程内 OS 峰值内存的单调累积边界。
  - **全新 clone 独立复现与升级版防循环审计**：`reproduce_independently.py` 在外部隔离目录从零构建 Rust 产物并重跑核心确定性验证与可视化溯源，验证真实 `run_key[:12]:` 前缀后按 `(birth_ordinal, birth_canonical_rank)` 核对全部主/次边、事件、三族指标与可视化清单 100% 语义一致；`verify_report.py` 支持多证据引用 `evidence_refs` SHA-256 校验、路径穿越拦截、只读重跑 `TraceabilityEvaluator` 与 `--self-check` 坏证据反例拦截。
- **Rationale**：完整延续并强化 M1/M2 已验证的工程治理标准，确保 M3 的每一条结论均可机器复核。
- **Alternatives considered**：
  - *仅抽跑 M2 部分单测代替完整 M2 门禁回归*：不足以支撑 SC-008“M1 与 M2 原有完整验收全部通过（0 退化）”的严格承诺，拒绝。
