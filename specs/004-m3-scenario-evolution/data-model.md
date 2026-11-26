# M3 数据模型

本文件定义 M3 情景演化分析的核心数据结构、持久化历史实体、字段约束、状态迁移与全局不变量。数学公式与稳定性证书见 [numerics.md](contracts/numerics.md)，Rust 核心接口见 [core.md](contracts/core.md)，DuckDB 标量函数与历史表 DDL 见 [sql-history.md](contracts/sql-history.md)，离线可交互可视化模型见 [visualization.md](contracts/visualization.md)，验证与证据索引见 [validation-evidence.md](contracts/validation-evidence.md)。

---

## 1. 与 M1 / M2 既有模型的继承与零修改边界

M3 完全复用并保持 M1（`ensabc-core::cluster`、`ens_cluster`）和 M2（`ensabc-core::ensemble_dispersion`、`match_scenarios`、`match_scenarios_scaled`、`ens_dispersion`、`ens_match_scenarios` 及 M2 五张正式历史表）原有定义不变：

| 上游层级 | 复用对象 | M3 对齐与约束说明 |
| --- | --- | --- |
| M1 单批情景 | `ScenarioConfig`, `ClusterScenario` | 实际聚类配置字段为 `projection_dim`, `cluster_count`, `initialization`, `seed`, `restarts`, `initial_centers`, `stopping_rule`, `tolerance`（非 `convergence_tolerance`）, `max_iterations`, `rank_threshold`（无 `standardize` 布尔字段）；M3 读取 `cluster_id`, `member_count`, `member_fraction`, `mean_features` |
| M2 样本清单 | `OmSampleManifest` (`m2.om_sample.v1`) | 沿用 WB2 8 批 `2m_temperature`（`K`）`50 × 3 × 3` 无损 `fpx_xor_2d` OM 样本与 `cos_lat_center_v1` 9 点空间权重 |
| M2 匹配内核 | `ScenarioNodeInput`, `MatchResult`, `UnmatchedScenario` | `ScenarioNodeInput` 已含 `member_count` 与 `member_fraction`；M2 `match_scenarios` 保持不校验占比，占比合法性仅在 M3 `evolution` 模块内强制校验；M2 未关联原因字段为 `UnmatchedScenario.reason: String`（`"no_candidate_within_threshold"` / `"not_selected_in_global_optimum"`），最优解计数为 `MatchResult.optima_count: usize`（映射至 SQL `optimal_solution_count`） |
| M2 持续身份 | `scenario_nodes.branch_id` | M3 `evolution_nodes.branch_id` 逐字符直接沿用 `<tracking_run_key[:12]>:b<ordinal:02d>:k<canonical_rank:02d>`，不改写前缀、不改变继承规则 |
| M2 正式历史表 | `tracking_runs` 等 5 张表 | 原样保留，零 DDL 修改；M3 新增 6 张正式历史表通过 `tracking_run_key` 外键关联 |

---

## 2. Rust Core 演化数据结构（`crates/ensabc-core/src/evolution.rs`）

### 2.1 演化配置与主干核验输入

```rust
pub enum SecondaryRule {
    NearestFeasibleV1, // 字符串标识 "nearest_feasible_v1"
}

pub enum FlowRule {
    StarMinMassProportionalV1, // 字符串标识 "star_min_mass_proportional_v1"
}

pub struct EvolutionConfig {
    pub secondary_rule: SecondaryRule,
    pub flow_rule: FlowRule,
    pub fraction_sum_tolerance: f64,           // 默认 1e-9，有限且 in [0.0, 1e-3]
    pub high_uncertainty_threshold_ratio: f64, // 默认 0.10，有限且 >= 0.0
    pub high_uncertainty_optimality_ratio: f64,// 默认 0.10，有限且 >= 0.0
    pub high_uncertainty_secondary_ratio: f64, // 默认 0.10，有限且 >= 0.0
}

pub struct BackboneAssertion {
    pub prev_cluster_ids: Vec<usize>,
    pub curr_cluster_ids: Vec<usize>,
    pub threshold: f64,
    pub distance_matrix: Option<Vec<Vec<f64>>>,
    pub eligible_matrix: Option<Vec<Vec<bool>>>,
    pub primary_links: Vec<(usize, usize)>, // (prev_cluster_id, curr_cluster_id)
}

pub struct EvolutionInput {
    pub prev_scenarios: Vec<ScenarioNodeInput>,
    pub curr_scenarios: Vec<ScenarioNodeInput>,
    pub weights: Vec<f64>,
    pub threshold: f64, // 绝对门槛 τ >= 0
    pub config: EvolutionConfig,
    pub backbone: Option<BackboneAssertion>,
}

pub struct ScaledEvolutionInput {
    pub prev_scenarios: Vec<ScenarioNodeInput>,
    pub curr_scenarios: Vec<ScenarioNodeInput>,
    pub weights: Vec<f64>,
    pub threshold_coefficient: f64, // c >= 0
    pub prev_dispersion: f64,       // s_prev > 0
    pub curr_dispersion: f64,       // s_curr > 0
    pub config: EvolutionConfig,
    pub backbone: Option<BackboneAssertion>,
}
```

**输入校验规则（FR-012 与边界情况）**：
- 继承 M2 全部输入校验（单侧情景数 $1..=12$、本批 `cluster_id` 唯一、`mean_features` 非空等长有限、`weights` 非负有限且 $\operatorname{fsum}(w) > 0$、$\tau \ge 0$、比例模式下 $s_{\text{prev}}, s_{\text{curr}} > 0$）；
- **成员数与占比校验**：每个情景 `member_count >= 1`（否则报 `ZeroMemberScenario`）；`member_fraction` 有限且满足 $0.0 < f \le 1.0$（否则报 `InvalidMemberFraction`）；单侧全部情景 $|\operatorname{fsum}(f) - 1.0| \le \text{fraction\_sum\_tolerance}$（否则报 `InvalidFractionSum`）；
- **配置校验**：`EvolutionConfig` 各容差与比例参数必须有限、非负，且 `fraction_sum_tolerance <= 1e-3`（否则报 `InvalidEvolutionConfig`）；
- **显式主干一致性核验**（当 `backbone = Some(b)` 时）：`prev_cluster_ids` / `curr_cluster_ids` 必须与输入情景序列一一匹配；`b.threshold == threshold`；`primary_links` 引用的 `cluster_id` 必须存在、满足一对一互不重复且每条边 $d_{i,j} \le \tau$（违反报 `InvalidPrimaryLink`）；若提供 `distance_matrix` / `eligible_matrix` 则必须与重算结果逐项相等，且 `primary_links` 必须达到 M2 全局最优目标 $(q^*, \Sigma^*)$（违反报 `BackboneInconsistency`）。

### 2.2 相邻跃迁演化输出 `EvolutionResult`

| 字段 | 类型 | 含义与不变量 |
| --- | --- | --- |
| `algorithm_version` | `String` | 固定 `"m3.evolve.v1"` |
| `secondary_rule` / `flow_rule` | `String` | `"nearest_feasible_v1"` / `"star_min_mass_proportional_v1"` |
| `uncertainty_rule` | `String` | `"m3.uncertainty.v1"` |
| `non_physical_disclaimer` | `String` | `"algorithmic_attribution_only_not_physical_member_migration"` |
| `prev_cluster_ids` / `curr_cluster_ids` | `Vec<usize>` | 按输入顺序保存的 `cluster_id` 列表 |
| `threshold` | `f64` | 生效的绝对门槛 $\tau$ |
| `pair_scale` / `threshold_coefficient` | `Option<f64>` | 比例模式下为 `Some(S)` / `Some(c)`，绝对模式下为 `None` |
| `prev_dispersion` / `curr_dispersion` | `Option<f64>` | 比例模式下为 `Some`，绝对模式下为 `None` |
| `reference_scale` | `Option<f64>` | 高不确定性参考尺度 $R$（比例模式为 $S$；绝对模式 $\tau > 0$ 时为 $\tau$，$\tau = 0$ 时为 `None`） |
| `backbone` | `MatchResult` | M2 全局最优一对一匹配完整结果 |
| `edges` | `Vec<EvolutionEdge>` | 主干与次级有向演化边，按 `(prev_cluster_id, curr_cluster_id)` 升序排列，无平行重边 |
| `prev_nodes` | `Vec<NodeEvolutionSummary>` | 前批各情景演化角色与指标摘要，按 `cluster_id` 升序排列 |
| `curr_nodes` | `Vec<NodeEvolutionSummary>` | 后批各情景演化角色与指标摘要，按 `cluster_id` 升序排列 |
| `events` | `Vec<EvolutionEvent>` | 连通分量分解得到的互斥演化事件列表，按规范序赋 `event_index = 0..K-1` |
| `summary` | `TransitionEvolutionSummary` | 相邻跃迁级三族演化指标与事件计数汇总 |

#### `EvolutionEdge`（单条多对多演化边）

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `prev_cluster_id` / `curr_cluster_id` | `usize` | 前后批情景编号 |
| `edge_role` | `EdgeRole` | `Primary` \| `SecondarySplit` \| `SecondaryMerge` |
| `event_type` | `TransitionEventKind` | 所属连通分量事件：`Continuation` \| `Split` \| `Merge` |
| `rule_basis` | `String` | 触发依据（如 `"global_optimum_primary"`、`"nearest_feasible_split_unique"`、`"nearest_feasible_merge_canonical_tiebreak"` 等） |
| `distance` | `f64` | 空间加权 RMSE $d_{i,j} \le \tau$ |
| `distance_over_scale` | `Option<f64>` | $d_{i,j} / S$（比例模式下为 `Some`） |
| `prev_member_fraction` / `curr_member_fraction` | `f64` | 起止节点各自的成员占比 $p_i, q_j$ |
| `edge_delta_fraction` | `f64` | $q_j \ominus p_i$ |
| `attributed_flow` | `f64` | 算法归因流量 $f(i,j) \in (0, \min(p_i, q_j)]$ |
| `edge_strength` | `f64` | 几何贴近度 $\in [0, 1]$ |
| `threshold_margin` / `threshold_margin_ratio` | `f64` / `Option<f64>` | $\tau \ominus d_{i,j} \ge 0$ 及其与 $R$ 比值 |
| `primary_alternative_status` | `EdgeAlternativeStatus` | 主干边为 `Tied` \| `SameCount` \| `CountDrop`；次级边为 `PromotionGap` |
| `optimality_margin` / `optimality_margin_ratio` | `Option<f64>` | 主干同基数替代差或次级强制晋升差 $\Delta\Sigma \ge 0$（`CountDrop` 为 `None`） |
| `secondary_margin` / `secondary_margin_ratio` | `Option<f64>` | 次级边存在第 2 个可行候选时为 $d_{(2)} \ominus d_{(1)} \ge 0$，否则为 `None` |
| `ambiguous_tie` | `bool` | 主干 `Tied` 或次级 `matched_in_some_optimum \|\| secondary_margin == Some(0.0)` |
| `high_uncertainty` | `bool` | 按冻结规则 `m3.uncertainty.v1` 判定的高不确定性标志 |
| `uncertainty_reasons` | `Vec<UncertaintyReason>` | `AmbiguousTie` / `NearThreshold` / `NearPrimarySwitch` / `NearSecondarySwitch` |

#### `NodeEvolutionSummary`（单侧节点角色与残差）

- 标识与基础量：`cluster_id`, `canonical_rank`, `member_count`, `member_fraction`；
- 拓扑度数：`primary_degree`（`0` 或 `1`）、`secondary_degree`（$\ge 0$）；
- 节点事件角色 `node_role`：
  - 前批节点：`ContinuationSource` \| `SplitParent` \| `MergePrimaryParent` \| `MergeSecondaryParent` \| `Disappeared`；
  - 后批节点：`ContinuationTarget` \| `SplitPrimaryChild` \| `SplitSecondaryChild` \| `MergeTarget` \| `Emerged`；
- 辅助布尔标记：`participates_in_split: bool`、`participates_in_merge: bool`（由定理 2 在单节点上互斥）；
- 未关联与最近候选：`unmatched_reason: Option<UnmatchedReason>`（由 M2 `UnmatchedScenario.reason: String` 解析映射）、`min_candidate_distance: Option<f64>`、`min_candidate_distance_over_scale: Option<f64>`；
- 占比分解：`primary_counterpart_cluster_id: Option<usize>`、`primary_delta_fraction: Option<f64>`、`attributed_flow_fraction: f64`、`unattributed_fraction: f64`（前批为未归因流出，后批为未归因流入）；
- 不确定性：`threshold_margin: Option<f64>`、`ambiguous_tie: bool`、`high_uncertainty: bool`。

#### `EvolutionEvent`（连通分量事件记录）

- `event_index: usize`：按分量规范键（优先含前批最小 `canonical_rank`，纯后批新生再按后批 `canonical_rank`）排序赋 `0..K-1`；
- `event_type`: `Continuation` \| `Split` \| `Merge` \| `Disappearance` \| `Emergence`（整链编排在缺口处另产 `GapInterrupted` \| `GapRestarted`）；
- `prev_cluster_ids: Vec<usize>`、`curr_cluster_ids: Vec<usize>`、`primary_link: Option<(usize, usize)>`；
- `rule_basis: String`；
- 三族分量摘要：`max_morphology_distance: f64`（无边事件为 `0.0`）、`max_morphology_over_scale: Option<f64>`、`prev_fraction_sum: f64`、`curr_fraction_sum: f64`、`net_delta_fraction: f64`、`attributed_flow_sum: f64`、`min_margin_ratio: Option<f64>`、`ambiguous_tie: bool`、`high_uncertainty: bool`。

---

## 3. 多批次整链与 DuckDB 历史表模型（FR-007–FR-010）

### 3.1 批次缺口与事件状态迁移规则

对计划起报序列 $b = 0, 1, \dots, B-1$（$4 \le B \le 8$）：
1. **批次状态**（复用 `replay_batches.status`）：`ok`、`missing`、`failed`（仅限输入合法的单批白名单算法错误：`INSUFFICIENT_RANK`、`AMBIGUOUS_SUBSPACE`、`EMPTY_CLUSTER`、`NOT_CONVERGED`、`INSUFFICIENT_DISTINCT_VECTORS`、`INSUFFICIENT_PROJECTED_VECTORS`、`SVD_NOT_CONVERGED`、`ZERO_DISPERSION`）。
2. **相邻跃迁状态**（`evolution_transitions`，共 $B-1$ 行，覆盖每对 $(b-1, b)$）：
   - 若批次 $b-1$ 与 $b$ 均为 `ok`：`transition_status = 'ok'`，`chain_broken = false`，生成完整多对多边与五类基础事件；
   - 若任一侧为 `missing` 或 `failed`：`transition_status = 'gap'`，`chain_broken = true`，`gap_reason` 为 `'prev_unavailable'` / `'curr_unavailable'` / `'both_unavailable'`，**生成 0 条主干边与 0 条次级边**；若 $b-1$ 为 `ok`，为 $b-1$ 的每个情景生成一条 `'gap_interrupted'` 事件；若 $b$ 为 `ok`，为 $b$ 的每个情景生成一条 `'gap_restarted'` 事件（绝不标为普通 `'disappearance'` 或 `'emergence'`）。
3. **持续身份与跨分支谱系**：
   - 每个 `ok` 节点的 `branch_id` 与 M2 `scenario_nodes.branch_id` 完全一致；
   - 次级分裂边 `(i, j)`（`secondary_split`）不改变 $j$ 作为新 `branch_id` 起点的身份（M2 `assignment_reason = 'not_selected_in_global_optimum'`），但在 `evolution_edges`（`prev_branch_id`）与 `evolution_branch_summaries` 中记录 $j$ 的次级分裂母分支 `split_from_parent_branch_id = branch_id(i)`；
   - 次级合并边 `(i, j)`（`secondary_merge`）在 $i$ 处正常结束其 M2 `branch_id`，同时记录 $i$ 汇入的目标主干分支 `merged_into_branch_id = branch_id(j)`。

### 3.2 六张新增 M3 正式历史表与外键拓扑

- `tracking_runs (PK: run_key)` *(M2 既有表)*
  - 1 对多 → `evolution_runs (PK: evolution_run_id; FK: tracking_run_key -> tracking_runs.run_key)`
    - 1 对多 → `evolution_nodes (PK: evolution_run_id, ordinal, cluster_id; UNIQUE: evolution_run_id, ordinal, branch_id; UNIQUE: evolution_run_id, ordinal, canonical_rank)`
    - 1 对多 → `evolution_transitions (PK: evolution_run_id, prev_ordinal; UNIQUE: evolution_run_id, curr_ordinal; CHECK: curr_ordinal = prev_ordinal + 1)`
      - 1 对多 → `evolution_edges (PK: evolution_run_id, prev_ordinal, prev_cluster_id, curr_cluster_id)`
      - 1 对多 → `evolution_events (PK: evolution_run_id, prev_ordinal, event_index)`
    - 1 对多 → `evolution_branch_summaries (PK: evolution_run_id, branch_id)`

**版本键与内容摘要规则（FR-009）**：
- `evolution_run_id` 为绑定结构（`tracking_run_key`、语义输入清单摘要、情景配置摘要、匹配配置摘要、演化配置摘要、权重版本及实际扩展/DuckOMO/DuckDB 版本摘要）的 SHA-256；
- `content_sha256` 为拟写入的六张 M3 表全部记录按表序与主键排序、`-0.0` 归一为 `+0.0`、有限 `f64` round-trip JSON 序列化后的 SHA-256。

### 3.3 发布事务提交前强制校验的 8 条全局不变量（`E1`–`E8`）

1. **`E1`（M2 节点与身份逐字一致）**：`evolution_nodes` 与对应 `tracking_run_key` 的 `scenario_nodes` 在 `(ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction)` 上双向 `EXCEPT` 均为空集；
2. **`E2`（M2 主干边双向一致与一对一继承）**：`evolution_edges WHERE edge_role = 'primary'` 与 `scenario_links` 在 `(prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, distance, ambiguous)` 上双向 `EXCEPT` 均为空集，且每条主干边 `prev_branch_id = curr_branch_id`；
3. **`E3`（次级边可行性与角色互斥）**：每条 `secondary_split` 终点满足 `in_degree_primary = 0` 且 M2 `assignment_reason = 'not_selected_in_global_optimum'`；每条 `secondary_merge` 起点满足 `out_degree_primary = 0`；全部演化边满足 `distance <= threshold`；`no_candidate_within_threshold` 节点关联边数为 0；
4. **`E4`（缺口断链与零跨缺口边）**：`evolution_transitions` 行数恰等于 `planned_batch_count - 1`；`chain_broken = true` 的跃迁在 `evolution_edges` 中行数为 0；每个 `branch_id` 的 `ordinal` 集合构成无跳跃连续整数区间；
5. **`E5`（缺口事件与新生/消失严格互斥）**：`transition_status = 'gap'` 的跃迁仅含 `'gap_interrupted'` / `'gap_restarted'` 事件；`transition_status = 'ok'` 的跃迁不含任何 `'gap_*'` 事件；
6. **`E6`（节点度数守恒）**：`evolution_nodes` 存储的主/次出入度精确等于 `evolution_edges` 实际入射计数；
7. **`E7`（分支生命周期守恒）**：`evolution_branch_summaries` 的 `branch_id` 集合与 `evolution_nodes` 完全相同，每个分支 `node_count = end_ordinal - birth_ordinal + 1 = step_count + 1`，且 $\sum \text{node\_count}$ 等于总结点数；
8. **`E8`（有限数值与非物理迁移声明）**：所有数值列满足 `isfinite(...)`，且所有运行、边与事件记录携带非空的算法解释边界标识。

---

## 4. 离线可交互溯源视图与敏感性实验实体（FR-014、FR-016）

### 4.1 `TraceableEvolutionViewManifest`（`schema_version = "m3.visualization.v1"`）

嵌入单文件 `.html` 内 `<script id="ensabc-evolution-provenance" type="application/json">`，并与内联 `<svg id="ensabc-evolution-svg">` 图元严格双射：
- `run`: `{evolution_run_id, tracking_run_key, content_sha256, algorithm_version, secondary_rule, flow_rule, uncertainty_rule, planned_batch_count, chain_summary}`
- `disclaimer`: `{version: "m3.disclaimer.v1", non_physical_token, clauses: [non_member_migration, non_physical_cyclogenesis, non_calibrated_probability]}`
- `batches`: 长度 $B$，按 `ordinal` 排列 `{ordinal, init_time, lead_hours, status, status_reason, dispersion}`
- `nodes`: 以 `"b{ordinal}:c{cluster_id}"` 为键，映射至 `evolution_nodes` 完整字段（含 `branch_id`, `member_fraction`, `node_role`, `primary_delta_fraction`, `unattributed_inflow/outflow_fraction`, `high_uncertainty`）
- `edges`: 以 `"b{prev_ordinal}:c{prev_cluster_id}->b{curr_ordinal}:c{curr_cluster_id}"` 为键，映射至 `evolution_edges` 完整字段（含 `edge_role`, `event_type`, `rule_basis`, `distance`, `distance_over_scale`, `attributed_flow`, `edge_strength`, 三类余量, `ambiguous_tie`, `high_uncertainty`, `uncertainty_reasons`）
- `transitions`: 长度 $B-1$，映射至 `evolution_transitions` 与对应 `evolution_events`
- `branches`: 以 `branch_id` 为键，映射至 `evolution_branch_summaries`

### 4.2 `SensitivityAndPerturbationBundle`（`schema_version = "m3.sensitivity.v1"`）

- `perturbation_evolution_level`: 记录 Level 1a（`epsilon_ratios = [0.005, 0.01, 0.02]`, 100 种子）与 Level 1b（纯占比 $\pm 1$ 成员重分配）下每对跃迁的四条件证书状态、主/次边翻转率、事件翻转矩阵及触发翻转样本的基线余量；
- `perturbation_member_level`: 记录 Level 2（`sigma_levels_k = [0.0, 0.001, 0.01]`, 20 种子）下成员重叠映射状态（含 `evaluation_mapping_ambiguous`）、单批失败数、四条件证书校验结果、单对跃迁翻转与整链谱系翻转清单；
- `sensitivity_thresholds`: 记录 `c ∈ {0.5, 0.75, 1.0, 1.25, 1.5, 2.0}` × `k ∈ {2, 3, 4}` 共 18 个已发布 `evolution_run_id` 的五类事件计数、主/次边数、TV 重分配均值与高不确定性比例对照；
- `sensitivity_clustering_configs`: 记录主门槛 `c = 1.0` 下四组聚类配置轴（`cluster_count`、`projection_dim`、`seed/restarts` 及单批退化断链边界配置）各自发布的 `evolution_run_id`、批次 `ok/failed` 状态分布、断链位置与演化事件结构对照。
