# M3 SQL 扩展接口与 DuckDB 演化历史保存契约

版本：`m3.sql.v1`。目标宿主：DuckDB CLI `v1.5.5` / Linux aarch64（ABI：`C_STRUCT_UNSTABLE`）。对应 FR-007–FR-012、FR-017、SC-002–SC-005、SC-007。

---

## 1. 新增无状态 SQL 标量函数（`crates/ensabc-duckdb`）

在同一扩展（`ensabc_m0_extension.duckdb_extension`）中保留 `ens_m0_echo`、`ens_cluster`、`ens_dispersion` 与 `ens_match_scenarios` 100% 不变，新增 `crates/ensabc-duckdb/src/evolve_ext.rs` 注册以下三个 C API 无状态标量函数（均启用 `duckdb_scalar_function_set_special_handling` 显式拦截 `NULL`、包裹 `catch_unwind`、遵循“全 chunk 计算成功后再一次性 `reserve_list` + 写向量”的安全模式）：

### 1.1 `ens_fsum`

```sql
ens_fsum(values DOUBLE[]) -> DOUBLE
```

- **语义**：调用 `ensabc_core::fsum::Expansion` 对非空或空有限双精度列表执行单层平铺精确求和并一次 round-half-even 舍入（空列表返回 `0.0`），用于 SQL 分支/全链汇总中的累计距离、累计绝对占比差与重分配度计算，确保与 Rust `evolve_chain` 及 Python `math.fsum` **0 ULP 差异**。
- **错误处理**：参数为 `NULL`、含 `NULL` 元素、含非有限值或中间展开溢出时，报错 `ens_fsum [CODE] chunk_row=N: ...`。

### 1.2 `ens_diagnose_batch`

```sql
ens_diagnose_batch(
  members STRUCT(member_id VARCHAR, features DOUBLE[])[],
  weights DOUBLE[],
  config STRUCT(
    projection_dim BIGINT,
    cluster_count BIGINT,
    initialization VARCHAR,
    seed BIGINT,
    restarts BIGINT,
    initial_centers DOUBLE[][][],
    stopping_rule VARCHAR,
    tolerance DOUBLE,
    max_iterations BIGINT,
    rank_threshold DOUBLE
  )
) -> VARCHAR
```

- **语义**：对单批集合成员与聚类配置精确执行 `compute_ensemble_dispersion` 与 `compute_scenarios` 预检。若两者全部成功则返回 `'OK'`；若命中 8 类白名单单批算法失败码之一（`'ZERO_DISPERSION'`、`'INSUFFICIENT_RANK'`、`'AMBIGUOUS_SUBSPACE'`、`'EMPTY_CLUSTER'`、`'NOT_CONVERGED'`、`'INSUFFICIENT_DISTINCT_VECTORS'`、`'INSUFFICIENT_PROJECTED_VECTORS'`、`'SVD_NOT_CONVERGED'`），则返回对应精确 `CoreErrorCode` 字符串供 Stage A 记为 `status='failed'`；若出现 `NULL`、非有限值、非法配置或非白名单错误（如 `NUMERICAL_FAILURE` 浮点溢出），立即以 `ens_diagnose_batch [CODE] chunk_row=N: ...` 中止整批 SQL 查询，杜绝启发式错误码猜测或吞掉溢出故障。

### 1.3 `ens_evolve_scenarios`

```sql
ens_evolve_scenarios(
  prev_scenarios STRUCT(
    cluster_id BIGINT,
    member_count BIGINT,
    member_fraction DOUBLE,
    mean_features DOUBLE[]
  )[],
  curr_scenarios STRUCT(
    cluster_id BIGINT,
    member_count BIGINT,
    member_fraction DOUBLE,
    mean_features DOUBLE[]
  )[],
  weights DOUBLE[],
  backbone_match STRUCT(...), -- 直接复用 ens_match_scenarios 的完整返回类型
  config STRUCT(
    secondary_rule VARCHAR,
    flow_rule VARCHAR,
    fraction_sum_tolerance DOUBLE,
    high_uncertainty_threshold_ratio DOUBLE,
    high_uncertainty_optimality_ratio DOUBLE,
    high_uncertainty_secondary_ratio DOUBLE
  )
) -> STRUCT(
  algorithm_version VARCHAR,
  secondary_rule VARCHAR,
  flow_rule VARCHAR,
  uncertainty_rule VARCHAR,
  non_physical_disclaimer VARCHAR,
  prev_cluster_ids BIGINT[],
  curr_cluster_ids BIGINT[],
  prev_dispersion DOUBLE,
  curr_dispersion DOUBLE,
  pair_scale DOUBLE,
  threshold_coefficient DOUBLE,
  threshold DOUBLE,
  has_reference_scale BOOLEAN,
  reference_scale DOUBLE,
  edges STRUCT(
    prev_cluster_id BIGINT,
    curr_cluster_id BIGINT,
    edge_role VARCHAR,
    event_type VARCHAR,
    rule_basis VARCHAR,
    distance DOUBLE,
    distance_over_scale DOUBLE,
    prev_member_fraction DOUBLE,
    curr_member_fraction DOUBLE,
    edge_delta_fraction DOUBLE,
    attributed_flow DOUBLE,
    edge_strength DOUBLE,
    threshold_margin DOUBLE,
    threshold_margin_ratio DOUBLE,
    primary_alternative_status VARCHAR,
    has_optimality_margin BOOLEAN,
    optimality_margin DOUBLE,
    optimality_margin_ratio DOUBLE,
    has_secondary_margin BOOLEAN,
    secondary_margin DOUBLE,
    secondary_margin_ratio DOUBLE,
    ambiguous_tie BOOLEAN,
    high_uncertainty BOOLEAN,
    uncertainty_reasons VARCHAR[]
  )[],
  prev_nodes STRUCT(
    cluster_id BIGINT,
    canonical_rank BIGINT,
    member_count BIGINT,
    member_fraction DOUBLE,
    out_degree_primary BIGINT,
    out_degree_secondary BIGINT,
    node_role VARCHAR,
    participates_in_split BOOLEAN,
    participates_in_merge BOOLEAN,
    has_unmatched_reason BOOLEAN,
    unmatched_reason VARCHAR,
    has_min_candidate_distance BOOLEAN,
    min_candidate_distance DOUBLE,
    min_candidate_distance_over_scale DOUBLE,
    has_primary_counterpart BOOLEAN,
    primary_counterpart_cluster_id BIGINT,
    primary_delta_fraction DOUBLE,
    attributed_flow_fraction DOUBLE,
    unattributed_fraction DOUBLE,
    has_threshold_margin BOOLEAN,
    threshold_margin DOUBLE,
    ambiguous_tie BOOLEAN,
    high_uncertainty BOOLEAN
  )[],
  curr_nodes STRUCT(
    cluster_id BIGINT,
    canonical_rank BIGINT,
    member_count BIGINT,
    member_fraction DOUBLE,
    in_degree_primary BIGINT,
    in_degree_secondary BIGINT,
    node_role VARCHAR,
    participates_in_split BOOLEAN,
    participates_in_merge BOOLEAN,
    has_unmatched_reason BOOLEAN,
    unmatched_reason VARCHAR,
    has_min_candidate_distance BOOLEAN,
    min_candidate_distance DOUBLE,
    min_candidate_distance_over_scale DOUBLE,
    has_primary_counterpart BOOLEAN,
    primary_counterpart_cluster_id BIGINT,
    primary_delta_fraction DOUBLE,
    attributed_flow_fraction DOUBLE,
    unattributed_fraction DOUBLE,
    has_threshold_margin BOOLEAN,
    threshold_margin DOUBLE,
    ambiguous_tie BOOLEAN,
    high_uncertainty BOOLEAN
  )[],
  events STRUCT(
    event_index BIGINT,
    event_type VARCHAR,
    prev_cluster_ids BIGINT[],
    curr_cluster_ids BIGINT[],
    has_primary_link BOOLEAN,
    primary_prev_cluster_id BIGINT,
    primary_curr_cluster_id BIGINT,
    rule_basis VARCHAR,
    max_morphology_distance DOUBLE,
    max_morphology_over_scale DOUBLE,
    prev_fraction_sum DOUBLE,
    curr_fraction_sum DOUBLE,
    net_delta_fraction DOUBLE,
    attributed_flow_sum DOUBLE,
    has_min_margin_ratio BOOLEAN,
    min_margin_ratio DOUBLE,
    ambiguous_tie BOOLEAN,
    high_uncertainty BOOLEAN
  )[],
  summary STRUCT(
    primary_edge_count BIGINT,
    secondary_split_edge_count BIGINT,
    secondary_merge_edge_count BIGINT,
    continuation_count BIGINT,
    split_count BIGINT,
    merge_count BIGINT,
    emergence_count BIGINT,
    disappearance_count BIGINT,
    primary_total_distance DOUBLE,
    primary_max_distance DOUBLE,
    primary_max_distance_over_scale DOUBLE,
    all_edges_max_distance DOUBLE,
    all_edges_max_distance_over_scale DOUBLE,
    transition_tv_redistribution DOUBLE,
    secondary_attributed_flow DOUBLE,
    total_attributed_flow DOUBLE,
    disappeared_fraction_sum DOUBLE,
    emerged_fraction_sum DOUBLE,
    component_tv_residual DOUBLE,
    min_threshold_margin DOUBLE,
    min_threshold_margin_ratio DOUBLE,
    raw_matrix_threshold_margin DOUBLE,
    has_optimality_margin BOOLEAN,
    min_optimality_margin DOUBLE,
    min_optimality_margin_ratio DOUBLE,
    has_secondary_margin BOOLEAN,
    min_secondary_margin DOUBLE,
    min_secondary_margin_ratio DOUBLE,
    ambiguous_tie BOOLEAN,
    high_uncertainty_edge_count BIGINT,
    high_uncertainty BOOLEAN
  )
)
```

- **非 `NULL` 哨兵约定**：所有返回字段和列表元素均非 `NULL` 且有限；可选指标统一由对应 `has_* BOOLEAN` 控制（`false` 时数值写 `0.0`、索引写 `-1`、字符串写 `''`）。
- **主干强制核验与 M2 字段映射**：`ens_evolve_scenarios` 将 `backbone_match` 全部 21 个顶层字段与 `links[*]` 全部 11 个嵌套字段完整解码并对照重算的 `MatchResult` 逐位核验（含 `distance_matrix`、`distance_ratio_matrix`、`eligible_matrix`、`ambiguous`、`threshold_margin`、`has_optimality_margin`、`optimality_margin`、`links[*]` 端点/升序顺序/距离与替代余量字段及 `unmatched_prev/curr[*]` 的 `min_candidate_distance` 等）；若人为篡改 `backbone_match` 任意字段，立即以 `ens_evolve_scenarios [BACKBONE_INCONSISTENCY|INVALID_PRIMARY_LINK] chunk_row=N: ...` 拒绝整批查询。
- **合成绝对门槛用例的 SQL 比例适配与无尺度模式测试边界**：因 SQL `ens_match_scenarios` 与 `ens_evolve_scenarios` 统一接收比例模式输入（`prev_dispersion > 0, curr_dispersion > 0, threshold_coefficient >= 0`），SQL 合成绝对门槛用例通过传入 `prev_dispersion = 1.0, curr_dispersion = 1.0, threshold_coefficient = tau`（使 `pair_scale S = 1.0, threshold = tau`）进行测试；而完全无离散尺度的 `EvolutionInput`（尤其当 `tau = 0` 时 `reference_scale R = None`）由原生 CLI `m3_runner --mode pair` 直接覆盖验证。

---

## 2. 六张新增 M3 正式历史表 DDL（与 M2 五表并存）

`validation/m3/sql/schema.sql` 原样包含 M2 五张表（`tracking_runs`、`replay_batches`、`scenario_nodes`、`pair_comparisons`、`scenario_links`），并追加以下 6 张不可变演化表：

```sql
CREATE TABLE IF NOT EXISTS evolution_runs (
    evolution_run_id        VARCHAR PRIMARY KEY,
    tracking_run_key        VARCHAR NOT NULL REFERENCES tracking_runs(run_key),
    evolution_binding       STRUCT(
                              tracking_run_key VARCHAR,
                              input_manifest_sha256 VARCHAR,
                              scenario_config_sha256 VARCHAR,
                              match_config_sha256 VARCHAR,
                              evolve_config_sha256 VARCHAR,
                              weights_version VARCHAR,
                              secondary_rule VARCHAR,
                              flow_rule VARCHAR,
                              uncertainty_rule VARCHAR,
                              fraction_sum_tolerance DOUBLE,
                              high_uncertainty_threshold_ratio DOUBLE,
                              high_uncertainty_optimality_ratio DOUBLE,
                              high_uncertainty_secondary_ratio DOUBLE,
                              software STRUCT(
                                ensabc_extension_sha256 VARCHAR,
                                duckomo_version VARCHAR,
                                duckomo_sha256 VARCHAR,
                                duckdb_version VARCHAR,
                                cluster_algorithm_version VARCHAR,
                                match_algorithm_version VARCHAR,
                                evolution_algorithm_version VARCHAR
                              )
                            ) NOT NULL,
    weights                 DOUBLE[] NOT NULL,
    planned_batch_count     INTEGER NOT NULL CHECK (planned_batch_count BETWEEN 4 AND 8),
    chain_summary           STRUCT(
                              ok_batch_count INTEGER,
                              missing_batch_count INTEGER,
                              failed_batch_count INTEGER,
                              ok_transition_count INTEGER,
                              gap_transition_count INTEGER,
                              branch_count INTEGER,
                              primary_edge_count INTEGER,
                              secondary_split_edge_count INTEGER,
                              secondary_merge_edge_count INTEGER,
                              continuation_count INTEGER,
                              split_count INTEGER,
                              merge_count INTEGER,
                              emergence_count INTEGER,
                              disappearance_count INTEGER,
                              gap_interrupted_count INTEGER,
                              gap_restarted_count INTEGER,
                              cumulative_primary_distance DOUBLE,
                              max_edge_distance DOUBLE,
                              max_edge_distance_over_scale DOUBLE,
                              mean_transition_tv_redistribution DOUBLE,
                              max_transition_tv_redistribution DOUBLE,
                              ambiguous_transition_count INTEGER,
                              high_uncertainty_transition_count INTEGER,
                              high_uncertainty_transition_ratio DOUBLE,
                              high_uncertainty_edge_count INTEGER,
                              high_uncertainty_edge_ratio DOUBLE
                            ) NOT NULL,
    non_physical_disclaimer VARCHAR NOT NULL CHECK (length(non_physical_disclaimer) > 0),
    content_sha256          VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS evolution_nodes (
    evolution_run_id              VARCHAR NOT NULL REFERENCES evolution_runs(evolution_run_id),
    ordinal                       INTEGER NOT NULL CHECK (ordinal >= 0),
    cluster_id                    INTEGER NOT NULL CHECK (cluster_id >= 0),
    canonical_rank                INTEGER NOT NULL CHECK (canonical_rank >= 0),
    branch_id                     VARCHAR NOT NULL,
    mean_features                 DOUBLE[] NOT NULL,
    member_count                  INTEGER NOT NULL CHECK (member_count > 0),
    member_fraction               DOUBLE NOT NULL CHECK (isfinite(member_fraction) AND member_fraction > 0 AND member_fraction <= 1),
    assignment_reason             VARCHAR NOT NULL,
    in_degree_primary             INTEGER NOT NULL CHECK (in_degree_primary IN (0, 1)),
    in_degree_secondary           INTEGER NOT NULL CHECK (in_degree_secondary >= 0),
    out_degree_primary            INTEGER NOT NULL CHECK (out_degree_primary IN (0, 1)),
    out_degree_secondary          INTEGER NOT NULL CHECK (out_degree_secondary >= 0),
    incoming_role                 VARCHAR NOT NULL,
    outgoing_role                 VARCHAR NOT NULL,
    primary_delta_fraction        DOUBLE NOT NULL CHECK (isfinite(primary_delta_fraction)),
    has_primary_delta_fraction    BOOLEAN NOT NULL,
    unattributed_inflow_fraction  DOUBLE NOT NULL CHECK (isfinite(unattributed_inflow_fraction) AND unattributed_inflow_fraction >= 0),
    unattributed_outflow_fraction DOUBLE NOT NULL CHECK (isfinite(unattributed_outflow_fraction) AND unattributed_outflow_fraction >= 0),
    high_uncertainty              BOOLEAN NOT NULL,
    PRIMARY KEY (evolution_run_id, ordinal, cluster_id),
    UNIQUE (evolution_run_id, ordinal, branch_id),
    UNIQUE (evolution_run_id, ordinal, canonical_rank)
);

CREATE TABLE IF NOT EXISTS evolution_transitions (
    evolution_run_id              VARCHAR NOT NULL REFERENCES evolution_runs(evolution_run_id),
    prev_ordinal                  INTEGER NOT NULL CHECK (prev_ordinal >= 0),
    curr_ordinal                  INTEGER NOT NULL,
    transition_status             VARCHAR NOT NULL CHECK (transition_status IN ('ok', 'gap')),
    chain_broken                  BOOLEAN NOT NULL,
    gap_reason                    VARCHAR,
    pair_scale                    DOUBLE CHECK (pair_scale IS NULL OR (isfinite(pair_scale) AND pair_scale > 0)),
    threshold                     DOUBLE CHECK (threshold IS NULL OR (isfinite(threshold) AND threshold >= 0)),
    primary_edge_count            INTEGER NOT NULL CHECK (primary_edge_count >= 0),
    secondary_split_edge_count    INTEGER NOT NULL CHECK (secondary_split_edge_count >= 0),
    secondary_merge_edge_count    INTEGER NOT NULL CHECK (secondary_merge_edge_count >= 0),
    transition_tv_redistribution  DOUBLE CHECK (transition_tv_redistribution IS NULL OR (isfinite(transition_tv_redistribution) AND transition_tv_redistribution >= 0)),
    min_threshold_margin          DOUBLE CHECK (min_threshold_margin IS NULL OR (isfinite(min_threshold_margin) AND min_threshold_margin >= 0)),
    ambiguous_tie                 BOOLEAN NOT NULL,
    high_uncertainty              BOOLEAN NOT NULL,
    evolve_result                 STRUCT(...) , -- ok 时 NOT NULL，gap 时 NULL
    PRIMARY KEY (evolution_run_id, prev_ordinal),
    UNIQUE (evolution_run_id, curr_ordinal),
    CHECK (curr_ordinal = prev_ordinal + 1),
    CHECK ((transition_status = 'gap') = chain_broken),
    CHECK ((transition_status = 'ok') = (evolve_result IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS evolution_edges (
    evolution_run_id          VARCHAR NOT NULL,
    prev_ordinal              INTEGER NOT NULL,
    prev_cluster_id           INTEGER NOT NULL,
    curr_ordinal              INTEGER NOT NULL,
    curr_cluster_id           INTEGER NOT NULL,
    prev_branch_id            VARCHAR NOT NULL,
    curr_branch_id            VARCHAR NOT NULL,
    edge_role                 VARCHAR NOT NULL CHECK (edge_role IN ('primary', 'secondary_split', 'secondary_merge')),
    event_type                VARCHAR NOT NULL CHECK (event_type IN ('continuation', 'split', 'merge')),
    rule_basis                VARCHAR NOT NULL,
    distance                  DOUBLE NOT NULL CHECK (isfinite(distance) AND distance >= 0),
    distance_over_scale       DOUBLE NOT NULL CHECK (isfinite(distance_over_scale) AND distance_over_scale >= 0),
    prev_member_fraction      DOUBLE NOT NULL CHECK (isfinite(prev_member_fraction) AND prev_member_fraction > 0),
    curr_member_fraction      DOUBLE NOT NULL CHECK (isfinite(curr_member_fraction) AND curr_member_fraction > 0),
    edge_delta_fraction       DOUBLE NOT NULL CHECK (isfinite(edge_delta_fraction)),
    attributed_flow           DOUBLE NOT NULL CHECK (isfinite(attributed_flow) AND attributed_flow > 0),
    edge_strength             DOUBLE NOT NULL CHECK (isfinite(edge_strength) AND edge_strength >= 0 AND edge_strength <= 1),
    threshold_margin          DOUBLE NOT NULL CHECK (isfinite(threshold_margin) AND threshold_margin >= 0),
    threshold_margin_ratio    DOUBLE NOT NULL CHECK (isfinite(threshold_margin_ratio) AND threshold_margin_ratio >= 0),
    primary_alternative_status VARCHAR NOT NULL,
    has_optimality_margin     BOOLEAN NOT NULL,
    optimality_margin         DOUBLE NOT NULL CHECK (isfinite(optimality_margin) AND optimality_margin >= 0),
    optimality_margin_ratio   DOUBLE NOT NULL CHECK (isfinite(optimality_margin_ratio) AND optimality_margin_ratio >= 0),
    has_secondary_margin      BOOLEAN NOT NULL,
    secondary_margin          DOUBLE NOT NULL CHECK (isfinite(secondary_margin) AND secondary_margin >= 0),
    secondary_margin_ratio    DOUBLE NOT NULL CHECK (isfinite(secondary_margin_ratio) AND secondary_margin_ratio >= 0),
    ambiguous_tie             BOOLEAN NOT NULL,
    high_uncertainty          BOOLEAN NOT NULL,
    uncertainty_reasons       VARCHAR[] NOT NULL,
    non_physical_disclaimer   VARCHAR NOT NULL CHECK (length(non_physical_disclaimer) > 0),
    PRIMARY KEY (evolution_run_id, prev_ordinal, prev_cluster_id, curr_cluster_id),
    CHECK (curr_ordinal = prev_ordinal + 1),
    FOREIGN KEY (evolution_run_id, prev_ordinal) REFERENCES evolution_transitions(evolution_run_id, prev_ordinal),
    FOREIGN KEY (evolution_run_id, prev_ordinal, prev_cluster_id) REFERENCES evolution_nodes(evolution_run_id, ordinal, cluster_id),
    FOREIGN KEY (evolution_run_id, curr_ordinal, curr_cluster_id) REFERENCES evolution_nodes(evolution_run_id, ordinal, cluster_id)
);

CREATE TABLE IF NOT EXISTS evolution_events (
    evolution_run_id            VARCHAR NOT NULL,
    prev_ordinal                INTEGER NOT NULL,
    event_index                 INTEGER NOT NULL CHECK (event_index >= 0),
    event_type                  VARCHAR NOT NULL CHECK (event_type IN (
                                  'continuation', 'split', 'merge',
                                  'emergence', 'disappearance',
                                  'gap_interrupted', 'gap_restarted')),
    prev_cluster_ids            INTEGER[] NOT NULL,
    curr_cluster_ids            INTEGER[] NOT NULL,
    branch_ids                  VARCHAR[] NOT NULL,
    primary_branch_id           VARCHAR,
    rule_basis                  VARCHAR NOT NULL,
    max_morphology_distance     DOUBLE NOT NULL CHECK (isfinite(max_morphology_distance) AND max_morphology_distance >= 0),
    max_morphology_over_scale   DOUBLE NOT NULL CHECK (isfinite(max_morphology_over_scale) AND max_morphology_over_scale >= 0),
    prev_fraction_sum           DOUBLE NOT NULL CHECK (isfinite(prev_fraction_sum) AND prev_fraction_sum >= 0),
    curr_fraction_sum           DOUBLE NOT NULL CHECK (isfinite(curr_fraction_sum) AND curr_fraction_sum >= 0),
    net_delta_fraction          DOUBLE NOT NULL CHECK (isfinite(net_delta_fraction)),
    attributed_flow_sum         DOUBLE NOT NULL CHECK (isfinite(attributed_flow_sum) AND attributed_flow_sum >= 0),
    has_min_margin_ratio        BOOLEAN NOT NULL,
    min_margin_ratio            DOUBLE NOT NULL CHECK (isfinite(min_margin_ratio) AND min_margin_ratio >= 0),
    ambiguous_tie               BOOLEAN NOT NULL,
    high_uncertainty            BOOLEAN NOT NULL,
    non_physical_disclaimer     VARCHAR NOT NULL CHECK (length(non_physical_disclaimer) > 0),
    PRIMARY KEY (evolution_run_id, prev_ordinal, event_index),
    FOREIGN KEY (evolution_run_id, prev_ordinal) REFERENCES evolution_transitions(evolution_run_id, prev_ordinal)
);

CREATE TABLE IF NOT EXISTS evolution_branch_summaries (
    evolution_run_id                VARCHAR NOT NULL REFERENCES evolution_runs(evolution_run_id),
    branch_id                       VARCHAR NOT NULL,
    birth_ordinal                   INTEGER NOT NULL CHECK (birth_ordinal >= 0),
    end_ordinal                     INTEGER NOT NULL CHECK (end_ordinal >= birth_ordinal),
    node_count                      INTEGER NOT NULL CHECK (node_count >= 1),
    step_count                      INTEGER NOT NULL CHECK (step_count = node_count - 1),
    birth_reason                    VARCHAR NOT NULL,
    termination_reason              VARCHAR NOT NULL,
    cumulative_morphology_distance  DOUBLE NOT NULL CHECK (isfinite(cumulative_morphology_distance) AND cumulative_morphology_distance >= 0),
    cumulative_morphology_over_scale DOUBLE NOT NULL CHECK (isfinite(cumulative_morphology_over_scale) AND cumulative_morphology_over_scale >= 0),
    max_step_morphology_distance    DOUBLE NOT NULL CHECK (isfinite(max_step_morphology_distance) AND max_step_morphology_distance >= 0),
    max_step_morphology_over_scale  DOUBLE NOT NULL CHECK (isfinite(max_step_morphology_over_scale) AND max_step_morphology_over_scale >= 0),
    end_to_end_morphology_distance  DOUBLE NOT NULL CHECK (isfinite(end_to_end_morphology_distance) AND end_to_end_morphology_distance >= 0),
    initial_member_fraction         DOUBLE NOT NULL CHECK (isfinite(initial_member_fraction) AND initial_member_fraction > 0),
    final_member_fraction           DOUBLE NOT NULL CHECK (isfinite(final_member_fraction) AND final_member_fraction > 0),
    min_member_fraction             DOUBLE NOT NULL CHECK (isfinite(min_member_fraction) AND min_member_fraction > 0),
    max_member_fraction             DOUBLE NOT NULL CHECK (isfinite(max_member_fraction) AND max_member_fraction <= 1),
    net_delta_fraction              DOUBLE NOT NULL CHECK (isfinite(net_delta_fraction)),
    cumulative_abs_delta_fraction   DOUBLE NOT NULL CHECK (isfinite(cumulative_abs_delta_fraction) AND cumulative_abs_delta_fraction >= 0),
    split_out_event_count           INTEGER NOT NULL CHECK (split_out_event_count >= 0),
    spawned_secondary_branch_ids    VARCHAR[] NOT NULL,
    split_from_parent_branch_id     VARCHAR,
    merge_in_event_count            INTEGER NOT NULL CHECK (merge_in_event_count >= 0),
    absorbed_secondary_branch_ids   VARCHAR[] NOT NULL,
    merged_into_branch_id           VARCHAR,
    ambiguous_step_count            INTEGER NOT NULL CHECK (ambiguous_step_count >= 0),
    high_uncertainty_step_count     INTEGER NOT NULL CHECK (high_uncertainty_step_count >= 0),
    high_uncertainty_step_ratio     DOUBLE NOT NULL CHECK (isfinite(high_uncertainty_step_ratio) AND high_uncertainty_step_ratio >= 0 AND high_uncertainty_step_ratio <= 1),
    PRIMARY KEY (evolution_run_id, branch_id)
);
```

---

## 3. 两阶段暂存、单事务发布与 `E1`–`E8` 守卫协议

1. **阶段 A（`TEMP` 表暂存，autocommit）**：
   - 沿用 M2 单批故障隔离暂存（对 `present` 批次逐条执行 `read_om` 准备门禁、`ens_cluster`、`ens_dispersion`；仅允许 8 类白名单单批算法失败——`INSUFFICIENT_RANK`、`AMBIGUOUS_SUBSPACE`、`EMPTY_CLUSTER`、`NOT_CONVERGED`、`INSUFFICIENT_DISTINCT_VECTORS`、`INSUFFICIENT_PROJECTED_VECTORS`、`SVD_NOT_CONVERGED`、`ZERO_DISPERSION`——转为 `status='failed'`，元数据/输入/扩展核验错误立即中止）；
   - 对相邻 `status='ok'` 批次对调用 `ens_match_scenarios` 与递归 CTE 生成 M2 持续身份，再逐对调用 `ens_evolve_scenarios`（任一匹配或演化错误立即中止，禁止进入阶段 B）；
   - 在纯 SQL 中物化 `stage_evolution_nodes`、`stage_evolution_transitions`（含 `gap` 对）、`stage_evolution_edges`、`stage_evolution_events`（含 `gap_interrupted` / `gap_restarted`）、`stage_evolution_branch_summaries`（使用 `ens_fsum` 求精确和）与 `stage_chain_summary`，并计算确定性规范摘要 `m3_content_sha256`。
2. **阶段 B（单事务原子发布，`BEGIN TRANSACTION ... COMMIT`）**：
   - 若对应 `tracking_run_key` 尚未写入 M2 五表，先执行 M2 幂等/冲突检查与 G1–G4 守卫写入 M2 五表；若已存在同 `run_key` 但 `content_sha256`、归一化 `weights` 或 `execution_binding` 不一致，立即报 `RUN_VERSION_CONFLICT` 中止事务；
   - 执行 M3 幂等与冲突守卫：若 `evolution_runs` 中已存在同 `evolution_run_id` 且 `content_sha256`（或 `tracking_run_key`）不相等，立即触发 `error('EVOLUTION_RUN_VERSION_CONFLICT: evolution_run_id=' || ...)` 中止事务；若相等则幂等跳过插入并正常提交；
   - 按外键顺序插入 `evolution_runs` → `evolution_nodes` → `evolution_transitions` → `evolution_edges` → `evolution_events` → `evolution_branch_summaries`；
   - 提交前依次执行 [data-model.md](../data-model.md) 第 3.3 节定义的 **`E1`–`E8` 八条全局不变量查询**，任一违反即调用 `error()` 中止事务；捕获错误后执行 `ROLLBACK;`，确保 0 残留且同一会话后续合法回放正常成功。
