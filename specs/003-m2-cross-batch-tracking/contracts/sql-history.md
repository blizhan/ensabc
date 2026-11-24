# M2 SQL 扩展接口与 DuckDB 历史保存契约

版本：`m2.sql.v1`。目标宿主：DuckDB CLI `v1.5.5` / Linux aarch64。对应 FR-007–FR-012、SC-002–SC-005。

## 1. 新增无状态 SQL 标量函数（`crates/ensabc-duckdb`）

在同一扩展（`ensabc_m0_extension.duckdb_extension`）中保留 `ens_m0_echo` 与 `ens_cluster` 完全不变，新增以下两个 C API 标量函数（均启用 `duckdb_scalar_function_set_special_handling` 以拦截顶层 `NULL`）：

### 1.1 `ens_dispersion`

```sql
ens_dispersion(
  members STRUCT(member_id VARCHAR, features DOUBLE[])[],
  weights DOUBLE[]
) -> DOUBLE
```

- 语义：调用 `ensabc_core::ensemble_dispersion` 计算单批全体成员相对集合平均场的空间加权 RMS 偏差 $s$。
- 错误前缀：`ens_dispersion [CODE] chunk_row=N: ...`。

### 1.2 `ens_match_scenarios`

```sql
ens_match_scenarios(
  prev_scenarios STRUCT(cluster_id BIGINT, mean_features DOUBLE[])[],
  curr_scenarios STRUCT(cluster_id BIGINT, mean_features DOUBLE[])[],
  weights DOUBLE[],
  config STRUCT(
    threshold_coefficient DOUBLE,
    prev_dispersion DOUBLE,
    curr_dispersion DOUBLE
  )
) -> STRUCT(
  algorithm_version VARCHAR,
  prev_cluster_ids BIGINT[],
  curr_cluster_ids BIGINT[],
  prev_dispersion DOUBLE,
  curr_dispersion DOUBLE,
  pair_scale DOUBLE,
  threshold_coefficient DOUBLE,
  threshold DOUBLE,
  distance_matrix DOUBLE[][],
  distance_ratio_matrix DOUBLE[][],
  eligible_matrix BOOLEAN[][],
  edge_count BIGINT,
  total_distance DOUBLE,
  optimal_solution_count BIGINT,
  ambiguous BOOLEAN,
  threshold_margin DOUBLE,
  optimality_margin DOUBLE,
  has_optimality_margin BOOLEAN,
  links STRUCT(
    prev_cluster_id BIGINT,
    curr_cluster_id BIGINT,
    distance DOUBLE,
    distance_ratio DOUBLE,
    alternative_status VARCHAR,
    alternative_edge_count BIGINT,
    alternative_total_distance DOUBLE,
    alternative_distance_increase DOUBLE,
    alternative_distance_increase_ratio DOUBLE,
    has_same_count_alternative BOOLEAN,
    ambiguous BOOLEAN
  )[],
  unmatched_prev STRUCT(
    cluster_id BIGINT,
    reason VARCHAR,
    min_candidate_distance DOUBLE,
    matched_in_some_optimum BOOLEAN
  )[],
  unmatched_curr STRUCT(
    cluster_id BIGINT,
    reason VARCHAR,
    min_candidate_distance DOUBLE,
    matched_in_some_optimum BOOLEAN
  )[]
)
```

- **非 NULL 输出约定**：与 M1 `ens_cluster` 一致，成功返回结构体的所有字段与数组元素均非 `NULL` 且有限：
  - 当 `alternative_status = 'count_drop'` 时，`has_same_count_alternative = false`，`alternative_distance_increase` 与 `alternative_distance_increase_ratio` 写 `0.0`；当 `alternative_status IN ('tied', 'same_count')` 时，`has_same_count_alternative = true`；
  - 当结对不存在任何 `same_count` 或 `tied` 选中边时，`has_optimality_margin = false` 且 `optimality_margin = 0.0`。
- **绝对门槛直接调用约定**：当合成用例需要直接传入绝对门槛 $\tau$（例如反例 `[[0,4],[4,6]]`, $\tau=5$）时，传 `prev_dispersion = 1.0, curr_dispersion = 1.0, threshold_coefficient = τ`，此时 $S = 1.0, \tau = \text{threshold\_coefficient}$，与 core `match_scenarios` 完全等价。
- **无状态与错误隔离**：每行独立计算，不访问数据库表、不生成 `branch_id`。任一行或任一嵌套元素为 `NULL`、形状不合法、含 `NaN/±Inf`、重复 `cluster_id`、非法权重或零离散时，整个 chunk 报错（前缀 `ens_match_scenarios [CODE] chunk_row=N: ...`），不写出半成品，同一连接后续合法查询正常执行。

## 2. 五张正式历史表 DDL

```sql
CREATE TABLE IF NOT EXISTS tracking_runs (
    run_key             VARCHAR PRIMARY KEY,
    binding             STRUCT(
                          model VARCHAR, variable VARCHAR, unit VARCHAR, region_id VARCHAR,
                          grid_sha256 VARCHAR, feature_version VARCHAR, valid_time TIMESTAMP,
                          planned_interval_hours INTEGER, planned_batch_count INTEGER,
                          input_manifest_sha256 VARCHAR,
                          scenario_config_sha256 VARCHAR, match_config_sha256 VARCHAR,
                          weights_version VARCHAR, threshold_coefficient DOUBLE,
                          cluster_count INTEGER,
                          software STRUCT(
                            ensabc_extension_sha256 VARCHAR,
                            duckomo_version VARCHAR,
                            duckomo_sha256 VARCHAR,
                            duckdb_version VARCHAR,
                            cluster_algorithm_version VARCHAR,
                            match_algorithm_version VARCHAR
                          )
                        ) NOT NULL,
    weights             DOUBLE[] NOT NULL,
    planned_batch_count INTEGER NOT NULL CHECK (planned_batch_count BETWEEN 4 AND 8),
    content_sha256      VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS replay_batches (
    run_key               VARCHAR NOT NULL REFERENCES tracking_runs(run_key),
    ordinal               INTEGER NOT NULL CHECK (ordinal >= 0),
    init_time             TIMESTAMP NOT NULL,
    lead_hours            INTEGER NOT NULL,
    status                VARCHAR NOT NULL CHECK (status IN ('ok', 'missing', 'failed')),
    status_reason         VARCHAR,
    input_sha256          VARCHAR,
    om_sha256             VARCHAR,
    dispersion            DOUBLE CHECK (dispersion IS NULL OR (isfinite(dispersion) AND dispersion > 0)),
    cluster_result_sha256 VARCHAR,
    PRIMARY KEY (run_key, ordinal),
    UNIQUE (run_key, init_time),
    CHECK ((status = 'ok') = (dispersion IS NOT NULL)),
    CHECK (status = 'ok' OR status_reason IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS scenario_nodes (
    run_key                  VARCHAR NOT NULL,
    ordinal                  INTEGER NOT NULL,
    cluster_id               INTEGER NOT NULL CHECK (cluster_id >= 0),
    canonical_rank           INTEGER NOT NULL CHECK (canonical_rank >= 0),
    mean_features            DOUBLE[] NOT NULL,
    member_count             INTEGER NOT NULL CHECK (member_count > 0),
    member_fraction          DOUBLE NOT NULL CHECK (member_fraction > 0 AND member_fraction <= 1),
    representative_member_id VARCHAR NOT NULL,
    within_cluster_sse       DOUBLE NOT NULL CHECK (isfinite(within_cluster_sse) AND within_cluster_sse >= 0),
    branch_id                VARCHAR NOT NULL,
    assignment_reason        VARCHAR NOT NULL CHECK (assignment_reason IN (
                               'first_batch', 'inherited', 'inherited_ambiguous',
                               'no_candidate_within_threshold', 'not_selected_in_global_optimum',
                               'previous_planned_batch_unavailable')),
    PRIMARY KEY (run_key, ordinal, cluster_id),
    UNIQUE (run_key, ordinal, branch_id),
    FOREIGN KEY (run_key, ordinal) REFERENCES replay_batches(run_key, ordinal)
);

CREATE TABLE IF NOT EXISTS pair_comparisons (
    run_key        VARCHAR NOT NULL,
    prev_ordinal   INTEGER NOT NULL,
    curr_ordinal   INTEGER NOT NULL,
    pair_scale     DOUBLE NOT NULL CHECK (isfinite(pair_scale) AND pair_scale > 0),
    threshold      DOUBLE NOT NULL CHECK (isfinite(threshold) AND threshold >= 0),
    match_result   STRUCT(
                     algorithm_version VARCHAR,
                     prev_cluster_ids BIGINT[],
                     curr_cluster_ids BIGINT[],
                     prev_dispersion DOUBLE,
                     curr_dispersion DOUBLE,
                     pair_scale DOUBLE,
                     threshold_coefficient DOUBLE,
                     threshold DOUBLE,
                     distance_matrix DOUBLE[][],
                     distance_ratio_matrix DOUBLE[][],
                     eligible_matrix BOOLEAN[][],
                     edge_count BIGINT,
                     total_distance DOUBLE,
                     optimal_solution_count BIGINT,
                     ambiguous BOOLEAN,
                     threshold_margin DOUBLE,
                     optimality_margin DOUBLE,
                     has_optimality_margin BOOLEAN,
                     links STRUCT(
                       prev_cluster_id BIGINT, curr_cluster_id BIGINT,
                       distance DOUBLE, distance_ratio DOUBLE,
                       alternative_status VARCHAR, alternative_edge_count BIGINT,
                       alternative_total_distance DOUBLE, alternative_distance_increase DOUBLE,
                       alternative_distance_increase_ratio DOUBLE,
                       has_same_count_alternative BOOLEAN, ambiguous BOOLEAN
                     )[],
                     unmatched_prev STRUCT(
                       cluster_id BIGINT, reason VARCHAR,
                       min_candidate_distance DOUBLE, matched_in_some_optimum BOOLEAN
                     )[],
                     unmatched_curr STRUCT(
                       cluster_id BIGINT, reason VARCHAR,
                       min_candidate_distance DOUBLE, matched_in_some_optimum BOOLEAN
                     )[]
                   ) NOT NULL,
    PRIMARY KEY (run_key, prev_ordinal),
    UNIQUE (run_key, curr_ordinal),
    CHECK (curr_ordinal = prev_ordinal + 1),
    FOREIGN KEY (run_key, prev_ordinal) REFERENCES replay_batches(run_key, ordinal),
    FOREIGN KEY (run_key, curr_ordinal) REFERENCES replay_batches(run_key, ordinal)
);

CREATE TABLE IF NOT EXISTS scenario_links (
    run_key            VARCHAR NOT NULL,
    prev_ordinal       INTEGER NOT NULL,
    prev_cluster_id    INTEGER NOT NULL,
    curr_ordinal       INTEGER NOT NULL,
    curr_cluster_id    INTEGER NOT NULL,
    distance           DOUBLE NOT NULL CHECK (isfinite(distance) AND distance >= 0),
    distance_ratio     DOUBLE NOT NULL CHECK (isfinite(distance_ratio) AND distance_ratio >= 0),
    alternative_status VARCHAR NOT NULL CHECK (alternative_status IN ('tied', 'same_count', 'count_drop')),
    ambiguous          BOOLEAN NOT NULL,
    PRIMARY KEY (run_key, prev_ordinal, prev_cluster_id),
    UNIQUE (run_key, curr_ordinal, curr_cluster_id),
    CHECK (curr_ordinal = prev_ordinal + 1),
    FOREIGN KEY (run_key, prev_ordinal) REFERENCES pair_comparisons(run_key, prev_ordinal),
    FOREIGN KEY (run_key, prev_ordinal, prev_cluster_id) REFERENCES scenario_nodes(run_key, ordinal, cluster_id),
    FOREIGN KEY (run_key, curr_ordinal, curr_cluster_id) REFERENCES scenario_nodes(run_key, ordinal, cluster_id)
);
```

## 3. 两阶段回放、递归 CTE 身份分配与原子发布协议

1. **阶段 A（TEMP 表暂存，autocommit，单批故障隔离）**：
   - 在 `TEMP` 表中写入计划批次清单 `stage_manifest(ordinal, init_time, lead_hours, availability)`；
   - 对每个 `availability = 'present'` 的批次，单独执行一条语句完成 `read_om` 门禁、`ens_cluster` 与 `ens_dispersion` 并写入 `stage_cluster`；若输入合法而发生已分类单批算法失败（秩不足、不能形成指定簇数、未收敛或零离散），在 `.bail off` 下仅该条语句报错，该批不进入 `stage_cluster`，随后记 `status='failed'` 并保存具体错误码/位置；元数据不兼容、非法输入、来源/扩展身份错误及任何未知错误均不属于该白名单，必须阻止发布；
   - 单批阶段结束后核对每条执行结果与错误白名单，并退出容错模式；对所有相邻且两侧均为 `status='ok'` 的批次对调用 `ens_match_scenarios`。匹配失败或应有比较缺失均禁止进入发布，不能因 TEMP 表无记录而视为缺批；
   - 用 `WITH RECURSIVE` 沿 `stage_links_all` 从每个无前驱节点（`first_batch`、`previous_planned_batch_unavailable`、`no_candidate_within_threshold`、`not_selected_in_global_optimum`）向下传递确定性 `branch_id`（`<run_key[:12]>:b<ordinal:02d>:k<canonical_rank:02d>`）。
2. **阶段 B（单事务原子发布，`BEGIN TRANSACTION ... COMMIT`）**：
   - 计算拟发布全部子表内容的规范摘要 `content_sha256`；
   - 检查 `tracking_runs`：若同 `run_key` 已存在且 `content_sha256` 不同，调用 `error('RUN_VERSION_CONFLICT ...')` 立即中止事务；若已存在且 `content_sha256` 相同，跳过插入并正常提交（幂等）；
   - 依次向五张表插入新版本记录；
   - 提交前执行 4 类完整性守卫（节点精确集合/数量一致、无断链复活、相邻 `ok` 比较及端点齐全、关联两端身份与实际继承一致），任一失败 error 并回滚整笔事务。错误后显式 `ROLLBACK`，同连接合法回放须恢复成功；非交互 CLI 因 bail 退出时检查退出码并核对数据库无该版本记录，不依赖最后一条 SELECT 判断成功。

### 3.1 发布状态与版本身份

发布成功表示计划状态完整、全部应执行比较及身份/历史守卫通过；允许明确 missing 和白名单算法 failed 状态，不表示所有批次均成功计算。匹配/发布错误及非白名单错误均使整个版本不发布。保留 TEMP/日志诊断不算正式历史残留。

binding 按固定 STRUCT 字段顺序构造，所有语义列表显式 ORDER BY，有限 f64 使用 round-trip 表示并归一 -0。`input_manifest_sha256` 指排除个人路径/获取时间的规范语义输入摘要，原始 manifest 文件 hash 单独存证；run_key 同时绑定权重和实际扩展身份。content 摘要覆盖所有子表，按表种类/主键排序且每行固定字段顺序。SQL 与驱动交叉核对同一序列化定义，不以两种默认 JSON 输出恰好相同作假设。

同一构建重复精确比较 run_key/branch_id；独立重建、双版本和扰动按验证契约分别核验来源/构建身份，再以出生节点键比较继承链。
