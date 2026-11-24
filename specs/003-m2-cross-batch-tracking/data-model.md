# M2 数据模型

本文件定义 M2 跨批次情景追踪的实体、字段、校验规则与状态迁移。数值公式见 [numerics.md](contracts/numerics.md)，Rust 核心接口见 [core.md](contracts/core.md)，OM 读取与 DuckOMO 联调见 [om-duckomo.md](contracts/om-duckomo.md)，SQL 函数与历史表 DDL 见 [sql-history.md](contracts/sql-history.md)。

## 1. 与 M1 实际代码的接口对齐说明

M2 复用 M1 已验收的 `ensabc-core` 与 `ensabc-duckdb`，不修改其既有签名或算法行为。以下记录 M1 设计文档与实际实现之间的已知命名差异，M2 统一以**实际代码**为准：

| 概念 | M1 文档旧称 | M1 实际实现（M2 引用名） | 说明 |
| --- | --- | --- | --- |
| 成员输入 | `MemberInput { member_id, features }` | `MemberInput { member_index, member_id, features }` | SQL 适配器按输入数组下标赋 `0..n-1` |
| 单簇摘要类型 | `ScenarioSummary` | `ClusterScenario` | 含 `mean_features: Vec<f64>`（原始物理空间均值） |
| 簇内误差字段 | `within_cluster_sse` | Rust 为 `cluster_sse`；SQL 输出 STRUCT 为 `within_cluster_sse` | M2 仅透传/记录，不参与跨批次距离 |
| 诊断与错误码 | `Diagnostics` / `ErrorCode` | `ScenarioDiagnostics` / `CoreErrorCode` | `distance_space` 实际返回 `"projected_scores_v1"` |

## 2. OM 样本与追踪任务上下文（调用层持有，不进入数值 core）

### 2.1 `OmSampleManifest`（`schema_version = "m2.om_sample.v1"`）

- `sample_id`: `"wb2_ifs_ens_20200105t00z_8runs_t2m_eu3x3"`
- `kind`: 固定为 `"real_ensemble_converted_to_om"`（FR-014 要求，禁止标为原生公开 OM）
- `source_dataset_uri`: WB2 IFS ENS Zarr 根路径
- `zmetadata_sha256`、`coordinate_chunk_sha256`: 源元数据与 5 个坐标块的 SHA-256
- `model`: `"ifs_ens"`，`variable`: `"2m_temperature"`，`unit`: `"K"`，`feature_version`: `"m2.eu3x3.lat_lon_major.v1"`
- `valid_time`: `"2020-01-05T00:00:00Z"`，`planned_interval_hours`: `12`，`planned_batch_count`: `8`
- `points`: 9 个有序格点 `{point_index: 0..8, latitude, longitude, source_lat_index, source_lon_index, cos_lat_weight, cos_lat_weight_hex}`，按纬度升序（`42.1875, 47.8125, 53.4375`）为主序、经度升序（`5.625, 11.25, 16.875`）为次序
- `members`: 50 个扰动成员 `{member_index: 0..49, member_label: 1..50, member_id: "ifs_ens_01".."ifs_ens_50", role: "perturbed"}`
- `om_format`: `{writer: "omfiles 1.2.0", writer_wheel_sha256, om_file_format_core: "18bee85", value_compression: "fpx_xor_2d", scale_factor: 1.0, add_offset: 0.0, dtype: "float32", layout: "per_batch", value_array: "/temperature_2m", axes: ["member", "lat", "lon"], chunks: [50, 3, 3]}`
- `batches`: 8 项有序批次记录，每项含 `ordinal (0..7), init_time, lead_hours (96..12), valid_time, source_object, source_object_generation, source_object_bytes, source_object_sha256, payload_file, payload_sha256, om_file, om_bytes, om_sha256, status ("ok" | "missing" | "failed")`

**准备阶段校验规则（FR-002、FR-013、FR-014）**：
- 所有可用批次的 `model, variable, unit, feature_version, valid_time` 与 9 个格点坐标、`grid_id`、50 个成员标签完全一致；
- `ordinal` 为 `0..planned_batch_count-1` 连续整数，`init_time[k] = init_time[0] + k * planned_interval_hours`，且每批 `init_time + lead_hours == valid_time`；
- 每批 `read_om` 必须恰好返回 `50 × 9 = 450` 行，`NULL` 计数为 0，非有限值（`±Inf`）计数为 0；任一不满足则在进入距离计算前整批拒绝。

## 3. Rust Core 匹配与离散数据结构（`ensabc-core`）

### 3.1 单批集合离散输入与输出

- 输入：`members: &[MemberInput]`（或二维切片，要求 $N \ge 2, p \ge 1$，成员 ID 非空唯一，特征等长有限）、`weights: &[f64]`（长度 $p$，有限、非负、$\operatorname{fsum}(w) > 0$）。
- 输出：`f64`（单批集合离散 $s$，单位与输入特征相同）。若 $s \le 0$ 报 `ZERO_DISPERSION`，中间溢出或非有限报 `NUMERICAL_FAILURE`。

### 3.2 两批匹配输入 `ScenarioNodeInput`、`MatchInput` 与 `ScaledMatchInput`

```rust
pub struct ScenarioNodeInput {
    pub cluster_id: usize,
    pub mean_features: Vec<f64>,
}

pub struct MatchInput {
    pub prev_scenarios: Vec<ScenarioNodeInput>,
    pub curr_scenarios: Vec<ScenarioNodeInput>,
    pub weights: Vec<f64>,
    pub threshold: f64,               // 绝对门槛 τ (K)，有限且 >= 0
}

pub struct ScaledMatchInput {
    pub prev_scenarios: Vec<ScenarioNodeInput>,
    pub curr_scenarios: Vec<ScenarioNodeInput>,
    pub weights: Vec<f64>,
    pub threshold_coefficient: f64,   // 比例系数 c，有限且 >= 0
    pub prev_dispersion: f64,         // 前批离散 s_prev，有限且 > 0
    pub curr_dispersion: f64,         // 后批离散 s_curr，有限且 > 0
}
```

**输入校验规则**：
- `prev_scenarios` 与 `curr_scenarios` 均非空，且各自长度 $\in [1, 12]$（超出 12 报 `TOO_MANY_SCENARIOS`）；
- 单侧内 `cluster_id` 唯一（不要求连续 `0..k-1`，以便测试任意编号置换），否则报 `DUPLICATE_SCENARIO_ID`；
- 所有 `mean_features` 非空、长度均等于 `weights.len()`、所有元素有限；
- `weights` 非空、所有元素有限且 $\ge 0$，$\operatorname{fsum}(w) > 0$；
- `threshold`（或 `threshold_coefficient`）有限且 $\ge 0$；`ScaledMatchInput` 中 `prev_dispersion` 与 `curr_dispersion` 必须有限且 $> 0$（$=0$ 报 `ZERO_DISPERSION`）。

绝对输入 `MatchInput` 不接收尺度字段。其结果中的 `pair_scale`、`threshold_coefficient`、两批离散及全部比值字段为 `None`；`ScaledMatchInput` 的三个尺度参数必填，换算后复用相同内核，并补齐结果尺度/比值（均为 `Some`）。不得允许部分尺度参数或输入 threshold 与换算 threshold 相互矛盾的组合。

### 3.3 两批匹配输出 `MatchResult`

| 字段 | 类型 | 含义与不变量 |
| --- | --- | --- |
| `algorithm_version` | `String` | 固定 `"m2.match.v1"` |
| `prev_cluster_ids` | `Vec<usize>` | 按前批输入顺序保存的 `cluster_id` 列表（长 $m$） |
| `curr_cluster_ids` | `Vec<usize>` | 按后批输入顺序保存的 `cluster_id` 列表（长 $n$） |
| `threshold` | `f64` | 生效的绝对门槛 $\tau$ |
| `pair_scale` | `Option<f64>` | 相邻批次离散尺度 $S = (s_{\text{prev}} + s_{\text{curr}})/2$ |
| `threshold_coefficient` | `Option<f64>` | 比例系数 $c$ |
| `prev_dispersion` / `curr_dispersion` | `Option<f64>` | 前后批各自的集合离散 |
| `distance_matrix` | `Vec<Vec<f64>>` | $m \times n$ 候选加权 RMSE 矩阵，行列对应输入顺序 |
| `distance_ratio_matrix` | `Option<Vec<Vec<f64>>>` | 当 `pair_scale` 存在时为 $d_{i,j} / S$，仅作展示 |
| `eligible_matrix` | `Vec<Vec<bool>>` | $m \times n$ 可行标记，严格等于 `distance_matrix[i][j] <= threshold` |
| `edge_count` | `usize` | 选中关联条数 $q \in [0, \min(m,n)]$ |
| `total_distance` | `f64` | 选中关联距离的 Shewchuk 精确和（正确舍入一次；$q=0$ 时为 `0.0`） |
| `optimal_solution_count` | `u64` | 达到 `(edge_count, 精确距离和)` 的可行匹配方案总数（$\ge 1$） |
| `ambiguous` | `bool` | 结对级完全并列标记，等价于 `optimal_solution_count > 1` |
| `threshold_margin` | `f64` | $\min_{i,j} |d_{i,j} - \tau|$ |
| `optimality_margin` | `Option<f64>` | 所有 `same_count` 选中边的最小 `distance_increase`；若存在 `tied` 边则为 `Some(0.0)`；若所有选中边均为 `count_drop` 或 $q=0$ 则为 `None` |
| `links` | `Vec<ScenarioLink>` | 选中关联列表（长 $q$），按 `prev_cluster_id` 升序排列 |
| `unmatched_prev` | `Vec<UnmatchedScenario>` | 前批未关联情景（长 $m - q$），按 `cluster_id` 升序排列 |
| `unmatched_curr` | `Vec<UnmatchedScenario>` | 后批未关联情景（长 $n - q$），按 `cluster_id` 升序排列 |

#### `ScenarioLink`（单条选中关联）

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `prev_cluster_id` / `curr_cluster_id` | `usize` | 前后批情景编号（在 `links` 中各自唯一） |
| `distance` | `f64` | 该关联的加权 RMSE $d$（满足 $d \le \tau$） |
| `distance_ratio` | `Option<f64>` | $d / S$（若提供尺度） |
| `alternative_status` | `AlternativeStatus` | `Tied` \| `SameCount` \| `CountDrop` |
| `alternative_edge_count` | `usize` | 禁用该关联后重求的最优条数 $q'$（`Tied`/`SameCount` 为 $q$，`CountDrop` 为 $q-1$） |
| `alternative_total_distance` | `f64` | 禁用该关联后重求的最优总距离 $\Sigma'$ |
| `alternative_distance_increase` | `Option<f64>` | 当 $q' = q$ 时为 $\operatorname{round}(\Sigma' - \Sigma)$（`Tied` 为 `0.0`，`SameCount` $> 0$）；`CountDrop` 为 `None` |
| `alternative_distance_increase_ratio` | `Option<f64>` | 上述增量除以 $S$ |
| `ambiguous` | `bool` | 该边是否可被另一完全并列最优解替换（等价于 `alternative_status == Tied`） |

#### `UnmatchedScenario`（未关联情景）

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `cluster_id` | `usize` | 该侧情景编号 |
| `reason` | `UnmatchedReason` | `NoCandidateWithinThreshold`（可行度为 0）\| `NotSelectedInGlobalOptimum`（有可行边但未入选） |
| `min_candidate_distance` | `Option<f64>` | 该情景到对侧所有候选的最小距离 |
| `matched_in_some_optimum` | `bool` | 是否在某个完全并列最优解中被关联 |

## 4. 持续身份与 DuckDB 历史表模型（FR-007–FR-010）

### 4.1 身份状态迁移规则

对计划起报序列 $b = 0, 1, \dots, B-1$（其中 $4 \le B \le 8$）：
1. **批次状态**：`ok`（数据可用、单批聚类成功、集合离散有限且 $>0$）、`missing`（计划批次缺失）、`failed`（输入合法且已分类的单批算法失败，如秩不足、无法形成指定簇数、未收敛或离散为 0；保存错误码及位置）。`missing` 与 `failed` 批次在 `scenario_nodes` 中有 0 行。
2. **相邻比较**：仅当计划批次 $b-1$ 与 $b$ 状态**均为 `ok`** 时，才在 `pair_comparisons` 中生成一条 `(prev_ordinal = b-1, curr_ordinal = b)` 记录。若任一侧非 `ok`，则不跨越缺口做比较。
3. **身份分配（每个 `ok` 批次的每个情景恰有一个 `branch_id`）**：
   - 当 $b = 0$ 时：`assignment_reason = 'first_batch'`，分配新 `branch_id`；
   - 当 $b > 0$ 且批次 $b-1$ 状态非 `ok` 时：`assignment_reason = 'previous_planned_batch_unavailable'`，该批所有情景均分配新 `branch_id`（断链）；
   - 当 $b > 0$ 且批次 $b-1$ 状态为 `ok` 时：
     - 若该情景在 `(b-1, b)` 的 `links` 中有前驱 $p$：继承 $p$ 的 `branch_id`；若该关联 `ambiguous = true`，`assignment_reason = 'inherited_ambiguous'`，否则为 `'inherited'`；
     - 若该情景在 `unmatched_curr` 中：分配新 `branch_id`，`assignment_reason` 取其未关联原因（`'no_candidate_within_threshold'` 或 `'not_selected_in_global_optimum'`）。
4. **确定性 `branch_id` 格式**：`<run_key前12位>:b<出生ordinal两位>:k<出生规范序两位>`（例如 `a1b2c3d4e5f6:b00:k01`），其中出生规范序为该情景在出生批次全部情景中按 `mean_features` 规范键升序排列的零基位置。

元数据不兼容、非法输入、来源或扩展身份核验失败不归入可发布的 failed 批次；与匹配错误、未知执行错误或发布失败一样，阻止整次发布。TEMP 失败记录用于诊断，不是正式回放版本。

### 4.2 五张正式历史表关系与不变量

- `tracking_runs (PK: run_key)`
  - 1 对多 → `replay_batches (PK: run_key, ordinal; UNIQUE: run_key, init_time)`
  - `replay_batches` 1 对多 → `scenario_nodes (PK: run_key, ordinal, cluster_id; UNIQUE: run_key, ordinal, branch_id)`
  - `replay_batches` 1 对 0..1 → `pair_comparisons (PK: run_key, prev_ordinal; UNIQUE: run_key, curr_ordinal; CHECK: curr_ordinal = prev_ordinal + 1)`
  - `pair_comparisons` + `scenario_nodes` → `scenario_links (PK: run_key, prev_ordinal, prev_cluster_id; UNIQUE: run_key, curr_ordinal, curr_cluster_id)`

`run_key` 绑定来源内容、配置、权重和实际软件身份，不含个人路径或获取时间。原始 manifest 字节 hash 用于证据核对；版本绑定采用排除路径/获取时间后的语义输入摘要，避免同内容恢复产生伪版本。`content_sha256` 按固定字段顺序和主键排序、-0 归一及有限 f64 round-trip 序列化计算，并覆盖全部子表内容。

同一二进制的三进程重复要求完整身份精确一致。同有效源码新 clone 重建或跨 DuckOMO 版本比较时，独立验证各自构建 hash 和 run_key，再按出生节点键比较完整继承链；只允许归一经验证由版本身份派生的前缀，不能任意重新编号。详见 [验证证据契约](contracts/validation-evidence.md)。

**事务提交前必须满足的 4 条全局不变量**：
1. 节点完整性：`scenario_nodes` 行数等于所有 `status='ok'` 批次的情景总数；
2. 无断链复活：对同一 `run_key` 下的每个 `branch_id`，其出现的 `ordinal` 集合必须是连续整数区间（`max(ordinal) - min(ordinal) + 1 == count(*)`）；
3. 相邻比较完整性：每一对满足 `b.ordinal = a.ordinal + 1` 且两侧 `status='ok'` 的批次，在 `pair_comparisons` 中恰有一条记录；任何含 `missing`/`failed` 的相邻对在 `pair_comparisons` 中有 0 条记录；
4. 身份传递一致性：`scenario_links` 中每条关联连接的 `prev` 节点与 `curr` 节点必须具有相同的 `branch_id`。
