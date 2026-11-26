# M3 Rust Core 情景演化公共接口契约

版本：`m3.evolve.v1`。目标 crate：`ensabc-core`（在保持 M1 `cluster(&TaskInput)` 及 M2 `ensemble_dispersion`、`match_scenarios`、`match_scenarios_scaled` 公共接口、类型与行为 100% 不变的前提下增量添加 `evolution` 模块）。对应 FR-002–FR-008、FR-011–FR-013、FR-017；数据结构定义见 [data-model.md](../data-model.md)，数学公式与定理证明见 [numerics.md](numerics.md)。

---

## 1. 公共常量、函数与模块边界

在 `crates/ensabc-core/src/` 下新增 `evolution.rs`（复用 `fsum.rs` 及 `matching.rs` 中的 `pub(crate) fn canonical_scenario_order` 与位掩码 DP 求解辅助），并在 `lib.rs` 中导出：

```rust
pub const EVOLUTION_ALGORITHM_VERSION: &str = "m3.evolve.v1";
pub const SECONDARY_RULE_V1: &str = "nearest_feasible_v1";
pub const FLOW_RULE_V1: &str = "star_min_mass_proportional_v1";
pub const UNCERTAINTY_RULE_V1: &str = "m3.uncertainty.v1";
pub const NON_PHYSICAL_DISCLAIMER: &str =
    "algorithmic_attribution_only_not_physical_member_migration";

pub fn evolve_scenarios(
    input: &EvolutionInput,
) -> Result<EvolutionResult, CoreError>;

pub fn evolve_scenarios_scaled(
    input: &ScaledEvolutionInput,
) -> Result<EvolutionResult, CoreError>;

pub fn evolve_chain(
    input: &EvolutionChainInput,
) -> Result<EvolutionChainResult, CoreError>;
```

### 1.1 纯函数与解耦边界
- 三个函数均只借用内存切片与结构体，无文件 I/O、无数据库或网络访问、不读取环境变量或系统时钟、不依赖 DuckDB / Arrow / Python。
- **M1 / M2 零侵入保证**：
  - 不修改 `ScenarioConfig`、`ClusterScenario`、`ScenarioNodeInput`、`MatchInput`、`ScaledMatchInput`、`MatchResult` 的字段或语义；
  - `match_scenarios` 继续忽略 `ScenarioNodeInput.member_count` 与 `member_fraction`，使 M2 `match_ext.rs` 与全部既有测试保持原样通过；成员数与占比合法性仅在 `evolve_scenarios*` 与 `evolve_chain` 内校验。
- **主干核验与复用模式（`backbone: Option<BackboneAssertion>`）**：
  - 当 `backbone = None` 时，`evolve_scenarios` / `evolve_scenarios_scaled` 内部先调用 `match_scenarios` / `match_scenarios_scaled` 求得全局最优主干 `MatchResult`，再构建次级边、五类事件与三族指标；
  - 当 `backbone = Some(assertion)` 时（例如 SQL `ens_evolve_scenarios` 传入上游 `ens_match_scenarios` 输出，或单元测试注入篡改主干测试拒绝行为），core 先重算距离矩阵与全局最优匹配，并严格核验 `assertion` 的维度、`cluster_id` 序列、阈值相等性、距离/可行矩阵一致性、一对一唯一性、`d <= threshold` 及全局最优目标 `(edge_count, total_distance)` 一致性；任一不符立即返回明确错误码，绝不静默采信非法主干。
- **纯内存整链编排 `evolve_chain`**：接收 4–8 个计划起报批次（每批状态为 `Ok(Vec<ScenarioNodeInput>, dispersion)`、`Missing` 或 `Failed { code, message }`）、权重、门槛比例系数与 `EvolutionConfig`，在内存中独立完成相邻 `ok` 跃迁演化、缺口断链（生成 `GapInterrupted` / `GapRestarted`）、M2 规则一致的确定性 `branch_id` 分配、分支汇总 `EvolutionBranchSummary` 与全链汇总 `EvolutionChainSummary`，供 `m3_runner --mode chain` 与 SQL 历史表进行逐字段交叉核验。

---

## 2. 错误码扩展与 `CoreError` 大小硬约束

### 2.1 `clippy::result_large_err` 字节预算不变量
`crates/ensabc-core` 开启无豁免的 `cargo clippy --locked --all-targets -- -D warnings`（`result_large_err` 上限为 `128` 字节）。在 64 位平台上，`CoreError` 当前精确占 **`112` 字节**（`Result<T, CoreError>` 占 `120` 字节）。因此：
- **禁止**在 `CoreError` 结构体上新增任何字段；
- 仅在 1 字节枚举 `CoreErrorCode` 中新增以下 M3 单元变体（新增 `0` 字节），位置定位统一复用已有的 `prev_index: Option<u32>`、`curr_index: Option<u32>` 与 `feature_index: Option<usize>`；
- 在 `crates/ensabc-core/tests/m3_evolution_contract.rs` 中添加编译期/运行期断言 `assert!(std::mem::size_of::<CoreError>() <= 112)`。

### 2.2 M3 新增错误码清单（叠加在 M1/M2 既有错误码之上）

| 错误码 (`CoreErrorCode`) | 触发条件 | 位置字段填写约定 |
| --- | --- | --- |
| `InvalidScenarioId` | CLI / 外部反序列化传入负数 `cluster_id < 0` | 前批填 `prev_index`，后批填 `curr_index` |
| `ZeroMemberScenario` | 任一情景 `member_count <= 0` | 前批填 `prev_index`，后批填 `curr_index` |
| `InvalidMemberFraction` | 任一情景 `member_fraction` 非有限、`<= 0.0` 或 `> 1.0` | 前批填 `prev_index`，后批填 `curr_index` |
| `InvalidFractionSum` | 单侧全部情景 `\|fsum(member_fraction) - 1.0\| > config.fraction_sum_tolerance` | 前批违约填 `prev_index = Some(0)`，后批填 `curr_index = Some(0)` |
| `InvalidEvolutionConfig` | `fraction_sum_tolerance` 非有限/`< 0.0`/`> 1e-3`，或任一 `high_uncertainty_*_ratio` 非有限/`< 0.0`，或整链计划批次数不在 `4..=8`，或整链 `Failed` 批次携带非 8 类白名单错误码 | 填写违约批次下标（整链批次级）或在 `message` 中指明非法字段名 |
| `InvalidPrimaryLink` | `BackboneAssertion.primary_links` 引用不存在的 `cluster_id`、违反一对一唯一性，或某条主干边 `d > threshold` | 填写对应 `prev_index` / `curr_index` |
| `BackboneInconsistency` | `BackboneAssertion` 的 `cluster_id` 轴顺序、`threshold`、`distance_matrix`、`eligible_matrix` 与实际重算结果不符，或未达到全局最优目标 `(q*, Σ*)` | 填写首个不一致矩阵单元 `(prev_index, curr_index)`（若为轴/全局目标不符则留 `None`） |

*注*：空情景列表、超过 12 个情景、重复 `cluster_id`、特征不等长、非有限特征、非法权重、负门槛、零离散与中间 `fsum` 溢出继续返回 M2 既有错误码（`EmptyScenarios`、`TooManyScenarios`、`DuplicateScenarioId`、`EmptyFeatures`/`RaggedFeatures`、`NonFiniteInput`、`InvalidWeights`、`InvalidThreshold`、`ZeroDispersion`、`NumericalFailure`）。

---

## 3. 独立命令行验证程序 `examples/m3_runner.rs`

与 `m1_runner.rs`、`m2_runner.rs` 并列，提供完全脱离 DuckDB 与 Python 的原生 CLI 执行入口（用于合成穷举 oracle 对照、正交分离测试、置换不变性验证与 `--network none` 最小容器核验）：

```bash
cargo run --locked --release -p ensabc-core --example m3_runner -- \
  --mode pair|chain \
  --input /path/input.json \
  --output /path/output.json
```

- **精确浮点序列化**：`serde_json` 启用 `features = ["std", "float_roundtrip"]`，消除十进制双精度文本往返的 1 ulp 解析误差；支持测试输入中的 `"NaN"` / `"Infinity"` / `"-Infinity"` 令牌以触发 core 非有限值拒绝测试；输出采用临时文件原子重命名写入。
- **`--mode pair`**：支持单条或批量 `EvolutionInput` / `ScaledEvolutionInput`（含可选 `backbone` 断言），逐项输出 `EvolutionResult` 或结构化错误码/位置。
- **`--mode chain`**：读取本地多批成员矩阵（或单批情景快照）+ M1 聚类配置 + M2 匹配配置 + M3 演化配置，在纯 Rust 中依次执行（或消费）单批聚类/离散与 `evolve_chain`，输出完整的节点、跃迁、多对多边、事件、分支汇总与全链汇总 JSON，不得接收预期身份或预期事件答案作为输入。
