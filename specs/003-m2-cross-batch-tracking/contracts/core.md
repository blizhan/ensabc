# M2 Rust Core 匹配与离散公共接口契约

版本：`m2.match.v1`。目标 crate：`ensabc-core`（在保持 M1 `cluster(&TaskInput)` 公共接口、类型与行为完全不变的前提下新增模块）。对应 FR-003–FR-006、FR-011–FR-012、FR-016、FR-018；数据结构见 [data-model.md](../data-model.md)，公式与优化定义见 [numerics.md](numerics.md)。

## 1. 公共函数与模块边界

在 `crates/ensabc-core/src/` 下新增 `dispersion.rs`、`matching.rs`（以及内部精确浮点求和辅助），并在 `lib.rs` 中导出：

```rust
pub const MATCH_ALGORITHM_VERSION: &str = "m2.match.v1";
pub const MAX_SCENARIOS_PER_SIDE: usize = 12;

pub fn ensemble_dispersion(
    members: &[MemberInput],
    weights: &[f64],
) -> Result<f64, CoreError>;

pub fn match_scenarios(
    input: &MatchInput,
) -> Result<MatchResult, CoreError>;

pub fn match_scenarios_scaled(
    input: &ScaledMatchInput,
) -> Result<MatchResult, CoreError>;
```

- **纯函数边界**：三个函数均只借用输入切片/结构体，不读取数据库、不访问网络、不读取环境变量或系统时钟、不分配跨批次 `branch_id`、不依赖 DuckDB/Arrow/Python。
- `MatchInput` 仅包含前后场列表、权重和绝对 threshold，不接收可选尺度参数；绝对模式输出的尺度上下文与比值均为 `None`。
- `match_scenarios_scaled` 先校验 `prev_dispersion`、`curr_dispersion`（有限且 $>0$）与 `threshold_coefficient`（有限且 $\ge 0$），按 `pair_scale = (prev_dispersion + curr_dispersion) / 2.0`、`threshold = threshold_coefficient * pair_scale` 计算绝对门槛后，委托 `match_scenarios` 执行，再补齐两批离散、S、c 及所有比值。比例输入三参数必须完整，不能与另一个绝对 threshold 同时提供。

## 2. 错误码与校验分工

复用或扩展 `CoreError`（保持现有 M1 错误码不变，新增 M2 专属错误码变体，位置字段补充 `prev_index: Option<usize>`、`curr_index: Option<usize>`）：

| 错误码 (`CoreErrorCode`) | 触发条件 |
| --- | --- |
| `EMPTY_SCENARIOS` | `prev_scenarios` 或 `curr_scenarios` 为空 |
| `TOO_MANY_SCENARIOS` | `prev_scenarios.len() > 12` 或 `curr_scenarios.len() > 12` |
| `DUPLICATE_SCENARIO_ID` | 同一侧内 `cluster_id` 重复 |
| `EMPTY_FEATURES` / `RAGGED_FEATURES` | `mean_features` 或 `weights` 为空，或任一 `mean_features.len() != weights.len()` |
| `NON_FINITE_INPUT` | `mean_features` 含 `NaN` 或 `±Inf`（带侧别、情景下标与特征下标） |
| `INVALID_WEIGHTS` | `weights` 含负数、非有限值，或 `fsum(weights) <= 0.0` / 非有限 |
| `INVALID_THRESHOLD` | `threshold` 或 `threshold_coefficient` 为负或非有限，或换算后的 `pair_scale` 非正/非有限 |
| `ZERO_DISPERSION` | `ensemble_dispersion` 算得 $s \le 0.0$（如所有成员在所有正权重格点上完全相同），或 `ScaledMatchInput` 传入的任一离散 $\le 0.0$ |
| `NUMERICAL_FAILURE` | 差值平方、加权乘积、`fsum` 累加或均值计算中出现溢出/非有限中间值（不得把溢出当作“距离过大所以未关联”） |

所有语义校验在 `ensabc-core` 内单一维护；DuckDB SQL 适配器只负责 `NULL` 检查、向量类型解码与 `chunk_row` 标记。

## 3. 独立命令行验证程序 `examples/m2_runner.rs`

与 M1 `examples/m1_runner.rs` 并列，提供脱离 DuckDB 的 core 独立执行入口（用于合成验证、穷举 oracle 对照与断网最小容器验证 FR-018）：

```bash
cargo run --locked --release -p ensabc-core --example m2_runner -- \
  --mode match|dispersion|chain \
  --input /path/input.json \
  --output /path/output.json
```

- `Cargo.toml` 的 `[dev-dependencies]` 中 `serde_json` 必须启用 `features = ["std", "float_roundtrip"]`，确保 17 位十进制浮点数反序列化逐位精确，不引入 1 ulp 解析偏差。
- `--mode match`：读取单对或多对 `MatchInput` / `ScaledMatchInput`，输出 `MatchResult`；
- `--mode dispersion`：读取成员矩阵与权重，输出单批离散；
- `--mode chain`：读取本地多批成员矩阵 + M1 配置 + 匹配配置，在纯 Rust 中依次调用 `cluster` → `ensemble_dispersion` → `match_scenarios_scaled` 并按 FR-007 规则输出回放结果，供最小运行环境与 SQL 全流程逐字段比对。身份上下文可提供经核对的 run_key，但不得提供预期出生/继承结果；runner 独立推导关系。单批已分类算法失败按发布分类断链，其他错误退出失败；跨构建或扰动版本按出生节点键核对逻辑链而非要求 hash 前缀相同。
