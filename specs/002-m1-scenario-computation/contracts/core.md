# M1 Rust core 公共接口契约

版本：`m1.native.v1`。首版库发布版本随 workspace 为 0.1.0，尚未声明跨 minor 的稳定 ABI。对应 FR-001–FR-010、FR-013–FR-014；字段见 [data-model.md](../data-model.md)，算法见 [numerics.md](numerics.md)。

## 1. 边界

目标 crate：`ensabc-core`。公共入口：

```rust
pub fn cluster(task: &TaskInput) -> Result<ScenarioResult, CoreError>;
```

公开类型为 `TaskInput, MemberInput, ScenarioConfig, InitMethod, StopRule, ScenarioResult, MemberAssignment, ScenarioSummary, Diagnostics, RestartSummary, CoreError, ErrorCode`，字段以数据模型为准。算法版本常量为 `ALGORITHM_VERSION = "m1.native.v1"`。

该函数借用输入，不修改输入成员或配置。成功结果持有自己的字符串/数组，无引用悬挂。core 不含 DuckDB、Arrow、Python、文件格式、网络、SQL 行号或历史状态依赖。数值及错误可直接在 Rust 调用中使用；不为公共函数加入 JSON parser、临时目录或数据下载能力。

浮点计算使用 f64、单任务顺序归约，不调用外部 BLAS/LAPACK/子进程。多个调用可在独立线程执行；不保证跨平台逐位一致，只在声明环境及冻结比较规则下验证。所有随机状态每任务局部初始化，不使用 thread_rng 或可变全局状态。

## 2. 校验与错误

公共函数统一执行 ID、形状、有限性、参数范围、可区分行数、秩、谱边界与收敛校验。SQL adapter 只负责 NULL、类型表示、offset/length 与安全转换；core 错误可附加 SQL 位置，但语义规则不能复制成两套独立实现。

| ErrorCode | 条件 |
| --- | --- |
| EMPTY_MEMBERS / EMPTY_MEMBER_ID / DUPLICATE_MEMBER_ID | 空成员列表、空或重复身份 |
| EMPTY_FEATURES / RAGGED_FEATURES / NON_FINITE_INPUT | 空向量、不等长、非有限输入 |
| INVALID_CONFIG / INVALID_INITIAL_CENTERS | 范围、枚举、长度、中心形状或有限性错误 |
| INSUFFICIENT_DISTINCT_VECTORS / INSUFFICIENT_PROJECTED_VECTORS | 原始/投影可区分行数不足 |
| INSUFFICIENT_RANK / AMBIGUOUS_SUBSPACE | 有效秩或截断子空间不满足 |
| SVD_NOT_CONVERGED | 有限迭代分解未完成 |
| EMPTY_CLUSTER / NOT_CONVERGED | 迭代失败；带 restart_index/iteration |
| NUMERICAL_FAILURE | 数值溢出、无法解释的负方差等非有限或不合法中间量 |
| SIZE_OVERFLOW / ALLOCATION_FAILURE | 长度溢出或可恢复的分配失败 |

错误 message 可改进，不作为逐字兼容面；code 与位置字段是测试断言依据。不承诺通过 catch_unwind 恢复系统 OOM/进程信号/abort。长度溢出必须在分配前拒绝，可恢复的分配优先采用 try_reserve。

## 3. 最小内部模块与测试入口

实现按实际职责拆为 `lib.rs`（公共类型/编排）、`preprocess.rs`（验证/标准化/PCA）、`kmeans.rs`（初始化/Lloyd）、`summary.rs`（规范化编号/物理均值/代表）。不预建算法注册器、通用 pipeline trait、数据适配层或跨批次状态管理。

共享初始化的逐迭代检查由 crate 内部测试访问 Lloyd 私有 helper，测试用 trace collector 可通过 callback 或可选内部参数实现，不作为 SQL 模式和稳定公共 API。验证驱动通过 `ENSABC_M1_TRACE_INPUT` 与 `ENSABC_M1_TRACE_OUTPUT` 两个必填环境路径，调用 crate 根 tests 模块中标为 `#[ignore]` 的命名导出测试 `tests::m1_shared_trace`（`cargo test --locked -p ensabc-core --lib tests::m1_shared_trace -- --ignored --exact`）；普通 `cargo test` 不依赖这两个环境路径，驱动则必须显式运行该导出测试并检查产物；输入为冻结 scores/centers/tol/cap 的测试 envelope，输出事件字段及观察时点按数值契约。缺失路径或身份不符时该显式导出测试失败，不得静默跳过后伪称产物存在。

collector 必须观察同一个私有 Lloyd 实现，验证开启/关闭观察时最终结果等价；只在该诊断用例存全量轨迹。常规结果只返回含 initial_member_indices 的 restart summaries，不把逐轮 trace 或环境变量读入生产 cluster()。

## 4. 验证程序

`examples/m1_runner.rs` 是开发/验收辅助程序，由 core 示例构建；serde/serde_json 仅作为 dev-dependencies，版本以实际 Cargo.lock 为准并记录，不泄漏到核心运行依赖。运行界面为：

```bash
cargo run --locked --release -p ensabc-core --example m1_runner -- \
  --input /path/task.json --output /path/result.json
```

参数都必填；输入为 `m1.task.v1`，结果 envelope 为 `m1.result.v1`。runner 创建输出父目录并以临时文件原子写入，输出拒绝覆盖已有不同身份文件；成功返回 0，验证/计算失败返回 1 并写 stderr 类型化错误，任何失败不产生成功 result 文件。验证驱动捕获错误产物。文件加载与身份核验留在 runner，不进入 cluster()。

此示例在最小容器里可脱离 DuckDB 独立执行，证明独立调用和无参考工具运行。其序列化代码不是第二套算法实现。

## 5. 契约检查

必须覆盖：合法参数的 d=1、d=有效秩、k=2、k=可区分行数；不同合法形状；重复数值成员、常量及接近常量列；代表成员等距；所有错误码的可构造类别；SVD/秩边界；投影折叠；空簇；有限迭代未收敛。固定 seed/顺序多次调用和独立线程的结果按数值契约等价。SQL 结果必须与此入口的结果逐字段对照，不能把两个不同算法的输出都称为同一 core。
