# M1 数据模型

本文件定义设计契约，尚未实现。算法规则见 [numerics.md](contracts/numerics.md)，公开入口见 [core.md](contracts/core.md) 与 [sql.md](contracts/sql.md)。

## 1. 任务与来源上下文

一个 TaskContext 关联一个模型、起报、有效时刻、变量、区域与有序网格。字段为 `task_id, model, init_time, valid_time, variable, unit, feature_version, sample_id, manifest_sha256, payload_sha256, source_matrix_sha256`，另保存清单的成员角色与网格键。source_matrix_sha256 保留源 float32 字节身份；进入 core 的精确 f64 数值来自还原 float32 后提升，不直接改变精度语义。

TaskContext 由调用表、样本清单或验证 envelope 持有，不传入数值 core。函数不能从值推断单位、角色或网格；真实数据示例在调用前验证上下文兼容性。结果持久化时，调用方保存任务、输入/配置身份、算法版本、环境与源码摘要。

## 2. 成员输入 MemberInput

| 字段 | Rust 语义 | 规则 |
| --- | --- | --- |
| member_id | String | 非空、任务内唯一；不改大小写、不 trim |
| features | Vec<f64> | 有限、非空；任务内相同 p；索引即共同特征顺序 |

一个 TaskInput 持有有序 `Vec<MemberInput>` 及 ScenarioConfig。不同任务可有不同 n、p、d、k。重复值但不同 ID 的行不合并。SQL NULL/offset 属于适配层错误，core 接到的值不包含 NULL；重复 ID、ragged、非有限和配置等语义验证只由 core 统一维护，适配层可以在安全解码时补充 SQL 行号。

## 3. ScenarioConfig

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| projection_dim | usize | 1≤d≤有效秩 |
| cluster_count | usize | 2≤k≤成员及原始/投影可区分向量数 |
| initialization | InitMethod enum | `KMeansPlusPlusChaCha8V1` 或 `ExplicitCentersV1` |
| seed | u64 | SQL 可表达范围 0..i64::MAX；显式中心须为 0 |
| restarts | usize | r>0 |
| initial_centers | Vec<Vec<Vec<f64>>> | 随机模式空；显式模式 r×k×d，有限 |
| stopping_rule | StopRule enum | `SklearnVarianceScaledV1` |
| tolerance | f64 | 有限且≥0 |
| max_iterations | usize | >0 |
| rank_threshold | f64 | 有限且>0 |

config 文件 envelope 包含 `schema_version=m1.config.v1`、上述计算字段、`algorithm_version=m1.native.v1` 和比较规则引用。JSON 外部键不能静默忽略；core 类型只表达计算字段。运行记录分别保存原始文件字节 SHA-256 与规范化语义配置 SHA-256，避免格式变化被误认为参数变化。M0 的 config hash 单独保留，不用 M1 配置冒充原基线配置。

## 4. 成功结果 ScenarioResult

顶层字段：`algorithm_version, config, members, scenarios, diagnostics`。没有成功/失败混合行：成功返回完整结果，错误返回类型化错误或 SQL 查询错误。

### MemberAssignment

`member_id: String, cluster_id: usize`。严格按输入顺序输出 n 项；每个成员恰好出现一次。cluster_id 是本次计算内按首次成员出现规范化的 0..k-1 编号，没有 branch_id 或跨批次持续身份。

### ScenarioSummary

| 字段 | 维度/含义 |
| --- | --- |
| cluster_id | 本次计算内编号 |
| member_count | 正整数 |
| member_fraction | member_count/n，未校准成员占比 |
| mean_features | p；最终归属成员的原物理单位均值 |
| representative_member_id | 本簇内到返回中心最近的输入成员；等距按输入序 |
| within_cluster_sse | 最终归属 scores 到返回中心的平方距离和 |

scenarios 按 cluster_id 升序输出 k 项；所有 member_count 之和=n，fraction 总和在绝对误差 1e-12 内为 1。

### Diagnostics

| 字段 | 维度/含义 |
| --- | --- |
| input_member_count, feature_count, projection_dim, cluster_count | n、p、d、k |
| effective_rank | 标准化矩阵有效秩 |
| normalization_mean, normalization_scale, pca_mean | 各 p |
| scaled | n×p，标准化矩阵 Z |
| singular_values | min(n,p)，中心化矩阵 A 的降序奇异值 |
| components | d×p，投影轴 |
| explained_variance, explained_variance_ratio | 各 d |
| scores | n×d，投影成员坐标 |
| centers | k×d，按规范化 cluster_id 排列 |
| total_sse | 簇 SSE 之和 |
| iterations | 被选 restart 迭代数 |
| converged | 成功只能为 true |
| stop_reason | labels_stable 或 center_shift |
| final_assignment_changed | 容差停止后的最后 E-step 是否改变标签 |
| selected_restart | 从 0 开始 |
| restart_summaries | r 条：restart_index、initial_centers(k×d)、initial_member_indices、iterations、total_sse、stop_reason |
| distance_space | 固定 `pca_scores_unwhitened` |

每个 RestartSummary.initial_member_indices 为 `Vec<usize>`：随机模式恰有 k 个零基输入行下标，与该 restart 的初始中心顺序绑定；显式中心模式为空列表，不从中心数值推断来源。它不含 NULL，SQL 表示为 BIGINT[]。各 restart 的初始中心/来源使用初始化时的中心顺序，最终 cluster_id 规范化只重排被选结果的 centers/members/scenarios，不重写初始化顺序。共享验证的起点下标另放 initialization 证据。

输入原始 X 留在输入证据，不再复制到结果。scaled、scores、中心和完整诊断保留在首版 typed 结果中以满足可核对性；因此输出大小计入 FR-016 资源测量，不提前引入 debug 开关或诊断存储服务。逐迭代 trace 仅为共享初始化测试产物，不进入每个公开结果。

## 5. 错误与状态

CoreError 字段：`code, message, member_index?, feature_index?, restart_index?, iteration?`。数值位置为零基；SQL wrapper 额外附加 chunk row，而全局 task_id 仍由调用方识别。错误类型列表及分工见 core 契约。

单任务流转：`decoded → validated → standardized → projected → clustered → summarized → returned`；任一步可进入 `failed`，失败不产生完整成功结果。core 每次调用局部持有全部临时状态；SQL 一个 callback 的所有行先计算后写回，发生错误时整个查询失败，不能将之前流出的行当作完整成功。

证据步骤状态为 `not_run → pass|fail`，每次重跑写新目录。阶段结论为 `passed|failed|incomplete`。若必要项 fail 则 failed；无 fail 但存在 not_run 则 incomplete；全部必要项 pass 且追溯与审计通过才允许整体成功。

## 6. 验收 envelope、身份与持久化

数值 runner 读写 JSON 仅为验证边界，`schema_version=m1.task.v1|m1.result.v1`，包括 TaskContext、配置引用、源码/环境引用、member inputs 或 ScenarioResult。JSON 禁止 NaN/Infinity；字符串、数组和数值形状显式验证。

样本 payload 的已存在字节 SHA-256 保持不变。M1 自身 envelope 的内容摘要采用 UTF-8、键排序、紧凑分隔、禁止非有限数值的 JSON 定义；同时保存读入文件字节摘要。不要要求 Rust/Python 的默认浮点字符串恰好同字节；跨语言验证使用由同一文件提供的身份与数值比较，生成规范化身份由验证驱动统一负责。

验收结果保存 `input_ref, config_ref, source_ref, environment_ref, result_ref`，每个 ref 为运行目录内相对 path 和实际字节 sha256。具体文件和 FR/SC 断言见 [evidence.md](contracts/evidence.md)。来源许可不明确的真实数值载荷、其投影/中心/均值等派生结果不加入版本控制；只提交允许的合成数据、清单、规则和恢复说明。
