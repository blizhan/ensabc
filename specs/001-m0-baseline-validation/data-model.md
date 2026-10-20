# M0 数据模型

下列为实现目标。规划期 spike 文件可以使用更小格式，但转为正式样本时必须补齐字段并验证。

## 1. FrozenSample / SampleManifest

采用小型 JSON 清单与 JSON 数值文件，矩阵按成员优先排列。JSON 对 M0 的小输入足够，暂不增加通用列式存储层；冻结气象输入总量受 spec 的 100 MiB 上限约束。

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `schema_version` | string | 首版 `m0.sample.v1` |
| `sample_id` | string | 非空、稳定，不含机器绝对路径 |
| `kind` | enum | `real_ensemble` 或 `synthetic`，不能互换 |
| `source` | object | 提供商、产品、模型、源 URI、版本/快照（若有）、获取时间、来源文档 |
| `source.member_set` | array | 来源声明的成员集合及控制/扰动角色；不假设每个产品包含控制成员 |
| `init_time` | string | UTC ISO 8601，真实样本必须有证据 |
| `valid_time` | string | UTC ISO 8601 |
| `lead_seconds` | integer | `valid_time - init_time`，与源提供的时效一致 |
| `variable` | object | 源变量名、含义、高度、原单位、规范化单位、转换公式 |
| `region` | object | 明确边界和经纬度约定；保存实际选择点而非仅请求坐标 |
| `points` | array | 每点的唯一 `point_id`、lat、lon、可用的源网格索引 |
| `members` | array | 每项唯一 `member_id`、原始标签/类型、控制/扰动角色 |
| `layout` | object | 矩阵顺序、shape、成员排序、空间点排序、dtype |
| `data_file` | string | 相对样本目录路径 |
| `data_sha256` | string | 对实际文件字节计算 |
| `provenance` | object | 来源片段、提取步骤/版本、所有变换，不存凭据 |
| `usage` | object | 源说明的许可/限制、归档与恢复方式，不猜测再分发权限 |

数值文件包含 `member_ids`、`point_ids`、`values`，其中 `values[m][p]` 对应有序成员和有序网格点，数值使用有限的双精度表示。清单存数据哈希；证据记录另存清单哈希，避免清单自引用哈希。

选定的 WeatherBench2 spike 原值为 float32，CSV 是 float32 round-trip 表示。转为正式 JSON 时先按 float32 还原，再提升为 float64，并在 provenance 中记录两个 dtype 和转换；不能直接按 CSV 十进制解析结果假设源数据为 float64。单位保持 K，矩阵为 50×9；具体时空坐标见 [研究记录](research.md#1-固定输入优先于跨提供商对照)。

原标签若转成字符串，映射必须一对一；拒绝不同原标签规范化后碰撞。排序不能靠字符串字典序猜测源的数值成员顺序，应在清单中显式保存。

验证要求：

- 成员集合与源声明一致，不能用硬编码 51 替代。
- 每个成员具有完全相同的点集合和排序；坐标可规范化但不能用未说明插值制造缺点。
- 真实样本同属一个起报、有效时刻、模型和变量。
- 数据维度与清单一致、所有数值有限；纬度范围和经度约定明确。
- 原始源记录不可含“latest”作为最终身份；解析为实际起报及对象/快照证据后冻结。
- 可恢复的字节副本与元数据同时保存；远端 URL 不是长期恢复保证。

## 2. ValidationTask

| 字段 | 类型 | 语义 |
| --- | --- | --- |
| `task_id` | string | 调用者持有的行身份；不参与数值变换 |
| `members` | ordered list | 每项 `member_id: string` 与 `features: list<double>` |

任务内所有特征长度相同且大于零。不同任务形状可不同。任务中无时间/网格字符串推断逻辑：样本映射在调用前完成，调用输出与输入保持关联。

任务是传递边界，不是跨起报 branch；M0 不引入 `branch_id` 或 lineage 状态。

## 3. ReferenceConfiguration / ReferenceRun

配置保存标准化规则、浮点精度、投影维度及求解器、聚类数、初始化算法、初始化次数、随机种子、迭代阈值和上限、有效秩判定、数值容差及等价比较方法。配置按确定的字节序列保存并计算哈希。

运行记录包含：

- `run_id`、样本/清单/配置哈希。
- 所有直接和间接参考依赖版本、解释器、平台、数值线程设置及源码哈希。
- 输入形状、标准化均值/尺度、标准化矩阵、投影结果/分量/方差信息、标签、中心、目标函数和迭代次数。
- 每个输出文件相对路径与内容哈希，起止时间及执行步骤。
- `status: pass|fail` 和诊断信息；输出不覆盖旧运行。

三次独立进程运行是三个 ReferenceRun，由 ComparisonRecord 关联。数值不一致不是自动触发重建参考结果的理由，必须保留差异证据。

## 4. ComparisonRecord

记录比较双方运行身份、规则版本、最大绝对/相对误差、分区是否等价、投影等价检查和最终状态。零附近误差遵循 spec 的绝对容差，不计算无意义的除零相对误差。

成员分区用成员身份关联，而非假设行序未变化；往返协议要求行内顺序不变，因此顺序改变仍算往返失败，即使聚类分区等价。

## 5. Measurement / GateReport

Measurement 保存任务规模、成员/特征形状、执行模式、连接数、线程数、输入输出大小、耗时和峰值 RSS、测量程序、环境及测量边界。

GateReport 关联全部功能要求、输入/环境版本及证据文件。`not_run`、`fail`、`pass` 为唯一单项状态；总体为 `incomplete`、`failed` 或 `passed`。任何必要项未执行为 incomplete，已执行失败为 failed，只有全部通过为 passed。

数据关系：

```text
FrozenSample + ReferenceConfiguration -> ReferenceRun × 3 -> ComparisonRecord
FrozenSample / synthetic input -> ValidationTask -> exchange evidence / Measurement
all evidence + requirement mapping -> GateReport
```

## 6. 生命周期与存储

样本先以候选状态提取，资格检查后冻结；资格失败不生成“合格样本”标记。冻结内容改变时生成新 sample_id 或新版本，不原位替换旧证据。

仓库保留小型测试材料、清单、脚本和必要来源声明；临时环境、构建产物、缓存及可再生大量日志放入忽略路径。若来源限制使数值样本不能提交，应提供合规恢复途径和哈希，验收前实际恢复检查。
