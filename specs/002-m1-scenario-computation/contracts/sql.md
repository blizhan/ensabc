# M1 SQL 接口契约

接口版本：`m1.sql.v1`。目标宿主 DuckDB CLI 1.5.5 / Linux aarch64。`ens_m0_echo` 的既有签名与错误行为保持有效；新增函数在同一扩展中注册，保留 `ensabc_m0_extension_init_c_api` 和既有打包名称，避免为了重命名产物增加迁移成本。

## 1. 调用

每个 SQL 行是一个独立任务，调用方保留 task_id、样本元数据、输入/配置/源码/环境身份。唯一签名如下（字段顺序也是类型构造顺序）：

```sql
ens_cluster(
  members STRUCT(member_id VARCHAR, features DOUBLE[])[],
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
) -> STRUCT(
  algorithm_version VARCHAR,
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
  ),
  members STRUCT(member_id VARCHAR, cluster_id BIGINT)[],
  scenarios STRUCT(
    cluster_id BIGINT,
    member_count BIGINT,
    member_fraction DOUBLE,
    mean_features DOUBLE[],
    representative_member_id VARCHAR,
    within_cluster_sse DOUBLE
  )[],
  diagnostics STRUCT(
    input_member_count BIGINT,
    feature_count BIGINT,
    projection_dim BIGINT,
    cluster_count BIGINT,
    effective_rank BIGINT,
    normalization_mean DOUBLE[],
    normalization_scale DOUBLE[],
    pca_mean DOUBLE[],
    scaled DOUBLE[][],
    singular_values DOUBLE[],
    components DOUBLE[][],
    explained_variance DOUBLE[],
    explained_variance_ratio DOUBLE[],
    scores DOUBLE[][],
    centers DOUBLE[][],
    total_sse DOUBLE,
    iterations BIGINT,
    converged BOOLEAN,
    stop_reason VARCHAR,
    final_assignment_changed BOOLEAN,
    selected_restart BIGINT,
    restart_summaries STRUCT(
      restart_index BIGINT,
      initial_centers DOUBLE[][],
      initial_member_indices BIGINT[],
      iterations BIGINT,
      total_sse DOUBLE,
      stop_reason VARCHAR
    )[],
    distance_space VARCHAR
  )
)
```

输入不允许 NULL，包括 config 及其任何字段/数组元素；随机模式 initial_centers 必须是非 NULL 的空 `DOUBLE[][][]`。成功输出每个字段/元素非 NULL 且数值有限，不使用 NULL 或 status 字符串掩盖计算失败。

restart_summaries.initial_member_indices 在随机模式长为 k，保存与实际初始中心绑定的零基成员输入下标；显式中心模式是非 NULL 空 BIGINT[]。不在最终 cluster_id 规范化时重排 restart 的初始化记录，不为任意显式中心伪造成员来源。

BIGINT 输入在转换为 usize 前检查非负、目标范围与溢出，seed 只支持非负 i64。枚举和参数范围见 [numerics.md](numerics.md)。矩阵维度见 [data-model.md](../data-model.md)，不依赖调用方猜测 flatten 顺序。

## 2. 示例与保存

以下仅是实现后的调用范式，当前函数尚不存在。示例以一维投影与两簇展示配置，正式真实样本验收仍为二维/三簇：

```sql
WITH tasks AS (
  SELECT 'demo' AS task_id,
         [{'member_id':'a','features':[0.0,0.0]},
          {'member_id':'b','features':[0.2,0.1]},
          {'member_id':'c','features':[10.0,9.0]},
          {'member_id':'d','features':[10.2,9.1]}]
           ::STRUCT(member_id VARCHAR, features DOUBLE[])[] AS members
), config AS (
  SELECT {'projection_dim':1, 'cluster_count':2,
          'initialization':'kmeans++_chacha8_v1', 'seed':0,
          'restarts':10, 'initial_centers':[]::DOUBLE[][][],
          'stopping_rule':'sklearn_variance_scaled_v1',
          'tolerance':0.0001, 'max_iterations':300,
          'rank_threshold':1e-10}
          ::STRUCT(projection_dim BIGINT, cluster_count BIGINT,
                   initialization VARCHAR, seed BIGINT, restarts BIGINT,
                   initial_centers DOUBLE[][][], stopping_rule VARCHAR,
                   tolerance DOUBLE, max_iterations BIGINT,
                   rank_threshold DOUBLE) AS params
)
SELECT task_id, ens_cluster(members, params) AS result
FROM tasks CROSS JOIN config;
```

真实数据示例 `validation/m1/examples/real_sample.sql` 将按 `member_index` 显式执行 `list(struct_pack(member_id := member_id, features := features) ORDER BY member_index)`，不能按成员字符串的字典序替代清单顺序。输入成员 ID 与特征必须绑定同一 STRUCT。调用结果可 `CREATE TABLE ... AS SELECT task_id, sample_id, input_hash, config_hash, ens_cluster(...) AS result` 保存，再分别展开 result.members/result.scenarios 继续分析。来源时间、单位、特征版本和构建/环境身份由同一结果表或其外键保留。

## 3. 错误、批量与确定性

特殊 NULL handling 让两个顶层参数的 NULL 到达 callback 并明确报错。绑定期类型错误与函数内部数据错误分别记录。SQL 错误前缀为 `ens_cluster [CODE] chunk_row=N`，可带 member/feature/restart/iteration；行号只指当前 callback chunk，不能冒充全局 task_id。

core 错误码以 [core.md](core.md) 为准；adapter 增加 `NULL_INPUT, INVALID_VECTOR, INTERNAL_PANIC, OUTPUT_ALLOCATION`。一个 callback 内任一行失败则整个 callback 报错；查询后续或之前流出的行均不能标记整条查询成功。消费端须完整完成查询后才接受结果，数据库事务/保存流程不能提交部分失败结果。

不同任务可以有不同 n/p/d/k，各自配置生效。chunk 所有行先计算为 owned 结果后写回；跨 chunk 不缓存拟合或随机状态。并发保证对象是至少两个独立连接，不声明同一连接可被多线程并行访问。固定输入与配置下按 [numerics.md](numerics.md) 等价，SQL 函数不能访问来源、历史或网络。

## 4. C API 实施约束

- 在锁定 `duckdb` / `libduckdb-sys` `=1.10505.0` 的 loadable C API 路径新增注册，不引入 bundled DuckDB。
- 每层校验 validity、LIST offset/length 和 child size；使用 checked arithmetic，常量 config、字典/选择向量等表示须以当前 C API 可读表示核实，不能假设参数有独立填满的行缓冲。
- 构造逻辑类型采用可在错误路径释放的 scoped handle；注册后释放临时 function/type handles。callback 捕获 panic 并置查询错误，禁止 unwind 越过 FFI。
- 统计各 LIST child 的总长度，调用 reserve 和 set_size 并检查返回值。缓冲 reserve 后重新读取所有会受移动影响的 child/data/validity 指针。为每层输出获取可写 validity，所有实际输出行置 valid；不用输入引用代替新结果存储。
- 字符串用 `duckdb_vector_assign_string_element_len` 复制明确字节长度，支持 UTF-8 和内嵌 NUL；不将成员 ID 作为 C 字符串截断。错误信息转 C 字符串时替换内嵌 NUL。
- 使用 `duckdb_vector_size()` 记录实际批量阈值。验证任务行数和单任务成员数分别大于该阈值，不仅写死一个 2048 断言。

## 5. 必要 SQL 验证

类型 introspection 与上述签名逐字段一致；常量 config、每行不同配置、不同形状、切片/过滤后列表、UTF-8/内嵌 NUL ID 正常绑定；与直接 core 输出一致。逐类嵌套 NULL、非法参数、数值退化均在同连接合法/非法/合法序列验证。跨 task/member 批量分别验证，混合合法/非法跨 chunk 整体失败，并有完整消费和保存回滚证据；两连接真实重叠计算与串行等价。M0 echo 原套件作为回归运行，不能以 M1 成功替代。
