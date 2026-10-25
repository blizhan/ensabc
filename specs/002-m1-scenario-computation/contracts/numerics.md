# M1 数值与比较契约

版本：`m1.native.v1` / `m1.comparison.v1`。本契约是实施目标，尚无原生运行通过证据。对应 FR-002–FR-010、FR-013。

## 1. 输入、标准化与投影

输入矩阵 X 为 n×p、成员优先、有序、有限 f64；n≥2、p≥1。重复数值行保留独立身份，`-0.0` 与 `0.0` 在可区分行计数中视为相等。成员 ID 必须非空且唯一，不 trim、不排序或猜测角色。

逐列计算总体均值 μ 和总体方差 v（分母 n），采用修正两遍：先求均值，再累计偏差及偏差平方，使用 `sum(delta²) - sum(delta)²/n` 修正，检查所有中间值有限。由浮点舍入产生的负方差只在机器误差界内截为 0，低于 `-64*eps*max(sum(delta²),1)/n` 则报 `NUMERICAL_FAILURE`。常量检测为 `v <= n*eps*v + (n*μ*eps)^2`，满足时 scale=1，否则 scale=sqrt(v)。保留所有列，Z=(X-μ)/scale。

有效秩用 Z 的奇异值判定，规则为 `s > rank_threshold * max(s_max,1)`，默认 rank_threshold=`1e-10`。要求 `1 <= d <= rank(Z)`；Z 再中心化后的矩阵 A 的有效秩也须≥d。记录 PCA 中心 μz，A=Z-μz。使用 `nalgebra::linalg::SVD::try_new`，f64 epsilon、最多 10000 次内部迭代，失败报 `SVD_NOT_CONVERGED`。不得用无限迭代或静默降维。

令 A=UΣVᵀ，奇异值降序。components 为 Vᵀ 前 d 行（d×p），scores=A·componentsᵀ（n×d）；不白化。解释方差为 `s_j²/(n-1)`，比例分母为全部奇异值平方和。每个 component 的最大绝对值载荷取正；最大值并列取最小特征下标，并同步翻转 scores。

当存在第 d+1 个奇异值，且 `abs(s[d-1]-s[d]) <= rank_threshold*max(s[0],1)` 时，保留边界不确定，报 `AMBIGUOUS_SUBSPACE`。边界内同值奇异向量可旋转；不据基的表示差异拒绝相同子空间。d 等于全部有效秩时仍检查与剩余近零谱的间隙（若剩余谱存在）。

实现同时检查原始行和投影后行的可区分数量≥k；投影后少于 k 时报 `INSUFFICIENT_PROJECTED_VECTORS`，不能合并成员或减簇。可区分性按有限 f64 行的逐值相等判断，不用比较容差改变样本资格。

## 2. 配置

所有字段显式提供，未知枚举或非法值拒绝。默认验收配置是 fixtures 中的具体记录，函数不隐式补默认值。

| 字段 | 合法值/含义 |
| --- | --- |
| projection_dim | 整数 d≥1，且满足上述秩条件 |
| cluster_count | 整数 k≥2，且≤n及原始/投影可区分行数 |
| initialization | `kmeans++_chacha8_v1` 或 `explicit_centers_v1` |
| seed | 0..2^63-1；显式中心时须为 0 |
| restarts | 正整数 r；无静默截断 |
| initial_centers | 随机模式须为空列表；显式模式为 r×k×d 有限数组，属于本任务符号规范化后的投影坐标 |
| stopping_rule | 仅 `sklearn_variance_scaled_v1` |
| tolerance | 有限且≥0；首个基线 `1e-4` |
| max_iterations | 正整数；首个基线 300 |
| rank_threshold | 有限且>0；首个基线 `1e-10` |

维度和长度转换使用 checked arithmetic；内存分配失败可恢复时返回错误，不任意承诺可容纳任意大的合法矩阵。资源不足不算成功。

## 3. 原生随机与共享初始化

原生随机模式使用固定版本 `ChaCha8Rng::seed_from_u64(seed)`，每个任务新建同一独立流；restarts 顺序使用该流，不依赖 SQL 行号、全局 RNG 或线程调度。

随机抽样使用 `u = (next_u64() >> 11) / 2^53`，u∈[0,1)。首中心索引为 `floor(u*n)`。后续中心按到已选最近中心的 D² 权重累计抽样，以输入行顺序构造累积和，选择第一个累积值严格大于 `u*sum(D²)` 的行；舍入到尾端时选最后一个正权重行。每次只抽一个候选，不宣称复现 sklearn 的多候选贪心 k-means++。sum(D²) 非有限或非正则报错，不随机替换或降簇。

每个公开 RestartSummary 记录实际初始中心和 `initial_member_indices`。随机模式该列表长 k，每项为 0..n-1 的零基输入成员下标，顺序与该 restart 的初始中心一致，可由有序输入恢复成员 ID；显式中心模式为非 NULL 的空列表，因为配置中心可以不是任何真实成员，不能猜测最近成员冒充来源。显式中心直接使用配置数组；重复/无效中心及导致空簇的中心明确拒绝。共享验证选择真实起点行的下标单独保存在 initialization 证据，不填入显式模式的公开来源列表。共享初始化验证先把 Python scores/中心转换到同一对齐坐标，再交给相应实现；主路径还要分别拟合 PCA 并检查同一几何，不能仅在一个预投影矩阵上通过就替代端到端迁移。

共享对照允许验证程序调用 core 的 crate 内部测试入口运行单独 Lloyd，不增加 SQL 的调试模式。完整公共调用仍经标准化与投影；Python 参考中心经 Q 对齐后才能用于 core 拟合所得坐标。记录双方各自接收的中心、Q、坐标身份和数值误差。

## 4. Lloyd、停止与失败

每次迭代：

1. 按平方欧氏距离分配到当前最近中心；完全等距取最小当前中心下标。
2. 各簇以所属 scores 的算术均值更新中心；空簇报 `EMPTY_CLUSTER`，不重定位。
3. 先判断 labels 是否与上一轮相同，再判断总中心位移平方和≤τ；`τ = tolerance * mean(var(scores, ddof=0))`。
4. 两者均不满足时继续；达到 max_iterations 仍未满足，报 `NOT_CONVERGED`。在最后一轮恰好满足条件算收敛。

以容差而非严格标签稳定停止时，执行一次不更新中心的最终分配，保证 labels 对应返回中心；再次检查非空及有限性。成功结果中心是最后一次更新的中心；最终分配如有变化，不伪称中心一定是最终分区的精确均值。物理平均场始终独立由最终归属的原始 X 计算。诊断记录 `stop_reason=labels_stable|center_shift` 和最终分配是否改变。

任一 restart 出现空簇、非有限或不收敛，整个任务报错，并记录 restart 与迭代位置；不静默忽略坏 restart。所有 restart 成功时选总 SSE 最小者，完全相等取最早 restart。最终按第一次出现的成员顺序将簇编号规范化为 0..k-1，同步重排所有中心、摘要和索引。

每簇 SSE 是最终归属成员到返回中心的平方距离和；total_sse 为其和。代表成员是在所属簇中到返回中心距离最小的真实成员，完全相等取输入最先出现者。成员占比为 count/n，物理 mean_features 是最终分区 X 的逐列均值。

## 5. 两条强制数值对照

### 原 M0 基线对照

调用原 M0 runner 与原 config 生成三进程参考，或消费已审计且身份相同的原参考记录。输入内容哈希及原 config 文件哈希必须匹配，不能覆盖其文件。另行运行原生随机默认配置（二维/三簇、10 重启、seed 0 等），再做 M1 语义比较。

分区必须相同；scaler mean/scale/Z、rank、投影几何、解释方差比例、对齐后的 scores/centers、总 SSE 和物理平均场必须满足固定门槛。参考不存在的物理均值、代表成员与簇 SSE 由原始矩阵和参考分区/中心独立重算。原生和原参考初始化不同，`iterations` 和 `selected_restart` 记录但不要求与原参考相等；原 M0 自身的三进程整数一致性检查保持不变。

### 共享初始中心对照

单独配置与运行目录，r=1，记录真正的初始中心及其 hash，不能从最终参考中心反推并作为已共享的初始中心。以固定输入行下标 `[0, floor(n/3), floor(2*n/3)]` 选取三簇真实样本起点；若这些投影点重复或产生失败，则该诊断失败，须在生成最终验收前版本化改用另一组明确下标，保留失败证据。

执行顺序固定为 original 参考 → native 完整结果 → original 比较及投影对齐 Q → shared 参考/初始化 → shared native → shared 比较；各命令必填 stage，见 [evidence.md](evidence.md)。original 阶段不依赖 shared 结果，shared 阶段不反向生成 original/native 前置文件。

先对齐完整投影，再同时验证：（a）共享同一 scores 数组和中心时的逐步 Lloyd；（b）各自完整拟合流程下、映射到各自坐标的共享中心运行。以 `scores_native * Q ≈ scores_reference` 定义对齐，故公共 native 调用的中心为 `C_reference * Qᵀ`。检查每轮 labels、更新中心、SSE、中心位移、停止次数和原因。

### 锁定参考的真实轨迹观察

`_kmeans_single_lloyd` 只返回最终结果。参考验证必须在专用单线程子进程内，临时用透明 wrapper 替换 `_kmeans.lloyd_iter_chunked_dense` 的模块全局绑定，再调用原封不动的 `_kmeans_single_lloyd`；wrapper 每次调用原保存的内核恰好一次，不自行分配标签或重写迭代。禁止修改已安装源码文件或在多个并发任务共享该 monkeypatch。

该锁定版本内核签名为 `(X, sample_weight, centers_old, centers_new, weight_in_clusters, labels, center_shift, n_threads, update_centers=True)`。按参数名绑定，调用前复制中心，调用后复制 labels/centers/weights/shift，不能只保存之后会被覆盖的数组引用。更新步记录更新后中心及按当前 labels 的 SSE、`sum(center_shift²)`；最终 `update_centers=False` 只分配标签，不计入迭代数，shift 记 null，不能读取残留的上一轮 shift 当新位移。所有观察都在原内核返回后完成。

以与锁定原循环一致的优先级从轨迹推导 `labels_stable`、`center_shift` 或 `iteration_limit`；每次 wrapper 更新步对应一次实际迭代。将同一数据、同一初始中心和缩放后的 tol 另行交给未包装的 `_kmeans_single_lloyd`，要求最终 labels、centers、inertia、n_iter 逐字段精确相同，并确认更新步次数=n_iter、最终仅分配步次数为 0 或 1。使用 try/finally 恢复原绑定，错误亦恢复；记录符号、sklearn 版本、源码/模块身份及一致性结果到 observer-check。观察完整性失败不得给出 shared 通过。私有 API 只存在于参考环境，版本变化须重新验证。

Rust 私有 trace collector 使用同一事件字段/时点，并在内部测试核对开关观察前后的公共结果一致。此设计没有向生产 core 或 SQL 加调试参数。

## 6. 几何、比较器与反例

所有可比数值逐元素满足 `|a-r| <= 1e-8 + 1e-6*|r|`；不另引入整体最大量缩放掩盖个别差异。分区按成员共簇关系精确检查。member_fraction 总和另用绝对误差≤`1e-12`。

令 B 为 d×p components。先比较 `BᵀB`，再比较 scores 的成员对平方距离。在几何通过后，以正交 Procrustes 求 Q，使 `B_actualᵀ Q` 对齐 `B_referenceᵀ`；实际 scores 和 centers 同乘 Q。original 比较把 Q 与双方投影及规则身份单独写入 projection-alignment.json，供后续 shared 阶段消费；不能用 shared 输出反算该前置 Q。对齐坐标再比较。重复特征值块的解释方差按排序谱比较，不比较无意义的块内轴身份。外部成员身份及特征顺序必须完全一致。

正例：簇编号置换、单轴符号翻转、保留子空间内部正交旋转。反例：成员错簇、超容差 mean/scale/score/center/物理均值/SSE、错误输入身份/成员顺序、不同子空间、保留边界退化。不能仅靠旋转对齐把不同子空间放行。比较器必须验证所有必需数组的形状、有限性和缺失字段，不能用空数组的 vacuous pass 冒充通过。

比较结果保存参数、规则版本、identity、每个字段的判定及最大差值、分区差异成员、Q 和反例结果。不得直接改 M0 comparator 来重解释旧报告。
