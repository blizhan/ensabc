# M1 技术研究与决策

日期：2026-10-24。范围为技术规划，不是 M1 实现或验收。三个 `magpie/codex/gpt-6-luna:max` 只读子代理分别调查数值迁移、SQL 边界和验收复用，主代理复核关键源码并运行小型 Python 参考实验后作出以下设计决策。后续已生成 41 项任务，规划修订已解决只读分析的 I1/U1/I2/I3/I4，补齐阶段调度、真实轨迹观察、来源下标、独立复现所有权与状态说明。未运行原生算法、扩展写回实验或完整门禁，不据此宣称 M1 可用。

## 1. 输入、环境及 M0 资产复用

**Decision**：沿用 M0 真实 50×9 温度矩阵、源 float32 身份、成员及网格顺序、原参考配置和环境版本。M1 输出写入新的运行目录，原 M0 文件及报告只读。数据与参考依赖复用 M0 锁文件；M1 环境核验另行记录新增 Rust 依赖和构建产物。

**Rationale**：同时改变输入、环境和算法会使偏差难以定位。首个真实数据路径已经满足 M1 的来源要求，无需先开发 OM 或第二家来源。

**Alternatives considered**：重新取实时集合、把数据源转换加入 core、强制新增 DuckOMO 联调，均不能帮助隔离本次算法迁移。保留 DuckOMO 的 OM 路径方向，但未经实际验证不列为已支持能力。

证据：`validation/m0/fixtures/reference_config.json`、`real_ifs_ens_20200101_t2m_manifest.json`、`version_combination.json`；`validation/m0/run_reference.py:93` 起的实际流程。主代理确认 `/tmp/ensabc-m0-baseline/bin/python` 可导入 scikit-learn 1.9.1 和 NumPy 2.5.4；这只证明参考环境当前可读，不是重跑参考结果。子代理最初在默认 Python 中未找到 sklearn，不应将其推广为锁定虚拟环境不存在。

## 2. Rust 数值依赖与算法所有权

**Decision**：新增独立 `ensabc-core`，选用 `nalgebra = { version = "=0.35.0", default-features = false, features = ["std"] }` 做 dense f64 SVD；标准化、初始化、Lloyd 迭代、结果摘要由窄模块实现。随机初始化使用 `rand = { version = "=0.9.2", default-features = false, features = ["std"] }` 和 `rand_chacha = { version = "=0.9.0", default-features = false, features = ["std"] }` 的 `ChaCha8Rng`，每任务独立种子，不启用 OS/thread RNG。

**Rationale**：纯 Rust SVD 和明确的迭代语义可隔离宿主依赖，也便于检查空簇、停止规则和初始化差异。`std` 足够用于动态矩阵，不需要 nalgebra 的默认宏或随机 features。明确随机算法和抽样规则，避免依赖可变化的通用随机分布默认值。

**Alternatives considered**：`ndarray` + `linfa-linalg` + `linfa-clustering` 增加需要对齐的停止、重启和错误语义；外部 BLAS/LAPACK 或 Python 后端不符合既有 core 约束。自行编写 SVD 维护成本过高。只使用显式初始中心会使正常用户必须先准备中心，因此同时保留固定种子的原生 k-means++。

主代理读取 crates.io 的 0.35.0 元数据和已发布源码包：MSRV 为 1.89.0，低于本项目 Rust 1.96.0；默认 features 为 `std, macros`，所选 `std` 使用 `matrixmultiply`，未启用 BLAS/LAPACK 路径。包校验值为 `adc43a60c217b0c6ff46e47f26911015ad8d2e5a8be1af668c67e370d99a4346`。`src/linalg/svd.rs:712` 的 `SVD::try_new` 保证奇异值降序，接受 epsilon 及有限迭代上限并在超限时返回 `None`。实现采用 epsilon `f64::EPSILON`、上限 10000，作为 `m1.native.v1` 算法常量记录；禁止以 0 使用无限迭代。

版本与 features 的来源：

- <https://crates.io/api/v1/crates/nalgebra/0.35.0>
- <https://docs.rs/nalgebra/0.35.0/nalgebra/linalg/struct.SVD.html>
- <https://crates.io/api/v1/crates/rand/0.9.2>
- <https://crates.io/api/v1/crates/rand_chacha/0.9.0>

完整 Cargo 依赖树、目标平台编译和运行数值对照仍须在实施阶段完成；本轮未改 Cargo.toml/Cargo.lock，也未构建候选 crate。

## 3. 标准化、秩与投影

**Decision**：以 f64 的修正两遍求和计算列总体均值和方差，沿用 sklearn 的常量检测误差界；常量列保留且 scale=1。标准化后再减去其列均值进行 PCA；对中心化矩阵求 SVD，scores 用矩阵与保留的右奇异向量相乘，解释方差为 `s²/(n-1)`。维度上界为有效秩。相邻奇异值在保留/舍弃边界无法区分时拒绝；内部同值子空间允许等价基。

**Rationale**：仅对零方差设 scale=1 会与参考对接近常量列的数值处理不同。PCA 的再中心化和数值秩需要显式记录，不能只从列数推断。使用有限迭代 SVD 避免异常输入无限等待。

**Alternatives considered**：直接比较未经对齐的 components、用库默认 rank 阈值、对退化输入自动降维，均不满足规格。

已复核的锁定源码：

- sklearn `preprocessing/_data.py:83`：`var <= n*eps*var + (n*mean*eps)^2` 判定数值常量。
- `validation/m0/run_reference.py:132`：M0 有效秩使用 `s > rank_threshold * max(s_max, 1)`，不只是绝对阈值。
- M0 比较器 `validation/m0/compare_reference.py:230` 起同时逐项比较 components/scores，不能直接接受任意等价旋转；M1 新建比较器，保持 M0 规则和旧证据不变。

精确算法及对齐规则见 [数值契约](contracts/numerics.md)。主代理用锁定参考环境对现有载荷运行一次规划检查：载荷 SHA-256 为 `ae8f0349cc1c6cf189e1757c454163a5ec550dc5768b50c03cb9adf34f7baa64`，中心化标准矩阵前 3 个奇异值为 11.307624995288199、8.833154407656208、7.861905394964503，二维截断间隙约 0.971249，大于当前阈值。该观察只证明当前参考样本的边界有分离，未验证原生 SVD。

## 4. 初始化、停止及两条比较路径

**Decision**：原生默认配置采用 `kmeans++_chacha8_v1`、10 重启、seed 0、300 迭代和 `tol=1e-4`。另一种初始化为显式中心，每个重启都有独立的 `k×d` 数组。共享中心的算法步骤对照设置一次重启；真实 M0 原基线与原生默认运行的最终分区及可比数值另作独立比较。

**Rationale**：同一 seed 不等于 NumPy 和 Rust 产生相同初始中心。两种检查分别回答“原生最终结果是否符合原基线”和“在相同起点下计算步骤是否正确”。不能用注入参考的最终标签或最终中心伪造初始化一致。

**Alternatives considered**：移植 NumPy MT19937 及 sklearn 贪心 k-means++ 的全部随机路径，成本较高且不是 M1 必需条件；直接调用 Python 违反 core 边界；用相同 seed 声称随机流相同没有依据。

原生随机初始化为标准单候选 D² 抽样，不宣称逐次复现 sklearn 贪心多候选初始化。正常原生默认运行仍须在冻结真实样本上达到 M0 同一分区；若未达到，FR-007 失败，不能用共享中心检查替代该门禁。允许在最终验收前据实修改并版本化初始化方案，保留失败运行，不改原 M0 基线或事后放宽分区/数值门槛。

主代理已读取锁定环境 `_kmeans.py:285`、`:680–760`：停止阈值为投影列总体方差均值乘 tol，先判断标签稳定，再判断中心位移平方和；容差停止后还有一次仅分配标签的 E-step。计划沿用这些语义。空簇、未收敛和非有限值按 M1 规格报错，不模仿 sklearn 的空簇修复或未收敛成功返回。这些有意差异用合成案例明确验证。原生与原参考的随机起点不同，不要求 `n_iter` 恰好相同；原参考三进程自身仍按 M0 的严格整数规则对照，共享初始化轨迹则检查停止次数和原因。

同组分区的 restart 选择与 sklearn 存在小差异：原生选择最低 inertia，相等选择最早 restart；不把参考内部 `_is_same_clustering` 的选择启发式列为产品语义。相关结果必须在冻结容差下等价。

**规划期 Python 实验**：在单线程锁定环境对同一 50×9 载荷运行原参考，得到簇大小 `[20,16,14]`、inertia `87.09307906385827`、3 次迭代。另以输入行下标 `[0,16,33]` 的 scores 为显式起点、n_init=1，得到非空簇 `[12,36,2]`、inertia `119.99258497127667`、3 次迭代。两者分区不同，证明共享初始化诊断不能替代原基线符合性。两者均只运行一次，不是三进程验收；未实现或测量原生算法。原始临时记录为 `/tmp/ensabc-m1-research/reference-probe.json`，不作为新检出目录的依赖。

## 5. 类型化 SQL 输出与独立 core

**Decision**：新增 `ens_cluster(members, config)` 标量函数，保留 `ens_m0_echo` 的签名、行为和已存在的扩展入口/产物名称。SQL config 为必填 STRUCT，输出为类型化嵌套 STRUCT/LIST；矩阵采用 `DOUBLE[][]`，明确每一维含义。core 使用自身 Rust 类型，直接接口不含 DuckDB、Arrow、序列化或网络类型。

**Rationale**：M0 已经用 C API 特殊 NULL handling 证明了安全输入路径；锁定版本 C API 提供嵌套结果所需 API。类型化结果便于保存、展开和查询，同时不会把结果 JSON 解析转嫁给用户。

**Alternatives considered**：更换 Arrow 路径需要重新解决顶层 NULL；把 M0 echo 改成聚类会破坏既有契约；将 task_id、来源元数据或内容哈希逻辑塞进数值 core 不必要。SQL 结果保留算法版本及已用配置，任务、样本、源码和环境身份由调用方及验收文件关联。

主代理复核缓存 `libduckdb-sys-1.10505.0/src/bindgen_bundled_version_loadable.rs`：具备 `duckdb_vector_size`、`duckdb_list_vector_reserve`、`duckdb_list_vector_set_size`、`duckdb_struct_vector_get_child`、`duckdb_vector_ensure_validity_writable`、`duckdb_vector_assign_string_element_len` 等符号。`reserve` 与 `set_size` 的返回状态都须检查；reserve 后重新取得子向量及数据指针，防止缓冲移动后继续使用旧指针。

采用每个回调先把本 chunk 所有行计算成 owned 结果，再统一分配并写回。检查嵌套长度、offset 和所有类型转换；UTF-8 字符串按长度复制；FFI 用 catch_unwind，NULL config 和 NULL members 分别拒绝。每行拟合及 RNG 状态局部持有，线程间不共享可变状态。主代理在实际 DuckDB CLI 对完整返回 STRUCT 类型执行 `typeof(NULL::类型)` 并用临时 SQL macro 绑定文档示例的成员/config，解析和字段类型均成功；macro 仅回传输入，不执行聚类。实际 C API 嵌套输出、常量参数向量、错误后恢复和批量边界仍属于实施验收，不能从类型解析或符号存在推导成功。

来源：`crates/ensabc-duckdb/src/lib.rs`；<https://duckdb.org/docs/stable/clients/c/vector>；<https://duckdb.org/docs/stable/clients/c/scalar_functions>。以本地锁定 bindings 为确切接口依据，在线资料只作说明。

## 6. 验收、资源与复现

**Decision**：新增 `validation/m1/` 的契约、门禁和证据，不修改 M0 判定规则。复用 M0 的样本准备/核验 CLI、参考 runner 和版本锁文件；M1 专用比较器、结果断言、报告 schema 及 traceability 单独实现。最小运行容器同时验证 core-only 小程序及实际 SQL 聚类。报告必须覆盖 17 FR、8 SC 并重算引用哈希。

**Rationale**：M0 的 echo 和 14/6 固定矩阵不能证明 M1 的实际计算。复用已存在的来源恢复能力，避免建立通用验证平台。M0 的进程 RSS 代码含累计子进程口径，M1 以独立 PID 的 wait4.ru_maxrss 采集，不能无修改照搬。

**Alternatives considered**：扩展 M0 门禁条件会改变旧证据意义；复制全部 M0 文件造成维护分叉；仅检查 status 字段、链接依赖或编译结果无法验证正确性。

完整执行依赖为：环境/源码身份 → 输入/配置冻结 → 原参考 → 原生完整结果 → 原基线比较及投影 Q → 共享参考/初始化/轨迹 → 共享原生结果 → 共享比较 → SQL、错误、并发及边界 → 最小运行、测量及独立复现 → 追溯 → 报告 → 独立审计。三类数值命令必填 stage，缺少前置返回 2，不静默反向补跑。审计不写入被自己哈希引用的 report，避免循环。整体成功须同时满足 report.passed、traceability.pass、audit.pass。

干净复现采用同一有效源码快照在新目录重建：若当前源码已提交则 clone 同一 commit；开发目录有改动时，只将明确列出的相关源码、锁文件、脚本和 fixture（不含受限制载荷、密钥和旧 artifacts）导出到临时仓库并提交，记录 snapshot digest，然后从临时仓库 clone。新目录 `git status` 必须干净；载荷在外部目录按记录途径恢复并以参数传入。标明同机/缓存/网络限制，不把源码快照的临时 commit 声称为原仓库 HEAD。

## 7. 分析问题的设计修订

### I1：阶段依赖必须显式

**Decision**：`run_reference` 分 original/shared，`verify_core` 分 native/shared，`compare_results` 分 original/shared；stage 必填。先生成两条原始运行再计算 original 比较/Q，随后生成 shared 参考和 native shared 结果，最后比较。各 stage 写独占产物，门禁及复现子运行遵循同一顺序。

**Rationale**：原指南仅调用一次 reference，不能同时满足 original 不依赖 native 和 shared 需要 native 投影/Q 的前置条件。

**Alternatives considered**：默认 all 自动递归补跑会模糊失败、覆盖和身份边界；将共享初始化硬编码在参考坐标而不记录 Q 不能验证各自完整 PCA 路径。

### U1：观察实际参考内核，验证观察无扰动

**Decision**：使用专用单线程 Python 子进程，在内存中临时包装锁定模块 `_kmeans.lloyd_iter_chunked_dense`，每次调用真实保存的内核一次并复制输入/输出状态，调用未改写的 `_kmeans_single_lloyd` 控制循环；最后用未包装调用逐字段精确验证，try/finally 恢复符号。无需复制或改写 sklearn 算法，详见数值契约。

**Rationale**：`_kmeans_single_lloyd` 不提供轨迹回调；透明包装它实际调用的每轮内核能记录真实执行过程，还可识别最终只分配步骤。

**Alternatives considered**：解析 verbose 只含部分 inertia/停止文字，无法恢复全量中心/标签；自行翻译参考迭代循环引入第二套实现；修改安装的源码会破坏锁定环境身份。

**规划验证**：在锁定 sklearn 1.9.1、NumPy 2.5.4、单线程环境，对真实 50×9 输入的二维 scores 和起点 `[0,16,33]` 分别运行包装/未包装参考。tol=1e-4 时，3 个更新步、无额外分配步，位移平方和依次约 0.8145466892、0.0131286470、0，惯性 119.99258497127667；tol=1.0 时，1 个更新步加 1 个最终分配步，惯性 120.18776101731086。两种情况下 labels、centers、inertia、n_iter 均与未包装结果逐项精确相同，原符号恢复。后一案例只用于覆盖容差停止，不更改验收配置或门槛。最终分配调用的 shift 缓冲保留先前值，因此 trace 明确将其记 null 且不计迭代，避免伪造新位移。

临时规划记录 `/tmp/ensabc-m1-research/trace-observer-probe.json` 不作为干净检出的输入；正式 observer-check 与引用哈希仍由后续 T021/T022 生成。未验证 Rust trace 或正式共享对照。

### I2：公开初始化来源与显式中心区分

**Decision**：在公开 RestartSummary/SQL 增加 `initial_member_indices`；随机模式为与初始中心绑定的 k 个零基输入下标，显式中心模式为空非 NULL 列表。共享验证所选真实起点另记入 initialization 证据，不能把任意显式中心伪造为成员来源。

**Rationale**：数值契约原要求来源下标，原模型没有存储字段；显式中心未必对应真实成员。现在字段和模式约束在 core/数据模型/SQL/测试任务一致，初始顺序不随最终簇编号重排。

**Alternatives considered**：最近成员下标不能证明中心来源；optional/NULL 字段破坏首版 SQL 非 NULL 结果约定；仅在开发日志存储会使公共结果无法核验原要求。

### I3：任务已生成状态

**Decision**：状态统一为“41 项任务已生成、分析问题已修订、尚未实施”，下一步为只读复核后 `$speckit-implement`。保留任务编号和未勾选状态，不将规划修订写成算法验收通过。

### I4：独立复现子流程不等待完整门禁

**Decision**：T034 实现 reproduce_independently.py 父模式（--run-dir、可选 --work-dir）和独立子模式（--reproduction-child 及必填 --output-dir/--source-manifest/--manifest/--payload/--config）。子模式只依赖 T001–T031 现有构建、六阶段数值与 SQL/恢复入口，不调用 T035/T036/T037 或递归父模式；T037 只调用该父模式。clone 外置输入/输出/build/cache，开始结束核对源码干净；子记录 scope=reproduction_subset，父模式比较结果并保留可审计副本，状态/退出码见命令契约。

**Rationale**：原方案在 T034 要求复现，但只在后续 T037 创建未完整定义的 gate 子模式，且外部输出没有具体参数。单一所有者让复现脚本可先独立验证，避免报告生成反过来成为 producer 的前置。

**Alternatives considered**：提前实现整个 gate 引入不必要的验收框架依赖；父/门禁各维护一套子流程容易漂移；只保存临时路径会使清理工作目录后审计失效。

这是文档与任务修订，尚未运行实际新 clone 或子模式；保留 41 项任务编号和未完成状态。

## 8. 决策完成度与验证边界

本轮已确定依赖、算法、初始化、SQL 结构、验证分工和复现方案，无待产品澄清项。以下是明确的实施门禁，不是尚待选定的设计：

| 实施门禁 | 当前证据 | 未满足时 |
| --- | --- | --- |
| 新 Rust 依赖的特性树及链接审计 | 包元数据、局部源码阅读 | 阻止原生运行通过 |
| 真实输入的原生结果对齐原 M0 | 未运行 | FR-007/SC-003 不通过 |
| 共享起点的标准化/PCA/Lloyd 对照 | 停止规则源码已核对，算法未实现 | 阻止迁移通过 |
| C API 嵌套输出和恢复 | API 符号确认，未实现写回 | 阻止 SQL 集成通过 |
| 无参考工具环境、资源和干净复现 | M0 有既有方法，M1 未执行 | 保持 M1 incomplete 或 failed |

派生样本再分发、其他平台、WASM 和签名发布的边界保持不变。不能将本研究或后续任务勾选当作 M1 整体通过。
