# M1 验证协议

本协议是实施验收目标。规划期只做源码/元数据核对，以下 M1 检查均未执行。对应全部 FR-001–FR-017、SC-001–SC-008；产物格式见 [evidence.md](evidence.md)。

## 1. 冻结与原参考

每次运行创建唯一 `artifacts/m1/<run-id>/`，拒绝覆盖；写入真实样本身份、manifest/payload/source-matrix 字节 SHA-256、M1 配置文件及语义摘要、算法与比较规则版本、源码清单和环境。已有受限制载荷不提交入库。缺失载荷时，可按 M0 步骤回源恢复到显式外部目录；源不可达则 dependent step 为 not_run，不能替换成合成样本宣布通过。

继续用原 M0 配置及 runner 生成至少 3 个独立进程参考，PID 各不相同。可复用同样身份且经过审计的既有参考，但必须把所消费的记录和原始引用哈希固定在本次运行目录，并验证完整性；不能只抄 `[20,16,14]` 和惯性作为参考。默认门禁重跑参考以验证环境。

M0 配置和源码作为只读输入，结果写到 M1 的 `reference/original/`，不修改原证据。原生默认结果与原基线的语义比较，以及共享中心的步骤/端到端对照，都按 [数值契约](numerics.md) 独立报告。

数值子步骤固定按 `run_reference --stage original → verify_core --stage native → compare_results --stage original → run_reference --stage shared → verify_core --stage shared → compare_results --stage shared` 执行。original 比较写出带双方投影身份的 Q 后才可生成共享参考；shared 不反向依赖自己的 native shared 结果。缺失/未执行前置记录为 not_run，已经发生的 original 失败保持 fail；不得自动省略该比较后放行 shared。顶层门禁及独立复现子集都使用同一顺序，省略 stage 属参数错误。

## 2. core 与摘要验证

在真实输入及合成用例直接运行 core runner。每个成功结果由独立验证驱动重算：

- n 个成员恰好一次归属，k 个非空簇，编号规则及成员顺序正确。
- count 之和=n，fraction=count/n，fraction 之和的绝对误差≤1e-12。
- 每簇 mean_features 从原始物理 X 独立求均值；代表成员根据最后返回中心与 scores 的平方距离及输入序决胜独立重算。
- 按最终 labels/scores/centers 重算每簇 SSE 及 total；验证所有诊断形状、有限性、秩、停止状态和配置。每个随机 restart 的 initial_member_indices 长度=k且下标合法，对应初始中心等于指定成员的 scores；显式中心模式该列表为空且中心与配置一致。
- 不能只检查结果中的 `converged=true` 或数组非空；它们必须由计算和数值断言支持。

合成正向集合包含 d=1、d=rank、k=2、k=distinct 的边界、不同合法形状、常量/接近常量列、同值不同 ID、代表成员并列、非 ASCII 及内嵌 NUL ID。具有等距数据时选取可解释、不会触发不确定谱边界的固定输入。共享迭代轨迹检验真实初始中心和停止过程；比较器正反例验证等价基、分区和数值的区分能力。

非法/失败用例分类至少包括：

| 组 | 类别 |
| --- | --- |
| SQL 表示 | 顶层 members/config NULL、NULL 成员、ID NULL、features NULL、NULL 元素、config 字段 NULL、中心数组任意层 NULL |
| 身份与形状 | 空成员、空 ID、重复 ID、空 features、ragged features |
| 数值输入 | NaN、+Inf、-Inf；序列化通路另验证非标准 JSON 被拒绝 |
| 参数 | d/k/seed/restarts/max_iterations 越界及非法符号；非整数绑定错误；负/非有限 tolerance，非正/非有限 rank_threshold，未知枚举，缺失字段、非法初始化数组 |
| 算法条件 | 原始或投影可区分行不足、rank<d、保留边界退化、重复中心导致空簇、达到 cap 未收敛、可构造数值溢出 |
| 工程错误 | checked length 溢出、模拟 SVD 不收敛及可恢复分配/FFI panic 路径，用内部受控测试；不要求故意耗尽系统内存 |

绑定期错误和运行期错误分别断言。SVD 上限等难以自然构造的异常允许内部故障注入，不在生产 SQL 添加开关。工程注入用于证明错误映射，不代替真实数值正例。

共享参考逐轮观察必须调用锁定真实内核，不自行重写参考 Lloyd。对标签稳定与容差停止（含最终 assignment_only）分别运行包装/未包装对照，要求最终结果精确一致，更新步计数=n_iter，assignment_only 不计入迭代且 shift=null；异常时也恢复 wrapper。私有符号/版本不符、观察改变结果、数组未复制或事件数不完整都使 observer-check fail。Rust 通过显式 ignored 导出测试产生相同事件语义的轨迹，普通测试无环境路径依赖。

## 3. SQL、批量、并发及恢复

真实样本经来源恢复 → 清单核验 → 有序成员构造 → ens_cluster → 保存结果 → 展开摘要，和直接 core 逐字段对照。配置常量、逐行配置变化、过滤/切片列表和不同合法任务形状都要覆盖。

通过 `duckdb_vector_size()` 取得 B，记录值。分别构造 B+1 以上 task rows 以及单任务 B+1 以上 members；用身份/数值差异避免常量折叠，检查每个任务和成员内容。以小维度且无退化的合成数据减少运行开销，不能只检查输出行数。

并发至少两个独立连接，barrier 同步开始并记录 query start/end 时间，证明执行区间重叠；不足以重叠时增加任务负载后重试并保留记录。分别与串行的固定输入结果核对。不能从同时启动进程推导 SQL 实际重叠。

每种可经 SQL 到达的非法类别在同一连接执行 valid → invalid → valid，核对具体错误码/层级及前后完整合法结果，不使用 TRY 抹掉错误。跨 chunk 的混合合法/非法查询须完整消费并最终失败；验证保存事务回滚，不把已流出的行算作成功批次。FFI panic 测试用受控测试构建，其产物和生产构建单独标识。

新增聚类前后均运行 `validation/m0/verify_extension.py` 的既有 echo 回归，使用同一扩展产物。

## 4. 最小运行隔离

使用锁定 digest 的 Linux ARM64 基础镜像，容器 `--network none`。只复制匹配 DuckDB CLI、扩展、core-only runner、本地样本/配置/预期、SQL 与必要系统库。容器内不安装 Python/NumPy/SciPy/sklearn/CDO/wgrib/wgrib2，并通过文件/命令清单及运行记录核对；ldd 作为辅助证据。

实际执行两条路径：core runner 独立计算；DuckDB 加载后运行 ens_cluster 的真实输入、非法输入恢复。宿主验证驱动比较完整输出。预期可在宿主用 reference 准备，但运行容器不能通过挂载整个开发目录获得参考工具。断网只限制计算运行，不要求首次下载依赖和镜像断网成功。

## 5. 资源测量

固定真实 50×9 输入分别构造 1、10、100 个任务。每规模至少 3 次独立测量，保存每次记录及中位数，记录 n/p/d/k/restarts、输入/输出字节、线程数和构建修订。

冷启动：独立 DuckDB CLI 子进程的 wall time 与该 PID 的 `os.wait4(...).ru_maxrss`，Linux 单位 KiB 明确换算。不要读取累计 `RUSAGE_CHILDREN` 当单次峰值。已加载：同一个新建的测量进程中，LOAD 后计时查询，计时范围只含查询；RSS 仍是整个进程生命周期峰值，明确含启动/加载和 warmup，不能称为查询独占峰值。载荷生成、结果解码、验证驱动若不在计时范围，分别说明。

计算、输入解码、输出编码可通过专用测量构建在 adapter 内计时，不能改变算法语义；无法可靠分离时填 not_separated，理由明确。core-only runner 的独立基准可补充算法开销，不能简单从两套端到端时长相减得转换开销。所有测量完整消费结果并核对串行/并发等价；输入包含可核对的逐任务身份，记录防常量折叠策略。不设吞吐阈值。

## 6. 新目录复现与源码身份

源码清单覆盖 core、adapter、Cargo manifests/lock/toolchain、打包工具身份、M1 全部验证脚本/配置/合成 fixture、所依赖 M0 脚本及锁文件。每个相关文件保存路径和字节 SHA-256；Git HEAD/dirty 状态不能代替此清单。相关 untracked 文件必须纳入，排除密钥、真实派生载荷和旧 artifacts，忽略的无关文件另行列明。

干净源码可 clone 同一 commit；开发脏目录允许导出明确 allowlist 的有效源码快照到临时仓库并创建临时 commit，再 clone 到全新目录。记录原 HEAD、相关改动摘要、有效 snapshot digest、临时 revision 及 clone 位置；新目录的相关源码文件摘要须逐项相同且 git status 干净。此流程不修改原仓库历史。不能只复制已提交文件漏掉未提交实现。

T034 的 `reproduce_independently.py` 拥有父/子两模式：父模式创建新 clone 并比较基线，子模式通过 `--reproduction-child --output-dir DIR --source-manifest PATH --manifest PATH --payload PATH --config PATH` 在该 clone 内直接编排已有 producer；T037 gate 只调用父模式，子流程不依赖 gate/traceability/auditor 或再次复现。具体参数互斥、路径及状态见 [evidence.md](evidence.md)。

在 clone 内按锁文件重建、实际加载扩展并执行真实数据路径、六阶段数值对照、非法输入恢复及 echo 回归。使用外部位置的样本、配置、输出、Cargo target 与 Python cache，避免生成文件或清单 retrieved_at 修改工作树；开始和结束均核验工作树干净及有效源码身份。环境可使用声明的缓存，但不能隐含引用作者个人路径；同机/共享网络/缓存限制明确记录。

子运行只生成 scope=reproduction_subset 的 `reproduction-child.json` 及实际 producer 证据，0/1/2 分别表示子集通过/失败/未执行完整；不生成完整 M1 passed 报告，也不代替隔离、资源或全需求审计。父模式重新核对输入/配置/源码和基线结果，将必需子证据保留到父 run 的 reproduction/child/，重新计算引用哈希后写独立复现判定；不能仅凭子进程退出 0 或临时目录存在宣布复现成功。

## 7. 门禁汇总与反例

独立可执行断言覆盖全部 17 FR 和 8 SC，并检验无孤立断言。必需类别缺失/未执行为 not_run；实际不符为 fail。最终 report 依据断言和 producer steps 生成，再由 auditor 重算所有引用哈希、重新评价 traceability 并核对总体判定；auditor 不启动新计算，也不修复原证据。

门禁/审计自检至少覆盖：all-pass、某 FR fail、某 SC not_run、缺失 requirement、重复 ID、孤立 assertion、仅合成样本、仅共享初始化无原基线、缺失前置投影/Q、observer-check 未执行或不一致、随机/显式来源下标规则错误、无实际聚类的 echo、hash 不符、路径越界、traceability 与 report 矛盾以及无干净新目录复现。复现接口还须覆盖父/子参数混用、输出或 build 落入 clone、源码不符、子集 0 冒充整体通过、临时工作目录失效后父报告所需副本仍可审计；这些检查由 T034 完成，不等待 T037 才能验证父/子边界。任何反例不能被接受为完整 M1 通过。
