# M1 证据与验收命令契约

版本 `m1.report.v1`。本协议只定义待实现的产物与 CLI，不是已有检查通过记录。

## 1. 命令与退出码

所有新 Python 验证入口位于 `validation/m1/`，默认从仓库根运行，用 `python3 validation/m1/<name>.py`。依赖参考栈的入口由环境配置的 reference interpreter 启动；主门禁只用 Python 标准库编排，不能假设全局有 sklearn。

| 入口 | 参数与职责 |
| --- | --- |
| setup_env.sh | `--check` 仅核验；无参数复用 M0 安装并检查新增依赖/工具；读取 ENSABC_M0_* 既有环境覆盖与 ENSABC_M1_TARGET_DIR |
| run_m1_gate.py | `--run-id ID [--manifest PATH --payload PATH]`；所有步骤完整编排；输出 artifacts/m1/ID；拒绝目录已存在；默认使用 M0 固定清单及本地 payload 路径；独立复现调用 reproduce_independently.py 父模式，不提供 reproduction-child 模式 |
| prepare_input.py | `--manifest PATH --payload PATH --config PATH --output-dir DIR`；核验身份并生成 task/config/input refs 及绑定本次路径的 real_sample.sql，不修改来源清单或载荷 |
| run_reference.py | `--run-dir DIR --stage original\|shared`，stage 必填；original 仅执行原 M0 三进程参考，shared 读取已通过的 original 比较/Q 和双方投影后生成共享起点与带观察轨迹的参考；通过配置的 reference interpreter 子进程执行，记录版本/源码身份 |
| verify_core.py | `--run-dir DIR --stage native\|shared`，stage 必填；native 构建/执行真实、合成和三进程 core 结果，不读取 shared 产物；shared 消费已固定的共享初始化/参考并生成内部 trace 及公共完整流程结果；不代替 comparator 给出比较通过 |
| compare_results.py | `--run-dir DIR --stage original\|shared` 或互斥的 `--self-check`；只比较对应阶段的已有记录，拒绝身份不符；original 写 projection-alignment.json，shared 不覆盖原比较 |
| build_extension.sh | `[--output-dir DIR]`；默认 `/tmp/ensabc-m1-extension-target`，构建 core runner 和同名 ensabc_m0_extension 扩展；与 M0 target 隔离 |
| verify_extension.py | `--run-dir DIR --extension PATH`；真实 SQL/保存、边界/并发/错误恢复与 echo 回归 |
| verify_minimal_runtime.sh | `--run-dir DIR --extension PATH`；隔离环境实际 core/SQL 执行，host driver 生成 JSON |
| measure_tasks.py | `--run-dir DIR --extension PATH`；1/10/100、冷/已加载及串并行结果检查 |
| reproduce_independently.py | 父模式：`--run-dir DIR [--work-dir DIR]`，创建源码快照/全新 clone，调度本脚本子模式并写父运行的独立复现证据；子模式：`--reproduction-child --output-dir DIR --source-manifest PATH --manifest PATH --payload PATH --config PATH`，全部路径参数必填，独立编排已存在的构建、六阶段数值及 SQL/恢复入口；两模式参数互斥；独立的 `--self-check` 不接收父/子参数并检查模式/路径/源码/判定边界 |
| traceability.py | `--run-dir DIR`；重算所有断言，可在 report 前运行；如 report 存在则核对一致性 |
| verify_report.py | `--run-dir DIR` 或 `--self-check`；独立引用哈希及总体推导，自检坏证据 |

新命令计算失败/证据错误返回 1，缺少必要环境或未执行返回 2，当前命令范围全部满足返回 0。顶层 gate 的 0 要求完整 M1 通过；单步骤 0 仅证明该步骤。构建脚本沿用常规非零失败语义，由 gate 映射到 fail/not_run，不能把失败安装算作执行了算法。

不为本规划预建空脚本。上述参数是后续任务的实现约束，quickstart 命令在实施后才可执行。

### 数值阶段的唯一执行顺序

`prepare_input → run_reference(original) → verify_core(native) → compare_results(original) → run_reference(shared) → verify_core(shared) → compare_results(shared)`。

original 参考只依赖输入和原 M0 配置；native 验证只依赖输入与 M1 配置，其验收可使用已存在 original 记录，但不能依赖 shared。original 比较消费两者，校验几何和原基线符合性并写入 Q。shared 参考只消费这些既有记录及 Q，禁止反过来调用 shared core 来生成自己的前置结果。original 不通过时 shared 前置未满足，返回 2 并在 steps 记录 not_run；原失败保持 fail。缺少所需阶段记录同样返回 2，身份/哈希不符返回 1。每阶段只写自己的新产物，禁止覆盖已有目录或修改前阶段文件。

顶层 gate 与独立复现子集必须使用同一顺序；没有默认 all 模式或省略 stage 时自动递归补跑的行为。

### 独立复现的所有权与调用边界（I4）

T034 单独实现 `reproduce_independently.py` 的父/子两模式；子模式只调用已由 T001–T031 交付的入口，不导入或执行 T035 追溯、T036 审计、T037 gate，也不再次调用父模式。T037 只在完整门禁的 producer 阶段调用该脚本的父模式，不再实现第二套子运行调度。这样 T034 可以在 T032 后独立验收，不依赖尚不存在的 T037。

父模式 `--run-dir` 指向已有基线运行，消费其输入/配置/源码身份和 core/SQL 结果，不要求已有 report.json。可选 `--work-dir` 指向一个尚不存在的绝对目录；省略时自动创建唯一临时根目录。根目录内的 clone、sample、child-output、build 是相互分离的子目录，sample/output/build 必须位于 clone 外。父模式将恢复/核验后的样本与本次冻结 native 配置以显式路径传给新 clone 中的子脚本；不能用参考结果替代重新计算。工作目录不会覆盖或静默删除已有失败证据。

子模式以新 clone 为 cwd：`--output-dir` 必须是尚不存在的绝对路径，`--source-manifest` 为父运行冻结的有效源码清单，其他三个输入必须为已有的绝对文件路径，均位于 clone 外；路径规范化后拒绝落在 clone 内及通过 symlink 越界的输出。参数缺失/模式混用由 parser 返回 2；缺少环境或必要前置返回 2，身份不符或实际计算失败返回 1。子模式成功返回 0 仅表示下面固定子集通过，不代表完整 M1 passed。

子模式先核验 clone 干净及有效源码清单相同，再以该 clone 的锁文件重建到外部 build 目录；显式设置 CARGO_TARGET_DIR、ENSABC_M1_TARGET_DIR 与外部 Python bytecode cache，防止固定 /tmp 目录串扰或生成文件污染 clone。运行 prepare_input、六阶段数值验证、真实 SQL/保存/错误恢复和 M0 echo 回归，写输出到该 output-dir。最小容器、资源测量、再次独立复现、全 FR/SC 追溯及整体 report/audit 不属于子集，不能被子运行宣称已覆盖。结束时再次检查源码清单与工作树干净。

子输出包含 `reproduction-child.json`，版本 `m1.reproduction-child.v1`，字段为 `scope=reproduction_subset, status=pass|fail|not_run, source_manifest_sha256, executed_steps, excluded_steps, artifacts, reason`；没有 overall=passed 字段，也不生成完整 report.json。父模式独立核对子运行与基线的输入/配置/源码及数值/查询结果，连同子集证据复制到父 run 的 `reproduction/child/`；原始子记录保持字节不变，其内部 refs 仍以子 output-dir 为根解释并检查容纳性，父记录引用副本时统一使用相对父 run 根的 `reproduction/child/...` 路径及重算的字节哈希，再写 `independent-reproduction.json`。临时绝对路径只作 locator，不作为可审计 artifact ref；父运行最终审计无需依赖临时工作根目录仍存在。

## 2. 文件集合

```text
artifacts/m1/<run-id>/
  environment.json
  source-identity.json
  source-manifest.json
  inputs/identity.json
  inputs/task.json
  config/native.json
  config/comparison.json
  reference/original/run-{1,2,3}.json
  reference/original/summary.json
  reference/shared/initialization.json
  reference/shared/reference.json
  reference/shared/trace.json
  reference/shared/observer-check.json
  core/run-{1,2,3}.json
  core/synthetic.json
  core/shared.json
  core/shared-trace.json
  comparison/projection-alignment.json
  comparison/original.json
  comparison/shared.json
  comparison/self-check.json
  extension.json
  minimal-runtime.json
  measurements.json
  independent-reproduction.json
  reproduction/child/reproduction-child.json
  reproduction/child/           # 子运行实际产物的保留副本，父证据引用相对父 run 根
  steps.json
  traceability.json
  report.json
  report-audit.json
```

真实派生数据及证据不入库，全部由脚本重建。每条结果带 sample_kind=real|synthetic、输入/配置/源码/算法身份。步骤日志可另存 `logs/`，失败仍保存已有记录和原因，不覆盖重跑。

`comparison/projection-alignment.json` 绑定双方输入/投影/规则哈希、Q、几何断言和 original 比较引用；按 `scores_native * Q ≈ scores_reference` 定义方向，参考中心映射到 native 使用 `C_reference * Qᵀ`。`reference/shared/initialization.json` 记录双方中心、起点下标、同分数空间诊断的坐标及各文件 hash；不能用 native shared 结果反推初始化。

`reference/shared/trace.json` 与 `core/shared-trace.json` 分开记录迭代序号、update/assignment_only 类型、更新前后中心、标签、更新后 SSE 和 shift；最终仅分配步骤的 shift 为 null 且 `counts_as_iteration=false`。该 nullable 字段仅属于验证 trace，不改变公开 SQL 的非 NULL 结果约束。observer-check 保存相同输入的未包装参考结果及与带观察运行的逐字段精确比较、kernel 调用数和 wrapper 恢复状态；版本/符号不符或观察改变结果时该阶段失败。

## 3. Report、refs 与状态

report 必填：`schema_version, run_id, created_at, overall, source_identity_ref, environment_ref, input_refs, config_refs, algorithm_version, steps_ref, traceability_ref, requirements, success_criteria, limitations`。

requirements 必须恰有 FR-001…FR-017 各一项，success_criteria 必须恰有 SC-001…SC-008 各一项，禁止重复或未知 ID。每项为 `{id, status, assertion_ids, expected, observed, artifacts, reason}`；status=pass|fail|not_run，reason 对非 pass 必填。判定从断言重新推导，不使用手工“已完成”字段代替。

artifact ref 为 `{path, sha256}`，path 限本 run 目录内的相对规范路径，禁止绝对路径、`..`、越界 symlink。引用文件必须存在且逐字节 sha256 一致。输入源外部路径仅作为 source locator 保存；用于判定的内容身份和记录复制进 run 目录。清单 retrieved_at 可不同，但 payload/source-matrix 身份必须相同；二者的判断分别报告。

总体：任一必要 FR/SC/执行步骤 fail 则 failed；无 fail 但任一必要项 not_run 则 incomplete；全部 pass 且 traceability 有效时 report 可为 passed。audit 独立核对后，顶层 gate 只有在 `report.overall=passed AND traceability.status=pass AND report-audit.status=pass` 时返回 0。

## 4. 可执行追溯矩阵

traceability 为 `m1.traceability.v1`：`assertions, requirement_results, success_criteria_results, coverage, status`。每个 assertion 定义 `id, target_ref, json_pointer, operator, expected, observed, status, mapped_ids`。operator 的有限集合为 equality、numeric_close、set/partition equality、shape、sum、unique、finite、hash、all/count；复杂物理均值和最近成员断言由具名 verifier 计算并保留其输入与重算输出，不只检查 producer 自报 status。

| FR | 必须绑定的证据/实际断言 | SC |
| --- | --- | --- |
| FR-001 | 清单完整成员/点序/时间身份，task 形状及唯一身份 | SC-001 |
| FR-002 | 标准化、谱/投影、Lloyd 中间量与参考一致 | SC-003 |
| FR-003 | config 字段合法及参数正负边界用例 | SC-003、SC-005 |
| FR-004 | 每成员恰一次，物理均值、数量和占比重算 | SC-001、SC-002 |
| FR-005 | 最近成员/并列规则、簇 SSE 和总 SSE 独立重算 | SC-002 |
| FR-006 | 结果诊断完整形状、有限性、初始中心与 initial_member_indices（随机长 k、显式为空）、input/config/source/env refs | SC-002、SC-008 |
| FR-007 | 原 M0 对照与共享中心对照分别真实通过，阶段依赖/Q/轨迹及 observer-check 均可核对 | SC-003 |
| FR-008 | 冻结容差、子空间对齐及比较器正反例 | SC-003 |
| FR-009 | 三进程 PID、完整等价、两连接区间重叠及跨批量 | SC-004 |
| FR-010 | 全非法类别、错误位置、同会话恢复、混合失败 | SC-005 |
| FR-011 | 精确 SQL 类型、core/SQL 一致、批量与保存回滚 | SC-001、SC-004 |
| FR-012 | 真来源字段映射、有序输入到保存结果的完整路径 | SC-001 |
| FR-013 | 合成正例和每类非法/退化覆盖计数与实际断言 | SC-002、SC-003、SC-005 |
| FR-014 | 独立 core、最小环境工具清单与真实 SQL 运行 | SC-006 |
| FR-015 | 版本/依赖特性/实际加载、新 clone 干净且有效源码相同 | SC-008 |
| FR-016 | 三种规模×至少三次、时间/RSS 范围及串并行等价 | SC-007 |
| FR-017 | 可执行示例、新目录复现、全 FR/SC 覆盖及审计 | SC-008 |

coverage 要求 17/17 FR、8/8 SC，每项至少一条实际执行且足够证明要求的断言，无孤立 assertion。未执行 producer 的断言为 not_run，不能读取旧同名文件冒充当前运行。

## 5. 无循环审计与有效源码

顺序为 producers → traceability → report → audit。traceability 不引用 report 的哈希，report 可引用最终 traceability；audit 记录 report 的 SHA-256 和重新计算结果，但 report 不引用 audit。audit 不能反过来更新被它哈希的 report；不一致时退出 1，原文件保留，下次重新生成新 run。顶层 gate 读取二者，不把 audit 写进 report 制造循环。

source-identity 保存原 git HEAD、dirty 状态、相关文件清单的摘要及有效快照 commit（如有）。新 clone 的源码和锁文件必须与 source-manifest 一致。允许导出脏目录的明确源码 allowlist 到临时仓库，但这必须形成可重建的有效源码身份；不得暗中忽略 untracked 实现或复制真实受限制载荷。audit 必须确认独立复现针对同一有效源码，而不只确认某个任意 clone 干净。
