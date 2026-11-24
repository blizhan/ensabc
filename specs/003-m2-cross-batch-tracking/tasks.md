# Tasks: M2 跨批次情景追踪

**Input**: `specs/003-m2-cross-batch-tracking/` 下的 [spec.md](spec.md)、[plan.md](plan.md)、[research.md](research.md)、[data-model.md](data-model.md)、[contracts/](contracts/) 与 [quickstart.md](quickstart.md)。
**Prerequisites**: M1 已完成；本清单依据 2026-10-29 的 5 项澄清与 Phase 0/1 设计生成。constitution 仍是未填写模板，不从示例推导额外审批。
**Status**: T001–T056 全部 56 项实施与验收任务已完成并通过正式门禁验证（见 `specs/003-m2-cross-batch-tracking/quickstart.md`）。
**Tests**: spec FR-012、FR-016–FR-021 与 SC-001–SC-008 明确要求数值参考、错误恢复、事务、扰动、隔离、资源、回归与复现验证，因此包含相应测试任务。各故事先定义可失败的契约测试，再实现并执行；测试准备任务完成不代表故事已通过。
**Organization**: 按 US1–US5 分组。故事可分别验收，但真实数据闭环依赖 core、SQL 与历史保存；具体依赖见文末。

## Format: `[ID] [P?] [Story] Description`

- 所有路径相对仓库根目录 `/home/blizhan/repo/github/ensabc`。
- `[P]` 表示满足注明的前置条件后，可与同一依赖层中不写相同文件的任务并行；不表示可跳过前置或自动授权派生 worker。并行写入使用独立 worktree，同文件的修改由单一写者顺序合并。
- `[US1]`–`[US5]` 对应 spec 中的 5 个用户故事；Setup、Foundational、Polish 不带故事标签。
- 当前生产 SQL 扩展集中在 `crates/ensabc-duckdb/src/lib.rs`，尚无 `cluster.rs`、`output.rs`。新增 `match_ext.rs`，仅在需要时提取窄的公共辅助，不全面拆分 M1。
- 只提交源码、清单、合成数据、锁文件和规则；真实数值、均值场、OM、数据库、实验输出放在忽略的运行目录或外部路径。

## Phase 1: Setup — 环境与版本基线

**Purpose**: 固定依赖与发布包身份，建立独立 M2 运行目录；不重建整个工程。

- [X] T001 核对并记录 `Cargo.toml`、`Cargo.lock`、`rust-toolchain.toml` 与 M1 版本组合，在 `Cargo.toml` 的 workspace `serde_json` 开发使用路径启用 `float_roundtrip`，保持锁中 `serde 1.0.229` 与 `serde_json 1.0.154`，不引入生产依赖或为匹配计划文字降级依赖；在 `specs/003-m2-cross-batch-tracking/plan.md` 同步实际依赖/源码布局。
- [X] T002 [P] 在 `.gitignore` 与 `validation/m2/README.md` 定义 `artifacts/m2/<run-id>/`、外部缓存/build/venv 路径和首次联网/后续离线边界，说明 WB2 研究用途、派生载荷不入库、omfiles 仅在准备/参考进程使用与各命令 0/1/2 退出含义。
- [X] T003 [P] 建立 `validation/m2/requirements-om.lock` 与 `validation/m2/fixtures/duckomo_releases.json`：锁定 omfiles 1.2.0、numpy 2.5.4、numcodecs 0.17.0 及所有所需传递依赖的 hash；钉住 DuckOMO rc.1/0.3.4 的 v1.5.5/linux_arm64 URL、ZIP/manifest/扩展 SHA-256、source_commit 与宿主身份，使用 `contracts/om-duckomo.md` 的完整哈希而非省略值。
- [X] T004 在 `validation/m2/setup_env.sh` 实现只核验的 `--check` 和显式安装/下载模式，复用 M0 数据与参考环境、单独准备 OM venv，验证两版发布 ZIP/manifest/SHA256SUMS/解压扩展；支持外部路径覆盖、缓存与 hash 不符拒绝，禁止静默换成本地 DuckOMO 构建（依赖 T001–T003）。

**Checkpoint**: 支持环境可核验，准备工具与生产依赖隔离，未生成或提交真实载荷。

## Phase 2: Foundational — 公共输入、身份与执行边界

**Purpose**: 给所有故事提供冻结规则、证据身份和一致的失败语义。以下基础完成后进入故事实现。

- [X] T005 [P] 在 `validation/m2/fixtures/wb2_8run_source.json` 固定 8 个源对象的 generation/长度/完整 SHA-256、预期 float32 载荷 SHA-256、元数据与坐标块身份、起报槽/lead 切片、UTC 时间、50 个扰动成员映射和 9 点顺序；在 `validation/m2/fixtures/spatial_weights_eu3x3.json` 固定 `cos_lat_center_v1` 十进制与 hex 位值、源网格坐标及单位（FR-002、FR-003、FR-013）。
- [X] T006 [P] 在 `validation/m2/fixtures/tracking_experiment_config.json` 冻结 d=2、M1 native_config 全字段（seed=3、10 restarts 等）、主 k=3/c=1.0、k=2/4、6 个 c 值、匹配级 ε=0.01/100 方向、成员级 σ=0.001 K/20 样本与 0.01 K 压力档；明确 NumPy 2.5.4 的 PCG64、种子 20261029/4242、循环顺序、抽样/归一化方法、σ=0 对照和稳定性证书，不能保留“PCG64 或 ChaCha8”二选一。
- [X] T007 在 `specs/003-m2-cross-batch-tracking/contracts/numerics.md`、`contracts/core.md`、`contracts/sql-history.md` 与 `contracts/validation-evidence.md` 落实并核对 I1/I2/I3/I4/I5/U1/U2 修订后的执行细节：离散平方项对全部 `(member, point)` 做一次精确累加、最终舍入后除 N/W；绝对 MatchInput 仅四字段、比例输入三尺度参数必填；单批预期失败可记 failed/断链，元数据不兼容及匹配/发布异常禁止发布；匹配暂存失败不得继续提交；明确显式 ROLLBACK 和 CLI 总退出码检查；定义固定字段顺序、-0 归一、浮点 round-trip 的 binding/content 序列化、三进程精确重复/新 clone 重建语义复现/跨 DuckOMO 版本比较规则及 cells→real→replay 阶段边界，保留所有用户决定。
- [X] T008 在 `validation/m2/common.py` 与 `validation/m2/schema.py` 实现独占运行目录、原子 JSON 写入、有限数/形状校验、步骤 pass/fail/not_run、run 内相对 refs 与重算哈希、有效源码 allowlist（含相关 untracked 源码、锁文件、SQL 和 fixtures）及外部工具/产物定位；binding 排除个人路径/获取时间等运行噪声，保留源内容、配置、权重、算法/软件身份；定义同版本逐位对照与跨版本/独立重建验证实际产物身份后仅归一合法 run_key/branch 前缀的比较，以出生节点键核对完整继承链（依赖 T005–T007）。

**Checkpoint**: 实验配置可冻结、输入身份可核对、命令和发布失败边界明确。所有参考只能消费当前运行的已核对输入，不依赖旧会话或 `/tmp` 原型产物。

## Phase 3: User Story 1 — 判断两次起报的情景是否延续 (Priority: P1) — MVP

**Goal**: 独立于 DuckDB/OM/历史，完成两批物理场的可拒绝匹配，并报告距离、歧义和可替代程度。
**Independent Test**: 用绝对门槛直接调用 Rust core，与 Python Fraction 穷举比较；反例 `[[0,4],[4,6]]`、τ=5 得两条距离 4 的关联，重编号后节点级关系不变。

### Tests and fixtures

- [X] T009 [P] [US1] 在 `validation/m2/fixtures/synthetic_match_cases.json` 定义手算距离、τ=0/等号/全超距、矩形 2→3 与 3→2、门槛反例的可实现物理场、完全并列/不同边多重集精确并列、1 ulp 近并列、相同场/-0、全部形状/非有限/权重/离散错误用例；区分内部距离矩阵求解测试与公共物理场接口，禁止为了精确复现矩阵绕过公共距离计算。
- [X] T010 [P] [US1] 在 `validation/m2/oracle.py` 实现 Python 标准库 Fraction 穷举全部部分匹配、精确目标比较、全部最优解/规范决胜、禁边替代解及未匹配项是否出现在某最优解；独立生成 4,000 个固定种子随机小例（单侧 1..5），不得调用或翻译生产 DP 作为参考（固定边界用例来自 T009）。
- [X] T011 [US1] 在 `crates/ensabc-core/tests/m2_matching_contract.rs` 定义公开函数的输入边界、12/13 情景上限、单成员侧、重复场、缩放与编号置换测试，覆盖三态替代程度/空匹配/未关联歧义；在 `crates/ensabc-core/src/fsum.rs` 的内部测试定义精确和/取消/末位舍入/溢出以及平铺/嵌套舍入反例，并增加一般权重缩放在门槛处的 1 ulp 决策翻转用例，记录实现前的预期失败（依赖 T009）。

### Implementation and verification

- [X] T012 [US1] 在 `crates/ensabc-core/src/lib.rs`（必要时使用 `src/matching.rs` 存放新类型并 re-export）加入 `ScenarioNodeInput`、绝对/比例门槛输入、MatchResult/替代程度/未关联类型、M2 错误码与定位、版本常量和 12 情景上限；为新增错误位置更新 M1 构造点的默认空值而不改变 M1 签名、已有错误码或算法。
- [X] T013 [US1] 在 `crates/ensabc-core/src/fsum.rs` 实现有限值精确展开累加、一次 round-half-even 舍入及展开差符号比较，检查长度/分配/数值溢出，不将近似容差引入完全并列判定；通过 T011 的求和反例（依赖 T012）。
- [X] T014 [US1] 在 `crates/ensabc-core/src/dispersion.rs` 实现全部 (member, point) 项共用展开、最后舍入一次的总体口径 `ensemble_dispersion`、权重与成员形状/身份校验、零离散拒绝，并在 `crates/ensabc-core/src/matching.rs` 实现同权重加权 RMSE、有限中间量检查与 `S=(s_prev+s_curr)/2; τ=c*S`；验证仅 `d<=τ` 决定可行性、比值只展示（依赖 T013）。
- [X] T015 [US1] 在 `crates/ensabc-core/src/matching.rs` 实现规范场键（-0 归一、逐格 total_cmp、相同场才用 cluster_id）、位掩码 DP 最大条数/最小精确距离和、最优解计数与伙伴向量字典序重构，复杂度限于单侧≤12，拒绝超限而非降级（依赖 T014）。
- [X] T016 [US1] 在 `crates/ensabc-core/src/matching.rs` 加入逐选中边禁边重求、tied/same_count/count_drop、未匹配原因与 `matched_in_some_optimum`、完整距离/比值/可行矩阵和余量；按输入 ID 映射输出并保留矩阵轴身份，不能把 pair 歧义直接赋给所有选中边（依赖 T015）。
- [X] T017 [US1] 在 `crates/ensabc-core/examples/m2_runner.rs` 实现 `--mode match|dispersion --input --output` 的严格 JSON DTO、单对/批量输入、绝对/比例模式、原子输出与 0/1/2 退出；校验未知字段/非标准数值并验证 float_roundtrip，文件与序列化不进入 core（依赖 T016）。
- [X] T018 [P] [US1] 在 `validation/m2/verify_matching_core.py` 组织 runner 与 T010 oracle 对照：固定正反例及≥4,000 随机小例、≥2,000 编号/顺序置换，逐字段比较目标/方案数/边/替代/未匹配及歧义；独立按冻结公式核对距离与离散、合法权重缩放（精确 2 的幂逐位/一般常数 rtol=1e-15；远离边界要求决策一致、边界翻转记录）和错误码，写 `core/synthetic-oracle.json`、`permutation-invariance.json`、`error-cases.json`（依赖 T010、T017）。
- [X] T019 [P] [US1] 在 `validation/m2/build_extension.sh` 增量复用 M1 已锁定打包流程，支持外部 target/output，构建同名扩展及 m1_runner/m2_runner 并记录源码、工具链与二进制哈希；不修改 `validation/m1/build_extension.sh` 或假设构建产物字节可重复（依赖 T017）。
- [X] T020 [US1] 执行 `cargo test --locked -p ensabc-core`、`validation/m2/verify_matching_core.py` 与依赖/链接审计，核对无生产序列化/数据库/参考依赖，并在 `artifacts/m2/<run-id>/core/` 保存当前源码对应证据；全部必需用例通过才标记 US1 完成（依赖 T011–T019）。

**Checkpoint**: 两批匹配独立可用；这是算法 MVP，不代表真实数据接入或 M2 整体通过。

## Phase 4: User Story 2 — 沿多次起报保持身份并保存历史 (Priority: P1)

**Goal**: 从合成多批情景得到确定身份链与可审计历史，支持缺批断链、幂等和整体回滚。
**Independent Test**: 不依赖真实来源或 DuckOMO，用合成成员/情景执行 4–8 批 SQL 回放，保存后验证五表、三进程一致、冲突拒绝与失败零残留。

### Tests

- [X] T021 [P] [US2] 在 `validation/m2/test_sql_history.py` 定义新函数类型/core 等价、嵌套 NULL/绑定错误、跨 chunk 混合失败，以及元数据/非法输入/匹配错误必须阻止发布、已分类单批算法失败可发布并断链的区分和 4/8 批链、2→3→2、缺批/聚类失败/零离散、并列继承、同键幂等/冲突、新版补插重放与中途故障的预期；验证失败后同一连接完整恢复，测试不得依赖真实数据（依赖 US1）。

### SQL interface and history implementation

- [X] T022 [US2] 在 `crates/ensabc-duckdb/src/match_ext.rs` 建立 C API 类型/解码与两函数注册，结合 `src/lib.rs` 暴露必要窄辅助；逐层检查 NULL、LIST offset/length、BIGINT→usize、常量/字典输入、checked arithmetic，并启用 special handling、panic 边界与资源释放，不为此重构整个 M1 adapter（依赖 T012、T019）。
- [X] T023 [US2] 在 `crates/ensabc-duckdb/src/match_ext.rs` 实现 `ens_dispersion` callback，绑定真实 core 离散函数并附 chunk row/成员/格点位置；整个 chunk 先计算 owned 结果再写回，非法成员/权重/零离散明确失败（依赖 T022）。
- [X] T024 [US2] 在 `crates/ensabc-duckdb/src/match_ext.rs` 实现 `ens_match_scenarios` callback 和 `contracts/sql-history.md` 的完整嵌套输出，reserve/set_size 后重取指针、检查返回状态；Option 按 has_* 标志与有限占位数映射，不把 count_drop 的 0.0 当成 tied；确保两函数无历史/身份副作用（依赖 T023）。
- [X] T025 [P] [US2] 在 `validation/m2/sql/schema.sql` 落实五表 typed DDL、主外键、单侧关联唯一性、状态/有限值/相邻 ordinal 约束，并补足同批 canonical_rank 唯一性；跨表语义由发布守卫验证，不依赖 CHECK 子查询（依赖 T007、T021）。
- [X] T026 [US2] 在 `validation/m2/sql/stage_and_publish.sql` 实现计划批次 TEMP 暂存、每批独立 autocommit 计算、缺批/已分类且输入合法的单批算法错误状态（保存具体错误码与位置），以及相邻 ok 批次匹配；验证计划间隔/元数据，分离 `.bail off` 的已声明单批故障与其他异常；匹配失败、未知 SQL 错误或未完成暂存必须阻止进入发布（依赖 T024、T025）。
- [X] T027 [US2] 在 `validation/m2/sql/stage_and_publish.sql` 加入同一规范键的出生 rank、递归 CTE 身份继承/断链、新生原因、歧义边到身份链的可追溯标记、run_key/content_sha256 及单事务发布；核对五表内容/节点精确集合、相邻比较和关联端点/继承/无复活不变量，同键异内容 error、同内容幂等，禁止简单 ON CONFLICT DO NOTHING（依赖 T026）。
- [X] T028 [US2] 在 `crates/ensabc-core/examples/m2_runner.rs` 加入开发验证用 `--mode chain`，只消费本地矩阵/预计算身份上下文，依次调用既有 cluster→dispersion→match，独立核对首批/继承/断链身份逻辑与 SQL；不引入 OM reader、数据库或生产整链函数（依赖 T027）。
- [X] T029 [US2] 在 `validation/m2/verify_sql_and_history.py` 实现 `--scope synthetic|real|all`（synthetic 不要求 DuckOMO），执行 T021：核对函数全字段、`duckdb_vector_size()+1` 以上任务行、dispersion 成员跨边界、混合合法/非法完整消费失败、同会话恢复，逐类验证单批允许断链与元数据/非法输入/匹配错误禁止发布；验证历史约束、三个独立 PID 的完整结果、幂等/冲突、缺批和补插新版保留、各插入阶段故障显式回滚后五表差集为零（依赖 T027、T028）。
- [X] T030 [US2] 用 `validation/m2/build_extension.sh` 构建新产物，执行 `validation/m2/test_sql_history.py` 与 `verify_sql_and_history.py --scope synthetic`，比较 chain runner/SQL 全部节点、距离、边和身份并保存 `artifacts/m2/<run-id>/sql-history/`；CLI 即使有预期单批 stderr/非零退出也须按白名单逐项解释，未知错误不得通过（依赖 T029）。

**Checkpoint**: 合成历史回放可单独验收，正式表看不到失败发布的半条链；尚未证明真实 OM 路径。

## Phase 5: User Story 3 — 最新 DuckOMO 读取真实集合并完成追踪 (Priority: P1)

**Goal**: 真实 8 批源值无损转 OM，双版本读取一致，在同连接完成读取到历史保存。
**Independent Test**: 从 generation/哈希清单恢复输入，三路径逐值核对 3,600 个值，分别用两版 DuckOMO 完成真实回放并比较可比结果。

### Tests and sample preparation

- [X] T031 [P] [US3] 在 `validation/m2/test_om_sample.py` 定义源 generation/长度/hash、坐标/成员/时间不符、49 成员、重复/缺点、NaN/Inf、有损 PFOR、非法轴与损坏 OM 的反例，以及已核对缓存离线恢复、转换失败不发布清单、三路径位级比较器正反例（依赖 T005）。
- [X] T032 [US3] 在 `validation/m2/prepare_om_sample.py` 实现有限下载/缓存读取、校验 8 批源块和坐标、Blosc 解码与正确槽/lead 切片，输出 50×3×3 `<f4` 和 `om_sample_manifest.json`，记录身份/许可/获取时间；运行 OM venv 的独立解释器，禁止用普通 Python 环境的隐含包或提交真实数值（依赖 T004、T005、T031）。
- [X] T033 [US3] 在 `validation/m2/prepare_om_sample.py` 加入每批 FPX 无损 OM v3 写入与完整子属性、Float32 坐标/Int64 时间、显式 scale/offset/compression，并用独立 omfiles reader 读回检查编码、轴/属性和值字节；文件 hash 与 payload hash 分开，失败无成功清单（依赖 T032）。
- [X] T034 [P] [US3] 在 `validation/m2/sql/real_8run_replay.sql` 加入按清单生成的常量路径 `read_om` 调用、两扩展同连接加载、隔离扩展目录/关闭 autoload、release 身份检查；计算前验证 450 行/成员点键/坐标/单位/模型/特征版本/valid_time/init+lead，显式按 member_index/point_index 聚合后调用 US2 暂存/发布流程；脚本在无 Python 环境同样可执行（依赖 T027、T033）。
- [X] T035 [US3] 在 `validation/m2/verify_om_paths.py` 实现必填 `--stage cells`（不调用历史 verifier、不生成双版本完整回放结论）的源 Zarr 解码、omfiles reader 与两版 DuckOMO 的逐键/逐值/float32 精确加宽比较，记录共享 C codec 的独立性边界；完整执行 T031 负例，核对磁盘扩展 hash、加载版本及实际映射路径，保留位差反例证明比较器能拒绝错误（依赖 T034）。
- [X] T036 [US3] 在 `validation/m2/verify_sql_and_history.py` 实现 real/all 范围，仅消费已完成 `verify_om_paths --stage cells` 证据，运行真实同连接 8 批/7 比较、与源矩阵直接 core/chain 逐字段核对，独立重算 mean_features/成员数/占比；跨 3 个独立进程比对节点/边/距离/身份并验证所有输入/版本绑定一致（依赖 T030、T035）。
- [X] T037 [US3] 在 `validation/m2/verify_om_paths.py` 实现必填 `--stage replay` 的双版本完整回放对照，检查 cells 前置、不反向补跑或覆盖其产物并复用已验证的 SQL 回放入口：两版在独立进程/数据库加载，逐位比较矩阵、聚类、匹配与出生/继承关系；run_key 含 DuckOMO 身份时只归一由该身份派生的前缀/元数据，不能无差别删除 branch_id 或掩盖身份交换；写双版本证据（依赖 T036）。
- [X] T038 [US3] 执行 `validation/m2/test_om_sample.py`、`prepare_om_sample.py`、`verify_om_paths.py --stage cells` → `verify_sql_and_history.py --scope real` → `verify_om_paths.py --stage replay`，核验 SC-001/002/003 的 3,600 值零差异、8 批/7 对完整历史、双版本与三进程一致；将重建说明和当前证据身份写入 `artifacts/m2/<run-id>/om-verification/`，不沿用规划实验为通过依据（依赖 T031–T037）。

**Checkpoint**: US1+US2+US3 组成首个真实数据闭环；远程读取、速度收益与 M2 整体完成均未由此证明。

## Phase 6: User Story 4 — 案例解释、门槛/簇数敏感性与扰动 (Priority: P2)

**Goal**: 报告真实关系链、拒绝/竞争/歧义及对参数和微扰的敏感性，区分 M1 分区变化和 M2 关联变化。
**Independent Test**: 消费同一冻结样本与可执行回放，遍历 3×6 配置及两种扰动，对远离/接近边界用例独立核对余量和翻转。

- [X] T039 [P] [US4] 在 `validation/m2/test_stability.py` 定义稳定性证书正反例：门槛等号/最优切换/完全并列、无 same_count 备选或 q=0、成员重叠映射并列、簇数/分区变化、向外舍入保守误差界（q=0 仅检查可行图为空、无同条数替代用显式状态）和早期翻转导致后续身份变化；用有明确余量的合成场保证远离与接近两类均非空，不能用零样本宣称稳定（依赖 T006、T020）。
- [X] T040 [US4] 在 `validation/m2/run_case_and_sensitivity.py` 实现主回放与 k∈{2,3,4} × c∈{0.5,0.75,1,1.25,1.5,2} 的 18 个不可变版本，复用真实 SQL/历史流程，输出每对距离/离散/替代/未匹配/身份及成员占比；相应批次计算失败按清单规则记录并断链，不调参求成功（依赖 T038）。
- [X] T041 [US4] 在 `validation/m2/run_case_and_sensitivity.py` 加入只扰动两批情景均值的匹配级实验：固定 S/τ、100 个 PCG64 方向、加权 RMS 位移 ε*S；记录实际浮点位移、门槛与最优余量、所选边翻转，不把样本自身重新估计的尺度混入匹配级实验（依赖 T040）。
- [X] T042 [US4] 在 `validation/m2/run_case_and_sensitivity.py` 加入成员级 σ=0/0.001/0.01 K 实验，固定种子和遍历顺序，重跑 M1、离散及匹配；用成员最大重叠的全局一对一映射仅作扰动评估，映射歧义独立记录，不作为生产跨起报依据；报告成员移动数、均值位移、ΔS、边和身份变化（依赖 T041）。
- [X] T043 [US4] 在 `validation/m2/run_case_and_sensitivity.py` 实现冻结证书与报告生成，严格按 T039 处理 q=0/无替代、tied、映射歧义与浮点余量，按数值契约用精确有理表示及平方根界实现向外舍入区间，不能形成保守界时不得授予证书；保留所有接近边界翻转及归因，按出生节点键和整条上游继承关系检查逻辑身份稳定（局部 pair 稳定不等于上游已翻转的 branch_id 不变），输出 JSON 与 `case-study/report.md`（依赖 T042）。
- [X] T044 [US4] 执行 `validation/m2/test_stability.py` 和完整 `run_case_and_sensitivity.py`，验证 18 配置齐全、σ=0 复现、非空远离组零翻转、接近组每个翻转均有余量和层次归因，记录 M1 微扰敏感性而不修改其算法；保存 `artifacts/m2/<run-id>/case-study/`（依赖 T043）。

**Checkpoint**: 可解释案例与稳定性证据完整；报告只证明规则行为和局限，不声称校准、预报技巧或物理新生/消亡。

## Phase 7: User Story 5 — 独立复现、资源与阶段完成判定 (Priority: P2)

**Goal**: 通过隔离运行、全新源码复现、M1 回归和可执行证据支持 M2 完成结论。
**Independent Test**: 全新检出目录重建同一有效源码并复现固定子集；主运行独立生成追溯、报告和审计，失败/未执行不能冒充 passed。

- [X] T045 [P] [US5] 在 `validation/m2/test_evidence.py` 定义 report/traceability/refs 状态正反例、缺 FR/SC/孤立断言、错误 hash/越界路径、仅合成或仅读取无匹配、双版本/独立重建元数据归一越界（合法 hash 前缀变化可通过，伪造前缀/错误出生/继承/身份交换必须拒绝）、子集通过冒充整体、旧源码/遗漏 untracked 实现和失败发布被误接受；先有可失败的独立审计测试（依赖 T008）。
- [X] T046 [P] [US5] 在 `validation/m2/verify_minimal_runtime.sh` 复用 M1 固定基础镜像方法，以 `--network none` 运行无 Python/NumPy/sklearn/CDO/wgrib 的受控容器，仅复制 CLI/两扩展/必要库、本地 OM、生成 SQL 和 m2_runner 输入；分别跑脱离 DuckDB 的 core 与从 OM 到五表的完整 SQL、同连接错误恢复并在宿主核对全部结果，记录工具缺席及加载 hash（依赖 T038、T028）。
- [X] T047 [P] [US5] 在 `validation/m2/measure_pipeline.py` 测量已冻结真实 8 批的转换/读取/聚类+离散/匹配/身份保存五环节各≥3 次，转换单独进程且不计网络下载；SQL 前缀独立 PID 用 wait4 取 wall/RSS、已加载语句独立计时，报告输入/输出/线程/样本与产物 hash、整体 RSS 和不可分离项，禁止以前缀 RSS 差值冒充单环节峰值（依赖 T038、T033）。
- [X] T048 [P] [US5] 在 `validation/m2/run_m1_regression.py` 实现调用原 `validation/m1/run_m1_gate.py` 的完整回归：使用当前有效 M2 源码、外部恢复的 M0 样本、唯一 run-id，尊重现有脚本真实输出/target 行为（硬编码共享 target 必须串行）；不改原判定，把 report/traceability/audit 与实际必需证据复制到 M2 运行目录并重算 refs，不能只读取历史 passed（依赖 T024、T038，之后有相关源码变更须重跑）。
- [X] T049 [US5] 在 `validation/m2/reproduce_independently.py` 独立实现父/子模式和 self-check：子模式必填 `--reproduction-child --output-dir --source-manifest --sample-manifest --experiment-config --duckomo-target --duckomo-baseline`，外部样本/配置/build/cache/输出、干净新 clone、同有效源码清单；重建扩展并执行 core 合成、按 cells→real→replay 顺序执行真实三路径/双版本回放和 SQL 错误恢复，生成 `scope=reproduction_subset` 记录；父模式核对同有效源码和实际新构建 hash，重算各自 run_key，以出生节点键比较基线数值/端点/完整继承链，只归一合法版本前缀并保留子证据副本，子模式不得调用整体 gate、追溯、审计或递归复现（依赖 T020、T038，不依赖 T050–T052）。
- [X] T050 [US5] 在 `validation/m2/traceability.py` 实现 21 FR/8 SC 的双向可执行断言矩阵，独立重算距离/最优性/分区均值/身份/发布残留及证据 refs，覆盖 `contracts/validation-evidence.md` 全表且无孤立断言；为未执行 producer 产生 not_run，不从步骤 status 或期望关联数抄出 pass（依赖 T044、T046–T049）。
- [X] T051 [US5] 在 `validation/m2/verify_report.py` 与 `validation/m2/schema.py` 实现从步骤/断言推导 passed/failed/incomplete 的报告生成及只读独立审计，严格按 producers→traceability→report→audit，audit 重算 hash/路径/身份与总体状态但不回写 report；运行 T045 的全部坏证据自检（依赖 T050）。
- [X] T052 [US5] 在 `validation/m2/run_m2_gate.py` 实现 `--run-id [--cache-dir]` 全流程编排：环境/源码/冻结配置→样本→core→SQL合成→cells 阶段→真实 real/三进程→replay 双版本阶段→案例/扰动→隔离/测量→M1回归→独立复现父模式→追溯→报告→审计；拒绝覆盖目录、保留失败证据、缺前置标 not_run，只有三份总体记录及全部必要步骤通过才返回 0（依赖 T051）。

**Checkpoint**: 验收工具实现具备执行条件；完整 M2 判定仍须最后运行和审计通过。

## Phase 8: Polish & Cross-Cutting — 文档、全量门禁与交付

**Purpose**: 以最终有效源码完成全部验收并更新可复跑说明。

- [X] T053 [P] 根据已实现 CLI 在 `specs/003-m2-cross-batch-tracking/quickstart.md`、`contracts/validation-evidence.md` 和 `validation/m2/README.md` 校准参数、synthetic/real 范围、准备/双版本验证顺序、复现父/子参数及离线缓存行为；同步 `specs/002-m1-scenario-computation/data-model.md`、`contracts/core.md`、`contracts/sql.md`、`quickstart.md` 的明确状态/命名漂移，保持 M1 历史规格和验收结论原义，不改其算法/容差（依赖 T052）。
- [X] T054 运行格式/依赖检查、Rust 测试、M2 验证器自检和 `validation/m2/run_m2_gate.py --run-id <新ID>`，实际执行全部必要 producer（包括完整 M1 回归与独立 clone）；失败保留目录，修复后用新运行而非覆盖或事后放宽冻结参数，通过前不得勾选本任务；证据写 `artifacts/m2/<新ID>/`（依赖 T052、T053）。
- [X] T055 独立运行 `validation/m2/traceability.py` 与 `validation/m2/verify_report.py` 复核 T054 的最终目录，重算 21/21 FR、8/8 SC、全部引用 hash、M1 回归身份、新 clone 及发布失败残留断言，确认整体 passed/traceability pass/audit pass 且源清单未变，记录剩余平台/数据/稳定性限制（依赖 T054）。
- [X] T056 根据实际证据更新 `README.md`、`docs/ROADMAP.md`、`specs/003-m2-cross-batch-tracking/plan.md`、`spec.md` 和 `quickstart.md` 的状态、最终 run-id 与复跑入口，核对 `git diff` 无真实派生数据、旧证据被改写或未说明的源码变动；文档完成不代表自动发布或提交（依赖 T055）。

## Dependencies & Execution Order

### Phase and story dependency graph

```text
Phase 1 T001–T004
        ↓
Phase 2 T005–T008
        ↓
US1 T009–T020 (pure core MVP)
        ↓
US2 T021–T030 (synthetic SQL history)
        ↓
US3 T031–T038 (real OM / two releases)
        ↓
US4 T039–T044 (case / sensitivity / perturbation)
        ↓
US5 T045–T052 (isolation / measurements / regression / reproduction / evidence)
        ↓
Polish T053–T056 (final gate / audit / status)
```

该图表示**完成顺序**。准备/测试定义可按下表提前，并非所有文件工作都必须等待整个前一故事完成：

| 工作块 | 最早前置 | 必须顺序完成的集成点 |
| --- | --- | --- |
| US1 oracle 与 fixtures | Phase 2 | T011→T012→T013→T014→T015→T016→T017→T018/T019→T020 |
| US2 合成历史测试/DDL | US1 契约和 fixtures；T021 测试执行需 US1 | T022→T023→T024；T024+T025→T026→T027→T028→T029→T030 |
| US3 样本准备 | T004/T005，不需要 US2 已实现 | T031→T032→T033；T027+T033→T034→T035→T036→T037→T038 |
| US4 稳定性测试定义 | T006/T020 | T038→T040→T041→T042→T043→T044 |
| US5 审计测试定义 | T008 | T038 后可开展 T046/T047/T048/T049；T044 和这些 producer 完成后 T050→T051→T052 |
| 文档与最后验收 | T052 | T053→T054→T055→T056 |

M2 `build_extension.sh` 和 M1 原门禁可能使用不同或硬编码 target；**不得同时写同一 target**。修改了 core/adapter、锁文件、SQL 或判定脚本后，以新源码清单重跑受影响检查；完整门禁前确保 M1 回归不是改动前旧产物。

### Shared implementation boundaries

- 单批 `ens_cluster` 数值/签名保持不变；M2 只读取其实际 `mean_features` 与成员归属。新 error 位置字段必须让已有 M1 构造路径保持相同行为。
- 生产只做数值匹配；历史身份由 SQL 管理。开发 runner 的 chain 模式用于独立对照，不是第二套生产服务。
- CLI `.bail off` 只限声明的单批阶段；记录其预期错误后必须明确检查整体执行状态。不能因为后续 SELECT 成功就把之前失败的匹配/发布算作成功。
- `run_key` 绑定源内容/配置/软件身份，内容 hash 校验全部表行。跨 DuckOMO 版本可能得到不同 run_key；对照必须验证同一逻辑链而不能直接要求两份数据库字节或整个 branch_id 前缀相同。
- 禁止循环依赖：oracle 不读生产期望；独立复现子集不依赖 gate；traceability 不哈希 report；audit 不回写被它引用的 report。

## Parallel Execution Examples

以下是具备前置条件后的任务并行示例，不是启动指令；同一个源码目录保持单写者，独立 worktree 合并时重新检查接口。

| 故事 | 可以并行的任务 | 条件与禁止事项 |
| --- | --- | --- |
| US1 | T009 fixtures 与 T010 oracle 基础；T018 core verifier 与 T019 build wrapper | T010 的固定例集成需等 T009；T018/T019 均等 T017，不同时修改 Cargo/lib.rs |
| US2 | T021 测试定义与 T022 adapter；T025 schema 与 T023/T024 callbacks | T021 后才能定 T025 验收，schema 和 adapter 不同文件；T026/T027 同一 SQL 文件必须串行 |
| US3 | T031/样本准备工作与 US2 adapter/历史；T034 SQL 读取与三路径比较器草拟 | 正式 T034 等 T027/T033；T035/T037 同一 verifier 文件串行，双版本完整对照等 T036 |
| US4 | T039 稳定性测试与 US3 真实集成/US4 主回放 | 冻结规则来自 T006；T040–T043 写同一脚本且递进，保持串行 |
| US5 | T046 隔离、T047 测量、T049 复现实现 | 均消费 T038；实际构建/测量/回归串行安排，避免 target 竞争和 CPU 测量污染；T048 M1 回归不与同 target 构建并行 |

## Requirement Coverage

下表是任务生成阶段的覆盖检查，不是运行证据。正式断言所有权在 T050，覆盖目标仍为 21 FR / 8 SC 全部通过。

| Requirement | Implementation / validation tasks |
| --- | --- |
| FR-001 | T005–T006、T026–T030、T036、T040 |
| FR-002 | T005、T007、T031–T036 |
| FR-003 | T005、T011、T013–T014、T018、T035 |
| FR-004 | T006–T007、T013–T018、T023–T024 |
| FR-005 | T012、T016–T018、T024、T029、T036 |
| FR-006 | T009–T011、T015–T018、T024、T027、T029 |
| FR-007 | T021、T027–T030、T036、T043 |
| FR-008 | T007、T021、T026–T030、T040 |
| FR-009 | T007–T008、T027、T029–T030、T036–T037 |
| FR-010 | T021、T025–T030、T036 |
| FR-011 | T022–T024、T029–T030、T048 |
| FR-012 | T009、T011–T014、T018、T021–T024、T029–T031、T035 |
| FR-013 | T005、T031–T033、T038 |
| FR-014 | T003、T031–T033、T035、T038 |
| FR-015 | T003–T004、T034–T038 |
| FR-016 | T009–T011、T018、T021、T029、T031、T039 |
| FR-017 | T006、T039–T044 |
| FR-018 | T012–T020、T028、T034、T046 |
| FR-019 | T001–T004、T008、T019、T048–T049、T054 |
| FR-020 | T047、T054–T055 |
| FR-021 | T045、T049–T056 |
| SC-001 | T031–T035、T038 |
| SC-002 | T026–T030、T034、T036、T038 |
| SC-003 | T029–T030、T036–T038 |
| SC-004 | T009–T020、T029–T030 |
| SC-005 | T011、T018、T021–T024、T029–T031、T035、T038 |
| SC-006 | T039–T044 |
| SC-007 | T046 |
| SC-008 | T045、T047–T056 |

## Implementation Strategy

### MVP First

1. 完成 Setup 和 Foundational；冻结规则及接口细节后写 US1 契约测试。
2. 完成 US1 的原生匹配、离散、oracle 和 runner，独立核对。这是最小算法交付，不要求有 WB2、OM 或历史数据库。
3. 不为赶进度把算法 MVP 记为 M2 完成；下一增量是 US2 合成历史，之后是 US3 真实数据闭环。

### Incremental Delivery

1. US1：可解释两批匹配，带距离、拒绝、歧义和替代诊断。
2. US2：明确持续身份，缺批断链与事务历史可独立复跑。
3. US3：真实集合转 OM 后经两版 DuckOMO 完成同连接回放；这是首个真实研究闭环。
4. US4：给出真实案例的参数/扰动敏感性和归因，不以修改 M1 隐藏负面结果。
5. US5 与 Polish：隔离、资源、M1 回归、新 clone、全需求追溯及审计实际通过后，才更新 M2 整体状态。

所有阶段都保留失败证据、明确尚未执行的检查；更改冻结规则须版本化并重新运行，不覆盖旧报告。