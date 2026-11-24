# M2 技术研究与决策

日期：2026-10-29。范围为技术规划（Phase 0），不是 M2 实现或正式验收。三个 worker 在隔离临时目录中分别完成了 OM 转换与 DuckOMO 联调、匹配算法原型与真实样本距离分布、历史保存 DDL 与验证框架的实验。主代理复核了实验脚本、输出产物及仓库源码，整理出以下技术决定。项目仓库在调查期间只读，尚未编写生产实现或运行正式 M2 门禁。

## 1. 真实样本转 OM 与 DuckOMO 联调（FR-013–FR-015）

### D1：样本准备工具与编码

- **Decision**：样本准备使用独立 Python 环境中的 `omfiles==1.2.0`（wheel SHA-256 `bb40290c7661978490ae43ec156ab0a14ccb2311f81d74ff7c5067c04094195e`），单独写入锁文件 `validation/m2/requirements-om.lock`，不进入 Rust workspace、core 或 DuckDB 扩展。值数组采用 Float32 + `fpx_xor_2d` 无损编码，`scale_factor=1.0`、`add_offset=0.0`，chunk 取完整形状 `[50,3,3]`；附属浮点坐标属性同样用 Float32 `fpx_xor_2d`，整数属性用 Int64 `pfor_delta_2d`，不写入任何 Float64 数组。
- **Rationale**：
  - DuckOMO 值数组仅支持 Float32 的 `FPX_XOR2D` 或 `PFOR_DELTA2D_INT16`（`duckomo/src/om/metadata.cpp:100-104`、`reader.cpp:376-384`）。实测 `pfor_delta_2d_int16`（scale 100）在 450 个值中改变了 448 个，最大误差 `0.004974 K`，且 DuckOMO 读取时不报错；omfiles 默认的 `pfor_delta_2d` 用于浮点会把 `42.1875` 截成 `42.0`。因此只能用显式指定的 `fpx_xor_2d` 并在样本准备阶段用独立 reader 断言 `compression_name == "fpx_xor_2d"` 及读回字节相等。
  - `omfiles 1.2.0` 内置 `om-file-format` C 核心提交为 `18bee85`，早于上游修复双精度 XOR 指针宽度的 `dbeaa23`（2026-06-05）；避免写 Float64 数组可彻底绕开该缺陷。
  - `omfiles` 许可为 `GPL-2.0-only`。仅在样本准备子进程中调用、不链接、不随仓库分发，保持 `ensabc` 仓库的 MIT 边界清晰。
- **Alternatives considered**：
  - `rust-omfiles`：未发布到 crates.io（API 返回 404），只能用 git 依赖，且会把 GPL-2.0 引入 Rust workspace，拒绝。
  - 自写 C/FFI OM writer：增加维护面，无必要。
  - `PFOR_DELTA2D_INT16`：有损，且可能改变距离门槛边界判定，违反 FR-014。

### D2：OM 文件布局与内部元数据

- **Decision**：每批一个 OM 文件（共 8 个文件，每个 2,120–2,160 B），根 group 下放置三维 Float32 数组 `/temperature_2m`，维度顺序 `[member=50, lat=3, lon=3]`。在 `/temperature_2m` 下写入子属性：`coordinates="member lat lon"`（String）、`valid_time=1578182400`（Int64 标量，2020-01-05 00 UTC Unix 秒），以及供独立核对的 `latitude`（Float32[3]）、`longitude`（Float32[3]）、`member_label`（Int64[50]，值 `1..50`）、`run_unix_seconds`（Int64[1]）、`lead_seconds`（Int64[1]）、`units="K"`、`source_variable="2m_temperature"`。
- **Rationale**：
  - 与 WB2 的 8 个源块一一对应；FR-008 的缺批/失败批可直接按单批文件状态表达，FR-009 插入或替换批次不必重写其他批次。
  - DuckOMO 只解析 `coordinates`、`valid_time`、`time`、`crs_wkt`，忽略值数组下的其他子项；若在根 group 放非 Float32 数组会被 DuckOMO 当成非法变量拒绝（`metadata.cpp:236-240`）。
  - DuckOMO 不从文件猜测 `run` 或 `lead_time`，也不支持向不存在的 `run` 轴广播标量（实测传 `axes.run` 会报 `Binder Error: axes.run references unknown logical axis 'run'`），因此 `run` 与 `lead_hours` 由样本清单注入 SQL 并做恒等式检查。
- **Alternatives considered**：
  - 含 `run` 轴的单文件 `[8,50,3,3]`：实测可读且数值一致，但缺批/补批需重写整个文件，`run` 标签仍靠 SQL 注入，单点出错影响全部 8 批。

### D3：三路径逐值对照与双版本 DuckOMO 联调

- **Decision**：目标版本固定为 DuckOMO `v0.3.5-rc.1`（`duckomo-v0.3.5-rc.1-duckdb-v1.5.5-linux_arm64.zip`），对照版本为 `v0.3.4`（同平台包），宿主固定为 DuckDB CLI `v1.5.5`（`d8cdaa33fd`）。加载前必须离线核对 ZIP、根 `manifest.json`、`SHA256SUMS` 和解压后 `duckomo.duckdb_extension` 的 SHA-256，并在 SQL 会话开头设置 `SET extension_directory='<隔离空目录>'`、`SET autoinstall_known_extensions=false`、`SET autoload_known_extensions=false`，加载后核对 `duckdb_extensions()` 的 `extension_version`（`55d66f3` vs `c3084c6`）及进程 `/proc/self/maps` 中的实际映射路径。
- **Rationale**：
  - 规划实验发现本机 `~/.duckdb/extensions/v1.5.5/linux_arm64/duckomo.duckdb_extension` 已存在一个非发布构建（SHA-256 `7475609c…`）。不隔离 `extension_directory` 会造成元数据混淆或误加载本地文件，违反 FR-015。
  - 规划实验已实测：8 批 3,600 个值在 WB2 源解码、`omfiles` 独立 reader、DuckOMO `v0.3.5-rc.1` 和 `v0.3.4` 之间 **0 位差异**；同一连接内 `LOAD duckomo` + `LOAD ensabc_m0_extension` 后执行 8 批 `read_om → ens_cluster` 成功，两版本各跑 3 个独立进程，输出的矩阵与聚类结果逐字节相同。
  - 独立性说明：`omfiles` 与 DuckOMO 共用 Open-Meteo `om-file-format` C 编解码实现（修订 `18bee85` 与 `d8855e4`），真正独立的是源 Zarr Blosc 解码；报告须如实记录这一边界。
- **Alternatives considered**：
  - 升级到 DuckDB 2.0：需要重验 `ensabc` 自身 ABI、Python 绑定和 M1 全部门禁，且异步读取不在本阶段范围。

| 项目 | `v0.3.5-rc.1`（目标） | `v0.3.4`（对照） |
| --- | --- | --- |
| 发布 ZIP SHA-256 | `cee19b262b49fe501a69787149cec4f26cfbaa2899c137e17f0b6834dd099890` | `fd123e090a31ba69219eed4e613e56947ba503d127cc38c3d444aadfb929f713` |
| 根 `manifest.json` SHA-256 | `d7b36a255faa4ab02d21f01cb3384ea307ee4e20823bf456aa7a358dcabb8e6b` | `6a0c921b35f24fba599ad12623558a13cffebad1c72669c44da4f433f6315c54` |
| 解压后 `duckomo.duckdb_extension` SHA-256 | `586bc49de1da86c26b9efd46ff1adc07dca8e206967793dad88e20dd53f3047b` | `554e36f68fade713c19a94ac4db3788933c95c22db6ef288da047e2f3e620b80` |
| `source_commit` / `extension_version` | `55d66f3fbd3199d7471b8cd569c43cc91a8cec7e` / `55d66f3` | `c3084c6937d7d4de02a4edb7aaa68cdb5086f7f6` / `c3084c6` |

## 2. 距离、集合离散与两批匹配算法（FR-003–FR-006）

### D4：空间加权 RMSE 与集合离散的精确公式

- **Decision**：所有跨格点或跨成员求和均使用 Shewchuk 非重叠浮点展开（与 CPython `math.fsum` 同算法，精确累加后 round-half-even 舍入一次，记为 `fsum`）。
  - 空间权重：版本 `cos_lat_center_v1`，对 3×3 欧洲区域按纬度优先、经度次序固定为 9 个常数（42.1875°N: `0.7409511253549591` / `0x1.7b5df226aafafp-1`；47.8125°N: `0.6715589548470183` / `0x1.57d69348cec9fp-1`；53.4375°N: `0.5956993044924334` / `0x1.30ff7fce17035p-1`，每个纬度重复 3 次），写入 fixture，匹配计算直接接收权重数组。
  - 情景距离：$W = \operatorname{fsum}_g(w_g)$，$\delta_g = a_g \ominus b_g$，$d(a,b) = \sqrt{\operatorname{fsum}_g(w_g \otimes (\delta_g \otimes \delta_g)) \oslash W}$。
  - 单批集合离散（总体口径 $1/N$）：对每格点先算集合平均 $m_g = \operatorname{fsum}_n(x_{n,g}) \oslash N$，偏差 $e_{n,g} = x_{n,g} \ominus m_g$，离散 $s = \sqrt{(\operatorname{fsum}_{n,g}(w_g \otimes (e_{n,g} \otimes e_{n,g})) \oslash N) \oslash W}$。
  - 相邻批次尺度与门槛：$S = (s_{\text{prev}} \oplus s_{\text{curr}}) \oslash 2$，$\tau = c \otimes S$。可行条件严格以 $d \le \tau$（绝对值，含等号）为准；展示比值 $r = d \oslash S$ 不反向覆盖可行性判断。
- **Rationale**：
  - 等角经纬网格上，格元球面面积 $\propto \sin(\phi+\Delta/2)-\sin(\phi-\Delta/2) = 2\cos\phi\sin(\Delta/2)$，故中心纬度余弦与真实格元面积严格成正比；冻结位值可消除不同平台 `libm::cos` 的舍入风险。
  - `fsum` 使求和结果与成员顺序、格点归约顺序和情景编号无关。规划实测中，真实 8 批 21 对距离与离散在 Rust 原型和 Python `math.fsum` 间逐位一致（0 差异）。
  - 规划实验的冻结缩放样本满足相应数值口径；这不是任意输入的普遍保证。修订后只对合法、无相关溢出/下溢且满足精确条件的 2 的幂用例要求逐位一致；一般常数用例检验 rtol=1e-15，决策一致还需余量覆盖误差，边界舍入翻转单独记录。
  - 若 $s \le 0$ 或非有限（例如所有成员完全相同），明确报 `ZERO_DISPERSION` 或 `NUMERICAL_FAILURE`，不退回固定门槛。
- **Alternatives considered**：
  - 朴素顺序累加或 Kahan 求和：在接近并列或不同排列下可能产生 1 ulp 差异，导致并列判定依赖顺序。
  - 样本口径 $1/(N-1)$：与 spec“全体成员相对集合平均场的均方根偏差”字面定义不符。

### D5：门槛内字典序最优匹配、完全并列与规范决胜

- **Decision**：在 `ensabc-core` 中自研“后批位掩码子集动态规划（bitmask DP） + Shewchuk 展开精确比较”，不引入第三方指派库。单侧情景数支持范围为 `1..=12`，超过 12 报 `TOO_MANY_SCENARIOS`。
  - **优化目标**：仅在可行边集 $E = \{(i,j) : d_{i,j} \le \tau\}$ 上求一对一部分匹配 $M$，先最大化 $|M|$，再最小化 $\sum_{(i,j)\in M} d_{i,j}$ 的**精确实数和**（由各边 `f64` 距离位值的精确展开表示）。
  - **完全并列（歧义）**：存在另一可行匹配 $M' \ne M^*$ 使 $|M'| = |M^*|$ 且精确距离和相等（`optimum_count > 1`）。
  - **与编号无关的规范决胜**：两侧情景各按 `mean_features` 的规范键（先把 `-0.0` 归一为 `+0.0`，再按 `f64::total_cmp` 字典序）升序排列；仅当两个情景的物理平均场逐位完全相同时才按本批 `cluster_id` 决胜。在所有最优匹配中，按前批规范序考察其匹配的“后批规范序号（未关联记为 $n$）”构成的伙伴向量，取字典序最小者。
  - **可替代程度（FR-006）**：对每条选中关联 $e \in M^*$，在 $E \setminus \{e\}$ 上按同一字典序重求最优 $(q', \Sigma')$：
    1. `tied`：$q' = q$ 且 $\Sigma' = \Sigma$（精确相等），此时该关联必属于歧义（`ambiguous = true`）；
    2. `same_count`：$q' = q$ 且 $\Sigma' > \Sigma$，报告精确差正确舍入后的 `distance_increase = round(\Sigma' - \Sigma)` 及其与 $S$ 的比值；
    3. `count_drop`：$q' < q$（必为 $q-1$，当 $q'=0$ 时 $\Sigma'=0$），报告 `edge_count_loss = 1` 与次优距离和，`distance_increase` 为空（SQL 中为 `0.0` 并由状态区分，见 SQL 契约）。
  - **未关联原因（FR-005）**：若该情景在 $E$ 中度为 0，记为 `no_candidate_within_threshold`；若度 $>0$ 但未进入 $M^*$，记为 `not_selected_in_global_optimum`，并附 `matched_in_some_optimum: bool`。
- **Rationale**：
  - 评估了 `pathfinding 4.16.0`、`lsap`、`lapjv` 等现有 crate：`pathfinding::kuhn_munkres` 要求整数/全序权重、行数大于列数会 panic、只做全指派、不提供禁止边或并列计数；big-M 与整数量化会改变距离语义。
  - $k \le 12$ 时位掩码 DP 单次完整报告（含每条边的可替代程度重求）在 $k=4$ 时仅需约 `30 µs`，$k=12$ 约 `89 ms`，代码约 200 行且零外部依赖。
  - 规划期用 Python `fractions.Fraction` 编写独立穷举参考（不共享 DP 代码），在 8 个固定边界用例 + 4,000 个随机用例上与 Rust 原型逐字段比对，**4,008 / 4,008 完全一致（0 差异）**；2,000 次随机编号置换不变性测试全部通过。
  - 规划实验同时发现：`serde_json` 默认浮点解析在随机 17 位小数上有约 11.3% 的 1 ulp 偏差；开启 `serde_json` 的 `float_roundtrip` feature 后降为 0。因此 M2 的 JSON runner/验证必须开启 `float_roundtrip`（SQL 入口直接传二进制 `DOUBLE`，不受文本解析影响）。

## 3. 真实 8 批距离分布与冻结标定参数（FR-004、FR-017）

### D6：门槛比例系数、扰动幅度与远离/接近边界判定规则

- **Decision**：基于规划期对 WB2 8 批样本的实际 `ens_cluster` 计算结果，确定以下参数并在 `validation/m2/fixtures/` 中冻结（验收运行前不再调整）：
  - **主门槛比例系数**：`c = 1.0`（配置版本 `m2.match.v1`）。
  - **门槛比例系数敏感性范围**：`c ∈ {0.5, 0.75, 1.0, 1.25, 1.5, 2.0}`（共 6 个取值）。
  - **情景数敏感性范围**：主结果为每批 `k = 3`，敏感性对照为每批 `k = 2` 与 `k = 4`（共 3 种设定，均在 $c=1.0$ 及上述系数范围下完整回放并保存为独立回放版本）。
  - **匹配级轻微扰动**：保持各批情景与尺度 $S$ 不变，对每个情景的物理平均场施加加权 RMS 范数为 $\varepsilon \cdot S$ 的随机方向扰动，冻结 $\varepsilon = 0.01$，每对批次 100 个固定随机种子（PCG64/ChaCha8 固定种子表）。
  - **成员级完整流程轻微扰动**：对每批 50×9 原始成员温度场的每个值加独立高斯扰动 $\mathcal{N}(0, \sigma^2)$，冻结主幅度 $\sigma = 0.001\text{ K}$（20 个固定种子），另附 $\sigma = 0.01\text{ K}$ 作为压力对照档记录。
  - **远离边界 vs 接近边界的判定规则（三角不等式证书）**：令 $\varepsilon_{\text{abs}}$ 为受扰后两批各情景平均场相对基线的最大加权 RMS 位移（成员级先按成员最大重叠建立与基线情景的一一对应；若无法建立无歧义一一对应则直接归入“情景层分区变化”），$\Delta S = |S' - S|$。当且仅当同时满足：
    1. **门槛余量**：$\min_{i,j} |d_{i,j} - \tau| > 2\varepsilon_{\text{abs}} + c\Delta S$；
    2. **同条数最优余量**：对所有 `same_count` 选中边，$\min_e \text{gap}_e > 4 q \varepsilon_{\text{abs}}$；
    3. **无并列**：`optimum_count == 1`；
    在保守浮点误差界下仍满足时才判为**远离边界**（约束当前 pair 映射后的关联，整链逻辑身份还需出生及全部上游继承一致）；否则判为**接近边界**（允许翻转，但必须 100% 记录翻转及上述两个余量值）。
- **Rationale**：
  - 实测 WB2 8 批的集合离散 $s$ 随预报时效缩短单调下降：从 96h 的 `0.6727 K` 降至 12h 的 `0.1763 K`（差 3.8 倍），印证了固定 K 门槛在长短时效间松紧不一、而相对离散门槛可自动适应时效尺度。
  - 在 $k=3, c=1.0$ 下，7 对相邻批次的关联条数依次为 `[2, 3, 2, 2, 3, 2, 2]`（共 16/21 条关联，产生 8 个 `branch_id`），同时覆盖了延续、因超出门槛未关联（`no_candidate_within_threshold`）、因全局竞争未关联（`not_selected_in_global_optimum`）以及 `3→4` 对上的接近并列竞争（gap 仅 `0.018 K` / `0.037 S`）。
  - 实测 $\sigma = 0.001\text{ K}$ 下，M1 在第 7 批（12h）有 3/20 次发生跨 >2 个成员的分区切换；分别报告匹配级扰动与成员级扰动，并用三角不等式证书划分边界，能够严格区分“M1 单批次聚类对微扰敏感”与“M2 匹配器在边界翻转”。规划实测中，满足远离边界证书的 30,100 个匹配级样本和 9,226 个成员级样本**翻转数全部为 0**。

## 4. SQL 接口、持续身份分配与历史保存（FR-007–FR-012、FR-018）

### D7：SQL 扩展入口与最小运行环境编排

- **Decision**：在现有 `crates/ensabc-duckdb`（产物名仍为 `ensabc_m0_extension.duckdb_extension`，保留 `ens_m0_echo` 与 `ens_cluster` 不变）中新增两个无状态标量函数：
  1. `ens_dispersion(members STRUCT(member_id VARCHAR, features DOUBLE[])[], weights DOUBLE[]) -> DOUBLE`：调用 core 计算单批集合离散 $s$；
  2. `ens_match_scenarios(prev_scenarios STRUCT(cluster_id BIGINT, mean_features DOUBLE[])[], curr_scenarios STRUCT(cluster_id BIGINT, mean_features DOUBLE[])[], weights DOUBLE[], config STRUCT(threshold_coefficient DOUBLE, prev_dispersion DOUBLE, curr_dispersion DOUBLE)) -> STRUCT(...)`：调用 core 校验参数、由 $(c, s_{\text{prev}}, s_{\text{curr}})$ 换算 $S$ 与 $\tau$，并执行两批匹配，返回完整距离矩阵、可行矩阵、选中关联、未关联项、目标值、最优解计数与余量诊断。
  - core 同时暴露直接接收绝对门槛 $\tau$ 的 `match_scenarios(&MatchInput)` 与接收两批离散 + 比例系数的 `match_scenarios_scaled(&ScaledMatchInput)`，使合成用例既能直接指定绝对 $\tau$（如 `[[0,4],[4,6]]`, $\tau=5$），也能测试尺度换算。
  - **持续身份分配与历史表写入**采用**纯 DuckDB SQL（递归 CTE + 显式事务脚本）**实现，不在标量函数内部查表或生成 `branch_id`。
- **Rationale**：
  - 严格满足 FR-011（匹配入口每行显式接收前后两批情景、权重和配置，不查历史、不分配身份）与 FR-018（断网且无 Python/参考工具的最小容器中，仅靠 DuckDB CLI + DuckOMO + 本项目扩展即可从本地 OM 文件跑完“读取 → 聚类 → 离散 → 匹配 → 身份分配 → 事务保存”全流程）。
  - 规划实验已在 DuckDB 1.5.5 CLI 中实测了递归 CTE 身份链推导与事务发布，8 批规模耗时在毫秒级。
- **Alternatives considered**：
  - 用 Python 驱动分配 `branch_id` 再 INSERT：在无 Python 的最小运行环境（FR-018 / SC-007）中无法独立完成整条回放，拒绝。
  - 在 Rust 中再写一个整链编排表函数：把批次状态与表结构硬编码进数值扩展，不如 SQL 透明易查。

### D8：单批失败捕获、不可变回放版本与原子发布

- **Decision**：
  - **单批计算失败的 SQL 捕获**：DuckDB 1.5.5 实测确认内置 `TRY(expr)` 不能捕获标量扩展通过 `duckdb_function_set_error` 抛出的运行期错误。因此回放分为两个阶段：
    1. **暂存阶段（autocommit，CLI `.bail off`）**：先创建 `TEMP` 表并插入计划批次清单（默认状态为 `missing` 或 `failed`）；再对每个可用批次单独执行一条 `INSERT INTO temp_batch_cluster SELECT ... ens_cluster(...)` 与 `ens_dispersion(...)`。若某批因秩不足或零离散报错，仅该条语句失败，其余批次正常写入，随后用 `UPDATE` 将未产生结果的可用批次标记为 `status='failed'`（原因 `SINGLE_BATCH_COMPUTATION_FAILED`）。
    2. **匹配与发布阶段**：单批容错阶段后先核对错误白名单，仅对相邻且两侧均 ok 的批次执行匹配暂存；任一匹配错误或应有比较缺失阻止进入发布。全部暂存完整后在单个 `BEGIN ... COMMIT` 内发布五表，错误显式 ROLLBACK；CLI 退出场景检查非零状态和实际零残留。元数据/非法输入/身份核验/未知错误不得当作可发布的单批 failed。
  - **五张正式历史表**：`tracking_runs`、`replay_batches`、`scenario_nodes`、`pair_comparisons`、`scenario_links`，全部以 `run_key`（绑定内容的 SHA-256）为分区主键或外键。
  - **确定性 `branch_id` 编码**：格式为 `<run_key前12位>:b<出生ordinal两位>:k<出生规范序两位>`（在单次 run 展示中也可简写为 `b00.k0`），其中出生规范序为该情景在出生批次内按 `mean_features` 规范键排序的零基序号（与 D5 决胜序一致）。这样即使把某批的本地 `cluster_id` 人工重排，生成的 `branch_id` 也保持不变。
  - **同版本幂等与冲突拒绝**：`tracking_runs` 额外保存 `content_sha256`（拟发布的全部子表行按主键规范排序后的 JSON 摘要）。在 `BEGIN TRANSACTION` 内先执行：
    ```sql
    SELECT CASE WHEN EXISTS (
      SELECT 1 FROM tracking_runs
      WHERE run_key = getvariable('run_key') AND content_sha256 <> getvariable('content_sha256')
    ) THEN error('RUN_VERSION_CONFLICT: run_key=' || getvariable('run_key')) END;
    ```
    若 `run_key` 已存在且 `content_sha256` 相同，则后续五张表的 `INSERT ... WHERE NOT already_published` 均跳过并正常 `COMMIT`（幂等成功）；若 `content_sha256` 不同，则 `error()` 触发事务中止，回滚后原记录完好无损。发布事务末尾再执行 4 条不动点守卫查询（节点数一致、同一 `branch_id` 的批次序号连续无断链复活、相邻 `ok` 批次必有比较记录、每条关联两端 `branch_id` 相同），任一违反即 `error()` 中止事务。
- **Rationale**：
  - 规划实验在 DuckDB 1.5.5 上完整实测了“正常发布 → 同内容重跑幂等 → 篡改 1e-9 后同 `run_key` 冲突报错回滚 → 注入中途故障回滚 0 残留 → 同连接恢复发布新版本”，全部符合 FR-009、FR-010、FR-012 与 SC-005。
  - DuckDB 1.5.5 在非交互脚本中遇到事务内错误后，后续直到 `COMMIT` 的语句会报 `Current transaction is aborted (please ROLLBACK)` 并自动回滚；驱动脚本在捕获发布错误后显式发送 `ROLLBACK;` 即可在同一连接继续执行后续合法回放。

### D8.1：分析后修订（I1–I5/U1–U2）

2026-10-29 用户确认的修订：离散对全部平方项一次精确累加；绝对 MatchInput 仅保留前后场、权重和 threshold，比例模式参数完整必填；已分类且输入合法的单批算法失败允许断链，其余准备/匹配/发布错误阻止发布；同构建精确重复与新 clone 重建语义复现分别验收，保留实际二进制身份并按出生节点键核对完整继承；权重缩放和扰动证书均限制到合法数值范围并考虑舍入误差；三路径/双版本阶段固定为 cells→real→replay。具体规则以更新后的 numerics/core/sql-history/validation-evidence 契约为准。规划观察值仍是历史实验，不代替正式门禁。

## 5. M1 文档漂移同步与 M2 验证框架（FR-019–FR-021）

### D9：M1 文档与实现的已知漂移处理

- **Decision**：不动 M1 已验收源码、不改 `artifacts/m1/m1-gate-final` 历史证据；在 M2 的契约与代码中以 M1 **实际源码接口**为准，并在 `specs/003-m2-cross-batch-tracking/data-model.md` 中列明映射关系：
  - M1 Rust 类型实际为 `ClusterScenario` / `ScenarioDiagnostics` / `CoreErrorCode`（M1 文档写为 `ScenarioSummary` / `Diagnostics` / `ErrorCode`），且 `MemberInput` 含 `member_index: usize`；
  - M1 core `ClusterScenario` 的簇内误差字段名为 `cluster_sse`，SQL `ens_cluster` 输出结构体中映射为 `within_cluster_sse`；
  - M1 实际返回的 `diagnostics.distance_space` 字符串为 `"projected_scores_v1"`。
  - M1 原有门禁 `validation/m1/run_m1_gate.py` 必须在 M2 完成后的代码基线上完整重跑一次（写入新的 `artifacts/m1/<run-id>` 或 `artifacts/m2/<run-id>/m1-regression/`），证明新增 M2 代码未破坏 M1（规划期在临时克隆上实测重跑 M1 全门禁耗时 `2m 11.8s`，峰值 RSS `771.9 MiB`，17/17 步骤通过）。

### D10：资源测量（FR-020）与验证编排（FR-021）

- **Decision**：
  - 新增 `validation/m2/`，沿用 M1 的“producers → traceability → report → 无循环独立 audit”架构，覆盖 FR-001–FR-021（21 项）与 SC-001–SC-008（8 项）。
  - **FR-020 资源测量**：对真实 8 批回放的 5 个环节（① WB2 载荷转 OM、② DuckOMO `read_om` 读取与准备门禁、③ 8 批 `ens_cluster` + `ens_dispersion`、④ 7 对 `ens_match_scenarios`、⑤ 身份递归分配与五表事务保存）分别测量墙钟时间与峰值内存，每项至少 3 次独立运行。
    - 冷启动：用累积阶段前缀的独立 DuckDB CLI 子进程测量（阶段 prefix 1..5），采集各子进程 `wait4.ru_maxrss`；
    - 已加载：在同一进程内先 `LOAD` 两个扩展，再用 DuckDB `.timer on` / Python 高精度计时器分别记录②③④⑤各阶段 SQL 语句耗时；由于 OS 进程级 `ru_maxrss` 是单调不减的进程生命周期峰值，同一连接内各阶段的独立增量 RSS 属于不可分离项，同时采集 `pragma_database_size()` / `duckdb_memory()` 快照，并将单阶段独占 OS 峰值 RSS 明确标为 `measured_by_cumulative_subprocess_prefix`。
- **Rationale**：诚实区分可分离的语句墙钟时间与不可在单进程内分离的 OS 峰值 RSS，完全符合 FR-020“无法分离的成本明确标记”的要求。
