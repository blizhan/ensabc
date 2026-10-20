# M0 技术研究与 spike 决策

日期：2026-10-20。三个 Luna max worker 分别进行数据提取、Rust 扩展和数值参考实验。本文件区分设计决策、实际观察与尚未满足的正式验收，研究完成不等于 M0 通过。

## 1. 固定输入优先于跨提供商对照

**Decision**：算法比较消费同一份冻结的成员优先矩阵。数据资格以真实区域、单起报/有效时刻、近地面气温、完整产品成员、共同网格和可恢复性为准；core 不绑定来源格式。

**Rationale**：提供商即使使用同名模式，也可能有不同起报、网格选择、插值或海拔订正。若分别取数后直接比算法，不能定位误差来源。

**Alternatives considered**：不将 Open-Meteo 点位 API 当区域场；不要求公开 OM 分发必须满足 M0；不开发通用 Icechunk/GRIB 适配器。Dynamical 和 WeatherBench2 作为具有显式集合轴的候选，实际提取证据由 data spike 决定。

前置信息及来源见 [M0-RESEARCH.md](../../docs/M0-RESEARCH.md#7-补充讨论原生-rust-与开放集合数据)。特别注意：Open-Meteo 公开 OM 分发未包含 ensemble，不代表 OM 格式或 Ensemble API 不能表达逐成员值。

**Selected sample**：WeatherBench2 `ifs_ens/2018-2022-64x32_equiangular_conservative.zarr`，`2m_temperature`，起报 2020-01-01 00:00 UTC，时效 24 小时，有效时刻 2020-01-02 00:00 UTC。选用该归档产品的全部 50 个成员（number 1–50），不能据此宣称还包含控制成员。实际纬度为 42.1875、47.8125、53.4375，经度为 5.625、11.25、16.875；按成员、纬度、经度顺序形成 50×9 矩阵，单位保留 K，不插值。

**Observed**：来源对象的 payload 合计 1,071,163 字节（不是网络总流量统计）；矩阵 float32 little-endian 字节哈希为 `09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe`。主会话重跑本地恢复检查通过。来源 chunk 自身覆盖比最终区域更大的空间/起报范围，但只保留小区域矩阵，不下载整套归档。

CSV 使用能 round-trip float32 的十进制表示。正式标准化前必须先还原源 float32，再准确提升为 float64；不能直接把 CSV 小数作为源 float64 值。主会话按该路径运行一次参考函数，得到有效秩 9、簇大小 14/20/16、惯性 87.09307906385827，证明样本可支持当前算法条件；这不是三进程真实基线验收。

样本清单、具体源对象 generation、提取工具和使用条件见 [data findings](spikes/data/findings.md) 与 [sample.json](spikes/data/sample.json)。正式验收仍须按数据模型补齐规范化清单、离线恢复和报告。

**使用边界（含 `snv/deepseek-v4.1-flash` 补充核查）**：
- WeatherBench2 指南（`https://weatherbench2.readthedocs.io/en/latest/data-guide.html#ifs-ens`）原文声明 `All data from the TIGGE archive can only be used for research purposes. Please check the license for more specific constraints.`，且对应 GCS 路径下的 `LICENSE` 探测返回 404；旧版 TIGGE 条款（`https://apps.ecmwf.int/datasets/licences/tigge/`）写明 ECMWF 贡献为 CC BY-NC 4.0、非 ECMWF 数据不得向组织外第三方提供，而新版 ECDS TIGGE（`https://ecds.ecmwf.int/licences/tigge-licence`）将 ECMWF 列为 CC BY 4.0。由于 WeatherBench2 再加工对象的具体许可证文件缺失，本地研究保存与回源按 GCS generation 恢复可行，但随仓库公开再分发许可链未闭合。
- Dynamical STAC 元数据（`https://stac.dynamical.org/ecmwf-ifs-ens-forecast-15-day-0-25-degree/collection.json` 及 GEFS 对应 collection）明确声明 `"license": "CC-BY-4.0"`（IFS 另附 ECMWF Terms of Use 署名要求）。若正式 M0 需要随仓库直接分发冻结样本且无法闭合 WB2 许可文件，可在实施阶段切换为 Dynamical CC BY 4.0 切片或采用不提交派生数值、仅提交清单+回源 generation 校验的恢复模式。

## 2. 显式参考配置

**Decision**：在独立 Python 3.13.13 环境使用 NumPy 2.5.4、SciPy 1.18.1、scikit-learn 1.9.1。完整依赖见 [requirements-lock.txt](spikes/baseline/requirements-lock.txt)。计算使用 float64，数值线程限制为 1。

| 步骤 | 固定配置 |
| --- | --- |
| StandardScaler | `with_mean=True, with_std=True`，常量特征保留 |
| PCA | `n_components=2, svd_solver="full", whiten=False` |
| KMeans | `n_clusters=3, init="k-means++", n_init=10, random_state=0` |
| 迭代 | `algorithm="lloyd", max_iter=300, tol=1e-4` |
| 有效性 | 有限矩阵、唯一成员身份、可区分向量数 >=3、标准化后有效秩 >=2 |
| 比较 | `atol=1e-8, rtol=1e-6`，消除簇编号和 PCA 符号差异 |

**Rationale**：锁定 solver 与初始化次数，避免依赖版本或矩阵形状改变隐式算法。旧 notebook 的 `n_init="auto"` 未显式固定重启次数；本配置是保留原流程的显式参考版本，不能声称重建了旧环境的逐值结果。10 次初始化不代表证明了最优算法或收益。

**Alternatives considered**：不使用 PCA auto solver；不依赖全局 Python；不在 M0 为追求跨语言随机结果而编写新聚类实现。M1 若选用不同随机初始化，应另行决定固定中心/共享初始化还是仅比较分区与目标函数，不能假设相同 seed 就相同结果。

**Observed**：9 个成员、3 个特征的合成矩阵含常量特征和不同身份的重复向量。三个独立进程的比较数值最大差异为 0。空、ragged、非有限、秩不足和可区分向量不足被拒绝。完整观察以 [baseline findings](spikes/baseline/findings.md) 及其结果文件为准。

**Remaining acceptance**：当前合成测试不是 FR-001 的真实样本。正式参考必须接入真实冻结输入，补齐所有中间输出（含簇中心、迭代次数）、明确有效秩阈值与退化子空间检查，保存每次运行和环境/源码身份。按 [验证契约](contracts/validation.md) 比较全部必要量。

## 3. SQL 输入保留成员绑定

**Decision**：使用一个标量函数 `ens_m0_echo(STRUCT(member_id VARCHAR, features DOUBLE[])[])`，返回同类型。任务身份留在调用 SQL 行中；每行一个任务，成员列表内保持顺序。

**Rationale**：ID 和特征在同一结构里，避免两个独立列表错位；最小 echo 足以验证嵌套数据边界，暂不需要自定义扫描器、聚合函数或生产计算。

**Alternatives considered**：不采用互相独立的 member_ids/features 参数，不让函数猜测网格或起报，不将 task_id 塞入数值 core，不把 echo 命名为聚类函数。

DuckDB 默认 NULL 传播不能满足明确拒绝 NULL 的要求。实现必须设置 special NULL handling，并逐层检查嵌套有效性；若 Rust 高层注册接口不暴露该设置，最小使用所锁定版本的 C API，不能靠 SQL 包装器掩盖接口缺陷。具体实测见 extension spike。

**Observed**：spike 使用 `VArrowScalar`，已生成并在 Linux ARM64 / DuckDB 1.5.5 加载独立扩展。主会话新建 CLI 进程实测合法嵌套值原样返回；对类型正确的顶层 NULL 调用却返回 NULL，没有报错。这是一个确定的契约失败，不能算 FR-009 通过。

主会话还用一个 CLI 进程执行“合法查询 -> NULL 特征元素查询 -> 合法查询”：中间实际报错，后续合法查询成功，CLI 最终退出码 1；未使用 TRY 消除错误。这证明一个嵌套错误类别的同连接恢复，完整复跑片段见 [quickstart](quickstart.md#4-rust-扩展)。其余异常类型仍需各自覆盖。

锁定 crate 的 `vscalar/function.rs` 中函数指针封装为私有字段，没有 special handling setter；`Connection::register_scalar_function` 也未暴露注册配置入口。完整路径和其他实验见 [extension findings](spikes/extension/findings.md)。

**Implementation decision（含 C API 符号核查）**：核查 `duckdb-1.10505.0`、`duckdb-loadable-macros-1.10505.0` 与 `libduckdb-sys-1.10505.0/src/bindgen_bundled_version_loadable.rs` 确认：
- `mod arrow_interop` 在 `duckdb-1.10505.0/src/lib.rs` 中为私有模块，`Connection.db.con` 也为包内私有，但 `libduckdb-sys` 的 loadable-extension 桩表已完整导出并初始化：
  - 注册与特殊 NULL 处理：`duckdb_connect`、`duckdb_disconnect`、`duckdb_create_scalar_function`、`duckdb_scalar_function_set_name`、`duckdb_scalar_function_add_parameter`、`duckdb_scalar_function_set_return_type`、`duckdb_scalar_function_set_special_handling`、`duckdb_scalar_function_set_function`、`duckdb_scalar_function_set_error`、`duckdb_register_scalar_function`、`duckdb_destroy_scalar_function`
  - 嵌套向量零拷贝验证与原样返回：`duckdb_data_chunk_get_size`、`duckdb_data_chunk_get_vector`、`duckdb_vector_get_validity`、`duckdb_validity_row_is_valid`、`duckdb_list_vector_get_child`、`duckdb_list_vector_get_size`、`duckdb_struct_vector_get_child`、`duckdb_string_t_length`、`duckdb_string_t_data`、`duckdb_vector_reference_vector`
- 因此正式 M0 扩展无需 fork `duckdb` crate，也无需走 Arrow 往返拷贝：直接在 `*_init_c_api` 入口通过 `duckdb_rs_extension_api_init` 初始化函数表、用 `duckdb_connect` 获取原生 `duckdb_connection` 并注册带 `duckdb_scalar_function_set_special_handling` 的 C 回调；回调内用 `catch_unwind` 包裹逐层校验（顶层 `NULL`、空列表、成员 `NULL`、空/重复 `member_id`、任务内特征不等长、元素 `NULL`/`NaN`/`Inf`），校验通过后直接调用 `duckdb_vector_reference_vector(output, input_vec)` 返回原向量。此修复留给正式 M0 实现阶段验证。

## 4. 版本、平台与依赖隔离

**Decision**：首个组合为 Linux aarch64、Rust 1.96.0、DuckDB CLI 1.5.5、Rust `duckdb` / `libduckdb-sys` `=1.10505.0`。Cargo 精确版本加 lockfile，宿主与扩展匹配；禁用 bundled DuckDB。

**Rationale**：前期研究的模板 main 与本机宿主版本不同，不能复制浮动模板后把编译成功当兼容证明。实际加载才是判断依据。

**Alternatives considered**：不在 M0 升级整个宿主或扩展发布工具链，不关闭版本检查，不将 DuckDB 打包进计算 core。本地测试允许加载未签名扩展，但仍保留版本检查。Arrow 是当前 spike 的数据交换路径，不是 M1 core 的存储或数值库承诺；正式窄 C API 回调仅保留实际需要的依赖。

原生 Rust 是计算核心的约束。参考数据读取、Python 数值基线和开发打包辅助脚本与 core 构建/运行依赖分开记录。M0 只需一个 extension crate；M1 才创建有实际算法的独立 core crate。

## 5. 证据与阶段边界

**Decision**：JSON 小样本和结果、SHA-256 内容身份、明确 pass/fail/not_run 状态。真实样本、参考、往返、资源和最小运行环境分别验收，最终按全部 FR 聚合。

**Rationale**：文件存在或单个 spike 成功不能证明来源合规恢复、参考重复性和宿主错误恢复都完成。已执行失败与尚未执行也不能混为一谈。

**Alternatives considered**：不建立证据数据库、调度服务或通用实验框架。普通小文件与可执行断言足够；Git HEAD 不能替代未提交源码哈希。

具体模型和接口见 [data-model.md](data-model.md)、[SQL 契约](contracts/sql.md)、[验证契约](contracts/validation.md)、[证据报告](contracts/evidence.md)。

## 6. M1 数值依赖建议

**Decision**：M0 不加入生产数值依赖。M1 首个窄原型优先评估 `nalgebra` 的 Rust SVD 路径，加明确规则的标准化和最小 Lloyd/KMeans 实现；备用为 `ndarray` + `linfa-linalg` + `linfa-clustering`。这是一项候选顺序，不是已验证的迁移方案。

**Rationale**：先控制初始化、重启、空簇和停止规则，避免将 Python/Rust 差异误判为数值误差；同时保留复用现有聚类包的选择。两个方案都必须用同一冻结矩阵检验，不仅对齐 API 参数名。

**Alternatives considered**：排除强制外部 BLAS/LAPACK 的配置，避免其进入 core；不因为库默认值看起来相同就假定随机流、停止语义和聚类分区相同。自实现 KMeans 的维护成本是真实代价，M1 原型后再最终选择，不在 M0 提前建设通用数值框架。

本轮只读核查的版本、默认/可选 features、官方资料和限制见 [baseline findings](spikes/baseline/findings.md#rust-候选只读)。未编译这两条 Rust 数值路径，完整 feature tree 和 native 链接验证仍属于 M1。

## 7. 本轮结果与正式门禁

| 范围 | 本轮证据 | 正式 M0 状态 |
| --- | --- | --- |
| 真实数据提取 | 50×9 矩阵、源对象 generation、哈希、离线恢复和拒绝覆盖检查通过 | 仍需规范化清单及来源使用/恢复条件 |
| 合成参考 | 三个独立进程一致，比较器正反例通过；主会话复跑确认 | 不能替代真实样本参考 |
| 真实样本算法条件 | 单次运行：秩 9、三簇、全部 50 个成员保留 | 仍需三进程、完整中间结果和持久证据 |
| 独立扩展加载/合法往返 | ARM64 / 1.5.5 新 CLI 会话实际通过 | 仅证明已测路径 |
| 顶层 NULL 拒绝 | 实际返回 NULL，未进入明确错误路径 | 已知失败，先修注册路径 |
| 完整异常恢复、并发、资源、无参考工具环境 | spike 只覆盖 findings 中列出的局部案例 | 全量验收尚未执行 |
| M1 数值依赖 | 两种候选的只读文档/feature 评估 | 未构建、未数值对照 |

总体结论：技术路线已可分解为任务；M0 整体验收未通过。来源使用条件与顶层 NULL 是明确的前置工作，其余正式门禁按 [证据契约](contracts/evidence.md) 落实，不把“后续有计划”记为通过。
