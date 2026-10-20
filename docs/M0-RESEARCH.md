# M0 前置信息收集

调查日期：2026-10-20

本文是规划输入，不是 M0 spec，也不代表 M0 已开始实施或完成验收。按用户要求，先收集信息，再据此讨论 M0 spec，之后才实施。

后续状态：2026-10-20 已生成 [M0 规格](../specs/001-m0-baseline-validation/spec.md)，随后按用户调用开展技术规划和三个 Luna max spike，见 [计划](../specs/001-m0-baseline-validation/plan.md) 与 [新研究记录](../specs/001-m0-baseline-validation/research.md)。本文中“尚未编写 spec”“未下载/构建”等表述保留为前置调查当时的状态；后续实验不代表完整 M0 验收通过。

## 1. 调查方式

- 主线程盘点 ensabc 当前环境与代码状态。
- 两个 Luna（max）子代理分别只读调查数据来源/DuckOMO 对接、Rust 扩展/算法对照约束。
- 文档和源码证据不替代构建、查询或气象效果实验。
- 本轮不修改算法，不生成测试样本，不构建扩展，不安装项目依赖。

主线程曾用 `cargo info duckdb@1.10505.0` 获取该版本的元数据与源码，写入 Cargo 缓存，未新增项目依赖。未读取 `.aws`。

## 2. 本地环境实测

以下来自当前 shell；不表示其他环境中不存在这些工具或依赖。

| 项目 | 结果 |
| --- | --- |
| 平台 | Linux aarch64 |
| Rust | rustc 1.96.0；cargo 1.96.0 |
| DuckDB CLI | v1.5.5，构建标识 `d8cdaa33fd` |
| Python | 3.13.13，当前来自 Miniconda |
| uv | 0.11.21 |
| 当前 Python 已有 | NumPy 2.5.2、pandas 3.0.5 |
| 当前 Python 未安装 | SciPy、scikit-learn、xarray、duckdb、cfgrib、netCDF4 |
| PATH 中已有 | cc、cmake、make |
| PATH 中未找到 | grib_ls、grib_copy、cdo |
| ensabc 中未找到 | data、tutorial/data、.venv、pyproject.toml、Cargo.toml、tests |
| 本地最近代码提交 | `07e0907`，2023-08-08 |

检查命令包括 `git log`、`git status`、工具的 `--version`、`importlib.metadata.version`、`shutil.which` 与路径存在性检查。未执行旧 notebook。

对后续 spec 的影响：

- 数值算法参考环境与完整气象数据读取环境需要分别定义依赖和验收。
- 本地 ARM64 是可验证目标，不能据此宣称其他平台可运行。
- 旧 notebook 的历史数据没有随当前仓库提供，样本选择仍是明确待决事项。
- 当前全局 Python 版本和依赖仅是环境事实，不应直接成为项目的兼容性承诺。

## 3. 当前流程的证据边界

`tutorial/ensemble_to_probability_product.ipynb` 包含区域成员场展平、`StandardScaler`、二维 PCA、三簇 K-means 和绘图。它是后续参考计算的候选，不是已在当前环境复现的基线。

以下仍未验证：

- 旧依赖环境和数据下的原始数值结果。
- 当前环境执行结果与旧结果的一致性。
- Rust 算法实现和 Python 的对应关系。
- 跨起报特征可比性、情景匹配与预测价值。

## 4. 数据与 DuckOMO 调查

### 已确认的文档证据

- DuckOMO 的季节域记录明确区分合成 ensemble 与真实数据，未取得原始 ensemble 的公开 run 样本。
- 本地记录中的真实 GFS 温度对象是 `temperature_80m`，不是已确认的多成员 `temperature_2m` 集合。不能把它当作旧教程的等价输入。
- Open-Meteo 的 AWS Open Data 官方说明称 ensemble 模型因体量而不在该桶发布。这是该数据渠道的限制，不代表所有来源都无法取得 ensemble。
- DuckOMO 可映射 `member BIGINT/VARCHAR`、`run TIMESTAMP`、`valid_time TIMESTAMP`、`lead_time INTERVAL`。它不自动转置或广播；`dimensions` 描述原始完整有序轴。
- 区域 `points` 模式保留逐网格点记录；`nearest` 每个区域及非空间轴组合返回一个点，不能替代区域场。
- SQL 未显式 `ORDER BY` 时不能依赖返回顺序；成员矩阵需要固定空间点键、成员顺序及一致的点集合。
- domain 不推断单位。单位需按具体源验证并记录，不能从变量名或数值量级单独确认。

本地证据（调查时位于相邻 `duckomo` 仓库，并非 ensabc 自带文件）：

| 文件 | 相关位置 |
| --- | --- |
| `duckomo/docs/seasonal-domains.md` | 第 26、49、53 行：单位、真实样本与原始 ensemble 验证范围 |
| `duckomo/docs/spec.md` | 第 55-87 行：读取参数、轴映射、区域语义、输出顺序 |
| `duckomo/test/data/sampling/real-inputs.json` | 第 60、73、99 行附近：真实温度对象和归档状态 |
| `duckomo/README.md` | 第 22、174 行附近：单对象读取与真实温度示例 |

官方来源：

- [Open-Meteo AWS 模型范围](https://github.com/open-meteo/open-data#other-models)。
- [Open-Meteo 数据组织与保留期](https://github.com/open-meteo/open-data#data-organization)。

### 多起报数据与归档

官方记录的 AWS 公开保留期为：`data_spatial` 7 天、`data_run` 3 个月，后者最多每 3 小时保留一个起报。它不能证明特定模型拥有完整、连续的历史集合起报。

DuckOMO 单次读取单个对象，跨起报需要分别读取并明确其 `run`；不能把“能读 run 轴”当作“历史 ensemble 已可取得”。

本地 GFS 样本清单记录的公开生命周期截至 2026-10-22，并注明归档与再分发约束。该日期和归档状态来自清单，本轮没有重新检查远端对象、归档或恢复能力。

### 对后续 spec 的影响

1. **真实 ensemble 样本来源尚未落实。** 合成 ensemble 可验证结构，真实确定性数据可验证读取，但二者不能合并算作真实多成员验收。
2. 单批次矩阵候选定义为固定模型、区域、起报与目标有效时间下的“成员 × 空间点”；需要验证各成员具有相同点集合和顺序。
3. `run` 可来自可靠外部清单或显式轴映射，不要求它一定内嵌在 OM 文件中。若同时有 `lead_time`，应检查其与有效时间一致。
4. 样本清单需记录对象身份/哈希、成员标签、网格与轴、单位、时间、控制成员规则、来源和可恢复方式。
5. 后续需决定真实集合从何处取得，以及是否需要格式转换；本轮没有因此扩大范围到新下载器或转换器。

未确认：本地真实多成员温度 OM 的二进制内容、单位、成员轴，以及同一目标时间的多起报配对。没有下载、解码或执行 DuckDB 查询。

## 5. Rust 扩展与算法对照调查

### 可加载扩展与版本

官方方案区分连接内注册的 Rust UDF 和独立可 `LOAD` 的扩展。模板使用 `cdylib`、`duckdb_entrypoint_c_api` 及扩展元数据 footer；单独 `cargo build` 不是完整打包流程。

模板工具链包含 Rust/Cargo、Python 3/venv、Make、Git 和 `extension-ci-tools`。本地开发加载未签名扩展需显式启用对应选项，不能假定默认运行时会接受产物。

调查时模板 `main` 的配置：

- `duckdb ~1.10506.0`。
- `TARGET_DUCKDB_VERSION=v1.5.6`。
- `USE_UNSTABLE_C_API=1`，模板注明该模式仅适用于目标 DuckDB 版本。

本机 CLI 为 1.5.5，调查用源码为 crate 1.10505.0。模板当前目标与本机 CLI 不一致；以上不是建议混用的版本组合。本轮没有锁定项目版本或验证 ARM64 构建。

官方来源：

- [Rust 函数接口](https://duckdb.org/docs/current/clients/rust/functions)。
- [扩展模板说明](https://github.com/duckdb/extension-template-rs/blob/main/README.md)。
- [扩展入口示例](https://github.com/duckdb/extension-template-rs/blob/main/src/lib.rs)。
- [模板 Cargo 配置](https://github.com/duckdb/extension-template-rs/blob/main/Cargo.toml)。
- [模板 Makefile](https://github.com/duckdb/extension-template-rs/blob/main/Makefile)。

这些 `main` 链接会变化；实施时需要记录具体修订与锁定版本。

### 嵌套类型与执行约束

本地 `duckdb-1.10505.0` 源码提供 LIST/STRUCT 类型构造、对应向量访问器及 `WritableVector` 写接口。因此“成员 ID 与特征绑定在同一个结构列表中”有接口依据，但精确的嵌套签名尚未端到端验证。

候选类型表达仅用于说明调查对象，不是冻结接口：

```text
LIST<STRUCT(member_id, features DOUBLE[])>
    -> Rust UDF
    -> LIST<STRUCT(...)>
```

源码明确的约束：

- `input.len()` 是当前 chunk 的外层行数，不是每行列表中的成员数；每个外层任务都要产生对应输出。
- 外层列表、成员结构、特征列表及元素的 NULL 需分层检查，具体拒绝或传播策略尚待决定。
- 普通 `Result` 错误和可展开的 panic 有查询错误转换机制；`panic=abort` 不受此保护。
- 注册状态可由多个 worker 线程共享，内部可变状态需要同步。
- 向量的 unsafe 访问必须遵守类型、长度、生命周期与可变别名约束。

本地源码位置，相对 Cargo registry 中的 `duckdb-1.10505.0/`：

| 文件 | 证据位置 |
| --- | --- |
| `src/core/logical_type.rs` | 第 238 行起的 list 类型及第 289 行起的 struct 类型构造 |
| `src/core/vector/list.rs` | 第 99 行起的子向量访问及第 261 行起的容量处理 |
| `src/core/vector/struct.rs` | 第 48 行起的字段访问 |
| `src/arrow_interop/to_duckdb.rs` | 第 57 行附近的可写向量接口 |
| `src/vscalar/mod.rs` | 第 49 行附近的 callback/state 约束 |
| `src/callback.rs` | 第 23 行附近的错误与 panic 处理 |

未确认：精确类型往返、列表长度超过 chunk 行数时的处理、多任务隔离、NULL/error 后连接恢复、并发、峰值内存及数据转换成本。源码存在对应接口不等于这些验收已经通过。

### Python 参考结果的迁移陷阱

- 现有 notebook 逐批拟合 scaler 和 PCA，不能直接比较不同批次独立拟合的坐标。
- sklearn 的零方差特征缩放因子为 1；不能未经确认就在 Rust 中删除常量特征或除以零。
- `PCA(n_components=2)` 没有显式指定 solver，自动选择受库版本和矩阵形状影响。
- PCA 分量有符号歧义；重复或接近的特征值还可能导致基底不稳定。对照对象需区分坐标、重构与子空间。
- 默认 k-means++ 下 `n_init="auto"` 对应一次初始化。相同随机种子不保证不同语言、不同随机数生成器产生相同初始中心。
- 簇编号没有固定语义，不能直接按标签整数逐项比较；应使用分区或对齐后的结果。
- 缺失、非有限值、空输入、重复成员、有效秩不足及不同点集合的策略尚未定义。

官方来源：

- [StandardScaler](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html)。
- [PCA](https://scikit-learn.org/stable/modules/generated/sklearn.decomposition.PCA.html)。
- [KMeans](https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html)。

对后续 spec 的影响：必须明确 Python 基线版本、初始化、求解器、比较对象及容差；先区分“移植结果一致性”和“新算法优于旧算法”的验证目标。

## 6. 收集结论与后续待决事项

本轮两项专题调查和本地环境盘点已完成。信息足以列出下一轮 spec 的决策点，不足以宣称技术或数据链路已验收。

| 待决事项 | 当前证据 | 后续需要决定什么 |
| --- | --- | --- |
| 真实集合样本 | 尚未确认可用的多成员温度 OM；公开桶不能作为默认保证 | 数据来源、格式、固定方式与可恢复性 |
| 样本分层 | 合成 ensemble 和真实确定性数据各有用途 | 是否将结构测试、读取测试、真实集合验证拆成不同门禁 |
| 环境与版本 | ARM64、CLI 1.5.5；模板当前目标 1.5.6 | 项目支持组合、锁定版本与隔离环境 |
| SQL 数据契约 | 有嵌套类型接口依据，没有精确签名实测 | 成员 ID 类型、排序、NULL/错误策略及输入规模 |
| 数值基线 | 旧 notebook 流程明确，环境和原始样本未复现 | solver、初始化、退化数据处理与对照标准 |
| 历史起报 | 未确认连续 ensemble 样本 | M0 是否只验单批次；历史存档准备放在哪一阶段 |

本轮没有制定 M0 spec、生成样本、执行 notebook、编译或加载扩展，也没有做性能与气象效果测试。后续先讨论这些决策点，再写 spec 和实施任务。

## 7. 补充讨论：原生 Rust 与开放集合数据

2026-10-20 用户进一步明确：

- core 使用原生 Rust 或 Rust 包实现，不依赖 CDO、wgrib/wgrib2 或 Python 运行核心算法。
- 可以按需安装这些外部工具进行独立验证；参考工具不进入生产 core 的依赖。
- 继续先收集信息，之后才规划 M0 spec。

已读取用户提供的 [DuckDB接入集合预报讨论](https://chatgpt.com/share/6ad7133c-4f7c-83e8-879d-7f1eaab54034)。讨论是候选方案来源，其中示例 SQL 与拟议接口不视为已运行代码。以下关键内容已对照官方资料。

### 区分数据格式与分发渠道

| 对象 | 本轮确认的范围 | 不能据此推出的结论 |
| --- | --- | --- |
| OM 格式与 DuckOMO | 多维格式与 member 轴映射能够表达集合数据 | 格式支持不等于公开桶必有完整逐成员产品 |
| Open-Meteo AWS OM 分发 | 官方说明 ensemble 模型因体量不在 AWS 发布 | 不能据此说 Open-Meteo 完全不提供逐成员值 |
| Open-Meteo Ensemble API | 文档提供逐成员预报，历史回看最多 3 天，均值/散度保留更久 | 不等同于可任意选起报的原始网格归档 |
| Dynamical IFS ENS 归档 | Catalog 给出 `init_time × lead_time × ensemble_member × latitude × longitude`，成员 0-50；该产品仅含 00 UTC 起报，示例经 Icechunk 打开 | 不能宣称通用 Zarr 扩展已能直接读取这个 Icechunk 仓库 |
| Dynamical Forecast API | 文档返回逐成员值、显式起报、实际网格点及 snapshotId，可指定起报或起报区间 | 单点接口不等于区域网格扫描；具体 IFS 请求尚未实测 |
| WeatherBench 2 IFS ENS | 文档提供 2018-2022 年 Zarr，示例成员为 1-50，并单独提供均值数据 | 不要硬编码所有 IFS 产品必含 51 成员；它不是同一时期的实时 IFS 数据 |

以上为官方文档证据；本轮没有完整枚举公开桶，也未请求 ensemble 数据值或验证这些来源在 DuckDB 中的实际读取。

来源：

- [Open-Meteo AWS 分发范围](https://github.com/open-meteo/open-data#other-models)。
- [Open-Meteo Ensemble API](https://open-meteo.com/en/docs/ensemble-api)。
- [Dynamical IFS ENS Catalog](https://dynamical.org/catalog/ecmwf-ifs-ens-forecast-15-day-0-25-degree/)。
- [Dynamical API](https://dynamical.org/api/)。
- [WeatherBench 2 IFS ENS](https://weatherbench2.readthedocs.io/en/latest/data-guide.html#ifs-ens)；其中 TIGGE 来源数据的使用限制按官方说明处理。

### 对旧讨论中对照实验的修正

旧讨论建议从 Dynamical 和 Open-Meteo 获取“同一区域、同一起报”的 IFS ENS 再做对照。这个前提尚未验证，不应直接写成验收要求：

1. Dynamical API 明确支持 `initTime`。本次查阅的 Open-Meteo Ensemble API 参数表没有同等的起报选择参数；`start_date`/`start_hour` 描述查询时间范围，不能替代起报身份。
2. Open-Meteo 文档有选格点与海拔订正选项，Dynamical API 描述为选择最近源网格点。即使请求坐标相同，也需要核验实际点位、单位、时效、成员身份与后处理。
3. 单点或多站点成员序列可用于概率/轨迹实验，但不自动等同于现有 notebook 的区域空间场聚类。
4. 校验 Rust 算法时，优先让两套实现消费同一份冻结输入。跨数据提供商差异应作为独立的数据一致性问题，避免与算法误差混在一起。
5. WeatherBench 2 可作为独立历史案例来源，不应把其 2018-2022 年资料与较新实时预报直接视为逐值对照样本。

因此需要调整前一轮结论的范围：**尚未落实的是本地可复现的真实集合样本，尤其是 OM 逐成员样本；不是所有开放集合数据都缺失。** Dynamical 及 Open-Meteo API 应进入样本候选调查，是否优先采用仍待讨论。

本轮只更新研究与路线记录，未安装验证工具，也未创建 M0 spec 或实现新的数据适配器。
