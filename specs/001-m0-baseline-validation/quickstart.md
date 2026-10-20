# M0 复跑与验收指南

本文件是 M0 正式实现的复跑与验收入口。规划期 spike 的实验记录仍在
[spikes/](spikes/) 下，但本文件描述的是 [tasks.md](tasks.md) 定义的正式验收流程。

**不要把单个步骤的 PASS 解释为整个 M0 通过。** 总体判定只来自
`artifacts/m0/<run-id>/report.json`，并由 `validation/m0/verify_report.py` 独立复核。

## 1. 环境

从仓库根目录执行。已验证环境为 Linux aarch64 / Ubuntu 24.04、Rust 1.96.0、
DuckDB CLI 1.5.5、Python 3.13.13。

```bash
cd /home/blizhan/repo/github/ensabc
python3 --version
rustc --version
duckdb --version
```

一次性建立两个隔离环境并核对版本。参考环境与数据环境职责不同，互不替代；
两者都不会被加入 Rust core 的依赖。

```bash
bash validation/m0/setup_env.sh          # 建立环境
bash validation/m0/setup_env.sh --check  # 只核对已建立的环境
```

依赖安装需要网络；冻结样本核验、参考计算和扩展验收不需要网络（样本首次
重建除外，见第 2 节）。

本次验收使用的**完整版本组合**固定在
[version_combination.json](fixtures/version_combination.json)：平台、Rust 工具链、
DuckDB 宿主与扩展 ABI 类型、扩展依赖版本、参考与数据环境的库版本，以及打包
工具的修订。M1 可以换用其他数值栈，但必须记录自己用的组合；扩展的 DuckDB
版本与 ABI 类型与宿主绑定，改动任一项都需重新验证扩展加载与错误恢复。

## 2. 冻结样本

**派生的数值载荷不提交到仓库**：WeatherBench 2 再加工衍生物的再分发许可尚未
落实，仓库只提交清单、源对象 generation 证据和恢复步骤。任何验证者都可用
下列命令重建出逐字节相同的载荷：

```bash
/tmp/ensabc-m0-data/.venv/bin/python \
  validation/m0/extract_sample.py --rebuild --from-source
```

预期：`sample_id` 为 `ifs_ens_20200101_t2m_50x9`，形状 `[50, 9]`，载荷
SHA-256 为 `ae8f0349cc1c6cf189e1757c454163a5ec550dc5768b50c03cb9adf34f7baa64`。
源矩阵的 float32 SHA-256 为
`09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe`。

离线核验（不需要网络）：

```bash
python3 validation/m0/verify_sample.py --offline
```

预期：`PASS offline sample verification`，50 个成员、9 个有序网格点、
源矩阵 SHA-256 一致、冻结总量远低于 100 MiB，并拒绝 mean-only、单点、
缺成员、重复点和 `latest` 起报身份这五类不合格样本。

从源回源恢复（核对源对象 generation 未变，并重新提取后与冻结副本逐字节比较）：

```bash
python3 validation/m0/verify_sample_recovery.py
```

预期：`sample recovery: pass`，7 个源对象 generation 一致，重新提取的 CSV
SHA-256 与冻结副本相同。源不可达时该命令返回 `not_run` 并说明原因，不会
谎报成功。

## 3. 三进程参考基线

```bash
/tmp/ensabc-m0-baseline/bin/python validation/m0/run_reference.py \
  --output-dir /tmp/m0-reference-check
```

预期：三个**独立进程**（pid 互不相同）产生等价结果，秩 9，簇大小
`[20, 16, 14]`，惯性 `87.0930790638583`（与规划期 spike 记录一致）。
比较规则在生成任何验收结果之前已固定于
[reference_config.json](fixtures/reference_config.json)。

比较器自身的正反例自检：

```bash
/tmp/ensabc-m0-baseline/bin/python validation/m0/compare_reference.py --self-check
```

预期：标签置换和 PCA 符号翻转被接受；成员换簇、数值超差、惯性改变、秩
改变、输入不同、成员顺序改变、**保留子空间内的旋转**和**退化子空间**均被检出。

## 4. Rust 扩展

```bash
bash validation/m0/build_extension.sh
```

产物：`/tmp/ensabc-m0-extension-target/ensabc_m0_extension.duckdb_extension`
（`C_STRUCT_UNSTABLE`、`linux_arm64`、`v1.5.5`）。构建锁定 `Cargo.lock`，
打包工具锁定 `extension-ci-tools` 的固定修订。

在真实宿主中验收：

```bash
python3 validation/m0/verify_extension.py
```

预期：`extension verification: pass`，覆盖 6 个正向用例、10 类非法输入
（每类都在**同一连接**上执行“合法 → 非法 → 合法”并确认恢复）、类型绑定
错误单独报告、跨 2048 行任务批量边界、跨 2048 成员列表边界、混合合法/非法
批次整体失败且不交付部分结果、两个**同时启动**的连接结果一致。

单个会话内的人工核对：

```bash
duckdb -unsigned :memory: <<'SQL'
.bail off
LOAD '/tmp/ensabc-m0-extension-target/ensabc_m0_extension.duckdb_extension';
SELECT 'before' AS stage, ens_m0_echo([{'member_id':'ok','features':[1.0,2.0]}]) AS echoed;
SELECT ens_m0_echo(NULL::STRUCT(member_id VARCHAR, features DOUBLE[])[]) AS echoed;
SELECT 'after'  AS stage, ens_m0_echo([{'member_id':'ok','features':[1.0,2.0]}]) AS echoed;
SQL
```

预期：`before` 和 `after` 都返回完整结构，中间顶层 `NULL` 明确报错
（`task input layer: the members list of task row 0 is NULL`）。CLI 因预期错误
返回退出码 1，这是正常的；验收点是错误内容和随后的成功输出。

资源测量（1、10、100 个任务）：

```bash
python3 validation/m0/measure_tasks.py
```

预期：记录每个规模的任务数、成员数、特征数、输入/输出字节、冷启动与已加载
两种口径的墙钟时间、峰值 RSS、宿主线程设置和测量边界，并对 1/10/100 各验证
**串行结果与并发结果一致**。**不设吞吐阈值**；冷启动口径覆盖整个进程，已加载
口径是 DuckDB 的单语句计时，两者分别记录、不相加。无法分离的转换/计算成本、
进程启动与扩展加载、测量程序自身开销都明确记为“未分离”，不伪造分项计时。

## 5. 无参考工具的最小运行环境

```bash
bash validation/m0/verify_minimal_runtime.sh
```

预期：`minimal runtime verification: pass`。脚本把 DuckDB CLI、所需共享库
和扩展复制进一个以 `--network none` 启动的隔离容器；容器内没有 Python 参考
栈、没有 `cdo`、没有 `wgrib`/`wgrib2`，正向往返和错误恢复都在容器内实际执行。
仅清空 PATH 或检查链接依赖不算通过。

## 6. 完整 M0 门禁

```bash
python3 validation/m0/run_m0_gate.py
```

该命令执行全部验收步骤，把证据写入 `artifacts/m0/<run-id>/`：

```text
environment.json          source-hashes.json
sample-validation.json    sample-recovery.json
reference/run-1.json      reference/run-2.json     reference/run-3.json
reference/comparison.json reference/comparator-self-check.json
exchange.json             measurements.json        minimal-runtime.json
independent-reproduction.json
steps.json                traceability.json        report.json
report-audit.json
```

报告中的每个 artifact 引用都带**相对路径和 SHA-256**；复核器会重新计算并比对，
引用不解析或哈希不符都会判为审计失败。

退出码：`0` 表示总体 `passed`，`1` 表示验证失败，`2` 表示证据不完整或执行
条件未满足。

独立复核报告：

```bash
python3 validation/m0/verify_report.py --run-dir artifacts/m0/<run-id>
```

复核器不信任门禁自身的结论，而是重新推导总体判定、核对引用是否可解析**并重算
每个 artifact 的 SHA-256**，并要求追溯矩阵与报告一致；运行门禁逻辑自检：

```bash
python3 validation/m0/verify_report.py --self-check
```

预期：声称 `passed` 但有 `fail` 或 `not_run` 的报告被拒绝；仅覆盖合成样本
的报告被拒绝为 M0 通过。

## 7. 需求与成功标准到证据的追溯矩阵

`validation/m0/traceability.py` 把 FR-001 至 FR-014 **以及 SC-001 至 SC-006**
逐条绑定到**可执行断言**，每条断言指向一个证据文件里的具体字段并声明比较方式。
矩阵是**从证据推导**的，不是手写状态，因此不会与实现悄悄偏离：证据一旦不再
满足断言，矩阵就会报出来。

```bash
python3 validation/m0/traceability.py --run-dir artifacts/m0/<run-id>
```

预期：`traceability: pass`，14/14 需求与 6/6 成功标准都有断言，正向覆盖完整
（无遗漏需求或成功标准）、反向覆盖完整（无孤立断言），且矩阵推导出的每条需求
判定与 `report.json` 记录的判定一致。

门禁在写出 `report.json` 之后自动运行该矩阵。若矩阵不支持报告中的某个
`pass`，该需求会被降级为 `fail`，整体判定随之改变——已通过注入故障验证
（把非法输入类别数从 10 改成 8 后，门禁退出码变为 1、FR-006 降级）。

### 成功标准（SC）的验证

SC-006 要求“另一名验证者或独立干净环境能按步骤重现 SC-001 至 SC-004”。
`validation/m0/reproduce_independently.py` 在**单独目录**里从已提交的源码重跑
样本重建、参考基线与扩展验收，再把**内容**（而非文件名或时间戳）与本次运行
逐项比对：

```bash
python3 validation/m0/reproduce_independently.py --baseline-run artifacts/m0/<run-id>
```

门禁把它作为一步自动执行，结果记入 `independent-reproduction.json`。

## 8. 独立验证者复现

本轮已按“另一名验证者”场景实测：把仓库复制到独立目录并提交，然后 `git clone`
到一个全新的工作目录，在该克隆中运行完整门禁。

```bash
# 在独立克隆中
python3 validation/m0/run_m0_gate.py --run-id verifier-run
python3 validation/m0/verify_report.py --run-dir artifacts/m0/verifier-run
```

实测结果：独立克隆的门禁 `passed`（14/14），审计 `pass`，追溯矩阵 `pass`。
与原仓库的证据对比：

| 证据项 | 结果 |
| --- | --- |
| 源矩阵 SHA-256 | 逐字节相同 |
| 载荷 SHA-256 | 逐字节相同 |
| 成员集、点数、起报/有效时刻 | 相同 |
| 成员分区（聚类归属） | 相同 |
| 标准化、投影、簇中心、分数 | 差值 0 |
| `inertia` | 差值 2.8e-14（BLAS 线程求和次序，远低于 1e-8 容差） |
| 清单 `manifest_sha256` | **不同**，仅因 `source.retrieved_at` 记录了本次获取时间（FR-002 要求） |

**注意：只有派生的数值载荷不进入版本控制。** 规划期 spike 的提取工具和
findings 属于规划期证据，必须留在版本控制里，否则恢复链会断——本轮曾因
`.gitignore` 的 `data/` 模式误伤 `spikes/data/`，导致全新克隆的门禁返回
`incomplete`（退出码 2）。现规则已把该模式锚定为仅匹配顶层 `/data/`。

## 9. 结论边界

- 总体 `passed` 要求 FR-001 至 FR-014 全部有满足要求的证据；目录存在、
  研究完成、编译成功、spike 通过或清单勾选都不能单独推出 M0 通过。
- `report.json` 的 `limitations` 记录**本次未覆盖的平台、范围与限制**，即使
  全部通过也非空；它与 `docs/ROADMAP.md` 的遗留限制说明一致。
- 本次验收覆盖的平台只有 Linux aarch64 + DuckDB CLI 1.5.5。其他宿主版本、
  x86_64、macOS、Windows、WASM 和扩展签名发布均未覆盖。
- 样本的再分发许可仍未落实；因此派生载荷不进入仓库，任何验证者需按第 2 节
  从源重建。许可状态记录在清单的 `usage.license_status` 字段。
- 正式原生聚类移植属于 M1。M0 只验证数据边界、参考可复现性和调用安全性。
