# M1 复跑与验收指南

**当前状态：M1 实施已完成并通过完整门禁验收（`artifacts/m1/m1-gate-final`）。** 本指南记录了 M1 单批次情景计算（ensabc-core、DuckDB SQL 扩展 `ens_cluster` 及完整门禁验证工具链）的复跑与验收命令。CLI 细节见 [evidence.md](contracts/evidence.md)。

## 1. 已存在的环境与输入准备

从仓库根目录执行。已锁定环境为 Linux aarch64 / Ubuntu 24.04、Rust 1.96.0、DuckDB CLI 1.5.5、Python 3.13.13。参考与数据依赖继续复用 M0 锁文件，安装可能需要网络。

```bash
bash validation/m0/setup_env.sh --check
```

尚未建立时运行 `bash validation/m0/setup_env.sh`。解释器覆盖沿用 M0 的 `ENSABC_M0_REFERENCE_VENV` 与 `ENSABC_M0_DATA_VENV`，默认参考路径 `/tmp/ensabc-m0-baseline`，默认数据路径 `/tmp/ensabc-m0-data/.venv`。以 setup_env 的实际参数/日志为准，不能假设系统 Python 安装了 sklearn。

已有本地冻结载荷时核验：

```bash
"${ENSABC_M0_DATA_VENV:-/tmp/ensabc-m0-data/.venv}/bin/python" \
  validation/m0/verify_sample.py --offline
```

首次获取缺失载荷需访问钉住来源；派生数值不随仓库分发。为避免修改已提交的原清单，恢复到外部目录：

```bash
"${ENSABC_M0_DATA_VENV:-/tmp/ensabc-m0-data/.venv}/bin/python" \
  validation/m0/extract_sample.py --rebuild --from-source \
  --output-dir /tmp/ensabc-m1-sample

"${ENSABC_M0_DATA_VENV:-/tmp/ensabc-m0-data/.venv}/bin/python" \
  validation/m0/verify_sample.py --offline \
  --manifest /tmp/ensabc-m1-sample/real_ifs_ens_20200101_t2m_manifest.json \
  --payload /tmp/ensabc-m1-sample/real_ifs_ens_20200101_t2m_data.json
```

外部目录须为新目录；不使用 `--force` 覆盖旧证据。来源不可达应报告恢复受阻，而非用合成矩阵替代。预期样本为 50 个扰动成员、9 个共同有序网格特征、K 单位、2020-01-01 00 UTC 起报 +24h。payload SHA-256 为 `ae8f0349cc1c6cf189e1757c454163a5ec550dc5768b50c03cb9adf34f7baa64`，源 float32 矩阵 SHA-256 为 `09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe`。新清单获取时间可不同，不能以时间戳变化误报数值身份改变。

## 2. 实现后的构建与独立 core 验证

以下 `validation/m1/`、`ensabc-core` 及 runner 由后续实施任务交付，当前不存在。

```bash
bash validation/m1/setup_env.sh --check
cargo test --locked -p ensabc-core
cargo tree --locked -p ensabc-core -e features
bash validation/m1/build_extension.sh
```

依赖审计预期：core 没有 DuckDB、Arrow、Python 或外部 BLAS/LAPACK；nalgebra 仅 std，随机源不依赖 OS/thread RNG。构建产物默认位于 `/tmp/ensabc-m1-extension-target/`，扩展保持 `ensabc_m0_extension.duckdb_extension` 名称，避免与 M0 的构建目录混用。具体 ABI 和链接库记录在 environment。

准备一个全新运行目录并生成验证输入：

```bash
python3 validation/m1/prepare_input.py \
  --manifest /tmp/ensabc-m1-sample/real_ifs_ens_20200101_t2m_manifest.json \
  --payload /tmp/ensabc-m1-sample/real_ifs_ens_20200101_t2m_data.json \
  --config validation/m1/fixtures/native_config.json \
  --output-dir artifacts/m1/manual-check

cargo run --locked --release -p ensabc-core --example m1_runner -- \
  --input artifacts/m1/manual-check/inputs/task.json \
  --output artifacts/m1/manual-check/core/manual-result.json

python3 validation/m1/run_reference.py --run-dir artifacts/m1/manual-check --stage original
python3 validation/m1/verify_core.py --run-dir artifacts/m1/manual-check --stage native
python3 validation/m1/compare_results.py --run-dir artifacts/m1/manual-check --stage original
python3 validation/m1/run_reference.py --run-dir artifacts/m1/manual-check --stage shared
python3 validation/m1/verify_core.py --run-dir artifacts/m1/manual-check --stage shared
python3 validation/m1/compare_results.py --run-dir artifacts/m1/manual-check --stage shared
python3 validation/m1/compare_results.py --self-check
```

这六个 stage 调用按顺序执行，各阶段仅消费已完成的前置产物并写自己的文件。original 比较输出 `comparison/projection-alignment.json` 后才能生成 shared 参考；shared 步骤不能反向补跑 native 或覆盖原比较。省略 stage 是参数错误；前置未完成返回 2，输入/引用不符或实际检查失败返回 1。reference 入口通过 `ENSABC_M0_REFERENCE_VENV` 指定的锁定解释器执行子进程，不要求全局 Python 安装 sklearn。

预期：原生默认运行达到原 M0 的同一成员分区，簇大小集合 `{20,16,14}`、总 SSE 约 `87.0930790638583`，全部可比中间量满足冻结容差；输出包含物理均值、真实代表成员、占比和诊断，可独立重算。规范化簇编号后顺序可能与原参考任意标签不同，不能按大小列表顺序直接判错。三进程重复等价。

随机初始化的每个 restart 结果包含长 k 的 `initial_member_indices`，与该 restart 的初始中心顺序对应；显式中心模式返回空列表，共享验证所选起点行另在初始化证据中记录。参考轨迹来自隔离子进程包装实际单轮内核，`observer-check.json` 必须证明最终结果与未包装参考精确相同；最后仅分配步骤不计迭代，不能读取其残留 shift。Rust 轨迹导出测试由 shared 驱动显式运行，不成为普通测试或生产调用的环境前提。

共享起点 `[0,16,33]` 的单次参考规划观察为簇大小 `{12,36,2}`、SSE 约 `119.99258497127667`；其验收目标是双方从相同起点得到等价过程和结果，不要求与原 M0 的随机最优分区相同。共享对照与原基线对照都必须通过。详细数值/对齐规则见 [numerics.md](contracts/numerics.md)。

## 3. 实现后的查询集成

```bash
python3 validation/m1/verify_extension.py \
  --run-dir artifacts/m1/manual-check \
  --extension /tmp/ensabc-m1-extension-target/ensabc_m0_extension.duckdb_extension
```

预期：实际加载并验证 SQL 签名、core/SQL 全字段对照、真实样本到结果表保存、跨内部 task/member 批量、两连接有时间区间重叠的并发、全部非法类别的同连接恢复、混合任务失败及保存回滚。M0 echo 原用例也通过。

人工执行已生成的真实样本示例时，由脚本将本次样本/配置路径写进 SQL，使用固定产物：

```bash
duckdb -unsigned :memory: <<'SQL'
LOAD '/tmp/ensabc-m1-extension-target/ensabc_m0_extension.duckdb_extension';
.read artifacts/m1/manual-check/real_sample.sql
SQL
```

脚本依据 `validation/m1/examples/real_sample.sql` 模板生成 SQL，不依赖个人固定路径。查询入口、类型、保存方式及 standalone 示例见 [sql.md](contracts/sql.md)。`-unsigned` 只用于本地开发，版本检查仍开启，不表示签名发布已完成。

## 4. 实现后的隔离与测量

```bash
bash validation/m1/verify_minimal_runtime.sh \
  --run-dir artifacts/m1/manual-check \
  --extension /tmp/ensabc-m1-extension-target/ensabc_m0_extension.duckdb_extension

python3 validation/m1/measure_tasks.py \
  --run-dir artifacts/m1/manual-check \
  --extension /tmp/ensabc-m1-extension-target/ensabc_m0_extension.duckdb_extension
```

隔离预期：固定 digest 镜像、断网、无 Python/参考工具，core runner 单独执行和 SQL 实际聚类/错误恢复均通过。不能以只运行 echo 或 ldd 替代。

测量预期：1/10/100 任务各至少 3 次，正确结果、wall time、独立进程峰值 RSS、线程和字节范围完整。已加载时间和整个进程峰值内存分别说明，转换/计算无法分离时标记 not_separated，不作未经测试的性能承诺。

## 5. 实现后的完整门禁

顶层命令使用新的 run-id，不对手工目录补写整体 passed：

```bash
python3 validation/m1/run_m1_gate.py --run-id m1-acceptance \
  --manifest /tmp/ensabc-m1-sample/real_ifs_ens_20200101_t2m_manifest.json \
  --payload /tmp/ensabc-m1-sample/real_ifs_ens_20200101_t2m_data.json

python3 validation/m1/traceability.py --run-dir artifacts/m1/m1-acceptance
python3 validation/m1/verify_report.py --run-dir artifacts/m1/m1-acceptance
python3 validation/m1/verify_report.py --self-check
```

预期：17/17 FR、8/8 SC 覆盖，无孤立断言；report `passed`、traceability `pass`、report-audit `pass` 才整体通过。任何实际失败返回 1，缺失环境或必要未执行返回 2。单步骤 0 不代表整体通过。所有引用重新计算 SHA-256；比较器、门禁和审计坏证据反例均被拒绝。

文件集合及无循环审计顺序见 [evidence.md](contracts/evidence.md)。原基线与共享初始化、真实与合成、core 与 SQL 各自具有明确身份，不能混用。

## 6. 实现后的独立复现

门禁会自动执行复现，也可单独运行：

```bash
python3 validation/m1/reproduce_independently.py \
  --run-dir artifacts/m1/m1-acceptance \
  --work-dir /tmp/ensabc-m1-independent-check

python3 validation/m1/reproduce_independently.py --self-check
```

work-dir 必须为尚不存在的绝对路径；省略时自动创建唯一临时目录。父模式不要求已有完整 report，因此 T034 完成后即可对已生成 core/SQL 结果验证，不必等 T037 gate 实现。父模式由该目录建立 clone/sample/child-output/build，并用新 clone 内同一脚本的子模式调度已存在入口。子模式参数范式（由父脚本填入真实路径，不需要用户手动执行）：

```bash
python3 /tmp/ensabc-m1-independent-check/clone/validation/m1/reproduce_independently.py \
  --reproduction-child \
  --output-dir /tmp/ensabc-m1-independent-check/child-output \
  --source-manifest /tmp/ensabc-m1-independent-check/source-manifest.json \
  --manifest /tmp/ensabc-m1-independent-check/sample/manifest.json \
  --payload /tmp/ensabc-m1-independent-check/sample/payload.json \
  --config /tmp/ensabc-m1-independent-check/sample/native-config.json
```

预期：全新 clone 的有效源码摘要与本次运行一致，开始/结束工作树均干净，按锁文件重建并实际复现真实结果/六阶段数值对照/错误恢复。脏开发树允许先导出明确源码 allowlist 到临时仓库形成有效快照；不得修改原历史，也不得遗漏未提交实现或混入真实派生载荷。样本、配置、输出、Cargo target 和 Python cache 放 clone 外，缓存/网络/同机限制记录。

子模式的 0/1/2 只表示固定子集通过/失败/不完整，不生成 M1 整体报告，不调用完整 gate、追溯、审计或再次复现。完整 gate 只调用此脚本父模式，不新增 gate 子模式。父模式独立比较结果，并把必需子证据及重新计算的引用哈希保存至父 run 的 reproduction/child/ 后写 independent-reproduction.json；审计不能依赖临时工作目录仍然存在。

## 7. 支持边界

M1 实现与验收已在锁定环境（Linux aarch64 / Ubuntu 24.04、Rust 1.96.0、DuckDB CLI 1.5.5、Python 3.13.13）完成并通过门禁。其他平台、WASM、签名发布、跨起报持续身份、概率校准及下游收益验证属于后续里程碑（M2/M3/M4）。样本派生物再分发许可仍未落实，真实数值载荷不加入仓库提交。所有结论由实际门禁报告（`artifacts/m1/m1-gate-final/report.json`）及独立审计支持。
