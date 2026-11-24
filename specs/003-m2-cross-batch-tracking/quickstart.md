# M2 复跑与验收指南

**当前状态**：修复审查发现后，完整门禁 `artifacts/m2/m2-review-v2-gate/` 已通过。21 项功能要求、8 项成功标准均为通过，`report-audit.json` 无发现。接口定义见 [core.md](contracts/core.md)、[numerics.md](contracts/numerics.md)、[om-duckomo.md](contracts/om-duckomo.md)、[sql-history.md](contracts/sql-history.md) 和 [validation-evidence.md](contracts/validation-evidence.md)。

## 1. 环境与完整门禁

平台为 Linux aarch64，使用锁定的 Rust、DuckDB 1.5.5 与 Python 环境。首次执行需要网络恢复依赖、DuckOMO 发布包和 WeatherBench2 数据；缓存齐全后可复用。最小运行验收在 `--network none` 容器中执行完整回放。

```bash
bash validation/m2/setup_env.sh
bash validation/m2/setup_env.sh --check
cargo test --locked -p ensabc-core
python3 validation/m2/run_m2_gate.py --run-id m2-acceptance
python3 validation/m2/verify_report.py --run-dir artifacts/m2/m2-acceptance
```

`run-id` 必须未使用过。证据写入 `artifacts/m2/<run-id>/`；失败记录保留。`execution-steps.json` 记录真实命令、退出码和耗时，报告不填造通过状态。完整门禁包含 SQL 契约、M1 回归和独立检出重建。

DuckOMO 使用 `validation/m2/.cache/v0.3.5-rc.1/` 与 `v0.3.4/` 两个发布包。每次加载前核对发布哈希，每个进程使用新的 HOME/扩展目录，禁用自动安装和加载，并断言加载身份、版本及 `NOT_INSTALLED`。

## 2. 手动构建和准备样本

以下命令从仓库根目录顺序执行。OM 准备和分析使用专用虚拟环境。

```bash
bash validation/m2/build_extension.sh --output-dir "$PWD/artifacts/m2/manual-check/build"
validation/m2/.venv/bin/python validation/m2/prepare_om_sample.py \
  --cache-dir validation/m2/.cache \
  --out-dir artifacts/m2/manual-check/sample
validation/m2/.venv/bin/python validation/m2/test_om_sample.py \
  --sample-dir artifacts/m2/manual-check/sample \
  --cache-dir validation/m2/.cache \
  --out-report artifacts/m2/manual-check/om-verification/test-om-sample-report.json
```

构建产物包括 `ensabc_m0_extension.duckdb_extension`、`m1_runner` 和 `m2_runner`。打包工具前置位置可通过 `EXTENSION_CI_TOOLS` 指定，脚本核对其固定版本。

恢复路径为真实 WeatherBench2 集合转 OM 后接入。清单绑定 8 次起报、每批 50 个扰动成员及 3×3 区域格点。派生载荷和运行证据不提交 Git，许可边界见 [验证套件说明](../../validation/m2/README.md)。

## 3. Core、SQL 和双版本回放

```bash
validation/m2/.venv/bin/python validation/m2/verify_matching_core.py \
  --run-dir "$PWD/artifacts/m2/manual-check" \
  --runner "$PWD/artifacts/m2/manual-check/build/m2_runner"
ENSABC_M2_EXTENSION="$PWD/artifacts/m2/manual-check/build/ensabc_m0_extension.duckdb_extension" \
  validation/m2/.venv/bin/python -m unittest validation.m2.test_sql_history validation.m2.test_review_regressions
python3 validation/m2/verify_sql_and_history.py \
  --scope synthetic --run-dir "$PWD/artifacts/m2/manual-check" \
  --extension "$PWD/artifacts/m2/manual-check/build/ensabc_m0_extension.duckdb_extension"
validation/m2/.venv/bin/python validation/m2/verify_om_paths.py \
  --stage cells --run-dir "$PWD/artifacts/m2/manual-check" \
  --sample-dir "$PWD/artifacts/m2/manual-check/sample" --cache-dir validation/m2/.cache
python3 validation/m2/verify_sql_and_history.py \
  --scope real --run-dir "$PWD/artifacts/m2/manual-check" \
  --sample-dir "$PWD/artifacts/m2/manual-check/sample" --cache-dir validation/m2/.cache \
  --extension "$PWD/artifacts/m2/manual-check/build/ensabc_m0_extension.duckdb_extension"
validation/m2/.venv/bin/python validation/m2/verify_om_paths.py \
  --stage replay --run-dir "$PWD/artifacts/m2/manual-check" \
  --sample-dir "$PWD/artifacts/m2/manual-check/sample" --cache-dir validation/m2/.cache
```

顺序固定为 cells → real → replay。cells 核对源载荷摘要、独立 OM 解码与两版 DuckOMO 的 3,600 个值逐值一致；real 在同一连接完成读取、聚类、匹配与五表发布，并以三个独立进程重跑核对结果；replay 比较两版 DuckOMO 产生的情景及关联。

版本键由有序 OM 文件实际摘要、配置、权重及软件身份决定。数据库保留旧版本；同键同内容重复发布幂等，同键不同内容拒绝。元数据错误在聚类前失败，正式历史不留部分版本。

## 4. 扰动、最小环境与复现

```bash
validation/m2/.venv/bin/python validation/m2/run_case_and_sensitivity.py \
  --run-dir "$PWD/artifacts/m2/manual-check"
bash validation/m2/verify_minimal_runtime.sh \
  --run-dir "$PWD/artifacts/m2/manual-check" \
  --extension "$PWD/artifacts/m2/manual-check/build/ensabc_m0_extension.duckdb_extension" \
  --runner "$PWD/artifacts/m2/manual-check/build/m2_runner"
validation/m2/.venv/bin/python validation/m2/measure_pipeline.py \
  --run-dir "$PWD/artifacts/m2/manual-check" \
  --extension "$PWD/artifacts/m2/manual-check/build/ensabc_m0_extension.duckdb_extension"
python3 validation/m2/run_m1_regression.py --run-dir "$PWD/artifacts/m2/manual-check"
python3 validation/m2/reproduce_independently.py --run-dir "$PWD/artifacts/m2/manual-check"
```

敏感性和测量脚本使用 `validation/m2/data/om_sample/` 的默认冻结样本；运行前可用 `prepare_om_sample.py --out-dir validation/m2/data/om_sample` 从同一缓存恢复。主结果为 `k=3, c=1.0`，同时执行 `k=2/4` 和冻结门槛系数范围。成员扰动先按成员重叠映射情景，映射歧义单独记录；只有出生节点对应且上游继承不变时才断言逻辑身份不变。

最小容器内没有 Python 和参考工具，执行完整 8 批历史发布并与正常环境逐字段核对，另在同连接注入发布失败和非法调用，确认无残余且合法回放恢复。Python 仅在容器外准备 SQL、检查输出。

独立复现建立新 clone，覆盖当前有效源码快照（包括未提交和未跟踪实现），单独重建产物。缓存与冻结样本是显式复用输入，构建产物不复用；逐字段比较情景、完整比较诊断、关联、距离和出生节点身份，允许构建哈希派生的版本前缀变化。

手动子集通过不能冒充完整 M2 通过。最终完成判定应运行第 1 节完整门禁，检查 `report.overall_status == "passed"`、`traceability.status == "pass"`、`report-audit.audit_status == "passed"`，并核对实际执行记录。
