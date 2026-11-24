# M2 验证协议与证据契约

版本：`m2.report.v1` / `m2.traceability.v1`。对应 FR-001–FR-021（21 项）与 SC-001–SC-008（8 项）。

## 1. 验证命令清单（`validation/m2/`）

所有命令从仓库根目录执行，退码遵循 M1 约定：`0` 通过，`1` 校验或计算失败，`2` 缺少前置或参数错误。

| 脚本 | 参数与职责 |
| --- | --- |
| `setup_env.sh` | `[--check]`；核验/准备 M0 数据与参考 venv、M2 OM 转换 venv（`requirements-om.lock`：omfiles 1.2.0、numpy 2.5.4、numcodecs 0.17.0），下载并按官方 SHA256SUMS/manifest 核对 DuckOMO `v0.3.5-rc.1` 与 `v0.3.4` 发布包 |
| `build_extension.sh` | `[--output-dir DIR]`；默认 `/tmp/ensabc-m2-extension-target`，以 `--locked` 构建 `ensabc-core`（含 `m1_runner`、`m2_runner`）与 `ensabc_m0_extension.duckdb_extension` |
| `prepare_om_sample.py` | `--source-manifest validation/m2/fixtures/wb2_8run_source.json --output-dir DIR [--cache-dir DIR]`；从 WB2 恢复或从已核对缓存读取 8 批源块，解码 `<f4` 载荷，写成 8 个 FPX 无损 OM 文件，独立 reader 读回核验并输出 `om_sample_manifest.json` |
| `verify_om_paths.py` | `--run-dir DIR --stage cells\|replay --duckomo-target PATH --duckomo-baseline PATH --extension PATH`；stage 必填，cells 只做三路径逐值和输入负例，replay 消费已通过 cells 证据及真实回放入口做双版本完整对照（SC-001、SC-003） |
| `verify_matching_core.py` | `--run-dir DIR`；运行 Python `Fraction` 独立穷举 oracle 与 `m2_runner` 对照（覆盖 FR-016 全部合成类别 + 固定种子随机用例、2,000 次编号置换不变性、权重缩放、全部 core 错误码） |
| `verify_sql_and_history.py` | `--run-dir DIR --scope synthetic\|real\|all --extension PATH [--duckomo PATH]`；synthetic 不需要 DuckOMO，real/all 消费 cells 证据；验证 `ens_dispersion` / `ens_match_scenarios` 类型与错误恢复、跨向量批量边界、五表 DDL、多批链（含缺批与单批失败断链）、3 进程重复重跑、同版本幂等、同版本冲突拒绝、中途故障注入事务回滚 0 残留（SC-002–SC-005） |
| `run_case_and_sensitivity.py` | `--run-dir DIR --extension PATH --duckomo PATH`；执行真实 8 批主回放（`k=3, c=1.0`）、比例系数敏感性（6 个 $c$）、情景数敏感性（`k=2, 3, 4`）、匹配级扰动（$\varepsilon=0.01$，100 种子）与成员级扰动（$\sigma=0.001\text{ K}$，20 种子 + $\sigma=0.01\text{ K}$ 压力档），核对远离边界证书 0 翻转（FR-017、SC-006） |
| `verify_minimal_runtime.sh` | `--run-dir DIR --extension PATH --duckomo PATH`；在 `--network none` 且无 Python/参考工具的最小容器中，用 `m2_runner` 及 `DuckDB CLI + DuckOMO + ensabc` 从本地冻结 OM 文件完成完整 8 批回放、结果导出与同连接错误恢复（FR-018、SC-007） |
| `measure_pipeline.py` | `--run-dir DIR --extension PATH --duckomo PATH`；测量①OM 转换、②OM 读取、③单批聚类+离散、④两批匹配、⑤身份分配与五表保存五个环节的冷启动前缀进程耗时/峰值 RSS 与已加载语句耗时（每项 $\ge 3$ 次，FR-020） |
| `run_m1_regression.py` | `--run-dir DIR`；在当前 M2 代码基线上调用 `validation/m1/run_m1_gate.py` 完整重跑 M1 门禁并核对 `passed`（FR-019、SC-008） |
| `reproduce_independently.py` | 父模式 `--run-dir DIR [--work-dir DIR]`；子模式必填 `--reproduction-child --output-dir DIR --source-manifest PATH --sample-manifest PATH --experiment-config PATH --duckomo-target PATH --duckomo-baseline PATH`；独立 `--self-check`。全新 clone 外置输入/build/cache/输出，重建并执行固定子集，不调用完整 gate/traceability/audit/递归复现；生成 scope=reproduction_subset 并保留子证据副本（SC-008） |
| `traceability.py` | `--run-dir DIR`；执行全部 FR-001..FR-021 与 SC-001..SC-008 的可执行断言 |
| `verify_report.py` | `--run-dir DIR` 或 `--self-check`；无循环独立审计，重算所有引用文件 SHA-256、重新推导总体判定并执行坏证据反例自检 |
| `run_m2_gate.py` | `--run-id ID [--cache-dir DIR]`；按固定顺序编排上述全部步骤，生成 `artifacts/m2/<run-id>/report.json` |

### 1.1 三路径与双版本的唯一阶段顺序

`prepare_om_sample → verify_om_paths --stage cells → verify_sql_and_history --scope real → verify_om_paths --stage replay`。

cells 写 `three-path-comparison.json` 与 `negative-gate-checks.json`，不调用历史 verifier，也不生成双版本完整回放通过结论。replay 只消费已完成的 cells/真实入口，写 `duckomo-version-comparison.json`，不得反向补跑或覆盖 cells。T035 与 T037 分别拥有这两个 stage；门禁及复现子集使用同一顺序。缺前置返回 2/not_run，hash 或实际比较不符返回 1/fail。

### 1.2 重复、重建与扰动的身份比较

- **相同产物精确重复**：同一输入/配置/宿主/扩展二进制，三个独立 PID 的情景、距离、关联、run_key 与 branch_id 完全相同。
- **相同源码独立重建**：clone 的有效源码、锁文件和构建配置必须相同；实际二进制 hash 各自记录和核对，不要求二进制逐字节可复现。按各自实际 hash 独立重算 run_key，再以 `(birth_ordinal, birth_canonical_rank)` 比较所有节点的身份链；数值、分区、端点、出生/继承关系与歧义仍须一致。
- **两版 DuckOMO 对照**：先验证两个钉住发布身份，除经核实的版本元数据及由其派生的 run_key/branch 前缀外，不得忽略任何数值或关系差异。
- **扰动版本**：保留原始身份，以冻结的评估节点映射比较出生/继承关系；输入变化可能改变版本键，不能把 hash 前缀变化算作天气身份翻转。映射歧义、上游翻转分别报告，不任意重编号抹平差异。
- 所有归一均验证完整原始前缀确实来自相应 run_key，再生成比较投影。正例仅改变合法构建/版本前缀可通过；错误出生节点、身份交换、继承端点或伪造前缀必须拒绝。

### 1.3 发布与稳定性断言

合成验证须区分可保存的 missing/已分类算法 failed 与阻止发布的元数据/非法输入/匹配/未知执行错误；每种失败后同连接恢复，发布失败五表差集为零。稳定性按 [numerics.md](numerics.md) 的保守证书、q=0/无替代/映射歧义及整链出生和上游规则检查；不能以局部 pair 证书证明整链身份稳定。

## 2. 产物目录结构（`artifacts/m2/<run-id>/`）

```text
artifacts/m2/<run-id>/
  environment.json
  source-identity.json
  source-manifest.json
  sample/
    om_sample_manifest.json
    om/batch_0..7.om              # 受限派生数据，不入库
    payloads/batch_0..7.bin
  om-verification/
    three-path-comparison.json
    duckomo-version-comparison.json
    negative-gate-checks.json
  core/
    synthetic-oracle.json
    permutation-invariance.json
    error-cases.json
  sql-history/
    sql-contract.json
    replay-runs.duckdb
    three-process-repro.json
    transaction-atomicity.json
  case-study/
    main-replay-k3-c1.0.json
    sensitivity-coefficients.json
    sensitivity-cluster-counts.json
    perturbation-matching-level.json
    perturbation-member-level.json
  minimal-runtime.json
  measurements.json
  m1-regression/
    summary.json
  independent-reproduction.json
  reproduction/child/
  steps.json
  traceability.json
  report.json
  report-audit.json
```

## 3. FR-001–FR-021 与 SC-001–SC-008 追溯映射表

| 需求 ID | 关键可执行断言与目标证据文件 | 关联 SC |
| --- | --- | --- |
| FR-001 | 8 批计划序列、同配置独立 `ens_cluster`、`k=3` 主回放与 `k=2,4` 对照（`case-study/main-replay-k3-c1.0.json`） | SC-002, SC-006 |
| FR-002 | 准备门禁验证模型/变量/单位/9 点顺序/`valid_time`/递增 `init_time`/时效恒等式，不一致负例 100% 拒绝（`om-verification/negative-gate-checks.json`） | SC-001, SC-005 |
| FR-003 | `cos_lat_center_v1` 权重、加权 RMSE 手算与 2 的幂/一般常数缩放不变性、仅用 `mean_features`（`core/synthetic-oracle.json`） | SC-004 |
| FR-004 | 总体口径离散 $s$、$S=(s_1+s_2)/2$、$\tau=c\cdot S$、门槛内最大条数再最小精确距离和、`[[0,4],[4,6]]` $\tau=5$ 得两条距离 4 关联（`core/synthetic-oracle.json`） | SC-004 |
| FR-005 | 每对返回完整距离矩阵、比值矩阵、可行矩阵、选中关联、两侧未关联及原因、零关联正常成功（`core/synthetic-oracle.json`, `sql-history/sql-contract.json`） | SC-002, SC-004 |
| FR-006 | 每条选中边可替代程度三态（`tied`/`same_count`/`count_drop`）、完全并列 `ambiguous=true`、2,000 次编号置换不变性（`core/permutation-invariance.json`） | SC-004 |
| FR-007 | 首批新身份、关联继承、歧义继承保留标记、未关联新身份、断链不复活、每节点恰一身份、同批唯一（`sql-history/sql-contract.json`） | SC-002, SC-003 |
| FR-008 | 12h 计划序列、缺批（`missing`）与单批聚类失败（`failed`）处断链并标 `previous_planned_batch_unavailable`（`sql-history/sql-contract.json`） | SC-002, SC-005 |
| FR-009 | 3 进程重跑 100% 相同、同 `run_key` 同内容幂等、同 `run_key` 篡改内容报 `RUN_VERSION_CONFLICT`、新参数生成新 `run_key` 保留旧版（`sql-history/three-process-repro.json`, `transaction-atomicity.json`） | SC-003, SC-005 |
| FR-010 | 五张历史表约束、4 条事务守卫、中途故障注入回滚后 0 残留（`sql-history/transaction-atomicity.json`） | SC-002, SC-005 |
| FR-011 | `ens_dispersion` 与 `ens_match_scenarios` 无状态签名、`ens_cluster` 保持不变、core 与 SQL 逐字段一致（`sql-history/sql-contract.json`） | SC-002, SC-004 |
| FR-012 | NULL/重复 ID/ragged/NaN/Inf/非法权重/零离散/溢出全类别拒绝，同会话 `valid -> invalid -> valid` 恢复，跨 chunk 混合查询失败（`core/error-cases.json`, `sql-history/sql-contract.json`） | SC-005 |
| FR-013 | WB2 8 批 generation/字节/哈希核对、50×9 有限值、清单完整（`sample/om_sample_manifest.json`） | SC-001 |
| FR-014 | `fpx_xor_2d` 无损编码、源解码 vs `omfiles` vs DuckOMO 3,600 值 0 位差、`kind="real_ensemble_converted_to_om"`（`om-verification/three-path-comparison.json`） | SC-001 |
| FR-015 | DuckOMO `v0.3.5-rc.1` 与 `v0.3.4` 发布包哈希核对、隔离 `extension_directory`、同连接加载、双版本矩阵与追踪结果 100% 一致（`om-verification/duckomo-version-comparison.json`） | SC-001, SC-003 |
| FR-016 | 14 类合成与边界用例全部通过并与独立穷举 oracle 100% 一致（`core/synthetic-oracle.json`, `sql-history/sql-contract.json`） | SC-004, SC-005 |
| FR-017 | 真实 8 批回放报告、6 个比例系数、$k=2/3/4$、匹配级与成员级扰动分离报告、有保守证书的 pair 关联 0 翻转、身份按出生/上游规则单独核对、接近边界翻转和评估歧义 100% 记录（`case-study/*.json`） | SC-006 |
| FR-018 | 断网无参考工具最小容器中 `m2_runner` 与 `DuckDB + DuckOMO + ensabc` 完整回放及错误恢复通过（`minimal-runtime.json`） | SC-007 |
| FR-019 | 环境与版本记录、M1 门禁在 M2 基线重跑 `overall=passed`（`environment.json`, `m1-regression/summary.json`） | SC-008 |
| FR-020 | 5 个环节冷启动前缀与已加载耗时/峰值 RSS 测量（各 $\ge 3$ 次，不可分离项明确标记，`measurements.json`） | SC-008 |
| FR-021 | 可执行示例、quickstart、全新 clone 独立复现、`report.json` + `traceability.json` + `report-audit.json` 全部通过 | SC-008 |
