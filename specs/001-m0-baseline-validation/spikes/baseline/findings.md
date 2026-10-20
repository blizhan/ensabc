# 数值参考基线与 Rust 依赖选型 Spike

日期：2026-10-20

范围：只验证教程的核心数值路径、固定比较规则及少量输入策略；Rust 部分仅做文档和依赖特性只读检查，不实现聚类。依据 `spec.md` 的 FR-004/005、SC-002 和合成输入要求。本结果是合成参考证据，不代表真实样本门禁或 M0 通过。

## Python 参考配置

教程将成员作为矩阵行、区域网格点展平成特征列，然后执行 `StandardScaler -> PCA(2) -> KMeans(3)`。本 spike 锁定：

- Python 3.13.13，Linux aarch64；NumPy 2.5.4、SciPy 1.18.1、scikit-learn 1.9.1。
- `StandardScaler(with_mean=True, with_std=True)`；`PCA(n_components=2, svd_solver="full", whiten=False)`。
- `KMeans(n_clusters=3, init="k-means++", n_init=10, random_state=0, algorithm="lloyd", max_iter=300, tol=1e-4)`；数值库线程限制为 1。
- 运行依赖版本见 `requirements-lock.txt`。该锁定精确到包版本，未包含 wheel 哈希或跨平台二进制锁。

**与 notebook 的 `n_init="auto"` 差异**：scikit-learn 1.9 文档规定，`auto` 配合 `k-means++` 执行一次初始化；本 spike 明确设为 10 次初始化并取较低 inertia 的运行。这是为固定参考行为、降低单次初始化落入较差局部解的机会而选择的新参考配置，不是旧 notebook 在 `auto` 下的字节级复现。输入、配置和脚本哈希都会进入证据身份。

## 比较与输入策略

- 数值比较严格沿用 FR-005：`abs(actual - reference) <= 1e-8 + 1e-6 * abs(reference)`。实现使用 `np.allclose(actual, reference, atol=1e-8, rtol=1e-6)`，actual 位于第一参数，reference 位于第二参数。
- 标签按成员首次出现顺序规范化后要求完全相同；PCA 每个分量以绝对值最大的 loading 为正，并同步翻转对应 score。分量顺序不变；不对主成分轴置换或退化子空间旋转做等价匹配。
- 输入须为非空、矩形、有限的二维数值矩阵；成员 ID 须唯一且与行数相符；成员数至少为 3；完全相同的特征行可以保留为不同成员，但不同特征行少于 3 时拒绝；StandardScaler 后矩阵秩小于 2 时拒绝。
- 合成矩阵为 9 个成员 × 3 个特征，第三列为常量；有三组完全重复的特征行，重复成员仍各自保留。输入与配置均有 SHA-256。

## 实际结果

当前权威输出为 `synthetic-result-v2.json`，证据 ID：`302514f138654ea9d89d5fb9dd864e4df6eb9ca3ff00d39c69463cb693c328aa`。

- 3 个独立子进程 PID 不同；每个分区均为 `[0,0,0,1,1,1,2,2,2]`，每个输出的可比较数值最大绝对差均为 0。
- 比较器自检通过：标签编号置换与 PCA 符号翻转视为等价；把 `member-02` 移入另一簇会失败；数值扰动会失败。
- 五类非法输入均被拒绝：空矩阵、ragged、非有限值、标准化后秩不足、唯一特征行少于 `k`。重复行但唯一行不少于 `k` 的合法矩阵被接受。
- 旧的 `synthetic-result.json` 保留为修改比较器前的首轮记录；应以 v2 的 actual/reference 参数方向和成员错簇负例结果为准。

## Rust 候选（只读）

| 候选 | 组合与匹配点 | C/BLAS 边界 | 评估 |
|---|---|---|---|
| A | `nalgebra` 0.35.0 做缩放后的矩阵与 SVD；本地实现最小 Lloyd KMeans。 | 默认特性使用 Rust `matrixmultiply`；默认路径不要求外部 C/BLAS。 | 部件少，能明确控制 sklearn 对照所需的初始化、10 次重启、随机数、空簇和停止规则；代价是需维护并验证本地 KMeans。 |
| B | `ndarray` + `linfa-linalg` 0.2.1 的 SVD + `linfa-clustering` 0.8.1 的 KMeans。 | 文档描述为纯 Rust；`linfa-linalg` 不依赖外部 BLAS/LAPACK。`linfa-clustering` 的 `blas` / `ndarray-linalg` 是可选路径，采用时须检查完整 Cargo feature tree。 | KMeans 参数可设初始化、`n_runs`、容差和迭代上限；文档默认值包含 k-means++、10 runs、300 iterations、1e-4。其收敛描述按质心位移判定，须验证与 sklearn 的停止语义、随机流及结果差异。 |

**M1 建议**：先以候选 A 做窄原型，目标是显式匹配已锁定的 Python 比较契约，而非追求通用数值框架。StandardScaler 可直接用简单 Rust 数组循环；用 `nalgebra` 完成 SVD，KMeans 只实现此契约要求的 `k-means++ / n_init=10 / seed` 与明确的退化处理。若不希望维护聚类实现，再评估候选 B，并以同一矩阵对初始化、停止条件、空簇处理和最终分区做对照。两个候选都没有在本 spike 中构建或运行。

Rust 官方资料（本 spike 实际查阅的页面）：[nalgebra 特性](https://docs.rs/crate/nalgebra/latest/features)、[nalgebra Cargo 清单源码](https://docs.rs/crate/nalgebra/latest/source/Cargo.toml.orig)、[linfa-clustering `KMeansParams`](https://docs.rs/linfa-clustering/0.8.1/linfa_clustering/struct.KMeansParams.html)、[linfa-clustering 特性](https://docs.rs/crate/linfa-clustering/0.8.1/features)、[linfa-linalg API](https://docs.rs/linfa-linalg/0.2.1/linfa_linalg/)。

## 建议输出与复跑

对本 spike 这种很小的固定矩阵，JSON 足够直观：保存 `sample_kind`、成员 ID 顺序、矩阵、shape、配置、输入/配置 SHA、缩放结果、PCA 分量与 scores、规范化标签及依赖版本。真实样本仍需按规格清单记录来源、时间语义、特征键、单位和恢复方式；此 JSON 不作为通用天气文件格式。

从仓库根目录复跑：

```sh
python3.13 -m venv /tmp/ensabc-m0-baseline
/tmp/ensabc-m0-baseline/bin/python -m pip install -r specs/001-m0-baseline-validation/spikes/baseline/requirements-lock.txt
/tmp/ensabc-m0-baseline/bin/python specs/001-m0-baseline-validation/spikes/baseline/run_baseline.py --output /tmp/ensabc-m0-baseline/replay.json
```

相同证据身份的已存在输出会被比较并保留；不同输入、配置或脚本哈希不会静默覆盖。官方资料检查截至 2026-10-20：scikit-learn 1.9.1 `KMeans` API；nalgebra 0.35.0 特性与清单；linfa-clustering 0.8.1 `KMeansParams`/特性；linfa-linalg 0.2.1 API 与依赖说明。

## 未覆盖与未知

- 本 spike 没有真实天气样本、来源恢复、DuckDB 扩展加载/往返、并发隔离、1/10/100 任务性能或内存测量；这些仍是各自规格门禁，合成结果不能替代。
- 未保存或比较 KMeans 中心、`n_iter_`/迭代轨迹；未覆盖重复特征造成的主成分退化子空间、接近簇边界的成员或不同平台 BLAS/浮点差异。正式验证应按需求增加这些证据；当前不据此放宽 FR-005。
- Rust 选型只检查 crate 文档，不含工作区实际 feature tree、编译、目标平台链接或与 Python 的数值对照。M1 开始时需按仓库锁文件复核 Rust 版本和是否意外启用 C/BLAS 后端。
