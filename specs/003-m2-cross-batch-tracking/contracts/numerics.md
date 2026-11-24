# M2 数值、距离与匹配优化契约

版本：`m2.match.v1` / `m2.calibration.v1`。对应 FR-003–FR-006、FR-016–FR-017、SC-004、SC-006。

## 1. 精确浮点求和（Shewchuk Expansion）

记 $\oplus, \ominus, \otimes, \oslash$ 为 IEEE-754 binary64（`f64`）默认 round-to-nearest-even 运算。所有格点求和、成员求和与匹配目标距离和均使用 Shewchuk 非重叠浮点展开（`Grow-Expansion` / `Expansion-Sum`，与 CPython `math.fsum` 一致）：
- 输入存在非有限值或展开累加溢出时，立即返回 `NUMERICAL_FAILURE`；
- 将展开转换为单个 `f64` 时只做一次正确舍入（记为 $\operatorname{fsum}$）；
- 比较两个距离和 $\Sigma_1$ 与 $\Sigma_2$ 时，直接比较两个精确展开的差值符号，结果与求和顺序、情景排列顺序完全无关。

## 2. 空间权重、加权 RMSE 与集合离散（FR-003、FR-004）

### 2.1 冻结空间权重 `cos_lat_center_v1`

对规则经纬网格，权重定义为 $w_g = \cos(\phi_g \otimes (\pi \oslash 180))$。M2 的 3×3 欧洲区域（纬度 `[42.1875, 47.8125, 53.4375]`，经度 `[5.625, 11.25, 16.875]`，纬度优先、经度次序，共 9 点）冻结为以下精确 `f64` 位值（在 `validation/m2/fixtures/spatial_weights_eu3x3.json` 中同时记录十进制与 hex）：

| `point_index` | 纬度 (°N) | 经度 (°E) | 权重十进制（round-trip） | IEEE-754 hex |
| --- | --- | --- | --- | --- |
| `0, 1, 2` | `42.1875` | `5.625, 11.25, 16.875` | `0.7409511253549591` | `0x1.7b5df226aafafp-1` |
| `3, 4, 5` | `47.8125` | `5.625, 11.25, 16.875` | `0.6715589548470183` | `0x1.57d69348cec9fp-1` |
| `6, 7, 8` | `53.4375` | `5.625, 11.25, 16.875` | `0.5956993044924334` | `0x1.30ff7fce17035p-1` |

### 2.2 情景物理均值场距离（加权 RMSE）

给定前批情景物理均值场 $a \in \mathbb{R}^p$、后批情景物理均值场 $b \in \mathbb{R}^p$ 与权重 $w \in \mathbb{R}^p$：

$$W = \operatorname{fsum}_{g=0}^{p-1}(w_g), \quad \delta_g = a_g \ominus b_g, \quad d(a, b) = \sqrt{\operatorname{fsum}_{g=0}^{p-1}\bigl(w_g \otimes (\delta_g \otimes \delta_g)\bigr) \oslash W}$$

- **权重整体缩放验收口径**：仅对冻结的合法缩放用例验收，缩放后权重须有效，相关中间量不得溢出、下溢或变为非有限值。对满足精确缩放条件的 2 的幂用例（如 `1024.0`），距离和离散要求逐位一致；一般正数缩放用例要求 `rtol=1e-15`，零值另作精确检查。只有门槛和最优组合余量覆盖缩放导致的数值误差时才要求决策一致；边界用例允许舍入翻转但必须记录，不以容差改写 `d <= τ` 或精确并列规则。

### 2.3 单批集合离散与相邻批次门槛换算

给定单批 $N$ 个成员的原始物理特征矩阵 $X \in \mathbb{R}^{N \times p}$（总体口径 $1/N$）：

$$m_g = \operatorname{fsum}_{n=0}^{N-1}(X_{n,g}) \oslash N, \quad e_{n,g} = X_{n,g} \ominus m_g$$

$$Q = \operatorname{fsum}_{(n,g)\in\{0..N-1\}\times\{0..p-1\}}\bigl(w_g \otimes (e_{n,g} \otimes e_{n,g})\bigr), \quad s = \sqrt{(Q \oslash N) \oslash W}$$

全部 `(member, point)` 平方项进入同一个精确展开，完成后仅舍入一次得到 Q；禁止先把各成员格点和舍入为 f64 再累加。集合平均各列独立 fsum；乘法、除法和平方根按上述 binary64 顺序执行。Rust 与独立 Python 参考均须覆盖平铺累加与嵌套舍入产生不同末位的反例。

对相邻两批的离散 $s_{\text{prev}}, s_{\text{curr}} > 0$ 与比例系数 $c \ge 0$：

$$S = (s_{\text{prev}} \oplus s_{\text{curr}}) \oslash 2, \quad \tau = c \otimes S, \quad r_{i,j} = d_{i,j} \oslash S$$

- **唯一可行性判据**：候选边 $(i, j)$ 可行当且仅当 **`d_{i,j} <= τ`**（含等号）。`r_{i,j}` 仅用于报告展示，不得用 `r_{i,j} <= c` 替代 `d_{i,j} <= τ`。

## 3. 字典序最优匹配、决胜与可替代程度（FR-004–FR-006）

### 3.1 优化目标

令可行边集 $E = \{(i,j) \in \{0..m-1\} \times \{0..n-1\} : d_{i,j} \le \tau\}$。合法匹配 $M \subseteq E$ 满足每个前批情景 $i$ 与每个后批情景 $j$ 在 $M$ 中至多出现一次。在全部合法匹配中按字典序选优：
1. **首要目标**：关联条数 $|M|$ 最大；
2. **次要目标**：精确距离和 $\Sigma(M) = \sum_{(i,j)\in M} d_{i,j}$（Shewchuk 展开精确值）最小。

### 3.2 与编号无关的规范决胜规则

1. **情景规范键**：对每个情景的 `mean_features` 向量，将每个元素 `-0.0` 归一为 `+0.0` 后，按格点顺序 `0..p-1` 使用 `f64::total_cmp` 字典序比较。
2. **单侧规范序**：前批情景按规范键升序排列（若两个情景的 `mean_features` 逐位完全相同，再按输入 `cluster_id` 升序排列），得到规范序号 $0..m-1$；后批同理得到规范序号 $0..n-1$。
3. **伙伴向量字典序**：对任一最优匹配 $M$，构造长度为 $m$ 的伙伴向量 $\pi(M)$，其中第 $r$ 项为前批第 $r$ 个规范情景所关联的后批规范序号，若未关联则记为 $n$（即“未关联”排在所有关联列之后）。在全部达到最优 `(q*, Σ*)` 的匹配中，选择使 $\pi(M)$ **字典序最小**的唯一匹配 $M^*$。

### 3.3 完全并列与可替代程度

- **完全并列计数 `optimal_solution_count`**：达到 `(q*, Σ*)`（精确展开相等）的合法匹配总数。`optimal_solution_count > 1` 时顶层 `ambiguous = true`。
- **单边可替代程度**：对每条选中边 $e \in M^*$，在 $E \setminus \{e\}$ 上重新求最优匹配，得到最优条数 $q'$ 与精确距离和 $\Sigma'$：
  - 若 $q' == q^*$ 且 $\Sigma' == \Sigma^*$（精确相等）：`alternative_status = Tied`，`alternative_distance_increase = Some(0.0)`，`ambiguous = true`；
  - 若 $q' == q^*$ 且 $\Sigma' > \Sigma^*$：`alternative_status = SameCount`，`alternative_distance_increase = Some(round(Σ' - Σ*))`，`ambiguous = false`；
  - 若 $q' < q^*$（必有 $q' = q^* - 1$）：`alternative_status = CountDrop`，`alternative_distance_increase = None`，`ambiguous = false`。

## 4. 冻结的实验标定参数与稳定性证书（FR-017、SC-006）

以下参数保存在 `validation/m2/fixtures/tracking_experiment_config.json`，在正式验收运行前冻结：

- **主门槛比例系数**：`c = 1.0`；
- **门槛比例系数敏感性网格**：`[0.5, 0.75, 1.0, 1.25, 1.5, 2.0]`；
- **情景数设定**：主结果 `k = 3`（沿用 `validation/m1/fixtures/native_config.json`），敏感性对照 `k = 2` 与 `k = 4`；
- **匹配级轻微扰动**：$\varepsilon = 0.01$（每个情景平均场加权 RMS 位移为 $0.01 \cdot S$），每对批次 100 个固定随机方向（固定种子 `20261029`）；
- **成员级轻微扰动**：每个成员格点值加独立 $\mathcal{N}(0, \sigma^2)$，主幅度 $\sigma = 0.001\text{ K}$（20 个固定种子 `4242`），压力对照档 $\sigma = 0.01\text{ K}$；
- **远离边界证书（充分条件）**：设受扰样本相对基线的单情景平均场最大加权 RMS 位移为 $\varepsilon_{\text{abs}}$，离散尺度变化为 $\Delta S = |S' - S|$。当且仅当同时满足：
  1. $\min_{i,j}|d_{i,j} - \tau| > 2\varepsilon_{\text{abs}} + c\Delta S$；
  2. 所有 `SameCount` 选中边的最小增量 $\min_e \Delta\Sigma_e > 4 q^* \varepsilon_{\text{abs}}$；
  3. `optimal_solution_count == 1`；
  在覆盖浮点计算误差的保守界下仍满足以上条件，才将该受扰对判为**远离边界**；否则判为**接近边界**（记录翻转、门槛余量与最优切换余量）。不得仅凭舍入后数值严格大于就宣称实数意义的证明。

### 4.1 证书的边界与身份比较

- 无同条数替代时，最优余量状态为 `no_same_count_alternative`，该条件不构成限制；不得把输出占位 0 当作真实余量。q=0 时验证所有候选仍不可行即可，不比较不存在的选中边。
- 成员级以最大成员重叠的一对一映射作评估；映射并列标为 `evaluation_mapping_ambiguous`，情景数不等或单批失败分别报告，不纳入已证明稳定组。
- 验证器以向外舍入区间包围均值差、加权平方项、求和、除法、平方根、门槛及目标差；可使用精确有理数表示 binary64 操作数并用有理平方根上下界实现。判据使用左侧下界大于右侧上界；溢出、下溢、无法形成有限保守界时归入接近边界或数值失败，不给予证书。实现与自检属于 T039/T043；生产匹配仍遵循原 binary64 公式。
- 证书只约束当前 pair 映射后的关联集合。逻辑身份以 `(birth_ordinal, birth_canonical_rank)` 比较，只有出生对应和全部上游继承关系均一致时才要求身份稳定。扰动回放的 run_key 前缀可变；保留原始身份与映射依据，不通过任意重新编号隐藏身份交换。
- 远离组实际关联翻转必须为 0；接近边界、映射歧义、上游翻转和单批失败分别计数。合成验证须保证可验证的远离/接近两组均非空。
