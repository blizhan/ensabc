# M3 多对多演化拓扑、三族正交指标与稳定性证书契约

版本：`m3.evolve.v1` / `m3.uncertainty.v1` / `m3.certificate.v1`。对应 FR-002–FR-008、FR-013、FR-015–FR-016、SC-001–SC-005。精确浮点展开规则与空间加权 RMSE / 集合离散公式完全沿用 [M2 数值契约](../../003-m2-cross-batch-tracking/contracts/numerics.md)（`cos_lat_center_v1`、Shewchuk `Expansion`、单层平铺求和一次舍入）。

---

## 1. 多对多演化图构造与星形分解定理（FR-002、FR-003、FR-004）

### 1.1 主干与未入选集合划分
设前批情景集 $V_{\text{prev}} = \{0..m-1\}$（成员占比 $p_i \in (0, 1]$，$\operatorname{fsum}_i p_i = 1$），后批情景集 $V_{\text{curr}} = \{0..n-1\}$（成员占比 $q_j \in (0, 1]$，$\operatorname{fsum}_j q_j = 1$），加权 RMSE 矩阵 $d_{i,j}$，生效门槛 $\tau \ge 0$。令可行边集 $E_\tau = \{(i, j) : d_{i,j} \le \tau\}$，$M^* \subseteq E_\tau$ 为 M2 字典序全局最优一对一匹配（先最大化 $|M|$，再最小化精确距离和 $\Sigma(M)$，并列按 `canonical_scenario_order` 伙伴向量字典序最小决胜）。

将两侧未进入 $M^*$ 的节点划分为：
- **门槛内竞争落选集**：
  $$U_{\text{prev}}^{\text{comp}} = \{i \notin V_{\text{prev}}(M^*) : \exists j,\; d_{i,j} \le \tau\}, \quad U_{\text{curr}}^{\text{comp}} = \{j \notin V_{\text{curr}}(M^*) : \exists i,\; d_{i,j} \le \tau\}$$
- **门槛外无候选集**：
  $$U_{\text{prev}}^{\text{none}} = \{i \notin V_{\text{prev}}(M^*) : \forall j,\; d_{i,j} > \tau\}, \quad U_{\text{curr}}^{\text{none}} = \{j \notin V_{\text{curr}}(M^*) : \forall i,\; d_{i,j} > \tau\}$$

### 1.2 单近邻可行次级归属规则（`nearest_feasible_v1`）
1. **主干延续边集** $E_{\text{primary}} = M^*$（角色 `primary`）。
2. **次级分裂边集** $E_{\text{split}}$（角色 `secondary_split`）：对每个 $j \in U_{\text{curr}}^{\text{comp}}$，令 $F_{\text{in}}(j) = \{i \in V_{\text{prev}} : d_{i,j} \le \tau\}$，$d_{(1)}(j) = \min_{i \in F_{\text{in}}(j)} d_{i,j}$（按 `f64::total_cmp` 比较，`-0.0` 归一为 `+0.0`），并列最优点集 $T_{\text{in}}(j) = \{i \in F_{\text{in}}(j) : d_{i,j} == d_{(1)}(j)\}$。选取 $T_{\text{in}}(j)$ 中按前批 `canonical_scenario_order` 排名最小者 $i^*(j)$，加入唯一一条边 $(i^*(j), j)$。若 $|T_{\text{in}}(j)| > 1$，该边标记 `ambiguous_tie = true`。
3. **次级合并边集** $E_{\text{merge}}$（角色 `secondary_merge`）：对每个 $i \in U_{\text{prev}}^{\text{comp}}$，令 $F_{\text{out}}(i) = \{j \in V_{\text{curr}} : d_{i,j} \le \tau\}$，$d_{(1)}(i) = \min_{j \in F_{\text{out}}(i)} d_{i,j}$，$T_{\text{out}}(i) = \{j \in F_{\text{out}}(i) : d_{i,j} == d_{(1)}(i)\}$。选取 $T_{\text{out}}(i)$ 中按后批 `canonical_scenario_order` 排名最小者 $j^*(i)$，加入唯一一条边 $(i, j^*(i))$。若 $|T_{\text{out}}(i)| > 1$，该边标记 `ambiguous_tie = true`。
4. **总演化图**：$G_{\text{evol}} = (V_{\text{prev}} \sqcup V_{\text{curr}}, E_{\text{evol}})$，其中 $E_{\text{evol}} = E_{\text{primary}} \sqcup E_{\text{split}} \sqcup E_{\text{merge}}$。

### 1.3 四条数学结构定理与星形五分类完备性
由 Berge 最大匹配增广路定理（$G_\tau$ 中不存在关于 $M^*$ 的增广路）直接导出以下不变量，实现与测试均须对其实行断言：
- **定理 1（边角色两两互斥，无平行边）**：任何 $i \notin V_{\text{prev}}(M^*)$ 与 $j \notin V_{\text{curr}}(M^*)$ 必有 $d_{i,j} > \tau$。因此 $i^*(j) \in V_{\text{prev}}(M^*)$ 且 $j^*(i) \in V_{\text{curr}}(M^*)$，从而 $E_{\text{primary}} \subseteq V_{\text{prev}}(M^*) \times V_{\text{curr}}(M^*)$、$E_{\text{split}} \subseteq V_{\text{prev}}(M^*) \times U_{\text{curr}}^{\text{comp}}$、$E_{\text{merge}} \subseteq U_{\text{prev}}^{\text{comp}} \times V_{\text{curr}}(M^*)$ 两两互不相交。
- **定理 2（同一主干边上分裂与合并互斥）**：对任意 $(i, j) \in M^*$，不可能同时存在 $c' \in U_{\text{curr}}^{\text{comp}}$ 使 $(i, c') \in E_\tau$ 且存在 $p' \in U_{\text{prev}}^{\text{comp}}$ 使 $(p', j) \in E_\tau$（否则 $p' - j - i - c'$ 为长度 3 的增广路，使匹配条数增至 $|M^*| + 1$）。
- **定理 3（次级节点强制入主干必保最大基数）**：对任意 $u \in U^{\text{comp}}$，因其在 $G_\tau$ 中至少邻接一个 $v \in V(M^*)$，将 $u$ 替换进 $v$ 的主干配对必得到基数仍为 $|M^*|$ 的合法匹配。故强制包含 $u$ 的位掩码 DP 必有可行解且最优条数恰等于 $|M^*|$，次级晋升代价差 $\Delta\Sigma_{\text{prom}}(u) = \operatorname{round}(\Sigma_{\text{force}}(u) - \Sigma^*) \ge 0$ 恒良定义且有限。
- **定理 4（非活跃可行交叉边无关性）**：任意已可行但未入选 $E_{\text{evol}}$ 的单元 $(i, j) \in E_\tau \setminus E_{\text{evol}}$ 从可行集中移除时（在无精确并列的前提下），$M^*$、$E_{\text{split}}$ 与 $E_{\text{merge}}$ 100% 保持不变。

**推论（星形连通分量唯一事件划分）**：$G_{\text{evol}}$ 的每个连通分量 $C = (P_C, Q_C)$ 必属于且仅属于以下五类之一：
1. `continuation`：$|P_C| = 1, |Q_C| = 1$（1 条 `primary` 边）；
2. `split`：$|P_C| = 1, |Q_C| = 1 + k \ge 2$（1 条 `primary` 边 + $k$ 条同源 `secondary_split` 边）；
3. `merge`：$|P_C| = 1 + k \ge 2, |Q_C| = 1$（1 条 `primary` 边 + $k$ 条同汇 `secondary_merge` 边）；
4. `disappearance`：$|P_C| = 1, |Q_C| = 0$（$i \in U_{\text{prev}}^{\text{none}}$，0 条边）；
5. `emergence`：$|P_C| = 0, |Q_C| = 1$（$j \in U_{\text{curr}}^{\text{none}}$，0 条边）。

---

## 2. 算法流向归因与几何贴近强度（`star_min_mass_proportional_v1`）

为保证 Rust、DuckDB SQL 与 Python `Fraction` oracle 之间 **0 ULP 差异**，显式固定括号与 binary64（$\oplus, \ominus, \otimes, \oslash$）求值顺序：

1. **几何贴近强度 `edge_strength`**：
   $$\text{edge\_strength}(i, j) = \begin{cases} \min\bigl(1.0,\; \max(0.0,\; 1.0 \ominus (d_{i,j} \oslash \tau))\bigr) & \tau > 0 \\ 1.0 & \tau == 0 \end{cases}$$
2. **连通分量算法归因流量 `attributed_flow` $f(i, j)$**：
   记分量前驱精确展开 $E_P = \operatorname{Expansion}(p_i : i \in P_C)$（舍入值 $P = E_P.\text{round}()$），后继精确展开 $E_Q = \operatorname{Expansion}(q_j : j \in Q_C)$（舍入值 $Q = E_Q.\text{round}()$）：
   - `continuation`（$P_C = \{i\}, Q_C = \{j\}$）：
     $$f(i, j) = \min(p_i, q_j)$$
   - `split`（$P_C = \{i\}, |Q_C| \ge 2$）：
     - 若 $E_P.\text{cmp\_exact}(E_Q) \ge 0$（即精确 $\sum p \ge \sum q$）：对每个 $j \in Q_C$，$f(i, j) = q_j$；
     - 否则（精确 $p_i < \sum_{j \in Q_C} q_j$）：对每个 $j \in Q_C$，
       $$f(i, j) = \min\bigl(p_i.\min(q_j),\; (p_i \otimes q_j) \oslash Q\bigr)$$
   - `merge`（$|P_C| \ge 2, Q_C = \{j\}$）：
     - 若 $E_Q.\text{cmp\_exact}(E_P) \ge 0$（即精确 $\sum q \ge \sum p$）：对每个 $i \in P_C$，$f(i, j) = p_i$；
     - 否则（精确 $q_j < \sum_{i \in P_C} p_i$）：对每个 $i \in P_C$，
       $$f(i, j) = \min\bigl(p_i.\min(q_j),\; (q_j \otimes p_i) \oslash P\bigr)$$
3. **节点归因汇总与显式未归因残差**：
   - 对前批节点 $i$：$\text{attributed\_outflow}_i = \operatorname{fsum}_{(i,j) \in E_{\text{evol}}} f(i, j)$，$\text{unattributed\_outflow}_i = \max\bigl(0.0,\; \operatorname{fsum}([p_i, -\text{attributed\_outflow}_i])\bigr)$；
   - 对后批节点 $j$：$\text{attributed\_inflow}_j = \operatorname{fsum}_{(i,j) \in E_{\text{evol}}} f(i, j)$，$\text{unattributed\_inflow}_j = \max\bigl(0.0,\; \operatorname{fsum}([q_j, -\text{attributed\_inflow}_j])\bigr)$。

---

## 3. 三族正交演化指标与高不确定性规则（FR-005–FR-007）

记比例模式下参考尺度 $R = S = (s_{\text{prev}} \oplus s_{\text{curr}}) \oslash 2$；绝对模式下若 $\tau > 0$ 则 $R = \tau$，若 $\tau == 0$ 则比值字段为 `None`（SQL 中以 `has_reference_scale = false` + `0.0` 哨兵表示）。

### 3.1 第一族：情景形态变化指标（Morphology Change）
- **单边**：`distance` $= d_{i,j}$；比例模式下 `distance_over_scale` $= d_{i,j} \oslash S$。
- **跃迁汇总**：
  - `primary_total_distance` $= \operatorname{fsum}_{e \in E_{\text{primary}}} d(e)$（空集为 `0.0`）；
  - `primary_max_distance` $= \max_{e \in E_{\text{primary}}} d(e)$（空集为 `0.0`）及 `/ S` 比值；
  - `all_edges_max_distance` $= \max_{e \in E_{\text{evol}}} d(e)$（空集为 `0.0`）及 `/ S` 比值。
- **分支整链汇总**（对持续身份 $b$ 按时间升序的主干延续边序列 $e_1, \dots, e_L$；单节点分支 $L=0$ 时距离汇总均为 `0.0`）：
  - `cumulative_morphology_distance` $= \operatorname{fsum}_{t=1}^L d(e_t)$；
  - `cumulative_morphology_over_scale` $= \operatorname{fsum}_{t=1}^L (d(e_t) \oslash S_t)$；
  - `max_step_morphology_distance` $= \max_{t=1..L} d(e_t)$ 及 `max_step_morphology_over_scale` $= \max_{t=1..L} (d(e_t) \oslash S_t)$；
  - `end_to_end_morphology_distance` $= d(\mu_{b,\text{birth}}, \mu_{b,\text{end}})$（$L=0$ 时为 `0.0`）。

### 3.2 第二族：成员占比变化指标（Member Fraction Change）
- **单边**：`edge_delta_fraction` $= q_j \ominus p_i$，`attributed_flow` $= f(i, j)$。
- **节点 / 分量**：主干节点对 $(i, j) \in E_{\text{primary}}$ 上 `primary_delta_fraction` $= q_j \ominus p_i$；分量 $C$ 净变动 `net_delta_fraction` $= \operatorname{fsum}(\{q_j : j \in Q_C\} \cup \{-p_i : i \in P_C\})$。
- **跃迁汇总**：
  - *主干分支总变差重分配度*：对前批与后批出现的全部持续身份并集 $\mathcal{B}$（仅在后批新生的分支记 $p(b)=0$，仅在前批结束的分支记 $q(b)=0$，主干延续分支取两端占比 $p(b), q(b)$）：
    $$L_1^{\text{branch}} = \operatorname{fsum}_{b \in \mathcal{B}} |q(b) \ominus p(b)|, \quad \text{transition\_tv\_redistribution} = 0.5 \otimes L_1^{\text{branch}} \in [0, 1]$$
  - *次级与残差分解*：`secondary_attributed_flow` $= \operatorname{fsum}_{e \in E_{\text{split}} \cup E_{\text{merge}}} f(e)$，`total_attributed_flow` $= \operatorname{fsum}_{e \in E_{\text{evol}}} f(e)$，`disappeared_fraction_sum` $= \operatorname{fsum}_{i \in U_{\text{prev}}^{\text{none}}} p_i$，`emerged_fraction_sum` $= \operatorname{fsum}_{j \in U_{\text{curr}}^{\text{none}}} q_j$，`component_tv_residual` $= 0.5 \otimes \operatorname{fsum}_C |\text{net\_delta\_fraction}(C)|$。
- **分支整链汇总**（分支节点占比序列 $f_0, f_1, \dots, f_L$）：
  - `initial_member_fraction` $= f_0$，`final_member_fraction` $= f_L$，`min_member_fraction`、`max_member_fraction`；
  - `net_delta_fraction` $= f_L \ominus f_0$，`cumulative_abs_delta_fraction` $= \operatorname{fsum}_{t=1}^L |f_t \ominus f_{t-1}|$。

### 3.3 第三族：匹配与演化不确定性指标及冻结 `m3.uncertainty.v1` 规则
- **三个余量通道**：
  1. `threshold_margin`：对边 $e = (i, j) \in E_{\text{evol}}$ 为 $\Delta_\tau(e) = \tau \ominus d_{i,j} \ge 0$；对节点 $u \in U^{\text{none}}$ 为 $\Delta_\tau(u) = d_{\min}(u) \ominus \tau > 0$；跃迁有效最小门槛余量 `min_threshold_margin` $= \min_{(i,j) \in E_{\text{evol}} \cup \{(i,j) : d_{i,j} > \tau\}} |d_{i,j} \ominus \tau|$（另存全矩阵最小 `raw_matrix_threshold_margin` $= \min_{i,j} |d_{i,j} \ominus \tau|$）。
  2. `optimality_margin`：主干边沿用 M2 禁用该边重求差（`Tied` 为 `Some(0.0)`，`SameCount` 为 `Some(alt_sum.minus_rounded(&opt_sum))`，`CountDrop` 为 `None`）；次级边由定理 3 取强制包含端点 $u \in U^{\text{comp}}$ 重求差 `Some(force_sum.minus_rounded(&opt_sum))`（$\ge 0.0$）。
  3. `secondary_margin`：次级边若 $|F(u)| \ge 2$，令 $d_{(2)}(u)$ 为 $F(u) \setminus \{v^*(u)\}$ 中的最小距离（若存在并列 $d_{(2)} == d_{(1)}$ 则为 $d_{(1)}$），取 `Some(fsum([d_{(2)}(u), -d_{(1)}(u)]))`（$\ge 0.0$）；当 $|F(u)| == 1$ 或在主干边上时为 `None`。
- **冻结高不确定性判定（默认系数 $\varepsilon_\tau = \varepsilon_{\text{opt}} = \varepsilon_{\text{sec}} = 0.10$）**：
  - 单边 `high_uncertainty = true` 当且仅当以下至少一项成立（同步写入 `uncertainty_reasons`）：
    - `ambiguous_tie`：`ambiguous_tie == true`；
    - `near_threshold`：$\tau == 0.0$，或 $R$ 存在且 $\Delta_\tau(e) \le \varepsilon_\tau \otimes R$；
    - `near_primary_switch`：`optimality_margin` 为 `Some(g)` 且（$g == 0.0$ 或（$R$ 存在且 $g \le \varepsilon_{\text{opt}} \otimes R$））；
    - `near_secondary_switch`：`secondary_margin` 为 `Some(s)` 且（$s == 0.0$ 或（$R$ 存在且 $s \le \varepsilon_{\text{sec}} \otimes R$））。
  - $U^{\text{none}}$ 节点 `high_uncertainty = true` 当且仅当 $\tau == 0.0$ 或（$R$ 存在且 $\Delta_\tau(u) \le \varepsilon_\tau \otimes R$）。
  - 分量事件 / 跃迁 `high_uncertainty = true` 当且仅当其包含任一 `high_uncertainty` 边、任一 `high_uncertainty` 的 $U^{\text{none}}$ 节点，或跃迁 `min_threshold_margin <= eps_tau * R`（或 `ambiguous_tie == true`）。

---

## 4. 四条件演化稳定性证书与实验冻结参数（FR-016、SC-005）

全部实验参数写入 `validation/m3/fixtures/evolution_experiment_config.json` 并在门禁运行前冻结：
- **主回放配置**：`k = 3`, `projection_dim = 2`, `seed = 3`, `restarts = 10`, `c = 1.0`, `secondary_rule = "nearest_feasible_v1"`, `flow_rule = "star_min_mass_proportional_v1"`, 不确定性比例 `(0.10, 0.10, 0.10)`。
- **门槛扫描网格**：`c ∈ [0.5, 0.75, 1.0, 1.25, 1.5, 2.0]` × `k ∈ [2, 3, 4]`。
- **聚类配置对照清单（固定 `c = 1.0`）**：
  - `cluster_count` 轴：`k ∈ {2, 3, 4, 5}`（`projection_dim = 2, seed = 3, restarts = 10`）；
  - `projection_dim` 轴：`projection_dim ∈ {1, 2, 3, 4}`（`k = 3, seed = 3, restarts = 10`）；
  - 初始化与重启轴：`(seed = 0, restarts = 10)`、`(seed = 42, restarts = 10)`、`(seed = 3, restarts = 1)`（`k = 3, projection_dim = 2`）；
  - 单批不可计算断链轴：`(k = 3, projection_dim = 3, rank_threshold = 0.35)` 与 `(k = 8, projection_dim = 2, seed = 3, restarts = 10)`。
- **两层三档扰动方案**：
  - Level 1a（均值场微扰）：$\varepsilon \in \{0.005, 0.01, 0.02\}$，100 个单位加权 RMS 方向，固定种子 `20261029`；
  - Level 1b（纯占比微扰）：固定均值场，$\pm 1$ 成员成对重分配；
  - Level 2（逐成员高斯微扰）：$\sigma \in \{0.0, 0.001, 0.01\}\text{ K}$，20 个固定种子 `4242`。
- **四条件保守有理区间演化稳定性证书（充分条件）**：
  令受扰后各情景相对基线的最大加权 RMS 位移上界为 $\varepsilon_{\text{abs}}$，尺度变化上界为 $\Delta S = |S' - S|$。使用精确有理数表示 `f64` 并做向外舍入区间算术，当且仅当同时满足：
  1. 对所有 $(i, j) \in E_{\text{evol}} \cup \{(i, j) : d_{i,j} > \tau\}$：$\operatorname{lower}(|d_{i,j} - \tau|) > \operatorname{upper}(2\varepsilon_{\text{abs}} + c\Delta S)$；
  2. 对所有 `SameCount` 主干边 $e \in E_{\text{primary}}$：$\operatorname{lower}(\Delta\Sigma_e) > \operatorname{upper}(4 q^* \varepsilon_{\text{abs}})$；
  3. `optimal_solution_count == 1`；
  4. 无次级距离并列，且对所有满足 $|F(u)| \ge 2$ 的 $u \in U^{\text{comp}}$：$\operatorname{lower}(d_{(2)}(u) - d_{(1)}(u)) > \operatorname{upper}(4\varepsilon_{\text{abs}})$；
  则判为**已获证书的稳健演化对**，其映射后的主干边、次级边与五类事件翻转数必须 **严格等于 0**。不满足上述任一条件（或成员级评估映射出现并列/单批失败）的样本归入接近边界或映射/单批异常组，100% 记录翻转详情与原始余量。
