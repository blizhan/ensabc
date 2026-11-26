# M3 离线单文件可交互演化可视化与溯源核验契约

版本：`m3.visualization.v1` / `m3.disclaimer.v1`。对应 FR-008、FR-014、SC-006、SC-008，以及边界情况“可视化与历史脱节”和“解释边界越界”。

---

## 1. 生成器接口与单一事实源约束（`validation/m3/render_evolution_html.py`）

```bash
validation/m2/.venv/bin/python validation/m3/render_evolution_html.py \
  --db /path/to/history.duckdb \
  --evolution-run-id <EVOLUTION_RUN_ID> \
  --output /path/to/evolution-view.html
```

- **零新增第三方依赖**：仅使用锁定的 Python `3.13.13` 标准库（`json`, `html`, `math`, `hashlib`, `pathlib`）与隔离环境调用的 DuckDB CLI `v1.5.5`（`-json` 模式）。
- **单一事实源与渲染前置门禁**：
  - 严禁从内存临时计算或测试 fixture 直接拼装图表；只允许从已发布的 DuckDB 正式历史表（`evolution_runs`、`replay_batches`、`evolution_nodes`、`evolution_transitions`、`evolution_edges`、`evolution_events`、`evolution_branch_summaries`）查询指定 `evolution_run_id`。
  - 若 `evolution_run_id` 不存在、关联节点数为 0、或任一必需溯源字段（版本标识、批次时间、持续身份、角色、事件、距离、离散比、占比与归因流量、三类余量、歧义/高不确定性标记、规则依据）缺失或含非有限值，立即以退码 `1`（错误码 `MISSING_PROVENANCE_FIELD`）失败，并通过原子临时文件机制保证目标路径 **0 字节残留**。

---

## 2. 离线单文件 `.html` 结构契约

生成的 `.html` 文件必须为 UTF-8 自包含单文件，在 `file://` 或完全断网浏览器中无需任何后台服务即可完整交互：

### 2.1 强制离线 CSP 头
`<head>` 内必须包含严格的 Content Security Policy：
```html
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline';">
```
全文禁止出现任何外部资源引用（`<script src=...>`, `<link rel="stylesheet" href=...>`, `@import`, `<img src="http...">`, `http://`, `https://`, `//`, `ws://`）。

### 2.2 置顶不可折叠解释边界横幅（`#ensabc-disclaimer-banner`）
页面顶部必须包含常驻可见（禁止 `hidden`、`display: none`、`visibility: hidden` 或默认收起）的 `<header id="ensabc-disclaimer-banner" data-disclaimer-version="m3.disclaimer.v1">`，显式载明三条解释边界：
1. **非物理成员迁移**：分裂/合并连线与 `attributed_flow` 仅是基于情景物理均值场距离和成员计数的算法归因，不代表集合成员跨起报发生物理迁移；
2. **非物理新生/消亡**：`emergence` 与 `disappearance` 仅表示当前距离门槛下无可行前驱或后继，不代表天气系统的物理生成或消亡；
3. **非校准概率或业务技巧**：`member_fraction` 为样本内成员比例，未经概率校准，不构成预报修正技巧或交易决策依据。

### 2.3 严格良构内联演化流图（`<svg id="ensabc-evolution-svg">`）
`<svg>` 必须可作为严格良构 XML 独立解析，并满足以下 DOM 数据属性规范：
1. **批次列与断链屏障**：
   - 每个计划批次 $b \in \{0..B-1\}$ 对应 `<g class="evo-batch-col" data-batch-ordinal="{b}" data-batch-status="{ok|missing|failed}" data-init-time="{iso}" data-lead-hours="{h}">`；
   - 当状态为 `missing` 或 `failed` 时，渲染显式断链屏障 `<g class="evo-gap-marker" data-batch-ordinal="{b}" data-status="{status}" data-status-reason="{reason}">`，且缺口两侧无任何跨缺口边。
2. **情景节点图元（`.evo-node`）**：
   - 键格式：`data-node-id="b{ordinal}:c{cluster_id}"`；
   - 必填属性：`data-evolution-run-id`, `data-batch-index`, `data-cluster-id`, `data-canonical-rank`, `data-branch-id`, `data-incoming-role`, `data-outgoing-role`, `data-member-count`, `data-member-fraction`, `data-high-uncertainty`；
   - 节点高度与 `member_fraction` 成正比，对 `emergence`、`disappearance`、`gap_interrupted`、`gap_restarted` 与 `high_uncertainty` 提供可区分视觉徽标与无障碍焦点（`tabindex="0"`）。
3. **多对多有向演化连线（`.evo-edge`）**：
   - 键格式：`data-edge-id="b{prev_ordinal}:c{prev_cluster_id}->b{curr_ordinal}:c{curr_cluster_id}"`；
   - 必填属性：`data-evolution-run-id`, `data-prev-node-id`, `data-curr-node-id`, `data-edge-role`（`primary` | `secondary_split` | `secondary_merge`）, `data-event-type`（`continuation` | `split` | `merge`）, `data-distance`, `data-distance-over-scale`, `data-attributed-flow`, `data-ambiguous-tie`, `data-high-uncertainty`；
   - 视觉编码：`primary` 采用分支色实线彩带（带宽正比于 `attributed_flow`），`secondary_split` / `secondary_merge` 采用次级虚线描边彩带，`high_uncertainty` / `ambiguous_tie` 叠加醒目双层警示轮廓。
4. **零孤立图形规则**：`<svg>` 内除 `<defs>` 或显式标记 `data-decorative="true"` 的坐标轴/图例背景外，每一个几何图元（`path`, `rect`, `circle`, `line`, `polygon`, `polyline`）必须隶属于某个带唯一键的 `.evo-node`、`.evo-edge` 或 `.evo-gap-marker` 分组。

### 2.4 内嵌规范化溯源清单、交互抽屉与静态兜底账本
1. **`<script id="ensabc-evolution-provenance" type="application/json">`**：以规范 JSON（键排序、`allow_nan=False`、`-0.0 -> 0.0`）完整嵌入 `TraceableEvolutionViewManifest`（含 `run`, `disclaimer`, `batches`, `nodes`, `edges`, `transitions`, `branches`）。
2. **原生交互检查抽屉（`<aside id="ensabc-inspector-drawer">`）**：内联原生 ES6 脚本监听所有 `.evo-node` 与 `.evo-edge` 的悬停（`mouseenter`）、点击（`click`）与键盘确认（`Enter`/`Space`），高亮整条关联 `branch_id` 谱系及次级母/子关联，并在抽屉面板中逐项渲染完整溯源依据（`evolution_run_id`、`tracking_run_key`、批次序号与起报时刻、`branch_id`、`edge_role`、`event_type`、`rule_basis`、`distance`、`distance_over_scale`、`member_fraction`、`edge_delta_fraction` / `primary_delta_fraction`、`attributed_flow` 与未归因残差、`threshold_margin`、`optimality_margin`、`secondary_margin`、`ambiguous_tie`、`high_uncertainty` 与 `uncertainty_reasons`）。
3. **静态可检索账本（`<details id="ensabc-provenance-ledger">`）**：在无需执行 JS 的纯静态审阅场景下，直接列出全部节点、演化边与分支汇总的对照表格。

---

## 3. 自动溯源核验器契约（`validation/m3/verify_visualization_traceability.py`）

```bash
validation/m2/.venv/bin/python validation/m3/verify_visualization_traceability.py \
  --html /path/to/evolution-view.html \
  --db /path/to/history.duckdb \
  --evolution-run-id <EVOLUTION_RUN_ID> \
  --out-report /path/to/traceability-audit.json

# 10 类恶意篡改与缺字段反例自检
validation/m2/.venv/bin/python validation/m3/verify_visualization_traceability.py --self-check
```

核验器必须依次通过以下 **6 重检查**（任一失败退码 `1`）：
1. **离线零外链检查**：验证严格 CSP 头存在，扫描全 HTML 属性与样式块确认零外部 URL / CDN / 远程脚本或字体。
2. **SVG 严格 XML 与零孤立图元检查**：用 `xml.etree.ElementTree` 严格解析 `<svg>`，核验所有 `.evo-node` / `.evo-edge` 键唯一、`curr_batch == prev_batch + 1`、端点存在，且非装饰图元 100% 归属于有键实体（孤立图元数 `== 0`）。
3. **SVG DOM ↔ 内嵌 JSON 双向双射检查**：核对 `svg_nodes.keys() == json_nodes.keys()`、`svg_edges.keys() == json_edges.keys()`，且所有 `data-*` 属性值与 JSON 对应字段一致，交互抽屉 `#ensabc-inspector-drawer` 绑定齐全。
4. **HTML ↔ DuckDB 正式历史表 100% 逐字段真值核对**：直接独立查询 `db` 中的六张 M3 正式表，核对 `evolution_run_id`、`content_sha256`、全部批次状态（含零跨缺口边）、全部节点、全部主/次演化边、全部事件、全部跃迁及分支汇总 100% 一一对应且数值一致（防止同时篡改 SVG 与内嵌 JSON 蒙混过关）。
5. **解释边界与违禁宣称检查**：确认 `#ensabc-disclaimer-banner` 可见且完整包含三条非物理/非校准声明，扫描全文无违禁夸大表述。
6. **`--self-check` 十类篡改反例强制拦截测试**：在临时目录对合法产物分别注入 ① 删除 SVG 节点、② 插入无键孤立 `<path>`、③ 篡改 SVG `data-distance`、④ **同时篡改 SVG 与内嵌 JSON 距离企图绕过内部核对**、⑤ 将 `secondary_split` 改为 `primary`、⑥ 跨 `missing`/`failed` 缺口注入连线、⑦ 注入外链 `<script src="https://...">`、⑧ 删除或 `display:none` 隐藏免责横幅、⑨ 剥离 `optimality_margin` / `rule_basis` 溯源键、⑩ 传入错误 `evolution_run_id` 绑定（外加缺溯源字段渲染拒绝测试），断言核验器 100% 检出并以退码 `1` 拒绝。
