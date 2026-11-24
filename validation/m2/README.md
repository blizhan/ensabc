# M2 验证套件 (Cross-Batch Scenario Tracking)

本文档说明 Milestone 2 (M2) 跨批次情景追踪的验证架构、环境配置、数据合规边界与运行规范。

## 1. 目录结构与隔离规则

- **运行证据目录**：`artifacts/m2/<run-id>/`。每次执行门禁或验证脚本均要求独占的 `<run-id>` 目录，写入原子证据与状态报告。此目录被 `.gitignore` 忽略，绝对禁止提交到 Git。
- **派生载荷目录**：`validation/m2/data/`。存放解压或转换后的 OM v3 文件、NetCDF/Zarr 临时块与中间 DuckDB 数据库。此目录被 `.gitignore` 忽略，绝对禁止提交到 Git。
- **虚拟环境与缓存**：
  - `validation/m2/.venv/`：专用于 OM 数据准备与独立参考比对的独立 Python venv。
  - `validation/m2/.cache/`：存放锁定的 DuckOMO release 压缩包与下载的 WB2 原始数据块。
- **构建与测试隔离**：支持 `--target-dir` 与 `--output-dir` 外部路径参数，避免污染主工作树或并发构建竞争。每次 DuckDB 进程使用新建 HOME 与扩展目录，禁用自动安装/加载；显式加载 DuckOMO 前检查发布哈希，加载后检查版本和 `NOT_INSTALLED` 身份。
- **实际执行记录**：`execution-steps.json` 记录命令、退出码、耗时与结果；报告读取这些记录，缺少或失败的必要步骤不能判为通过。独立审计重新执行证据断言。
- **回放版本**：版本键绑定有序 OM 文件实际摘要、情景配置、匹配配置、权重及软件身份；重跑保留原数据库，同键同内容幂等，同键不同内容失败。
- **独立复现**：新 clone 后覆盖当前有效源码（包括未提交与未跟踪实现），单独重新构建；缓存与冻结样本作为显式输入复用。逐字段比较数值、关联和出生节点身份，允许构建哈希派生的版本前缀变化。
- **断网验收**：容器执行真实 8 批完整历史发布，逐字段核对普通环境结果，并在同连接内验证失败发布无残余及后续合法调用恢复。

## 2. 联网与离线边界

1. **首次联网（下载与环境准备）**：
   - `bash validation/m2/setup_env.sh`：拉取固定版本的 Python 依赖 wheel，下载钉住的 DuckOMO `v0.3.5-rc.1` 与 `v0.3.4` Linux aarch64 预编译扩展包，并核对 SHA-256 校验和。
   - `python3 validation/m2/prepare_om_sample.py`：在缓存缺失时从 WeatherBench 2 官方公开存储拉取 8 批次的 2m 温度数据块（共 9 个格点 × 50 扰动成员）。
2. **后续离线（严格无网络运行）**：
   - 所有核心验证、SQL 历史发布、案例敏感性分析、双版本回放与门禁脚本均在完全离线状态下运行。
   - 最小运行环境验证脚本 `verify_minimal_runtime.sh` 显式以 `--network none` 容器模式运行。

## 3. 数据许可与 GPL 隔离

- **WeatherBench 2 许可**：WB2 IFS ENS 数据派生物仅供研究用途，重采样和派生载荷不可进入 Git 仓库，仅提交抽取清单、SHA-256 指纹与可复现抽取脚本。
- **GPL 隔离**：`omfiles`（GPL-2.0-only）严格隔离在 Python 准备/比对脚本中（通过 `requirements-om.lock` 安装至独立 venv），严禁作为 Rust workspace 依赖或引入 `crates/ensabc-*`。

## 4. 命令退出码语义

M2 所有验证脚本统一遵循标准退出码规范：
- **`0` (PASS / OK)**：所有断言验证通过，产物哈希一致，状态完整。
- **`1` (FAIL / ERROR)**：验证断言失败、产物差异、哈希不匹配、事务不变量破坏或非法输入。
- **`2` (NOT_RUN / MISSING_PREREQUISITE)**：前置依赖（如 DuckOMO 二进制、输入样本文件、前阶段验证产物）缺失或显式跳过。
