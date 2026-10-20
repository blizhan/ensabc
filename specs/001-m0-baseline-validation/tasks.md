---

description: "Task list for M0 可复现基线与接口验证 implementation"
---

# Tasks: M0 可复现基线与接口验证

**Input**: Design documents from `/specs/001-m0-baseline-validation/`

**Prerequisites**: [plan.md](plan.md) (required), [spec.md](spec.md) (required for user stories), [research.md](research.md), [data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md)

**Tests**: M0 本身是基线与接口验证里程碑；各用户故事的可执行验证脚本、正反例断言与门禁检查直接作为故事实现交付物纳入各阶段，不额外拆分冗余的先行空测试桩。

**Organization**: Tasks are grouped by user story (`US1`–`US4`) to enable independent implementation and verification of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: Which user story this task belongs to (`[US1]`, `[US2]`, `[US3]`, `[US4]`)
- Every task description includes exact repository-relative file paths

## Path Conventions

- **Rust workspace & extension**: `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `crates/ensabc-duckdb/`
- **Validation & preparation scripts**: `validation/m0/`
- **Frozen/synthetic fixtures & manifests**: `validation/m0/fixtures/`
- **Run evidence output (git-ignored)**: `artifacts/m0/<run-id>/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 初始化 Rust workspace、工具链锁文件、Python 验证环境锁文件及产物忽略规则。

- [X] T001 Create Rust toolchain pin (`1.96.0`, `edition = "2024"`) and workspace manifest for `crates/ensabc-duckdb` in `rust-toolchain.toml` and `Cargo.toml`
- [X] T002 [P] Update ignore rules for `artifacts/m0/` and local build outputs while preserving `validation/m0/fixtures/` in `.gitignore`
- [X] T003 [P] Promote locked Python dependency files for data extraction and reference computation from `specs/001-m0-baseline-validation/spikes/` to `validation/m0/requirements-data.lock` and `validation/m0/requirements-reference.lock`
- [X] T004 [P] Create environment bootstrap and version verification script (checking `rustc 1.96.0`, `duckdb v1.5.5`, `python 3.13.13`, and `extension-ci-tools` commit `b777c70d30942cca5bef62d6d4fa23a13362f398`) in `validation/m0/setup_env.sh`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 提供所有用户故事共用的哈希计算、JSON 序列化规范（禁止 `NaN`/`Infinity`）、源码脏树清单及 Schema 校验基础模块。

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T005 Create canonical JSON serializer, SHA-256 file/bytes hashing, and dirty-worktree source manifest generator in `validation/m0/common.py`
- [X] T006 [P] Implement `m0.sample.v1` and `m0.report.v1` structural and field validator (distinguishing `real_ensemble` vs `synthetic`, checking finite float64 values and required metadata keys) in `validation/m0/schema.py`
- [X] T007 [P] Create synthetic positive and 10-category negative task fixture generator (labeled `kind: "synthetic"`) in `validation/m0/fixtures/synthetic_tasks.json`

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel.

---

## Phase 3: User Story 1 - 获得身份明确的固定集合样本 (Priority: P1) 🎯 MVP

**Goal**: 冻结单模型、单起报（`2020-01-01T00:00:00Z`）、单有效时刻（`2020-01-02T00:00:00Z`）、近地面气温（`2m_temperature`，单位 `K`）的真实区域集合样本，补齐 `m0.sample.v1` 清单、源对象 generation/许可说明与离线恢复核验（覆盖 `FR-001`、`FR-002`、`FR-003`、`FR-013`、`SC-001`）。

**Independent Test**: 不运行聚类或 DuckDB 扩展，单独执行 `validation/m0/verify_sample.py --offline`，核对清单必填字段完整性 100%、50 个扰动成员与 9 个有序网格点一致、冻结数据总大小 `<= 100 MiB`、矩阵 SHA-256 与离线恢复结果完全一致，并拒绝均值/单点/缺成员或“latest”无起报身份的伪样本。

### Implementation for User Story 1

- [X] T008 [P] [US1] Document source field mapping, WeatherBench 2 TIGGE research-only / GCS generation recovery terms, and Dynamical `CC-BY-4.0` fallback provenance in `validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json`
- [X] T009 [US1] Implement deterministic conversion from spike `float32` slice (`09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe`) and optional source re-extraction with overwrite protection into `m0.sample.v1` member-major JSON payload in `validation/m0/extract_sample.py` and `validation/m0/fixtures/real_ifs_ens_20200101_t2m_data.json`
- [X] T010 [US1] Implement offline sample recovery,<=100 MiB size budget check, and negative eligibility checks (rejecting mean-only, single-point, missing-member, duplicate-point, or unverified `init_time` inputs) in `validation/m0/verify_sample.py`

**Checkpoint**: At this point, User Story 1 is fully functional and independently verifiable offline via `validation/m0/verify_sample.py`.

---

## Phase 4: User Story 2 - 重复得到同一份参考计算结果 (Priority: P1)

**Goal**: 在固定输入（真实区域样本与合成样本）和锁定 Python 环境中跨 3 个独立进程复现 `StandardScaler -> PCA(2, svd_solver="full") -> KMeans(3, init="k-means++", n_init=10, random_state=0)`，保存完整中间结果并通过置换/符号不变性比较器正反例检验（覆盖 `FR-004`、`FR-005`、`SC-002`）。

**Independent Test**: 不依赖实时数据下载或 DuckDB 扩展，从 `validation/m0/fixtures/real_ifs_ens_20200101_t2m_data.json` 启动 3 个独立进程执行 `validation/m0/run_reference.py`，并用 `validation/m0/compare_reference.py` 验证成员分区一致、可比数值满足 `abs(actual - reference) <= 1e-8 + 1e-6 * abs(reference)`，同时确认比较器能通过标签置换/符号翻转正例并拦截错簇与数值超差反例。

### Implementation for User Story 2

- [X] T011 [P] [US2] Create locked reference configuration file (`float64`, `thread_limit=1`, `svd_solver="full"`, `k-means++`, `n_init=10`, `random_state=0`, rank threshold, and `atol=1e-8`/`rtol=1e-6`) in `validation/m0/fixtures/reference_config.json`
- [X] T012 [P] [US2] Implement label-permutation-invariant and PCA-sign/subspace-aware comparator with positive/negative self-checks (label rename, sign flip, single-member cluster move, tolerance breach, and degenerate subspace rejection) in `validation/m0/compare_reference.py`
- [X] T013 [US2] Implement 3-process reference runner outputting `mean`, `scale`, `scaled`, `rank_after_scaling`, `components`, `explained_variance_ratio`, `scores`, `labels`, `cluster_centers`, `inertia`, `n_iter`, and input/config hashes without overwriting prior runs in `validation/m0/run_reference.py`

**Checkpoint**: At this point, User Stories 1 and 2 work independently and produce reproducible 3-process reference artifacts on both synthetic and real frozen inputs.

---

## Phase 5: User Story 3 - 确认计算调用边界可安全使用 (Priority: P1)

**Goal**: 在 Linux aarch64 + DuckDB CLI `v1.5.5` 下编译、打包并实际加载 `crates/ensabc-duckdb` 的 `ens_m0_echo` 标量函数，通过 C API 注册 `duckdb_scalar_function_set_special_handling` 拦截顶层 `NULL` 与全部嵌套非法输入，验证同连接恢复、跨批量边界、双连接并发、1/10/100 任务资源测量及无参考工具最小环境隔离（覆盖 `FR-006`、`FR-007`、`FR-008`、`FR-009`、`FR-011`、`FR-012`、`SC-003`、`SC-004`、`SC-005`）。

**Independent Test**: 使用 `validation/m0/fixtures/synthetic_tasks.json` 在新建 DuckDB 会话及无 Python 参考工具/CDO/wgrib 的最小运行环境中加载扩展，验证正常任务零串扰往返、10 类非法输入 100% 报错且同连接恢复、双连接并发结果一致，并输出 1/10/100 任务耗时与峰值 RSS 记录。

### Implementation for User Story 3

- [X] T014 [P] [US3] Create `crates/ensabc-duckdb/Cargo.toml` pinning `duckdb = "=1.10505.0"` (`loadable-extension`) and generate workspace `Cargo.lock`
- [X] T015 [US3] Implement C API extension entrypoint, `duckdb_scalar_function_set_special_handling` registration, `catch_unwind` FFI boundary, nested `LIST<STRUCT<member_id VARCHAR, features DOUBLE[]>>` validation (top-level `NULL`, empty list, item `NULL`, ` NULL`/empty/duplicate `member_id`, unequal feature lengths within a task, element `NULL`/`NaN`/`Inf`), and `duckdb_vector_reference_vector` output in `crates/ensabc-duckdb/src/lib.rs`
- [X] T016 [US3] Create extension build and metadata packaging script (`C_STRUCT_UNSTABLE`, `linux_arm64`, `v1.5.5`) in `validation/m0/build_extension.sh`
- [X] T017 [US3] Implement SQL roundtrip, >2048-row chunk boundary, >2048-member list, 2-connection concurrency, and 10-category same-connection `valid -> invalid -> valid` recovery verifier in `validation/m0/verify_extension.py`
- [X] T018 [P] [US3] Implement 1, 10, and 100 task wall-clock time and peak RSS measurement runner (separating cold start vs warm invocation and preventing SQL constant folding) in `validation/m0/measure_tasks.py`
- [X] T019 [P] [US3] Implement reference-tool-free minimal runtime verification script (executing `ens_m0_echo` positive and error-recovery suites inside a clean Ubuntu 24.04 ARM64 container or isolated environment without Python data/sklearn stacks, `cdo`, or `wgrib2`, plus `ldd` audit) in `validation/m0/verify_minimal_runtime.sh`

**Checkpoint**: At this point, User Story 3 is fully functional and verified for roundtrip fidelity, error recovery, concurrency, resource measurement, and runtime isolation.

---

## Phase 6: User Story 4 - 判断是否具备进入 M1 的条件 (Priority: P2)

**Goal**: 汇总 `FR-001` 至 `FR-014` 及 `SC-001` 至 `SC-006` 的全部实测证据，生成符合 `m0.report.v1` 契约的 `artifacts/m0/<run-id>/report.json`，明确 `passed|failed|incomplete` 判定与 M1 交接依据（覆盖 `FR-010`、`FR-014`、`SC-006`）。

**Independent Test**: 对照 `specs/001-m0-baseline-validation/contracts/evidence.md` 运行 `validation/m0/run_m0_gate.py`，核对 `artifacts/m0/<run-id>/` 下全部 10 类证据文件及 SHA-256 引用，并验证当任一必要门禁（如真实样本许可或某类异常恢复）为 `fail` 或 `not_run` 时，总报告状态拒绝给出 `passed` 且返回对应退出码（`1` 或 `2`）。

### Implementation for User Story 4

- [X] T020 [US4] Implement end-to-end M0 gate orchestrator and `m0.report.v1` aggregator (writing `environment.json`, `source-hashes.json`, `sample-validation.json`, `reference/run-1..3.json`, `reference/comparison.json`, `exchange.json`, `measurements.json`, `minimal-runtime.json`, and `report.json` under `artifacts/m0/<run-id>/` with exit codes `0|1|2`) in `validation/m0/run_m0_gate.py`
- [X] T021 [P] [US4] Implement independent report auditor and partial-failure gate self-test (verifying that synthetic-only pass or any `fail`/`not_run` requirement prevents overall `passed`) in `validation/m0/verify_report.py`

**Checkpoint**: All user stories (`US1`–`US4`) are complete and produce auditable M0 gate reports.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: 同步复跑文档、路线图状态与最终完整门禁复核。

- [X] T022 [P] Update runnable formal verification commands and artifact paths in `specs/001-m0-baseline-validation/quickstart.md`
- [X] T023 [P] Update M0 task checkboxes, evidence paths, and decision log in `docs/ROADMAP.md` and `README.md`
- [X] T024 Execute full M0 gate run via `validation/m0/run_m0_gate.py` and verify output report with `validation/m0/verify_report.py`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1: T001–T004)**: No dependencies - can start immediately.
- **Foundational (Phase 2: T005–T007)**: Depends on Setup completion - BLOCKS all user stories.
- **User Stories (Phase 3–6)**:
  - **US1 (Phase 3: T008–T010)**: Depends on Phase 2 (`T005`, `T006`). No dependency on `US2` or `US3`.
  - **US2 (Phase 4: T011–T013)**: `T011` and `T012` depend only on Phase 2 (`T005`, `T006`) and can run in parallel with `US1`; running `T013` on the frozen real sample depends on `T009` (`US1`).
  - **US3 (Phase 5: T014–T019)**: Depends on Phase 1 (`T001`, `T004`) and Phase 2 (`T005`, `T007`). Completely independent of `US1` and `US2` during implementation (`T018` can use the frozen real matrix from `T009` or a fixed 50×9 matrix fixture).
  - **US4 (Phase 6: T020–T021)**: Depends on `US1` (`T010`), `US2` (`T013`), and `US3` (`T017`, `T018`, `T019`) to aggregate full evidence, while `T021` report-logic self-test can be built in parallel once `T006` is ready.
- **Polish (Phase 7: T022–T024)**: Depends on completion of `US1`–`US4`.

### User Story Dependency Graph

```text
Phase 1 (Setup: T001-T004)
          │
          ▼
Phase 2 (Foundational: T005-T007)
    ├──► Phase 3: US1 (Frozen Real Sample: T008-T010) ──┐
    │         │                                         │
    │         ▼                                         ├──► Phase 6: US4 (M0 Gate Report: T020-T021)
    ├──► Phase 4: US2 (3-Process Reference: T011-T013) ─┤                 │
    │                                                   │                 ▼
    └──► Phase 5: US3 (Rust DuckDB Echo: T014-T019) ────┘    Phase 7 (Polish: T022-T024)
```

### Parallel Opportunities

- **Phase 1**: `T002`, `T003`, `T004` modify disjoint files and can run in parallel after `T001`.
- **Phase 2**: `T006` (`validation/m0/schema.py`) and `T007` (`validation/m0/fixtures/synthetic_tasks.json`) can run in parallel after `T005`.
- **Cross-Story Parallelism**: After Phase 2, `US1` (`T008`–`T010`), `US2` comparator/config (`T011`–`T012`), and `US3` Rust extension (`T014`–`T019`) touch completely disjoint directories (`validation/m0/fixtures/`, `validation/m0/compare_reference.py`, and `crates/ensabc-duckdb/`) and can run in parallel.
- **Within US3**: Once `T016` packages the extension binary, `T017` (correctness & recovery), `T018` (1/10/100 task measurement), and `T019` (minimal runtime container check) can run in parallel.

---

## Parallel Example: User Stories 1, 2, and 3

```bash
# Worker A (US1 - Real Frozen Sample):
Task: "T008 [P] [US1] Document source field mapping and provenance in validation/m0/fixtures/real_ifs_ens_20200101_t2m_manifest.json"
Task: "T009 [US1] Implement deterministic conversion into validation/m0/fixtures/real_ifs_ens_20200101_t2m_data.json"

# Worker B (US2 - Reference Comparator & Runner):
Task: "T011 [P] [US2] Create locked reference configuration in validation/m0/fixtures/reference_config.json"
Task: "T012 [P] [US2] Implement label-permutation and PCA-sign comparator in validation/m0/compare_reference.py"

# Worker C (US3 - Rust DuckDB Extension):
Task: "T014 [P] [US3] Create crates/ensabc-duckdb/Cargo.toml and workspace Cargo.lock"
Task: "T015 [US3] Implement C API special NULL handling echo extension in crates/ensabc-duckdb/src/lib.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (`T001`–`T004`)
2. Complete Phase 2: Foundational (`T005`–`T007`)
3. Complete Phase 3: User Story 1 (`T008`–`T010`)
4. **STOP and VALIDATE**: Run `validation/m0/verify_sample.py --offline` to confirm the frozen real regional sample and provenance manifest pass all eligibility and hash checks.

### Incremental Delivery

1. Setup + Foundational → Shared hashing, schema, and synthetic fixtures ready.
2. Add User Story 1 (`US1`) → Frozen real regional sample verified offline.
3. Add User Story 2 (`US2`) → 3-process Python numerical baseline verified on synthetic and real inputs.
4. Add User Story 3 (`US3`) → Native Rust DuckDB extension verified for nested roundtrip, top-level/nested `NULL` rejection, same-connection recovery, 1/10/100 task resource measurement, and reference-tool-free runtime execution.
5. Add User Story 4 (`US4`) + Polish → Full `m0.report.v1` evidence bundle audited for M1 entry readiness.
