---
description: "Dependency-ordered implementation tasks for M1 单批次情景计算"
---

# Tasks: M1 单批次情景计算

**Input**: Design documents from `specs/002-m1-scenario-computation/`。

**Prerequisites**: [spec.md](spec.md)、[plan.md](plan.md)、[research.md](research.md)、[data-model.md](data-model.md)、[contracts/](contracts/) 和 [quickstart.md](quickstart.md)。

**Status**: 41 项任务已生成，并按分析问题 I1/U1/I2/I3/I4 修订阶段参数、参考观察、来源下标、复现所有权和状态说明；尚未开始实现或 M1 验收。任务编号与所有未勾选状态保留，完成任务、通过规格质量检查与 M1 整体通过是不同结论。

**Tests**: spec 的 FR-007–FR-010、FR-013–FR-017 与 SC-001–SC-008 明确要求数值/非法输入/集成/资源/复现验证，因此包含相应测试任务。契约测试先写并确认相应断言失败，随后完成实际实现；不为文档或每个 helper 添加镜像测试。尚未存在的 core 可以先通过编译失败确认缺失接口，后续必须进入行为断言，不能把缺 crate 当作最终测试证据。

**Organization**: US1、US2、US3 为 P1，US4 为 P2。每个故事有可单独执行的验证入口，但依赖真实存在的 core/数值通过后才能验收后续故事。没有实施、发布、push 或对外写入动作随本次任务生成执行。

## Format: `[ID] [P?] [Story] Description`

- `[P]` 表示在明确前置任务完成后可与同波其他任务并行，必须没有共享文件写入；不表示所有标记任务可同时开始。
- `[US1]`–`[US4]` 对应 spec 的用户故事；Setup、Foundational、Polish 不标故事。
- 路径均相对仓库根。只创建当前任务需要的模块，不预建通用框架。
- 每项括号内列直接前置与覆盖的 FR/SC；运行记录写 `artifacts/m1/<run-id>/`，真实数值和派生证据不入库。

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 冻结实施所需配置、环境边界和证据输出策略。

- [X] T001 Freeze `m1.config.v1` native defaults (d=2, k=3, ChaCha8 seed=0, 10 restarts, tol=1e-4, cap=300, rank threshold=1e-10) and `m1.comparison.v1` fixed tolerance/alignment rules in `validation/m1/fixtures/native_config.json` and `validation/m1/fixtures/comparison_rules.json`; retain original M0 config as read-only input (FR-003, FR-007, FR-008).
- [X] T002 [P] Implement version/environment bootstrap and `--check` in `validation/m1/setup_env.sh`, reusing M0 reference/data lockfiles and environment overrides; verify Linux ARM64, Rust 1.96.0, DuckDB 1.5.5, packaging-tool revision, Docker and the chosen nalgebra/rand feature policy; distinguish missing prerequisites from computation failures (FR-014, FR-015).
- [X] T003 [P] Add `/artifacts/m1/` and M1 temporary build/output exclusions to `.gitignore`, preserving existing M0 rules and ensuring no broad `data/` pattern hides committed recovery tools; do not ignore M1 source fixtures or scripts (FR-012, FR-015, FR-017).

**Checkpoint**: Configuration and comparison rules have immutable input identities; reference tooling remains separate from core and generated real-data evidence stays outside version control.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 为独立计算、查询和验收建立共同类型、身份及输入入口。

- [X] T004 Implement strict JSON loading, byte/semantic hashes, unique run-directory creation, located validation errors and relevant-source inventory in `validation/m1/common.py`; include dirty tracked and relevant untracked file bytes, original HEAD and explicit exclusions without copying secrets or real-data payloads; use only the standard library for orchestration (after T001–T003; FR-006, FR-015, FR-017).
- [X] T005 Implement structural validators for task/result/config/reference/comparison/report/traceability records in `validation/m1/schema.py`, including the data-model fields and random-vs-explicit `initial_member_indices` rules, stage prerequisites/Q/observer-check identity, exact 17 FR/8 SC IDs, finite matrices and dimensions, path-contained SHA-256 refs and pass/fail/not_run aggregation (after T004; FR-003, FR-006, FR-008, FR-017).
- [X] T006 Implement `validation/m1/prepare_input.py` with the documented manifest/payload/config/output arguments; invoke existing M0 eligibility checks, preserve source float32→f64 identity and ordered member/grid/time/unit semantics, write `inputs/task.json`, identity/config refs and run metadata without mutating M0 files; defer SQL rendering until T029 provides its template (after T005; FR-001, FR-006, FR-012, FR-015; SC-001).
- [X] T007 Define public `TaskInput`, member/config/result/diagnostic/restart/error types and `ALGORITHM_VERSION` in `crates/ensabc-core/src/lib.rs`, create `crates/ensabc-core/Cargo.toml`, update workspace `Cargo.toml`/`Cargo.lock` with exact nalgebra/rand/rand_chacha versions and std-only features; declare serde/serde_json only as dev-dependencies for the runner and add the core path dependency to `crates/ensabc-duckdb/Cargo.toml` (after T005; FR-001, FR-003, FR-006, FR-014, FR-015).

**Checkpoint**: 输入/配置/证据可以严格核验，公共类型边界确定；core 不依赖 DuckDB、Arrow、Python 或网络。US1 开始前完成 Phase 1–2，不将临时未实现入口标为成功。

---

## Phase 3: User Story 1 — 从一个集合批次得到可解释的情景 (Priority: P1) — MVP

**Goal**: 不依赖查询宿主，计算可解释的完整情景与明确错误。

**Independent Test**: `cargo test --locked -p ensabc-core` 与独立 runner 使用合法合成矩阵，验证成员恰一次归属、物理均值、代表成员并列规则、占比/SSE/诊断和全部 core 错误；不要求网络或 SQL。

### Tests for User Story 1

- [X] T008 [P] [US1] Write public core contract tests in `crates/ensabc-core/tests/scenario_contract.rs` for d=1/d=rank, k=2/k=distinct, duplicate-valued distinct IDs, constant/near-constant features, representative ties, complete typed diagnostics, independent input-order identity, repeated calls/threads and config/rank/empty-cluster/non-convergence errors; assert error codes/positions and confirm missing behavior fails before implementation (after T007; FR-001–FR-006, FR-009, FR-010, FR-013; SC-002, SC-005).
- [X] T009 [P] [US1] Create only redistributable synthetic cases with explicit expected outcomes and category IDs in `validation/m1/fixtures/synthetic_tasks.json`, including explainable groups, distinct identities with equal values, parameter boundaries, projection collapse, rank/cutoff degeneracy, non-finite/overflow cases and located failures; encode SQL NULL/literal cases as test descriptions rather than illegal JSON numeric values (after T007; FR-003, FR-010, FR-013; SC-002, SC-005).

### Implementation for User Story 1

- [X] T010 [US1] Implement shared semantic input/config validation
- [X] T011 [US1] Add bounded `SVD::try_new`
- [X] T012 [US1] Implement explicit-center and `kmeans++_chacha8_v1` initialization
- [X] T013 [US1] Implement Lloyd assignment/update
- [X] T014 [US1] Implement canonical cluster numbering
- [X] T015 [US1] Implement `--input/--output` task/result envelope handling

**Checkpoint**: US1 可作为独立 core 演示，合法合成任务和错误契约通过；这不代表原 M0 对照、SQL 或整体 M1 通过。

---

## Phase 4: User Story 2 — 确认原生结果与参考结果一致 (Priority: P1)

**Goal**: 分别证明原 M0 结果符合性、共享起点下的算法步骤正确性及三进程可重复性。

**Independent Test**: 直接对 runner 输出与原参考/共享中心记录执行 M1 comparator，接受编号/符号/等价旋转，拒绝错分与超差；不依赖 DuckDB 集成。

### Tests for User Story 2

- [X] T016 [P] [US2] Define comparator positive/negative and identity/shape self-check cases in `validation/m1/test_comparison.py`, covering cluster renaming, sign flips, orthogonal rotation, wrong subspaces/partitions, per-element tolerance failures, missing/non-finite arrays and original-vs-shared evidence confusion; verify the new comparator fails these contracts before implementation (after T015; FR-007, FR-008, FR-013; SC-003).
- [X] T017 [P] [US2] Add test-only actual-Lloyd trace collection inside `crates/ensabc-core/src/kmeans.rs` and an ignored root `tests::m1_shared_trace` export test in `crates/ensabc-core/src/lib.rs`; bind `ENSABC_M1_TRACE_INPUT`/`ENSABC_M1_TRACE_OUTPUT` as specified in `contracts/core.md`, copy per-update labels/centers/SSE/shift/stop events and separate final assignment_only (shift=null, not an iteration); explicitly invoke the ignored test from the driver, reject missing export inputs, preserve ordinary cargo-test independence and verify observation does not change core results (after T015; FR-007, FR-008).

### Implementation for User Story 2

- [X] T018 [US2] Implement required `--stage original` in `validation/m1/run_reference.py` using a subprocess of the configured locked reference interpreter and unchanged M0 runner/config, writing three distinct-PID outputs under `reference/original/` with original hashes and independent summary expectations; depend only on prepared input/config (not native/shared results), reject mutation of consumed M0 evidence and reject omitted stage (after T016, T017; FR-007, FR-009, FR-015).
- [X] T019 [US2] Implement required `--stage native` in `validation/m1/verify_core.py` to build/run the real and synthetic inputs, independently recompute membership/means/representatives/SSE, validate diagnostics/configuration and random/explicit initialization index rules, and collect three native process results plus errors; write native projection artifacts without reading shared evidence or claiming comparator success (after T018; FR-004–FR-007, FR-009, FR-010, FR-013; SC-001–SC-005).
- [X] T020 [US2] Implement required `--stage original` geometry-first per-element comparison and Procrustes alignment in `validation/m1/compare_results.py`, with exact identity/partition checks, `B^T B` and pairwise squared-distance checks, aligned scores/centers, physical means, frozen tolerances and mutually exclusive `--self-check`; write `comparison/original.json` and source-bound `comparison/projection-alignment.json` with `scores_native * Q ≈ scores_reference`; do not consume shared outputs; original-vs-native iteration counts remain diagnostic (after T019; FR-007, FR-008; SC-003).
- [X] T021 [US2] Add required `--stage shared` in `validation/m1/run_reference.py`, consuming passing original comparison and Q/native projection identity, deriving starts `[0,16,33]` and mapping reference centers by `Q^T`; in an isolated single-thread subprocess wrap the saved `lloyd_iter_chunked_dense` exactly once per call while running unchanged `_kmeans_single_lloyd`, bind/copy arguments by name, record actual update/assignment_only events, restore in finally and require exact final equality against an unwrapped run; write shared initialization/reference/trace/observer-check files, cover stable-label and tolerance-stop paths, return 2 on missing prerequisites and never infer traces from final centers (after T020; FR-007, FR-008).
- [X] T022 [US2] Complete required `--stage shared` in `validation/m1/verify_core.py` and `validation/m1/compare_results.py`: first export the internal trace on identical scores/centers and run public end-to-end paths with independently fitted aligned projections, then compare existing shared results/reference and require valid observer-check plus the separate original-M0 comparison; keep producer/comparator writes separate, return 2 for missing stages and reject mismatched hashes, preserve prior original outputs and prove blocked-stage/self-check behavior in `validation/m1/test_comparison.py` (after T021; FR-007–FR-009; SC-003, SC-004).

**Checkpoint**: 两条比较路径和比较器反例均通过，三进程 native/reference 结果符合要求。原生初始化达不到原基线时停在 US2，不能用共享对照代替并继续声称数值门禁通过。

---

## Phase 5: User Story 3 — 在查询中批量计算真实集合情景 (Priority: P1)

**Goal**: 将已验证 core 接入真实 DuckDB，以可保存的类型化结果完成真实数据路径及安全边界验证。

**Independent Test**: 在目标宿主加载同一扩展，真实样本经 SQL 排序/计算/保存/展开后与 direct core 对照，验证每类错误的同连接恢复、跨任务及成员边界和至少两个实际重叠的连接；最小隔离环境计算真实结果。

### Tests for User Story 3

- [X] T023 [P] [US3] Write SQL signature/result/NULL/config/binding-error/recovery assertions and a bounded harness entry in `validation/m1/verify_extension.py`; include constants, per-row configs, sliced/filtered lists, UTF-8/embedded-NUL IDs, all reachable invalid/degenerate cases and mixed-query save rollback, and confirm the absent `ens_cluster` causes the expected pre-implementation failure (after US2 checkpoint; FR-010–FR-013; SC-001, SC-002, SC-005).
- [X] T024 [P] [US3] Implement scoped nested logical-type construction, checked LIST reservation/size/offset handling, validity initialization and length-aware strings in `crates/ensabc-duckdb/src/output.rs`, following the complete `contracts/sql.md` return type; add test-only overflow/allocation-failure checks with no production debug parameter (after US2 checkpoint; FR-006, FR-010, FR-011).

### Implementation for User Story 3

- [X] T025 [US3] Implement members/config decoding and `ens_cluster` registration in `crates/ensabc-duckdb/src/cluster.rs`, special NULL handling on both arguments, checked integer/vector conversions, semantic validation through core and positioned error mapping; compute each chunk fully into owned results before invoking the T024 writer, catch callback panics and keep fit/RNG state local (after T023, T024; FR-001, FR-003, FR-009–FR-011).
- [X] T026 [US3] Register `ens_cluster` beside unchanged `ens_m0_echo` in `crates/ensabc-duckdb/src/lib.rs`, retain the existing init symbol/artifact basename, clean temporary handles on errors and add controlled FFI panic recovery tests in `crates/ensabc-duckdb/src/cluster.rs` with separately identified test builds (after T025; FR-010, FR-011, FR-015).
- [X] T027 [US3] Implement isolated-target build/packaging and runner build in `validation/m1/build_extension.sh`, honoring `--output-dir`/`ENSABC_M1_TARGET_DIR`, exact Cargo lock and M0 packaging revision/ABI; build/load on Linux ARM64, record build/link hashes and run `validation/m0/verify_extension.py --extension PATH` against the same new artifact (after T026; FR-011, FR-014, FR-015).
- [X] T028 [US3] Complete actual SQL/direct-core equality, precise nested output type checks and every valid→invalid→valid/rollback assertion in `validation/m1/verify_extension.py`, separating binder/type errors from callback errors and fully consuming outputs before accepting success; verify real M0 echo regression behavior remains intact (after T027; FR-006, FR-010, FR-011, FR-013; SC-002, SC-005).
- [X] T029 [P] [US3] Add the ordered real-input query/result-table template in `validation/m1/examples/real_sample.sql` and finish per-run rendering in `validation/m1/prepare_input.py`, keeping task/sample/time/unit/feature/source/config identities and `ORDER BY member_index`; write only the template and input-preparation files in this task, leaving end-to-end verification to T031 after T030 (after T028; FR-001, FR-011, FR-012; SC-001).
- [X] T030 [P] [US3] Add dynamic `duckdb_vector_size()` boundary cases for both task rows and nested members, varying task inputs to prevent folding, plus two independent barrier-synchronized connections with recorded overlapping query intervals and serial equivalence to `validation/m1/verify_extension.py`; retain failed/insufficient-overlap attempts and enforce complete-query failure across chunks (after T028; FR-009–FR-011; SC-004, SC-005).
- [X] T031 [US3] First verify restore→mapping→SQL→saved result→expanded summaries in `validation/m1/verify_extension.py` using T029's rendered template after T030; then implement `validation/m1/verify_minimal_runtime.sh` and its host-side helpers in `validation/m1/common.py` to run both core-only runner and real SQL clustering/recovery in a digest-pinned network-disabled ARM64 container with no reference tools; copy only declared runtime binaries/libs/sample/expected/SQL and compare complete results, recording image/artifact identity and tool absence (after T029, T030 and final real-query verification; FR-014, FR-015; SC-006).

**Checkpoint**: 完整真实查询路径、类型化输出、非法恢复、批量/并发及无参考运行验证通过。单机 SQL 成功不等于复现、测量和整体报告通过。

---

## Phase 6: User Story 4 — 按文档复现结果并判断是否可发布 (Priority: P2)

**Goal**: 形成可复现、可独立审计的 M1 验收结果及明确资源/平台范围。

**Independent Test**: 从完整 producer 记录重新计算 17 FR/8 SC 追溯及报告；对坏证据拒绝通过；在干净新 clone 重建同一有效源码并复现真实结果；资源测量可单独复核。

### Tests for User Story 4

- [X] T032 [US4] Write strict schema/traceability/report-audit negative cases in `validation/m1/test_evidence.py` for fail/not_run, missing/duplicate FR/SC, orphan assertions, synthetic-only/echo-only/shared-only false pass, hash/path/symlink mismatch, missing clean-source reproduction and cyclic/inconsistent report references; confirm current schema/auditor gaps fail before completing them (after US3 checkpoint; FR-015, FR-017; SC-008).

### Implementation for User Story 4

- [X] T033 [P] [US4] Implement 1/10/100 real-task measurements in `validation/m1/measure_tasks.py`, at least 3 repeats each, independent-PID wait4.ru_maxrss, distinct cold/query-loaded time scopes, whole-lifetime RSS disclosure, threads/shapes/bytes, medians, anti-folding inputs and serial/concurrent result checks; mark costs not_separated unless measured independently (after T032; FR-016; SC-007).
- [X] T034 [P] [US4] Implement both parent `--run-dir [--work-dir]` and child `--reproduction-child --output-dir --source-manifest --manifest --payload --config` modes in `validation/m1/reproduce_independently.py` per `contracts/evidence.md`: consume T004 allowlist/hashes, clone the same commit or temporary committed effective-source snapshot, verify clean matching source before/after, keep inputs/output/build/bytecode cache outside clone and directly invoke existing T001–T031 build/six-stage/SQL/recovery producers without gate/traceability/auditor dependency or recursion; compare baseline and child content, copy child evidence with rehashed contained refs into parent reproduction/child/, write subset-only child status plus parent independent-reproduction.json; add mode/path/source/false-whole-pass checks to this script's `--self-check`, prove completion with T037 absent, and record cache/network/same-machine limits (after T032; FR-015, FR-017; SC-008).
- [X] T035 [US4] Implement all executable FR-001–FR-017 and SC-001–SC-008 assertions in `validation/m1/traceability.py`, pointing to precise artifact fields and independently checking shape/identity/partition/summary/numeric/overlap/runtime/measurement/reproduction facts; reject omissions/orphans, derive not_run from missing producers and allow pre-report execution without circular report hashes (after T033, T034; FR-001–FR-017; SC-001–SC-008).
- [X] T036 [US4] Complete independent reference/path/hash/schema checks and overall re-derivation in `validation/m1/verify_report.py`, including original/shared stage separation, projection/Q/trace observer-check identity, initialization source indices, real clustering, 17/8 coverage and matching clean-source reproduction; rerun trace assertions rather than trusting producer statuses; implement `--self-check` and immutable report→audit hashing (after T035; FR-008, FR-015, FR-017; SC-003, SC-008).
- [X] T037 [US4] Implement `validation/m1/run_m1_gate.py` with unique run-id, all documented inputs, dependency-aware producer execution using exactly original-reference→native-core→original-comparison→shared-reference→shared-core→shared-comparison before SQL and later producers, explicit pass/fail/not_run logs, source/environment/artifact identities and traceability→report→audit ordering; exit 0 only when all three final verdicts pass, otherwise 1 for failure or 2 for incomplete; invoke T034's existing reproduce_independently.py parent mode for the reproduction producer and reuse its subset orchestration without adding a gate --reproduction-child mode or duplicating child scheduling (after T036; FR-015, FR-017; SC-008).
- [X] T038 [US4] Add final current-version build/reference/data/container/dependency feature identities to environment production in `validation/m1/setup_env.sh` and `validation/m1/run_m1_gate.py`, run all `validation/m1/test_evidence.py` and comparison self-check cases, and verify schema false-pass and required skipped-step exit codes before attempting full acceptance (after T037; FR-006, FR-008, FR-015, FR-017; SC-003, SC-008).

**Checkpoint**: producer、追溯、report 和 audit 全部可执行且坏证据不能被放行；尚未执行完整门禁时仍不能勾选整体 M1 完成。

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: 用实际命令和证据完成验收，维护文档状态，不自动发布。

- [X] T039 Audit and correct implemented command arguments/output paths and supported-platform/data-license statements in `specs/002-m1-scenario-computation/quickstart.md`, `specs/002-m1-scenario-computation/plan.md` and `README.md`; replace future-command wording only where actual implementations exist, and ensure a new user can follow examples without personal paths (after T038; FR-012, FR-015, FR-017).
- [X] T040 Execute the complete `validation/m1/run_m1_gate.py` against the frozen real sample in a new `artifacts/m1/<run-id>/`, independently rerun `validation/m1/traceability.py` and `validation/m1/verify_report.py`, verify 17/17 FR plus 8/8 SC and all mandatory outputs/reproduction/measurements; retain fail/not_run evidence and keep this task incomplete until the full gate and audit pass (after T039; FR-001–FR-017; SC-001–SC-008).
- [X] T041 Update checked task states and actual evidence/limitations in `specs/002-m1-scenario-computation/tasks.md`, `specs/002-m1-scenario-computation/spec.md`, `docs/ROADMAP.md` and `README.md` only after T040 supports the claimed conclusion; distinguish M1 algorithm/SQL availability from unverified OM/other-platform/signature/release support (after T040; FR-017; SC-008).

---

## Dependencies & Execution Order

### Phase dependencies

```text
Phase 1: T001 + (T002 || T003)
              ↓
Phase 2: T004 → T005 → T006 → T007
              ↓
US1: (T008 || T009) → T010 → T011 → T012 → T013 → T014 → T015
              ↓
US2: (T016 || T017) → T018 → T019 → T020 → T021 → T022
              ↓
US3: (T023 || T024) → T025 → T026 → T027 → T028
                                       ↓
                         (T029 || T030) → T031
              ↓
US4: T032 → (T033 || T034) → T035 → T036 → T037 → T038
              ↓
Polish: T039 → T040 → T041
```

Phase 2 保守按顺序执行；T007 的具体直接依赖为 T005，但默认调度在 T006 后，避免基础工作被漏过。T029 只写模板/prepare_input，可与 T030 并行；T031 的 real-query verification 必须等待 T030 交回 `verify_extension.py`，不得并行修改该文件。

US2 运行时阶段顺序与 T018–T022 的实现顺序对应：`run_reference --stage original → verify_core --stage native → compare_results --stage original → run_reference --stage shared → verify_core --stage shared → compare_results --stage shared`。shared 生成不反向依赖 shared 执行，省略阶段不得自动 all/递归运行；门禁和独立复现都遵守该顺序。

US2/US3/US4 在完整故事验收上分别依赖此前故事；这不是四个能独立同时开发验收的产品功能。它们各自可用独立入口测试，是依次增加的可验证交付。SQL adapter 的预研不得绕过 US2 数值门禁。T034 完全拥有 independent reproduction 父/子模式并可在 T037 不存在时验证；其子运行只调用 T001–T031 已有入口，输出外置的子集证据，不调用完整 gate、追溯、审计或递归复现。T037 单向调用 T034 父模式，不新增第二套子调度；因此 T034→T035→T036→T037 无反向依赖。子集成功不能生成完整 passed 报告。

### Within each story

1. 编写规格要求的契约/反例并确认缺失行为会失败。
2. 完成实际模块和真实数据/错误路径；每个任务执行其相关检查，遇到失败保持未完成。
3. 在故事 checkpoint 完成独立验证，保留输入、配置、源码和运行身份。
4. 才推进依赖该结果的下一故事。不因函数可编译或文档存在就勾选验收任务。

### Parallel opportunities and examples

仅在用户授权并行执行且使用独占文件所有权/隔离工作区时采用以下波次；本清单不自动启动子代理。

| 波次 | 并行任务 | 前置完成 | 边界 |
| --- | --- | --- | --- |
| Setup | T002 / T003 | T001 | setup_env.sh / .gitignore |
| US1 | T008 / T009 | T007 | Rust tests / synthetic JSON |
| US2 | T016 / T017 | T015 | Python comparator tests / Rust test-only trace |
| US3 adapter | T023 / T024 | T022 | SQL harness tests / output writer |
| US3 integration | T029 模板准备 / T030 | T028 | SQL template+prepare_input / extension verifier；验证合并顺序执行 |
| US4 | T033 / T034 | T032 | measurement / reproduction scripts，producer 产物目录各自独占 |

每个波次先完成前置 gate，再分配所列文件；不把较晚的 `[P]` 任务与未完成依赖同时启动。Cargo.lock、公共类型、shared helper 和最终报告目录统一由顺序任务维护。

## Requirement Coverage

此表为任务覆盖设计，不是运行证据。实际实现必须在 T035 用可执行断言替代仅有任务引用的覆盖声明。

| Requirement | Task IDs |
| --- | --- |
| FR-001 | T006, T007, T008, T010, T025, T029 |
| FR-002 | T008, T010, T011, T012, T013, T020 |
| FR-003 | T001, T005, T007, T008, T009, T010, T011, T012, T025 |
| FR-004 | T008, T014, T019, T028 |
| FR-005 | T008, T013, T014, T019 |
| FR-006 | T004, T005, T006, T007, T008, T011, T012, T013, T014, T015, T019, T024, T028, T038 |
| FR-007 | T001, T016, T017, T018, T019, T020, T021, T022 |
| FR-008 | T001, T005, T016, T017, T020, T021, T022, T036, T038 |
| FR-009 | T008, T012, T018, T019, T022, T025, T030 |
| FR-010 | T008, T009, T010, T011, T013, T015, T019, T023, T024, T025, T026, T028, T030 |
| FR-011 | T023, T024, T025, T026, T027, T028, T029, T030 |
| FR-012 | T003, T006, T023, T029, T039 |
| FR-013 | T008, T009, T016, T019, T023, T028 |
| FR-014 | T002, T007, T015, T027, T031 |
| FR-015 | T002, T004, T006, T007, T015, T018, T026, T027, T031, T034, T036, T037, T038, T039 |
| FR-016 | T033 |
| FR-017 | T003, T004, T005, T032, T034, T035, T036, T037, T038, T039, T040, T041 |
| SC-001 | T006, T019, T023, T029 |
| SC-002 | T008, T009, T014, T019, T028 |
| SC-003 | T016, T019, T020, T022, T036, T038 |
| SC-004 | T019, T022, T030 |
| SC-005 | T008, T009, T019, T023, T028, T030 |
| SC-006 | T031 |
| SC-007 | T033 |
| SC-008 | T032, T034, T035, T036, T037, T038, T039, T040, T041 |

## Implementation Strategy

### MVP first

完成 Setup、Foundational 和 US1（T001–T015），独立 core 可在合成输入上演示成员划分、物理均值与代表成员。此时明确标为 core MVP，不能宣称 Python 迁移正确、SQL 可用或 M1 完成。

### Incremental delivery

US2 补齐真实输入及双路径数值对照；US3 接入实际 SQL、保存、恢复、并发与隔离；US4 提供测量、干净复现、可执行追溯和审计。最后 T040 完整验收通过，再由 T041 更新阶段状态。性能、发布签名、其他平台、DuckOMO OM 或后续跨批次功能不通过扩大当前任务范围来“顺便完成”。

### Execution notes

所有 shell/脚本命令以 plan/quickstart 和 contracts/evidence 的参数为准。测试与产物只能证明明确执行的范围；失败/环境阻塞记录 reason，最终门禁以 0/1/2 区分通过/失败/不完整。用户数据和历史失败证据不静默覆盖。最终提交、外部发布或新增长期治理规则不由本任务清单代替用户决策。
