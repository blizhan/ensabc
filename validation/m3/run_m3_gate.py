#!/usr/bin/env python3
"""Full M3 scenario evolution verification gate orchestrator (T045).

Usage:
    validation/m2/.venv/bin/python validation/m3/run_m3_gate.py --run-id RUN_ID [--cache-dir DIR]

Executes all 17 gate stages in deterministic acyclic order, standardizing every Python
subcommand on `validation/m2/.venv/bin/python`:
  1. `setup_env_check` -> writes `environment.json` & `source-manifest.json`
  2. `cargo_unit_and_clippy` -> isolated `CARGO_TARGET_DIR`
  3. `build_extension` -> `<run_dir>/build` (`m1_runner`, `m2_runner`, `m3_runner`, extension)
  4. `prepare_om_sample` -> `<run_dir>/sample`
  5. `unit_suites` -> `test_stability`, `test_sql_history`, `test_evidence_and_viz`
  6. `verify_evolution_core`
  7. `verify_sql_and_history` (`--scope all`)
  8. `run_case_and_sensitivity`
  9. `render_and_verify_visualization` (`real-8run-main.html`, `synthetic-showcase.html`,
     `traceability-audit.json`, `tamper-self-check.json`)
 10. `verify_minimal_runtime`
 11. `measure_pipeline`
 12. `m1_regression` (strictly sequential BEFORE `m2_regression`)
 13. `m2_regression` (strictly sequential AFTER `m1_regression`)
 14. `independent_reproduction` (`--self-check` + fresh-clone `--run-dir`)
 15. `traceability` -> writes `traceability.json`
 16. Generate `<run_dir>/report.json` (`schema_version = "m3.report.v1"`, never faking pass)
 17. Run `verify_report.py --run-dir <run_dir>` -> writes `report-audit.json`
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    LocatedError,
    M2_CACHE_DIR,
    M2_VALIDATION_DIR,
    VALIDATION_DIR,
    atomic_write_json,
    collect_effective_source_manifest,
    create_run_dir,
    load_json,
)
from validation.m3.verify_report import (
    REQUIRED_STEPS,
    compose_report_dict,
)

VENV_PY = REPO_ROOT / "validation" / "m2" / ".venv" / "bin" / "python"


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class GateStepFailure(Exception):
    """Raised when a required gate step exits non-zero."""

    def __init__(self, step: str, exit_code: int, message: str = "") -> None:
        self.step = step
        self.exit_code = exit_code
        super().__init__(message or f"Step '{step}' failed with exit code {exit_code}")


class GateRunner:
    """Deterministic step runner recording execution telemetry to `execution-steps.json`."""

    def __init__(self, run_id: str, run_dir: Path, cache_dir: Path) -> None:
        self.run_id = run_id
        self.run_dir = run_dir
        self.cache_dir = cache_dir
        self.steps_path = run_dir / "execution-steps.json"
        self.steps: list[dict[str, Any]] = []
        self._flush_steps()

    def _flush_steps(self) -> None:
        atomic_write_json(
            self.steps_path,
            {
                "schema_version": "m3.execution-steps.v1",
                "run_id": self.run_id,
                "updated_at_utc": _utc_now(),
                "steps": self.steps,
            },
        )

    def run_commands_step(
        self,
        step_name: str,
        commands: list[list[str]],
        *,
        extra_env: dict[str, str] | None = None,
        post_hook: Callable[[list[subprocess.CompletedProcess[str]]], None] | None = None,
        capture_stdout: bool = False,
        timeout: float = 3600.0,
    ) -> list[subprocess.CompletedProcess[str]]:
        if step_name not in REQUIRED_STEPS:
            raise ValueError(f"Unregistered gate step name: {step_name!r}")

        print(f"\n[{step_name}] Starting ({len(commands)} command(s))...")
        started_utc = _utc_now()
        t0 = time.perf_counter()

        record: dict[str, Any] = {
            "step": step_name,
            "command": commands[0] if len(commands) == 1 else commands,
            "started_at_utc": started_utc,
            "status": "running",
        }
        self.steps.append(record)
        self._flush_steps()

        env = os.environ.copy()
        env["ENSABC_M3_PYTHON"] = str(VENV_PY)
        env["ENSABC_M3_RUN_DIR"] = str(self.run_dir)
        if extra_env:
            env.update(extra_env)

        completed_procs: list[subprocess.CompletedProcess[str]] = []
        try:
            for cmd in commands:
                print(f"  $ {' '.join(str(c) for c in cmd)}")
                proc = subprocess.run(
                    cmd,
                    cwd=REPO_ROOT,
                    env=env,
                    capture_output=capture_stdout,
                    text=True,
                    timeout=timeout,
                    check=False,
                )
                completed_procs.append(proc)
                if proc.returncode != 0:
                    elapsed = round(time.perf_counter() - t0, 4)
                    if capture_stdout and proc.stderr:
                        print(proc.stderr, file=sys.stderr)
                    record.update(
                        completed_at_utc=_utc_now(),
                        exit_code=proc.returncode,
                        elapsed_seconds=elapsed,
                        status="not_run" if proc.returncode == 2 else "failed",
                    )
                    self._flush_steps()
                    print(
                        f"[{step_name}] FAILED (exit_code={proc.returncode}) after {elapsed:.2f}s",
                        file=sys.stderr,
                    )
                    raise GateStepFailure(step_name, proc.returncode)

            if post_hook is not None:
                post_hook(completed_procs)

            elapsed = round(time.perf_counter() - t0, 4)
            record.update(
                completed_at_utc=_utc_now(),
                exit_code=0,
                elapsed_seconds=elapsed,
                status="passed",
            )
            self._flush_steps()
            print(f"[{step_name}] PASSED in {elapsed:.2f}s")
            return completed_procs
        except GateStepFailure:
            raise
        except subprocess.TimeoutExpired as exc:
            elapsed = round(time.perf_counter() - t0, 4)
            record.update(
                completed_at_utc=_utc_now(),
                exit_code=1,
                elapsed_seconds=elapsed,
                status="failed",
                error=f"TimeoutExpired after {timeout}s: {exc}",
            )
            self._flush_steps()
            raise GateStepFailure(step_name, 1, str(exc)) from exc
        except Exception as exc:
            elapsed = round(time.perf_counter() - t0, 4)
            record.update(
                completed_at_utc=_utc_now(),
                exit_code=1,
                elapsed_seconds=elapsed,
                status="failed",
                error=str(exc),
            )
            self._flush_steps()
            raise GateStepFailure(step_name, 1, str(exc)) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run full M3 scenario evolution verification gate.")
    parser.add_argument("--run-id", required=True, help="Exclusive run identifier under artifacts/m3/<run-id>/")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=M2_CACHE_DIR,
        help="Cache directory containing DuckOMO releases and WB2 chunks (default: validation/m2/.cache)",
    )
    args = parser.parse_args(argv)

    try:
        run_dir = create_run_dir(args.run_id, allow_existing=False)
    except LocatedError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if not (VENV_PY.exists() and os.access(VENV_PY, os.X_OK)):
        print(
            f"Error: Locked Python environment missing at {VENV_PY} (run bash validation/m3/setup_env.sh)",
            file=sys.stderr,
        )
        return 2

    cache_dir = args.cache_dir.resolve()
    build_dir = run_dir / "build"
    sample_dir = run_dir / "sample"
    viz_dir = run_dir / "visualization"
    build_dir.mkdir(parents=True, exist_ok=True)
    sample_dir.mkdir(parents=True, exist_ok=True)
    viz_dir.mkdir(parents=True, exist_ok=True)

    ext_path = build_dir / "ensabc_m0_extension.duckdb_extension"
    m3_runner = build_dir / "m3_runner"
    prepare_script = (
        VALIDATION_DIR / "prepare_om_sample.py"
        if (VALIDATION_DIR / "prepare_om_sample.py").is_file()
        else M2_VALIDATION_DIR / "prepare_om_sample.py"
    )

    print("================================================================================")
    print(f"Starting M3 Scenario Evolution Verification Gate: {args.run_id}")
    print(f"Run Directory: {run_dir}")
    print(f"Python Binary: {VENV_PY}")
    print("================================================================================")

    runner = GateRunner(args.run_id, run_dir, cache_dir)
    abort_exit_code: int | None = None

    try:
        # 1. setup_env_check -> environment.json + source-manifest.json
        def _save_env_and_manifest(procs: list[subprocess.CompletedProcess[str]]) -> None:
            env_doc = json.loads(procs[0].stdout)
            atomic_write_json(run_dir / "environment.json", env_doc)
            atomic_write_json(
                run_dir / "source-manifest.json", collect_effective_source_manifest(REPO_ROOT)
            )

        runner.run_commands_step(
            "setup_env_check",
            [
                [
                    "bash",
                    str(VALIDATION_DIR / "setup_env.sh"),
                    "--check",
                    "--json",
                    "--cache-dir",
                    str(cache_dir),
                ]
            ],
            capture_stdout=True,
            post_hook=_save_env_and_manifest,
        )

        # 2. cargo_unit_and_clippy (isolated CARGO_TARGET_DIR cleaned up afterwards)
        cargo_gate_target = Path(f"/tmp/ensabc-m3-gate-cargo-{args.run_id}")
        try:
            runner.run_commands_step(
                "cargo_unit_and_clippy",
                [
                    ["cargo", "test", "--locked"],
                    [
                        "cargo",
                        "clippy",
                        "--locked",
                        "--workspace",
                        "--all-targets",
                        "--",
                        "-D",
                        "warnings",
                    ],
                ],
                extra_env={"CARGO_TARGET_DIR": str(cargo_gate_target)},
            )
        finally:
            if cargo_gate_target.exists():
                shutil.rmtree(cargo_gate_target, ignore_errors=True)

        # 3. build_extension -> <run_dir>/build
        ext_build_target = Path(f"/tmp/ensabc-m3-gate-ext-build-{args.run_id}")
        try:
            runner.run_commands_step(
                "build_extension",
                [
                    [
                        "bash",
                        str(VALIDATION_DIR / "build_extension.sh"),
                        "--output-dir",
                        str(build_dir),
                        "--target-dir",
                        str(ext_build_target),
                    ]
                ],
                extra_env={"ENSABC_M3_TARGET_DIR": str(ext_build_target)},
            )
        finally:
            if ext_build_target.exists():
                shutil.rmtree(ext_build_target, ignore_errors=True)

        # 4. prepare_om_sample
        runner.run_commands_step(
            "prepare_om_sample",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(prepare_script),
                    "--cache-dir",
                    str(cache_dir),
                    "--out-dir",
                    str(sample_dir),
                ]
            ],
        )

        # 5. unit_suites
        runner.run_commands_step(
            "unit_suites",
            [
                [
                    str(VENV_PY),
                    "-B",
                    "-m",
                    "unittest",
                    "validation.m3.test_stability",
                    "validation.m3.test_sql_history",
                    "validation.m3.test_evidence_and_viz",
                ]
            ],
        )

        # 6. verify_evolution_core
        runner.run_commands_step(
            "verify_evolution_core",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "verify_evolution_core.py"),
                    "--run-dir",
                    str(run_dir),
                    "--runner",
                    str(m3_runner),
                ]
            ],
        )

        # 7. verify_sql_and_history (--scope all)
        runner.run_commands_step(
            "verify_sql_and_history",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "verify_sql_and_history.py"),
                    "--scope",
                    "all",
                    "--run-dir",
                    str(run_dir),
                    "--sample-dir",
                    str(sample_dir),
                    "--cache-dir",
                    str(cache_dir),
                    "--extension",
                    str(ext_path),
                ]
            ],
        )

        # 8. run_case_and_sensitivity
        runner.run_commands_step(
            "run_case_and_sensitivity",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "run_case_and_sensitivity.py"),
                    "--run-dir",
                    str(run_dir),
                    "--sample-dir",
                    str(sample_dir),
                    "--cache-dir",
                    str(cache_dir),
                    "--extension",
                    str(ext_path),
                ]
            ],
        )

        # 9. render_and_verify_visualization
        real_erid = str(load_json(run_dir / "sql-history" / "real-evolution-replay.json")["evolution_run_id"])
        synth_doc = load_json(run_dir / "sql-history" / "synthetic-evolution-replay.json")
        synth_erid = str(synth_doc.get("showcase_evolution_run_id") or synth_doc["evolution_run_id"])
        db_path = run_dir / "sql-history" / "evolution-runs.duckdb"
        real_html = viz_dir / "real-8run-main.html"
        synth_html = viz_dir / "synthetic-showcase.html"
        real_audit_p = viz_dir / "real-traceability-audit.json"
        synth_audit_p = viz_dir / "synthetic-traceability-audit.json"

        def _combine_viz_audits(_: list[subprocess.CompletedProcess[str]]) -> None:
            r_aud = load_json(real_audit_p)
            s_aud = load_json(synth_audit_p)
            combined_ok = r_aud.get("status") == "pass" and s_aud.get("status") == "pass"
            atomic_write_json(
                viz_dir / "traceability-audit.json",
                {
                    "schema_version": "m3.visualization-audit.v1",
                    "status": "pass" if combined_ok else "fail",
                    "audited_at_utc": _utc_now(),
                    "views": {
                        "real_8run_main": r_aud,
                        "synthetic_showcase": s_aud,
                    },
                    "passes": r_aud.get("passes", {}),
                    "findings": [*r_aud.get("findings", []), *s_aud.get("findings", [])],
                },
            )

        runner.run_commands_step(
            "render_and_verify_visualization",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "render_evolution_html.py"),
                    "--db",
                    str(db_path),
                    "--evolution-run-id",
                    real_erid,
                    "--output",
                    str(real_html),
                ],
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "render_evolution_html.py"),
                    "--db",
                    str(db_path),
                    "--evolution-run-id",
                    synth_erid,
                    "--output",
                    str(synth_html),
                ],
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "verify_visualization_traceability.py"),
                    "--html",
                    str(real_html),
                    "--db",
                    str(db_path),
                    "--evolution-run-id",
                    real_erid,
                    "--out-report",
                    str(real_audit_p),
                ],
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "verify_visualization_traceability.py"),
                    "--html",
                    str(synth_html),
                    "--db",
                    str(db_path),
                    "--evolution-run-id",
                    synth_erid,
                    "--out-report",
                    str(synth_audit_p),
                ],
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "verify_visualization_traceability.py"),
                    "--self-check",
                    "--out-report",
                    str(viz_dir / "tamper-self-check.json"),
                ],
            ],
            post_hook=_combine_viz_audits,
        )

        # 10. verify_minimal_runtime
        runner.run_commands_step(
            "verify_minimal_runtime",
            [
                [
                    "bash",
                    str(VALIDATION_DIR / "verify_minimal_runtime.sh"),
                    "--run-dir",
                    str(run_dir),
                    "--extension",
                    str(ext_path),
                    "--runner",
                    str(m3_runner),
                    "--sample-dir",
                    str(sample_dir),
                    "--cache-dir",
                    str(cache_dir),
                ]
            ],
        )

        # 11. measure_pipeline
        runner.run_commands_step(
            "measure_pipeline",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "measure_pipeline.py"),
                    "--run-dir",
                    str(run_dir),
                    "--extension",
                    str(ext_path),
                    "--runner",
                    str(m3_runner),
                    "--sample-dir",
                    str(sample_dir),
                    "--cache-dir",
                    str(cache_dir),
                ]
            ],
        )

        # 12. m1_regression — MUST run sequentially BEFORE m2_regression!
        runner.run_commands_step(
            "m1_regression",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "run_m1_regression.py"),
                    "--run-dir",
                    str(run_dir),
                ]
            ],
        )

        # 13. m2_regression — sequential AFTER m1_regression!
        runner.run_commands_step(
            "m2_regression",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "run_m2_regression.py"),
                    "--run-dir",
                    str(run_dir),
                ]
            ],
        )

        # 14. independent_reproduction (--self-check + fresh-clone reproduction)
        runner.run_commands_step(
            "independent_reproduction",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "reproduce_independently.py"),
                    "--self-check",
                    "--out-report",
                    str(run_dir / "reproduction" / "self-check.json"),
                ],
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "reproduce_independently.py"),
                    "--run-dir",
                    str(run_dir),
                    "--sample-dir",
                    str(sample_dir),
                    "--cache-dir",
                    str(cache_dir),
                ],
            ],
        )

        # 15. traceability -> writes traceability.json and verifies --check mode
        runner.run_commands_step(
            "traceability",
            [
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "traceability.py"),
                    "--run-dir",
                    str(run_dir),
                ],
                [
                    str(VENV_PY),
                    "-B",
                    str(VALIDATION_DIR / "traceability.py"),
                    "--run-dir",
                    str(run_dir),
                    "--check",
                ],
            ],
        )
    except GateStepFailure as exc:
        abort_exit_code = 2 if exc.exit_code == 2 else 1
        # Ensure traceability.json exists with honest fail/not_run states before writing report.json
        if not (run_dir / "traceability.json").is_file():
            subprocess.run(
                [str(VENV_PY), "-B", str(VALIDATION_DIR / "traceability.py"), "--run-dir", str(run_dir)],
                cwd=REPO_ROOT,
                check=False,
            )

    # 16. Generate <run_dir>/report.json (never faking pass!)
    report_doc = compose_report_dict(run_dir)
    atomic_write_json(run_dir / "report.json", report_doc)

    # 17. Run verify_report.py --run-dir <run_dir> -> writes report-audit.json
    audit_proc = subprocess.run(
        [str(VENV_PY), "-B", str(VALIDATION_DIR / "verify_report.py"), "--run-dir", str(run_dir)],
        cwd=REPO_ROOT,
        check=False,
    )
    audit_doc = load_json(run_dir / "report-audit.json") if (run_dir / "report-audit.json").is_file() else {}

    if abort_exit_code is not None:
        return abort_exit_code

    if (
        report_doc.get("overall_status") == "passed"
        and audit_proc.returncode == 0
        and audit_doc.get("audit_status") == "passed"
        and audit_doc.get("findings") == []
    ):
        print("\n================================================================================")
        print(f"M3 VERIFICATION GATE PASSED: {args.run_id}")
        print(f"  overall_status: {report_doc['overall_status']}")
        print(f"  audit_status:   {audit_doc['audit_status']} (0 findings)")
        print(f"  artifacts:      {run_dir}")
        print("================================================================================")
        return 0

    print(
        f"\nM3 VERIFICATION GATE FAILED: overall_status={report_doc.get('overall_status')!r}, "
        f"audit_status={audit_doc.get('audit_status')!r}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
