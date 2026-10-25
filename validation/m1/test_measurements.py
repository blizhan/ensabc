"""Check measurement boundaries without requiring the extension."""
import unittest
from pathlib import Path
from unittest.mock import patch

from validation.m1.common import M1Error
from validation.m1.measure_tasks import _run_measured_process, run_warm_measurement


class MeasurementTests(unittest.TestCase):
    def test_child_rss_and_output(self):
        elapsed, rss, output = _run_measured_process("SELECT 42;", 10)
        self.assertGreater(elapsed, 0)
        self.assertGreater(rss, 0)
        self.assertIn("42", output)

    def test_warm_uses_query_timer(self):
        with patch("validation.m1.measure_tasks._run_measured_process", return_value=(9.0, 1234, "Run Time (s): real 0.012 user 0 sys 0\n")) as run:
            elapsed, rss, _ = run_warm_measurement(Path("/tmp/example.duckdb_extension"), "SELECT 1;")
        self.assertEqual(elapsed, 0.012)
        self.assertEqual(rss, 1234)
        script = run.call_args.args[0]
        self.assertLess(script.index("LOAD"), script.index(".timer on"))

    def test_missing_timer_fails(self):
        with patch("validation.m1.measure_tasks._run_measured_process", return_value=(9.0, 1234, "[]")):
            with self.assertRaises(M1Error):
                run_warm_measurement(Path("/tmp/example.duckdb_extension"), "SELECT 1;")
