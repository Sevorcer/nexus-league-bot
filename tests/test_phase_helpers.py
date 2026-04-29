"""Tests for the format_phase_labels helper in nexus_league_bot."""
import importlib.util
import os
import sys
import unittest


def _load_format_phase_labels():
    """Load format_phase_labels from nexus_league_bot.py without running main()."""
    bot_path = os.path.join(os.path.dirname(__file__), "..", "nexus_league_bot.py")
    spec = importlib.util.spec_from_file_location("nexus_league_bot", bot_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod.format_phase_labels


# Load once at module level.
try:
    format_phase_labels = _load_format_phase_labels()
    _LOAD_ERROR = None
except Exception as exc:  # pragma: no cover
    format_phase_labels = None  # type: ignore[assignment]
    _LOAD_ERROR = exc


class TestFormatPhaseLabels(unittest.TestCase):
    """Unit tests for format_phase_labels(phase, week)."""

    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Could not import format_phase_labels: {_LOAD_ERROR}")

    # ------------------------------------------------------------------
    # Regular season (default)
    # ------------------------------------------------------------------

    def test_regular_category(self):
        result = format_phase_labels("regular", 5)
        self.assertEqual(result["category"], "Week 5 Games")

    def test_regular_prefix(self):
        result = format_phase_labels("regular", 5)
        self.assertEqual(result["prefix"], "wk5")

    def test_regular_display(self):
        result = format_phase_labels("regular", 5)
        self.assertEqual(result["display"], "Week 5")

    def test_none_defaults_to_regular(self):
        result = format_phase_labels(None, 11)
        self.assertEqual(result["category"], "Week 11 Games")
        self.assertEqual(result["prefix"], "wk11")
        self.assertEqual(result["display"], "Week 11")

    def test_empty_string_defaults_to_regular(self):
        result = format_phase_labels("", 3)
        self.assertEqual(result["category"], "Week 3 Games")

    def test_unknown_value_defaults_to_regular(self):
        result = format_phase_labels("unknown_phase", 7)
        self.assertEqual(result["category"], "Week 7 Games")

    # ------------------------------------------------------------------
    # Preseason
    # ------------------------------------------------------------------

    def test_preseason_category(self):
        result = format_phase_labels("preseason", 1)
        self.assertEqual(result["category"], "Preseason Week 1 Games")

    def test_preseason_prefix(self):
        result = format_phase_labels("preseason", 1)
        self.assertEqual(result["prefix"], "pre-wk1")

    def test_preseason_display(self):
        result = format_phase_labels("preseason", 2)
        self.assertEqual(result["display"], "Preseason Week 2")

    def test_preseason_case_insensitive(self):
        result = format_phase_labels("PRESEASON", 3)
        self.assertEqual(result["prefix"], "pre-wk3")

    # ------------------------------------------------------------------
    # Postseason
    # ------------------------------------------------------------------

    def test_postseason_category(self):
        result = format_phase_labels("postseason", 1)
        self.assertEqual(result["category"], "Postseason Week 1 Games")

    def test_postseason_prefix(self):
        result = format_phase_labels("postseason", 1)
        self.assertEqual(result["prefix"], "post-wk1")

    def test_postseason_display(self):
        result = format_phase_labels("postseason", 2)
        self.assertEqual(result["display"], "Postseason Week 2")

    def test_postseason_case_insensitive(self):
        result = format_phase_labels("POSTSEASON", 4)
        self.assertEqual(result["prefix"], "post-wk4")

    # ------------------------------------------------------------------
    # Return-type shape
    # ------------------------------------------------------------------

    def test_return_keys_present(self):
        for phase in ("preseason", "regular", "postseason"):
            with self.subTest(phase=phase):
                result = format_phase_labels(phase, 1)
                self.assertIn("category", result)
                self.assertIn("prefix", result)
                self.assertIn("display", result)

    def test_week_number_appears_in_all_fields(self):
        for phase in ("preseason", "regular", "postseason"):
            with self.subTest(phase=phase):
                result = format_phase_labels(phase, 99)
                self.assertIn("99", result["category"])
                self.assertIn("99", result["prefix"])
                self.assertIn("99", result["display"])


if __name__ == "__main__":
    unittest.main()
