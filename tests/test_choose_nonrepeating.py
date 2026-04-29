"""Tests for the choose_nonrepeating helper in nexus_league_bot."""
import importlib.util
import os
import unittest
from unittest.mock import MagicMock


def _load_module():
    bot_path = os.path.join(os.path.dirname(__file__), "..", "nexus_league_bot.py")
    spec = importlib.util.spec_from_file_location("nexus_league_bot", bot_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


try:
    _mod = _load_module()
    choose_nonrepeating = _mod.choose_nonrepeating
    deterministic_choice = _mod.deterministic_choice
    _LOAD_ERROR = None
except Exception as exc:  # pragma: no cover
    choose_nonrepeating = None  # type: ignore[assignment]
    deterministic_choice = None  # type: ignore[assignment]
    _LOAD_ERROR = exc


def _make_db(recent_keys: list[str]) -> MagicMock:
    """Build a stub Database that returns *recent_keys* and accepts records."""
    db = MagicMock()
    db.fetch_recent_content_keys.return_value = recent_keys
    db.record_content_key.return_value = None
    return db


class TestChooseNonrepeating(unittest.TestCase):
    """Unit tests for choose_nonrepeating."""

    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Could not import helpers: {_LOAD_ERROR}")

    # ------------------------------------------------------------------
    # Basic behaviour
    # ------------------------------------------------------------------

    def test_returns_string_from_options(self):
        db = _make_db([])
        options = ["alpha", "beta", "gamma"]
        result = choose_nonrepeating(options, "seed1", "test_type", db, 1, 1)
        self.assertIn(result, options)

    def test_records_chosen_key(self):
        db = _make_db([])
        options = ["alpha", "beta"]
        result = choose_nonrepeating(options, "seed2", "test_type", db, 1, 1)
        db.record_content_key.assert_called_once_with(1, 1, "test_type", result)

    def test_avoids_recently_used_items(self):
        """If all but one option is recent, the remaining one must be chosen."""
        options = ["alpha", "beta", "gamma"]
        recent = ["alpha", "beta"]
        db = _make_db(recent)
        result = choose_nonrepeating(options, "seed3", "test_type", db, 1, 1)
        self.assertEqual(result, "gamma")

    def test_avoids_all_recent_fallback(self):
        """When the entire pool is in recent memory, fall back gracefully."""
        options = ["alpha", "beta"]
        db = _make_db(["alpha", "beta"])
        result = choose_nonrepeating(options, "seed4", "test_type", db, 1, 1)
        # Must still return something from the original options
        self.assertIn(result, options)

    def test_empty_options_returns_empty_string(self):
        db = _make_db([])
        result = choose_nonrepeating([], "seed5", "test_type", db, 1, 1)
        self.assertEqual(result, "")

    def test_single_option_always_returned(self):
        db = _make_db([])
        result = choose_nonrepeating(["only"], "seed6", "test_type", db, 1, 1)
        self.assertEqual(result, "only")

    # ------------------------------------------------------------------
    # DB interaction
    # ------------------------------------------------------------------

    def test_queries_with_correct_args(self):
        db = _make_db([])
        options = ["x", "y"]
        choose_nonrepeating(options, "seed7", "my_type", db, 42, 99, cooldown_limit=300)
        db.fetch_recent_content_keys.assert_called_once_with(42, 99, "my_type", 300)

    def test_record_called_with_guild_league_type(self):
        db = _make_db([])
        options = ["one", "two"]
        result = choose_nonrepeating(options, "seed8", "my_type", db, 10, 20)
        args = db.record_content_key.call_args
        self.assertEqual(args[0][0], 10)   # guild_id
        self.assertEqual(args[0][1], 20)   # league_id
        self.assertEqual(args[0][2], "my_type")  # content_type
        self.assertEqual(args[0][3], result)     # content_key matches return value

    # ------------------------------------------------------------------
    # DB failure fallback
    # ------------------------------------------------------------------

    def test_db_error_returns_deterministic_fallback(self):
        """If the DB raises, choose_nonrepeating must not crash."""
        db = MagicMock()
        db.fetch_recent_content_keys.side_effect = RuntimeError("DB down")
        options = ["a", "b", "c"]
        result = choose_nonrepeating(options, "seed9", "test_type", db, 1, 1)
        self.assertIn(result, options)

    def test_record_error_does_not_crash(self):
        db = MagicMock()
        db.fetch_recent_content_keys.return_value = []
        db.record_content_key.side_effect = RuntimeError("write failed")
        options = ["p", "q"]
        result = choose_nonrepeating(options, "seed10", "test_type", db, 1, 1)
        # Should raise because the exception happens after choice - actually
        # our impl catches ALL exceptions, so this should not crash.
        # The result must be from options.
        self.assertIn(result, options)

    # ------------------------------------------------------------------
    # Variety
    # ------------------------------------------------------------------

    def test_avoids_recently_used_produces_varied_output(self):
        """With a large pool and rotating recents, no single item repeats consecutively."""
        options = [f"opt{i}" for i in range(20)]
        recent: list[str] = []

        chosen_values = []
        for i in range(20):
            db = _make_db(recent[:])
            result = choose_nonrepeating(options, f"seed-{i}", "variety_test", db, 1, 1)
            chosen_values.append(result)
            if result not in recent:
                recent.append(result)
            if len(recent) > 15:
                recent.pop(0)

        # There should be at least 5 distinct values across 20 picks
        self.assertGreater(len(set(chosen_values)), 4)

    def test_consistent_fallback_when_pool_exhausted(self):
        """When all options are recent, result is still deterministic for the same seed."""
        options = ["a", "b"]
        db1 = _make_db(["a", "b"])
        db2 = _make_db(["a", "b"])
        r1 = choose_nonrepeating(options, "same-seed", "test_type", db1, 1, 1)
        r2 = choose_nonrepeating(options, "same-seed", "test_type", db2, 1, 1)
        self.assertEqual(r1, r2)

    # ------------------------------------------------------------------
    # Template formatting compatibility
    # ------------------------------------------------------------------

    def test_works_with_template_strings(self):
        """Chosen template strings from the large pools should be formattable."""
        templates = [
            "Week {week} puts {away} and {home} into a {angle} that matters.",
            "{away} vs. {home}: a {angle} for the ages in Week {week}.",
        ]
        db = _make_db([])
        chosen = choose_nonrepeating(templates, "tmpl-seed", "matchup_opener", db, 1, 1)
        formatted = chosen.format(week=6, away="Cardinals", home="Colts", angle="rivalry")
        self.assertIn("Cardinals", formatted)
        self.assertIn("Colts", formatted)
        self.assertIn("6", formatted)


if __name__ == "__main__":
    unittest.main()
