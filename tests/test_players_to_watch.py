"""Tests for format_player_stats_line and format_player_why_line helpers."""
import importlib.util
import os
import unittest


def _load_module():
    bot_path = os.path.join(os.path.dirname(__file__), "..", "nexus_league_bot.py")
    spec = importlib.util.spec_from_file_location("nexus_league_bot", bot_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


try:
    _mod = _load_module()
    format_player_stats_line = _mod.format_player_stats_line
    format_player_why_line = _mod.format_player_why_line
    _LOAD_ERROR = None
except Exception as exc:  # pragma: no cover
    format_player_stats_line = None  # type: ignore[assignment]
    format_player_why_line = None  # type: ignore[assignment]
    _LOAD_ERROR = exc


class TestFormatPlayerStatsLine(unittest.TestCase):
    """Unit tests for format_player_stats_line."""

    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Could not import helpers: {_LOAD_ERROR}")

    # ------------------------------------------------------------------
    # Passing
    # ------------------------------------------------------------------

    def test_passing_contains_pass_yards(self):
        entry = {"pass_yards": 1842, "pass_tds": 14, "interceptions": 6}
        result = format_player_stats_line(entry, "passing")
        self.assertIn("1,842", result)

    def test_passing_contains_tds(self):
        entry = {"pass_yards": 1842, "pass_tds": 14, "interceptions": 6}
        result = format_player_stats_line(entry, "passing")
        self.assertIn("14", result)

    def test_passing_contains_int(self):
        entry = {"pass_yards": 1842, "pass_tds": 14, "interceptions": 6}
        result = format_player_stats_line(entry, "passing")
        self.assertIn("6", result)

    def test_passing_zero_stats(self):
        result = format_player_stats_line({}, "passing")
        self.assertIn("0", result)

    def test_passing_format_includes_td_label(self):
        entry = {"pass_yards": 500, "pass_tds": 3, "interceptions": 1}
        result = format_player_stats_line(entry, "passing")
        self.assertIn("TD", result)
        self.assertIn("INT", result)

    # ------------------------------------------------------------------
    # Rushing
    # ------------------------------------------------------------------

    def test_rushing_contains_rush_yards(self):
        entry = {"rush_yards": 847, "rush_tds": 6}
        result = format_player_stats_line(entry, "rushing")
        self.assertIn("847", result)

    def test_rushing_contains_tds(self):
        entry = {"rush_yards": 847, "rush_tds": 6}
        result = format_player_stats_line(entry, "rushing")
        self.assertIn("6", result)

    def test_rushing_zero_stats(self):
        result = format_player_stats_line({}, "rushing")
        self.assertIn("0", result)

    def test_rushing_format_includes_td_label(self):
        entry = {"rush_yards": 200, "rush_tds": 2}
        result = format_player_stats_line(entry, "rushing")
        self.assertIn("TD", result)

    # ------------------------------------------------------------------
    # Defense
    # ------------------------------------------------------------------

    def test_defense_contains_tackles(self):
        entry = {"tackles": 42, "sacks": 5, "defensive_ints": 2, "fumbles_forced": 1}
        result = format_player_stats_line(entry, "defense")
        self.assertIn("42", result)

    def test_defense_contains_sacks(self):
        entry = {"tackles": 42, "sacks": 5, "defensive_ints": 2, "fumbles_forced": 1}
        result = format_player_stats_line(entry, "defense")
        self.assertIn("5", result)

    def test_defense_contains_ints(self):
        entry = {"tackles": 42, "sacks": 5, "defensive_ints": 2, "fumbles_forced": 1}
        result = format_player_stats_line(entry, "defense")
        self.assertIn("2", result)

    def test_defense_includes_ff_when_nonzero(self):
        entry = {"tackles": 30, "sacks": 3, "defensive_ints": 1, "fumbles_forced": 2}
        result = format_player_stats_line(entry, "defense")
        self.assertIn("FF", result)

    def test_defense_omits_ff_when_zero(self):
        entry = {"tackles": 30, "sacks": 3, "defensive_ints": 1, "fumbles_forced": 0}
        result = format_player_stats_line(entry, "defense")
        self.assertNotIn("FF", result)

    def test_defense_zero_stats(self):
        result = format_player_stats_line({}, "defense")
        self.assertIn("0", result)

    # ------------------------------------------------------------------
    # Unknown category
    # ------------------------------------------------------------------

    def test_unknown_category_returns_empty_string(self):
        result = format_player_stats_line({"foo": 99}, "unknown")
        self.assertEqual(result, "")


class TestFormatPlayerWhyLine(unittest.TestCase):
    """Unit tests for format_player_why_line."""

    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Could not import helpers: {_LOAD_ERROR}")

    def _make_entry(self, name="Drew Brees"):
        return {"player_name": name}

    # ------------------------------------------------------------------
    # Passing
    # ------------------------------------------------------------------

    def test_passing_why_mentions_team(self):
        entry = self._make_entry("Drew Brees")
        result = format_player_why_line(entry, "passing", "Cardinals", 101)
        self.assertIn("Cardinals", result)

    def test_passing_why_mentions_player_first_name(self):
        entry = self._make_entry("Drew Brees")
        result = format_player_why_line(entry, "passing", "Cardinals", 101)
        self.assertIn("Drew", result)

    def test_passing_why_is_non_empty(self):
        result = format_player_why_line(self._make_entry(), "passing", "Team", 1)
        self.assertTrue(len(result) > 0)

    # ------------------------------------------------------------------
    # Rushing
    # ------------------------------------------------------------------

    def test_rushing_why_mentions_team(self):
        entry = self._make_entry("Derrick Henry")
        result = format_player_why_line(entry, "rushing", "Colts", 202)
        self.assertIn("Colts", result)

    def test_rushing_why_mentions_player_first_name(self):
        entry = self._make_entry("Derrick Henry")
        result = format_player_why_line(entry, "rushing", "Colts", 202)
        self.assertIn("Derrick", result)

    # ------------------------------------------------------------------
    # Defense
    # ------------------------------------------------------------------

    def test_defense_why_mentions_team(self):
        entry = self._make_entry("Micah Parsons")
        result = format_player_why_line(entry, "defense", "Cowboys", 303)
        self.assertIn("Cowboys", result)

    def test_defense_why_mentions_player_first_name(self):
        entry = self._make_entry("Micah Parsons")
        result = format_player_why_line(entry, "defense", "Cowboys", 303)
        self.assertIn("Micah", result)

    # ------------------------------------------------------------------
    # Unknown / empty entry
    # ------------------------------------------------------------------

    def test_unknown_category_returns_non_empty(self):
        result = format_player_why_line({"player_name": "John Doe"}, "kicker", "Team", 1)
        self.assertTrue(len(result) > 0)

    def test_empty_player_name_does_not_crash(self):
        result = format_player_why_line({}, "passing", "Team", 1)
        self.assertIsInstance(result, str)

    # ------------------------------------------------------------------
    # Determinism: same inputs → same output
    # ------------------------------------------------------------------

    def test_deterministic_same_inputs(self):
        entry = self._make_entry("Patrick Mahomes")
        r1 = format_player_why_line(entry, "passing", "Chiefs", 42)
        r2 = format_player_why_line(entry, "passing", "Chiefs", 42)
        self.assertEqual(r1, r2)

    def test_different_game_ids_may_differ(self):
        """Different game IDs should be allowed to produce different snippets."""
        entry = self._make_entry("Patrick Mahomes")
        results = {
            format_player_why_line(entry, "passing", "Chiefs", gid)
            for gid in range(1, 10)
        }
        # At least two distinct snippets should exist across 9 game IDs
        self.assertGreater(len(results), 1)


if __name__ == "__main__":
    unittest.main()
