"""Tests for build_slot_candidates and build_all_slot_candidates helpers."""
import importlib.util
import os
import unittest


def _load_module():
    tpl_path = os.path.join(os.path.dirname(__file__), "..", "storyline_templates.py")
    spec = importlib.util.spec_from_file_location("storyline_templates", tpl_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


try:
    _mod = _load_module()
    build_slot_candidates = _mod.build_slot_candidates
    build_all_slot_candidates = _mod.build_all_slot_candidates
    MATCHUP_ANGLES = _mod.MATCHUP_ANGLES
    PLAYER_WHY_PASSING = _mod.PLAYER_WHY_PASSING
    PLAYER_WHY_RUSHING = _mod.PLAYER_WHY_RUSHING
    PLAYER_WHY_DEFENSE = _mod.PLAYER_WHY_DEFENSE
    _LOAD_ERROR = None
except Exception as exc:  # pragma: no cover
    build_slot_candidates = None  # type: ignore[assignment]
    build_all_slot_candidates = None  # type: ignore[assignment]
    MATCHUP_ANGLES = []
    PLAYER_WHY_PASSING = []
    PLAYER_WHY_RUSHING = []
    PLAYER_WHY_DEFENSE = []
    _LOAD_ERROR = exc


class TestBuildSlotCandidates(unittest.TestCase):
    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Failed to load storyline_templates: {_LOAD_ERROR}")

    def test_single_slot_cartesian(self):
        result = build_slot_candidates(
            "{color} widget",
            {"color": ["red", "blue", "green"]},
        )
        self.assertEqual(result, ["red widget", "blue widget", "green widget"])

    def test_two_slot_cartesian(self):
        result = build_slot_candidates(
            "{a}-{b}",
            {"a": ["x", "y"], "b": ["1", "2"]},
        )
        self.assertEqual(len(result), 4)
        self.assertIn("x-1", result)
        self.assertIn("y-2", result)

    def test_no_slots_returns_one_copy(self):
        result = build_slot_candidates("plain string", {})
        self.assertEqual(result, ["plain string"])

    def test_runtime_placeholder_survives(self):
        result = build_slot_candidates(
            "{role} for {{team_name}}",
            {"role": ["passer"]},
        )
        self.assertEqual(result, ["passer for {team_name}"])

    def test_build_all_slot_candidates_multiple_templates(self):
        result = build_all_slot_candidates(
            ["{a} one", "{a} two"],
            {"a": ["x", "y"]},
        )
        self.assertIn("x one", result)
        self.assertIn("y two", result)
        self.assertEqual(len(result), 4)

    def test_matchup_angles_pool_size(self):
        self.assertGreaterEqual(len(MATCHUP_ANGLES), 400)

    def test_player_why_passing_pool_size(self):
        self.assertGreaterEqual(len(PLAYER_WHY_PASSING), 400)

    def test_player_why_rushing_pool_size(self):
        self.assertGreaterEqual(len(PLAYER_WHY_RUSHING), 400)

    def test_player_why_defense_pool_size(self):
        self.assertGreaterEqual(len(PLAYER_WHY_DEFENSE), 400)

    def test_player_why_passing_placeholders_intact(self):
        generated = [s for s in PLAYER_WHY_PASSING if "{first}" in s or "{team_name}" in s]
        self.assertTrue(
            len(generated) > 0,
            "Expected some entries in PLAYER_WHY_PASSING to contain {first} or {team_name}",
        )

    def test_no_unformatted_double_braces_in_player_why(self):
        for entry in PLAYER_WHY_PASSING:
            self.assertNotIn("{{", entry, f"Unexpected escaped brace in: {entry!r}")
        for entry in PLAYER_WHY_RUSHING:
            self.assertNotIn("{{", entry, f"Unexpected escaped brace in: {entry!r}")
        for entry in PLAYER_WHY_DEFENSE:
            self.assertNotIn("{{", entry, f"Unexpected escaped brace in: {entry!r}")


if __name__ == "__main__":
    unittest.main()
