"""Tests for match_weekly_channel_names pure helper."""
import importlib.util
import os
import unittest
from unittest.mock import AsyncMock, MagicMock


def _load_module():
    bot_path = os.path.join(os.path.dirname(__file__), "..", "nexus_league_bot.py")
    spec = importlib.util.spec_from_file_location("nexus_league_bot", bot_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


try:
    _mod = _load_module()
    match_weekly_channel_names = _mod.match_weekly_channel_names
    _LOAD_ERROR = None
except Exception as exc:  # pragma: no cover
    match_weekly_channel_names = None  # type: ignore[assignment]
    _LOAD_ERROR = exc


def _ch(name: str) -> MagicMock:
    """Build a minimal mock channel with a .name attribute."""
    ch = MagicMock()
    ch.name = name
    ch.delete = AsyncMock()
    return ch


class TestMatchWeeklyChannelNamesRegular(unittest.TestCase):
    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Could not import helpers: {_LOAD_ERROR}")

    def test_matches_wk_prefix(self):
        channels = [_ch("wk6-cardinals-vs-colts"), _ch("general-chat")]
        result = match_weekly_channel_names(channels, week=6)
        self.assertEqual([c.name for c in result], ["wk6-cardinals-vs-colts"])

    def test_matches_gotw_wk_prefix(self):
        channels = [_ch("gotw-wk6-chiefs-vs-ravens"), _ch("wk6-eagles-vs-cowboys")]
        result = match_weekly_channel_names(channels, week=6)
        names = [c.name for c in result]
        self.assertIn("gotw-wk6-chiefs-vs-ravens", names)
        self.assertIn("wk6-eagles-vs-cowboys", names)

    def test_does_not_match_different_week(self):
        channels = [_ch("wk5-bills-vs-dolphins"), _ch("wk6-cardinals-vs-colts")]
        result = match_weekly_channel_names(channels, week=6)
        names = [c.name for c in result]
        self.assertNotIn("wk5-bills-vs-dolphins", names)

    def test_none_phase_defaults_to_regular(self):
        channels = [_ch("wk3-a-vs-b")]
        result = match_weekly_channel_names(channels, week=3, phase=None)
        self.assertEqual(len(result), 1)

    def test_no_trailing_dash_not_matched(self):
        """'wk6' without trailing '-X' must not match (avoids wk60 collision)."""
        channels = [_ch("wk6"), _ch("wk60-a-vs-b")]
        result = match_weekly_channel_names(channels, week=6)
        names = [c.name for c in result]
        self.assertNotIn("wk6", names)
        self.assertNotIn("wk60-a-vs-b", names)

    def test_empty_list_returns_empty(self):
        self.assertEqual(match_weekly_channel_names([], week=6), [])

    def test_no_matches_returns_empty(self):
        channels = [_ch("general"), _ch("announcements")]
        self.assertEqual(match_weekly_channel_names(channels, week=4), [])

    def test_preserves_order(self):
        channels = [_ch("wk3-b"), _ch("wk3-a"), _ch("wk3-c")]
        result = match_weekly_channel_names(channels, week=3)
        self.assertEqual([c.name for c in result], ["wk3-b", "wk3-a", "wk3-c"])

    def test_case_sensitive(self):
        """Channel names are lowercase in Discord; uppercase prefix must not match."""
        channels = [_ch("WK6-cardinals-vs-colts")]
        result = match_weekly_channel_names(channels, week=6)
        self.assertEqual(result, [])


class TestMatchWeeklyChannelNamesPreseason(unittest.TestCase):
    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Could not import helpers: {_LOAD_ERROR}")

    def test_matches_pre_wk_prefix(self):
        channels = [_ch("pre-wk2-team-a-vs-team-b"), _ch("wk2-other")]
        result = match_weekly_channel_names(channels, week=2, phase="preseason")
        names = [c.name for c in result]
        self.assertIn("pre-wk2-team-a-vs-team-b", names)
        self.assertNotIn("wk2-other", names)

    def test_matches_gotw_pre_wk_prefix(self):
        channels = [_ch("gotw-pre-wk2-alpha-vs-beta")]
        result = match_weekly_channel_names(channels, week=2, phase="preseason")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "gotw-pre-wk2-alpha-vs-beta")

    def test_does_not_match_regular_prefix(self):
        channels = [_ch("wk2-regular-game"), _ch("pre-wk2-pre-game")]
        result = match_weekly_channel_names(channels, week=2, phase="preseason")
        names = [c.name for c in result]
        self.assertNotIn("wk2-regular-game", names)


class TestMatchWeeklyChannelNamesPostseason(unittest.TestCase):
    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Could not import helpers: {_LOAD_ERROR}")

    def test_matches_post_wk_prefix(self):
        channels = [_ch("post-wk1-team-x-vs-team-y"), _ch("wk1-regular")]
        result = match_weekly_channel_names(channels, week=1, phase="postseason")
        names = [c.name for c in result]
        self.assertIn("post-wk1-team-x-vs-team-y", names)
        self.assertNotIn("wk1-regular", names)

    def test_matches_gotw_post_wk_prefix(self):
        channels = [_ch("gotw-post-wk1-alpha-vs-beta")]
        result = match_weekly_channel_names(channels, week=1, phase="postseason")
        self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
