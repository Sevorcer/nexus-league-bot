"""Tests for the delete_weekly_channels slash command."""
import asyncio
import importlib.util
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


def _load_module():
    bot_path = os.path.join(os.path.dirname(__file__), "..", "nexus_league_bot.py")
    spec = importlib.util.spec_from_file_location("nexus_league_bot", bot_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


try:
    _mod = _load_module()
    match_weekly_channel_names = _mod.match_weekly_channel_names
    format_phase_labels = _mod.format_phase_labels
    _LOAD_ERROR = None
except Exception as exc:  # pragma: no cover
    match_weekly_channel_names = None  # type: ignore[assignment]
    format_phase_labels = None  # type: ignore[assignment]
    _LOAD_ERROR = exc


def _make_channel(name: str) -> MagicMock:
    ch = MagicMock()
    ch.name = name
    ch.delete = AsyncMock()
    return ch


class TestMatchWeeklyChannelNames(unittest.TestCase):
    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Failed to load nexus_league_bot: {_LOAD_ERROR}")

    def test_regular_season_matches_wk_prefix(self):
        channels = [
            _make_channel("wk3-eagles-vs-cowboys"),
            _make_channel("wk3-chiefs-vs-bills"),
            _make_channel("wk4-broncos-vs-raiders"),
            _make_channel("general"),
        ]
        result = match_weekly_channel_names(channels, week=3)
        names = [ch.name for ch in result]
        self.assertIn("wk3-eagles-vs-cowboys", names)
        self.assertIn("wk3-chiefs-vs-bills", names)
        self.assertNotIn("wk4-broncos-vs-raiders", names)
        self.assertNotIn("general", names)

    def test_preseason_matches_pre_wk_prefix(self):
        channels = [
            _make_channel("pre-wk1-team-a-vs-team-b"),
            _make_channel("wk1-eagles-vs-cowboys"),
        ]
        result = match_weekly_channel_names(channels, week=1, phase="preseason")
        names = [ch.name for ch in result]
        self.assertIn("pre-wk1-team-a-vs-team-b", names)
        self.assertNotIn("wk1-eagles-vs-cowboys", names)

    def test_postseason_matches_post_wk_prefix(self):
        channels = [
            _make_channel("post-wk2-semifinal"),
            _make_channel("wk2-regular-game"),
        ]
        result = match_weekly_channel_names(channels, week=2, phase="postseason")
        names = [ch.name for ch in result]
        self.assertIn("post-wk2-semifinal", names)
        self.assertNotIn("wk2-regular-game", names)

    def test_no_match_returns_empty(self):
        channels = [_make_channel("general"), _make_channel("announcements")]
        result = match_weekly_channel_names(channels, week=5)
        self.assertEqual(result, [])

    def test_empty_channel_list_returns_empty(self):
        result = match_weekly_channel_names([], week=3)
        self.assertEqual(result, [])

    def test_none_phase_defaults_to_regular(self):
        channels = [_make_channel("wk7-game-channel")]
        result = match_weekly_channel_names(channels, week=7, phase=None)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "wk7-game-channel")


class TestDeleteWeeklyChannelsIntegration(unittest.TestCase):
    """Smoke tests for the delete_weekly_channels command logic."""

    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Failed to load nexus_league_bot: {_LOAD_ERROR}")

    def test_match_then_delete_flow(self):
        channels = [
            _make_channel("wk3-eagles-vs-cowboys"),
            _make_channel("wk3-chiefs-vs-bills"),
            _make_channel("general"),
        ]
        matched = match_weekly_channel_names(channels, week=3)
        self.assertEqual(len(matched), 2)

        async def _run():
            for ch in matched:
                await ch.delete(reason="test")

        asyncio.run(_run())
        for ch in matched:
            ch.delete.assert_called_once()


if __name__ == "__main__":
    unittest.main()
