"""Tests for the cleanup_content_memory method on Database."""
import importlib.util
import os
import unittest
from unittest.mock import MagicMock, call, patch


def _load_module():
    bot_path = os.path.join(os.path.dirname(__file__), "..", "nexus_league_bot.py")
    spec = importlib.util.spec_from_file_location("nexus_league_bot", bot_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


try:
    _mod = _load_module()
    Database = _mod.Database
    _LOAD_ERROR = None
except Exception as exc:  # pragma: no cover
    Database = None  # type: ignore[assignment]
    _LOAD_ERROR = exc


def _make_db() -> "Database":
    db = Database.__new__(Database)
    db.dsn = "postgresql://fake"
    return db


class TestCleanupContentMemory(unittest.TestCase):
    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Failed to load nexus_league_bot: {_LOAD_ERROR}")

    def _mock_conn(self):
        """Return a mock connection context manager."""
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cur.__enter__ = MagicMock(return_value=mock_cur)
        mock_cur.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cur
        return mock_conn, mock_cur

    def test_cleanup_executes_delete_query(self):
        db = _make_db()
        mock_conn, mock_cur = self._mock_conn()
        with patch.object(db, "conn", return_value=mock_conn):
            db.cleanup_content_memory(111, 222, "matchup_angle", keep=100)
        mock_cur.execute.assert_called_once()
        args = mock_cur.execute.call_args[0]
        self.assertIn("DELETE", args[0])
        self.assertEqual(args[1], (111, 222, "matchup_angle", 100))

    def test_cleanup_calls_commit(self):
        db = _make_db()
        mock_conn, mock_cur = self._mock_conn()
        with patch.object(db, "conn", return_value=mock_conn):
            db.cleanup_content_memory(1, 1, "profile_contender")
        mock_conn.commit.assert_called_once()

    def test_record_content_key_triggers_cleanup_probabilistically(self):
        db = _make_db()
        mock_conn, mock_cur = self._mock_conn()
        cleanup_calls = []

        def fake_cleanup(guild_id, league_id, content_type, keep=200):
            cleanup_calls.append((guild_id, league_id, content_type))

        with patch.object(db, "conn", return_value=mock_conn), \
             patch.object(db, "cleanup_content_memory", side_effect=fake_cleanup), \
             patch("random.random", return_value=0.005):
            db.record_content_key(10, 20, "matchup_angle", "key-abc")

        self.assertEqual(len(cleanup_calls), 1)
        self.assertEqual(cleanup_calls[0], (10, 20, "matchup_angle"))

    def test_record_content_key_skips_cleanup_when_random_high(self):
        db = _make_db()
        mock_conn, mock_cur = self._mock_conn()
        cleanup_calls = []

        def fake_cleanup(guild_id, league_id, content_type, keep=200):
            cleanup_calls.append(content_type)

        with patch.object(db, "conn", return_value=mock_conn), \
             patch.object(db, "cleanup_content_memory", side_effect=fake_cleanup), \
             patch("random.random", return_value=0.99):
            db.record_content_key(10, 20, "matchup_angle", "key-abc")

        self.assertEqual(len(cleanup_calls), 0)


if __name__ == "__main__":
    unittest.main()
