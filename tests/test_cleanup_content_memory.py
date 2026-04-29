"""Tests for Database.cleanup_content_memory and opportunistic cleanup in record_content_key."""
import importlib.util
import os
import unittest
from unittest.mock import MagicMock, patch


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


def _make_db():
    db = Database.__new__(Database)
    db.dsn = "postgresql://fake"
    return db


def _make_mock_conn():
    mock_cur = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_cur.__enter__ = MagicMock(return_value=mock_cur)
    mock_cur.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cur
    return mock_conn, mock_cur


class TestCleanupContentMemory(unittest.TestCase):
    """Tests for Database.cleanup_content_memory."""

    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Failed to load nexus_league_bot: {_LOAD_ERROR}")

    def test_cleanup_executes_two_deletes(self):
        """cleanup_content_memory runs exactly two DELETE statements."""
        db = _make_db()
        mock_conn, mock_cur = _make_mock_conn()
        with patch.object(db, "conn", return_value=mock_conn):
            db.cleanup_content_memory(1, 2)
        self.assertEqual(mock_cur.execute.call_count, 2)

    def test_cleanup_first_delete_uses_age_param(self):
        """First DELETE must use the max_age_days value."""
        db = _make_db()
        mock_conn, mock_cur = _make_mock_conn()
        with patch.object(db, "conn", return_value=mock_conn):
            db.cleanup_content_memory(1, 2, max_age_days=60)
        first_params = mock_cur.execute.call_args_list[0][0][1]
        self.assertIn(60, first_params)

    def test_cleanup_second_delete_uses_cap_param(self):
        """Second DELETE must use the per_type_cap value."""
        db = _make_db()
        mock_conn, mock_cur = _make_mock_conn()
        with patch.object(db, "conn", return_value=mock_conn):
            db.cleanup_content_memory(1, 2, per_type_cap=999)
        second_params = mock_cur.execute.call_args_list[1][0][1]
        self.assertIn(999, second_params)

    def test_cleanup_commits(self):
        """cleanup_content_memory must commit the transaction."""
        db = _make_db()
        mock_conn, mock_cur = _make_mock_conn()
        with patch.object(db, "conn", return_value=mock_conn):
            db.cleanup_content_memory(1, 2)
        mock_conn.commit.assert_called_once()

    def test_cleanup_passes_guild_and_league_to_both_deletes(self):
        """Both DELETE statements must scope to guild_id and league_id."""
        db = _make_db()
        mock_conn, mock_cur = _make_mock_conn()
        with patch.object(db, "conn", return_value=mock_conn):
            db.cleanup_content_memory(42, 99)
        for call_args in mock_cur.execute.call_args_list:
            params = call_args[0][1]
            self.assertIn(42, params)
            self.assertIn(99, params)


class TestRecordContentKeyCleanupTrigger(unittest.TestCase):
    """Verify that record_content_key triggers cleanup opportunistically at ~1%."""

    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Failed to load nexus_league_bot: {_LOAD_ERROR}")

    def test_cleanup_called_when_random_below_threshold(self):
        """When random() < 0.01, cleanup_content_memory is invoked."""
        db = _make_db()
        mock_conn, _ = _make_mock_conn()
        with patch.object(db, "conn", return_value=mock_conn), \
             patch.object(db, "cleanup_content_memory") as mock_cleanup, \
             patch("nexus_league_bot.random.random", return_value=0.005):
            db.record_content_key(10, 20, "matchup_angle", "some-key")
        mock_cleanup.assert_called_once_with(10, 20)

    def test_cleanup_not_called_when_random_at_or_above_threshold(self):
        """When random() >= 0.01, cleanup_content_memory is NOT invoked."""
        db = _make_db()
        mock_conn, _ = _make_mock_conn()
        with patch.object(db, "conn", return_value=mock_conn), \
             patch.object(db, "cleanup_content_memory") as mock_cleanup, \
             patch("nexus_league_bot.random.random", return_value=0.5):
            db.record_content_key(10, 20, "matchup_angle", "some-key")
        mock_cleanup.assert_not_called()

    def test_cleanup_error_does_not_propagate(self):
        """A cleanup failure must not crash record_content_key."""
        db = _make_db()
        mock_conn, _ = _make_mock_conn()
        with patch.object(db, "conn", return_value=mock_conn), \
             patch.object(db, "cleanup_content_memory", side_effect=RuntimeError("db down")), \
             patch("nexus_league_bot.random.random", return_value=0.005):
            db.record_content_key(10, 20, "test_type", "key")  # must not raise

    def test_insert_params_are_correct(self):
        """record_content_key inserts the four expected values."""
        db = _make_db()
        mock_conn, mock_cur = _make_mock_conn()
        with patch.object(db, "conn", return_value=mock_conn), \
             patch("nexus_league_bot.random.random", return_value=0.99):
            db.record_content_key(7, 8, "weekly_news_opener", "line-abc")
        insert_params = mock_cur.execute.call_args[0][1]
        self.assertEqual(insert_params, (7, 8, "weekly_news_opener", "line-abc"))


if __name__ == "__main__":
    unittest.main()
