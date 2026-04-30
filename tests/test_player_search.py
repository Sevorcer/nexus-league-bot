"""Tests for Database.table_exists and the player_search fallback behaviour."""
import importlib.util
import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call


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


def _make_db() -> "Database":  # type: ignore[name-defined]
    """Return a Database instance that bypasses __init__ DB connection."""
    db = Database.__new__(Database)
    db.dsn = "postgres://dummy/dummy"
    return db


def _make_cursor_returning(row: dict) -> MagicMock:
    """Build a mock psycopg cursor that returns *row* from fetchone()."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = row
    return cur


def _make_conn(cur: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    return conn


class TestTableExists(unittest.TestCase):
    """Unit tests for Database.table_exists."""

    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Could not import Database: {_LOAD_ERROR}")

    def test_returns_true_when_table_present(self):
        db = _make_db()
        cur = _make_cursor_returning({"exists": True})
        conn = _make_conn(cur)
        with patch.object(db, "conn", return_value=conn):
            self.assertTrue(db.table_exists("player_receiving_stats"))

    def test_returns_false_when_table_absent(self):
        db = _make_db()
        cur = _make_cursor_returning({"exists": False})
        conn = _make_conn(cur)
        with patch.object(db, "conn", return_value=conn):
            self.assertFalse(db.table_exists("nonexistent_table"))

    def test_caches_result_on_second_call(self):
        db = _make_db()
        cur = _make_cursor_returning({"exists": True})
        conn = _make_conn(cur)
        with patch.object(db, "conn", return_value=conn):
            db.table_exists("player_receiving_stats")
            db.table_exists("player_receiving_stats")
        # DB should have been queried exactly once despite two calls.        self.assertEqual(cur.execute.call_count, 1)

    def test_different_tables_each_queried_once(self):
        db = _make_db()
        cur = _make_cursor_returning({"exists": True})
        conn = _make_conn(cur)
        with patch.object(db, "conn", return_value=conn):
            db.table_exists("table_a")
            db.table_exists("table_b")
            db.table_exists("table_a")  # cached - should NOT add another execute call
        self.assertEqual(cur.execute.call_count, 2)

    def test_returns_false_on_db_exception(self):
        db = _make_db()
        with patch.object(db, "conn", side_effect=Exception("connection refused")):
            self.assertFalse(db.table_exists("any_table"))

    def test_result_is_cached_after_exception(self):
        db = _make_db()
        # First call raises, result (False) should be cached.
        with patch.object(db, "conn", side_effect=Exception("refused")):
            db.table_exists("bad_table")
        # Second call: conn should NOT be invoked again (cached).
        mock_conn = MagicMock()
        with patch.object(db, "conn", return_value=mock_conn):
            result = db.table_exists("bad_table")
        self.assertFalse(result)
        mock_conn.assert_not_called()


class TestPlayerSearchSQLBuilding(unittest.TestCase):
    """Tests that player_search builds SQL using fallback tables when needed."""

    def setUp(self):
        if _LOAD_ERROR is not None:
            self.skipTest(f"Could not import Database: {_LOAD_ERROR}")

    def _run_player_search(self, table_flags: dict[str, bool]) -> str:
        """Call player_search with mocked table_exists flags and return the SQL executed."""
        db = _make_db()

        def _fake_table_exists(name: str) -> bool:
            return table_flags.get(name, False)

        db.table_exists = _fake_table_exists  # type: ignore[method-assign]

        executed_sql = []
        cur = MagicMock()
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchall.return_value = []
        cur.execute.side_effect = lambda sql, params: executed_sql.append(sql)
        conn = _make_conn(cur)

        with patch.object(db, "conn", return_value=conn):
            db.player_search(1, "Smith")

        self.assertEqual(len(executed_sql), 1)
        return executed_sql[0]

    def test_uses_specialized_table_when_present(self):
        sql = self._run_player_search(
            {
                "player_passing_stats": True,
                "player_rushing_stats": True,
                "player_receiving_stats": True,
                "player_defense_stats": True,
            }
        )
        self.assertIn("player_passing_stats", sql)
        self.assertIn("player_rushing_stats", sql)
        self.assertIn("player_receiving_stats", sql)
        self.assertIn("player_defense_stats", sql)
        self.assertNotIn("playerstats", sql)

    def test_falls_back_when_receiving_missing(self):
        sql = self._run_player_search(
            {
                "player_passing_stats": True,
                "player_rushing_stats": True,
                "player_receiving_stats": False,
                "player_defense_stats": True,
            }
        )
        self.assertNotIn("player_receiving_stats", sql)
        self.assertIn("playerstats", sql)
        self.assertIn("rec_yards", sql)

    def test_falls_back_all_when_none_present(self):
        sql = self._run_player_search(
            {
                "player_passing_stats": False,
                "player_rushing_stats": False,
                "player_receiving_stats": False,
                "player_defense_stats": False,
            }
        )
        self.assertNotIn("player_passing_stats", sql)
        self.assertNotIn("player_rushing_stats", sql)
        self.assertNotIn("player_receiving_stats", sql)
        self.assertNotIn("player_defense_stats", sql)
        self.assertIn("playerstats", sql)

    def test_fallback_params_include_league_id_for_each_missing_table(self):
        """When tables are missing, league_id is added to params for each fallback."""
        db = _make_db()
        db.table_exists = lambda name: False  # type: ignore[method-assign]

        captured_params: list = []
        cur = MagicMock()
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchall.return_value = []
        cur.execute.side_effect = lambda sql, params: captured_params.extend([params])
        conn = _make_conn(cur)

        with patch.object(db, "conn", return_value=conn):
            db.player_search(42, "Jones")

        self.assertEqual(len(captured_params), 1)
        params = captured_params[0]
        # 4 fallback subqueries + 1 outer league_id + 1 name ILIKE = 6 params
        self.assertEqual(len(params), 6)
        # All leading params should be league_id=42
        for p in params[:-1]:  # exclude the final ILIKE pattern
            self.assertEqual(p, 42)
        # Last param is the ILIKE pattern
        self.assertIn("Jones", params[-1])


if __name__ == "__main__":
    unittest.main()
