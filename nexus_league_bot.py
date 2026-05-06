import asyncio
import datetime
import hashlib
import json
import logging
import os
import random
import re
import time
from collections import defaultdict
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

import discord
from discord import app_commands
from discord.ext import tasks
import psycopg
from psycopg.rows import dict_row

from storyline_templates import (
    MATCHUP_ANGLES,
    MATCHUP_OPENERS,
    MATCHUP_BODIES,
    MATCHUP_STAKES,
    PLAYER_WHY_PASSING,
    PLAYER_WHY_RUSHING,
    PLAYER_WHY_DEFENSE,
    PROFILE_CONTENDER,
    PROFILE_STRUGGLING,
    PROFILE_TURNOVER,
    PROFILE_OFFENSE,
    PROFILE_DEFENSE,
    PROFILE_NEUTRAL,
    WEEKLY_NEWS_OPENERS,
    WEEKLY_NEWS_BODY_LINES,
    HEADLINE_LINE1,
    HEADLINE_LINE3,
    HEADLINE_LINE4,
    HEADLINE_LINE5,
)


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("nexus-league-bot")

NO_SETUP_MESSAGE = "Please run `/setup` first to configure your league."
DEFAULT_ADMIN_ROLES = "Commissioner,Admin,COMMISH"
ROSTER_PAGE_SIZE = 18
DEV_TRAIT_LABELS = {
    0: "Normal",
    1: "Star ⭐",
    2: "Superstar 🌟",
    3: "X-Factor 💎",
}

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
DEFAULT_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_API_KEY_TEMPLATE = os.getenv("OPENAI_API_KEY_TEMPLATE", "").strip()
AUTO_POST_MATCHUP_PREVIEWS = os.getenv("AUTO_POST_MATCHUP_PREVIEWS", "true").lower() in {"1", "true", "yes", "on"}
HEADLINE_MAX_OUTPUT_TOKENS = 260
HEADLINE_PROMPT_INSTRUCTIONS = (
    "You are writing six short football headline sentences for a Madden franchise Discord league.\n"
    "Rules:\n"
    "- Write exactly 6 numbered sentences (1. through 6.).\n"
    "- Use only provided facts.\n"
    "- Keep each sentence concise and punchy.\n"
)

_WEEKS_MEMORY: dict[str, dict[int, list[str]]] = {
    "angles": defaultdict(list),
}


def parse_guild_ids() -> list[int]:
    raw = os.getenv("GUILD_IDS", "").strip()
    if not raw:
        return []
    ids: list[int] = []
    for value in raw.split(","):
        value = value.strip()
        if not value:
            continue
        try:
            ids.append(int(value))
        except ValueError:
            LOGGER.warning("Ignoring invalid guild id in GUILD_IDS: %s", value)
    return ids


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return default


def safe_text(value: Any, default: str = "") -> str:
    text = str(value).strip() if value is not None else ""
    return text or default


def deterministic_choice(options: list[str], seed: str) -> str:
    if not options:
        return ""
    rng = random.Random(seed)
    return options[rng.randrange(len(options))]


def choose_nonrepeating(
    options: list[str],
    seed: str,
    content_type: str,
    db: "Database",
    guild_id: int,
    league_id: int,
    cooldown_limit: int = 500,
) -> str:
    """Pick from *options* avoiding recently used entries, then record the choice.

    Queries the last *cooldown_limit* used content_keys for this
    (guild_id, league_id, content_type) and tries to choose an option that
    is not in that recent set.  Falls back to deterministic_choice when the
    entire pool has been recently used.  Always records the chosen value.

    If *db* is None or any DB operation fails, falls back silently to
    deterministic_choice without recording.
    """
    if not options:
        return ""
    try:
        recent = set(db.fetch_recent_content_keys(guild_id, league_id, content_type, cooldown_limit))
        available = [o for o in options if o not in recent]
        chosen = deterministic_choice(available if available else options, seed)
        db.record_content_key(guild_id, league_id, content_type, chosen)
        return chosen
    except Exception as exc:
        LOGGER.debug("choose_nonrepeating DB error (%s): %s – falling back", content_type, exc)
        return deterministic_choice(options, seed)


def slugify_channel_name(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9\s-]", "", text).strip().lower()
    slug = re.sub(r"\s+", "-", slug)
    return slug[:90] or "team"


def format_phase_labels(phase: str | None, week: int) -> dict[str, str]:
    """Return category name, channel prefix, and display label for a given phase and week.

    Args:
        phase: One of ``"preseason"``, ``"regular"``, or ``"postseason"``.
               ``None`` or any unrecognised value is treated as ``"regular"``.
        week: The week number (1-based).

    Returns:
        A dict with keys ``"category"``, ``"prefix"``, and ``"display"``.
    """
    p = (phase or "regular").lower()
    if p == "preseason":
        return {
            "category": f"Preseason Week {week} Games",
            "prefix": f"pre-wk{week}",
            "display": f"Preseason Week {week}",
        }
    if p == "postseason":
        return {
            "category": f"Postseason Week {week} Games",
            "prefix": f"post-wk{week}",
            "display": f"Postseason Week {week}",
        }
    # default: regular season
    return {
        "category": f"Week {week} Games",
        "prefix": f"wk{week}",
        "display": f"Week {week}",
    }


def match_weekly_channel_names(
    channels: list[discord.TextChannel | discord.VoiceChannel],
    week: int,
    phase: str | None = None,
) -> list[discord.TextChannel | discord.VoiceChannel]:
    """Return channels whose names start with the week/phase prefix.

    Channel names are expected to follow the pattern ``{prefix}-{slug}`` or
    ``gotw-{prefix}-{slug}``.  The prefix is derived from *phase* and *week*
    via :func:`format_phase_labels`:

    * ``"regular"`` (or ``None``) → ``wk{week}``    (e.g. ``wk6``)
    * ``"preseason"``             → ``pre-wk{week}``
    * ``"postseason"``            → ``post-wk{week}``

    The trailing ``-`` is appended before matching so that prefix ``wk6`` does
    **not** accidentally match a channel named ``wk60-...``.  The ``gotw-``
    variant is also matched (e.g. ``gotw-wk6-...``).
    """
    info = format_phase_labels(phase, week)
    prefix = info["prefix"]
    plain = f"{prefix}-"
    gotw = f"gotw-{prefix}-"
    return [ch for ch in channels if ch.name.startswith(plain) or ch.name.startswith(gotw)]


def _parse_channel_ids(raw: str | None) -> set[int]:
    values: set[int] = set()
    for part in (raw or "").split(","):
        item = part.strip()
        if not item:
            continue
        try:
            values.add(int(item))
        except Exception:
            continue
    return values


def wins_losses_ties_text(row: dict[str, Any]) -> str:
    wins = safe_int(row.get("wins"))
    losses = safe_int(row.get("losses"))
    ties = safe_int(row.get("ties"))
    return f"{wins}-{losses}-{ties}"


def team_color_from_name(team_name: str | None) -> discord.Color:
    seed = team_name or "nexus"
    color_hex = hashlib.md5(seed.encode("utf-8")).hexdigest()[:6]
    return discord.Color(int(color_hex, 16))


def player_display_name(row: dict[str, Any]) -> str:
    full_name = safe_text(row.get("player_name") or row.get("full_name"))
    if full_name:
        return full_name
    return f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()


def leader_rank_text(rank: int) -> str:
    if rank == 1:
        return "🥇"
    if rank == 2:
        return "🥈"
    if rank == 3:
        return "🥉"
    return f"{rank}."


def season_leader_stat_text(row: dict[str, Any], category: str) -> str:
    if category == "passing":
        return (
            f"YDS `{safe_int(row.get('pass_yards')):>5}` | "
            f"TD `{safe_int(row.get('pass_tds')):>2}` | "
            f"INT `{safe_int(row.get('interceptions')):>2}`"
        )
    if category == "rushing":
        return (
            f"YDS `{safe_int(row.get('rush_yards')):>5}` | "
            f"TD `{safe_int(row.get('rush_tds')):>2}`"
        )
    if category == "receiving":
        return (
            f"YDS `{safe_int(row.get('rec_yards')):>5}` | "
            f"TD `{safe_int(row.get('rec_tds')):>2}` | "
            f"REC `{safe_int(row.get('receptions')):>3}`"
        )
    if category == "defense":
        return (
            f"TKL `{safe_int(row.get('tackles')):>3}` | "
            f"SCK `{safe_float(row.get('sacks')):>4.1f}` | "
            f"INT `{safe_int(row.get('defensive_ints')):>2}` | "
            f"FF `{safe_int(row.get('fumbles_forced')):>2}`"
        )
    return f"TOTAL TD `{safe_int(row.get('total_tds')):>2}`"


def dev_trait_label(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return "-"
        try:
            return DEV_TRAIT_LABELS.get(int(stripped), stripped)
        except ValueError:
            return stripped
    try:
        return DEV_TRAIT_LABELS.get(int(value), str(value))
    except Exception:
        return str(value)


def detect_profile_storyline(
    team_row: dict[str, Any],
    db: "Database | None" = None,
    guild_id: int = 0,
    league_id: int = 0,
) -> str:
    wins = safe_int(team_row.get("wins"))
    losses = safe_int(team_row.get("losses"))
    points_for = safe_int(team_row.get("pts_for"))
    points_against = safe_int(team_row.get("pts_against"))
    turnover_diff = safe_int(team_row.get("turnover_diff"))

    seed = f"profile-{wins}-{losses}-{points_for}-{points_against}-{turnover_diff}"
    if wins >= losses + 4:
        pool, ctype = PROFILE_CONTENDER, "profile_contender"
    elif losses >= wins + 3:
        pool, ctype = PROFILE_STRUGGLING, "profile_struggling"
    elif turnover_diff >= 5:
        pool, ctype = PROFILE_TURNOVER, "profile_turnover"
    elif points_for > points_against + 35:
        pool, ctype = PROFILE_OFFENSE, "profile_offense"
    elif points_against < points_for - 20:
        pool, ctype = PROFILE_DEFENSE, "profile_defense"
    else:
        pool, ctype = PROFILE_NEUTRAL, "profile_neutral"

    if db and guild_id and league_id:
        return choose_nonrepeating(pool, seed, ctype, db, guild_id, league_id)
    return deterministic_choice(pool, seed)


def build_team_storyline(
    team_row: dict[str, Any],
    leaders: dict[str, Any],
    db: "Database | None" = None,
    guild_id: int = 0,
    league_id: int = 0,
) -> str:
    team_name = safe_text(team_row.get("team_name"), "Unknown Team")
    record = wins_losses_ties_text(team_row)
    pf = safe_int(team_row.get("pts_for"))
    pa = safe_int(team_row.get("pts_against"))
    turnover_diff = safe_int(team_row.get("turnover_diff"))
    seed = safe_int(team_row.get("seed"))

    lines = [
        f"{team_name} is {record}, with {pf} points scored, {pa} allowed, and a {turnover_diff:+d} turnover margin.",
        detect_profile_storyline(team_row, db=db, guild_id=guild_id, league_id=league_id),
    ]
    if seed:
        lines.append(f"They currently sit on the {seed} seed line.")

    passer = leaders.get("passing") or {}
    rusher = leaders.get("rushing") or {}
    defender = leaders.get("defense") or {}

    if safe_text(passer.get("player_name")):
        lines.append(
            f"Top passer: {passer['player_name']} ({safe_int(passer.get('pass_yards'))} yards, {safe_int(passer.get('pass_tds'))} TD)."
        )
    if safe_text(rusher.get("player_name")):
        lines.append(
            f"Top rusher: {rusher['player_name']} ({safe_int(rusher.get('rush_yards'))} yards, {safe_int(rusher.get('rush_tds'))} TD)."
        )
    if safe_text(defender.get("player_name")):
        lines.append(
            f"Defensive tone-setter: {defender['player_name']} ({safe_int(defender.get('sacks'))} sacks, {safe_int(defender.get('defensive_ints'))} INT)."
        )
    return " ".join(line.strip() for line in lines if line.strip())


def build_gamerecap_prompt(facts: dict[str, Any], plan: dict[str, Any] | None = None) -> str:
    payload = dict(facts)
    if plan:
        payload["selected_plan"] = plan
    return (
        "You are writing an original football recap for a Madden franchise Discord league.\n"
        "Write one strong original headline on the first line, then one recap paragraph of 150 to 240 words.\n"
        "Use only provided facts, avoid invented details, and emphasize what shifted the game.\n"
        "Facts JSON:\n"
        f"{json.dumps(payload, indent=2, default=str)}"
    )


class Database:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def conn(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def init(self) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS guild_config(
                    guild_id BIGINT PRIMARY KEY,
                    league_id INTEGER NOT NULL,
                    log_channel_id BIGINT DEFAULT 0,
                    leaders_channel_id BIGINT DEFAULT 0,
                    news_channel_id BIGINT DEFAULT 0,
                    trade_committee_role_id BIGINT DEFAULT 0,
                    trade_review_channel_id BIGINT DEFAULT 0,
                    trade_announcements_channel_id BIGINT DEFAULT 0,
                    trade_required_approvals INTEGER DEFAULT 2,
                    trade_required_denials INTEGER DEFAULT 2,
                    level_up_channel_id BIGINT DEFAULT 0,
                    xp_cooldown_seconds INTEGER DEFAULT 45,
                    xp_min_message_len INTEGER DEFAULT 8,
                    xp_blacklist_channel_ids TEXT DEFAULT '',
                    admin_role_names TEXT DEFAULT 'Commissioner,Admin,COMMISH',
                    openai_api_key TEXT DEFAULT '',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            for statement in [
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS news_channel_id BIGINT DEFAULT 0",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS trade_committee_role_id BIGINT DEFAULT 0",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS trade_review_channel_id BIGINT DEFAULT 0",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS trade_announcements_channel_id BIGINT DEFAULT 0",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS trade_required_approvals INTEGER DEFAULT 2",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS trade_required_denials INTEGER DEFAULT 2",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS level_up_channel_id BIGINT DEFAULT 0",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS xp_cooldown_seconds INTEGER DEFAULT 45",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS xp_min_message_len INTEGER DEFAULT 8",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS xp_blacklist_channel_ids TEXT DEFAULT ''",
                "ALTER TABLE guild_config ADD COLUMN IF NOT EXISTS openai_api_key TEXT DEFAULT ''",
            ]:
                cur.execute(statement)

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_xp_users(
                    guild_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    username TEXT NOT NULL,
                    xp INTEGER NOT NULL DEFAULT 0,
                    level INTEGER NOT NULL DEFAULT 1,
                    messages_counted INTEGER NOT NULL DEFAULT 0,
                    last_xp_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (guild_id, user_id)
                )
                """
            )
            cur.execute("ALTER TABLE bot_xp_users ADD COLUMN IF NOT EXISTS guild_id BIGINT NOT NULL DEFAULT 0")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS bot_xp_users_guild_user_idx ON bot_xp_users (guild_id, user_id)")
            cur.execute("ALTER TABLE bot_xp_users DROP CONSTRAINT IF EXISTS bot_xp_users_pkey")
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = 'bot_xp_users'
                          AND table_schema = 'public'
                          AND constraint_type = 'PRIMARY KEY'
                    ) THEN
                        ALTER TABLE bot_xp_users ADD PRIMARY KEY (guild_id, user_id);
                    END IF;
                END$$
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_bounties(
                    id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    reward DOUBLE PRECISION NOT NULL,
                    created_by BIGINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    claimed_by BIGINT,
                    claimed_at TIMESTAMPTZ
                )
                """
            )
            cur.execute("ALTER TABLE bot_bounties ADD COLUMN IF NOT EXISTS guild_id BIGINT DEFAULT 0")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_trades(
                    id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    submitted_by BIGINT NOT NULL,
                    submitted_username TEXT NOT NULL,
                    coach_one_user_id BIGINT,
                    coach_two_user_id BIGINT,
                    team_one_name TEXT NOT NULL,
                    team_two_name TEXT NOT NULL,
                    team_one_gets TEXT NOT NULL,
                    team_two_gets TEXT NOT NULL,
                    notes TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    approve_count INTEGER NOT NULL DEFAULT 0,
                    deny_count INTEGER NOT NULL DEFAULT 0,
                    review_channel_id BIGINT,
                    review_message_id BIGINT,
                    announcement_channel_id BIGINT,
                    announcement_message_id BIGINT,
                    finalized_by BIGINT,
                    finalized_reason TEXT,
                    finalized_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute("ALTER TABLE bot_trades ADD COLUMN IF NOT EXISTS guild_id BIGINT DEFAULT 0")
            cur.execute("ALTER TABLE bot_trades ADD COLUMN IF NOT EXISTS coach_one_user_id BIGINT")
            cur.execute("ALTER TABLE bot_trades ADD COLUMN IF NOT EXISTS coach_two_user_id BIGINT")
            cur.execute("ALTER TABLE bot_trades ADD COLUMN IF NOT EXISTS announcement_channel_id BIGINT")
            cur.execute("ALTER TABLE bot_trades ADD COLUMN IF NOT EXISTS announcement_message_id BIGINT")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_trade_votes(
                    id BIGSERIAL PRIMARY KEY,
                    trade_id BIGINT NOT NULL REFERENCES bot_trades(id) ON DELETE CASCADE,
                    voter_user_id BIGINT NOT NULL,
                    voter_username TEXT NOT NULL,
                    vote TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (trade_id, voter_user_id)
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_content_memory(
                    id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    league_id INTEGER NOT NULL,
                    content_type TEXT NOT NULL,
                    content_key TEXT NOT NULL,
                    used_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS bot_content_memory_lookup_idx "
                "ON bot_content_memory (guild_id, league_id, content_type, used_at DESC)"
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS league_settings(
                    league_id INTEGER PRIMARY KEY,
                    reminder_interval_hours INTEGER NOT NULL DEFAULT 8
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS game_channel_state(
                    channel_id BIGINT PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    league_id INTEGER NOT NULL,
                    home_team_name TEXT,
                    away_team_name TEXT,
                    home_user_id BIGINT,
                    away_user_id BIGINT,
                    scheduled BOOLEAN NOT NULL DEFAULT FALSE,
                    completed BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_reminder_at TIMESTAMPTZ
                )
                """
            )
            cur.execute(
                "ALTER TABLE game_channel_state ADD COLUMN IF NOT EXISTS delete_at TIMESTAMPTZ"
            )

            conn.commit()

    def fetch_recent_content_keys(
        self,
        guild_id: int,
        league_id: int,
        content_type: str,
        limit: int,
    ) -> list[str]:
        """Return the most recently used content_key values for this scope."""
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT content_key
                FROM bot_content_memory
                WHERE guild_id = %s AND league_id = %s AND content_type = %s
                ORDER BY used_at DESC
                LIMIT %s
                """,
                (guild_id, league_id, content_type, limit),
            )
            return [row["content_key"] for row in cur.fetchall()]

    def cleanup_content_memory(
        self,
        guild_id: int,
        league_id: int,
        max_age_days: int = 120,
        per_type_cap: int = 2000,
    ) -> None:
        """Delete stale bot_content_memory rows for this guild+league.

        Two passes:
        1. Delete rows older than *max_age_days*.
        2. For each content_type, keep only the most recent *per_type_cap* rows.
        """
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM bot_content_memory
                WHERE guild_id = %s
                  AND league_id = %s
                  AND used_at < NOW() - (%s * INTERVAL '1 day')
                """,
                (guild_id, league_id, max_age_days),
            )
            cur.execute(
                """
                DELETE FROM bot_content_memory
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY guild_id, league_id, content_type
                                   ORDER BY used_at DESC
                               ) AS rn
                        FROM bot_content_memory
                        WHERE guild_id = %s AND league_id = %s
                    ) ranked
                    WHERE rn > %s
                )
                """,
                (guild_id, league_id, per_type_cap),
            )
            conn.commit()

    def record_content_key(
        self,
        guild_id: int,
        league_id: int,
        content_type: str,
        content_key: str,
    ) -> None:
        """Insert a new usage record for the given content_key.

        Runs cleanup_content_memory with ~1% probability to keep the table
        bounded without incurring overhead on every call.
        """
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_content_memory (guild_id, league_id, content_type, content_key)
                VALUES (%s, %s, %s, %s)
                """,
                (guild_id, league_id, content_type, content_key),
            )
            conn.commit()
        if random.random() < 0.01:
            try:
                self.cleanup_content_memory(guild_id, league_id)
            except Exception as exc:
                LOGGER.debug("content_memory cleanup error (non-fatal): %s", exc)

    # ------------------------------------------------------------------ #
    # Reminder / game-channel state methods                               #
    # ------------------------------------------------------------------ #

    def get_reminder_interval(self, league_id: int) -> int:
        """Return the configured reminder interval in hours for the league (default 8)."""
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT reminder_interval_hours FROM league_settings WHERE league_id = %s",
                (league_id,),
            )
            row = cur.fetchone()
            return int(row["reminder_interval_hours"]) if row else 8

    def set_reminder_interval(self, league_id: int, hours: int) -> None:
        """Upsert the reminder interval for a league."""
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO league_settings (league_id, reminder_interval_hours)
                VALUES (%s, %s)
                ON CONFLICT (league_id) DO UPDATE
                SET reminder_interval_hours = EXCLUDED.reminder_interval_hours
                """,
                (league_id, hours),
            )
            conn.commit()

    def upsert_game_channel_state(
        self,
        channel_id: int,
        guild_id: int,
        league_id: int,
        home_team_name: str,
        away_team_name: str,
        home_user_id: int | None,
        away_user_id: int | None,
    ) -> None:
        """Insert or update the game channel state row."""
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO game_channel_state
                    (channel_id, guild_id, league_id, home_team_name, away_team_name,
                     home_user_id, away_user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (channel_id) DO UPDATE SET
                    guild_id = EXCLUDED.guild_id,
                    league_id = EXCLUDED.league_id,
                    home_team_name = EXCLUDED.home_team_name,
                    away_team_name = EXCLUDED.away_team_name,
                    home_user_id = EXCLUDED.home_user_id,
                    away_user_id = EXCLUDED.away_user_id
                """,
                (
                    channel_id,
                    guild_id,
                    league_id,
                    home_team_name,
                    away_team_name,
                    home_user_id or None,
                    away_user_id or None,
                ),
            )
            conn.commit()

    def get_game_channel_state(self, channel_id: int) -> dict[str, Any] | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM game_channel_state WHERE channel_id = %s",
                (channel_id,),
            )
            return cur.fetchone()

    def get_pending_game_channels(self) -> list[dict[str, Any]]:
        """Return game channel rows that are not scheduled and not completed."""
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM game_channel_state
                WHERE scheduled = FALSE AND completed = FALSE
                """
            )
            return cur.fetchall()

    def mark_game_scheduled(self, channel_id: int) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE game_channel_state SET scheduled = TRUE WHERE channel_id = %s",
                (channel_id,),
            )
            conn.commit()

    def mark_game_completed(self, channel_id: int) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE game_channel_state SET completed = TRUE, delete_at = NOW() + INTERVAL '1 hour' WHERE channel_id = %s",
                (channel_id,),
            )
            conn.commit()

    def get_channels_due_for_deletion(self) -> list[dict[str, Any]]:
        """Return game channel rows that are completed and whose delete_at has passed."""
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM game_channel_state
                WHERE completed = TRUE AND delete_at IS NOT NULL AND delete_at <= NOW()
                """
            )
            return cur.fetchall()

    def update_last_reminder_at(self, channel_id: int) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE game_channel_state SET last_reminder_at = NOW() WHERE channel_id = %s",
                (channel_id,),
            )
            conn.commit()

    def delete_game_channel_state(self, channel_id: int) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM game_channel_state WHERE channel_id = %s",
                (channel_id,),
            )
            conn.commit()

    def get_guild_config(self, guild_id: int) -> dict[str, Any] | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM guild_config WHERE guild_id = %s", (guild_id,))
            return cur.fetchone()

    def upsert_guild_league(self, guild_id: int, league_id: int) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO guild_config (guild_id, league_id, admin_role_names)
                VALUES (%s, %s, %s)
                ON CONFLICT (guild_id)
                DO UPDATE SET
                    league_id = EXCLUDED.league_id,
                    updated_at = NOW()
                """,
                (guild_id, league_id, DEFAULT_ADMIN_ROLES),
            )
            conn.commit()

    def update_channels(self, guild_id: int, log_channel_id: int, leaders_channel_id: int) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE guild_config
                   SET log_channel_id = %s,
                       leaders_channel_id = %s,
                       updated_at = NOW()
                 WHERE guild_id = %s
                """,
                (log_channel_id, leaders_channel_id, guild_id),
            )
            conn.commit()

    def update_news_channel(self, guild_id: int, news_channel_id: int) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE guild_config SET news_channel_id=%s, updated_at=NOW() WHERE guild_id=%s",
                (news_channel_id, guild_id),
            )
            conn.commit()

    def update_openai_key(self, guild_id: int, key: str) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE guild_config SET openai_api_key=%s, updated_at=NOW() WHERE guild_id=%s",
                (key, guild_id),
            )
            conn.commit()

    def update_trade_channels(
        self,
        guild_id: int,
        committee_role_id: int,
        review_channel_id: int,
        announcements_channel_id: int,
        required_approvals: int,
        required_denials: int,
    ) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE guild_config
                SET trade_committee_role_id=%s,
                    trade_review_channel_id=%s,
                    trade_announcements_channel_id=%s,
                    trade_required_approvals=%s,
                    trade_required_denials=%s,
                    updated_at=NOW()
                WHERE guild_id=%s
                """,
                (
                    committee_role_id,
                    review_channel_id,
                    announcements_channel_id,
                    required_approvals,
                    required_denials,
                    guild_id,
                ),
            )
            conn.commit()

    def update_xp_settings(
        self,
        guild_id: int,
        level_up_channel_id: int,
        cooldown_seconds: int,
        min_message_len: int,
        blacklist_channels: str,
    ) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE guild_config
                SET level_up_channel_id=%s,
                    xp_cooldown_seconds=%s,
                    xp_min_message_len=%s,
                    xp_blacklist_channel_ids=%s,
                    updated_at=NOW()
                WHERE guild_id=%s
                """,
                (level_up_channel_id, cooldown_seconds, min_message_len, blacklist_channels, guild_id),
            )
            conn.commit()

    def team_autocomplete(self, league_id: int, query: str) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT team_name
                FROM team
                WHERE league_id = %s
                  AND team_name ILIKE %s
                ORDER BY team_name ASC
                LIMIT 25
                """,
                (league_id, f"%{query}%"),
            )
            return cur.fetchall()

    def get_league_name(self, league_id: int) -> str:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT name FROM league WHERE id = %s", (league_id,))
            row = cur.fetchone()
            return row["name"] if row and row.get("name") else f"League {league_id}"

    def fetch_team_info(self, league_id: int, team_name: str) -> dict[str, Any] | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, team_name, abbreviation, city_name, division,
                       overall_rating, wins, losses, ties
                FROM team
                WHERE league_id = %s
                  AND team_name ILIKE %s
                LIMIT 1
                """,
                (league_id, team_name),
            )
            return cur.fetchone()

    def fetch_team_info_by_id(self, league_id: int, team_id: int) -> dict[str, Any] | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.id,
                       t.team_name,
                       t.division,
                       t.wins,
                       t.losses,
                       t.ties,
                       t.overall_rating as team_ovr,
                       COALESCE(t.wins::float / NULLIF((t.wins + t.losses + t.ties), 0), 0) as win_pct,
                       0::int as pts_for,       -- not in standing table; kept for API compat
                       0::int as pts_against,   -- not in standing table; kept for API compat
                       0::int as turnover_diff,  -- not in standing table; kept for API compat
                       COALESCE(st.seed, 0)::int as seed
                FROM team t
                LEFT JOIN LATERAL (
                  SELECT seed
                  FROM standing s
                  WHERE s.team_id = t.id AND s.league_id = t.league_id
                  ORDER BY s.id DESC LIMIT 1
                ) st ON true
                WHERE t.league_id = %s
                  AND t.id = %s
                LIMIT 1
                """,
                (league_id, team_id),
            )
            return cur.fetchone()

    def fetch_all_teams(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, team_name, city_name, division, overall_rating,
                       wins, losses, ties, abbreviation
                FROM team
                WHERE league_id = %s
                ORDER BY team_name ASC
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_team_roster(self, league_id: int, team_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT first_name, last_name, position, overall_rating, age, dev_trait
                FROM player
                WHERE league_id = %s
                  AND team_id = %s
                ORDER BY overall_rating DESC, last_name ASC, first_name ASC
                """,
                (league_id, team_id),
            )
            return cur.fetchall()

    def fetch_passing_leaders(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ps.player_id AS roster_id,
                    COALESCE(MAX(p.first_name || ' ' || p.last_name), 'Unknown') AS player_name,
                    COALESCE(MAX(p.position), '-') AS position,
                    COALESCE(MAX(t.team_name), 'FA') AS team_name,
                    SUM(COALESCE(ps.pass_yards, 0)) AS pass_yards,
                    SUM(COALESCE(ps.pass_tds, 0)) AS pass_tds,
                    SUM(COALESCE(ps.interceptions, 0)) AS interceptions
                FROM playerstats ps
                LEFT JOIN player p ON p.id = ps.player_id
                LEFT JOIN team t ON t.id = p.team_id
                WHERE ps.league_id = %s
                  AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                GROUP BY ps.player_id
                ORDER BY pass_yards DESC, player_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_rushing_leaders(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ps.player_id AS roster_id,
                    COALESCE(MAX(p.first_name || ' ' || p.last_name), 'Unknown') AS player_name,
                    COALESCE(MAX(p.position), '-') AS position,
                    COALESCE(MAX(t.team_name), 'FA') AS team_name,
                    SUM(COALESCE(ps.rush_yards, 0)) AS rush_yards,
                    SUM(COALESCE(ps.rush_tds, 0)) AS rush_tds
                FROM playerstats ps
                LEFT JOIN player p ON p.id = ps.player_id
                LEFT JOIN team t ON t.id = p.team_id
                WHERE ps.league_id = %s
                  AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                GROUP BY ps.player_id
                ORDER BY rush_yards DESC, player_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_receiving_leaders(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ps.player_id AS roster_id,
                    COALESCE(MAX(p.first_name || ' ' || p.last_name), 'Unknown') AS player_name,
                    COALESCE(MAX(p.position), '-') AS position,
                    COALESCE(MAX(t.team_name), 'FA') AS team_name,
                    SUM(COALESCE(ps.rec_yards, 0)) AS rec_yards,
                    SUM(COALESCE(ps.rec_tds, 0)) AS rec_tds,
                    SUM(COALESCE(ps.receptions, 0)) AS receptions
                FROM playerstats ps
                LEFT JOIN player p ON p.id = ps.player_id
                LEFT JOIN team t ON t.id = p.team_id
                WHERE ps.league_id = %s
                  AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                GROUP BY ps.player_id
                ORDER BY rec_yards DESC, player_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_defense_leaders(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ps.player_id AS roster_id,
                    COALESCE(MAX(p.first_name || ' ' || p.last_name), 'Unknown') AS player_name,
                    COALESCE(MAX(p.position), '-') AS position,
                    COALESCE(MAX(t.team_name), 'FA') AS team_name,
                    SUM(COALESCE(ps.tackles, 0)) AS tackles,
                    SUM(COALESCE(ps.sacks, 0)) AS sacks,
                    SUM(COALESCE(ps.defensive_ints, 0)) AS defensive_ints,
                    SUM(COALESCE(ps.fumbles_forced, 0)) AS fumbles_forced
                FROM playerstats ps
                LEFT JOIN player p ON p.id = ps.player_id
                LEFT JOIN team t ON t.id = p.team_id
                WHERE ps.league_id = %s
                  AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                GROUP BY ps.player_id
                ORDER BY tackles DESC, player_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_touchdown_leaders(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    roster_id,
                    COALESCE(MAX(player_name), 'Unknown') AS player_name,
                    COALESCE(MAX(position), '-') AS position,
                    COALESCE(MAX(team_name), 'FA') AS team_name,
                    SUM(pass_tds) AS pass_tds,
                    SUM(rush_tds) AS rush_tds,
                    SUM(rec_tds) AS rec_tds,
                    SUM(pass_tds + rush_tds + rec_tds) AS total_tds
                FROM (
                    SELECT ps.player_id AS roster_id,
                           COALESCE(MAX(p.first_name || ' ' || p.last_name), 'Unknown') AS player_name,
                           COALESCE(MAX(p.position), '-') AS position,
                           COALESCE(MAX(t.team_name), 'FA') AS team_name,
                           SUM(COALESCE(ps.pass_tds, 0)) AS pass_tds,
                           SUM(COALESCE(ps.rush_tds, 0)) AS rush_tds,
                           SUM(COALESCE(ps.rec_tds, 0)) AS rec_tds
                    FROM playerstats ps
                    LEFT JOIN player p ON p.id = ps.player_id
                    LEFT JOIN team t ON t.id = p.team_id
                    WHERE ps.league_id = %s
                      AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                    GROUP BY ps.player_id
                ) combined
                GROUP BY roster_id
                ORDER BY total_tds DESC, player_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_team_top_leaders(self, league_id: int, team_id: int) -> dict[str, Any]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT ps.player_id,
                       COALESCE(MAX(p.first_name || ' ' || p.last_name), 'Unknown') AS player_name,
                       SUM(COALESCE(ps.pass_yards, 0)) AS pass_yards,
                       SUM(COALESCE(ps.pass_tds, 0)) AS pass_tds,
                       SUM(COALESCE(ps.interceptions, 0)) AS interceptions
                FROM playerstats ps
                JOIN player p ON p.id = ps.player_id
                WHERE p.team_id = %s AND ps.league_id = %s
                  AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                GROUP BY ps.player_id
                ORDER BY pass_yards DESC
                LIMIT 1
                """,
                (team_id, league_id),
            )
            passing = cur.fetchone() or {}
            cur.execute(
                """
                SELECT ps.player_id,
                       COALESCE(MAX(p.first_name || ' ' || p.last_name), 'Unknown') AS player_name,
                       SUM(COALESCE(ps.rush_yards, 0)) AS rush_yards,
                       SUM(COALESCE(ps.rush_tds, 0)) AS rush_tds
                FROM playerstats ps
                JOIN player p ON p.id = ps.player_id
                WHERE p.team_id = %s AND ps.league_id = %s
                  AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                GROUP BY ps.player_id
                ORDER BY rush_yards DESC
                LIMIT 1
                """,
                (team_id, league_id),
            )
            rushing = cur.fetchone() or {}
            cur.execute(
                """
                SELECT ps.player_id,
                       COALESCE(MAX(p.first_name || ' ' || p.last_name), 'Unknown') AS player_name,
                       SUM(COALESCE(ps.sacks, 0)) AS sacks,
                       SUM(COALESCE(ps.defensive_ints, 0)) AS defensive_ints,
                       SUM(COALESCE(ps.tackles, 0)) AS tackles,
                       SUM(COALESCE(ps.fumbles_forced, 0)) AS fumbles_forced,
                       SUM(COALESCE(ps.sacks, 0) + COALESCE(ps.defensive_ints, 0)) AS defensive_score
                FROM playerstats ps
                JOIN player p ON p.id = ps.player_id
                WHERE p.team_id = %s AND ps.league_id = %s
                  AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                GROUP BY ps.player_id
                ORDER BY defensive_score DESC
                LIMIT 1
                """,
                (team_id, league_id),
            )
            defense = cur.fetchone() or {}
            return {
                "passing": dict(passing),
                "rushing": dict(rushing),
                "defense": dict(defense),
            }

    def fetch_standings(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM (
                    SELECT DISTINCT ON (s.team_id)
                           t.team_name,
                           COALESCE(s.division_name, t.division, 'Unknown') AS division_name,
                           s.wins,
                           s.losses,
                           s.ties,
                           s.seed,
                           COALESCE(s.wins::float / NULLIF((s.wins + s.losses + s.ties), 0), 0) AS win_pct
                    FROM standing s
                    JOIN team t ON t.id = s.team_id AND t.league_id = s.league_id
                    WHERE s.league_id = %s
                      AND (s.season_type = 'reg' OR s.season_type IS NULL)
                    ORDER BY s.team_id, s.id DESC
                ) latest
                ORDER BY latest.wins DESC, latest.losses ASC
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_schedule_for_week(self, league_id: int, week_number: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.id as game_id,
                       s.week_number,
                       s.season_number,
                       s.is_complete,
                       s.home_team_id,
                       s.away_team_id,
                       home.team_name AS home_team,
                       away.team_name AS away_team,
                       home.division AS home_division,
                       away.division AS away_division,
                       home.wins AS home_wins,
                       home.losses AS home_losses,
                       home.ties AS home_ties,
                       away.wins AS away_wins,
                       away.losses AS away_losses,
                       away.ties AS away_ties,
                       s.home_score,
                       s.away_score
                FROM schedule s
                JOIN team home
                  ON home.id = s.home_team_id
                 AND home.league_id = s.league_id
                JOIN team away
                  ON away.id = s.away_team_id
                 AND away.league_id = s.league_id
                WHERE s.league_id = %s
                  AND s.week_number = %s
                ORDER BY s.id ASC
                """,
                (league_id, week_number),
            )
            return cur.fetchall()

    def latest_incomplete_week(self, league_id: int) -> int | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT MIN(week_number) AS week_number
                FROM schedule
                WHERE league_id = %s
                  AND is_complete = FALSE
                """,
                (league_id,),
            )
            row = cur.fetchone()
            return row["week_number"] if row and row["week_number"] is not None else None

    def latest_completed_week(self, league_id: int) -> int | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(week_number) AS week_number
                FROM schedule
                WHERE league_id = %s
                  AND is_complete = TRUE
                """,
                (league_id,),
            )
            row = cur.fetchone()
            return row["week_number"] if row and row["week_number"] is not None else None

    def table_exists(self, table_name: str) -> bool:
        """Return True if *table_name* exists in the current DB search_path.

        Uses ``to_regclass`` which is safe and requires no special privileges.
        Results are cached per-instance (i.e. per process lifetime) to avoid
        repeated round-trips for the same table name.
        """
        if not hasattr(self, "_table_exists_cache"):
            self._table_exists_cache: dict[str, bool] = {}
        if table_name in self._table_exists_cache:
            return self._table_exists_cache[table_name]
        try:
            with self.conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (table_name,))
                row = cur.fetchone()
                result = bool(row["exists"]) if row else False
        except Exception as exc:
            LOGGER.debug("table_exists(%s) check failed, assuming absent: %s", table_name, exc)
            result = False
        self._table_exists_cache[table_name] = result
        return result

    def player_search(self, league_id: int, name_query: str) -> list[dict[str, Any]]:
        """Search for players by name, falling back to ``playerstats`` for any
        specialised ``player_*_stats`` tables that do not yet exist."""
        has_passing = self.table_exists("player_passing_stats")
        has_rushing = self.table_exists("player_rushing_stats")
        has_receiving = self.table_exists("player_receiving_stats")
        has_defense = self.table_exists("player_defense_stats")

        params: list[Any] = []

        if has_passing:
            passing_join = """
                LEFT JOIN (
                    SELECT roster_id,
                           team_id,
                           SUM(COALESCE(pass_yds, 0)) AS pass_yards,
                           SUM(COALESCE(pass_tds, 0)) AS pass_tds,
                           SUM(COALESCE(pass_ints, 0)) AS interceptions
                    FROM player_passing_stats
                    WHERE (season_type = 'reg' OR season_type IS NULL)
                    GROUP BY roster_id, team_id
                ) pps ON pps.roster_id = p.id AND pps.team_id = p.team_id"""
        else:
            passing_join = """
                LEFT JOIN (
                    SELECT ps.player_id AS roster_id,
                           p2.team_id,
                           SUM(COALESCE(ps.pass_yards, 0)) AS pass_yards,
                           SUM(COALESCE(ps.pass_tds, 0)) AS pass_tds,
                           SUM(COALESCE(ps.interceptions, 0)) AS interceptions
                    FROM playerstats ps
                    JOIN player p2 ON p2.id = ps.player_id
                    WHERE ps.league_id = %s
                      AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                    GROUP BY ps.player_id, p2.team_id
                ) pps ON pps.roster_id = p.id AND pps.team_id = p.team_id"""
            params.append(league_id)

        if has_rushing:
            rushing_join = """
                LEFT JOIN (
                    SELECT roster_id,
                           team_id,
                           SUM(COALESCE(rush_yds, 0)) AS rush_yards,
                           SUM(COALESCE(rush_tds, 0)) AS rush_tds
                    FROM player_rushing_stats
                    WHERE (season_type = 'reg' OR season_type IS NULL)
                    GROUP BY roster_id, team_id
                ) prs ON prs.roster_id = p.id AND prs.team_id = p.team_id"""
        else:
            rushing_join = """
                LEFT JOIN (
                    SELECT ps.player_id AS roster_id,
                           p2.team_id,
                           SUM(COALESCE(ps.rush_yards, 0)) AS rush_yards,
                           SUM(COALESCE(ps.rush_tds, 0)) AS rush_tds
                    FROM playerstats ps
                    JOIN player p2 ON p2.id = ps.player_id
                    WHERE ps.league_id = %s
                      AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                    GROUP BY ps.player_id, p2.team_id
                ) prs ON prs.roster_id = p.id AND prs.team_id = p.team_id"""
            params.append(league_id)

        if has_receiving:
            receiving_join = """
                LEFT JOIN (
                    SELECT roster_id,
                           team_id,
                           SUM(COALESCE(rec_yds, 0)) AS rec_yards,
                           SUM(COALESCE(rec_tds, 0)) AS rec_tds,
                           SUM(COALESCE(receptions, 0)) AS receptions
                    FROM player_receiving_stats
                    WHERE (season_type = 'reg' OR season_type IS NULL)
                    GROUP BY roster_id, team_id
                ) prc ON prc.roster_id = p.id AND prc.team_id = p.team_id"""
        else:
            receiving_join = """
                LEFT JOIN (
                    SELECT ps.player_id AS roster_id,
                           p2.team_id,
                           SUM(COALESCE(ps.rec_yards, 0)) AS rec_yards,
                           SUM(COALESCE(ps.rec_tds, 0)) AS rec_tds,
                           SUM(COALESCE(ps.receptions, 0)) AS receptions
                    FROM playerstats ps
                    JOIN player p2 ON p2.id = ps.player_id
                    WHERE ps.league_id = %s
                      AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                    GROUP BY ps.player_id, p2.team_id
                ) prc ON prc.roster_id = p.id AND prc.team_id = p.team_id"""
            params.append(league_id)

        if has_defense:
            defense_join = """
                LEFT JOIN (
                    SELECT roster_id,
                           team_id,
                           SUM(COALESCE(def_tackles, 0)) AS tackles,
                           SUM(COALESCE(def_sacks, 0)) AS sacks,
                           SUM(COALESCE(def_ints, 0)) AS defensive_ints
                    FROM player_defense_stats
                    WHERE (season_type = 'reg' OR season_type IS NULL)
                    GROUP BY roster_id, team_id
                ) pds ON pds.roster_id = p.id AND pds.team_id = p.team_id"""
        else:
            defense_join = """
                LEFT JOIN (
                    SELECT ps.player_id AS roster_id,
                           p2.team_id,
                           SUM(COALESCE(ps.tackles, 0)) AS tackles,
                           SUM(COALESCE(ps.sacks, 0)) AS sacks,
                           SUM(COALESCE(ps.defensive_ints, 0)) AS defensive_ints
                    FROM playerstats ps
                    JOIN player p2 ON p2.id = ps.player_id
                    WHERE ps.league_id = %s
                      AND (ps.season_type = 'reg' OR ps.season_type IS NULL)
                    GROUP BY ps.player_id, p2.team_id
                ) pds ON pds.roster_id = p.id AND pds.team_id = p.team_id"""
            params.append(league_id)

        params.extend([league_id, f"%{name_query}%"])

        sql = f"""
            SELECT p.id AS roster_id,
                   p.first_name || ' ' || p.last_name AS full_name,
                   p.position,
                   p.overall_rating,
                   COALESCE(t.team_name, 'FA') AS team_name,
                   COALESCE(pps.pass_yards, 0) AS pass_yards,
                   COALESCE(pps.pass_tds, 0) AS pass_tds,
                   COALESCE(pps.interceptions, 0) AS interceptions,
                   COALESCE(prs.rush_yards, 0) AS rush_yards,
                   COALESCE(prs.rush_tds, 0) AS rush_tds,
                   COALESCE(prc.rec_yards, 0) AS rec_yards,
                   COALESCE(prc.rec_tds, 0) AS rec_tds,
                   COALESCE(prc.receptions, 0) AS receptions,
                   COALESCE(pds.tackles, 0) AS tackles,
                   COALESCE(pds.sacks, 0) AS sacks,
                   COALESCE(pds.defensive_ints, 0) AS defensive_ints,
                   0 AS fumbles_forced
            FROM player p
            JOIN team t ON t.id = p.team_id
            {passing_join}
            {rushing_join}
            {receiving_join}
            {defense_join}
            WHERE t.league_id = %s
              AND (p.first_name || ' ' || p.last_name) ILIKE %s
            ORDER BY p.first_name || ' ' || p.last_name ASC
            LIMIT 5
        """

        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def ensure_xp_user(self, guild_id: int, user: discord.abc.User) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_xp_users (guild_id, user_id, username, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (guild_id, user_id) DO UPDATE
                SET username = EXCLUDED.username,
                    updated_at = NOW()
                """,
                (guild_id, int(user.id), str(user)),
            )
            conn.commit()

    def get_xp_user(self, guild_id: int, user: discord.abc.User) -> dict[str, Any] | None:
        self.ensure_xp_user(guild_id, user)
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM bot_xp_users WHERE guild_id = %s AND user_id = %s", (guild_id, int(user.id)))
            row = cur.fetchone()
            return dict(row) if row else None

    def update_xp_progress(self, guild_id: int, user: discord.abc.User, xp: int, level: int, messages_counted: int, last_xp_at: float) -> None:
        self.ensure_xp_user(guild_id, user)
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_xp_users
                SET xp = %s,
                    level = %s,
                    messages_counted = %s,
                    last_xp_at = %s,
                    username = %s,
                    updated_at = NOW()
                WHERE guild_id = %s
                  AND user_id = %s
                """,
                (int(xp), int(level), int(messages_counted), float(last_xp_at), str(user), guild_id, int(user.id)),
            )
            conn.commit()

    def xp_leaderboard(self, guild_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM bot_xp_users
                WHERE guild_id = %s
                ORDER BY level DESC, xp DESC, messages_counted DESC, username ASC
                """,
                (guild_id,),
            )
            return [dict(row) for row in cur.fetchall()]

    def create_bounty(self, guild_id: int, title: str, description: str, reward: float, created_by: int) -> int:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_bounties (guild_id, title, description, reward, created_by)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (guild_id, title, description, float(reward), int(created_by)),
            )
            row = cur.fetchone()
            conn.commit()
            return safe_int((row or {}).get("id"))

    def list_active_bounties(self, guild_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM bot_bounties
                WHERE guild_id = %s
                  AND is_active = TRUE
                ORDER BY id DESC
                """,
                (guild_id,),
            )
            return [dict(row) for row in cur.fetchall()]

    def claim_bounty(self, guild_id: int, bounty_id: int, user: discord.abc.User) -> dict[str, Any] | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM bot_bounties
                WHERE guild_id = %s
                  AND id = %s
                  AND is_active = TRUE
                FOR UPDATE
                """,
                (guild_id, int(bounty_id)),
            )
            bounty = cur.fetchone()
            if bounty is None:
                return None
            cur.execute(
                """
                UPDATE bot_bounties
                SET is_active = FALSE,
                    claimed_by = %s,
                    claimed_at = NOW()
                WHERE id = %s
                RETURNING *
                """,
                (int(user.id), int(bounty_id)),
            )
            updated = cur.fetchone()
            conn.commit()
            return dict(updated) if updated else None

    def get_bounty(self, guild_id: int, bounty_id: int) -> dict[str, Any] | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM bot_bounties WHERE guild_id = %s AND id = %s", (guild_id, int(bounty_id)))
            row = cur.fetchone()
            return dict(row) if row else None

    def update_bounty(self, guild_id: int, bounty_id: int, title: str | None, description: str | None, reward: float | None) -> dict[str, Any] | None:
        current = self.get_bounty(guild_id, bounty_id)
        if current is None:
            return None
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_bounties
                SET title = %s,
                    description = %s,
                    reward = %s
                WHERE guild_id = %s
                  AND id = %s
                RETURNING *
                """,
                (
                    title.strip() if title else current["title"],
                    description.strip() if description else current["description"],
                    float(reward) if reward is not None else float(current["reward"]),
                    guild_id,
                    int(bounty_id),
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None

    def create_trade(
        self,
        guild_id: int,
        submitted_by: discord.abc.User,
        coach_one: discord.abc.User,
        coach_two: discord.abc.User,
        team_one_name: str,
        team_two_name: str,
        team_one_gets: str,
        team_two_gets: str,
        notes: str = "",
        announcement_channel_id: int = 0,
    ) -> dict[str, Any]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_trades (
                    guild_id,
                    submitted_by,
                    submitted_username,
                    coach_one_user_id,
                    coach_two_user_id,
                    team_one_name,
                    team_two_name,
                    team_one_gets,
                    team_two_gets,
                    notes,
                    announcement_channel_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    guild_id,
                    int(submitted_by.id),
                    str(submitted_by),
                    int(coach_one.id),
                    int(coach_two.id),
                    team_one_name,
                    team_two_name,
                    team_one_gets,
                    team_two_gets,
                    notes,
                    announcement_channel_id or None,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else {}

    def set_trade_review_message(self, trade_id: int, channel_id: int, message_id: int) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE bot_trades SET review_channel_id = %s, review_message_id = %s WHERE id = %s",
                (int(channel_id), int(message_id), int(trade_id)),
            )
            conn.commit()

    def set_trade_announcement_message(self, trade_id: int, channel_id: int, message_id: int) -> None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE bot_trades SET announcement_channel_id = %s, announcement_message_id = %s WHERE id = %s",
                (int(channel_id), int(message_id), int(trade_id)),
            )
            conn.commit()

    def get_trade(self, trade_id: int) -> dict[str, Any] | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM bot_trades WHERE id = %s", (int(trade_id),))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_trades(self, guild_id: int, limit: int = 10) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM bot_trades
                WHERE guild_id = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (guild_id, max(1, min(limit, 25))),
            )
            return [dict(row) for row in cur.fetchall()]

    def get_trade_by_message(self, message_id: int) -> dict[str, Any] | None:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM bot_trades WHERE review_message_id = %s", (int(message_id),))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_trade_votes(self, trade_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT voter_user_id, voter_username, vote, created_at, updated_at
                FROM bot_trade_votes
                WHERE trade_id = %s
                ORDER BY updated_at ASC, id ASC
                """,
                (int(trade_id),),
            )
            return [dict(row) for row in cur.fetchall()]

    def upsert_trade_vote(self, trade_id: int, voter: discord.abc.User, vote: str) -> tuple[dict[str, Any], bool]:
        vote = (vote or "").strip().lower()
        if vote not in {"approve", "deny"}:
            raise ValueError("Vote must be approve or deny")

        with self.conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM bot_trades WHERE id = %s FOR UPDATE", (int(trade_id),))
            trade = cur.fetchone()
            if trade is None:
                raise ValueError("Trade not found")
            if safe_text(trade.get("status"), "pending") != "pending":
                return dict(trade), False

            cur.execute(
                """
                INSERT INTO bot_trade_votes (trade_id, voter_user_id, voter_username, vote)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (trade_id, voter_user_id) DO UPDATE
                SET voter_username = EXCLUDED.voter_username,
                    vote = EXCLUDED.vote,
                    updated_at = NOW()
                """,
                (trade_id, int(voter.id), str(voter), vote),
            )
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE vote='approve') AS approve_count,
                    COUNT(*) FILTER (WHERE vote='deny') AS deny_count
                FROM bot_trade_votes
                WHERE trade_id = %s
                """,
                (trade_id,),
            )
            counts = cur.fetchone() or {}
            approve_count = safe_int(counts.get("approve_count"))
            deny_count = safe_int(counts.get("deny_count"))
            cur.execute(
                """
                UPDATE bot_trades
                SET approve_count=%s,
                    deny_count=%s
                WHERE id=%s
                RETURNING *
                """,
                (approve_count, deny_count, trade_id),
            )
            updated = cur.fetchone()
            conn.commit()
            return dict(updated) if updated else {}, True

    def finalize_trade(self, trade_id: int, decision: str, finalized_by: int | None = None, reason: str = "") -> dict[str, Any] | None:
        decision = (decision or "").strip().lower()
        if decision not in {"approved", "denied"}:
            raise ValueError("Decision must be approved or denied")
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_trades
                SET status = %s,
                    finalized_by = %s,
                    finalized_reason = %s,
                    finalized_at = NOW()
                WHERE id = %s
                RETURNING *
                """,
                (decision, finalized_by, reason, int(trade_id)),
            )
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None


def xp_required_for_level(level: int) -> int:
    if level <= 1:
        return 0
    return int(round(100 * ((level - 1) ** 1.5)))


def level_from_xp(xp: int) -> int:
    level = 1
    while xp >= xp_required_for_level(level + 1):
        level += 1
    return level


def xp_progress_text(xp: int) -> tuple[int, int, int]:
    level = level_from_xp(xp)
    current_floor = xp_required_for_level(level)
    next_floor = xp_required_for_level(level + 1)
    needed = max(next_floor - current_floor, 1)
    progress = xp - current_floor
    return level, progress, needed


def resolve_openai_api_key(config: dict[str, Any] | None = None, guild_id: int | None = None) -> str:
    cfg_key = safe_text((config or {}).get("openai_api_key"), "")
    if cfg_key:
        return cfg_key
    if OPENAI_API_KEY_TEMPLATE and guild_id is not None:
        template = OPENAI_API_KEY_TEMPLATE
        try:
            formatted = template.format(guild_id=guild_id)
            if formatted and "{" not in formatted:
                return formatted
        except Exception:
            pass
    return DEFAULT_OPENAI_API_KEY


def _redact_key_like_strings(text: str) -> str:
    """Replace API-key-like strings (prefix + many asterisks + suffix) with [REDACTED]."""
    return re.sub(r'[A-Za-z0-9+/\-_]{4,}\*{10,}[A-Za-z0-9+/\-_]{2,}', '[REDACTED]', text)


def call_openai_text(
    prompt: str,
    max_output_tokens: int = 220,
    config: dict[str, Any] | None = None,
    guild_id: int | None = None,
) -> str:
    openai_api_key = resolve_openai_api_key(config, guild_id)
    if not openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    payload = {
        "model": OPENAI_MODEL,
        "input": prompt,
        "max_output_tokens": max_output_tokens,
    }
    req = urllib_request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {openai_api_key}",
        },
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=45) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI HTTP {exc.code}: {_redact_key_like_strings(details[:500])}")
    except Exception as exc:
        raise RuntimeError(f"OpenAI request failed: {exc}")

    if isinstance(data, dict):
        direct = data.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        output = data.get("output", [])
        chunks: list[str] = []
        for item in output:
            if item.get("type") != "message":
                continue
            for content in item.get("content", []):
                if content.get("type") in {"output_text", "text"}:
                    text_piece = safe_text(content.get("text"))
                    if text_piece:
                        chunks.append(text_piece)
        joined = "\n".join(piece.strip() for piece in chunks if piece.strip()).strip()
        if joined:
            return joined

    raise RuntimeError("OpenAI returned no text output")



# MATCHUP_ANGLES is imported from storyline_templates


def format_player_stats_line(entry: dict[str, Any], category: str) -> str:
    """Return a compact season-to-date stats line for a player based on their category."""
    if category == "passing":
        yards = safe_int(entry.get("pass_yards"))
        tds = safe_int(entry.get("pass_tds"))
        ints = safe_int(entry.get("interceptions"))
        return f"{yards:,} pass yds | {tds} TD | {ints} INT"
    if category == "rushing":
        yards = safe_int(entry.get("rush_yards"))
        tds = safe_int(entry.get("rush_tds"))
        return f"{yards:,} rush yds | {tds} TD"
    if category == "defense":
        tackles = safe_int(entry.get("tackles"))
        sacks = safe_int(entry.get("sacks"))
        ints = safe_int(entry.get("defensive_ints"))
        ff = safe_int(entry.get("fumbles_forced"))
        parts = [f"{tackles} tkl", f"{sacks} sck", f"{ints} INT"]
        if ff:
            parts.append(f"{ff} FF")
        return " | ".join(parts)
    return ""


def format_player_why_line(
    entry: dict[str, Any],
    category: str,
    team_name: str,
    game_id: int,
    db: "Database | None" = None,
    guild_id: int = 0,
    league_id: int = 0,
) -> str:
    """Return a template snippet explaining why the player matters.

    Draws from the large per-category pools in storyline_templates.
    Uses choose_nonrepeating when *db* is provided, otherwise falls back
    to a deterministic choice.
    """
    name = safe_text(entry.get("player_name"))
    first = name.split()[0] if name else "This player"
    seed = f"why-{game_id}-{name}-{category}"

    if category == "passing":
        raw_options = PLAYER_WHY_PASSING
        content_type = "player_why_passing"
    elif category == "rushing":
        raw_options = PLAYER_WHY_RUSHING
        content_type = "player_why_rushing"
    elif category == "defense":
        raw_options = PLAYER_WHY_DEFENSE
        content_type = "player_why_defense"
    else:
        return f"{first} is a key contributor for {team_name} this season."

    if db is not None and guild_id and league_id:
        template = choose_nonrepeating(raw_options, seed, content_type, db, guild_id, league_id)
    else:
        template = deterministic_choice(raw_options, seed)
    return template.format(first=first, team_name=team_name)


def build_matchup_prompt(facts: dict[str, Any]) -> str:
    return (
        "You are writing one matchup preview for a Madden franchise Discord.\n"
        "Hard rules:\n"
        "- Write exactly 4 or 5 sentences.\n"
        "- Use only the provided facts.\n"
        "- Do not invent players, streaks, quotes, injuries, awards, or outcomes.\n"
        f"- The assigned narrative angle is: {facts['angle']}.\n"
        "- End with why this game matters.\n\n"
        f"Facts JSON:\n{json.dumps(facts, indent=2, default=str)}"
    )


def build_matchup_facts(
    db: Database,
    league_id: int,
    game_row: dict[str, Any],
    is_gotw: bool,
    guild_id: int = 0,
) -> dict[str, Any]:
    away_team = db.fetch_team_info_by_id(league_id, safe_int(game_row.get("away_team_id"))) or {
        "team_name": game_row.get("away_team", "Away Team"),
        "wins": safe_int(game_row.get("away_wins")),
        "losses": safe_int(game_row.get("away_losses")),
        "ties": safe_int(game_row.get("away_ties")),
        "pts_for": 0,
        "pts_against": 0,
        "turnover_diff": 0,
        "seed": 0,
    }
    home_team = db.fetch_team_info_by_id(league_id, safe_int(game_row.get("home_team_id"))) or {
        "team_name": game_row.get("home_team", "Home Team"),
        "wins": safe_int(game_row.get("home_wins")),
        "losses": safe_int(game_row.get("home_losses")),
        "ties": safe_int(game_row.get("home_ties")),
        "pts_for": 0,
        "pts_against": 0,
        "turnover_diff": 0,
        "seed": 0,
    }
    away_leaders = db.fetch_team_top_leaders(league_id, safe_int(game_row.get("away_team_id")))
    home_leaders = db.fetch_team_top_leaders(league_id, safe_int(game_row.get("home_team_id")))

    abs_win_gap = abs(safe_int(away_team.get("wins")) - safe_int(home_team.get("wins")))
    if is_gotw:
        angle = "marquee showdown"
    elif abs_win_gap >= 4:
        angle = "prove-it test"
    elif safe_text(away_team.get("division")) == safe_text(home_team.get("division")) and safe_text(away_team.get("division")):
        angle = "playoff pressure"
    elif safe_int(away_team.get("turnover_diff")) != safe_int(home_team.get("turnover_diff")):
        angle = "turnover battle"
    elif safe_text(away_leaders.get("passing", {}).get("player_name")) and safe_text(home_leaders.get("defense", {}).get("player_name")):
        angle = "quarterback-vs-pass-rush"
    else:
        angle = "style clash"

    week = safe_int(game_row.get("week_number"))
    game_id = safe_int(game_row.get("game_id"))
    angle_seed = f"angle-{game_id}-{angle}"

    if guild_id and league_id:
        angle = choose_nonrepeating(MATCHUP_ANGLES, angle_seed, "matchup_angle", db, guild_id, league_id)
    else:
        # Legacy in-memory fallback
        if week:
            used = _WEEKS_MEMORY["angles"][week]
            if angle in used:
                for candidate in MATCHUP_ANGLES:
                    if candidate not in used:
                        angle = candidate
                        break
            used.append(angle)

    facts: dict[str, Any] = {
        "week": week,
        "game_id": game_id,
        "is_gotw": bool(is_gotw),
        "angle": angle,
        "away_team": away_team,
        "home_team": home_team,
        "away_storyline": build_team_storyline(away_team, away_leaders, db=db, guild_id=guild_id, league_id=league_id),
        "home_storyline": build_team_storyline(home_team, home_leaders, db=db, guild_id=guild_id, league_id=league_id),
        "players_to_watch": [],
        "headline": "",
        "stakes_line": "",
    }

    facts["headline"] = deterministic_choice(
        [
            f"Week {week} spotlight: {safe_text(away_team.get('team_name'))} at {safe_text(home_team.get('team_name'))}",
            f"{safe_text(away_team.get('team_name'))} vs {safe_text(home_team.get('team_name'))}: {angle.title()}",
        ],
        f"headline-{facts['game_id']}-{angle}",
    )

    picks: list[dict[str, Any]] = []
    for leaders, team_name in [
        (away_leaders, safe_text(away_team.get("team_name"))),
        (home_leaders, safe_text(home_team.get("team_name"))),
    ]:
        # Pick the most prominent stat category per team in priority order:
        # passing → rushing → defense. Break after the first valid entry.
        for category in ("passing", "rushing", "defense"):
            entry = leaders.get(category) or {}
            if safe_text(entry.get("player_name")):
                picks.append({
                    "player_name": safe_text(entry["player_name"]),
                    "team_name": team_name,
                    "category": category,
                    "stats_line": format_player_stats_line(entry, category),
                    "why_line": format_player_why_line(
                        entry, category, team_name, game_id,
                        db=db, guild_id=guild_id, league_id=league_id,
                    ),
                })
                break
    facts["players_to_watch"] = picks[:2]

    stakes_seed = f"stakes-{game_id}"
    if guild_id and league_id:
        facts["stakes_line"] = choose_nonrepeating(
            MATCHUP_STAKES, stakes_seed, "matchup_stakes", db, guild_id, league_id
        )
    else:
        facts["stakes_line"] = deterministic_choice(MATCHUP_STAKES, stakes_seed)
    return facts


def template_matchup_preview_text(
    facts: dict[str, Any],
    db: "Database | None" = None,
    guild_id: int = 0,
    league_id: int = 0,
) -> str:
    away_name = safe_text(facts["away_team"].get("team_name"))
    home_name = safe_text(facts["home_team"].get("team_name"))
    week = safe_int(facts.get("week"))
    game_id = safe_int(facts.get("game_id"))
    angle = safe_text(facts.get("angle"))

    opener_seed = f"open-{game_id}"
    body_seed = f"body-{game_id}"

    if db is not None and guild_id and league_id:
        opener_template = choose_nonrepeating(
            MATCHUP_OPENERS, opener_seed, "matchup_opener", db, guild_id, league_id
        )
        body_template = choose_nonrepeating(
            MATCHUP_BODIES, body_seed, "matchup_body", db, guild_id, league_id
        )
    else:
        opener_template = deterministic_choice(MATCHUP_OPENERS, opener_seed)
        body_template = deterministic_choice(MATCHUP_BODIES, body_seed)

    opener = opener_template.format(week=week, away=away_name, home=home_name, angle=angle)
    body = body_template.format(away=away_name, home=home_name, week=week)

    players = facts.get("players_to_watch") or []

    def _player_display(p: Any) -> str:
        if isinstance(p, dict):
            return f"{p['player_name']} ({p['team_name']})"
        return str(p)

    if len(players) > 1:
        player_line = f"Players to watch include {_player_display(players[0])} and {_player_display(players[1])}."
    elif players:
        player_line = f"One player to watch is {_player_display(players[0])}."
    else:
        player_line = "Execution in key possessions will likely decide this."
    return " ".join([opener, body, player_line, facts.get("stakes_line", "")]).strip()


async def generate_matchup_preview_text(
    db: Database,
    config: dict[str, Any],
    league_id: int,
    game_row: dict[str, Any],
    is_gotw: bool,
    guild_id: int | None = None,
) -> tuple[str, bool, dict[str, Any]]:
    _guild_id = guild_id or 0
    facts = await asyncio.to_thread(build_matchup_facts, db, league_id, game_row, is_gotw, _guild_id)
    fallback = template_matchup_preview_text(facts, db=db, guild_id=_guild_id, league_id=league_id)
    if not resolve_openai_api_key(config, guild_id):
        return fallback, False, facts
    try:
        ai_text = await asyncio.to_thread(call_openai_text, build_matchup_prompt(facts), 220, config, guild_id)
        cleaned = re.sub(r"\s+", " ", ai_text).strip()
        return cleaned or fallback, True, facts
    except Exception as exc:
        LOGGER.warning("AI matchup preview failed for game %s: %s", game_row.get("game_id"), exc)
        return fallback, False, facts


def build_weekly_news_prompt(facts: dict[str, Any]) -> str:
    return (
        "You are writing a weekly league news article for a Madden franchise Discord.\n"
        "Rules:\n"
        "- Write 5 to 7 sentences.\n"
        "- Use only provided facts.\n"
        "- Highlight standings pressure, headline games, and stat-race movement.\n"
        "Facts JSON:\n"
        f"{json.dumps(facts, indent=2, default=str)}"
    )


def template_weekly_news_text(
    facts: dict[str, Any],
    db: "Database | None" = None,
    guild_id: int = 0,
    league_id: int = 0,
) -> str:
    week = safe_int(facts.get("week"))
    top_team = safe_text((facts.get("standings") or [{}])[0].get("team_name"), "the top seed")
    game = (facts.get("top_games") or [{}])[0]
    away = safe_text(game.get("away_team"), "Away Team")
    home = safe_text(game.get("home_team"), "Home Team")

    opener_seed = f"news-open-{week}-{top_team}"
    body_seed = f"news-body-{week}-{away}-{home}"
    fmt = {"week": week, "top_team": top_team, "away": away, "home": home}

    if db is not None and guild_id and league_id:
        opener_template = choose_nonrepeating(
            WEEKLY_NEWS_OPENERS, opener_seed, "weekly_news_opener", db, guild_id, league_id
        )
        body_template = choose_nonrepeating(
            WEEKLY_NEWS_BODY_LINES, body_seed, "weekly_news_body", db, guild_id, league_id
        )
    else:
        opener_template = deterministic_choice(WEEKLY_NEWS_OPENERS, opener_seed)
        body_template = deterministic_choice(WEEKLY_NEWS_BODY_LINES, body_seed)

    opener = opener_template.format(**fmt)
    body = body_template.format(**fmt)
    return " ".join([opener, body]).strip()


def find_closest_division_race(standings: list[dict[str, Any]]) -> str:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in standings:
        grouped[safe_text(row.get("division_name"), "Unknown")].append(row)

    fallback_text = "Division races are still taking shape."
    closest_gap: float | None = None
    for division, rows in grouped.items():
        ordered = sorted(
            rows,
            key=lambda item: (
                -safe_int(item.get("wins")),
                safe_int(item.get("losses")),
                -safe_int(item.get("ties")),
                safe_text(item.get("team_name")),
            ),
        )
        if len(ordered) < 2:
            continue
        leader = ordered[0]
        chaser = ordered[1]
        gap = ((safe_int(leader.get("wins")) - safe_int(chaser.get("wins"))) + (safe_int(chaser.get("losses")) - safe_int(leader.get("losses")))) / 2
        if closest_gap is None or gap < closest_gap:
            closest_gap = gap
            fallback_text = (
                f"The closest division race is {division}: {safe_text(leader.get('team_name'))} "
                f"({wins_losses_ties_text(leader)}) vs {safe_text(chaser.get('team_name'))} "
                f"({wins_losses_ties_text(chaser)}), separated by {gap:.1f} game(s)."
            )
    return fallback_text


def build_headline_prompt(facts: dict[str, Any]) -> str:
    return HEADLINE_PROMPT_INSTRUCTIONS + "Facts JSON:\n" + json.dumps(facts, indent=2, default=str)


def template_headline_text(
    facts: dict[str, Any],
    db: "Database | None" = None,
    guild_id: int = 0,
    league_id: int = 0,
) -> str:
    week = facts.get("week", "")
    top_team = facts["top_team"]
    top_record = facts["top_record"]
    passer = facts["passer"]
    pass_yards = facts["pass_yards"]
    pass_tds = facts["pass_tds"]
    rusher = facts["rusher"]
    rush_yards = facts["rush_yards"]
    receiver = facts["receiver"]
    rec_yards = facts["rec_yards"]
    rec_tds = facts["rec_tds"]
    td_leader = facts["td_leader"]
    total_tds = facts["total_tds"]
    scores_summary = facts["scores_summary"]
    closest_race = facts["closest_race"]

    seed_base = f"headline-{week}-{top_team}"

    def _pick(pool: list[str], ctype: str, seed: str) -> str:
        if db is not None and guild_id and league_id:
            return choose_nonrepeating(pool, seed, ctype, db, guild_id, league_id)
        return deterministic_choice(pool, seed)

    line1 = _pick(HEADLINE_LINE1, "headline_line1", f"{seed_base}-l1").format(
        top_team=top_team, top_record=top_record
    )
    line3 = _pick(HEADLINE_LINE3, "headline_line3", f"{seed_base}-l3").format(
        passer=passer, pass_yards=pass_yards, pass_tds=pass_tds
    )
    line4 = _pick(HEADLINE_LINE4, "headline_line4", f"{seed_base}-l4").format(
        rusher=rusher, rush_yards=rush_yards
    )
    line5 = _pick(HEADLINE_LINE5, "headline_line5", f"{seed_base}-l5").format(
        receiver=receiver, rec_yards=rec_yards, rec_tds=rec_tds
    )

    return "\n".join(
        [
            f"1. {line1}",
            f"2. {closest_race}",
            f"3. {line3}",
            f"4. {line4}",
            f"5. {line5}",
            f"6. {td_leader} leads the league in total touchdowns with {total_tds}. Week {week} is complete — {scores_summary}",
        ]
    )


class TradeReviewView(discord.ui.View):
    def __init__(self, bot: "NexusLeagueBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    async def _handle_vote(self, interaction: discord.Interaction, vote: str) -> None:
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("This must be used inside a server.", ephemeral=True)
            return
        if not interaction.guild:
            await interaction.response.send_message("This must be used inside a server.", ephemeral=True)
            return
        config = await asyncio.to_thread(self.bot.db.get_guild_config, interaction.guild.id)
        committee_role_id = safe_int((config or {}).get("trade_committee_role_id"))
        if committee_role_id and committee_role_id not in {role.id for role in interaction.user.roles}:
            await interaction.response.send_message("You are not on the trade committee.", ephemeral=True)
            return
        if interaction.message is None:
            await interaction.response.send_message("Could not resolve trade message.", ephemeral=True)
            return

        trade_row = await asyncio.to_thread(self.bot.db.get_trade_by_message, int(interaction.message.id))
        if not trade_row:
            await interaction.response.send_message("Trade not found.", ephemeral=True)
            return
        if safe_text(trade_row.get("status"), "pending").lower() != "pending":
            await interaction.response.send_message("This trade is already finalized.", ephemeral=True)
            return

        trade_row, _ = await asyncio.to_thread(self.bot.db.upsert_trade_vote, safe_int(trade_row.get("id")), interaction.user, vote)
        trade_row = await self.bot.finalize_trade_if_threshold_met(trade_row, int(interaction.user.id))
        status = safe_text(trade_row.get("status"), "pending").lower()
        content = f"<@&{committee_role_id}>" if committee_role_id else None
        await interaction.response.edit_message(
            content=content,
            embed=await self.bot.build_trade_embed(trade_row),
            view=TradeReviewView(self.bot) if status == "pending" else None,
        )

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, emoji="✅", custom_id="trade_vote_approve")
    async def approve(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._handle_vote(interaction, "approve")

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, emoji="❌", custom_id="trade_vote_deny")
    async def deny(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._handle_vote(interaction, "deny")


class RosterPaginationView(discord.ui.View):
    def __init__(self, embeds: list[discord.Embed], author_id: int | None = None) -> None:
        super().__init__(timeout=120)
        self.embeds = embeds
        self.author_id = author_id
        self.page_index = 0
        self.message: discord.Message | None = None
        self._update_buttons()

    def _update_buttons(self) -> None:
        self.previous_page.disabled = self.page_index <= 0
        self.next_page.disabled = self.page_index >= len(self.embeds) - 1

    async def _edit_page(self, interaction: discord.Interaction) -> None:
        if self.author_id is not None and interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command author can change roster pages.", ephemeral=True)
            return
        self._update_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.page_index], view=self)

    async def on_timeout(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def previous_page(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.page_index > 0:
            self.page_index -= 1
        await self._edit_page(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.page_index < len(self.embeds) - 1:
            self.page_index += 1
        await self._edit_page(interaction)


class GameSchedulingView(discord.ui.View):
    """Persistent view posted in each game channel when it is created.

    Buttons:
    - ⏰ Scheduled: stops reminders, marks game as scheduled.
    - ✅ Completed: marks game completed, enables Delete and triggers auto-delete in 1 hour.
    - 🗑️ Delete Channel: admin-only, enabled after game is completed.
    """

    def __init__(self, bot: "NexusLeagueBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_updated_view(self, *, scheduled: bool, completed: bool) -> "GameSchedulingView":
        """Return a new GameSchedulingView with buttons set to appropriate disabled states."""
        view = GameSchedulingView(self.bot)
        for child in view.children:
            if not isinstance(child, discord.ui.Button):
                continue
            if child.custom_id == "game_sched_scheduled":
                child.disabled = scheduled or completed
            elif child.custom_id == "game_sched_completed":
                child.disabled = completed
            elif child.custom_id == "game_sched_delete":
                child.disabled = not completed
        return view

    async def _check_owner_or_admin(self, interaction: discord.Interaction, state: dict) -> bool:
        """Return True if the interacting user is a matched owner or guild admin."""
        home_uid = state.get("home_user_id")
        away_uid = state.get("away_user_id")
        user_id = interaction.user.id
        if user_id == home_uid or user_id == away_uid:
            return True
        return await self.bot.user_is_admin(interaction)

    # ------------------------------------------------------------------
    # Buttons
    # ------------------------------------------------------------------

    @discord.ui.button(
        label="Scheduled",
        style=discord.ButtonStyle.primary,
        emoji="⏰",
        custom_id="game_sched_scheduled",
    )
    async def scheduled(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not interaction.guild or interaction.channel is None:
            await interaction.response.send_message("Cannot process this request.", ephemeral=True)
            return
        state = await asyncio.to_thread(self.bot.db.get_game_channel_state, int(interaction.channel.id))
        if not state:
            await interaction.response.send_message("No game state found for this channel.", ephemeral=True)
            return
        if state.get("scheduled") or state.get("completed"):
            await interaction.response.send_message("This game is already marked scheduled or completed.", ephemeral=True)
            return
        if not await self._check_owner_or_admin(interaction, state):
            await interaction.response.send_message(
                "Only the two matched users or an admin can mark this game as scheduled.",
                ephemeral=True,
            )
            return
        await asyncio.to_thread(self.bot.db.mark_game_scheduled, int(interaction.channel.id))
        await interaction.response.edit_message(
            content=(
                "**Game Scheduling Status**\n"
                "⏰ This game has been marked as **Scheduled**. Reminders have stopped.\n\n"
                f"Marked by {interaction.user.mention}."
            ),
            view=self._make_updated_view(scheduled=True, completed=False),
        )

    @discord.ui.button(
        label="Completed",
        style=discord.ButtonStyle.success,
        emoji="✅",
        custom_id="game_sched_completed",
    )
    async def completed(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not interaction.guild or interaction.channel is None:
            await interaction.response.send_message("Cannot process this request.", ephemeral=True)
            return
        state = await asyncio.to_thread(self.bot.db.get_game_channel_state, int(interaction.channel.id))
        if not state:
            await interaction.response.send_message("No game state found for this channel.", ephemeral=True)
            return
        if state.get("completed"):
            await interaction.response.send_message("This game is already marked completed.", ephemeral=True)
            return
        if not await self._check_owner_or_admin(interaction, state):
            await interaction.response.send_message(
                "Only the two matched users or an admin can mark this game as completed.",
                ephemeral=True,
            )
            return
        await asyncio.to_thread(self.bot.db.mark_game_completed, int(interaction.channel.id))
        await interaction.response.edit_message(
            content=(
                "**Game Scheduling Status**\n"
                "✅ This game has been marked as **Completed**. Reminders have stopped.\n\n"
                f"Marked by {interaction.user.mention}.\n\n"
                "⏳ This channel will be automatically deleted in **1 hour**.\n"
                "An admin may also delete it immediately using the button below."
            ),
            view=self._make_updated_view(scheduled=True, completed=True),
        )

    @discord.ui.button(
        label="Delete Channel",
        style=discord.ButtonStyle.danger,
        emoji="🗑️",
        custom_id="game_sched_delete",
    )
    async def delete_channel(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not interaction.guild or interaction.channel is None:
            await interaction.response.send_message("Cannot process this request.", ephemeral=True)
            return
        if not await self.bot.user_is_admin(interaction):
            await interaction.response.send_message(
                "Only admins can delete this channel.",
                ephemeral=True,
            )
            return
        state = await asyncio.to_thread(self.bot.db.get_game_channel_state, int(interaction.channel.id))
        if not state:
            await interaction.response.send_message("No game state found for this channel.", ephemeral=True)
            return
        if not state.get("completed"):
            await interaction.response.send_message(
                "Please mark the game as **Completed** before deleting the channel.",
                ephemeral=True,
            )
            return
        channel = interaction.channel
        await interaction.response.send_message("🗑️ Deleting channel...", ephemeral=True)
        await asyncio.to_thread(self.bot.db.delete_game_channel_state, int(channel.id))
        try:
            await channel.delete(reason=f"Game completed — deleted via bot by {interaction.user}")
        except discord.HTTPException as exc:
            LOGGER.warning("GameSchedulingView: failed to delete channel %s: %s", channel.id, exc)
            await interaction.followup.send("Failed to delete the channel. Please delete it manually.", ephemeral=True)


class GameChannelDeleteView(discord.ui.View):
    """Persistent view shown after a game is marked Completed, providing a Delete Channel button."""

    def __init__(self, bot: "NexusLeagueBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Delete Channel",
        style=discord.ButtonStyle.danger,
        emoji="🗑️",
        custom_id="game_completed_delete",
    )
    async def delete_channel(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not interaction.guild or interaction.channel is None:
            await interaction.response.send_message("Cannot process this request.", ephemeral=True)
            return
        if not await self.bot.user_is_admin(interaction):
            await interaction.response.send_message(
                "Only admins can delete this channel.",
                ephemeral=True,
            )
            return
        channel = interaction.channel
        await interaction.response.send_message("🗑️ Deleting channel...", ephemeral=True)
        await asyncio.to_thread(self.bot.db.delete_game_channel_state, int(channel.id))
        try:
            await channel.delete(reason=f"Game completed — deleted via bot by {interaction.user}")
        except discord.HTTPException as exc:
            LOGGER.warning("GameChannelDeleteView: failed to delete channel %s: %s", channel.id, exc)
            await interaction.followup.send("Failed to delete the channel. Please delete it manually.", ephemeral=True)


class NexusLeagueBot(discord.Client):
    def __init__(self, db: Database, guild_ids: list[int]) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.message_content = True
        super().__init__(intents=intents)
        self.db = db
        self.tree = app_commands.CommandTree(self)
        self.guild_ids = guild_ids
        # Idempotency guard: maps interaction_id -> monotonic timestamp for create_weekly_channels
        self._active_create_channel_ids: dict[int, float] = {}

    async def setup_hook(self) -> None:
        setup_group = app_commands.Group(name="setup", description="Configure this server")
        leaders_group = app_commands.Group(name="leaders", description="View season leaders")
        post_group = app_commands.Group(name="post", description="Post content to leaders channel")
        team_group = app_commands.Group(name="team", description="Team commands")
        player_group = app_commands.Group(name="player", description="Player commands")

        @setup_group.command(name="league", description="Set the league id for this Discord server")
        @app_commands.describe(league_id="League ID from the league table")
        async def setup_league(interaction: discord.Interaction, league_id: int) -> None:
            if not interaction.guild or not interaction.user:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return

            if not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return

            await asyncio.to_thread(self.db.upsert_guild_league, interaction.guild.id, league_id)
            league_name = await asyncio.to_thread(self.db.get_league_name, league_id)
            embed = discord.Embed(
                title="League Configuration Updated",
                description=f"This server now uses **{league_name}** (`league_id={league_id}`).",
                color=discord.Color.green(),
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

        @setup_group.command(name="channels", description="Set the log and leaders channels")
        async def setup_channels(
            interaction: discord.Interaction,
            log: discord.TextChannel,
            leaders: discord.TextChannel,
        ) -> None:
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            if not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return

            config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
            if not config:
                await interaction.response.send_message(NO_SETUP_MESSAGE, ephemeral=True)
                return

            await asyncio.to_thread(self.db.update_channels, interaction.guild.id, log.id, leaders.id)
            embed = discord.Embed(
                title="Channel Configuration Updated",
                description=f"Log channel: {log.mention}\nLeaders channel: {leaders.mention}",
                color=discord.Color.green(),
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

        @setup_group.command(name="news_channel", description="Set weekly news channel")
        async def setup_news_channel(interaction: discord.Interaction, channel: discord.TextChannel) -> None:
            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
            if not config:
                await interaction.response.send_message(NO_SETUP_MESSAGE, ephemeral=True)
                return
            await asyncio.to_thread(self.db.update_news_channel, interaction.guild.id, channel.id)
            await interaction.response.send_message(f"News channel set to {channel.mention}.", ephemeral=True)

        @setup_group.command(name="openai_key", description="Set OpenAI API key for this server")
        async def setup_openai_key(interaction: discord.Interaction, key: str) -> None:
            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
            if not config:
                await interaction.response.send_message(NO_SETUP_MESSAGE, ephemeral=True)
                return
            await asyncio.to_thread(self.db.update_openai_key, interaction.guild.id, key.strip())
            await interaction.response.send_message("OpenAI key saved.", ephemeral=True)

        @setup_group.command(name="trade_channels", description="Set trade committee role and channels")
        async def setup_trade_channels(
            interaction: discord.Interaction,
            committee_role: discord.Role,
            review_channel: discord.TextChannel,
            announcements_channel: discord.TextChannel,
            required_approvals: int = 2,
            required_denials: int = 2,
        ) -> None:
            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            cfg = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
            if not cfg:
                await interaction.response.send_message(NO_SETUP_MESSAGE, ephemeral=True)
                return
            await asyncio.to_thread(
                self.db.update_trade_channels,
                interaction.guild.id,
                committee_role.id,
                review_channel.id,
                announcements_channel.id,
                max(1, required_approvals),
                max(1, required_denials),
            )
            await interaction.response.send_message(
                f"Trade channels updated (committee: {committee_role.mention}, review: {review_channel.mention}, announcements: {announcements_channel.mention}).",
                ephemeral=True,
            )

        @setup_group.command(name="xp", description="Set XP channel/cooldown/min length/blacklist")
        async def setup_xp(
            interaction: discord.Interaction,
            level_up_channel: discord.TextChannel,
            cooldown_seconds: int,
            min_message_len: int,
            blacklist_channels: str = "",
        ) -> None:
            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            cfg = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
            if not cfg:
                await interaction.response.send_message(NO_SETUP_MESSAGE, ephemeral=True)
                return
            await asyncio.to_thread(
                self.db.update_xp_settings,
                interaction.guild.id,
                level_up_channel.id,
                max(0, cooldown_seconds),
                max(1, min_message_len),
                blacklist_channels.strip(),
            )
            await interaction.response.send_message("XP settings updated.", ephemeral=True)

        @setup_group.command(name="reminder_interval", description="Set reminder interval (hours) for unscheduled game channels")
        @app_commands.describe(hours="How many hours between reminders (e.g. 4, 8). Default is 8.")
        async def setup_reminder_interval(interaction: discord.Interaction, hours: int) -> None:
            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            league_id = await self.get_league_id(interaction)
            if league_id is None:
                return
            if hours < 1:
                await interaction.response.send_message("Interval must be at least 1 hour.", ephemeral=True)
                return
            await asyncio.to_thread(self.db.set_reminder_interval, league_id, hours)
            await interaction.response.send_message(
                f"Reminder interval set to **{hours} hour(s)** for this league.",
                ephemeral=True,
            )

        @self.tree.command(name="ping", description="Check if the bot is online")
        async def ping(interaction: discord.Interaction) -> None:
            latency_ms = round(self.latency * 1000)
            await interaction.response.send_message(f"Pong! `{latency_ms}ms`")

        @self.tree.command(name="config", description="Show this server's bot configuration")
        async def config_command(interaction: discord.Interaction) -> None:
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return

            config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
            if not config:
                await interaction.response.send_message(NO_SETUP_MESSAGE, ephemeral=True)
                return

            league_name = await asyncio.to_thread(self.db.get_league_name, config["league_id"])
            log_channel_id = config.get("log_channel_id") or 0
            leaders_channel_id = config.get("leaders_channel_id") or 0
            news_channel_id = config.get("news_channel_id") or 0
            embed = discord.Embed(title="Server Configuration", color=discord.Color.blurple())
            embed.add_field(name="League", value=f"{league_name} (`{config['league_id']}`)", inline=False)
            embed.add_field(name="Log Channel", value=f"<#{log_channel_id}>" if log_channel_id else "Not configured", inline=True)
            embed.add_field(name="Leaders Channel", value=f"<#{leaders_channel_id}>" if leaders_channel_id else "Not configured", inline=True)
            embed.add_field(name="News Channel", value=f"<#{news_channel_id}>" if news_channel_id else "Not configured", inline=True)
            embed.add_field(name="Trade Committee Role", value=f"<@&{safe_int(config.get('trade_committee_role_id'))}>" if safe_int(config.get("trade_committee_role_id")) else "Not configured", inline=False)
            embed.add_field(name="Trade Review", value=f"<#{safe_int(config.get('trade_review_channel_id'))}>" if safe_int(config.get("trade_review_channel_id")) else "Not configured", inline=True)
            embed.add_field(name="Trade Announcements", value=f"<#{safe_int(config.get('trade_announcements_channel_id'))}>" if safe_int(config.get("trade_announcements_channel_id")) else "Not configured", inline=True)
            embed.add_field(name="XP Settings", value=(
                f"Level Up: <#{safe_int(config.get('level_up_channel_id'))}>\n"
                f"Cooldown: {safe_int(config.get('xp_cooldown_seconds'))}s\n"
                f"Min Msg Length: {safe_int(config.get('xp_min_message_len'))}\n"
                f"Blacklist IDs: {safe_text(config.get('xp_blacklist_channel_ids'), 'None') or 'None'}"
            ), inline=False)
            openai_mask = "Not configured"
            key = safe_text(config.get("openai_api_key"))
            if key:
                openai_mask = "••••" + key[-4:]
            elif DEFAULT_OPENAI_API_KEY:
                openai_mask = "Using env fallback"
            embed.add_field(name="OpenAI", value=openai_mask, inline=False)
            embed.add_field(name="Admin Role Names", value=config.get("admin_role_names", DEFAULT_ADMIN_ROLES), inline=False)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        @leaders_group.command(name="passing", description="Top 5 passing leaders")
        async def leaders_passing(interaction: discord.Interaction) -> None:
            await self.respond_leaders(interaction, "Passing Leaders", await self.get_league_id(interaction), "passing")

        @leaders_group.command(name="rushing", description="Top 5 rushing leaders")
        async def leaders_rushing(interaction: discord.Interaction) -> None:
            await self.respond_leaders(interaction, "Rushing Leaders", await self.get_league_id(interaction), "rushing")

        @leaders_group.command(name="receiving", description="Top 5 receiving leaders")
        async def leaders_receiving(interaction: discord.Interaction) -> None:
            await self.respond_leaders(interaction, "Receiving Leaders", await self.get_league_id(interaction), "receiving")

        @leaders_group.command(name="defense", description="Top 5 defensive leaders")
        async def leaders_defense(interaction: discord.Interaction) -> None:
            await self.respond_leaders(interaction, "Defense Leaders", await self.get_league_id(interaction), "defense")

        @leaders_group.command(name="touchdowns", description="Top 5 touchdown leaders")
        async def leaders_touchdowns(interaction: discord.Interaction) -> None:
            await self.respond_leaders(interaction, "Touchdown Leaders", await self.get_league_id(interaction), "touchdowns")

        @post_group.command(name="season_leaders", description="Post all season leader categories to leaders channel")
        async def post_season_leaders(interaction: discord.Interaction) -> None:
            await self.post_season_leaders(interaction)

        @self.tree.command(name="standings", description="Show full standings")
        async def standings(interaction: discord.Interaction) -> None:
            await self.send_standings(interaction, post_to_channel=False)

        @post_group.command(name="standings", description="Post standings to leaders channel")
        async def post_standings(interaction: discord.Interaction) -> None:
            await self.send_standings(interaction, post_to_channel=True)

        @self.tree.command(name="openteams", description="Browse teams with no assigned coach")
        async def openteams(interaction: discord.Interaction) -> None:
            league_id = await self.get_league_id(interaction)
            if league_id is None:
                return
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            await interaction.response.defer()
            teams = await asyncio.to_thread(self.db.fetch_all_teams, league_id)
            open_teams = [t for t in teams if self.find_member_for_team(interaction.guild, t["team_name"]) is None]
            if not open_teams:
                await interaction.followup.send("✅ All teams are currently claimed!")
                return
            total = len(open_teams)
            embeds: list[discord.Embed] = []
            for idx, team in enumerate(open_teams, start=1):
                roster = await asyncio.to_thread(self.db.fetch_team_roster, league_id, int(team["id"]))
                top_players = sorted(roster, key=lambda p: safe_int(p.get("overall_rating")) or 0, reverse=True)[:10]
                player_lines = [
                    f"{p.get('first_name', '')} {p.get('last_name', '')} | {p.get('position', '-')} | OVR {p.get('overall_rating', '-')} | {dev_trait_label(p.get('dev_trait'))}"
                    for p in top_players
                ]
                embed = discord.Embed(
                    title=f"🔓 {team.get('city_name') or ''} {team['team_name']}".strip(),
                    color=team_color_from_name(team["team_name"]),
                )
                embed.add_field(name="Record", value=f"{team['wins']}-{team['losses']}-{team['ties']}", inline=True)
                embed.add_field(name="Overall", value=str(team.get("overall_rating") or "N/A"), inline=True)
                embed.add_field(name="Division", value=team.get("division") or "Unknown", inline=True)
                embed.add_field(name="Top Players", value="\n".join(player_lines) if player_lines else "No players found", inline=False)
                embed.set_footer(text=f"Team {idx} of {total} open teams")
                embeds.append(embed)
            if len(embeds) == 1:
                await interaction.followup.send(embed=embeds[0])
                return
            view = RosterPaginationView(embeds, author_id=interaction.user.id)
            msg = await interaction.followup.send(embed=embeds[0], view=view)
            view.message = msg

        @self.tree.command(name="roster", description="Show roster for a team")
        @app_commands.autocomplete(team=team_name_autocomplete)
        async def roster(interaction: discord.Interaction, team: str) -> None:
            await self.send_roster(interaction, team)

        @team_group.command(name="info", description="Show team overview")
        @app_commands.autocomplete(team=team_name_autocomplete)
        async def team_info(interaction: discord.Interaction, team: str) -> None:
            await self.send_team_info(interaction, team)

        @team_group.command(name="list", description="List all teams and their assigned coaches")
        async def team_list(interaction: discord.Interaction) -> None:
            league_id = await self.get_league_id(interaction)
            if league_id is None:
                return
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            await interaction.response.defer()
            teams = await asyncio.to_thread(self.db.fetch_all_teams, league_id)
            league_name = await asyncio.to_thread(self.db.get_league_name, league_id)
            open_count = 0
            lines: list[str] = []
            for team in teams:
                member = self.find_member_for_team(interaction.guild, team["team_name"])
                record = f"{team['wins']}-{team['losses']}-{team['ties']}"
                if member:
                    lines.append(f"{team['team_name']} ({record}) — {member.mention}")
                else:
                    lines.append(f"{team['team_name']} ({record}) — 🔓 Open")
                    open_count += 1
            page_size = 15
            pages = [lines[i : i + page_size] for i in range(0, len(lines), page_size)]
            total_pages = len(pages)
            embeds_list: list[discord.Embed] = []
            for idx, page_lines in enumerate(pages, start=1):
                embed = discord.Embed(
                    title=f"{league_name} — All Teams",
                    description="\n".join(page_lines),
                    color=discord.Color.blue(),
                )
                embed.set_footer(text=f"Page {idx} of {total_pages} | {len(teams)} teams | {open_count} open")
                embeds_list.append(embed)
            if len(embeds_list) == 1:
                await interaction.followup.send(embed=embeds_list[0])
                return
            view = RosterPaginationView(embeds_list, author_id=interaction.user.id)
            msg = await interaction.followup.send(embed=embeds_list[0], view=view)
            view.message = msg

        @self.tree.command(name="schedule", description="Show current week or specific week schedule")
        @app_commands.describe(week="Optional week number")
        async def schedule(interaction: discord.Interaction, week: int | None = None) -> None:
            await self.send_schedule(interaction, week)

        @self.tree.command(name="scores", description="Show scores for most recently completed week")
        async def scores(interaction: discord.Interaction) -> None:
            await self.send_recent_scores(interaction)

        @self.tree.command(name="headline", description="Generate league headline summary")
        async def headline(interaction: discord.Interaction) -> None:
            await self.send_headline(interaction, post_to_channel=False)

        @post_group.command(name="headline", description="Post league headline summary to leaders channel")
        async def post_headline(interaction: discord.Interaction) -> None:
            await self.send_headline(interaction, post_to_channel=True)

        @player_group.command(name="search", description="Search for a player by name")
        async def player_search(interaction: discord.Interaction, name: str) -> None:
            await self.send_player_search(interaction, name)

        @self.tree.command(name="xprank", description="Show XP rank and progress")
        async def xprank(interaction: discord.Interaction, user: discord.Member | None = None) -> None:
            await self.send_xp_rank(interaction, user)

        @self.tree.command(name="xpleaderboard", description="Show XP leaderboard")
        async def xpleaderboard(interaction: discord.Interaction) -> None:
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            rows = await asyncio.to_thread(self.db.xp_leaderboard, interaction.guild.id)
            rows = [row for row in rows if safe_int(row.get("xp")) > 0][:25]
            if not rows:
                await interaction.response.send_message("No XP data found yet.")
                return
            lines = [
                f"**{idx}.** <@{row['user_id']}> — Level **{row['level']}** | XP **{row['xp']}** | Messages **{row['messages_counted']}**"
                for idx, row in enumerate(rows, start=1)
            ]
            await interaction.response.send_message(embed=discord.Embed(title="📚 XP Leaderboard", description="\n".join(lines), color=0xFEE75C))

        @self.tree.command(name="createbounty", description="Admin: create a bounty")
        async def createbounty(interaction: discord.Interaction, title: str, reward: float, description: str) -> None:
            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            if reward <= 0:
                await interaction.response.send_message("Reward must be greater than 0.", ephemeral=True)
                return
            bounty_id = await asyncio.to_thread(self.db.create_bounty, interaction.guild.id, title.strip(), description.strip(), reward, interaction.user.id)
            embed = discord.Embed(
                title=f"🎯 New Bounty Created — #{bounty_id}",
                description=f"**Title:** {title}\n**Reward:** {reward:.2f}\n**Objective:** {description}",
                color=0xFEE75C,
            )
            await interaction.response.send_message(embed=embed)

        @self.tree.command(name="bounties", description="List active bounties")
        async def bounties(interaction: discord.Interaction) -> None:
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            rows = await asyncio.to_thread(self.db.list_active_bounties, interaction.guild.id)
            if not rows:
                await interaction.response.send_message("There are no active bounties right now.")
                return
            lines = []
            for row in rows[:20]:
                lines.append(
                    f"**#{row['id']} — {row['title']}**\n"
                    f"Reward: **{safe_float(row['reward']):.2f}**\n"
                    f"{row['description']}"
                )
            await interaction.response.send_message(embed=discord.Embed(title="🎯 Active Bounties", description="\n\n".join(lines), color=0xFEE75C))

        @self.tree.command(name="claimbounty", description="Claim a bounty by ID")
        async def claimbounty(interaction: discord.Interaction, bounty_id: int) -> None:
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            bounty = await asyncio.to_thread(self.db.claim_bounty, interaction.guild.id, bounty_id, interaction.user)
            if bounty is None:
                await interaction.response.send_message("That bounty does not exist or has already been claimed.", ephemeral=True)
                return
            embed = discord.Embed(
                title=f"🎯 Bounty Claimed — #{bounty['id']}",
                description=(
                    f"**Title:** {bounty['title']}\n"
                    f"**Reward:** {safe_float(bounty['reward']):.2f}\n"
                    f"**Claimed By:** {interaction.user.mention}"
                ),
                color=0x57F287,
            )
            await interaction.response.send_message(embed=embed)

        @self.tree.command(name="editbounty", description="Admin: edit a bounty")
        async def editbounty(
            interaction: discord.Interaction,
            bounty_id: int,
            title: str | None = None,
            description: str | None = None,
            reward: float | None = None,
        ) -> None:
            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            if reward is not None and reward <= 0:
                await interaction.response.send_message("Reward must be greater than 0.", ephemeral=True)
                return
            updated = await asyncio.to_thread(self.db.update_bounty, interaction.guild.id, bounty_id, title, description, reward)
            if not updated:
                await interaction.response.send_message("That bounty was not found.", ephemeral=True)
                return
            embed = discord.Embed(
                title=f"🎯 Bounty Updated — #{bounty_id}",
                description=f"**Title:** {updated['title']}\n**Reward:** {safe_float(updated['reward']):.2f}\n**Description:** {updated['description']}",
                color=0x5865F2,
            )
            await interaction.response.send_message(embed=embed)

        @self.tree.command(name="trade", description="Submit a trade for committee review")
        async def trade(
            interaction: discord.Interaction,
            coach_one: discord.Member,
            coach_two: discord.Member,
            team_one: str,
            team_two: str,
            team_one_gets: str,
            team_two_gets: str,
            notes: str = "",
        ) -> None:
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            cfg = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
            if not cfg:
                await interaction.followup.send(NO_SETUP_MESSAGE, ephemeral=True)
                return
            review_channel_id = safe_int(cfg.get("trade_review_channel_id"))
            committee_role_id = safe_int(cfg.get("trade_committee_role_id"))
            announcements_channel_id = safe_int(cfg.get("trade_announcements_channel_id"))
            if not review_channel_id:
                await interaction.followup.send("Trade review channel is not configured. Use `/setup trade_channels`.", ephemeral=True)
                return
            if not committee_role_id:
                await interaction.followup.send("Trade committee role is not configured. Use `/setup trade_channels`.", ephemeral=True)
                return
            if not announcements_channel_id:
                await interaction.followup.send("Trade announcements channel is not configured. Use `/setup trade_channels`.", ephemeral=True)
                return
            if coach_one.id == coach_two.id:
                await interaction.followup.send("Coach one and coach two must be different users.", ephemeral=True)
                return
            if team_one.strip().lower() == team_two.strip().lower():
                await interaction.followup.send("Team one and team two must be different.", ephemeral=True)
                return

            trade_row = await asyncio.to_thread(
                self.db.create_trade,
                interaction.guild.id,
                interaction.user,
                coach_one,
                coach_two,
                team_one.strip(),
                team_two.strip(),
                team_one_gets.strip(),
                team_two_gets.strip(),
                notes.strip(),
                announcements_channel_id,
            )

            review_channel = interaction.guild.get_channel(review_channel_id)
            if not isinstance(review_channel, discord.TextChannel):
                try:
                    fetched = await self.fetch_channel(review_channel_id)
                    review_channel = fetched if isinstance(fetched, discord.TextChannel) else None
                except Exception:
                    review_channel = None
            if not isinstance(review_channel, discord.TextChannel):
                await interaction.followup.send("Could not find the trade review channel.", ephemeral=True)
                return

            message = await review_channel.send(
                content=f"<@&{committee_role_id}>",
                embed=await self.build_trade_embed(trade_row),
                view=TradeReviewView(self),
            )
            await asyncio.to_thread(self.db.set_trade_review_message, safe_int(trade_row.get("id")), int(review_channel.id), int(message.id))
            await interaction.followup.send(
                f"Trade **#{safe_int(trade_row.get('id'))}** submitted in {review_channel.mention}. Final result will post in <#{announcements_channel_id}>.",
                ephemeral=True,
            )

        @self.tree.command(name="tradehistory", description="Show recent submitted trades")
        async def tradehistory(interaction: discord.Interaction, limit: int = 10) -> None:
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            rows = await asyncio.to_thread(self.db.list_trades, interaction.guild.id, limit)
            if not rows:
                await interaction.response.send_message("No trades found.", ephemeral=True)
                return
            lines = []
            for row in rows:
                status = safe_text(row.get("status"), "pending").title()
                lines.append(
                    f"**#{row['id']}** {safe_text(row.get('team_one_name'))} ↔ {safe_text(row.get('team_two_name'))} — {status} "
                    f"(✅ {safe_int(row.get('approve_count'))} / ❌ {safe_int(row.get('deny_count'))})"
                )
            await interaction.response.send_message(embed=discord.Embed(title="Trade History", description="\n".join(lines[:20]), color=0x5865F2))

        @self.tree.command(name="forcetrade", description="Admin: force-approve or force-deny a trade")
        @app_commands.choices(decision=[
            app_commands.Choice(name="approve", value="approve"),
            app_commands.Choice(name="deny", value="deny"),
        ])
        async def forcetrade(
            interaction: discord.Interaction,
            trade_id: int,
            decision: app_commands.Choice[str],
            reason: str = "",
        ) -> None:
            if not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            trade_row = await asyncio.to_thread(self.db.get_trade, trade_id)
            if not trade_row:
                await interaction.followup.send(f"Trade #{trade_id} was not found.", ephemeral=True)
                return
            final_status = "approved" if decision.value == "approve" else "denied"
            reason_text = reason.strip() or f"Force {decision.value}d by admin"
            trade_row = await asyncio.to_thread(self.db.finalize_trade, trade_id, final_status, int(interaction.user.id), reason_text)
            if not trade_row:
                await interaction.followup.send("Failed to finalize trade.", ephemeral=True)
                return
            await self.refresh_trade_message(trade_row)
            await self.post_trade_announcement(trade_row)
            await interaction.followup.send(f"Trade **#{trade_id}** was force-{decision.value}d.", ephemeral=True)

        @self.tree.command(name="post_weekly_news", description="Admin: post weekly league news")
        async def post_weekly_news(
            interaction: discord.Interaction,
            week: int,
            phase: str | None = None,
            gotw_pick: str | None = None,
            channel: discord.TextChannel | None = None,
        ) -> None:
            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            if week < 1:
                await interaction.response.send_message("Week must be 1 or higher.", ephemeral=True)
                return
            league_id = await self.get_league_id(interaction)
            if league_id is None:
                return
            await interaction.response.defer(ephemeral=True)
            games = await asyncio.to_thread(self.db.fetch_schedule_for_week, league_id, week)
            if not games:
                await interaction.followup.send(f"No games found for week {week}.", ephemeral=True)
                return

            cfg = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
            standings = await asyncio.to_thread(self.db.fetch_standings, league_id)
            scored = sorted(games, key=lambda row: abs(safe_int(row.get("home_wins")) - safe_int(row.get("away_wins"))))
            top_games = [{"away_team": row.get("away_team"), "home_team": row.get("home_team")} for row in scored[:3]]
            gotw_entry = top_games[0] if top_games else None
            if gotw_pick and "@" in gotw_pick:
                left, right = [part.strip() for part in gotw_pick.split("@", 1)]
                if left and right:
                    gotw_entry = {"away_team": left, "home_team": right}
            facts = {
                "phase": safe_text(phase, "regular season"),
                "week": week,
                "standings": standings[:5],
                "top_games": top_games,
                "gotw_pick": gotw_entry,
            }
            fallback = template_weekly_news_text(
                facts, db=self.db, guild_id=interaction.guild.id, league_id=league_id
            )
            used_ai = False
            article = fallback
            if resolve_openai_api_key(cfg, interaction.guild.id):
                try:
                    ai_text = await asyncio.to_thread(call_openai_text, build_weekly_news_prompt(facts), 320, cfg, interaction.guild.id)
                    cleaned = re.sub(r"\s+", " ", ai_text).strip()
                    if cleaned:
                        article = cleaned
                        used_ai = True
                except Exception as exc:
                    LOGGER.warning("AI weekly news failed for week %s: %s", week, exc)

            news_channel: discord.TextChannel | None = channel
            if news_channel is None:
                news_channel_id = safe_int((cfg or {}).get("news_channel_id"))
                resolved = interaction.guild.get_channel(news_channel_id) if news_channel_id else None
                if isinstance(resolved, discord.TextChannel):
                    news_channel = resolved
            if news_channel is None:
                if isinstance(interaction.channel, discord.TextChannel):
                    news_channel = interaction.channel
            if news_channel is None:
                await interaction.followup.send("No valid news channel found. Configure with `/setup news_channel` or pass channel.", ephemeral=True)
                return

            embed = discord.Embed(
                title=f"📰 {safe_text(phase, 'Regular Season').title()} Week {week} League News",
                description=article,
                color=0x1ABC9C,
            )
            if isinstance(gotw_entry, dict) and gotw_entry.get("away_team") and gotw_entry.get("home_team"):
                embed.add_field(
                    name="GOTW Pick",
                    value=f"{safe_text(gotw_entry.get('away_team'))} @ {safe_text(gotw_entry.get('home_team'))}",
                    inline=False,
                )
            embed.set_footer(text="AI-assisted report" if used_ai else "Template report")
            await news_channel.send(embed=embed)
            await interaction.followup.send(f"Posted week {week} news in {news_channel.mention} ({'AI' if used_ai else 'template'} mode).", ephemeral=True)

        @self.tree.command(name="create_weekly_channels", description="Admin: create one matchup channel per game")
        @app_commands.choices(phase=[
            app_commands.Choice(name="Preseason", value="preseason"),
            app_commands.Choice(name="Regular Season", value="regular"),
            app_commands.Choice(name="Postseason", value="postseason"),
        ])
        async def create_weekly_channels(
            interaction: discord.Interaction,
            week: int,
            phase: app_commands.Choice[str] | None = None,
            category_name: str | None = None,
        ) -> None:
            phase_value = phase.value if phase else "regular"
            LOGGER.info(
                "create_weekly_channels: interaction_id=%s guild=%s week=%s phase=%s",
                interaction.id,
                interaction.guild_id,
                week,
                phase_value,
            )

            # Idempotency guard: ignore duplicate deliveries of the same interaction
            _now = time.monotonic()
            _TTL = 300.0
            self._active_create_channel_ids = {
                k: v for k, v in self._active_create_channel_ids.items() if _now - v < _TTL
            }
            if interaction.id in self._active_create_channel_ids:
                LOGGER.warning(
                    "create_weekly_channels: duplicate interaction %s suppressed (guild=%s)",
                    interaction.id,
                    interaction.guild_id,
                )
                return
            self._active_create_channel_ids[interaction.id] = _now

            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            if week < 1:
                await interaction.response.send_message("Week must be 1 or higher.", ephemeral=True)
                return
            league_id = await self.get_league_id(interaction)
            if league_id is None:
                return
            await interaction.response.defer(ephemeral=True)
            games = await asyncio.to_thread(self.db.fetch_schedule_for_week, league_id, week)
            if not games:
                await interaction.followup.send(f"No games found for week {week}.", ephemeral=True)
                return

            guild = interaction.guild
            phase_info = format_phase_labels(phase_value, week)
            category_title = category_name or phase_info["category"]
            existing_category = discord.utils.get(guild.categories, name=category_title)
            if existing_category is None:
                existing_category = await guild.create_category(category_title)

            cfg = await asyncio.to_thread(self.db.get_guild_config, guild.id)
            scored_games = sorted(games, key=lambda row: abs(safe_int(row.get("home_wins")) - safe_int(row.get("away_wins"))))
            gotw_ids = {safe_int(row.get("game_id")) for row in scored_games[:2]}

            created_channels: list[str] = []
            skipped_channels: list[str] = []
            for game in games:
                is_gotw = safe_int(game.get("game_id")) in gotw_ids
                away_team_name = safe_text(game.get("away_team"), "away")
                home_team_name = safe_text(game.get("home_team"), "home")
                base_name = f"{phase_info['prefix']}-{slugify_channel_name(away_team_name)}-vs-{slugify_channel_name(home_team_name)}"
                channel_name = f"gotw-{base_name}" if is_gotw else base_name
                channel_name = channel_name[:100]

                # Safety guard: only create channels that include the selected phase prefix
                if not (channel_name.startswith(f"{phase_info['prefix']}-") or channel_name.startswith(f"gotw-{phase_info['prefix']}-")):
                    LOGGER.warning(
                        "create_weekly_channels: skipping channel '%s' — does not match phase prefix '%s'",
                        channel_name,
                        phase_info['prefix'],
                    )
                    continue

                existing_channel = discord.utils.get(guild.text_channels, name=channel_name)
                if existing_channel is not None:
                    skipped_channels.append(f"{channel_name} (already exists)")
                    continue

                channel = await guild.create_text_channel(
                    name=channel_name,
                    category=existing_category,
                    topic=f"Game ID {safe_int(game.get('game_id'))} | {phase_info['display']}",
                )

                away_member = self.find_member_for_team(guild, away_team_name)
                home_member = self.find_member_for_team(guild, home_team_name)
                mention_one = away_member.mention if away_member else f"**{away_team_name}** (no Discord match found)"
                mention_two = home_member.mention if home_member else f"**{home_team_name}** (no Discord match found)"

                message_lines = []
                if is_gotw:
                    message_lines.extend(["🔥 **GAME OF THE WEEK** 🔥", ""])
                message_lines.extend(
                    [
                        f"🏈 **{phase_info['display']} Matchup**",
                        f"**Away:** {away_team_name}",
                        f"**Home:** {home_team_name}",
                        "",
                        f"{mention_one} vs {mention_two}",
                        "",
                        "Use this channel to schedule your game.",
                    ]
                )
                await channel.send("\n".join(message_lines))

                # Post the game scheduling control message with buttons
                home_user_id = int(home_member.id) if home_member else None
                away_user_id = int(away_member.id) if away_member else None
                if home_user_id is None or away_user_id is None:
                    LOGGER.warning(
                        "create_weekly_channels: could not resolve Discord user(s) for channel %s "
                        "(home=%s away=%s) — reminders will be skipped",
                        channel.id,
                        home_team_name,
                        away_team_name,
                    )
                await asyncio.to_thread(
                    self.db.upsert_game_channel_state,
                    int(channel.id),
                    int(guild.id),
                    league_id,
                    home_team_name,
                    away_team_name,
                    home_user_id,
                    away_user_id,
                )
                await channel.send(
                    "**Game Scheduling Status:**\nUse the buttons below to update this game's status. "
                    "The two team owners will be reminded every few hours until the game is marked scheduled or completed.",
                    view=GameSchedulingView(self),
                )

                if safe_text(game.get("away_division")) and safe_text(game.get("away_division")) == safe_text(game.get("home_division")):
                    await channel.send("🔥 **This matchup has been tagged as a Rivalry Game.**")

                if AUTO_POST_MATCHUP_PREVIEWS:
                    preview_text, used_ai, facts = await generate_matchup_preview_text(
                        self.db,
                        cfg or {},
                        league_id,
                        game,
                        is_gotw,
                        guild.id,
                    )
                    embed = discord.Embed(
                        title=f"📰 {facts['headline']}",
                        description=preview_text,
                        color=0x3498DB if not is_gotw else 0xF39C12,
                    )
                    if facts["players_to_watch"]:
                        ptw_lines: list[str] = []
                        for ptw in facts["players_to_watch"]:
                            if isinstance(ptw, dict):
                                header = f"**{ptw['player_name']}** ({ptw['team_name']})"
                                entry_line = f"• {header}"
                                if ptw.get("stats_line"):
                                    entry_line += f"\n  📊 {ptw['stats_line']}"
                                if ptw.get("why_line"):
                                    entry_line += f"\n  💡 {ptw['why_line']}"
                                ptw_lines.append(entry_line)
                            else:
                                ptw_lines.append(f"• {ptw}")
                        embed.add_field(name="Players to Watch", value="\n".join(ptw_lines), inline=False)
                    embed.add_field(name="Why It Matters", value=facts["stakes_line"], inline=False)
                    embed.set_footer(text="AI-assisted preview" if used_ai else "Template preview")
                    await channel.send(embed=embed)

                created_channels.append(channel_name)

            summary_lines = [f"Created {len(created_channels)} channel(s) in **{category_title}** for **{phase_info['display']}**."]
            if created_channels:
                summary_lines.append("Created:\n" + "\n".join(f"• {name}" for name in created_channels[:20]))
            if skipped_channels:
                summary_lines.append("Skipped:\n" + "\n".join(f"• {name}" for name in skipped_channels[:20]))
            await interaction.followup.send("\n\n".join(summary_lines), ephemeral=True)

            if created_channels:
                news_status = await self._auto_post_weekly_news(guild, league_id, week, phase_value, cfg or {})
                headlines_status = await self._auto_post_weekly_headlines(guild, league_id, cfg or {})
                await interaction.followup.send(
                    f"**Auto-post results:**\n• {news_status}\n• {headlines_status}",
                    ephemeral=True,
                )

        @self.tree.command(name="delete_weekly_channels", description="Admin: delete matchup channels for a specific week")
        @app_commands.choices(phase=[
            app_commands.Choice(name="Preseason", value="preseason"),
            app_commands.Choice(name="Regular Season", value="regular"),
            app_commands.Choice(name="Postseason", value="postseason"),
        ])
        async def delete_weekly_channels(
            interaction: discord.Interaction,
            week: int,
            phase: app_commands.Choice[str] | None = None,
            confirm: bool = False,
        ) -> None:
            if not interaction.guild or not await self.user_is_admin(interaction):
                await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
                return
            if week < 1:
                await interaction.response.send_message("Week must be 1 or higher.", ephemeral=True)
                return

            phase_value = phase.value if phase else "regular"
            phase_info = format_phase_labels(phase_value, week)
            target_category_name = phase_info["category"]

            if not confirm:
                await interaction.response.send_message(
                    f"About to delete channels for **{phase_info['display']}** in category **{target_category_name}**.\n"
                    f"Re-run with `confirm: True` to proceed.",
                    ephemeral=True,
                )
                return

            await interaction.response.defer(ephemeral=True)

            guild = interaction.guild
            category = discord.utils.get(guild.categories, name=target_category_name)
            if category is None:
                await interaction.followup.send(f"Category **{target_category_name}** not found in this server.", ephemeral=True)
                return

            matched = match_weekly_channel_names(list(category.text_channels), week, phase_value)

            if not matched:
                # Still delete the category if it's empty (e.g., channels were manually deleted already)
                if not category.channels:
                    await category.delete(reason=f"delete_weekly_channels week={week} phase={phase_value} (category empty)")
                    await interaction.followup.send(
                        f"No matching channels found, but category **{target_category_name}** was empty and has been deleted.",
                        ephemeral=True,
                    )
                    return

                await interaction.followup.send(
                    f"No matching channels found in **{target_category_name}** for Week {week} ({phase_info['display']}).",
                    ephemeral=True,
                )
                return

            deleted: list[str] = []
            failed: list[str] = []

            for ch in matched:
                try:
                    await ch.delete(reason=f"delete_weekly_channels week={week} phase={phase_value}")
                    deleted.append(ch.name)
                    await asyncio.sleep(0.5)
                except discord.HTTPException as exc:
                    LOGGER.warning("Failed to delete channel %s: %s", ch.name, exc)
                    failed.append(ch.name)

            category_deleted = False
            try:
                if not category.channels:
                    await category.delete(reason=f"delete_weekly_channels week={week} phase={phase_value} (category cleanup)")
                    category_deleted = True
            except discord.HTTPException as exc:
                LOGGER.warning("Failed to delete category %s: %s", target_category_name, exc)

            lines = [f"Deleted **{len(deleted)}** channel(s) in **{target_category_name}** for **{phase_info['display']}**."]
            if deleted:
                lines.append("\nDeleted:\n" + "\n".join(f"• {n}" for n in deleted[:50]))
            if failed:
                lines.append("\nFailed:\n" + "\n".join(f"• {n}" for n in failed[:25]))
            lines.append(f"\nCategory deleted: **{'yes' if category_deleted else 'no'}**")

            await interaction.followup.send("\n".join(lines), ephemeral=True)

        self.tree.add_command(setup_group)
        self.tree.add_command(leaders_group)
        self.tree.add_command(post_group)
        self.tree.add_command(team_group)
        self.tree.add_command(player_group)
        self.add_view(TradeReviewView(self))
        self.add_view(GameSchedulingView(self))
        self.add_view(GameChannelDeleteView(self))
        self.reminder_loop.start()
        self.cleanup_loop.start()

        if self.guild_ids:
            for guild_id in self.guild_ids:
                guild_obj = discord.Object(id=guild_id)
                self.tree.copy_global_to(guild=guild_obj)
                await self.tree.sync(guild=guild_obj)
            self.tree.clear_commands(guild=None)
            await self.tree.sync()
        else:
            await self.tree.sync()

    async def on_guild_join(self, guild: discord.Guild) -> None:
        try:
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            LOGGER.info("Synced commands to new guild: %s (%s)", guild.name, guild.id)
        except Exception as exc:
            LOGGER.warning("Failed to sync commands to guild %s: %s", guild.id, exc)

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or not message.guild:
            return
        config = await asyncio.to_thread(self.db.get_guild_config, message.guild.id)
        if not config:
            return

        blacklist_ids = _parse_channel_ids(safe_text(config.get("xp_blacklist_channel_ids")))
        if message.channel.id in blacklist_ids:
            return

        content = safe_text(message.content)
        min_len = max(1, safe_int(config.get("xp_min_message_len"), 8))
        if len(content) < min_len:
            return

        row = await asyncio.to_thread(self.db.get_xp_user, message.guild.id, message.author)
        now_ts = message.created_at.timestamp() if message.created_at else time.time()
        last_xp_at = safe_float((row or {}).get("last_xp_at"), 0.0)
        cooldown = max(0, safe_int(config.get("xp_cooldown_seconds"), 45))
        if now_ts - last_xp_at < cooldown:
            return

        gained = random.randint(15, 25)
        old_xp = safe_int((row or {}).get("xp"), 0)
        new_xp = old_xp + gained
        old_level = safe_int((row or {}).get("level"), 1)
        new_level = level_from_xp(new_xp)
        messages_counted = safe_int((row or {}).get("messages_counted"), 0) + 1

        await asyncio.to_thread(self.db.update_xp_progress, message.guild.id, message.author, new_xp, new_level, messages_counted, now_ts)

        if new_level > old_level:
            level_up_channel_id = safe_int(config.get("level_up_channel_id"))
            if level_up_channel_id:
                channel = message.guild.get_channel(level_up_channel_id)
                if isinstance(channel, discord.TextChannel):
                    embed = discord.Embed(
                        title="⬆️ Level Up!",
                        description=f"🎉 {message.author.mention} leveled up to **Level {new_level}**!",
                        color=0x57F287,
                    )
                    await channel.send(embed=embed)

    def find_member_for_team(self, guild: discord.Guild, team_name: str) -> discord.Member | None:
        needle = team_name.lower().strip()
        if not needle:
            return None
        for member in guild.members:
            nick = (member.nick or "").lower()
            display = member.display_name.lower()
            if needle in nick or needle in display:
                return member
        return None

    async def user_is_admin(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return False
        member = interaction.user
        if not isinstance(member, discord.Member):
            return False

        config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
        role_names = DEFAULT_ADMIN_ROLES
        if config and config.get("admin_role_names"):
            role_names = config["admin_role_names"]

        allowed = {name.strip().lower() for name in role_names.split(",") if name.strip()}
        member_roles = {role.name.lower() for role in member.roles}
        return bool(allowed.intersection(member_roles)) or member.guild_permissions.administrator

    async def get_league_id(self, interaction: discord.Interaction) -> int | None:
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return None
        config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
        if not config:
            await interaction.response.send_message(NO_SETUP_MESSAGE, ephemeral=True)
            return None
        return int(config["league_id"])

    async def send_xp_rank(self, interaction: discord.Interaction, user: discord.Member | None = None) -> None:
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        target = user or interaction.user
        row = await asyncio.to_thread(self.db.get_xp_user, interaction.guild.id, target)
        xp = safe_int((row or {}).get("xp"))
        level, progress, needed = xp_progress_text(xp)
        desc = (
            f"**Level:** {level}\n"
            f"**XP:** {xp}\n"
            f"**Progress to next level:** {progress}/{needed}\n"
            f"**Messages Counted:** {safe_int((row or {}).get('messages_counted'))}"
        )
        await interaction.response.send_message(embed=discord.Embed(title=f"📈 {target.display_name}'s Rank", description=desc, color=0x5865F2))

    async def respond_leaders(self, interaction: discord.Interaction, title: str, league_id: int | None, category: str) -> None:
        if league_id is None:
            return

        fetch_map = {
            "passing": self.db.fetch_passing_leaders,
            "rushing": self.db.fetch_rushing_leaders,
            "receiving": self.db.fetch_receiving_leaders,
            "defense": self.db.fetch_defense_leaders,
            "touchdowns": self.db.fetch_touchdown_leaders,
        }
        rows = await asyncio.to_thread(fetch_map[category], league_id)
        if not rows:
            await interaction.response.send_message(embed=discord.Embed(title=title, description="No data found.", color=discord.Color.orange()))
            return

        embed = discord.Embed(title=title, color=discord.Color.blurple())
        for idx, row in enumerate(rows, start=1):
            name = player_display_name(row)
            team_name = row.get("team_name") or "FA"
            position = row.get("position") or "-"
            if category == "passing":
                value = f"{row['pass_yards']} yds | {row['pass_tds']} TD | {row['interceptions']} INT"
            elif category == "rushing":
                value = f"{row['rush_yards']} yds | {row['rush_tds']} TD"
            elif category == "receiving":
                value = f"{row['rec_yards']} yds | {row['rec_tds']} TD | {row['receptions']} REC"
            elif category == "defense":
                value = f"{row['tackles']} TKL | {row['sacks']} SCK | {row['defensive_ints']} INT | {row['fumbles_forced']} FF"
            else:
                value = f"{row['total_tds']} Total ({row['pass_tds']} pass / {row['rush_tds']} rush / {row['rec_tds']} rec)"
            embed.add_field(name=f"#{idx} {name} ({position})", value=f"{team_name}\n{value}", inline=False)

        await interaction.response.send_message(embed=embed)

    async def post_season_leaders(self, interaction: discord.Interaction) -> None:
        league_id = await self.get_league_id(interaction)
        if league_id is None:
            return
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
        leaders_channel_id = int(config.get("leaders_channel_id") or 0) if config else 0
        if not leaders_channel_id:
            await interaction.followup.send("Leaders channel is not configured. Use `/setup channels` first.", ephemeral=True)
            return

        channel = interaction.guild.get_channel(leaders_channel_id)
        if not isinstance(channel, discord.TextChannel):
            await interaction.followup.send("Configured leaders channel was not found.", ephemeral=True)
            return

        categories = [
            ("passing", "🏈 Passing Leaders", 0x3498DB, await asyncio.to_thread(self.db.fetch_passing_leaders, league_id)),
            ("rushing", "🏃 Rushing Leaders", 0x2ECC71, await asyncio.to_thread(self.db.fetch_rushing_leaders, league_id)),
            ("receiving", "🎯 Receiving Leaders", 0xE67E22, await asyncio.to_thread(self.db.fetch_receiving_leaders, league_id)),
            ("defense", "🛡️ Defense Leaders", 0xE74C3C, await asyncio.to_thread(self.db.fetch_defense_leaders, league_id)),
            ("touchdowns", "🏆 Touchdown Leaders", 0xF1C40F, await asyncio.to_thread(self.db.fetch_touchdown_leaders, league_id)),
        ]

        league_name = await asyncio.to_thread(self.db.get_league_name, league_id)
        header_embed = discord.Embed(
            title="Season Leaders",
            description=f"**{league_name}**\nTop 5 across all categories",
            color=discord.Color.gold(),
        )
        await channel.send(embed=header_embed)

        for category, title, color, rows in categories:
            embed = discord.Embed(title=title, color=color)
            if not rows:
                embed.description = "No data found."
                await channel.send(embed=embed)
                continue

            lines: list[str] = []
            for idx, row in enumerate(rows, start=1):
                name = player_display_name(row)
                team_name = row.get("team_name") or "FA"
                stat_text = season_leader_stat_text(row, category)
                lines.append(f"{leader_rank_text(idx)} **{name}** ({team_name}) — {stat_text}")
            embed.description = "\n".join(lines)
            await channel.send(embed=embed)
        await interaction.followup.send(f"Posted season leaders to {channel.mention}.", ephemeral=True)

    async def send_standings(self, interaction: discord.Interaction, post_to_channel: bool) -> None:
        league_id = await self.get_league_id(interaction)
        if league_id is None:
            return

        standings = await asyncio.to_thread(self.db.fetch_standings, league_id)
        if not standings:
            await interaction.response.send_message(embed=discord.Embed(title="Standings", description="No standings found.", color=discord.Color.orange()))
            return

        league_name = await asyncio.to_thread(self.db.get_league_name, league_id)
        embed = discord.Embed(title=f"{league_name} Standings", color=discord.Color.blue())

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in standings:
            division = row.get("division_name") or "Unknown"
            grouped.setdefault(division, []).append(row)

        for division, rows in grouped.items():
            lines = [
                f"{idx}. {r['team_name']} ({r['wins']}-{r['losses']}-{r['ties']})"
                for idx, r in enumerate(rows, start=1)
            ]
            embed.add_field(name=division, value="\n".join(lines), inline=False)

        if not post_to_channel:
            await interaction.response.send_message(embed=embed)
            return

        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return

        config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
        leaders_channel_id = int(config.get("leaders_channel_id") or 0) if config else 0
        channel = interaction.guild.get_channel(leaders_channel_id) if leaders_channel_id else None
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Leaders channel is not configured. Use `/setup channels` first.", ephemeral=True)
            return

        await channel.send(embed=embed)
        await interaction.response.send_message(f"Posted standings to {channel.mention}.", ephemeral=True)

    async def send_roster(self, interaction: discord.Interaction, team_name: str) -> None:
        league_id = await self.get_league_id(interaction)
        if league_id is None:
            return

        team = await asyncio.to_thread(self.db.fetch_team_info, league_id, team_name)
        if not team:
            await interaction.response.send_message("Team not found.", ephemeral=True)
            return

        roster = await asyncio.to_thread(self.db.fetch_team_roster, league_id, int(team["id"]))
        embed = discord.Embed(
            title=f"{team['team_name']} Roster",
            description=f"{team.get('city_name') or ''} {team['team_name']}".strip(),
            color=team_color_from_name(team["team_name"]),
        )

        if not roster:
            embed.description = f"{embed.description}\n\nNo roster data found."
            await interaction.response.send_message(embed=embed)
            return

        roster = sorted(
            roster,
            key=lambda player: (
                -safe_int(player.get("overall_rating")),
                safe_text(player.get("last_name")).lower(),
                safe_text(player.get("first_name")).lower(),
            ),
        )
        lines = [
            f"{safe_text(p.get('first_name'))} {safe_text(p.get('last_name'))} | "
            f"{p.get('position', '-')} | OVR {p.get('overall_rating', '-')} | "
            f"Age {p.get('age', '-')} | {dev_trait_label(p.get('dev_trait'))}"
            for p in roster
        ]
        pages = [lines[index : index + ROSTER_PAGE_SIZE] for index in range(0, len(lines), ROSTER_PAGE_SIZE)]
        embeds: list[discord.Embed] = []
        total_pages = len(pages)
        base_description = f"{team.get('city_name') or ''} {team['team_name']}".strip()
        for idx, page_lines in enumerate(pages, start=1):
            page_embed = discord.Embed(
                title=f"{team['team_name']} Roster",
                description=f"{base_description}\n\n" + "\n".join(page_lines),
                color=team_color_from_name(team["team_name"]),
            )
            page_embed.set_footer(text=f"Page {idx} of {total_pages}")
            embeds.append(page_embed)

        if len(embeds) == 1:
            await interaction.response.send_message(embed=embeds[0])
            return

        view = RosterPaginationView(embeds, author_id=interaction.user.id)
        await interaction.response.send_message(embed=embeds[0], view=view)
        view.message = await interaction.original_response()

    async def send_team_info(self, interaction: discord.Interaction, team_name: str) -> None:
        league_id = await self.get_league_id(interaction)
        if league_id is None:
            return

        team = await asyncio.to_thread(self.db.fetch_team_info, league_id, team_name)
        if not team:
            await interaction.response.send_message("Team not found.", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"{team.get('city_name') or ''} {team['team_name']}".strip(),
            color=team_color_from_name(team["team_name"]),
        )
        embed.add_field(name="Record", value=f"{team['wins']}-{team['losses']}-{team['ties']}", inline=True)
        embed.add_field(name="Overall", value=str(team.get("overall_rating") or "N/A"), inline=True)
        embed.add_field(name="Division", value=team.get("division") or "Unknown", inline=True)
        await interaction.response.send_message(embed=embed)

    async def send_schedule(self, interaction: discord.Interaction, week: int | None) -> None:
        league_id = await self.get_league_id(interaction)
        if league_id is None:
            return

        target_week = week
        if target_week is None:
            target_week = await asyncio.to_thread(self.db.latest_incomplete_week, league_id)
        if target_week is None:
            await interaction.response.send_message("No upcoming games found.", ephemeral=True)
            return

        games = await asyncio.to_thread(self.db.fetch_schedule_for_week, league_id, target_week)
        if not games:
            await interaction.response.send_message(f"No games found for week {target_week}.", ephemeral=True)
            return

        season_number = games[0].get("season_number")
        league_name = await asyncio.to_thread(self.db.get_league_name, league_id)
        embed = discord.Embed(
            title=f"{league_name} - Week {target_week} Schedule",
            description=f"Season {season_number}",
            color=discord.Color.dark_blue(),
        )

        for game in games:
            if game["is_complete"]:
                value = f"{game['away_team']} {game['away_score']} @ {game['home_team']} {game['home_score']}"
            else:
                value = f"{game['away_team']} @ {game['home_team']}"
            embed.add_field(name="Matchup", value=value, inline=False)

        await interaction.response.send_message(embed=embed)

    async def send_recent_scores(self, interaction: discord.Interaction) -> None:
        league_id = await self.get_league_id(interaction)
        if league_id is None:
            return

        week = await asyncio.to_thread(self.db.latest_completed_week, league_id)
        if week is None:
            await interaction.response.send_message("No completed week scores found.", ephemeral=True)
            return

        games = await asyncio.to_thread(self.db.fetch_schedule_for_week, league_id, week)
        if not games:
            await interaction.response.send_message("No scores found.", ephemeral=True)
            return

        season_number = games[0].get("season_number")
        league_name = await asyncio.to_thread(self.db.get_league_name, league_id)
        embed = discord.Embed(
            title=f"{league_name} - Week {week} Scores",
            description=f"Season {season_number}",
            color=discord.Color.purple(),
        )
        for game in games:
            embed.add_field(
                name="Final",
                value=f"{game['away_team']} {game['away_score']} @ {game['home_team']} {game['home_score']}",
                inline=False,
            )
        await interaction.response.send_message(embed=embed)

    async def _build_headline_embed(self, guild: discord.Guild, league_id: int, cfg: dict) -> discord.Embed:
        """Build a headlines embed without requiring a live interaction.

        Raises ValueError if there is no standings data to build a meaningful headline.
        """
        standings = await asyncio.to_thread(self.db.fetch_standings, league_id)
        if not standings:
            raise ValueError("No standings data available.")
        passing = await asyncio.to_thread(self.db.fetch_passing_leaders, league_id)
        rushing = await asyncio.to_thread(self.db.fetch_rushing_leaders, league_id)
        receiving = await asyncio.to_thread(self.db.fetch_receiving_leaders, league_id)
        touchdowns = await asyncio.to_thread(self.db.fetch_touchdown_leaders, league_id)
        week = await asyncio.to_thread(self.db.latest_completed_week, league_id)
        recent_games = await asyncio.to_thread(self.db.fetch_schedule_for_week, league_id, week) if week is not None else []

        top_team = standings[0] if standings else {}
        bottom_team = standings[-1] if standings else {}
        passer = passing[0] if passing else {}
        rusher = rushing[0] if rushing else {}
        receiver = receiving[0] if receiving else {}
        td_leader = touchdowns[0] if touchdowns else {}
        scores_summary = (
            f"{len(recent_games)} game(s) finalized and playoff pressure is rising."
            if week is not None and recent_games
            else "results are still coming in across the league."
        )

        facts = {
            "top_team": safe_text(top_team.get("team_name"), "The top seed"),
            "top_record": wins_losses_ties_text(top_team),
            "bottom_team": safe_text(bottom_team.get("team_name"), "The last-place team"),
            "bottom_record": wins_losses_ties_text(bottom_team),
            "closest_race": find_closest_division_race(standings),
            "passer": player_display_name(passer) if passer else "No passer data",
            "pass_yards": safe_int(passer.get("pass_yards")) if passer else 0,
            "pass_tds": safe_int(passer.get("pass_tds")) if passer else 0,
            "rusher": player_display_name(rusher) if rusher else "No rusher data",
            "rush_yards": safe_int(rusher.get("rush_yards")) if rusher else 0,
            "receiver": player_display_name(receiver) if receiver else "No receiver data",
            "rec_yards": safe_int(receiver.get("rec_yards")) if receiver else 0,
            "rec_tds": safe_int(receiver.get("rec_tds")) if receiver else 0,
            "td_leader": player_display_name(td_leader) if td_leader else "No touchdown data",
            "total_tds": safe_int(td_leader.get("total_tds")) if td_leader else 0,
            "week": week if week is not None else "N/A",
            "scores_summary": scores_summary,
        }
        headline_text = template_headline_text(
            facts,
            db=self.db,
            guild_id=guild.id,
            league_id=league_id,
        )
        used_ai = False
        if resolve_openai_api_key(cfg, guild.id):
            try:
                ai_text = await asyncio.to_thread(
                    call_openai_text,
                    build_headline_prompt(facts),
                    HEADLINE_MAX_OUTPUT_TOKENS,
                    cfg,
                    guild.id,
                )
                cleaned = "\n".join(line for line in (raw.strip() for raw in ai_text.splitlines()) if line)
                if cleaned:
                    headline_text = cleaned
                    used_ai = True
                else:
                    LOGGER.warning("AI headline generation returned empty text for league %s", league_id)
            except Exception as exc:
                LOGGER.warning("AI headline generation failed for league %s: %s", league_id, exc)

        league_name = await asyncio.to_thread(self.db.get_league_name, league_id)
        embed = discord.Embed(
            title=f"🗞️ {league_name} Headlines",
            description=headline_text,
            color=discord.Color.gold(),
        )
        embed.set_footer(text="AI-assisted headlines" if used_ai else "Template headlines")
        return embed

    async def _auto_post_weekly_news(
        self,
        guild: discord.Guild,
        league_id: int,
        week: int,
        phase_value: str,
        cfg: dict,
    ) -> str:
        """Generate weekly news embed and post it to the configured news channel.

        Returns a status string describing the result.
        """
        games = await asyncio.to_thread(self.db.fetch_schedule_for_week, league_id, week)
        if not games:
            return f"Weekly news skipped: no games found for week {week}."

        standings = await asyncio.to_thread(self.db.fetch_standings, league_id)
        scored = sorted(games, key=lambda row: abs(safe_int(row.get("home_wins")) - safe_int(row.get("away_wins"))))
        top_games = [{"away_team": row.get("away_team"), "home_team": row.get("home_team")} for row in scored[:3]]
        gotw_entry = top_games[0] if top_games else None
        facts = {
            "phase": phase_value,
            "week": week,
            "standings": standings[:5],
            "top_games": top_games,
            "gotw_pick": gotw_entry,
        }
        fallback = template_weekly_news_text(facts, db=self.db, guild_id=guild.id, league_id=league_id)
        used_ai = False
        article = fallback
        if resolve_openai_api_key(cfg, guild.id):
            try:
                ai_text = await asyncio.to_thread(call_openai_text, build_weekly_news_prompt(facts), 320, cfg, guild.id)
                cleaned = re.sub(r"\s+", " ", ai_text).strip()
                if cleaned:
                    article = cleaned
                    used_ai = True
            except Exception as exc:
                LOGGER.warning("AI weekly news failed for week %s: %s", week, exc)

        news_channel_id = safe_int(cfg.get("news_channel_id"))
        resolved = guild.get_channel(news_channel_id) if news_channel_id else None
        news_channel = resolved if isinstance(resolved, discord.TextChannel) else None
        if news_channel is None:
            return "Weekly news skipped: news channel not configured (use `/setup news_channel`)."

        embed = discord.Embed(
            title=f"📰 {phase_value.title()} Week {week} League News",
            description=article,
            color=0x1ABC9C,
        )
        if isinstance(gotw_entry, dict) and gotw_entry.get("away_team") and gotw_entry.get("home_team"):
            embed.add_field(
                name="GOTW Pick",
                value=f"{safe_text(gotw_entry.get('away_team'))} @ {safe_text(gotw_entry.get('home_team'))}",
                inline=False,
            )
        embed.set_footer(text="AI-assisted report" if used_ai else "Template report")
        try:
            await news_channel.send(embed=embed)
            return f"Weekly news posted to {news_channel.mention} ({'AI' if used_ai else 'template'} mode)."
        except discord.HTTPException as exc:
            LOGGER.warning("Failed to auto-post weekly news: %s", exc)
            return f"Weekly news failed to post: {exc}"

    async def _auto_post_weekly_headlines(
        self,
        guild: discord.Guild,
        league_id: int,
        cfg: dict,
    ) -> str:
        """Generate headlines embed and post it to the configured leaders channel.

        Returns a status string describing the result.
        """
        leaders_channel_id = safe_int(cfg.get("leaders_channel_id"))
        channel = guild.get_channel(leaders_channel_id) if leaders_channel_id else None
        if not isinstance(channel, discord.TextChannel):
            return "Headlines skipped: leaders channel not configured (use `/setup channels`)."

        try:
            embed = await self._build_headline_embed(guild, league_id, cfg)
            await channel.send(embed=embed)
            return f"Headlines posted to {channel.mention}."
        except ValueError as exc:
            return f"Headlines skipped: {exc}"
        except discord.HTTPException as exc:
            LOGGER.warning("Failed to auto-post weekly headlines: %s", exc)
            return f"Headlines failed to post: {exc}"

    async def send_headline(self, interaction: discord.Interaction, post_to_channel: bool) -> None:
        league_id = await self.get_league_id(interaction)
        if league_id is None:
            return
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=post_to_channel)

        if not interaction.guild:
            await interaction.followup.send("This command can only be used in a server.", ephemeral=True)
            return

        try:
            cfg = (await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)) or {}
            try:
                embed = await self._build_headline_embed(interaction.guild, league_id, cfg)
            except ValueError:
                await interaction.followup.send("No standings found.", ephemeral=True)
                return

            if not post_to_channel:
                await interaction.followup.send(embed=embed)
                return

            leaders_channel_id = safe_int(cfg.get("leaders_channel_id"))
            channel = interaction.guild.get_channel(leaders_channel_id) if leaders_channel_id else None
            if not isinstance(channel, discord.TextChannel):
                await interaction.followup.send("Leaders channel is not configured. Use `/setup channels` first.", ephemeral=True)
                return

            await channel.send(embed=embed)
            await interaction.followup.send(f"Posted headlines to {channel.mention}.", ephemeral=True)
        except Exception:
            LOGGER.exception("Unhandled error in send_headline for guild %s", interaction.guild_id)
            try:
                await interaction.followup.send("An error occurred while generating headlines. Please try again.", ephemeral=True)
            except Exception:
                LOGGER.warning("Failed to send error followup in send_headline for guild %s", interaction.guild_id)

    async def send_player_search(self, interaction: discord.Interaction, name_query: str) -> None:
        league_id = await self.get_league_id(interaction)
        if league_id is None:
            return

        try:
            players = await asyncio.to_thread(self.db.player_search, league_id, name_query)
        except Exception as exc:
            LOGGER.exception("player_search DB error for guild %s: %s", interaction.guild_id, exc)
            await interaction.response.send_message(
                "An error occurred while searching for players. Please try again later.",
                ephemeral=True,
            )
            return

        if not players:
            await interaction.response.send_message("No players found.", ephemeral=True)
            return

        embed = discord.Embed(title=f"Player Search: {name_query}", color=discord.Color.teal())
        for row in players:
            name = player_display_name(row)
            team = row.get("team_name") or "FA"
            lines = [
                f"Team: {team}",
                f"Position: {row.get('position') or '-'} | OVR: {row.get('overall_rating') or '-'}",
                f"Pass: {row['pass_yards']} yds, {row['pass_tds']} TD, {row['interceptions']} INT",
                f"Rush: {row['rush_yards']} yds, {row['rush_tds']} TD",
                f"Rec: {row['rec_yards']} yds, {row['rec_tds']} TD, {row['receptions']} REC",
                f"Defense: {row['tackles']} TKL, {row['sacks']} SCK, {row['defensive_ints']} INT, {row['fumbles_forced']} FF",
            ]
            embed.add_field(name=name, value="\n".join(lines), inline=False)

        await interaction.response.send_message(embed=embed)

    async def build_trade_embed(self, trade_row: dict[str, Any]) -> discord.Embed:
        trade_id = safe_int(trade_row.get("id"))
        guild_id = safe_int(trade_row.get("guild_id"))
        cfg = await asyncio.to_thread(self.db.get_guild_config, guild_id)
        required_approvals = max(1, safe_int((cfg or {}).get("trade_required_approvals"), 2))
        required_denials = max(1, safe_int((cfg or {}).get("trade_required_denials"), 2))

        embed = discord.Embed(
            title=f"Trade Review #{trade_id}",
            color=0x2ECC71 if safe_text(trade_row.get("status"), "pending") == "approved" else (0xE74C3C if safe_text(trade_row.get("status")) == "denied" else 0xF1C40F),
        )
        embed.add_field(name=safe_text(trade_row.get("team_one_name"), "Team One"), value=safe_text(trade_row.get("team_one_gets"), "None"), inline=True)
        embed.add_field(name=safe_text(trade_row.get("team_two_name"), "Team Two"), value=safe_text(trade_row.get("team_two_gets"), "None"), inline=True)
        embed.add_field(
            name="Vote Tally",
            value=(
                f"✅ Approvals: **{safe_int(trade_row.get('approve_count'))} / {required_approvals}**\n"
                f"❌ Denials: **{safe_int(trade_row.get('deny_count'))} / {required_denials}**"
            ),
            inline=True,
        )
        status_value = safe_text(trade_row.get("status"), "pending").title()
        if trade_row.get("finalized_reason"):
            status_value += f"\nReason: {safe_text(trade_row.get('finalized_reason'))}"
        embed.add_field(name="Status", value=status_value, inline=False)
        notes = safe_text(trade_row.get("notes"), "")
        if notes:
            embed.add_field(name="Notes", value=notes, inline=False)
        votes = await asyncio.to_thread(self.db.get_trade_votes, trade_id)
        if votes:
            approval_names = [safe_text(v.get("voter_username")) for v in votes if v.get("vote") == "approve"]
            denial_names = [safe_text(v.get("voter_username")) for v in votes if v.get("vote") == "deny"]
            embed.add_field(name="Approved By", value=", ".join(approval_names) if approval_names else "—", inline=True)
            embed.add_field(name="Denied By", value=", ".join(denial_names) if denial_names else "—", inline=True)
        embed.set_footer(text=f"Trade committee needs {required_approvals} approve(s) to pass or {required_denials} denial(s) to fail.")
        return embed

    async def fetch_channel_message(self, channel_id: int, message_id: int) -> discord.Message | None:
        if not channel_id or not message_id:
            return None
        channel = self.get_channel(int(channel_id))
        if channel is None:
            try:
                channel = await self.fetch_channel(int(channel_id))
            except Exception:
                return None
        if not isinstance(channel, discord.TextChannel):
            return None
        try:
            return await channel.fetch_message(int(message_id))
        except Exception:
            return None

    async def refresh_trade_message(self, trade_row: dict[str, Any]) -> None:
        if not trade_row:
            return
        message = await self.fetch_channel_message(safe_int(trade_row.get("review_channel_id")), safe_int(trade_row.get("review_message_id")))
        if message is None:
            return
        status = safe_text(trade_row.get("status"), "pending").lower()
        view = TradeReviewView(self) if status == "pending" else None
        await message.edit(embed=await self.build_trade_embed(trade_row), view=view)

    async def post_trade_announcement(self, trade_row: dict[str, Any]) -> None:
        if not trade_row:
            return
        guild_id = safe_int(trade_row.get("guild_id"))
        cfg = await asyncio.to_thread(self.db.get_guild_config, guild_id)
        announcement_channel_id = safe_int(trade_row.get("announcement_channel_id")) or safe_int((cfg or {}).get("trade_announcements_channel_id"))
        if not announcement_channel_id:
            return
        guild = self.get_guild(guild_id)
        channel = guild.get_channel(announcement_channel_id) if guild else None
        if not isinstance(channel, discord.TextChannel):
            try:
                fetched = await self.fetch_channel(announcement_channel_id)
                channel = fetched if isinstance(fetched, discord.TextChannel) else None
            except Exception:
                channel = None
        if not isinstance(channel, discord.TextChannel):
            return
        coach_mentions = []
        for coach_id in [safe_int(trade_row.get("coach_one_user_id")), safe_int(trade_row.get("coach_two_user_id"))]:
            if coach_id:
                coach_mentions.append(f"<@{coach_id}>")
        mention_text = " and ".join(coach_mentions) if coach_mentions else "Coaches"
        status = safe_text(trade_row.get("status"), "pending").lower()
        if status not in {"approved", "denied"}:
            return
        emoji = "✅" if status == "approved" else "❌"
        status_word = "approved" if status == "approved" else "denied"
        content = (
            f"{emoji} {mention_text} — Trade **#{safe_int(trade_row.get('id'))}** between "
            f"**{safe_text(trade_row.get('team_one_name'))}** and **{safe_text(trade_row.get('team_two_name'))}** was **{status_word}**."
        )
        existing_message_id = safe_int(trade_row.get("announcement_message_id"))
        existing = await self.fetch_channel_message(int(announcement_channel_id), existing_message_id) if existing_message_id else None
        if existing is not None:
            await existing.edit(content=content, embed=await self.build_trade_embed(trade_row))
            return
        message = await channel.send(content=content, embed=await self.build_trade_embed(trade_row))
        await asyncio.to_thread(self.db.set_trade_announcement_message, safe_int(trade_row.get("id")), int(channel.id), int(message.id))

    async def finalize_trade_if_threshold_met(self, trade_row: dict[str, Any], acting_user_id: int | None = None, reason: str = "") -> dict[str, Any]:
        if not trade_row:
            return trade_row
        status = safe_text(trade_row.get("status"), "pending").lower()
        if status != "pending":
            return trade_row
        guild_id = safe_int(trade_row.get("guild_id"))
        cfg = await asyncio.to_thread(self.db.get_guild_config, guild_id)
        required_approvals = max(1, safe_int((cfg or {}).get("trade_required_approvals"), 2))
        required_denials = max(1, safe_int((cfg or {}).get("trade_required_denials"), 2))
        approve_count = safe_int(trade_row.get("approve_count"))
        deny_count = safe_int(trade_row.get("deny_count"))
        if approve_count >= required_approvals:
            trade_row = await asyncio.to_thread(self.db.finalize_trade, safe_int(trade_row.get("id")), "approved", acting_user_id, reason or "Reached required approvals")
        elif deny_count >= required_denials:
            trade_row = await asyncio.to_thread(self.db.finalize_trade, safe_int(trade_row.get("id")), "denied", acting_user_id, reason or "Reached required denials")
        await self.refresh_trade_message(trade_row)
        if safe_text(trade_row.get("status"), "pending").lower() in {"approved", "denied"}:
            await self.post_trade_announcement(trade_row)
        return trade_row

    # ------------------------------------------------------------------ #
    # Reminder background task                                            #
    # ------------------------------------------------------------------ #

    @tasks.loop(minutes=30)
    async def reminder_loop(self) -> None:
        """Background task: send scheduling reminders for unscheduled game channels."""
        try:
            await self._send_pending_reminders()
        except Exception as exc:
            LOGGER.exception("reminder_loop: unhandled error: %s", exc)

    @reminder_loop.before_loop
    async def before_reminder_loop(self) -> None:
        await self.wait_until_ready()

    async def _send_pending_reminders(self) -> None:
        """Check all pending game channels and send reminders where the interval has elapsed."""
        try:
            pending = await asyncio.to_thread(self.db.get_pending_game_channels)
        except Exception as exc:
            LOGGER.exception("_send_pending_reminders: failed to fetch pending channels: %s", exc)
            return

        now = datetime.datetime.now(datetime.timezone.utc)

        for row in pending:
            channel_id = safe_int(row["channel_id"])
            league_id = safe_int(row["league_id"])

            try:
                interval_hours = await asyncio.to_thread(self.db.get_reminder_interval, league_id)
            except Exception as exc:
                LOGGER.warning(
                    "_send_pending_reminders: failed to get interval for league %s: %s",
                    league_id,
                    exc,
                )
                interval_hours = 8

            last_reminder_at: datetime.datetime | None = row.get("last_reminder_at")
            if last_reminder_at is not None:
                if last_reminder_at.tzinfo is None:
                    last_reminder_at = last_reminder_at.replace(tzinfo=datetime.timezone.utc)
                elapsed_hours = (now - last_reminder_at).total_seconds() / 3600.0
                if elapsed_hours < interval_hours:
                    continue
            # first reminder: also honour the interval since channel creation
            else:
                created_at: datetime.datetime | None = row.get("created_at")
                if created_at is not None:
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=datetime.timezone.utc)
                    elapsed_hours = (now - created_at).total_seconds() / 3600.0
                    if elapsed_hours < interval_hours:
                        continue

            # Resolve the Discord channel
            channel = self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except discord.NotFound:
                    LOGGER.info(
                        "_send_pending_reminders: channel %s not found, cleaning up state (league_id=%s)",
                        channel_id,
                        league_id,
                    )
                    try:
                        await asyncio.to_thread(self.db.delete_game_channel_state, channel_id)
                    except Exception:
                        pass
                    continue
                except Exception as exc:
                    LOGGER.warning(
                        "_send_pending_reminders: error fetching channel %s (league_id=%s): %s",
                        channel_id,
                        league_id,
                        exc,
                    )
                    continue

            if not isinstance(channel, discord.TextChannel):
                continue

            home_user_id = safe_int(row.get("home_user_id"))
            away_user_id = safe_int(row.get("away_user_id"))

            mentions: list[str] = []
            if home_user_id:
                mentions.append(f"<@{home_user_id}>")
            if away_user_id:
                mentions.append(f"<@{away_user_id}>")

            if not mentions:
                LOGGER.warning(
                    "_send_pending_reminders: no user IDs for channel %s (league_id=%s) — skipping reminder",
                    channel_id,
                    league_id,
                )
                continue

            mention_str = " ".join(mentions)
            try:
                await channel.send(
                    f"⏰ **Game Reminder** — {mention_str} your game hasn't been scheduled yet! "
                    "Please use the **⏰ Scheduled** or **✅ Completed** buttons above to update the status."
                )
                await asyncio.to_thread(self.db.update_last_reminder_at, channel_id)
            except discord.HTTPException as exc:
                LOGGER.warning(
                    "_send_pending_reminders: failed to send reminder to channel %s (league_id=%s): %s",
                    channel_id,
                    league_id,
                    exc,
                )

    # ------------------------------------------------------------------ #
    # Auto-delete background task                                         #
    # ------------------------------------------------------------------ #

    @tasks.loop(minutes=5)
    async def cleanup_loop(self) -> None:
        """Background task: delete completed game channels whose delete_at has passed."""
        try:
            await self._delete_completed_channels()
        except Exception as exc:
            LOGGER.exception("cleanup_loop: unhandled error: %s", exc)

    @cleanup_loop.before_loop
    async def before_cleanup_loop(self) -> None:
        await self.wait_until_ready()

    async def _delete_completed_channels(self) -> None:
        """Find completed game channels past their delete_at time and delete them."""
        try:
            due = await asyncio.to_thread(self.db.get_channels_due_for_deletion)
        except Exception as exc:
            LOGGER.exception("_delete_completed_channels: failed to fetch due channels: %s", exc)
            return

        for row in due:
            channel_id = safe_int(row["channel_id"])
            channel = self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except discord.NotFound:
                    LOGGER.info(
                        "_delete_completed_channels: channel %s not found, cleaning up DB state",
                        channel_id,
                    )
                    try:
                        await asyncio.to_thread(self.db.delete_game_channel_state, channel_id)
                    except Exception:
                        pass
                    continue
                except Exception as exc:
                    LOGGER.warning(
                        "_delete_completed_channels: error fetching channel %s: %s",
                        channel_id,
                        exc,
                    )
                    continue

            if not isinstance(channel, discord.TextChannel):
                try:
                    await asyncio.to_thread(self.db.delete_game_channel_state, channel_id)
                except Exception:
                    pass
                continue

            try:
                await channel.delete(reason="Game completed — auto-deleted by bot after 1 hour")
                await asyncio.to_thread(self.db.delete_game_channel_state, channel_id)
                LOGGER.info("_delete_completed_channels: deleted channel %s", channel_id)
            except discord.NotFound:
                LOGGER.info("_delete_completed_channels: channel %s already gone", channel_id)
                await asyncio.to_thread(self.db.delete_game_channel_state, channel_id)
            except discord.HTTPException as exc:
                LOGGER.warning(
                    "_delete_completed_channels: failed to delete channel %s: %s",
                    channel_id,
                    exc,
                )


async def team_name_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    bot = interaction.client
    if not isinstance(bot, NexusLeagueBot) or not interaction.guild:
        return []

    config = await asyncio.to_thread(bot.db.get_guild_config, interaction.guild.id)
    if not config:
        return []

    rows = await asyncio.to_thread(bot.db.team_autocomplete, int(config["league_id"]), current)
    return [app_commands.Choice(name=row["team_name"], value=row["team_name"]) for row in rows]


def main() -> None:
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    database_url = os.getenv("DATABASE_URL", "").strip()

    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")

    guild_ids = parse_guild_ids()
    db = Database(database_url)
    db.init()

    bot = NexusLeagueBot(db=db, guild_ids=guild_ids)
    bot.run(token)


if __name__ == "__main__":
    main()
