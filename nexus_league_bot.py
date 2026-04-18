import asyncio
import hashlib
import logging
import os
from typing import Any

import discord
from discord import app_commands
import psycopg
from psycopg.rows import dict_row


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("nexus-league-bot")

NO_SETUP_MESSAGE = "Please run `/setup` first to configure your league."
DEFAULT_ADMIN_ROLES = "Commissioner,Admin,COMMISH"
EMBED_FIELD_MAX_LENGTH = 1000


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
                    admin_role_names TEXT DEFAULT 'Commissioner,Admin,COMMISH',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
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
                SELECT p.id,
                       p.first_name,
                       p.last_name,
                       p.position,
                       t.team_name,
                       SUM(ps.pass_yards) AS pass_yards,
                       SUM(ps.pass_tds) AS pass_tds,
                       SUM(ps.interceptions) AS interceptions
                FROM playerstats ps
                JOIN player p
                  ON p.id = ps.player_id
                 AND p.league_id = ps.league_id
                LEFT JOIN team t
                  ON t.id = p.team_id
                 AND t.league_id = p.league_id
                WHERE ps.league_id = %s
                GROUP BY p.id, p.first_name, p.last_name, p.position, t.team_name
                ORDER BY SUM(ps.pass_yards) DESC, p.last_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_rushing_leaders(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id,
                       p.first_name,
                       p.last_name,
                       p.position,
                       t.team_name,
                       SUM(ps.rush_yards) AS rush_yards,
                       SUM(ps.rush_tds) AS rush_tds
                FROM playerstats ps
                JOIN player p
                  ON p.id = ps.player_id
                 AND p.league_id = ps.league_id
                LEFT JOIN team t
                  ON t.id = p.team_id
                 AND t.league_id = p.league_id
                WHERE ps.league_id = %s
                GROUP BY p.id, p.first_name, p.last_name, p.position, t.team_name
                ORDER BY SUM(ps.rush_yards) DESC, p.last_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_receiving_leaders(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id,
                       p.first_name,
                       p.last_name,
                       p.position,
                       t.team_name,
                       SUM(ps.rec_yards) AS rec_yards,
                       SUM(ps.rec_tds) AS rec_tds,
                       SUM(ps.receptions) AS receptions
                FROM playerstats ps
                JOIN player p
                  ON p.id = ps.player_id
                 AND p.league_id = ps.league_id
                LEFT JOIN team t
                  ON t.id = p.team_id
                 AND t.league_id = p.league_id
                WHERE ps.league_id = %s
                GROUP BY p.id, p.first_name, p.last_name, p.position, t.team_name
                ORDER BY SUM(ps.rec_yards) DESC, p.last_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_defense_leaders(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id,
                       p.first_name,
                       p.last_name,
                       p.position,
                       t.team_name,
                       SUM(ps.tackles) AS tackles,
                       SUM(ps.sacks) AS sacks,
                       SUM(ps.defensive_ints) AS defensive_ints,
                       SUM(ps.fumbles_forced) AS fumbles_forced
                FROM playerstats ps
                JOIN player p
                  ON p.id = ps.player_id
                 AND p.league_id = ps.league_id
                LEFT JOIN team t
                  ON t.id = p.team_id
                 AND t.league_id = p.league_id
                WHERE ps.league_id = %s
                GROUP BY p.id, p.first_name, p.last_name, p.position, t.team_name
                ORDER BY SUM(ps.tackles) DESC, p.last_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_touchdown_leaders(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id,
                       p.first_name,
                       p.last_name,
                       p.position,
                       t.team_name,
                       SUM(ps.pass_tds) AS pass_tds,
                       SUM(ps.rush_tds) AS rush_tds,
                       SUM(ps.rec_tds) AS rec_tds,
                       SUM(ps.pass_tds + ps.rush_tds + ps.rec_tds) AS total_tds
                FROM playerstats ps
                JOIN player p
                  ON p.id = ps.player_id
                 AND p.league_id = ps.league_id
                LEFT JOIN team t
                  ON t.id = p.team_id
                 AND t.league_id = p.league_id
                WHERE ps.league_id = %s
                GROUP BY p.id, p.first_name, p.last_name, p.position, t.team_name
                ORDER BY total_tds DESC, p.last_name ASC
                LIMIT 5
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_standings(self, league_id: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.team_name,
                       COALESCE(s.division_name, t.division, 'Unknown') AS division_name,
                       s.wins,
                       s.losses,
                       s.ties,
                       s.seed
                FROM standing s
                JOIN team t
                  ON t.id = s.team_id
                 AND t.league_id = s.league_id
                WHERE s.league_id = %s
                ORDER BY s.wins DESC, s.losses ASC, s.ties DESC, t.team_name ASC
                """,
                (league_id,),
            )
            return cur.fetchall()

    def fetch_schedule_for_week(self, league_id: int, week_number: int) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.week_number,
                       s.season_number,
                       s.is_complete,
                       home.team_name AS home_team,
                       away.team_name AS away_team,
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

    def player_search(self, league_id: int, name_query: str) -> list[dict[str, Any]]:
        with self.conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id,
                       p.first_name,
                       p.last_name,
                       p.position,
                       p.overall_rating,
                       t.team_name,
                       COALESCE(SUM(ps.pass_yards), 0) AS pass_yards,
                       COALESCE(SUM(ps.pass_tds), 0) AS pass_tds,
                       COALESCE(SUM(ps.interceptions), 0) AS interceptions,
                       COALESCE(SUM(ps.rush_yards), 0) AS rush_yards,
                       COALESCE(SUM(ps.rush_tds), 0) AS rush_tds,
                       COALESCE(SUM(ps.rec_yards), 0) AS rec_yards,
                       COALESCE(SUM(ps.rec_tds), 0) AS rec_tds,
                       COALESCE(SUM(ps.receptions), 0) AS receptions,
                       COALESCE(SUM(ps.tackles), 0) AS tackles,
                       COALESCE(SUM(ps.sacks), 0) AS sacks,
                       COALESCE(SUM(ps.defensive_ints), 0) AS defensive_ints,
                       COALESCE(SUM(ps.fumbles_forced), 0) AS fumbles_forced
                FROM player p
                LEFT JOIN team t
                  ON t.id = p.team_id
                 AND t.league_id = p.league_id
                LEFT JOIN playerstats ps
                  ON ps.player_id = p.id
                 AND ps.league_id = p.league_id
                WHERE p.league_id = %s
                  AND CONCAT(p.first_name, ' ', p.last_name) ILIKE %s
                GROUP BY p.id, p.first_name, p.last_name, p.position, p.overall_rating, t.team_name
                ORDER BY p.last_name ASC, p.first_name ASC
                LIMIT 5
                """,
                (league_id, f"%{name_query}%"),
            )
            return cur.fetchall()


def team_color_from_name(team_name: str | None) -> discord.Color:
    seed = team_name or "nexus"
    color_hex = hashlib.md5(seed.encode("utf-8")).hexdigest()[:6]
    return discord.Color(int(color_hex, 16))


def player_display_name(row: dict[str, Any]) -> str:
    return f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()


class NexusLeagueBot(discord.Client):
    def __init__(self, db: Database, guild_ids: list[int]) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        super().__init__(intents=intents)
        self.db = db
        self.tree = app_commands.CommandTree(self)
        self.guild_ids = guild_ids

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
            embed = discord.Embed(title="Server Configuration", color=discord.Color.blurple())
            embed.add_field(name="League", value=f"{league_name} (`{config['league_id']}`)", inline=False)
            embed.add_field(
                name="Log Channel",
                value=f"<#{log_channel_id}>" if log_channel_id else "Not configured",
                inline=True,
            )
            embed.add_field(
                name="Leaders Channel",
                value=f"<#{leaders_channel_id}>" if leaders_channel_id else "Not configured",
                inline=True,
            )
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

        @self.tree.command(name="roster", description="Show roster for a team")
        @app_commands.autocomplete(team=team_name_autocomplete)
        async def roster(interaction: discord.Interaction, team: str) -> None:
            await self.send_roster(interaction, team)

        @team_group.command(name="info", description="Show team overview")
        @app_commands.autocomplete(team=team_name_autocomplete)
        async def team_info(interaction: discord.Interaction, team: str) -> None:
            await self.send_team_info(interaction, team)

        @self.tree.command(name="schedule", description="Show current week or specific week schedule")
        @app_commands.describe(week="Optional week number")
        async def schedule(interaction: discord.Interaction, week: int | None = None) -> None:
            await self.send_schedule(interaction, week)

        @self.tree.command(name="scores", description="Show scores for most recently completed week")
        async def scores(interaction: discord.Interaction) -> None:
            await self.send_recent_scores(interaction)

        @player_group.command(name="search", description="Search for a player by name")
        async def player_search(interaction: discord.Interaction, name: str) -> None:
            await self.send_player_search(interaction, name)

        self.tree.add_command(setup_group)
        self.tree.add_command(leaders_group)
        self.tree.add_command(post_group)
        self.tree.add_command(team_group)
        self.tree.add_command(player_group)

        if self.guild_ids:
            for guild_id in self.guild_ids:
                guild_obj = discord.Object(id=guild_id)
                self.tree.copy_global_to(guild=guild_obj)
                await self.tree.sync(guild=guild_obj)
        else:
            await self.tree.sync()

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

        config = await asyncio.to_thread(self.db.get_guild_config, interaction.guild.id)
        leaders_channel_id = int(config.get("leaders_channel_id") or 0) if config else 0
        if not leaders_channel_id:
            await interaction.response.send_message("Leaders channel is not configured. Use `/setup channels` first.", ephemeral=True)
            return

        channel = interaction.guild.get_channel(leaders_channel_id)
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Configured leaders channel was not found.", ephemeral=True)
            return

        categories = [
            ("Passing", await asyncio.to_thread(self.db.fetch_passing_leaders, league_id), "passing"),
            ("Rushing", await asyncio.to_thread(self.db.fetch_rushing_leaders, league_id), "rushing"),
            ("Receiving", await asyncio.to_thread(self.db.fetch_receiving_leaders, league_id), "receiving"),
            ("Defense", await asyncio.to_thread(self.db.fetch_defense_leaders, league_id), "defense"),
            ("Touchdowns", await asyncio.to_thread(self.db.fetch_touchdown_leaders, league_id), "touchdowns"),
        ]

        league_name = await asyncio.to_thread(self.db.get_league_name, league_id)
        embed = discord.Embed(
            title=f"{league_name} - Season Leaders",
            description="Top 5 across all categories",
            color=discord.Color.gold(),
        )

        for label, rows, category in categories:
            if not rows:
                embed.add_field(name=label, value="No data found.", inline=False)
                continue

            lines: list[str] = []
            for idx, row in enumerate(rows, start=1):
                name = player_display_name(row)
                if category == "passing":
                    stat_text = f"{row['pass_yards']} yds, {row['pass_tds']} TD, {row['interceptions']} INT"
                elif category == "rushing":
                    stat_text = f"{row['rush_yards']} yds, {row['rush_tds']} TD"
                elif category == "receiving":
                    stat_text = f"{row['rec_yards']} yds, {row['rec_tds']} TD, {row['receptions']} REC"
                elif category == "defense":
                    stat_text = f"{row['tackles']} TKL, {row['sacks']} SCK, {row['defensive_ints']} INT, {row['fumbles_forced']} FF"
                else:
                    stat_text = f"{row['total_tds']} total TD"
                lines.append(f"{idx}. {name} ({row.get('team_name') or 'FA'}) - {stat_text}")
            embed.add_field(name=label, value="\n".join(lines), inline=False)

        await channel.send(embed=embed)
        await interaction.response.send_message(f"Posted season leaders to {channel.mention}.", ephemeral=True)

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

        lines = [
            f"**{p.get('first_name', '')} {p.get('last_name', '')}** | "
            f"{p.get('position', '-')} | OVR {p.get('overall_rating', '-')} | "
            f"Age {p.get('age', '-')} | {p.get('dev_trait', '-')}"
            for p in roster
        ]
        chunks: list[str] = []
        current = ""
        for line in lines:
            if len(current) + len(line) + 1 > EMBED_FIELD_MAX_LENGTH:
                chunks.append(current)
                current = line
            else:
                current = f"{current}\n{line}" if current else line
        if current:
            chunks.append(current)

        for idx, chunk in enumerate(chunks, start=1):
            embed.add_field(name=f"Players {idx}", value=chunk, inline=False)

        await interaction.response.send_message(embed=embed)

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

    async def send_player_search(self, interaction: discord.Interaction, name_query: str) -> None:
        league_id = await self.get_league_id(interaction)
        if league_id is None:
            return

        players = await asyncio.to_thread(self.db.player_search, league_id, name_query)
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
