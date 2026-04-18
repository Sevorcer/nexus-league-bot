# nexus-league-bot

Discord bot built for the `nexusexporter-clean` PostgreSQL schema with strict per-league isolation.

## Features

- Multi-league support: each Discord server maps to one `league_id` via `guild_config`
- Slash commands for setup, leaders, standings, rosters, teams, schedule/scores, and player search
- All game-data queries filter by `league_id`
- Autocomplete for team-name parameters
- Railway-ready deployment files included

## Environment Variables

- `DISCORD_BOT_TOKEN` (required)
- `DATABASE_URL` (required)
- `GUILD_IDS` (optional, comma-separated guild IDs for instant command sync)

## Local Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python nexus_league_bot.py
```

## Required Discord Commands

- `/setup league league_id:<int>`
- `/setup channels log:<channel> leaders:<channel>`
- `/config`
- `/leaders passing`
- `/leaders rushing`
- `/leaders receiving`
- `/leaders defense`
- `/leaders touchdowns`
- `/post season_leaders`
- `/standings`
- `/post standings`
- `/roster team:<team_name>`
- `/team info team:<team_name>`
- `/schedule [week:<int>]`
- `/scores`
- `/player search name:<string>`
