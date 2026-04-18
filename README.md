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
- `GUILD_IDS` (optional, comma-separated guild IDs for instant startup command sync; bot also globally syncs and syncs on new guild joins)

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
- `/setup news_channel channel:<channel>`
- `/setup trade_channels committee_role:<role> review_channel:<channel> announcements_channel:<channel> [required_approvals] [required_denials]`
- `/setup openai_key key:<string>`
- `/setup xp level_up_channel:<channel> cooldown_seconds:<int> min_message_len:<int> [blacklist_channels]`
- `/config`
- `/ping`
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
- `/trade coach_one:<member> coach_two:<member> team_one:<string> team_two:<string> team_one_gets:<string> team_two_gets:<string> [notes]`
- `/tradehistory [limit:<int>]`
- `/forcetrade trade_id:<int> decision:<approve|deny> [reason]`
- `/post_weekly_news week:<int> [phase] [gotw_pick:'Away @ Home'] [channel]`
- `/create_weekly_channels week:<int> [category_name]`
- `/xprank [user]`
- `/xplevel [user]`
- `/xpleaderboard`
- `/createbounty title:<string> reward:<number> description:<string>`
- `/bounties`
- `/claimbounty bounty_id:<int>`
- `/editbounty bounty_id:<int> [title] [description] [reward]`
