# nexus-league-bot

The Discord half of **NexusLeague** — a Madden franchise-league SaaS. Pairs with
[`nexusexporter-clean`](https://github.com/Sevorcer/nexusexporter-clean) (the
web app + ingest API). Both processes share **one PostgreSQL database**.

The bot reads league data from that database and exposes it through Discord
slash commands: leaders, standings, rosters, schedule, scores, player search,
trades, weekly news, XP/bounties, and more.

---

## Table of contents

1. [Prerequisites](#1-prerequisites)
2. [Quick start (with the web app)](#2-quick-start-with-the-web-app)
3. [Quick start (bot only)](#3-quick-start-bot-only)
4. [Discord application setup](#4-discord-application-setup)
5. [In-server setup commands](#5-in-server-setup-commands)
6. [Environment variables](#6-environment-variables)
7. [Slash commands reference](#7-slash-commands-reference)
8. [Deployment](#8-deployment)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

- Python **3.11.x** (matches `runtime.txt`; Railway uses this).
- Access to the same PostgreSQL database that `nexusexporter-clean` writes to.
- A Discord application + bot token (see [§4](#4-discord-application-setup)).
- *(Optional)* OpenAI API key for AI-generated headlines and matchup previews.

---

## 2. Quick start (with the web app)

The recommended path. Run the web app first so the database exists with all
tables, then point the bot at the same `DATABASE_URL`.

```bash
# 1. Start the web app + Postgres
cd ../nexusexporter-clean
docker compose up -d
# Postgres is now available on host port 5433.

# 2. Set up the bot
cd ../nexus-league-bot
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                # Windows: copy .env.example .env
# Edit .env — at minimum fill in DISCORD_BOT_TOKEN.
# DATABASE_URL=postgresql://nexus:nexus@localhost:5433/nexus

# 3. Run the bot
python nexus_league_bot.py
```

You should see `Logged in as <bot-name>` in the console once the connection
succeeds.

---

## 3. Quick start (bot only)

If your database is already running somewhere else (production, Railway, an
existing self-hosted Postgres), skip the web stack:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Fill in DISCORD_BOT_TOKEN and a DATABASE_URL pointing at your running DB.
python nexus_league_bot.py
```

> **Note:** The bot does not create or migrate game-data tables — that's the
> web app's job. The bot only creates its own config tables (`guild_config`,
> `bounties`, `trades`, `xp_*`) on first run via `db.init()`.

---

## 4. Discord application setup

1. Go to <https://discord.com/developers/applications> and click **New Application**.
2. Name it (e.g. *NexusLeague Bot*) → **Create**.
3. **Bot** tab on the left → **Reset Token** → copy the token. Paste into
   `DISCORD_BOT_TOKEN` in your `.env`. **Don't share or commit it.**
4. Still on the **Bot** tab, scroll to **Privileged Gateway Intents** and enable:
   - **Server Members Intent**
   - **Message Content Intent**
   *(Required for XP tracking and welcome flow.)*
5. **OAuth2 → URL Generator**:
   - **Scopes**: `bot`, `applications.commands`
   - **Bot Permissions**: `Send Messages`, `Embed Links`, `Manage Channels`,
     `Manage Roles`, `Mention Everyone`, `Read Message History`,
     `View Channels`, `Use Slash Commands`
6. Copy the generated URL, open it in a browser, pick the server, and authorize.
7. The bot should now appear in your server's member list (offline until you
   start the process).

---

## 5. In-server setup commands

After the bot joins your server, run these as a server admin:

```
/setup league league_id:<int>
/setup channels log:#bot-log leaders:#leaders
/setup news_channel channel:#news
/setup xp level_up_channel:#level-up cooldown_seconds:60 min_message_len:5
```

Optional but recommended:

```
/setup trade_channels committee_role:@TradeCommittee review_channel:#trade-review announcements_channel:#trades
/setup openai_key key:sk-...    # only if you want per-guild OpenAI keys
/config                         # check what's saved
/ping                           # confirm connectivity
```

`league_id` must match the **NexusLeague league ID** shown on the web
dashboard (`/league/{id}`). One Discord server = one league.

---

## 6. Environment variables

Full list is in [`.env.example`](.env.example). Summary:

| Variable                   | Required | Purpose                                                 |
| -------------------------- | -------- | ------------------------------------------------------- |
| `DISCORD_BOT_TOKEN`        | yes      | Bot token from Discord Developer Portal                 |
| `DATABASE_URL`             | yes      | Same Postgres URL as the web app                        |
| `GUILD_IDS`                | no       | Comma-separated guild IDs for instant slash-cmd sync    |
| `OPENAI_API_KEY`           | no       | Default OpenAI key for AI features (fallback per-guild) |
| `OPENAI_MODEL`             | no       | Model name (default `gpt-4o-mini`)                      |
| `OPENAI_API_KEY_TEMPLATE`  | no       | Advanced: per-guild key template                        |
| `AUTO_POST_MATCHUP_PREVIEWS` | no     | `true`/`false` — auto-post Game-of-the-Week preview     |

`GUILD_IDS` tip: during development, set it to your test server's ID so
slash-command updates appear immediately. Leave empty in production for
global sync.

---

## 7. Slash commands reference

**Setup / config**
- `/setup league league_id:<int>`
- `/setup channels log:<channel> leaders:<channel>`
- `/setup news_channel channel:<channel>`
- `/setup trade_channels committee_role:<role> review_channel:<channel> announcements_channel:<channel> [required_approvals] [required_denials]`
- `/setup openai_key key:<string>`
- `/setup xp level_up_channel:<channel> cooldown_seconds:<int> min_message_len:<int> [blacklist_channels]`
- `/config`
- `/ping`

**League data**
- `/leaders passing | rushing | receiving | defense | touchdowns`
- `/standings`
- `/openteams`
- `/roster team:<team_name>`
- `/team info team:<team_name>`
- `/team list`
- `/schedule [week:<int>]`
- `/scores`
- `/player search name:<string>`

**Posting / news**
- `/post standings`
- `/post season_leaders`
- `/post headline`
- `/post_weekly_news week:<int> [phase] [gotw_pick:'Away @ Home'] [channel]`
- `/headline`
- `/create_weekly_channels week:<int> [phase] [category_name]`

**Trades**
- `/trade coach_one:<member> coach_two:<member> team_one:<string> team_two:<string> team_one_gets:<string> team_two_gets:<string> [notes]`
- `/tradehistory [limit:<int>]`
- `/forcetrade trade_id:<int> decision:<approve|deny> [reason]`

**XP**
- `/xprank [user]`
- `/xpleaderboard`

**Bounties**
- `/createbounty title:<string> reward:<number> description:<string>`
- `/bounties`
- `/claimbounty bounty_id:<int>`
- `/editbounty bounty_id:<int> [title] [description] [reward]`

---

## 8. Deployment

### Railway (recommended)

1. Push this repo to GitHub.
2. Create a new Railway project → **Deploy from GitHub** → pick this repo.
3. Add the **same Postgres** that backs the web app (or point both services
   at one external DB).
4. Set environment variables in the Railway dashboard:
   - `DISCORD_BOT_TOKEN`
   - `DATABASE_URL` (Railway can inject via `${{Postgres.DATABASE_URL}}`)
   - Anything else from §6.
5. The included `Procfile` declares this as a `worker` process — Railway runs
   it automatically, no port to expose.

### Docker

A `Dockerfile` is included for users who prefer container deploys:

```bash
docker build -t nexus-league-bot .
docker run --rm --env-file .env nexus-league-bot
```

If your Postgres also runs in Docker, place the bot on the same network so
`DATABASE_URL` can use the service hostname.

### Bare VPS / always-on host

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Manage the process with systemd, pm2, supervisor, or your tool of choice.
python nexus_league_bot.py
```

---

## 9. Troubleshooting

| Symptom                                       | Likely cause / fix                                                           |
| --------------------------------------------- | ---------------------------------------------------------------------------- |
| `RuntimeError: DISCORD_BOT_TOKEN is required` | `.env` missing or token blank. Recopy from Discord Developer Portal.         |
| `RuntimeError: DATABASE_URL is required`      | `DATABASE_URL` not set. Confirm web stack is running and URL is correct.     |
| Bot online but slash commands not visible     | Global sync can take up to ~1h. Set `GUILD_IDS` to your guild for instant.  |
| `Please run /setup first`                     | New guild — run `/setup league league_id:<int>` once per server.             |
| `psycopg.OperationalError: could not connect` | Wrong host/port in `DATABASE_URL`, or DB not running. Try `psql` to confirm. |
| `/leaders` returns "no data"                  | Web app hasn't ingested stats yet. Push data via the Madden Companion App.   |
| `/headline` errors out                        | `OPENAI_API_KEY` not set — set one or use `/setup openai_key` per-guild.    |

Still stuck? Open an issue with the relevant log line from the bot console.
