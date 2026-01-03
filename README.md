# steam-gog

Crude but practical CLI tool to export your Steam library and match it against GOG titles using **GOGDB**.

The goal of this project is to **estimate how much it would cost to migrate your Steam library to GOG**, handling ambiguous matches and missing data safely, without false positives.

---

## Features

- Export Steam library to **CSV or SQLite**
- Match Steam games to **GOG products using GOGDB dumps**
- Safe, conservative matching strategy:
  - exact title match
  - normalized title match
  - unique LIKE fallback only
- Handles ambiguous matches without auto-assigning
- Incremental / idempotent runs
- SQLite-first workflow
- Designed for large GOGDB dumps (offline processing)

---

## Requirements

- Python 3.10+
- Steam Web API key
- Internet access (only for downloading GOGDB backups)

---

## Setup

### 1. Clone the repository

    git clone https://github.com/your-user/steam-gog.git
    cd steam-gog

### 2. Install dependencies

    pip install -r requirements.txt

### 3. Steam API key

Create a `.env` file or export an environment variable:

    KEY=your_steam_api_key_here

---

## Usage

### Export Steam library

You can export your Steam library either to CSV or directly into SQLite.

#### Export to SQLite (recommended)

    python cli.py steam --vanity meme_sommelier --sqlite steam.db

Or using a SteamID64:

    python cli.py steam --steamid 7656119XXXXXXXXXX --sqlite steam.db

---

### Match against GOG (using GOGDB)

This step will:

1. Download the latest GOGDB product dump (if not present)
2. Index products and prices into SQLite
3. Seed gogdb_games from your Steam library
4. Attempt safe matching to GOG products

    python cli.py gog match --sqlite-db steam.db

Matching is intentionally conservative.
Games with ambiguous matches are left unmatched.

---

## Matching Strategy

Matching is performed in this order:

1. Exact title match (case-insensitive)
2. Normalized title match (symbols, trademarks, punctuation removed)
3. LIKE match, only if:
   - the normalized name is long enough
   - exactly one GOG candidate exists

If more than one candidate exists, the game is skipped.

Each matched game stores:

- match_method (exact, norm_exact, like_unique)
- match_score (confidence indicator)

---

## Database Overview

Key tables:

- steam_games – raw Steam library
- gogdb_products – indexed GOGDB products
- gogdb_prices – historical and current prices
- gogdb_games – Steam to GOG mapping and results

Prices are stored in minor units (cents) and normalized during queries.

---

## Typical Next Steps

After matching, you can:

- Calculate the minimum total cost to rebuy your library on GOG
- Inspect unmatched or ambiguous games
- Compare base price vs discounted price
- Filter by discount percentage

Queries are intentionally kept outside the CLI for flexibility.

---

## Status

This project is experimental and exploratory.

- No automatic purchasing
- No direct GOG API integration (yet)
- Focused on correctness over completeness

---

## License

MIT