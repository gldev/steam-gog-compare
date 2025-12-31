# steam-gog

Crude cli tool to export your Steam library and match it against GOG titles.

## Features
- Export Steam library to CSV or SQLite
- Match Steam games to GOG products using GOGDB
- Handle missing games and conflicts safely
- Incremental / idempotent runs
- SQLite-first workflow

## Usage

### Import Steam library
python cli.py steam --vanity meme_sommelier --sqlite steam.db

### Match against GOG
python cli.py gog match --sqlite-db steam.db --limit 10