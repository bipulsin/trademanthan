"""
Load environment variables from the repository root `.env` file.

Why this exists:
- `load_dotenv()` with no path only searches the current working directory; systemd
  or uvicorn may start with a cwd where `.env` is not found.
- By default, python-dotenv does NOT override existing env vars; an empty
  `TELEGRAM_BOT_TOKEN=` in systemd would block values from `.env`.

We load `<project_root>/.env` explicitly with override=True so server `.env` wins.
Project root = parent of the `backend/` package directory.
"""
from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"

if _ENV_FILE.is_file():
    load_dotenv(_ENV_FILE, override=True)
else:
    # Fallback: cwd-relative (legacy); does not override existing keys
    load_dotenv()
