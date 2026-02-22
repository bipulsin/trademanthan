"""
CAR GPT config loader - reads number_of_weeks from car_config.json
"""
import json
import os
from pathlib import Path

_BASE = Path(__file__).parent.parent
DEFAULT_WEEKS = 52


def _config_path():
    p = _BASE / "car_config.json"
    if p.exists():
        return p
    data_dir = Path("/home/ubuntu/trademanthan/data")
    if data_dir.exists():
        return data_dir / "car_config.json"
    return p


def get_number_of_weeks() -> int:
    """Read number_of_weeks from config file, fallback to env/default."""
    for path in [_config_path(), _BASE / "car_config.json"]:
        try:
            if path.exists():
                with open(path, "r") as f:
                    data = json.load(f)
                return int(data.get("number_of_weeks", DEFAULT_WEEKS))
        except Exception:
            pass
    return int(os.getenv("CAR_NUMBER_OF_WEEKS", str(DEFAULT_WEEKS)))


def set_number_of_weeks(value: int) -> bool:
    """Write number_of_weeks to config file."""
    path = _config_path()
    try:
        data = {}
        if path.exists():
            with open(path, "r") as f:
                data = json.load(f)
        data["number_of_weeks"] = value
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception:
        return False
