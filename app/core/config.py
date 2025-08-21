import json
import os
from pathlib import Path
from typing import Dict, List
from dotenv import load_dotenv

# --- Base paths ---
CORE_DIR = Path(__file__).resolve().parent        # app/core
APP_DIR = CORE_DIR.parent                         # app
ROOT_DIR = APP_DIR.parent                         # project root

# Load .env from project root
load_dotenv(ROOT_DIR / ".env")

# --- Access Map ---
ACCESS_MAP_PATH = APP_DIR / "constants" / "access_map.json"
ACCESS_MAP: Dict[str, List[str]] = json.loads(ACCESS_MAP_PATH.read_text())

# --- Env Vars ---
DATABRICKS_HOST: str = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN: str = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_ACCOUNT_ID: str = os.getenv("DATABRICKS_ACCOUNT_ID", "")
