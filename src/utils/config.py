"""
src/utils/config.py - Configuration loader
"""
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL_DISCOVERY = os.getenv("GROQ_MODEL_DISCOVERY", "llama-3.3-70b-versatile")
GROQ_MODEL_ANALYSIS = os.getenv("GROQ_MODEL_ANALYSIS", "llama-3.1-8b-instant")
INPUT_DIR = BASE_DIR / os.getenv("INPUT_DIR", "Knowledge")
OUTPUT_DIR = BASE_DIR / os.getenv("OUTPUT_DIR", "output")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "5050"))
DB_PATH = OUTPUT_DIR / "analysis_cache.db"
CATEGORIES_PATH = OUTPUT_DIR / "categories.json"

# Collect all API keys (for parallel processing — up to 12)
GROQ_API_KEYS: list[str] = []
for _key_name in [
    "GROQ_API_KEY", "GROQ_API_KEY_2", "GROQ_API_KEY_3", "GROQ_API_KEY_4",
    "GROQ_API_KEY_5", "GROQ_API_KEY_6", "GROQ_API_KEY_7", "GROQ_API_KEY_8",
    "GROQ_API_KEY_9", "GROQ_API_KEY_10", "GROQ_API_KEY_11", "GROQ_API_KEY_12",
]:
    _k = os.getenv(_key_name, "").strip()
    if _k:
        GROQ_API_KEYS.append(_k)
if not GROQ_API_KEYS:
    GROQ_API_KEYS = [GROQ_API_KEY]

# Create output dir if not exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
