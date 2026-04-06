"""
src/utils/progress_tracker.py - Shared progress state for background analysis

Uses a JSON file as cross-process shared state so both the analyzer process
and the dashboard Flask process can see the same progress.
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path

# The file written by the analyzer process, read by the dashboard process
_PROGRESS_FILE = Path(__file__).parent.parent.parent / "output" / "progress.json"

# In-process state (for same-process use)
state: dict = {
    "running": False,
    "processed": 0,
    "total": 0,
    "phase": "idle",
    "status": "idle",     # idle | running | done | error
    "message": "",
    "percent": 0,
    "started_at": "",
    "current_file": "",
}


def _save() -> None:
    """Persist state to file so the dashboard process can read it."""
    try:
        _PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        pass


def load_from_file() -> dict:
    """Read the latest progress from the shared JSON file."""
    try:
        if _PROGRESS_FILE.exists():
            with open(_PROGRESS_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return state.copy()


def reset(total: int = 0, message: str = "Memulai analisis...") -> None:
    state.update({
        "running": True,
        "processed": 0,
        "total": total,
        "phase": "starting",
        "status": "running",
        "message": message,
        "percent": 0,
        "started_at": datetime.now().isoformat(),
        "current_file": "",
    })
    _save()


def update(processed: int = None, total: int = None,
           phase: str = None, message: str = None) -> None:
    if processed is not None:
        state["processed"] = processed
    if total is not None:
        state["total"] = total
    if phase is not None:
        state["phase"] = phase
    if message is not None:
        state["message"] = message
    t = state["total"]
    p = state["processed"]
    state["percent"] = round(p / t * 100, 1) if t > 0 else 0
    _save()


def finish(success: bool = True, message: str = "") -> None:
    state["running"] = False
    state["status"] = "done" if success else "error"
    state["message"] = message or ("Analisis selesai!" if success else "Analisis gagal")
    state["percent"] = 100 if success else state["percent"]
    _save()


def set_error(message: str) -> None:
    state["running"] = False
    state["status"] = "error"
    state["message"] = message
    _save()
