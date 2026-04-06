"""
src/agents/data_ingestion.py - Excel reader and data preparer
Reads all .xlsx files from input directory, cleans text, batches rows.
"""
from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Generator

from src.utils.html_cleaner import clean_text, clean_resolved_notes
from src.utils.config import INPUT_DIR

# Mapping dari nama kolom Excel → key internal
COLUMN_MAP = {
    "No. Tiket": "no_tiket",
    "Tiket Dibuat": "tiket_dibuat",
    "Channel": "channel",
    "Status": "status",
    "Pelapor": "pelapor",
    "Deskripsi Permasalahan": "deskripsi_raw",
    "Judul Permasalahan": "judul",
    "Kategori": "kategori_existing",
    "Prioritas": "prioritas",
    "Severity": "severity",
    "Kelompok yang Ditugaskan": "kelompok",
    "Resolved Notes": "resolved_raw",
    "Alasan": "alasan_existing",
    "Service offering": "service",
    "Lokasi Pelapor": "lokasi",
    "Root Cause and Solution": "root_cause_raw",
}


def load_excel_files(input_dir: Path = INPUT_DIR) -> pd.DataFrame:
    """Load all .xlsx files in the input directory into one DataFrame."""
    dfs = []
    xlsx_files = list(input_dir.glob("*.xlsx"))
    if not xlsx_files:
        raise FileNotFoundError(f"Tidak ada file .xlsx di {input_dir}")

    print(f"\n📂 Ditemukan {len(xlsx_files)} file Excel:")
    for f in xlsx_files:
        print(f"   • {f.name}")
        try:
            # Use openpyxl engine with read_only for large files (much faster)
            df = pd.read_excel(f, dtype=str, engine="openpyxl")
            df["_source_file"] = f.name
            dfs.append(df)
            print(f"     ✅ {len(df):,} baris dimuat")
        except Exception as e:
            print(f"     ❌ Error: {e}")

    combined = pd.concat(dfs, ignore_index=True)
    print(f"\n📊 Total: {len(combined):,} tiket dari {len(dfs)} file\n")
    return combined


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns and clean text fields."""
    # Rename only columns that exist
    rename_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Ensure required columns exist
    for col in ["no_tiket", "judul", "deskripsi_raw", "resolved_raw", "root_cause_raw", "kategori_existing", "alasan_existing", "service", "kelompok"]:
        if col not in df.columns:
            df[col] = ""

    # Fill NaN
    df = df.fillna("")

    # Clean text columns
    df["judul"] = df["judul"].apply(lambda x: clean_text(str(x))[:300])
    df["deskripsi"] = df["deskripsi_raw"].apply(lambda x: clean_text(str(x))[:800])
    df["resolved_notes"] = df["resolved_raw"].apply(lambda x: clean_resolved_notes(str(x))[:600])
    df["root_cause_solution"] = df["root_cause_raw"].apply(lambda x: clean_resolved_notes(str(x))[:600])
    df["no_tiket"] = df["no_tiket"].astype(str).str.strip()

    # Drop duplicates by no_tiket (keep latest file)
    df = df.drop_duplicates(subset=["no_tiket"], keep="last")

    return df


def get_sample_for_discovery(df: pd.DataFrame, n: int = 400) -> list[dict]:
    """Return a random sample of tickets for category discovery."""
    sample = df.sample(min(n, len(df)), random_state=42)
    return df_to_ticket_list(sample)


def df_to_ticket_list(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame rows to list of dicts for analysis. Optimized with itertuples."""
    # Use faster vectorized approach instead of iterrows
    records = df.to_dict(orient="records")
    tickets = []
    for row in records:
        tickets.append({
            "no_tiket": str(row.get("no_tiket", "") or ""),
            "judul": str(row.get("judul", "") or ""),
            "deskripsi": str(row.get("deskripsi", "") or ""),
            "resolved_notes": str(row.get("resolved_notes", "") or ""),
            "root_cause_solution": str(row.get("root_cause_solution", "") or ""),
            "kategori_existing": str(row.get("kategori_existing", "") or ""),
            "alasan_existing": str(row.get("alasan_existing", "") or ""),
            "tiket_dibuat": str(row.get("tiket_dibuat", "") or ""),
            "status": str(row.get("status", "") or ""),
            "kelompok": str(row.get("kelompok", "") or ""),
            "service": str(row.get("service", "") or ""),
            "lokasi": str(row.get("lokasi", "") or ""),
            "_source_file": str(row.get("_source_file", "") or ""),
        })
    return tickets


def batch_tickets(tickets: list[dict], batch_size: int) -> Generator[list[dict], None, None]:
    """Yield successive batches of tickets."""
    for i in range(0, len(tickets), batch_size):
        yield tickets[i: i + batch_size]
