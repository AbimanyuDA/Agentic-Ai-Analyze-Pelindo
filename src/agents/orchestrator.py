"""
src/agents/orchestrator.py - Main Orchestrator Agent
Coordinates the full analysis pipeline end-to-end.
"""
from __future__ import annotations
import time
from pathlib import Path

from src.utils.config import INPUT_DIR, OUTPUT_DIR, BATCH_SIZE, DB_PATH
from src.utils.cache_manager import CacheManager
from src.agents.data_ingestion import load_excel_files, prepare_dataframe, get_sample_for_discovery
from src.agents.category_schema import discover_categories
from src.agents.semantic_analyzer import run_analysis
from src.agents.output_agent import merge_results, write_excel, write_json_for_dashboard


def run_full_pipeline(
    input_dir: Path = INPUT_DIR,
    output_dir: Path = OUTPUT_DIR,
    batch_size: int = BATCH_SIZE,
    force_rediscover: bool = False,
    max_tickets: object = None,
    output_excel: bool = True,
) -> dict:
    """
    Full end-to-end pipeline:
    1. Load & clean Excel files
    2. Discover categories (or use cached)
    3. Analyze tickets in batches
    4. Write Excel + dashboard JSON

    Returns summary stats dict.
    """
    from src.utils import progress_tracker as pt

    start_time = time.time()
    cache = CacheManager(DB_PATH)

    print("=" * 60)
    print("  🚀 Pelindo AI Incident Analysis System")
    print("=" * 60)

    # ── Phase 1: Data Ingestion ──────────────────────────────────────────────
    pt.reset(message="Membaca file Excel...")
    pt.update(phase="ingestion", message="Membaca file Excel...")
    print("\n[Phase 1/4] 📂 Data Ingestion")
    df = load_excel_files(input_dir)
    df = prepare_dataframe(df)

    if max_tickets:
        df = df.head(max_tickets)
        print(f"   ℹ️  Dibatasi {max_tickets} tiket (mode test)")

    total_tickets = len(df)
    pt.update(total=total_tickets, message=f"Dimuat {total_tickets:,} tiket dari Excel")

    # ── Phase 2: Category Discovery ──────────────────────────────────────────
    pt.update(phase="discovery", message="AI sedang menemukan kategori semantik...")
    print("\n[Phase 2/4] 🔍 Category Discovery")
    sample = get_sample_for_discovery(df)
    categories = discover_categories(sample, cache, force_rediscover=force_rediscover)
    pt.update(message=f"Ditemukan {len(categories)} kategori semantik")

    # ── Phase 3: Semantic Analysis ───────────────────────────────────────────
    print("\n[Phase 3/4] 🤖 Semantic Analysis")
    newly_processed = run_analysis(df, categories, cache, batch_size=batch_size)

    # ── Phase 4: Output ──────────────────────────────────────────────────────
    pt.update(phase="output", message="Menulis output Excel dan dashboard...")
    print("\n[Phase 4/4] 📊 Output Generation")
    df_merged = merge_results(df, cache)
    total_processed = cache.get_total_processed()

    excel_path = None
    if output_excel:
        excel_path = write_excel(df_merged)

    json_path = write_json_for_dashboard(df_merged, cache)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  ✅ Pipeline selesai dalam {elapsed:.1f}s")
    print(f"  📊 Total tiket: {total_tickets:,}")
    print(f"  🤖 Baru dianalisa: {newly_processed:,}")
    print(f"  💾 Total di cache: {total_processed:,}")
    if excel_path:
        print(f"  📁 Excel: {excel_path}")
    print(f"  🌐 Dashboard data: {json_path}")
    print("=" * 60)

    pt.finish(
        success=True,
        message=f"Selesai! {total_processed:,} tiket teranalisa dalam {elapsed:.0f} detik"
    )

    return {
        "total_tickets": total_tickets,
        "newly_processed": newly_processed,
        "total_processed": total_processed,
        "elapsed_seconds": elapsed,
        "excel_path": str(excel_path) if excel_path else None,
        "json_path": str(json_path),
    }


def get_status(cache: CacheManager = None) -> dict:
    """Get current processing status from cache."""
    if cache is None:
        cache = CacheManager(DB_PATH)

    total_processed = cache.get_total_processed()
    categories = cache.get_categories()
    file_statuses = cache.get_all_file_status()

    return {
        "total_processed": total_processed,
        "categories_discovered": len(categories) if categories else 0,
        "files": file_statuses,
    }
