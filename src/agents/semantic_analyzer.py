"""
src/agents/semantic_analyzer.py - AI Analysis Agent (Multi-Key Parallel)
Uses multiple Groq API keys simultaneously for maximum throughput.
Each API key runs in its own thread with independent rate limits.
"""
from __future__ import annotations
import json
import time
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty

from groq import Groq, RateLimitError, APIConnectionError

from src.utils.config import GROQ_API_KEY, GROQ_API_KEYS, GROQ_MODEL_ANALYSIS, BATCH_SIZE
from src.utils.cache_manager import CacheManager
from src.agents.category_schema import format_categories_for_prompt


ANALYSIS_PROMPT = """Kamu analis IT Helpdesk PT Pelindo Indonesia.
Analisa tiket berikut. Field: id=no_tiket, j=judul, d=deskripsi, r=resolved_notes.
Prioritaskan d dan r untuk memahami masalah (j sering tidak akurat).

TAXONOMY (WAJIB):
{categories}

Panduan:
- Human Error: salah prosedur/input, lupa password, tidak tahu cara pakai. Solusi: edukasi, reset, koreksi.
- Bug Aplikasi: error teknis meski cara pakai benar, ada error message, fitur tidak jalan. Solusi: fix bug, restart, eskalasi dev.
- Tools & Knowledge: butuh panduan, minta akses/fitur baru, pertanyaan prosedural. Bukan error.
Untuk Bug Aplikasi: isi kategori_bug. Lainnya: isi "-".

TIKET ({n}):
{tickets_json}

Output JSON array PERSIS format ini (no_tiket dari field id):
[{{"no_tiket":"","tipe_masalah":"Human Error|Bug Aplikasi|Tools & Knowledge","kategori_bug":"-","sub_kategori":"","root_cause":"","summary":"","urgensi":"Low|Medium|High|Critical","tags":[],"aplikasi_terkait":""}}]

Hanya JSON, tidak ada teks lain."""


# ── Single batch analysis ────────────────────────────────────────────────────

def analyze_batch(
    tickets: list[dict],
    categories: list[dict],
    client: Groq,
    max_retries: int = 4,
) -> list[dict]:
    """Analyze a batch of tickets with one Groq client. Returns list of analysis results."""
    tickets_for_prompt = []
    for t in tickets:
        tickets_for_prompt.append({
            "id": t.get("no_tiket", ""),
            "j":  t.get("judul", "")[:60],
            "d":  t.get("deskripsi", "")[:150],
            "r":  t.get("resolved_notes", "")[:100],
        })

    categories_text = format_categories_for_prompt(categories)
    tickets_json = json.dumps(tickets_for_prompt, ensure_ascii=False, separators=(',', ':'))

    prompt = ANALYSIS_PROMPT.format(
        categories=categories_text,
        n=len(tickets),
        tickets_json=tickets_json,
    )

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL_ANALYSIS,
                messages=[
                    {"role": "system", "content": "Analis IT. Respond JSON array valid only."},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.1,
                max_tokens=2000,
            )

            raw = response.choices[0].message.content.strip()
            results = _parse_json_response(raw, tickets)
            # Map 'id' back to 'no_tiket' if AI returned id field
            for r in results:
                if not r.get('no_tiket') and r.get('id'):
                    r['no_tiket'] = r.pop('id')
            return results

        except RateLimitError:
            wait = 60 * (attempt + 1)
            print(f"\n   ⏳ Rate limit — tunggu {wait}s ...")
            time.sleep(wait)
        except APIConnectionError:
            wait = 10 * (attempt + 1)
            print(f"\n   🔌 Koneksi error — retry {attempt+1}/{max_retries} ...")
            time.sleep(wait)
        except Exception as e:
            err_str = str(e)
            if '413' in err_str or 'too large' in err_str.lower():
                print(f"\n   ⚠️  413 token too large — fallback untuk batch ini")
                return _make_fallback_results(tickets)
            elif 'json' in err_str.lower():
                print(f"\n   ⚠️  JSON error attempt {attempt+1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
            else:
                print(f"\n   ❌ Error attempt {attempt+1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)

    return _make_fallback_results(tickets)


# ── JSON parsing helpers ─────────────────────────────────────────────────────

def _parse_json_response(raw: str, tickets: list[dict]) -> list[dict]:
    """Extract and parse JSON from LLM response, with fallback."""
    clean = raw
    if "```" in clean:
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", clean)
        if match:
            clean = match.group(1)

    try:
        results = json.loads(clean)
        if isinstance(results, list):
            return _validate_results(results, tickets)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\[[\s\S]*\]", clean)
    if match:
        try:
            results = json.loads(match.group())
            if isinstance(results, list):
                return _validate_results(results, tickets)
        except json.JSONDecodeError:
            pass

    results = []
    for t in tickets:
        pat = r'\{[^{}]*"(?:no_tiket|id)"\s*:\s*"' + re.escape(t["no_tiket"]) + r'"[\s\S]*?\}'
        m = re.search(pat, clean)
        if m:
            try:
                obj = json.loads(m.group())
                results.append(obj)
                continue
            except Exception:
                pass
        results.append(_fallback_one(t))

    return results


VALID_TIPE = {"Human Error", "Bug Aplikasi", "Tools & Knowledge"}


def _validate_results(results: list, tickets: list[dict]) -> list[dict]:
    required_fields = {
        "no_tiket": "",
        "tipe_masalah": "Bug Aplikasi",
        "kategori_bug": "-",
        "sub_kategori": "-",
        "root_cause": "",
        "summary": "",
        "urgensi": "Low",
        "tags": [],
        "aplikasi_terkait": "N/A",
        "kategori_utama": "",
    }
    validated = []
    for r in results:
        if not isinstance(r, dict):
            continue
        # id → no_tiket mapping
        if not r.get("no_tiket") and r.get("id"):
            r["no_tiket"] = r.pop("id")
        v = {**required_fields, **r}
        tipe = v.get("tipe_masalah", "")
        if tipe not in VALID_TIPE:
            tipe = _infer_tipe(tipe, v.get("kategori_utama", ""))
        v["tipe_masalah"] = tipe
        v["kategori_utama"] = v["tipe_masalah"]
        if v["tipe_masalah"] != "Bug Aplikasi":
            v["kategori_bug"] = "-"
        if not isinstance(v["tags"], list):
            v["tags"] = [str(v["tags"])]
        validated.append(v)
    return validated


def _infer_tipe(raw_tipe: str, raw_kategori: str) -> str:
    combined = (raw_tipe + " " + raw_kategori).lower()
    if any(w in combined for w in ["human", "salah", "lupa", "pengguna"]):
        return "Human Error"
    if any(w in combined for w in ["bug", "error", "crash", "gagal", "teknis"]):
        return "Bug Aplikasi"
    if any(w in combined for w in ["tools", "knowledge", "panduan", "request", "akses"]):
        return "Tools & Knowledge"
    return "Bug Aplikasi"


def _fallback_one(ticket: dict) -> dict:
    return {
        "no_tiket": ticket.get("no_tiket", ""),
        "tipe_masalah": "Bug Aplikasi",
        "kategori_bug": "-",
        "kategori_utama": "Bug Aplikasi",
        "sub_kategori": "-",
        "root_cause": "Gagal dianalisa otomatis",
        "summary": ticket.get("judul", ""),
        "urgensi": "Low",
        "tags": [],
        "aplikasi_terkait": "N/A",
    }


def _make_fallback_results(tickets: list[dict]) -> list[dict]:
    return [_fallback_one(t) for t in tickets]


# ── Parallel multi-key worker ────────────────────────────────────────────────

def _worker(
    worker_id: int,
    api_key: str,
    batch_queue: Queue,
    categories: list[dict],
    cache: CacheManager,
    write_lock: threading.Lock,
    counter: dict,
    counter_lock: threading.Lock,
    total_batches: int,
    n_workers: int,
):
    """Single worker thread: pulls batches from queue and processes with its own API key."""
    client = Groq(api_key=api_key)
    wname = f"K{worker_id+1}"
    consecutive_errors = 0

    while True:
        try:
            batch_idx, batch = batch_queue.get(timeout=10)
        except Empty:
            break

        try:
            results = analyze_batch(batch, categories, client)
            if results:
                with write_lock:
                    cache.save_batch_results(results, model=GROQ_MODEL_ANALYSIS)
                with counter_lock:
                    counter["done"] += len(results)
                    counter["batches_done"] += 1
                    done = counter["done"]
                    total = counter["total"]
                    pct = done / total * 100 if total else 0
                    b_done = counter["batches_done"]
                print(
                    f"   [{wname}] {done:,}/{total:,} ({pct:.0f}%) | batch {b_done}/{total_batches}",
                    flush=True
                )
                consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            wait = min(60 * consecutive_errors, 300)
            print(f"\n   [{wname}] error #{consecutive_errors}: {e} — wait {wait}s")
            time.sleep(wait)
        finally:
            batch_queue.task_done()

        # 0.3s sleep — keys are independent orgs, no shared rate limit
        time.sleep(0.3)


# ── Main entry ───────────────────────────────────────────────────────────────

def run_analysis(
    df,
    categories: list[dict],
    cache: CacheManager,
    batch_size: int = BATCH_SIZE,
) -> int:
    """
    Main analysis loop — uses all available API keys in parallel.
    Each key runs independently in its own thread with its own rate limit bucket.
    """
    from src.agents.data_ingestion import df_to_ticket_list, batch_tickets
    from src.utils import progress_tracker as pt

    # Get pending tickets
    processed_ids = cache.get_processed_ids()
    all_tickets = df_to_ticket_list(df)
    pending = [t for t in all_tickets if t["no_tiket"] not in processed_ids]

    if not pending:
        print("✅ Semua tiket sudah dianalisa sebelumnya!")
        pt.update(phase="done", message="Semua tiket sudah dianalisa")
        return 0

    total = len(pending)
    n_workers = len(GROQ_API_KEYS)

    print(f"\n🚀 Parallel Analysis — {n_workers} worker × {n_workers} API key")
    print(f"   Model   : {GROQ_MODEL_ANALYSIS}")
    print(f"   Pending : {total:,} tiket")
    print(f"   Batch   : {batch_size} tiket/batch")

    batches = list(batch_tickets(pending, batch_size))
    total_batches = len(batches)
    est_min = total_batches / (n_workers * 1.5) / 60
    print(f"   Batches : {total_batches:,} | Est. ~{est_min:.0f} menit\n")

    pt.update(total=total, phase="analyzing",
              message=f"Menganalisa {total:,} tiket dengan {n_workers} workers paralel...")

    # Thread-safe structures
    batch_queue: Queue = Queue()
    for idx, b in enumerate(batches):
        batch_queue.put((idx, b))

    write_lock   = threading.Lock()
    counter_lock = threading.Lock()
    counter = {"done": 0, "total": total, "batches_done": 0}

    start_time = time.time()

    # Launch workers
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = []
        for i, api_key in enumerate(GROQ_API_KEYS):
            f = pool.submit(
                _worker,
                i, api_key, batch_queue, categories,
                cache, write_lock, counter, counter_lock,
                total_batches, n_workers
            )
            futures.append(f)
            # No extra stagger here — workers handle it internally

        # Monitor progress every 15 seconds
        while not batch_queue.empty() or any(not f.done() for f in futures):
            time.sleep(15)
            with counter_lock:
                done = counter["done"]
                pct  = done / total * 100 if total else 0
            elapsed = time.time() - start_time
            speed = done / elapsed if elapsed > 0 else 0
            remaining = (total - done) / speed / 60 if speed > 0 else 0
            pt.update(
                processed=done,
                message=(
                    f"{done:,}/{total:,} tiket ({pct:.0f}%) | "
                    f"{speed:.1f} tiket/s | sisa ~{remaining:.0f} mnt"
                )
            )

        batch_queue.join()

    elapsed = time.time() - start_time
    with counter_lock:
        newly_processed = counter["done"]

    print(f"\n\n✅ Selesai! {newly_processed:,} tiket dianalisa dalam {elapsed/60:.1f} menit")
    print(f"   Throughput: {newly_processed/elapsed:.1f} tiket/detik")
    return newly_processed
