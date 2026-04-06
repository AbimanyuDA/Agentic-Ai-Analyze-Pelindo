"""
src/dashboard/app.py - Flask Web Dashboard
Serves the analysis dashboard with REST API endpoints.

Endpoints:
  GET  /                      → Dashboard HTML
  GET  /api/stats             → Overall stats
  GET  /api/trends            → Monthly category trend
  GET  /api/tickets           → Paginated ticket list (with filters)
  GET  /api/categories        → Discovered categories from cache
  GET  /api/subcategories     → Sub-category breakdown
  GET  /api/filter_options    → Dropdown values
  GET  /api/monthly           → Monthly recap table
  GET  /api/progress          → Background analysis progress
  POST /api/upload            → Upload new Excel file & trigger analysis
  GET  /api/export/excel      → Download latest analysis Excel
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime

from flask import Flask, render_template, jsonify, request, send_file

from src.utils.config import OUTPUT_DIR, DASHBOARD_PORT, DB_PATH, INPUT_DIR, GROQ_API_KEY, GROQ_MODEL_ANALYSIS
from src.utils.cache_manager import CacheManager
from src.utils import progress_tracker as pt

try:
    from groq import Groq as _Groq
    _groq_client = _Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
except Exception:
    _groq_client = None

# Cache for discovered new categories (expensive LLM call)
_new_categories_cache: dict = {}
_new_categories_lock = threading.Lock()

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100 MB max upload

DASHBOARD_DATA_PATH = OUTPUT_DIR / "dashboard_data.json"
ALLOWED_EXTENSIONS = {"xlsx"}


# ── Helpers ─────────────────────────────────────────────────────────────────

def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _load_data() -> dict:
    """Load dashboard data from JSON file."""
    if not DASHBOARD_DATA_PATH.exists():
        return {"records": [], "total_tickets": 0, "generated_at": ""}
    with open(DASHBOARD_DATA_PATH, encoding="utf-8") as f:
        return json.load(f)


def _parse_date(raw: str):
    """Parse date string into datetime or None."""
    if not raw or raw in ("None", ""):
        return None
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y %H:%M:%S"]:
        try:
            return datetime.strptime(raw[:19], fmt)
        except ValueError:
            continue
    return None


# ── Background analysis thread ───────────────────────────────────────────────

def _run_analysis_background():
    """Run full pipeline in background thread after file upload."""
    try:
        from src.agents.orchestrator import run_full_pipeline
        run_full_pipeline()
    except Exception as e:
        pt.set_error(f"Error saat analisis: {str(e)}")


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── Upload ────────────────────────────────────────────────────────────────────

@app.route("/api/upload", methods=["POST"])
def api_upload():
    """Accept an .xlsx file upload and trigger background analysis."""
    if "file" not in request.files:
        return jsonify({"error": "Tidak ada file yang diunggah"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Nama file kosong"}), 400

    if not _allowed_file(file.filename):
        return jsonify({"error": "Hanya file .xlsx yang didukung"}), 400

    if pt.state["running"]:
        return jsonify({"error": "Analisis sedang berjalan, tunggu hingga selesai"}), 409

    # Save to Knowledge/input directory
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    save_path = INPUT_DIR / file.filename
    file.save(str(save_path))

    # Reset progress and start background analysis
    pt.reset(message=f"File '{file.filename}' diunggah, memulai analisis...")

    thread = threading.Thread(target=_run_analysis_background, daemon=True)
    thread.start()

    return jsonify({
        "success": True,
        "message": f"File '{file.filename}' berhasil diunggah. Analisis dimulai di background!",
        "filename": file.filename,
    })


# ── Progress ────────────────────────────────────────────────────────────────

@app.route("/api/progress")
def api_progress():
    """Return current analysis progress — reads from shared JSON file (cross-process safe)."""
    return jsonify(pt.load_from_file())


# ── Export Excel ─────────────────────────────────────────────────────────────

@app.route("/api/export/excel")
def api_export_excel():
    """Download the latest analysis Excel file, generating if needed."""
    # Check for existing Excel files
    excel_files = sorted(OUTPUT_DIR.glob("Incident_Analysis_*.xlsx"), reverse=True)

    if excel_files:
        latest = excel_files[0]
        return send_file(
            str(latest),
            as_attachment=True,
            download_name=latest.name,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # Generate fresh Excel from current cache
    try:
        data = _load_data()
        if not data["records"]:
            return jsonify({"error": "Belum ada data. Jalankan analisis terlebih dahulu."}), 404

        from src.agents.data_ingestion import load_excel_files, prepare_dataframe
        from src.agents.output_agent import merge_results, write_excel
        from src.utils.cache_manager import CacheManager

        cache = CacheManager(DB_PATH)
        df = load_excel_files(INPUT_DIR)
        df = prepare_dataframe(df)
        df_merged = merge_results(df, cache)
        excel_path = write_excel(df_merged)

        return send_file(
            str(excel_path),
            as_attachment=True,
            download_name=excel_path.name,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        return jsonify({"error": f"Gagal membuat Excel: {str(e)}"}), 500


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    """Overall statistics summary."""
    data = _load_data()
    records = data.get("records", [])

    if not records:
        return jsonify({"error": "No data available. Run analysis first."}), 404

    total = len(records)
    analyzed = sum(1 for r in records if r.get("kategori_utama", "") not in ["", "Tidak Terklasifikasi"])

    urgency_count = Counter(r.get("urgensi", "Low") for r in records)
    cat_count = Counter(r.get("kategori_utama", "N/A") for r in records)
    app_count = Counter(r.get("aplikasi_terkait", "N/A") for r in records)
    source_count = Counter(r.get("source_file", "unknown") for r in records)

    top_categories = [{"name": k, "count": v} for k, v in cat_count.most_common(15)]
    top_apps = [
        {"name": k, "count": v}
        for k, v in app_count.most_common(12)
        if k not in ("N/A", "", "None")
    ]

    return jsonify({
        "total_tickets": total,
        "analyzed_tickets": analyzed,
        "analysis_coverage": round(analyzed / total * 100, 1) if total else 0,
        "generated_at": data.get("generated_at", ""),
        "urgency": dict(urgency_count),
        "top_categories": top_categories,
        "top_applications": top_apps,
        "source_files": dict(source_count),
    })


# ── Trends ────────────────────────────────────────────────────────────────────

@app.route("/api/trends")
def api_trends():
    """Monthly category trend data for line chart."""
    data = _load_data()
    records = data.get("records", [])

    monthly = defaultdict(lambda: defaultdict(int))

    for r in records:
        dt = _parse_date(r.get("tiket_dibuat", ""))
        if not dt:
            continue
        month_key = dt.strftime("%Y-%m")
        cat = r.get("kategori_utama", "Lainnya") or "Lainnya"
        monthly[month_key][cat] += 1

    sorted_months = sorted(monthly.keys())
    cat_totals = Counter()
    for m in monthly.values():
        for cat, cnt in m.items():
            cat_totals[cat] += cnt

    top_cats = [c for c, _ in cat_totals.most_common(6)]

    return jsonify({
        "months": sorted_months,
        "categories": top_cats,
        "data": {
            cat: [monthly[m].get(cat, 0) for m in sorted_months]
            for cat in top_cats
        },
    })


# ── Monthly Recap ─────────────────────────────────────────────────────────────

@app.route("/api/monthly")
def api_monthly():
    """Full monthly recap table: per month breakdown."""
    data = _load_data()
    records = data.get("records", [])

    monthly = defaultdict(lambda: {
        "total": 0,
        "by_category": defaultdict(int),
        "by_urgency": defaultdict(int),
        "by_app": defaultdict(int),
        "critical_high": 0,
    })

    for r in records:
        dt = _parse_date(r.get("tiket_dibuat", ""))
        if not dt:
            continue
        month_key = dt.strftime("%Y-%m")
        cat = r.get("kategori_utama", "Lainnya") or "Lainnya"
        urg = r.get("urgensi", "Low") or "Low"
        appl = r.get("aplikasi_terkait", "N/A") or "N/A"

        monthly[month_key]["total"] += 1
        monthly[month_key]["by_category"][cat] += 1
        monthly[month_key]["by_urgency"][urg] += 1
        if appl not in ("N/A", ""):
            monthly[month_key]["by_app"][appl] += 1
        if urg in ("Critical", "High"):
            monthly[month_key]["critical_high"] += 1

    result = []
    prev_total = None
    for month in sorted(monthly.keys()):
        m = monthly[month]
        by_cat = dict(m["by_category"])
        by_urg = dict(m["by_urgency"])
        by_app = dict(m["by_app"])
        top_cat = max(by_cat, key=by_cat.get) if by_cat else "N/A"
        top_app = max(by_app, key=by_app.get) if by_app else "N/A"

        total = m["total"]
        growth = round((total - prev_total) / prev_total * 100, 1) if prev_total else None
        prev_total = total

        # Format month name in Indonesian
        try:
            dt_obj = datetime.strptime(month, "%Y-%m")
            month_names = ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun",
                           "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]
            month_label = f"{month_names[dt_obj.month - 1]} {dt_obj.year}"
        except Exception:
            month_label = month

        result.append({
            "month": month,
            "month_label": month_label,
            "total": total,
            "growth": growth,
            "by_category": by_cat,
            "by_urgency": by_urg,
            "critical_high": m["critical_high"],
            "top_category": top_cat,
            "top_app": top_app,
        })

    return jsonify(result)


# ── Tickets ───────────────────────────────────────────────────────────────────

@app.route("/api/tickets")
def api_tickets():
    """Paginated ticket list with filters."""
    data = _load_data()
    records = data.get("records", [])

    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    search = request.args.get("search", "").lower()
    kategori = request.args.get("kategori", "")
    urgensi = request.args.get("urgensi", "")
    aplikasi = request.args.get("aplikasi", "")
    source = request.args.get("source", "")

    filtered = records
    if search:
        filtered = [r for r in filtered if
                    search in (r.get("judul") or "").lower() or
                    search in (r.get("summary") or "").lower() or
                    search in (r.get("no_tiket") or "").lower() or
                    search in (r.get("root_cause") or "").lower()]
    if kategori:
        filtered = [r for r in filtered if r.get("kategori_utama") == kategori]
    if urgensi:
        filtered = [r for r in filtered if r.get("urgensi") == urgensi]
    if aplikasi:
        filtered = [r for r in filtered if r.get("aplikasi_terkait") == aplikasi]
    if source:
        filtered = [r for r in filtered if r.get("source_file") == source]

    total_filtered = len(filtered)
    start = (page - 1) * per_page
    page_records = filtered[start: start + per_page]

    slim = []
    for r in page_records:
        slim.append({
            "no_tiket": r.get("no_tiket", ""),
            "tiket_dibuat": r.get("tiket_dibuat", ""),
            "judul": r.get("judul", ""),
            "summary": r.get("summary", ""),
            "kategori_utama": r.get("kategori_utama", ""),
            "sub_kategori": r.get("sub_kategori", ""),
            "root_cause": r.get("root_cause", ""),
            "urgensi": r.get("urgensi", ""),
            "tags": r.get("tags", []),
            "aplikasi_terkait": r.get("aplikasi_terkait", ""),
            "status": r.get("status", ""),
            "source_file": r.get("source_file", ""),
        })

    return jsonify({
        "tickets": slim,
        "total": total_filtered,
        "page": page,
        "per_page": per_page,
        "total_pages": max(1, (total_filtered + per_page - 1) // per_page),
    })


# ── Categories & Sub-categories ───────────────────────────────────────────────

@app.route("/api/categories")
def api_categories():
    cache = CacheManager(DB_PATH)
    return jsonify(cache.get_categories() or [])


# ── Keyword-based suggestion engine for uncategorized tickets ───────────────
_HUMAN_ERROR_KEYWORDS = [
    "salah", "lupa", "tidak tahu", "tidak mengerti", "kesalahan", "prosedur",
    "user error", "pengguna", "keliru", "wrong", "mistake", "forgot", "forgot password",
    "reset password", "lupa password", "salah input", "salah masuk", "tidak sesuai",
    "tidak mengikuti", "tidak paham", "belum tahu", "salah klik", "human",
]
_BUG_KEYWORDS = [
    "error", "bug", "gagal", "tidak bisa", "tidak berfungsi", "tidak muncul", "crash",
    "exception", "500", "404", "timeout", "loading", "hang", "freeze", "blank",
    "tidak tersimpan", "submit", "failed", "failure", "sistem", "aplikasi mati",
    "tidak terbuka", "corrupt", "hilang", "duplikat data", "data salah", "kalkulasi",
]
_TOOLS_KEYWORDS = [
    "panduan", "cara", "bagaimana", "tutorial", "bantuan", "request", "akses",
    "hak akses", "tambah fitur", "informasi", "tanya", "konsultasi", "pertanyaan",
    "permohonan", "izin", "izin akses", "onboarding", "latihan", "training",
    "sosialisasi", "fitur baru", "upgrade", "verifikasi", "konfirmasi",
]


def _suggest_tipe(raw_tipe: str, sub_kategori: str, root_cause: str, summary: str) -> tuple[str, float, str]:
    """Returns (suggested_tipe, confidence 0-1, reasoning)."""
    text = " ".join([
        (raw_tipe or ""), (sub_kategori or ""),
        (root_cause or ""), (summary or ""),
    ]).lower()

    # Direct mapping if raw_tipe hints at one
    raw_lower = (raw_tipe or "").lower()
    if any(h in raw_lower for h in ["human", "error manusia", "user salah", "pengguna"]):
        return "Human Error", 0.85, f"Tipe AI '{raw_tipe}' mengindikasikan kesalahan pengguna"
    if any(b in raw_lower for b in ["bug", "aplikasi", "sistem", "teknis", "error"]):
        return "Bug Aplikasi", 0.80, f"Tipe AI '{raw_tipe}' mengindikasikan bug teknis"
    if any(t in raw_lower for t in ["tools", "knowledge", "panduan", "request", "akses"]):
        return "Tools & Knowledge", 0.80, f"Tipe AI '{raw_tipe}' mengindikasikan permintaan bantuan"

    # Keyword scoring
    he_score  = sum(1 for k in _HUMAN_ERROR_KEYWORDS if k in text)
    bug_score = sum(1 for k in _BUG_KEYWORDS if k in text)
    tk_score  = sum(1 for k in _TOOLS_KEYWORDS if k in text)
    total = he_score + bug_score + tk_score

    if total == 0:
        return "Bug Aplikasi", 0.35, "Tidak ada pola jelas; default ke Bug Aplikasi"

    scores = {"Human Error": he_score, "Bug Aplikasi": bug_score, "Tools & Knowledge": tk_score}
    best_tipe = max(scores, key=lambda k: scores[k])
    confidence = round(scores[best_tipe] / total, 2)

    reasons = {
        "Human Error":       "Kata kunci mengarah ke kesalahan / kurangnya pemahaman pengguna",
        "Bug Aplikasi":      "Kata kunci mengarah ke error teknis / malfungsi sistem",
        "Tools & Knowledge": "Kata kunci mengarah ke permintaan panduan / akses / informasi",
    }
    return best_tipe, confidence, reasons[best_tipe]


@app.route("/api/hierarchy")
def api_hierarchy():
    """
    Hierarchical clustering for all 3 types + uncategorized:
      Level 1 : tipe_masalah  (Human Error | Bug Aplikasi | Tools & Knowledge | Perlu Pertimbangan)
      Level 2 : sub_kategori  (per tipe)
      Level 2b: kategori_bug  (extra for Bug Aplikasi, keyed from kategori_bug field)
    """
    data = _load_data()
    records = data.get("records", [])

    if not records:
        return jsonify({"error": "No data available"}), 404

    VALID_TIPE = {"Human Error", "Bug Aplikasi", "Tools & Knowledge"}

    tipe_counts:   dict[str, int]             = {}
    tipe_sub:      dict[str, dict[str, int]]  = {}  # tipe → sub → count
    tipe_sub_apps: dict[str, dict[str, dict[str, int]]] = {}  # tipe → sub → app → count
    tipe_urgency:  dict[str, dict[str, int]]  = {}
    tipe_apps:     dict[str, dict[str, int]]  = {}
    uncategorized: list[dict]                 = []  # tickets that don't fit neatly

    for r in records:
        raw_tipe = r.get("tipe_masalah") or r.get("kategori_utama") or ""
        app  = r.get("aplikasi_terkait") or "N/A"
        sub  = r.get("sub_kategori") or "-"
        urg  = r.get("urgensi") or "Low"
        bug_cat = r.get("kategori_bug") or "-"

        # Determine effective tipe
        if raw_tipe in VALID_TIPE:
            tipe = raw_tipe
        else:
            # Collect as uncategorized + generate AI suggestion
            suggested, confidence, reasoning = _suggest_tipe(
                raw_tipe,
                r.get("sub_kategori", ""),
                r.get("root_cause", ""),
                r.get("summary", ""),
            )
            conf_label = "Tinggi" if confidence >= 0.7 else "Sedang" if confidence >= 0.5 else "Rendah"
            uncategorized.append({
                "no_tiket":        r.get("no_tiket", ""),
                "judul":           r.get("judul", ""),
                "summary":         r.get("summary", ""),
                "root_cause":      r.get("root_cause", ""),
                "raw_tipe":        raw_tipe or "—",
                "sub_kategori":    sub,
                "urgensi":         urg,
                "aplikasi_terkait":app,
                "suggested_tipe":  suggested,
                "confidence":      confidence,
                "conf_label":      conf_label,
                "reasoning":       reasoning,
            })
            continue

        # Level 1 counts
        tipe_counts[tipe] = tipe_counts.get(tipe, 0) + 1

        # Level 2: sub_kategori per tipe
        tipe_sub.setdefault(tipe, {})
        tipe_sub[tipe][sub] = tipe_sub[tipe].get(sub, 0) + 1

        # Apps per sub (for all types)
        tipe_sub_apps.setdefault(tipe, {})
        tipe_sub_apps[tipe].setdefault(sub, {})
        if app not in ("N/A", "", "None"):
            tipe_sub_apps[tipe][sub][app] = tipe_sub_apps[tipe][sub].get(app, 0) + 1

        # Urgency per tipe
        tipe_urgency.setdefault(tipe, {})
        tipe_urgency[tipe][urg] = tipe_urgency[tipe].get(urg, 0) + 1

        # Top apps per tipe
        if app not in ("N/A", "", "None"):
            tipe_apps.setdefault(tipe, {})
            tipe_apps[tipe][app] = tipe_apps[tipe].get(app, 0) + 1

    total = len(records)
    categorized_total = sum(tipe_counts.values())

    # Build nodes for all 3 tipes
    tipe_order = ["Human Error", "Bug Aplikasi", "Tools & Knowledge"]
    nodes = []
    for tipe in tipe_order:
        count = tipe_counts.get(tipe, 0)
        if count == 0:
            continue

        sub_items = sorted(tipe_sub.get(tipe, {}).items(), key=lambda x: -x[1])
        top_apps  = sorted((tipe_apps.get(tipe) or {}).items(), key=lambda x: -x[1])[:5]

        # Build sub-clusters (works for ALL 3 types)
        clusters = []
        for sub_name, sub_count in sub_items:
            if sub_name == "-":
                continue
            top_sub_apps = sorted(
                (tipe_sub_apps.get(tipe, {}).get(sub_name) or {}).items(),
                key=lambda x: -x[1]
            )[:3]
            clusters.append({
                "name": sub_name,
                "count": sub_count,
                "pct": round(sub_count / count * 100, 1) if count else 0,
                "top_apps": [{"name": k, "count": v} for k, v in top_sub_apps],
            })

        node = {
            "tipe":        tipe,
            "count":       count,
            "pct":         round(count / total * 100, 1) if total else 0,
            "urgency":     tipe_urgency.get(tipe, {}),
            "top_apps":    [{"name": k, "count": v} for k, v in top_apps],
            "sub_kategori":[{"name": k, "count": v} for k, v in sub_items[:15]],
            "clusters":    clusters,  # detailed sub-clusters with apps for ALL types
        }
        nodes.append(node)

    # Summary stats
    he_count  = tipe_counts.get("Human Error", 0)
    bug_count = tipe_counts.get("Bug Aplikasi", 0)
    tk_count  = tipe_counts.get("Tools & Knowledge", 0)
    unc_count = len(uncategorized)

    return jsonify({
        "total":               total,
        "categorized":         categorized_total,
        "uncategorized_count": unc_count,
        "summary": {
            "human_error":     {"count": he_count,  "pct": round(he_count  / total * 100, 1) if total else 0},
            "bug_aplikasi":    {"count": bug_count,  "pct": round(bug_count / total * 100, 1) if total else 0},
            "tools_knowledge": {"count": tk_count,   "pct": round(tk_count  / total * 100, 1) if total else 0},
            "uncategorized":   {"count": unc_count,  "pct": round(unc_count / total * 100, 1) if total else 0},
        },
        "nodes":            nodes,
        "uncategorized":    uncategorized[:200],  # cap at 200 for perf
    })


@app.route("/api/uncategorized/insights")
def api_uncategorized_insights():
    """Deep insight for uncategorized tickets — suggestion distribution + top sub-clusters."""
    data = _load_data()
    records = data.get("records", [])
    VALID_TIPE = {"Human Error", "Bug Aplikasi", "Tools & Knowledge"}

    unc_list = []
    for r in records:
        raw_tipe = r.get("tipe_masalah") or r.get("kategori_utama") or ""
        if raw_tipe in VALID_TIPE:
            continue
        suggested, confidence, reasoning = _suggest_tipe(
            raw_tipe,
            r.get("sub_kategori", ""),
            r.get("root_cause", ""),
            r.get("summary", ""),
        )
        unc_list.append({
            "no_tiket":         r.get("no_tiket", ""),
            "judul":            r.get("judul", ""),
            "summary":          r.get("summary", ""),
            "root_cause":       r.get("root_cause", ""),
            "raw_tipe":         raw_tipe or "—",
            "sub_kategori":     r.get("sub_kategori") or "-",
            "urgensi":          r.get("urgensi") or "Low",
            "aplikasi_terkait": r.get("aplikasi_terkait") or "N/A",
            "suggested_tipe":   suggested,
            "confidence":       confidence,
            "conf_label":       "Tinggi" if confidence >= 0.7 else "Sedang" if confidence >= 0.5 else "Rendah",
            "reasoning":        reasoning,
        })

    if not unc_list:
        return jsonify({"total": 0, "groups": [], "tickets": []})

    # Group by suggested_tipe
    from collections import defaultdict, Counter
    groups: dict = defaultdict(lambda: {"count": 0, "high_conf": 0, "sub_clusters": Counter(), "apps": Counter()})
    for u in unc_list:
        g = groups[u["suggested_tipe"]]
        g["count"] += 1
        if u["confidence"] >= 0.7:
            g["high_conf"] += 1
        g["sub_clusters"][u["sub_kategori"]] += 1
        if u["aplikasi_terkait"] not in ("N/A", "", "None"):
            g["apps"][u["aplikasi_terkait"]] += 1

    total_unc = len(unc_list)
    groups_out = []
    for tipe in ["Human Error", "Bug Aplikasi", "Tools & Knowledge"]:
        g = groups.get(tipe)
        if not g:
            continue
        groups_out.append({
            "suggested_tipe": tipe,
            "count":          g["count"],
            "pct":            round(g["count"] / total_unc * 100, 1),
            "high_conf":      g["high_conf"],
            "high_conf_pct":  round(g["high_conf"] / g["count"] * 100, 1) if g["count"] else 0,
            "top_sub_clusters": [{"name": k, "count": v} for k, v in g["sub_clusters"].most_common(8)],
            "top_apps":         [{"name": k, "count": v} for k, v in g["apps"].most_common(5)],
        })

    # Sort tickets: high confidence first
    unc_list.sort(key=lambda x: -x["confidence"])

    return jsonify({
        "total":   total_unc,
        "groups":  groups_out,
        "tickets": unc_list[:300],
    })





@app.route("/api/uncategorized/discover")
def api_uncategorized_discover():
    """
    Analyze uncategorized tickets and propose NEW categories beyond the 3 main ones.
    Step 1: Statistical clustering by raw_tipe + sub_kategori patterns.
    Step 2: LLM call to name, describe, and group proposed new categories.
    Results are cached to avoid repeated LLM calls.
    """
    force_refresh = request.args.get("refresh", "0") == "1"

    with _new_categories_lock:
        if _new_categories_cache and not force_refresh:
            return jsonify(_new_categories_cache)

    data = _load_data()
    records = data.get("records", [])
    VALID_TIPE = {"Human Error", "Bug Aplikasi", "Tools & Knowledge"}

    # Collect uncategorized tickets with their AI-assigned fields
    unc = []
    raw_tipe_counts: Counter = Counter()
    sub_counts: Counter = Counter()

    for r in records:
        raw_tipe = r.get("tipe_masalah") or r.get("kategori_utama") or ""
        if raw_tipe in VALID_TIPE:
            continue
        sub = r.get("sub_kategori") or "-"
        raw_tipe_counts[raw_tipe] += 1
        sub_counts[sub] += 1
        unc.append({
            "no_tiket":    r.get("no_tiket", ""),
            "raw_tipe":    raw_tipe,
            "sub":         sub,
            "summary":     (r.get("summary") or "")[:120],
            "root_cause":  (r.get("root_cause") or "")[:100],
            "urgensi":     r.get("urgensi") or "Low",
            "aplikasi":    r.get("aplikasi_terkait") or "N/A",
        })

    if not unc:
        result = {"total": 0, "statistical_clusters": [], "new_categories": [], "method": "none"}
        with _new_categories_lock:
            _new_categories_cache.update(result)
        return jsonify(result)

    # ── Step 1: Statistical clusters from raw_tipe ──────────────────
    # raw_tipe values that are NOT in VALID_TIPE are "AI-proposed new categories"
    statistical_clusters = []
    for raw, count in raw_tipe_counts.most_common(25):
        if not raw or raw == "—":
            continue
        # Get sub-categories within this raw_tipe cluster
        sub_in_cluster = Counter(
            t["sub"] for t in unc if t["raw_tipe"] == raw and t["sub"] != "-"
        )
        apps_in_cluster = Counter(
            t["aplikasi"] for t in unc
            if t["raw_tipe"] == raw and t["aplikasi"] not in ("N/A", "", "None")
        )
        examples = [t for t in unc if t["raw_tipe"] == raw][:3]
        statistical_clusters.append({
            "raw_tipe":  raw,
            "count":     count,
            "pct":       round(count / len(unc) * 100, 1),
            "top_subs":  [{"name": k, "count": v} for k, v in sub_in_cluster.most_common(5)],
            "top_apps":  [{"name": k, "count": v} for k, v in apps_in_cluster.most_common(4)],
            "examples":  examples,
        })

    # Also cluster by sub_kategori for uncategorized (independent view)
    sub_clusters = []
    for sub, count in sub_counts.most_common(20):
        if sub == "-":
            continue
        sub_clusters.append({"name": sub, "count": count})

    # ── Step 2: LLM call to synthesize NEW category proposals ───────
    new_categories = []
    llm_available = _groq_client is not None

    if llm_available and statistical_clusters:
        # Build compact prompt from statistical data
        cluster_summary = "\n".join([
            f'- "{c["raw_tipe"]}" ({c["count"]} tiket): {", ".join(s["name"] for s in c["top_subs"][:3])}'
            for c in statistical_clusters[:20]
        ])
        # Sample of summaries for context
        sample_summaries = "\n".join([
            f'  [{t["raw_tipe"]}] {t["summary"]}'
            for t in unc[:60]
            if t["summary"]
        ])

        prompt = f"""Kamu adalah konsultan IT Service Management untuk PT Pelindo Indonesia.

Data tiket helpdesk IT yang belum bisa dikategorikan ke 3 kategori utama (Human Error, Bug Aplikasi, Tools & Knowledge):

POLA YANG DITEMUKAN SECARA STATISTIK:
{cluster_summary}

CONTOH SUMMARY TIKET:
{sample_summaries}

TUGAS:
Berdasarkan pola di atas, usulkan 3-6 KATEGORI BARU yang relevan untuk konteks Pelindo (perusahaan pelabuhan/logistik).
Kategori baru ini HARUS:
1. Berbeda dari 3 kategori utama yang sudah ada
2. Menggambarkan pola nyata dari data
3. Relevan untuk manajemen insiden IT di perusahaan pelabuhan
4. Memiliki nama yang jelas dan mudah dipahami tim IT

Respond HANYA dengan JSON array:
[
  {{
    "nama_kategori": "...",
    "deskripsi": "Penjelasan singkat 1-2 kalimat tentang jenis tiket ini",
    "karakteristik": ["ciri 1", "ciri 2", "ciri 3"],
    "raw_tipe_terkait": ["raw_tipe dari data yang masuk ke kategori ini"],
    "jumlah_estimasi": 0,
    "rekomendasi_aksi": "Apa yang sebaiknya dilakukan tim IT untuk tipe tiket ini"
  }}
]

Hanya JSON, tidak ada teks lain."""

        try:
            resp = _groq_client.chat.completions.create(
                model=GROQ_MODEL_ANALYSIS,
                messages=[
                    {"role": "system", "content": "Kamu konsultan ITSM. Respond JSON valid only."},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.3,
                max_tokens=2000,
            )
            raw_resp = resp.choices[0].message.content.strip()
            # Parse JSON
            import re as _re
            json_match = _re.search(r'\[[\s\S]*\]', raw_resp)
            if json_match:
                parsed = json.loads(json_match.group())
                if isinstance(parsed, list):
                    # Enrich with ticket counts based on raw_tipe_terkait
                    for cat in parsed:
                        related = cat.get("raw_tipe_terkait", [])
                        count = sum(raw_tipe_counts.get(rt, 0) for rt in related)
                        cat["jumlah_estimasi"] = count or cat.get("jumlah_estimasi", 0)
                    new_categories = parsed
        except Exception as e:
            new_categories = [{"error": str(e), "nama_kategori": "LLM Error", "deskripsi": str(e)}]

    result = {
        "total":                len(unc),
        "method":               "llm+statistical" if llm_available else "statistical",
        "statistical_clusters": statistical_clusters,
        "sub_clusters":         sub_clusters,
        "new_categories":       new_categories,
        "llm_available":        llm_available,
        "generated_at":         datetime.now().isoformat(),
    }

    with _new_categories_lock:
        _new_categories_cache.clear()
        _new_categories_cache.update(result)

    return jsonify(result)


@app.route("/api/subcategories")

def api_subcategories():
    data = _load_data()
    records = data.get("records", [])
    kategori = request.args.get("kategori", "")

    filtered = [r for r in records if r.get("kategori_utama") == kategori] if kategori else records
    sub_count = Counter(r.get("sub_kategori", "-") for r in filtered)
    return jsonify([{"name": k, "count": v} for k, v in sub_count.most_common(15)])


@app.route("/api/filter_options")
def api_filter_options():
    data = _load_data()
    records = data.get("records", [])

    categories = sorted(set(r.get("kategori_utama", "") for r in records if r.get("kategori_utama")))
    applications = sorted(set(
        r.get("aplikasi_terkait", "") for r in records
        if r.get("aplikasi_terkait") and r.get("aplikasi_terkait") not in ("N/A", "")
    ))[:30]
    sources = sorted(set(r.get("source_file", "") for r in records if r.get("source_file")))

    return jsonify({
        "categories": categories,
        "urgencies": ["Critical", "High", "Medium", "Low"],
        "applications": applications,
        "sources": sources,
    })


# ── Server start ──────────────────────────────────────────────────────────────

def run_dashboard():
    """Start the Flask dashboard server."""
    print(f"\n🌐 Dashboard running at http://localhost:{DASHBOARD_PORT}")
    print("   Tekan Ctrl+C untuk berhenti\n")
    app.run(host="0.0.0.0", port=DASHBOARD_PORT, debug=False)
