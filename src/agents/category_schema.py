"""
src/agents/category_schema.py - Category Discovery Agent
Uses a FIXED 3-level taxonomy for structured clustering:
  Level 1: Human Error | Bug Aplikasi | Tools & Knowledge
  Level 2 (Bug Aplikasi): sub-cluster by error type
"""
from __future__ import annotations
import json
import time
from groq import Groq

from src.utils.config import GROQ_API_KEY, GROQ_MODEL_DISCOVERY
from src.utils.cache_manager import CacheManager


# ── Fixed Taxonomy ───────────────────────────────────────────────────────────
FIXED_TAXONOMY = [
    {
        "kategori_utama": "Human Error",
        "deskripsi": "Masalah yang disebabkan oleh kesalahan manusia: salah input, salah prosedur, kurang pengetahuan penggunaan sistem, lupa password, salah konfigurasi oleh user",
        "sub_kategori": [
            "Salah Input Data",
            "Prosedur Tidak Diikuti",
            "Lupa Password / Reset Akses",
            "Kesalahan Penggunaan Fitur",
            "Data Terhapus / Termodifikasi Tidak Sengaja",
            "Permintaan Koreksi Data",
        ],
        "contoh_keywords": ["lupa password", "salah input", "tidak tahu cara", "minta reset", "terhapus tidak sengaja"],
        "tipe": "human_error",
    },
    {
        "kategori_utama": "Bug Aplikasi",
        "deskripsi": "Masalah yang disebabkan oleh bug/defect pada sistem atau aplikasi: error teknis, crash, fitur tidak berjalan sesuai ekspektasi meski digunakan dengan benar",
        "sub_kategori": [
            "Error Submit / Gagal Simpan",
            "Crash / Aplikasi Tidak Bisa Dibuka",
            "Error Kalkulasi / Perhitungan Salah",
            "Data Tidak Sinkron / Tidak Muncul",
            "Timeout / Performa Lambat",
            "Error Notifikasi / Workflow",
            "Bug Laporan / Export",
            "Error Integrasi Antar Sistem",
            "Error Tampilan / UI Bermasalah",
            "Error Permission / Hak Akses Teknis",
        ],
        "contoh_keywords": ["error", "bug", "gagal submit", "tidak bisa simpan", "crash", "tidak muncul", "timeout", "kalkulasi salah"],
        "tipe": "bug_aplikasi",
    },
    {
        "kategori_utama": "Tools & Knowledge",
        "deskripsi": "Permintaan informasi, panduan penggunaan, request fitur baru, pertanyaan cara kerja sistem, request akses atau konfigurasi yang bersifat wajar/prosedural",
        "sub_kategori": [
            "Panduan Penggunaan Fitur",
            "Request Akses / Hak Akses Baru",
            "Request Fitur / Perubahan Sistem",
            "Instalasi / Setup Perangkat Baru",
            "Pertanyaan Prosedur IT",
            "Permintaan Data / Laporan Khusus",
            "Konfigurasi & Setting Sistem",
        ],
        "contoh_keywords": ["bagaimana cara", "minta akses", "request fitur", "mohon bantu", "instalasi", "setup", "panduan"],
        "tipe": "tools_knowledge",
    },
]


def discover_categories(
    ticket_sample: list[dict],
    cache: CacheManager,
    force_rediscover: bool = False,
) -> list[dict]:
    """
    Return the fixed 3-level taxonomy (no LLM call needed for discovery).
    We always use the fixed taxonomy — optionally save to cache for compatibility.
    """
    if not force_rediscover:
        cached = cache.get_categories()
        # Only use cache if it contains our fixed taxonomy structure
        if cached and any(c.get("tipe") for c in cached):
            print(f"✅ Taxonomy tetap sudah di-cache ({len(cached)} kategori utama)")
            return cached

    print(f"✅ Menggunakan taxonomy tetap 3-level (Human Error | Bug Aplikasi | Tools & Knowledge)")
    for cat in FIXED_TAXONOMY:
        subs = ", ".join(cat.get("sub_kategori", [])[:3])
        print(f"   📁 {cat['kategori_utama']}: {subs}...")

    cache.save_categories(FIXED_TAXONOMY)
    return FIXED_TAXONOMY


def format_categories_for_prompt(categories: list[dict]) -> str:
    """Format taxonomy as a compact string for the analysis prompt (token-efficient)."""
    lines = []
    for cat in categories:
        subs = "|".join(cat.get("sub_kategori", []))
        lines.append(f"[{cat['kategori_utama']}] Sub: {subs}")
    return "\n".join(lines)


def get_bug_subclusters() -> list[str]:
    """Return the list of Bug Aplikasi sub-clusters."""
    for cat in FIXED_TAXONOMY:
        if cat.get("tipe") == "bug_aplikasi":
            return cat.get("sub_kategori", [])
    return []


def _default_categories() -> list[dict]:
    return FIXED_TAXONOMY
