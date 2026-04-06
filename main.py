#!/usr/bin/env python3
"""
main.py - Pelindo AI Incident Analysis System
Entry point for the analysis pipeline and dashboard.

Usage:
  # Analisa semua tiket (pakai cache, bisa dilanjutkan)
  python main.py analyze

  # Test dengan 50 tiket pertama
  python main.py analyze --test

  # Paksa discover ulang kategori (jika ingin refresh)
  python main.py analyze --rediscover

  # Jalankan web dashboard
  python main.py dashboard

  # Cek status analisis
  python main.py status
"""
import sys
import argparse


def cmd_analyze(args):
    from src.agents.orchestrator import run_full_pipeline
    run_full_pipeline(
        force_rediscover=args.rediscover,
        max_tickets=50 if args.test else None,
        output_excel=True,
    )


def cmd_dashboard(args):
    from src.dashboard.app import run_dashboard
    run_dashboard()


def cmd_status(args):
    from src.utils.cache_manager import CacheManager
    from src.utils.config import DB_PATH
    from src.agents.orchestrator import get_status

    cache = CacheManager(DB_PATH)
    status = get_status(cache)

    print("\n📊 Status Analisis")
    print(f"   Tiket teranalisa : {status['total_processed']:,}")
    print(f"   Kategori         : {status['categories_discovered']}")
    if status["files"]:
        print("\n📁 File Status:")
        for f in status["files"]:
            print(f"   • {f['filename']}: {f['processed_rows']}/{f['total_rows']} rows ({f['last_updated']})")
    else:
        print("   Belum ada file diproses")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Pelindo AI Incident Analysis System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    subparsers = parser.add_subparsers(dest="command", help="Pilih command")

    # analyze
    p_analyze = subparsers.add_parser("analyze", help="Jalankan analisis tiket")
    p_analyze.add_argument("--test", action="store_true", help="Test mode: hanya proses 50 tiket")
    p_analyze.add_argument("--rediscover", action="store_true", help="Paksa discover kategori ulang")

    # dashboard
    subparsers.add_parser("dashboard", help="Jalankan web dashboard")

    # status
    subparsers.add_parser("status", help="Cek status analisis")

    args = parser.parse_args()

    if args.command == "analyze":
        cmd_analyze(args)
    elif args.command == "dashboard":
        cmd_dashboard(args)
    elif args.command == "status":
        cmd_status(args)
    else:
        parser.print_help()
        print("\n💡 Tip: Mulai dengan: python main.py analyze --test")


if __name__ == "__main__":
    main()
