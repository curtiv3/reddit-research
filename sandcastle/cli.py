from __future__ import annotations

import argparse
import sys

from sandcastle.collector.run import run_collect
from sandcastle.config import load_config
from sandcastle.processor.run import run_process
from sandcastle.reddit.run import run_reddit
from sandcastle.common.io import count_file
from sandcastle.common.logging import setup_logging
from sandcastle.doctor import run_doctor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sandcastle")
    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect", help="Run web collectors")
    collect.add_argument("--config", required=True)

    reddit = sub.add_parser("reddit", help="Run Reddit anchor")
    reddit.add_argument("--config", required=True)

    process = sub.add_parser("process", help="Run processor")
    process.add_argument("--config", required=True)

    count = sub.add_parser("count", help="Count JSONL objects")
    count.add_argument("--file", required=True)

    doctor = sub.add_parser("doctor", help="Check config and environment")
    doctor.add_argument("--config", required=True)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_logging()

    if args.command == "collect":
        config = load_config(args.config)
        run_collect(config)
        return
    if args.command == "reddit":
        config = load_config(args.config)
        run_reddit(config)
        return
    if args.command == "process":
        config = load_config(args.config)
        run_process(config)
        return
    if args.command == "count":
        count_file(args.file)
        return
    if args.command == "doctor":
        config = load_config(args.config)
        run_doctor(config)
        return

    parser.print_help()
    sys.exit(1)
