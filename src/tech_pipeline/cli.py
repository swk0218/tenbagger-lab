from __future__ import annotations

import argparse

from .pipeline import run


def main() -> None:
    parser = argparse.ArgumentParser(description="Tech stock research pipeline")
    parser.add_argument("mode", choices=["full", "quick", "deep"], nargs="?", default="full")
    parser.add_argument("--tickers", default="")
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()] or None
    out = run(mode=args.mode, tickers=tickers)
    print(f"Run completed: {out}")


if __name__ == "__main__":
    main()
