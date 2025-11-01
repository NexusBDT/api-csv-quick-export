#!/usr/bin/env python3
import argparse, csv, json, logging, os, sys, time
from typing import Any, Iterable, Dict

# Pretty console logging (and file logging)
try:
    from rich.console import Console
    from rich.logging import RichHandler
    _HAVE_RICH = True
except Exception:
    _HAVE_RICH = False

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging(no_console: bool = False):
    handlers = [logging.FileHandler(os.path.join(LOG_DIR, "app.log"), encoding="utf-8")]
    if not no_console and _HAVE_RICH:
        handlers.insert(0, RichHandler(console=Console(), markup=True, show_level=True, show_path=False))
    logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=handlers)

def parse_args():
    p = argparse.ArgumentParser(description="Fetch JSON from a URL and save as CSV.")
    p.add_argument("--url", required=True, help="HTTP(S) endpoint returning JSON")
    p.add_argument("--out", required=True, help="Output CSV path (e.g., data/out.csv)")
    p.add_argument("--max-rows", type=int, default=0, help="Limit rows (0 = no limit)")
    p.add_argument("--timeout", type=float, default=10.0, help="Per-request timeout seconds")
    p.add_argument("--retries", type=int, default=3, help="Retry attempts on failure")
    p.add_argument("--no-console", action="store_true", help="Disable console logging; log to file only")
    return p.parse_args()

import requests

def get_json(url: str, timeout: float, retries: int) -> Any:
    """Simple retry loop with exponential backoff."""
    backoff = 1.0
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            if 200 <= r.status_code < 300:
                return r.json()
            else:
                logging.warning(f"HTTP {r.status_code} from {url}")
        except requests.RequestException as e:
            logging.warning(f"Request error (attempt {attempt}/{retries}): {e}")
        if attempt < retries:
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts.")

def to_rows(obj: Any) -> Iterable[Dict[str, Any]]:
    """Normalize JSON into an iterable of dict rows."""
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                yield item
            else:
                yield {"value": json.dumps(item, ensure_ascii=False)}
    elif isinstance(obj, dict):
        yield obj
    else:
        yield {"value": json.dumps(obj, ensure_ascii=False)}

def write_csv(rows: Iterable[Dict[str, Any]], out_path: str, max_rows: int = 0) -> int:
    rows = list(rows)
    if max_rows and max_rows > 0:
        rows = rows[:max_rows]
    if not rows:
        raise RuntimeError("No rows to write.")

    header = set()
    for r in rows:
        header.update(r.keys())
    header = sorted(header)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})
    return len(rows)

def main():
    args = parse_args()
    setup_logging(no_console=args.no_console)
    try:
        data = get_json(args.url, timeout=args.timeout, retries=args.retries)
        n = write_csv(to_rows(data), args.out, max_rows=args.max_rows)
        msg = f"Wrote {n} rows to {args.out}"
        print(msg)
        logging.info(msg)
        sys.exit(0)
    except Exception as e:
        err = f"ERROR: {e}"
        print(err, file=sys.stderr)
        logging.error(err)
        sys.exit(1)

if __name__ == "__main__":
    main()
