import argparse
import json
import os

import pandas as pd

from scraper.gmaps_runner import GmapsRunner
from scraper.normalize import CSV_COLUMNS


def load_config():
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config.json: {e}")
    return {}


def save_csv(results, filename="results.csv"):
    if not results:
        print("No results to save.")
        return
    df = pd.DataFrame(results)
    ordered = [c for c in CSV_COLUMNS if c in df.columns]
    ordered += [c for c in df.columns if c not in ordered]
    df[ordered].to_csv(filename, index=False)
    print(f"Saved {len(results)} records to {filename}")


def main():
    config = load_config()

    parser = argparse.ArgumentParser(description="Google Maps Data Scraper")
    parser.add_argument("search_term", nargs="?", help="Search term/Query")
    parser.add_argument("--location", help="Location to search in")
    parser.add_argument("--total", type=int, help="Number of results to scrape")
    parser.add_argument("--no-email", action="store_true",
                        help="Skip email extraction (much faster)")
    parser.add_argument("--out", default="results.csv", help="Output CSV path")
    args = parser.parse_args()

    if args.search_term:
        config["search_query"] = args.search_term
    if args.location:
        config["location"] = args.location
    if args.total is not None:
        config["total"] = args.total
    if args.no_email:
        config["email"] = False

    runner = GmapsRunner(log_callback=print)
    results = runner.run("cli", config)
    save_csv(results, args.out)


if __name__ == "__main__":
    main()
