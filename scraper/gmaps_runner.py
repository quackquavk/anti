"""Wrapper around gosom/google-maps-scraper.

We shell out to the upstream scraper (Docker image or native binary), stream its
newline-delimited JSON output as it lands, and hand normalized results back
through the same callbacks the old ScraperEngine used.

Why a subprocess instead of a port: the upstream tool parses Google's structured
place payload rather than scraping the DOM, so phone/address/coordinates come
back correct and every place carries a place_id we can dedup on exactly.

Environment:
    GMAPS_MODE          "docker" (default) or "binary"
    GMAPS_DOCKER_CMD    docker executable, default "docker"
    GMAPS_IMAGE         image name, default "gosom/google-maps-scraper"
    GMAPS_BINARY        path to native binary when GMAPS_MODE=binary
    GMAPS_DATA_DIR      scratch dir for job working files, default "./gmapsdata"
"""

import json
import math
import os
import shutil
import subprocess
import threading
import time

from . import geocode
from .normalize import dedup_key, normalize

DEFAULT_IMAGE = "gosom/google-maps-scraper"
PLAYWRIGHT_CACHE_VOLUME = "gmaps-playwright-cache"

# Where docker usually lives. systemd units frequently pin a minimal PATH (ours
# is just the venv's bin), so relying on PATH alone fails under the service even
# though it works fine in a login shell.
DOCKER_FALLBACK_PATHS = (
    "/usr/bin/docker",
    "/usr/local/bin/docker",
    "/snap/bin/docker",
)


def _resolve_executable(cmd, fallbacks):
    """Find an executable that may not be on a stripped-down PATH."""
    if os.path.isabs(cmd):
        return cmd

    found = shutil.which(cmd)
    if found:
        return found

    for candidate in fallbacks:
        if os.access(candidate, os.X_OK):
            return candidate

    # Let the caller fail with a useful message rather than guessing further.
    return cmd

# Upstream caps a single non-grid search at roughly this many places, which is
# also where the old engine's scroll loop gave up. Above it we switch to grid.
SINGLE_SEARCH_CEILING = 120

# A grid cell wider than this is far bigger than one search covers at the
# zoom levels we use, so most of the cell never gets looked at. Areas that
# would need cells this large aren't grid-scrapable and fall back to search.
MAX_GRID_CELL_KM = 5.0


class GmapsRunner:
    """Runs one scrape job as a child process."""

    def __init__(self, log_callback=None, result_callback=None, is_active=None):
        self.log = log_callback or print
        self.on_result = result_callback
        # Called between polls; return False to abort the run.
        self.is_active = is_active or (lambda: True)

        self.mode = os.getenv("GMAPS_MODE", "docker")
        self.docker_cmd = _resolve_executable(
            os.getenv("GMAPS_DOCKER_CMD", "docker"), DOCKER_FALLBACK_PATHS
        )
        self.image = os.getenv("GMAPS_IMAGE", DEFAULT_IMAGE)
        self.binary = os.getenv("GMAPS_BINARY", "./google-maps-scraper")
        self.data_dir = os.path.abspath(os.getenv("GMAPS_DATA_DIR", "gmapsdata"))

        self._seen = set()
        self._results = []
        self._proc = None

    # ---------------------------------------------------------------- helpers

    def _job_dir(self, job_id):
        path = os.path.join(self.data_dir, str(job_id))
        os.makedirs(path, exist_ok=True)
        return path

    def _build_command(self, work_dir, opts):
        """Assemble the argv for the scraper, for either docker or binary mode."""
        # Flags are identical in both modes; only the paths differ.
        if self.mode == "binary":
            in_path = os.path.join(work_dir, "queries.txt")
            out_path = os.path.join(work_dir, "results.json")
            argv = [self.binary]
        else:
            in_path = "/work/queries.txt"
            out_path = "/work/results.json"
            argv = [
                self.docker_cmd, "run", "--rm",
                "-v", f"{PLAYWRIGHT_CACHE_VOLUME}:/opt",
                "-v", f"{work_dir}:/work",
                # Chromium is the memory hog here; keep shm modest on small VPSes.
                "--shm-size", opts.get("shm_size", "256m"),
            ]
            if opts.get("memory_limit"):
                argv += ["--memory", opts["memory_limit"]]
            argv.append(self.image)

        argv += [
            "-input", in_path,
            "-results", out_path,
            "-json",
            "-depth", str(opts["depth"]),
            "-c", str(opts["concurrency"]),
            "-pages-per-browser", str(opts["pages_per_browser"]),
            "-lang", opts["lang"],
            "-exit-on-inactivity", opts["exit_on_inactivity"],
        ]

        if opts.get("browser_pool_size"):
            argv += ["-browser-pool-size", str(opts["browser_pool_size"])]

        if opts.get("grid_bbox"):
            argv += [
                "-grid-bbox", opts["grid_bbox"],
                "-grid-cell", str(opts["grid_cell"]),
                "-zoom", str(opts["zoom"]),
            ]
        elif opts.get("geo"):
            argv += ["-geo", opts["geo"], "-zoom", str(opts["zoom"])]
            if opts.get("radius"):
                argv += ["-radius", str(opts["radius"])]

        if opts.get("email"):
            argv.append("-email")

        if opts.get("proxies"):
            argv += ["-proxies", opts["proxies"]]

        return argv

    def _drain(self, out_path, offset, buffer, target):
        """Read newly appended JSONL, emit fresh unique results.

        Returns (new_offset, leftover_buffer, hit_target). The scraper writes
        each entry with a plain json.Encoder, so lines land unbuffered and we
        can tail the file while the job is still running.
        """
        if not os.path.exists(out_path):
            return offset, buffer, False

        size = os.path.getsize(out_path)
        if size <= offset:
            return offset, buffer, False

        with open(out_path, "rb") as f:
            f.seek(offset)
            chunk = f.read(size - offset)
            offset = f.tell()

        buffer += chunk.decode("utf-8", errors="replace")
        lines = buffer.split("\n")
        # The final element is either empty or a half-written line; hold it back.
        buffer = lines.pop()

        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            key = dedup_key(entry)
            if key in self._seen:
                continue

            record = normalize(entry)

            # Grid cells overrun their bbox upstream, so clip here.
            bbox = getattr(self, "_clip_bbox", None)
            if bbox and not geocode.point_in_bbox(
                record.get("latitude"), record.get("longitude"), bbox
            ):
                continue

            self._seen.add(key)
            self._results.append(record)

            if self.on_result:
                self.on_result(record, len(self._results), target)

            if target and len(self._results) >= target:
                return offset, buffer, True

        return offset, buffer, False

    def _pump_stderr(self, proc):
        """Distil the scraper's stderr into a few human-readable lines.

        Upstream emits a JSON event per action: one per place visited carrying a
        full Google Maps URL, and one per business website that loads slowly
        carrying a Playwright call log. Forwarded verbatim that buried the job
        log in noise, so parse it and surface only what tells the user
        something. Website fetch timeouts are normal when chasing emails, so
        they're counted and reported once rather than line by line.
        """
        site_failures = 0
        last_stats = None

        for raw in iter(proc.stderr.readline, b""):
            line = raw.decode("utf-8", errors="replace").strip()

            # Startup banner and sponsor art aren't JSON; skip anything that
            # isn't a structured event.
            if not line.startswith("{"):
                continue

            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            message = event.get("message", "")

            if "places found" in message:
                self.log(f"  {message}")

            elif message == "scrapemate stats":
                done = event.get("numOfJobsCompleted", 0)
                failed = event.get("numOfJobsFailed", 0)
                speed = event.get("speed", "")
                stats = (done, failed, speed)
                # Emitted on a timer, so skip it when nothing has moved.
                if stats != last_stats:
                    last_stats = stats
                    note = f"  {done} pages processed"
                    if failed:
                        note += f", {failed} failed"
                    if speed:
                        note += f" ({speed})"
                    self.log(note)

            elif event.get("level") == "error":
                error = event.get("error", "")
                if "Frame.Goto" in error or "timeout" in error.lower():
                    site_failures += 1
                elif "context canceled" not in error:
                    # Genuinely unexpected; worth one short line.
                    self.log(f"  scraper error: {error.splitlines()[0][:160]}")

        if site_failures:
            self.log(
                f"  {site_failures} business website(s) were too slow to read "
                f"for an email; their other fields are unaffected."
            )

    # ------------------------------------------------------------------- main

    def _plan_area(self, location, config, want_grid):
        """Work out how to aim the scraper at `location`.

        Returns one of:
          {"mode": "grid",     bbox, cell, cells}  - tile the area
          {"mode": "anchored", lat, lon, zoom, radius} - one search, centred there
          None - location could not be resolved at all

        The anchored mode matters more than it looks: Google geolocates by the
        requesting IP, so an un-anchored search run from a Frankfurt VPS returns
        Frankfurt businesses no matter what location the query names.
        """
        try:
            self.log(f"Resolving location '{location}'...")
            place = geocode.lookup(location)
        except geocode.GeocodeError as e:
            self.log(
                f"  Could not resolve '{location}' ({e}). Falling back to an "
                f"unanchored search, which may return results near the server "
                f"rather than the requested area."
            )
            return None

        bbox = place["bbox"]
        self.log(f"  Matched: {place['display_name']}")

        def anchored(reason=None):
            if reason:
                self.log(reason)
            width, height = geocode.bbox_dimensions_km(bbox)
            zoom = geocode.zoom_for_span(max(width, height))
            radius = geocode.radius_for_span(max(width, height))
            self.log(
                f"  Centring search on {place['lat']:.4f},{place['lon']:.4f} "
                f"at zoom {zoom} (radius {radius / 1000:,.0f} km)"
            )
            return {
                "mode": "anchored",
                "lat": place["lat"],
                "lon": place["lon"],
                "zoom": zoom,
                "radius": radius,
            }

        if not want_grid:
            return anchored()

        # Countries with overseas territories report a box wrapping the globe.
        # Gridding it scatters searches across the planet, and the bbox is so
        # wide that clipping cannot reject the strays either.
        if geocode.spans_antimeridian(bbox):
            return anchored(
                f"  '{location}' spans the antimeridian, so its bounding box "
                f"covers most of the globe and cannot be gridded. "
                f"Narrow to a city or region for grid coverage."
            )

        width, height = geocode.bbox_dimensions_km(bbox)
        self.log(f"  Area: {width:,.1f} x {height:,.1f} km")

        cell = float(config.get("grid_cell_km", 1.0))
        max_cells = int(config.get("max_grid_cells", 400))

        cells = max(1, int((width / cell) * (height / cell)))
        if cells > max_cells:
            # Widen cells to fit the budget, but only so far: past
            # MAX_GRID_CELL_KM a cell is far bigger than one search covers at
            # this zoom, so most of its area is never actually looked at.
            cell = math.sqrt((width * height) / max_cells)
            cells = max_cells
            if cell <= MAX_GRID_CELL_KM:
                self.log(
                    f"  Widening cell to {cell:.2f} km to stay under "
                    f"max_grid_cells={max_cells}"
                )

        if cell > MAX_GRID_CELL_KM:
            return anchored(
                f"  '{location}' is too large to grid: covering it within "
                f"max_grid_cells={max_cells} needs {cell:,.0f} km cells, well "
                f"past the {MAX_GRID_CELL_KM} km a single search covers at this "
                f"zoom, so most of the area would never be visited. "
                f"Scrape city by city for full coverage."
            )

        return {"mode": "grid", "bbox": bbox, "cell": cell, "cells": cells}

    def run(self, job_id, config):
        """Execute a scrape. Returns the list of unique normalized results."""
        self._seen.clear()
        self._results = []
        self._clip_bbox = None

        query = (config.get("search_query") or "restaurant").strip()
        location = (config.get("location") or "").strip()
        target = int(config.get("total") or 10)

        opts = {
            "depth": config.get("depth", 10),
            "concurrency": config.get("concurrency", 1),
            "pages_per_browser": config.get("pages_per_browser", 1),
            "browser_pool_size": config.get("browser_pool_size", 1),
            "lang": config.get("lang", "en"),
            "zoom": min(int(config.get("zoom_level", 15)), 21),
            "email": config.get("email", True),
            "proxies": config.get("proxies") or os.getenv("GMAPS_PROXIES"),
            "exit_on_inactivity": config.get("exit_on_inactivity", "3m"),
            "shm_size": config.get("shm_size", "256m"),
            "memory_limit": config.get("memory_limit") or os.getenv("GMAPS_MEMORY_LIMIT"),
            "radius": config.get("radius"),
        }

        work_dir = self._job_dir(job_id)
        out_path = os.path.join(work_dir, "results.json")

        # Grid mode only pays for itself past the single-search ceiling.
        plan = None
        if location:
            plan = self._plan_area(
                location, config, want_grid=target > SINGLE_SEARCH_CEILING
            )

        if plan and plan["mode"] == "grid":
            search_line = query
            self.log(
                f"GRID MODE: ~{plan['cells']} cells of {plan['cell']:.2f} km "
                f"at zoom {opts['zoom']}"
            )
            opts["grid_bbox"] = geocode.format_bbox(plan["bbox"])
            opts["grid_cell"] = round(plan["cell"], 3)
            self._clip_bbox = plan["bbox"]
        else:
            search_line = f"{query} in {location}" if location else query
            self.log(f"SEARCH MODE: '{search_line}' (target {target})")

            if plan and plan["mode"] == "anchored":
                # Without this the search inherits the server's own location.
                opts["geo"] = f"{plan['lat']:.6f},{plan['lon']:.6f}"
                opts["zoom"] = plan["zoom"]
                opts["radius"] = plan["radius"]

            if target > SINGLE_SEARCH_CEILING:
                self.log(
                    f"  Note: a single search returns about "
                    f"{SINGLE_SEARCH_CEILING} places, so the target of {target} "
                    f"is unlikely to be reached without grid coverage."
                )

        with open(os.path.join(work_dir, "queries.txt"), "w") as f:
            f.write(search_line + "\n")

        # Start clean so a rerun of the same job id can't replay old rows.
        if os.path.exists(out_path):
            os.remove(out_path)

        cleanup = config.get("cleanup", True)
        try:
            return self._execute(work_dir, out_path, opts, target)
        finally:
            # Must be in a finally: a launch failure or a stop mid-run would
            # otherwise leave the job's work dir behind forever.
            if cleanup:
                shutil.rmtree(work_dir, ignore_errors=True)

    def _execute(self, work_dir, out_path, opts, target):
        """Launch the scraper and collect results until it stops or hits target."""
        argv = self._build_command(work_dir, opts)
        self.log(f"Launching scraper: {' '.join(argv[:6])} ...")
        if opts["email"]:
            self.log("  Email extraction ON - this visits each business site and is slow.")

        try:
            self._proc = subprocess.Popen(
                argv, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
            )
        except FileNotFoundError as e:
            raise RuntimeError(
                f"Could not launch scraper ({e}). Tried '{self.docker_cmd}'. "
                f"Check the user can run docker without sudo; if it lives somewhere "
                f"unusual, set GMAPS_DOCKER_CMD to its absolute path."
            ) from e

        stderr_thread = threading.Thread(
            target=self._pump_stderr, args=(self._proc,), daemon=True
        )
        stderr_thread.start()

        offset, buffer = 0, ""
        stopped, hit_target = False, False
        last_report = 0

        try:
            while True:
                offset, buffer, hit_target = self._drain(out_path, offset, buffer, target)

                if hit_target:
                    self.log(f"Target of {target} reached; stopping scraper.")
                    break

                if not self.is_active():
                    self.log("Stop requested; terminating scraper.")
                    stopped = True
                    break

                if self._proc.poll() is not None:
                    # Process exited; do a final drain to catch the tail.
                    offset, buffer, _ = self._drain(out_path, offset, buffer, target)
                    break

                # The UI streams each place into its results table, so log
                # milestones only rather than one line per place.
                found = len(self._results)
                if found and found != last_report and found % 10 == 0:
                    last_report = found
                    self.log(f"  {found} places found so far...")

                time.sleep(1.0)
        finally:
            self._terminate()

        if stopped:
            self.log(f"Stopped with {len(self._results)} unique places.")
        elif not hit_target:
            code = self._proc.returncode
            if code not in (0, None) and not self._results:
                raise RuntimeError(f"Scraper exited with code {code} and produced no results.")
            self.log(f"Scraper finished with {len(self._results)} unique places.")

        return self._results

    def _terminate(self):
        """Stop the child, escalating if it ignores SIGTERM."""
        proc = self._proc
        if not proc or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
