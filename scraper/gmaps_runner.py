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
        """Forward the scraper's own logs into the job log, lightly filtered."""
        for raw in iter(proc.stderr.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip()
            if not line:
                continue
            # These are per-request noise; the job log is user-facing.
            if any(s in line for s in ("level=DEBUG", "level=TRACE")):
                continue
            self.log(f"  [scraper] {line[:300]}")

    # ------------------------------------------------------------------- main

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
        if target > SINGLE_SEARCH_CEILING and location:
            search_line = query
            try:
                self.log(f"Resolving bounding box for '{location}'...")
                place = geocode.lookup(location)
                bbox = place["bbox"]
                width, height = geocode.bbox_dimensions_km(bbox)
                self.log(f"  Matched: {place['display_name']}")
                self.log(f"  Area: {width:.1f} x {height:.1f} km")

                cell = float(config.get("grid_cell_km", 1.0))
                max_cells = int(config.get("max_grid_cells", 400))

                # Guard against a huge bbox turning into thousands of searches.
                estimated = max(1, int((width / cell) * (height / cell)))
                if estimated > max_cells:
                    import math
                    cell = math.sqrt((width * height) / max_cells)
                    estimated = max_cells
                    self.log(
                        f"  Grid would be {estimated}+ cells; widening cell to "
                        f"{cell:.2f} km to stay under max_grid_cells={max_cells}"
                    )

                self.log(f"GRID MODE: ~{estimated} cells of {cell:.2f} km at zoom {opts['zoom']}")
                opts["grid_bbox"] = geocode.format_bbox(bbox)
                opts["grid_cell"] = round(cell, 3)
                self._clip_bbox = bbox
            except geocode.GeocodeError as e:
                self.log(f"  Geocoding failed ({e}); falling back to plain search.")
                search_line = f"{query} in {location}" if location else query
        else:
            search_line = f"{query} in {location}" if location else query
            self.log(f"SEARCH MODE: '{search_line}' (target {target})")

        with open(os.path.join(work_dir, "queries.txt"), "w") as f:
            f.write(search_line + "\n")

        # Start clean so a rerun of the same job id can't replay old rows.
        if os.path.exists(out_path):
            os.remove(out_path)

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

                if len(self._results) and len(self._results) != last_report:
                    last_report = len(self._results)
                    self.log(f"  {last_report} unique places so far...")

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

        if config.get("cleanup", True):
            shutil.rmtree(work_dir, ignore_errors=True)

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
