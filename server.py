from flask import Flask, render_template, jsonify, request, Response
from werkzeug.datastructures import MultiDict
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timezone
import threading
import time
import shutil
import json
import os
import re
import io
import csv
import gzip

from scraper.gmaps_runner import GmapsRunner
from scraper.normalize import CSV_COLUMNS

load_dotenv()

app = Flask(__name__)

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI")
client = MongoClient(MONGODB_URI)
db = client.get_database("scraper_db")
jobs_collection = db["jobs"]

# Track active job threads
active_jobs = {}

# Each running job is a headless Chromium in a container. On a small box two at
# once is the practical limit, so extra jobs wait their turn as "pending" and
# the UI tells the user they are queued instead of the server swapping itself
# to death.
MAX_CONCURRENT_JOBS = max(1, int(os.getenv("MAX_CONCURRENT_JOBS", "2")))
job_slots = threading.BoundedSemaphore(MAX_CONCURRENT_JOBS)

MAX_TOTAL = 10000
RESULTS_PAGE_SIZE = 100
MAX_RESULTS_PAGE_SIZE = 500
EXPORT_BATCH_SIZE = 250

# Statuses that only a live worker thread can move forward. If the process dies
# -- restart, deploy, crash, OOM -- jobs sat in these forever and the UI showed
# them as running indefinitely.
NON_TERMINAL_STATUSES = ("pending", "running", "stopping")


def reconcile_interrupted_jobs():
    """Close out jobs whose worker died, and bin their leftover work dirs.

    Safe because the service runs a single gunicorn worker, so nothing else can
    own a job at the moment this runs. Partial results are kept -- a scrape that
    got 60 places before a restart is still worth something.
    """
    result = jobs_collection.update_many(
        {"status": {"$in": list(NON_TERMINAL_STATUSES)}},
        {
            "$set": {
                "status": "interrupted",
                "updated_at": datetime.now(timezone.utc),
                "finished_at": datetime.now(timezone.utc),
            },
            "$push": {
                "logs": {
                    "$each": [
                        "Server restarted while this job was active, so it was "
                        "interrupted. Any results collected so far are kept; "
                        "re-run the job to continue."
                    ],
                    "$slice": -100,
                }
            },
        },
    )
    if result.modified_count:
        print(f"[startup] marked {result.modified_count} interrupted job(s)")

    # Work dirs belong to a live scrape; at startup none can be.
    data_dir = os.path.abspath(os.getenv("GMAPS_DATA_DIR", "gmapsdata"))
    if os.path.isdir(data_dir):
        for name in os.listdir(data_dir):
            shutil.rmtree(os.path.join(data_dir, name), ignore_errors=True)


reconcile_interrupted_jobs()

def log_to_job(job_id, message):
    """Append log message to job in MongoDB."""
    jobs_collection.update_one(
        {"_id": ObjectId(job_id)},
        {
            "$push": {"logs": {"$each": [message], "$slice": -100}},
            "$set": {"updated_at": datetime.now(timezone.utc)}
        }
    )
    print(f"[{job_id}] {message}")

def update_job_status(job_id, status, results_count=None, error=None):
    """Update job status in MongoDB."""
    update = {
        "status": status,
        "updated_at": datetime.now(timezone.utc)
    }
    if results_count is not None:
        update["results_count"] = results_count
    if error is not None:
        update["error"] = error
    if status == "running":
        update["started_at"] = update["updated_at"]
    elif status in ("completed", "stopped", "error"):
        update["finished_at"] = update["updated_at"]
    
    jobs_collection.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": update}
    )

def save_job_results(job_id, results):
    """Save results to job document."""
    jobs_collection.update_one(
        {"_id": ObjectId(job_id)},
        {
            "$set": {
                "results": results,
                "results_count": len(results),
                "updated_at": datetime.now(timezone.utc)
            }
        }
    )

def append_job_result(job_id, result):
    """Add one result to the job.

    A $push keeps each write the size of one place. Rewriting the whole array
    per result made a 5,000-place job send megabytes to Mongo for every place.
    """
    jobs_collection.update_one(
        {"_id": ObjectId(job_id)},
        {
            "$push": {"results": result},
            "$inc": {"results_count": 1},
            "$set": {"updated_at": datetime.now(timezone.utc)},
        },
    )


def set_job_notice(job_id, message, title=None):
    """Store a plain-language note for the person who started the search."""
    jobs_collection.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": {
            "notice": message,
            "notice_title": title,
            "updated_at": datetime.now(timezone.utc),
        }},
    )


def job_status(job_id):
    job = jobs_collection.find_one({"_id": ObjectId(job_id)}, {"status": 1})
    return job.get("status") if job else None


# Waiting jobs in arrival order. Only the head of the line may take a free
# slot, so the position a job is shown ("2 ahead") is the order it will run in.
wait_queue = []
queue_lock = threading.Lock()


def queue_ahead(job_id):
    """How many waiting jobs are ahead of this one. None if it is not queued."""
    with queue_lock:
        try:
            return wait_queue.index(str(job_id))
        except ValueError:
            return None


def wait_for_slot(job_id):
    """Block until this job may run. False if it was cancelled while queued."""
    try:
        while True:
            if job_status(job_id) != "pending":
                return False
            with queue_lock:
                first = bool(wait_queue) and wait_queue[0] == job_id
            if not first:
                time.sleep(2)
                continue
            if job_slots.acquire(timeout=2):
                if job_status(job_id) != "pending":
                    job_slots.release()
                    return False
                return True
    finally:
        with queue_lock:
            if job_id in wait_queue:
                wait_queue.remove(job_id)


def run_scraper(job_id, config):
    """Run one scrape job via the gosom scraper and stream results into Mongo."""

    all_results = []

    def log_cb(message):
        log_to_job(job_id, message)

    def result_cb(result, current_count, total):
        # The runner already deduped on place_id, so every result here is new.
        all_results.append(result)
        append_job_result(job_id, result)

    def is_job_active():
        job = jobs_collection.find_one(
            {"_id": ObjectId(job_id)}, {"status": 1}
        )
        return bool(job) and job.get("status") == "running"

    runner = GmapsRunner(
        log_callback=log_cb,
        result_callback=result_cb,
        is_active=is_job_active,
        notice_callback=lambda message, title=None: set_job_notice(job_id, message, title),
        place_callback=lambda name: jobs_collection.update_one(
            {"_id": ObjectId(job_id)}, {"$set": {"matched_place": name}}
        ),
    )

    if not wait_for_slot(job_id):
        active_jobs.pop(job_id, None)
        return

    try:
        update_job_status(job_id, "running")

        results = runner.run(job_id, config)

        # Final reconcile in case the last batch landed after the target check.
        if len(results) != len(all_results):
            all_results = results
            save_job_results(job_id, all_results)

        job = jobs_collection.find_one({"_id": ObjectId(job_id)}, {"status": 1})
        final_status = "stopped" if job and job.get("status") == "stopping" else "completed"

        update_job_status(job_id, final_status, len(all_results))
        log_to_job(job_id, f"Job {final_status} with {len(all_results)} results")

    except Exception as e:
        log_to_job(job_id, f"ERROR: {e}")
        update_job_status(job_id, "error", len(all_results), error=str(e))

    finally:
        job_slots.release()
        active_jobs.pop(job_id, None)


def run_job_thread(job_id, config):
    """Thread entrypoint. The scraper is a subprocess now, so no event loop."""
    run_scraper(job_id, config)

# Routes

def _asset_version():
    """Newest mtime under static/, so a deploy busts cached CSS and JS."""
    newest = 0
    for root, _, files in os.walk(app.static_folder):
        for name in files:
            newest = max(newest, int(os.path.getmtime(os.path.join(root, name))))
    return newest


@app.route("/")
def index():
    return render_template("index.html", asset_v=_asset_version())


def clean_config(raw):
    """Validate a job config. Returns (config, errors keyed by field)."""
    config = dict(raw)
    errors = {}

    query = str(raw.get("search_query") or "").strip()
    location = str(raw.get("location") or "").strip()
    if not query:
        errors["search_query"] = "Enter the kind of business you are looking for."
    elif len(query) > 120:
        errors["search_query"] = "Keep this under 120 characters."
    if len(location) > 120:
        errors["location"] = "Keep this under 120 characters."

    try:
        total = int(raw.get("total", 100))
    except (TypeError, ValueError):
        total = 0
    if not 1 <= total <= MAX_TOTAL:
        errors["total"] = f"Choose a number between 1 and {MAX_TOTAL:,}."

    # Optional tuning knobs: drop anything unusable so the runner's own
    # defaults apply instead of crashing the job on a bad number.
    for key, cast, low, high in (
        ("grid_cell_km", float, 0.2, 20.0),
        ("zoom_level", int, 10, 21),
    ):
        if key in config:
            try:
                value = cast(config[key])
                if not low <= value <= high:
                    raise ValueError
                config[key] = value
            except (TypeError, ValueError):
                config.pop(key)

    config.update(search_query=query, location=location, total=total)
    if "email" in config:
        config["email"] = bool(config["email"])
    return config, errors

@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    """List jobs, newest first. Optionally filter by user_id.

    ?limit=N returns up to N (default 50, at most 500).
    """
    user_id = request.args.get("user_id")
    query = {}
    if user_id:
        query["user_id"] = user_id
        
    # The list view only shows status and counts. Shipping results and logs
    # here made each poll ~14 MB, which got the Atlas cluster throttled.
    limit = request.args.get("limit", default=50, type=int) or 50
    limit = max(1, min(limit, 500))
    jobs = list(
        jobs_collection.find(query, {"results": 0, "logs": 0})
        .sort("created_at", -1)
        .limit(limit)
    )
    for job in jobs:
        job["_id"] = str(job["_id"])
        if job.get("status") == "pending":
            job["queue_ahead"] = queue_ahead(job["_id"])
    return jsonify(jobs)

@app.route("/api/jobs", methods=["POST"])
def create_job():
    """Create and start a new scraping job."""
    data = request.get_json(silent=True) or {}
    config = data.get("config", {})
    # Support direct config passed as body or wrapped in "config" key
    if not config and "search_query" in data:
        config = {k: v for k, v in data.items() if k != "user_id"}

    user_id = data.get("user_id")

    config, errors = clean_config(config)
    if errors:
        return jsonify({
            "error": next(iter(errors.values())),
            "fields": errors,
        }), 400
    
    # Create job document
    job = {
        "user_id": user_id,
        "config": config,
        "status": "pending",
        "results_count": 0,
        "results": [],
        "logs": [],
        "error": None,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc)
    }
    
    result = jobs_collection.insert_one(job)
    job_id = str(result.inserted_id)
    
    with queue_lock:
        wait_queue.append(job_id)

    # Start job in background thread
    thread = threading.Thread(target=run_job_thread, args=(job_id, config))
    thread.daemon = True
    thread.start()
    active_jobs[job_id] = thread
    
    log_to_job(job_id, f"Job created with config: {config}")
    
    return jsonify({"job_id": job_id, "message": "Job started"})

# Fields the UI's results table actually renders. The full record carries
# thumbnails, open_hours and review breakdowns, which are wasteful to ship on a
# 3-second poll but still available from the unslimmed endpoint and the CSV.
SLIM_RESULT_FIELDS = (
    "name", "phone", "email", "website", "category", "city", "address",
    "rating", "review_count", "location_link",
)


@app.route("/api/jobs/<job_id>", methods=["GET"])
def get_job(job_id):
    """Get single job details.

    Pass ?slim=1 to trim each result to the fields the UI displays.
    Pass ?since=N to get only results from index N on. Results are append-only,
    so a client that already holds N of them never re-downloads the list.
    """
    try:
        since = max(0, request.args.get("since", default=0, type=int) or 0)
        oid = ObjectId(job_id)

        if since:
            # Two small reads instead of one large one: the job without its
            # results, then only the tail the client is missing. Only the
            # "results" key is read from the second, so it does not matter
            # whether a server treats a bare $slice as inclusion or exclusion.
            job = jobs_collection.find_one({"_id": oid}, {"results": 0})
            if job:
                tail = jobs_collection.find_one(
                    {"_id": oid}, {"results": {"$slice": [since, MAX_TOTAL]}}
                )
                job["results"] = (tail or {}).get("results", [])
        else:
            job = jobs_collection.find_one({"_id": oid})

        if not job:
            return jsonify({"error": "Job not found"}), 404
        job["_id"] = str(job["_id"])
        if job.get("status") == "pending":
            job["queue_ahead"] = queue_ahead(job["_id"])

        results = job.get("results", [])
        job["results_offset"] = since
        job["has_results"] = (job.get("results_count") or 0) > 0 or len(results) > 0

        if request.args.get("slim") and results:
            job["results"] = [
                {k: r.get(k) for k in SLIM_RESULT_FIELDS} for r in results
            ]

        return jsonify(job)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

def results_filter(args):
    """The list filters as a MongoDB condition on one result."""
    conditions = []
    for param, field in (("has_email", "email"), ("has_phone", "phone"), ("has_website", "website")):
        if args.get(param):
            conditions.append({field: {"$nin": [None, ""]}})
    min_rating = args.get("min_rating", type=float)
    if min_rating is not None:
        conditions.append({"rating": {"$gte": min_rating}})
    needle = (args.get("q") or "").strip()
    if needle:
        pattern = {"$regex": re.escape(needle), "$options": "i"}
        conditions.append({"$or": [
            {k: pattern} for k in ("name", "category", "city", "address", "phone", "email", "website")
        ]})
    return {"$and": conditions} if conditions else {}


def results_sort_stages(key, direction):
    """Sort with blanks last in either direction, ties in the order found."""
    if key not in SLIM_RESULT_FIELDS:
        return [{"$sort": {"i": 1}}]
    value = f"${key}"
    sort_value = value if key in ("rating", "review_count") else {"$toLower": {"$ifNull": [value, ""]}}
    return [
        {"$addFields": {
            "_blank": {"$cond": [{"$eq": [{"$ifNull": [value, ""]}, ""]}, 1, 0]},
            "_sort": sort_value,
        }},
        {"$sort": {"_blank": 1, "_sort": direction, "i": 1}},
    ]


def read_results_page(oid, args, page, per_page):
    """One page, its matching total and the summary, in one database read.

    Atlas delivers about 100 KB/s here, so the filtering, sorting and counting
    happen there and only the rows on screen cross the network.
    """
    row_fields = {k: f"$results.{k}" for k in SLIM_RESULT_FIELDS}
    rows_base = []
    upto = args.get("upto", type=int)
    if upto is not None:
        rows_base.append({"$match": {"i": {"$lt": max(0, upto)}}})
    match = results_filter(args)
    if match:
        rows_base.append({"$match": match})

    def has(field):
        return {"$cond": [{"$gt": [{"$ifNull": [f"${field}", ""]}, ""]}, 1, 0]}

    rating = {"$ifNull": ["$rating", 0]}
    direction = -1 if args.get("dir") == "desc" else 1

    def run(skip):
        return next(jobs_collection.aggregate([
            {"$match": {"_id": oid}},
            {"$project": {"_id": 0, "results": 1}},
            {"$unwind": {"path": "$results", "includeArrayIndex": "i"}},
            {"$project": {"i": 1, **row_fields}},
            {"$facet": {
                "rows": rows_base + results_sort_stages(args.get("sort"), direction) + [
                    {"$skip": skip},
                    {"$limit": per_page},
                    {"$project": {"_blank": 0, "_sort": 0}},
                ],
                "matching": rows_base + [{"$count": "n"}],
                "stats": [{"$group": {
                    "_id": None,
                    "total": {"$sum": 1},
                    "email": {"$sum": has("email")},
                    "phone": {"$sum": has("phone")},
                    "website": {"$sum": has("website")},
                    "rated": {"$sum": {"$cond": [{"$gt": [rating, 0]}, 1, 0]}},
                    "rated4": {"$sum": {"$cond": [{"$gte": [rating, 4]}, 1, 0]}},
                    "rating_sum": {"$sum": rating},
                }}],
            }},
        ]), None)

    data = run((page - 1) * per_page)
    if data is None:  # no results array entries at all
        return [], 0, 1, None
    total = data["matching"][0]["n"] if data["matching"] else 0
    last = max(1, (total + per_page - 1) // per_page)
    if page > last:  # the list got shorter, or a stale link
        page = last
        data = run((page - 1) * per_page)
    stats = data["stats"][0] if data["stats"] else None
    if stats:
        stats.pop("_id", None)
    return data["rows"], total, page, stats


@app.route("/api/jobs/<job_id>/results", methods=["GET"])
def get_job_results(job_id):
    """The job plus one page of its list.

    Query: page, per_page, sort, dir (asc|desc), has_email, has_phone,
    has_website, min_rating, q, and upto (only consider the first N results,
    which holds a sorted list still while a search adds to it).
    """
    try:
        oid = ObjectId(job_id)
        job = jobs_collection.find_one({"_id": oid}, {"results": 0})
        if not job:
            return jsonify({"error": "Job not found"}), 404
        job["_id"] = str(job["_id"])
        if job.get("status") == "pending":
            job["queue_ahead"] = queue_ahead(job["_id"])

        page = max(1, request.args.get("page", default=1, type=int) or 1)
        per_page = request.args.get("per_page", default=RESULTS_PAGE_SIZE, type=int) or RESULTS_PAGE_SIZE
        per_page = min(max(1, per_page), MAX_RESULTS_PAGE_SIZE)

        rows, total, page, stats = read_results_page(oid, request.args, page, per_page)
        return jsonify({
            "job": job, "rows": rows, "total": total,
            "page": page, "per_page": per_page, "stats": stats,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.after_request
def compress_json(response):
    """nginx here only gzips HTML, so large JSON is compressed by the app."""
    if (
        response.mimetype == "application/json"
        and response.status_code == 200
        and not response.direct_passthrough
        and "gzip" in request.headers.get("Accept-Encoding", "")
        and "Content-Encoding" not in response.headers
    ):
        body = response.get_data()
        if len(body) >= 1024:
            response.set_data(gzip.compress(body, 5))
            response.headers["Content-Encoding"] = "gzip"
            response.headers.add("Vary", "Accept-Encoding")
    return response


@app.route("/api/jobs/<job_id>/stop", methods=["POST"])
def stop_job(job_id):
    """Request job to stop."""
    try:
        job = jobs_collection.find_one({"_id": ObjectId(job_id)})
        if not job:
            return jsonify({"error": "Job not found"}), 404
        
        if job.get("status") == "running":
            update_job_status(job_id, "stopping")
            log_to_job(job_id, "Stop requested by user")
            return jsonify({"message": "Stop requested"})
        elif job.get("status") == "pending":
            # Still queued: nothing to wind down, so it is stopped right away.
            update_job_status(job_id, "stopped", 0)
            log_to_job(job_id, "Cancelled before it started")
            return jsonify({"message": "Cancelled"})
        else:
            return jsonify({"message": "Job is not running"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

def filter_results(results, args):
    """Apply the UI's list filters, so a download matches what is on screen."""
    needle = (args.get("q") or "").strip().lower()
    want_email = bool(args.get("has_email"))
    want_phone = bool(args.get("has_phone"))
    want_website = bool(args.get("has_website"))
    min_rating = args.get("min_rating", type=float)

    def keep(r):
        if want_email and not r.get("email"):
            return False
        if want_phone and not r.get("phone"):
            return False
        if want_website and not r.get("website"):
            return False
        if min_rating is not None and (r.get("rating") or 0) < min_rating:
            return False
        if needle:
            haystack = " ".join(
                str(r.get(k) or "")
                for k in ("name", "category", "city", "address", "phone",
                          "email", "website")
            ).lower()
            if needle not in haystack:
                return False
        return True

    return [r for r in results if keep(r)]


def export_filename(job, filtered):
    """'dentists-austin-tx.csv' tells the user what is in the file; an id does not."""
    config = job.get("config") or {}
    label = f"{config.get('search_query') or ''} {config.get('location') or ''}"
    slug = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")[:80]
    if not slug:
        slug = f"job-{job['_id']}"
    return f"{slug}{'-filtered' if filtered else ''}.csv"


def iter_result_batches(oid):
    """Read a job's full results a slice at a time."""
    skip = 0
    while True:
        doc = next(jobs_collection.aggregate([
            {"$match": {"_id": oid}},
            {"$project": {"_id": 0, "batch": {"$slice": ["$results", skip, EXPORT_BATCH_SIZE]}}},
        ]), None)
        batch = (doc or {}).get("batch") or []
        if batch:
            yield batch
        if len(batch) < EXPORT_BATCH_SIZE:
            return
        skip += EXPORT_BATCH_SIZE


def csv_text(rows, columns, header=False):
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    if header:
        writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


@app.route("/api/jobs/<job_id>/export", methods=["GET"])
def export_job_csv(job_id):
    """Export job results as CSV, streamed so a large list cannot time out."""
    try:
        oid = ObjectId(job_id)
        job = jobs_collection.find_one({"_id": oid}, {"results": 0, "logs": 0})
        if not job:
            return jsonify({"error": "Job not found"}), 404
        job["_id"] = str(job["_id"])

        args = request.args.to_dict()
        batches = iter_result_batches(oid)
        first = []
        for batch in batches:  # hold the response until there is a row to send
            first = filter_results(batch, request.args)
            if first:
                break
        if not first:
            return jsonify({"error": "No results to export"}), 404

        # Lead with the columns people act on, then anything else returned.
        seen = list(dict.fromkeys(k for r in first for k in r))
        columns = [c for c in CSV_COLUMNS if c in seen] + [c for c in seen if c not in CSV_COLUMNS]

        filter_args = MultiDict(args)

        def generate():
            yield ("\ufeff" if args.get("excel") else "") + csv_text(first, columns, header=True)
            for batch in batches:
                rows = filter_results(batch, filter_args)
                if rows:
                    yield csv_text(rows, columns)

        filtered = any(args.get(k) for k in ("has_email", "has_phone", "has_website", "min_rating", "q"))
        filename = export_filename(job, filtered=filtered)
        return Response(
            generate(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/jobs/<job_id>", methods=["DELETE"])
def delete_job(job_id):
    """Delete a job."""
    try:
        result = jobs_collection.delete_one({"_id": ObjectId(job_id)})
        if result.deleted_count == 0:
            return jsonify({"error": "Job not found"}), 404
        return jsonify({"message": "Job deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    # Local development only. Production runs under gunicorn via systemd:
    #   gunicorn -w 1 --threads 4 --bind 127.0.0.1:5050 server:app
    # One worker is required: job threads and active_jobs are in-process state,
    # so a second worker would only see half the jobs.
    debug = os.getenv("FLASK_DEBUG", "").lower() in ("1", "true", "yes")
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5050"))

    print(f"Starting dev server at http://{host}:{port} (debug={debug})")
    print(f"MongoDB: {'Connected' if client else 'Not connected'}")
    app.run(debug=debug, host=host, port=port, threaded=True)
