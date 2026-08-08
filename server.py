from flask import Flask, render_template, jsonify, request, Response
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import threading
import json
import os
import io
import pandas as pd

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

def log_to_job(job_id, message):
    """Append log message to job in MongoDB."""
    jobs_collection.update_one(
        {"_id": ObjectId(job_id)},
        {
            "$push": {"logs": {"$each": [message], "$slice": -100}},
            "$set": {"updated_at": datetime.utcnow()}
        }
    )
    print(f"[{job_id}] {message}")

def update_job_status(job_id, status, results_count=None, error=None):
    """Update job status in MongoDB."""
    update = {
        "status": status,
        "updated_at": datetime.utcnow()
    }
    if results_count is not None:
        update["results_count"] = results_count
    if error is not None:
        update["error"] = error
    
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
                "updated_at": datetime.utcnow()
            }
        }
    )

def run_scraper(job_id, config):
    """Run one scrape job via the gosom scraper and stream results into Mongo."""

    all_results = []

    def log_cb(message):
        log_to_job(job_id, message)

    def result_cb(result, current_count, total):
        # The runner already deduped on place_id, so every result here is new.
        all_results.append(result)
        save_job_results(job_id, all_results)

    def is_job_active():
        job = jobs_collection.find_one(
            {"_id": ObjectId(job_id)}, {"status": 1}
        )
        return bool(job) and job.get("status") == "running"

    runner = GmapsRunner(
        log_callback=log_cb,
        result_callback=result_cb,
        is_active=is_job_active,
    )

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
        if job_id in active_jobs:
            del active_jobs[job_id]


def run_job_thread(job_id, config):
    """Thread entrypoint. The scraper is a subprocess now, so no event loop."""
    run_scraper(job_id, config)

# Routes

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    """List all jobs, newest first. Optionally filter by user_id."""
    user_id = request.args.get("user_id")
    query = {}
    if user_id:
        query["user_id"] = user_id
        
    jobs = list(jobs_collection.find(query).sort("created_at", -1).limit(50))
    for job in jobs:
        job["_id"] = str(job["_id"])
    return jsonify(jobs)

@app.route("/api/jobs", methods=["POST"])
def create_job():
    """Create and start a new scraping job."""
    data = request.json
    config = data.get("config", {})
    # Support direct config passed as body or wrapped in "config" key
    if not config and "search_query" in data:
        config = data
    
    user_id = data.get("user_id")
    
    # Create job document
    job = {
        "user_id": user_id,
        "config": config,
        "status": "pending",
        "results_count": 0,
        "results": [],
        "logs": [],
        "error": None,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
    
    result = jobs_collection.insert_one(job)
    job_id = str(result.inserted_id)
    
    # Start job in background thread
    thread = threading.Thread(target=run_job_thread, args=(job_id, config))
    thread.daemon = True
    thread.start()
    active_jobs[job_id] = thread
    
    log_to_job(job_id, f"Job created with config: {config}")
    
    return jsonify({"job_id": job_id, "message": "Job started"})

@app.route("/api/jobs/<job_id>", methods=["GET"])
def get_job(job_id):
    """Get single job details."""
    try:
        job = jobs_collection.find_one({"_id": ObjectId(job_id)})
        if not job:
            return jsonify({"error": "Job not found"}), 404
        job["_id"] = str(job["_id"])
        # Don't return full results in list view (too large)
        job["has_results"] = len(job.get("results", [])) > 0
        return jsonify(job)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

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
        else:
            return jsonify({"message": "Job is not running"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/jobs/<job_id>/export", methods=["GET"])
def export_job_csv(job_id):
    """Export job results as CSV."""
    try:
        job = jobs_collection.find_one({"_id": ObjectId(job_id)})
        if not job:
            return jsonify({"error": "Job not found"}), 404
        
        results = job.get("results", [])
        if not results:
            return jsonify({"error": "No results to export"}), 404
        
        df = pd.DataFrame(results)

        # Lead the export with the columns people actually act on, then append
        # anything else the scraper returned.
        ordered = [c for c in CSV_COLUMNS if c in df.columns]
        ordered += [c for c in df.columns if c not in ordered]
        df = df[ordered]

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        return Response(
            csv_buffer.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=job_{job_id}.csv"}
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
    print("Starting server at http://localhost:5050")
    print(f"MongoDB: {'Connected' if client else 'Not connected'}")
    app.run(debug=True, host='0.0.0.0', port=5050, threaded=True)
