from flask import Flask, render_template, jsonify, request, Response
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import threading
import asyncio
import json
import os
import io
import pandas as pd

from scraper.engine import ScraperEngine
from scraper.storage import Storage
from scraper.grid import GridGenerator

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

async def run_scraper_async(job_id, config):
    """Async scraping logic for a specific job."""
    
    scraper = ScraperEngine(headless=config.get("headless", True))
    grid_gen = GridGenerator()
    
    all_results = []
    unique_ids = set()
    
    search_query = config.get("search_query", "restaurant")
    location = config.get("location", "Kathmandu")
    total_target = config.get("total", 10)
    grid_size = config.get("grid_size", 3)
    zoom_level = min(config.get("zoom_level", 15), 21)
    
    # Check if job is still active (not stopped)
    def is_job_active():
        job = jobs_collection.find_one({"_id": ObjectId(job_id)})
        return job and job.get("status") == "running"
    
    try:
        update_job_status(job_id, "running")
        
        if total_target > 120:
            log_to_job(job_id, f"AUTO-GRID MODE: Target {total_target} for '{search_query}' in '{location}'")
            
            coords = await scraper.get_location_coordinates(location)
            if not coords:
                log_to_job(job_id, "Failed to find coordinates. Using simple query.")
                full_query = f"{search_query} in {location}"
                results = await scraper.run(full_query, total_target)
                save_job_results(job_id, results)
                update_job_status(job_id, "completed", len(results))
                return
            
            lat, lon = coords
            log_to_job(job_id, f"Found coordinates: {lat}, {lon}")
            
            step_km = 2.0
            grid_points = grid_gen.generate_grid(lat, lon, grid_size, step_km)
            log_to_job(job_id, f"Grid generated: {len(grid_points)} tiles")
            
            for i, (g_lat, g_lon) in enumerate(grid_points):
                if not is_job_active():
                    log_to_job(job_id, "Job stopped by user.")
                    update_job_status(job_id, "stopped", len(all_results))
                    return
                    
                remaining = total_target - len(all_results)
                if remaining <= 0:
                    log_to_job(job_id, "Target reached!")
                    break
                
                log_to_job(job_id, f"[{i+1}/{len(grid_points)}] Mining: {g_lat:.4f}, {g_lon:.4f}")
                batch_target = min(remaining, 500)
                
                try:
                    batch_results = await scraper.run(search_query, batch_target, lat=g_lat, lon=g_lon, zoom=zoom_level)
                    
                    new_count = 0
                    for item in batch_results:
                        name_clean = (item['name'] or "").lower().strip()
                        phone_clean = (item['phone'] or "").strip()
                        website_clean = (item['website'] or "").strip()
                        
                        if phone_clean:
                            key = f"{name_clean}|{phone_clean}"
                        elif website_clean:
                            key = f"{name_clean}|{website_clean}"
                        else:
                            raw_snippet = (item.get('raw_text') or "")[:20].lower().strip()
                            key = f"{name_clean}|{raw_snippet}"
                        
                        if key not in unique_ids:
                            unique_ids.add(key)
                            all_results.append(item)
                            new_count += 1
                    
                    log_to_job(job_id, f"  Scraped: {len(batch_results)}, New: {new_count}, Total: {len(all_results)}")
                    save_job_results(job_id, all_results)
                    
                except Exception as e:
                    log_to_job(job_id, f"  Error: {e}")
        else:
            full_query = f"{search_query} in {location}"
            log_to_job(job_id, f"STANDARD MODE: '{full_query}', Target: {total_target}")
            
            results = await scraper.run(full_query, total_target)
            save_job_results(job_id, results)
            log_to_job(job_id, f"Completed: {len(results)} results")
            all_results = results
        
        update_job_status(job_id, "completed", len(all_results))
        log_to_job(job_id, f"Job completed with {len(all_results)} results")
        
    except Exception as e:
        log_to_job(job_id, f"ERROR: {e}")
        update_job_status(job_id, "error", error=str(e))
    
    finally:
        # Clean up from active jobs
        if job_id in active_jobs:
            del active_jobs[job_id]

def run_job_thread(job_id, config):
    """Run async scraper in a new event loop thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_scraper_async(job_id, config))
    finally:
        loop.close()

# Routes

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    """List all jobs, newest first."""
    jobs = list(jobs_collection.find().sort("created_at", -1).limit(50))
    for job in jobs:
        job["_id"] = str(job["_id"])
    return jsonify(jobs)

@app.route("/api/jobs", methods=["POST"])
def create_job():
    """Create and start a new scraping job."""
    config = request.json
    
    # Create job document
    job = {
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
