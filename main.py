
import argparse
import asyncio
import json
import os
from scraper.engine import ScraperEngine
from scraper.storage import Storage
from scraper.grid import GridGenerator

def load_config():
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config.json: {e}")
    return {}

async def main():
    config = load_config()
    
    parser = argparse.ArgumentParser(description="Google Maps Data Scraper")
    parser.add_argument("search_term", nargs='?', help="Search term/Query")
    parser.add_argument("--total", type=int, help="Number of results to scrape")
    parser.add_argument("--visible", action="store_true", help="Run in visible mode")
    
    args = parser.parse_args()
    
    config_total = config.get("total", 10)
    total_target = args.total if args.total is not None else config_total
    
    if args.visible:
        headless = False
    else:
        headless = config.get("headless", True)

    cli_search_term = args.search_term
    
    scraper = ScraperEngine(headless=headless)
    storage = Storage()
    grid_gen = GridGenerator()
    
    all_results = []
    unique_ids = set()
    
    search_query = config.get("search_query", "restaurant")
    location = config.get("location", "Kathmandu")
    
    # Grid Config
    grid_size = config.get("grid_size", 3) 
    zoom_level = min(config.get("zoom_level", 15), 21) # Clamp max zoom to 21
    
    # CLI Override or Simple Mode
    if cli_search_term:
        print(f"--- SIMPLE MODE (CLI override) ---")
        print(f"Term: {cli_search_term}, Target: {total_target}")
        results = await scraper.run(cli_search_term, total_target)
        storage.save_to_csv(results)
        return

    # Auto-Grid Mode Check
    if total_target > 120:
        print(f"--- AUTO-GRID MINING MODE ACTIVATED ---")
        print(f"Target: {total_target} results for '{search_query}' in '{location}'")
        
        # 1. Discover Location
        print(f"Discovering coordinates for {location}...")
        coords = await scraper.get_location_coordinates(location)
        
        if not coords:
            print("Failed to find location coordinates. Fallback to simple query.")
            full_query = f"{search_query} in {location}"
            results = await scraper.run(full_query, total_target)
            storage.save_to_csv(results)
            return
            
        lat, lon = coords
        
        # 2. Generate Grid
        step_km = 2.0 # Standard step for neighborhood zoom
        
        # Estimate needed tiles
        needed_tiles = (total_target // 100) + 1
        current_steps = int(grid_size / step_km)
        current_tiles = (current_steps * 2 + 1) ** 2
        
        if current_tiles < needed_tiles:
            print(f"Config 'grid_size'={grid_size} only yields {current_tiles} tiles.")
            print(f"Auto-expanding grid to meet target of ~{needed_tiles} tiles...")
            import math
            needed_steps = math.ceil((math.sqrt(needed_tiles) - 1) / 2)
            grid_size = needed_steps * step_km + 1 # Adjust grid_size to fit
            print(f"New grid_size: {grid_size} km (Radius)")

        grid_points = grid_gen.generate_grid(lat, lon, grid_size, step_km)
        
        print(f"Grid generated: {len(grid_points)} tiles to mine.")
        
        # 3. Mine Grid
        for i, (g_lat, g_lon) in enumerate(grid_points):
            remaining = total_target - len(all_results)
            if remaining <= 0:
                print("Target reached!")
                break
                
            print(f"\n[{i+1}/{len(grid_points)}] Mining Tile: {g_lat:.4f}, {g_lon:.4f} (Zoom {zoom_level})")
            
            # Batch size limited to ~120 per scroll limit, ask for 500 to be safe
            batch_target = min(remaining, 500)
            
            try:
                # Run scraper on coordinate
                # search_term is just the query e.g. "restaurant" because map is already centered
                batch_results = await scraper.run(search_query, batch_target, lat=g_lat, lon=g_lon, zoom=zoom_level)
                
                new_count = 0
                for item in batch_results:
                    name_clean = (item['name'] or "").lower().strip()
                    phone_clean = (item['phone'] or "").strip()
                    website_clean = (item['website'] or "").strip()
                    
                    # Create a robust key
                    if phone_clean:
                        key = f"{name_clean}|{phone_clean}"
                    elif website_clean:
                        key = f"{name_clean}|{website_clean}"
                    else:
                        # Fallback for data-poor results: Name + first 20 chars of raw text (usually address)
                        raw_snippet = (item.get('raw_text') or "")[:20].lower().strip()
                        key = f"{name_clean}|{raw_snippet}"
                    
                    if key not in unique_ids:
                        unique_ids.add(key)
                        all_results.append(item)
                        new_count += 1
                
                print(f"  > Scraped: {len(batch_results)}, New Unique: {new_count}, Total Unique: {len(all_results)}")
                
                storage.save_to_csv(all_results)
                
            except Exception as e:
                print(f"  > Error mining tile: {e}")
                
    else:
        # Standard fallback
        full_query = f"{search_query} in {location}"
        print(f"--- STANDARD MODE ---")
        print(f"Query: {full_query}, Target: {total_target}")
        results = await scraper.run(full_query, total_target)
        storage.save_to_csv(results)

if __name__ == "__main__":
    asyncio.run(main())
