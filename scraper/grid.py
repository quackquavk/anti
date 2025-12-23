
import math

class GridGenerator:
    def generate_grid(self, center_lat, center_lon, size_km, step_km):
        """
        Generates a grid of coordinates around a center point.
        
        Args:
            center_lat: Latitude of the center point.
            center_lon: Longitude of the center point.
            size_km: Roughly the radius covers (e.g., 5km).
            step_km: Distance between grid points (e.g., 2km).
            
        Returns:
            List of (lat, lon) tuples.
        """
        coordinates = []
        
        # 1 degree of latitude is approx 111km
        # 1 degree of longitude is approx 111km * cos(latitude)
        
        lat_step_deg = step_km / 111.0
        lon_step_deg = step_km / (111.0 * math.cos(math.radians(center_lat)))
        
        # Determine number of steps
        steps = int(size_km / step_km)
        
        # Generate grid centered on 0,0 relative
        # (-steps to +steps)
        
        current_lat = center_lat - (steps * lat_step_deg)
        start_lon = center_lon - (steps * lon_step_deg)
        
        # Size of the grid (side length) = steps * 2 + 1
        
        for i in range(steps * 2 + 1):
            current_lon = start_lon
            for j in range(steps * 2 + 1):
                coordinates.append((current_lat, current_lon))
                current_lon += lon_step_deg
            current_lat += lat_step_deg
            
        print(f"Generated grid of {len(coordinates)} points around {center_lat}, {center_lon}")
        print(f"Grid range: {steps*2+1}x{steps*2+1}")
        return coordinates
