# data_simulation/utils/geo.py

"""
Geographic utility functions.
Haversine distance, nearest warehouse, customer location generation.
"""

import numpy as np
from math import radians, cos, sin, asin, sqrt
from typing import Tuple, Dict, List

from config.warehouse_config import WAREHOUSE_COORDS, WAREHOUSE_IDS


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points on Earth (in km).
    Uses the Haversine formula.
    """
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    
    earth_radius_km = 6371.0
    return earth_radius_km * c


def find_nearest_warehouse(customer_lat: float, customer_lon: float) -> str:
    """
    Find the nearest warehouse to a customer location.
    Returns warehouse_id.
    """
    min_dist = float("inf")
    nearest_wh = WAREHOUSE_IDS[0]
    
    for wh_id, (wh_lat, wh_lon) in WAREHOUSE_COORDS.items():
        dist = haversine_km(customer_lat, customer_lon, wh_lat, wh_lon)
        if dist < min_dist:
            min_dist = dist
            nearest_wh = wh_id
    
    return nearest_wh


def get_delivery_distance(warehouse_id: str, customer_lat: float, customer_lon: float) -> float:
    """
    Calculate delivery distance from warehouse to customer.
    Adds a 1.3x road factor to haversine (straight-line) distance.
    """
    wh_lat, wh_lon = WAREHOUSE_COORDS[warehouse_id]
    straight_line = haversine_km(wh_lat, wh_lon, customer_lat, customer_lon)
    
    # Road distance is typically 1.2-1.4x straight-line distance
    road_factor = 1.3
    return round(straight_line * road_factor, 2)


def generate_customer_location(warehouse_id: str, rng: np.random.Generator) -> Tuple[float, float]:
    """
    Generate a customer location near a specific warehouse.
    Customers are within ~50-150km of their regional warehouse.
    """
    wh_lat, wh_lon = WAREHOUSE_COORDS[warehouse_id]
    
    # Random offset: ~0.1 to 1.5 degrees (~10-150km)
    lat_offset = rng.uniform(-1.2, 1.2)
    lon_offset = rng.uniform(-1.2, 1.2)
    
    customer_lat = round(wh_lat + lat_offset, 6)
    customer_lon = round(wh_lon + lon_offset, 6)
    
    # Clamp to valid US coordinates
    customer_lat = max(24.5, min(49.0, customer_lat))
    customer_lon = max(-125.0, min(-66.0, customer_lon))
    
    return customer_lat, customer_lon


# ── City names near each warehouse region (for dim_customer) ──
REGIONAL_CITIES = {
    "WH-001": ["New York", "Newark", "Jersey City", "Brooklyn", "Stamford", "Hoboken", "White Plains", "Yonkers"],
    "WH-002": ["Los Angeles", "Long Beach", "Pasadena", "Glendale", "Santa Monica", "Burbank", "Torrance", "Pomona"],
    "WH-003": ["Chicago", "Naperville", "Aurora", "Evanston", "Schaumburg", "Joliet", "Elgin", "Oak Park"],
    "WH-004": ["Dallas", "Fort Worth", "Arlington", "Plano", "Garland", "Irving", "Frisco", "McKinney"],
    "WH-005": ["Seattle", "Tacoma", "Bellevue", "Everett", "Renton", "Redmond", "Kent", "Kirkland"],
    "WH-006": ["Miami", "Fort Lauderdale", "Hollywood", "Hialeah", "Coral Springs", "Pompano Beach", "Pembroke Pines", "Boca Raton"],
    "WH-007": ["Denver", "Aurora", "Lakewood", "Boulder", "Thornton", "Arvada", "Westminster", "Centennial"],
    "WH-008": ["Atlanta", "Marietta", "Roswell", "Sandy Springs", "Alpharetta", "Decatur", "Smyrna", "Kennesaw"],
}