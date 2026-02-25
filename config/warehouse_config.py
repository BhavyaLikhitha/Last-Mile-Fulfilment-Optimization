"""
Warehouse configuration for 8 US regional fulfillment centers.
Real city coordinates for accurate distance calculations.
"""

WAREHOUSES = [
    {
        "warehouse_id": "WH-001",
        "warehouse_name": "NYC-East",
        "region": "Northeast",
        "city": "Newark",
        "state": "NJ",
        "latitude": 40.735657,
        "longitude": -74.172367,
        "capacity_units": 75000,
        "operating_cost_per_day": 4500.00,
        "drivers": 40,
    },
    {
        "warehouse_id": "WH-002",
        "warehouse_name": "LA-West",
        "region": "West",
        "city": "Los Angeles",
        "state": "CA",
        "latitude": 33.942791,
        "longitude": -118.267586,
        "capacity_units": 80000,
        "operating_cost_per_day": 5000.00,
        "drivers": 42,
    },
    {
        "warehouse_id": "WH-003",
        "warehouse_name": "CHI-Central",
        "region": "Midwest",
        "city": "Chicago",
        "state": "IL",
        "latitude": 41.878114,
        "longitude": -87.629798,
        "capacity_units": 70000,
        "operating_cost_per_day": 4000.00,
        "drivers": 38,
    },
    {
        "warehouse_id": "WH-004",
        "warehouse_name": "DAL-South",
        "region": "South",
        "city": "Dallas",
        "state": "TX",
        "latitude": 32.776664,
        "longitude": -96.796988,
        "capacity_units": 72000,
        "operating_cost_per_day": 3800.00,
        "drivers": 36,
    },
    {
        "warehouse_id": "WH-005",
        "warehouse_name": "SEA-NW",
        "region": "Northwest",
        "city": "Seattle",
        "state": "WA",
        "latitude": 47.606209,
        "longitude": -122.332071,
        "capacity_units": 60000,
        "operating_cost_per_day": 4200.00,
        "drivers": 34,
    },
    {
        "warehouse_id": "WH-006",
        "warehouse_name": "MIA-SE",
        "region": "Southeast",
        "city": "Miami",
        "state": "FL",
        "latitude": 25.761680,
        "longitude": -80.191790,
        "capacity_units": 65000,
        "operating_cost_per_day": 3600.00,
        "drivers": 35,
    },
    {
        "warehouse_id": "WH-007",
        "warehouse_name": "DEN-Mountain",
        "region": "Mountain",
        "city": "Denver",
        "state": "CO",
        "latitude": 39.739236,
        "longitude": -104.990251,
        "capacity_units": 58000,
        "operating_cost_per_day": 3400.00,
        "drivers": 33,
    },
    {
        "warehouse_id": "WH-008",
        "warehouse_name": "ATL-Mid",
        "region": "Mid-Atlantic",
        "city": "Atlanta",
        "state": "GA",
        "latitude": 33.748996,
        "longitude": -84.387982,
        "capacity_units": 68000,
        "operating_cost_per_day": 3700.00,
        "drivers": 37,
    },
]

# Quick lookups
WAREHOUSE_IDS = [w["warehouse_id"] for w in WAREHOUSES]
WAREHOUSE_COORDS = {w["warehouse_id"]: (w["latitude"], w["longitude"]) for w in WAREHOUSES}
WAREHOUSE_CAPACITY = {w["warehouse_id"]: w["capacity_units"] for w in WAREHOUSES}
WAREHOUSE_REGIONS = {w["warehouse_id"]: w["region"] for w in WAREHOUSES}
DRIVERS_PER_WAREHOUSE = {w["warehouse_id"]: w["drivers"] for w in WAREHOUSES}

# Total drivers: 40+42+38+36+34+35+33+37 = 295 (~300)
TOTAL_DRIVERS = sum(w["drivers"] for w in WAREHOUSES)