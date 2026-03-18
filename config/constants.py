"""
Project-wide constants for the Fulfillment Platform simulation.
Single source of truth — every other file imports from here.

v4: Added warehouse-specific operational parameters:
    - WAREHOUSE_HOLDING_COST_MULTIPLIERS: NYC/LA higher storage costs
    - WAREHOUSE_CONGESTION_FACTORS: affects ETA variability per warehouse
    - WAREHOUSE_SLA_FAILURE_BIAS: encodes real-world on-time performance gaps
    - WAREHOUSE_UTILIZATION_TARGETS: drives driver utilization variation
    - COST_OPTIMAL_REDIRECT_PROBABILITY: per-warehouse redirect likelihood
"""

from datetime import date

RANDOM_SEED = 42

BACKFILL_START_DATE = date(2022, 2, 1)
BACKFILL_END_DATE = date(2025, 2, 1)

NUM_PRODUCTS = 500
NUM_WAREHOUSES = 8
NUM_DRIVERS = 300
NUM_CUSTOMERS = 10000
NUM_SUPPLIERS = 6
NUM_EXPERIMENTS = 10

DAILY_ORDERS = 5000
DAILY_ORDER_ITEMS = 10000
DAILY_SHIPMENTS = 500
DAILY_EXPERIMENT_ASSIGNMENTS = 2000

ANNUAL_GROWTH_RATE = 0.10
ANNUAL_PRICE_INFLATION = 0.04
BASE_YEAR = 2022


def get_year_growth_multiplier(year: int) -> float:
    years_elapsed = max(0, year - BASE_YEAR)
    return (1 + ANNUAL_GROWTH_RATE) ** years_elapsed


def get_price_inflation_multiplier(year: int) -> float:
    years_elapsed = max(0, year - BASE_YEAR)
    return (1 + ANNUAL_PRICE_INFLATION) ** years_elapsed


# ── Warehouse demand weights ───────────────────────────────────
# Higher = more orders assigned to this warehouse
# LA-West and NYC-East are major population centers
WAREHOUSE_DEMAND_WEIGHTS = {
    "WH-001": 1.35,  # NYC-East: dense metro, high demand
    "WH-002": 1.40,  # LA-West: largest market
    "WH-003": 1.15,  # CHI-Central
    "WH-004": 1.10,  # DAL-South
    "WH-005": 0.75,  # SEA-NW: smaller market
    "WH-006": 0.90,  # MIA-SE
    "WH-007": 0.70,  # DEN-Mountain: smallest market
    "WH-008": 0.95,  # ATL-Mid
}

# Maps warehouse to customer region column value in dim_customer
WAREHOUSE_REGION_MAP = {
    "WH-001": "Northeast",
    "WH-002": "West",
    "WH-003": "Midwest",
    "WH-004": "South",
    "WH-005": "Northwest",
    "WH-006": "Southeast",
    "WH-007": "Mountain",
    "WH-008": "Mid-Atlantic",
}

# ── Category demand weights ────────────────────────────────────
# Electronics/Health dominate; Grocery/Beauty lower
CATEGORY_DEMAND_WEIGHTS = {
    "Electronics": 2.20,
    "Home & Garden": 1.40,
    "Health": 1.30,
    "Sports": 1.10,
    "Apparel": 1.00,
    "Toys": 0.90,
    "Beauty": 0.80,
    "Grocery": 0.60,
}

# ── Warehouse distance factors ─────────────────────────────────
# Applied to ETA calculation: higher = longer effective travel time
# Reflects road network quality, urban density, traffic
WAREHOUSE_DISTANCE_FACTORS = {
    "WH-001": 1.45,  # NYC: dense urban, heavy traffic
    "WH-002": 1.35,  # LA: sprawl + freeway congestion
    "WH-003": 1.15,  # Chicago: moderate urban
    "WH-004": 1.10,  # Dallas: good road network
    "WH-005": 1.25,  # Seattle: hills + traffic
    "WH-006": 1.10,  # Miami: flat, good roads
    "WH-007": 1.05,  # Denver: suburban, low congestion
    "WH-008": 1.15,  # Atlanta: highway-dependent
}

# ── Warehouse congestion factors ───────────────────────────────
# Controls ETA variability (std dev in normal distribution).
# NYC has high unpredictability; Denver is very consistent.
# Used in deliveries.py: variability = rng.normal(1.0, congestion_factor)
WAREHOUSE_CONGESTION_FACTORS = {
    "WH-001": 0.35,  # NYC: very unpredictable, high variance
    "WH-002": 0.28,  # LA: moderate-high variance
    "WH-003": 0.20,  # Chicago: moderate
    "WH-004": 0.15,  # Dallas: low variance
    "WH-005": 0.25,  # Seattle: weather + traffic
    "WH-006": 0.18,  # Miami: moderate
    "WH-007": 0.10,  # Denver: very consistent, low variance
    "WH-008": 0.17,  # Atlanta: moderate
}

# ── Warehouse SLA failure bias ─────────────────────────────────
# Additional probability of SLA breach beyond what distance predicts.
# Reflects real-world factors: parking, access restrictions, driver experience.
# NYC has 20% additional SLA failure risk; Denver near zero.
WAREHOUSE_SLA_FAILURE_BIAS = {
    "WH-001": 0.20,  # NYC: access restrictions, parking, dense streets
    "WH-002": 0.12,  # LA: traffic unpredictability
    "WH-003": 0.05,  # Chicago: moderate
    "WH-004": 0.04,  # Dallas: good access
    "WH-005": 0.10,  # Seattle: weather events
    "WH-006": 0.06,  # Miami: weather + tourism traffic
    "WH-007": 0.02,  # Denver: suburban, easy access
    "WH-008": 0.05,  # Atlanta: highway-dependent but manageable
}

# ── Warehouse holding cost multipliers ────────────────────────
# Reflects real estate and operational cost differences across US cities.
# NYC and LA have significantly higher warehouse costs per sq ft.
WAREHOUSE_HOLDING_COST_MULTIPLIERS = {
    "WH-001": 1.45,  # NYC: highest real estate cost in US
    "WH-002": 1.35,  # LA: second highest
    "WH-003": 1.10,  # Chicago: above average
    "WH-004": 0.95,  # Dallas: below average, business-friendly
    "WH-005": 1.20,  # Seattle: tech hub premium
    "WH-006": 1.00,  # Miami: baseline
    "WH-007": 0.85,  # Denver: lowest, suburban
    "WH-008": 0.95,  # Atlanta: below average
}

# ── Warehouse utilization targets ─────────────────────────────
# Base driver utilization before adjustments.
# High demand warehouses have more deliveries per driver = higher utilization.
# Used in driver_activity.py to add warehouse-specific utilization floor.
WAREHOUSE_UTILIZATION_TARGETS = {
    "WH-001": 0.94,  # NYC: drivers fully stretched
    "WH-002": 0.88,  # LA: high utilization
    "WH-003": 0.82,  # Chicago: moderate-high
    "WH-004": 0.79,  # Dallas: moderate
    "WH-005": 0.85,  # Seattle: high
    "WH-006": 0.76,  # Miami: moderate
    "WH-007": 0.68,  # Denver: lowest utilization
    "WH-008": 0.81,  # Atlanta: moderate
}

# ── Cost-optimal redirect probability per warehouse ────────────
# When allocation_strategy = cost_optimal, probability of redirecting
# to a non-nearest warehouse. High-demand warehouses redirect more.
COST_OPTIMAL_REDIRECT_PROBABILITY = {
    "WH-001": 0.55,  # NYC: frequently over capacity, redirects many orders
    "WH-002": 0.50,  # LA: similar
    "WH-003": 0.30,  # Chicago: moderate
    "WH-004": 0.25,  # Dallas: mostly serves nearest
    "WH-005": 0.20,  # Seattle: small market, less redirection
    "WH-006": 0.28,  # Miami: moderate
    "WH-007": 0.15,  # Denver: lowest, rarely redirected
    "WH-008": 0.25,  # Atlanta: moderate
}

SHOCK_EVENT_PROBABILITY = 0.03

SHOCK_EVENTS = [
    {
        "type": "demand_spike",
        "multiplier": 1.8,
        "probability": 0.30,
        "duration_days": 2,
        "description": "Flash sale or viral product — 80% demand surge for 2 days",
    },
    {
        "type": "holiday_rush",
        "multiplier": 1.5,
        "probability": 0.20,
        "duration_days": 3,
        "description": "Unexpected holiday demand — 50% surge for 3 days",
    },
    {
        "type": "supplier_delay",
        "multiplier": 0.4,
        "probability": 0.25,
        "duration_days": 4,
        "description": "Supplier delay — only 40% of normal shipments arrive for 4 days",
    },
    {
        "type": "warehouse_outage",
        "multiplier": 0.2,
        "probability": 0.15,
        "duration_days": 2,
        "description": "Warehouse system outage — 80% capacity loss for 2 days",
    },
    {
        "type": "driver_shortage",
        "multiplier": 0.7,
        "probability": 0.10,
        "duration_days": 3,
        "description": "Driver shortage — 30% delivery capacity reduction",
    },
]

PRODUCT_CATEGORIES = {
    "Electronics": ["Smartphones", "Laptops", "Headphones", "Tablets", "Cameras", "Chargers"],
    "Grocery": ["Snacks", "Beverages", "Dairy", "Canned Goods", "Frozen", "Condiments"],
    "Apparel": ["Men's Clothing", "Women's Clothing", "Shoes", "Accessories", "Activewear"],
    "Home & Garden": ["Furniture", "Kitchen", "Bedding", "Tools", "Decor", "Lighting"],
    "Health": ["Vitamins", "Personal Care", "First Aid", "Fitness", "Supplements"],
    "Toys": ["Action Figures", "Board Games", "Puzzles", "Outdoor Toys", "Educational"],
    "Sports": ["Equipment", "Footwear", "Apparel", "Accessories", "Nutrition"],
    "Beauty": ["Skincare", "Makeup", "Hair Care", "Fragrances", "Bath & Body"],
}

PRODUCTS_PER_CATEGORY = {
    "Electronics": 75,
    "Grocery": 70,
    "Apparel": 65,
    "Home & Garden": 65,
    "Health": 60,
    "Toys": 55,
    "Sports": 55,
    "Beauty": 55,
}

CATEGORY_PRICE_RANGES = {
    "Electronics": (15.00, 500.00),
    "Grocery": (1.50, 25.00),
    "Apparel": (8.00, 120.00),
    "Home & Garden": (5.00, 300.00),
    "Health": (3.00, 60.00),
    "Toys": (5.00, 80.00),
    "Sports": (10.00, 200.00),
    "Beauty": (4.00, 75.00),
}

MARKUP_RANGE = (1.25, 2.50)

CATEGORY_WEIGHT_RANGES = {
    "Electronics": (0.1, 5.0),
    "Grocery": (0.2, 8.0),
    "Apparel": (0.1, 2.0),
    "Home & Garden": (0.5, 25.0),
    "Health": (0.1, 3.0),
    "Toys": (0.2, 5.0),
    "Sports": (0.3, 15.0),
    "Beauty": (0.1, 2.0),
}

CATEGORY_LEAD_TIME_RANGES = {
    "Electronics": (3, 10),
    "Grocery": (1, 5),
    "Apparel": (3, 8),
    "Home & Garden": (5, 14),
    "Health": (2, 7),
    "Toys": (3, 10),
    "Sports": (3, 10),
    "Beauty": (2, 7),
}

PERISHABLE_CATEGORIES = ["Grocery"]

CUSTOMER_SEGMENTS = {
    "Premium": 0.15,
    "Regular": 0.55,
    "Occasional": 0.30,
}

SEGMENT_FREQUENCY_RANGES = {
    "Premium": (0.70, 1.00),
    "Regular": (0.30, 0.69),
    "Occasional": (0.05, 0.29),
}

ORDER_PRIORITY_DISTRIBUTION = {
    "Standard": 0.60,
    "Express": 0.30,
    "Same-Day": 0.10,
}

SLA_MINUTES = {
    "Standard": 2880,
    "Express": 480,
    "Same-Day": 240,
}

ORDER_STATUS_DISTRIBUTION = {
    "Delivered": 0.85,
    "Shipped": 0.05,
    "Processing": 0.03,
    "Pending": 0.02,
    "Cancelled": 0.05,
}

RETURN_RATE = 0.08

DELIVERY_STATUS_DISTRIBUTION = {
    "Delivered": 0.88,
    "In Transit": 0.05,
    "Assigned": 0.03,
    "Failed": 0.04,
}

VEHICLE_TYPES = {
    "Van": {"proportion": 0.45, "capacity": (15, 25), "speed": (30, 50)},
    "Truck": {"proportion": 0.25, "capacity": (25, 40), "speed": (25, 40)},
    "Car": {"proportion": 0.20, "capacity": (8, 15), "speed": (35, 55)},
    "Bike": {"proportion": 0.10, "capacity": (3, 8), "speed": (15, 25)},
}

DRIVER_STATUS_DISTRIBUTION = {
    "Active": 0.85,
    "On Leave": 0.10,
    "Inactive": 0.05,
}

SUPPLIER_CONFIGS = [
    {
        "name": "FastShip Co",
        "region": "Northeast",
        "lead_time": 4,
        "std_dev": 1.2,
        "reliability": 0.92,
        "categories": "Electronics,Toys",
    },
    {
        "name": "GlobalParts Inc",
        "region": "West",
        "lead_time": 6,
        "std_dev": 2.0,
        "reliability": 0.85,
        "categories": "Electronics,Sports",
    },
    {
        "name": "PrimeDistributors",
        "region": "Southeast",
        "lead_time": 3,
        "std_dev": 0.8,
        "reliability": 0.95,
        "categories": "Grocery,Health",
    },
    {
        "name": "EcoSupply Ltd",
        "region": "Midwest",
        "lead_time": 5,
        "std_dev": 1.5,
        "reliability": 0.88,
        "categories": "Home & Garden,Beauty",
    },
    {
        "name": "MegaLogistics",
        "region": "South",
        "lead_time": 7,
        "std_dev": 2.5,
        "reliability": 0.80,
        "categories": "Apparel,Home & Garden",
    },
    {
        "name": "SwiftFreight",
        "region": "Northwest",
        "lead_time": 4,
        "std_dev": 1.0,
        "reliability": 0.93,
        "categories": "Beauty,Health,Grocery",
    },
]

EXPERIMENT_CONFIGS = [
    {
        "name": "Inventory Policy: Dynamic vs Static Reorder",
        "strategy": "dynamic_reorder",
        "type": "inventory_policy",
        "warehouses": "WH-001,WH-002,WH-003,WH-004",
    },
    {
        "name": "Inventory Policy: Safety Stock +20%",
        "strategy": "high_safety_stock",
        "type": "inventory_policy",
        "warehouses": "WH-005,WH-006,WH-007,WH-008",
    },
    {
        "name": "Routing: Greedy vs Balanced",
        "strategy": "balanced_routing",
        "type": "routing_algorithm",
        "warehouses": "WH-001,WH-002,WH-003,WH-004",
    },
    {
        "name": "Routing: Nearest Driver vs Load-Balanced",
        "strategy": "load_balanced_driver",
        "type": "routing_algorithm",
        "warehouses": "WH-005,WH-006,WH-007,WH-008",
    },
    {
        "name": "Allocation: Nearest vs Cost-Optimal Warehouse",
        "strategy": "cost_optimal_allocation",
        "type": "warehouse_allocation",
        "warehouses": "WH-001,WH-002,WH-003,WH-004,WH-005,WH-006,WH-007,WH-008",
    },
    {
        "name": "Allocation: Capacity-Aware Assignment",
        "strategy": "capacity_aware",
        "type": "warehouse_allocation",
        "warehouses": "WH-001,WH-003,WH-005,WH-007",
    },
    {
        "name": "Inventory Policy: Just-In-Time Reorder",
        "strategy": "jit_reorder",
        "type": "inventory_policy",
        "warehouses": "WH-002,WH-004,WH-006,WH-008",
    },
    {
        "name": "Routing: Priority-Based Driver Assignment",
        "strategy": "priority_routing",
        "type": "routing_algorithm",
        "warehouses": "WH-001,WH-002,WH-005,WH-006",
    },
    {
        "name": "Allocation: Region-Locked vs Flexible",
        "strategy": "flexible_allocation",
        "type": "warehouse_allocation",
        "warehouses": "WH-003,WH-004,WH-007,WH-008",
    },
    {
        "name": "Inventory Policy: ML-Driven Reorder Points",
        "strategy": "ml_reorder",
        "type": "inventory_policy",
        "warehouses": "WH-001,WH-002,WH-003,WH-004,WH-005,WH-006,WH-007,WH-008",
    },
]

HOLDING_COST_RATE = 0.001
DELIVERY_COST_PER_KM = 0.85
DELIVERY_BASE_COST = 3.50
SHIPMENT_COST_PER_UNIT = 0.50
SHIPMENT_BASE_COST = 25.00
STOCKOUT_COST_MULTIPLIER = 2.0

ALLOCATION_STRATEGIES = {
    "nearest": 0.65,
    "cost_optimal": 0.20,
    "load_balanced": 0.15,
}

INITIAL_STOCK_RANGE = (50, 500)
REORDER_POINT_RANGE = (20, 100)
SAFETY_STOCK_RANGE = (10, 50)
REORDER_QUANTITY_RANGE = (50, 200)

DISCOUNT_PROBABILITY = 0.15
DISCOUNT_RANGE = (0.05, 0.25)

US_HOLIDAYS = {
    (1, 1),
    (1, 17),
    (2, 21),
    (5, 30),
    (7, 4),
    (9, 5),
    (11, 24),
    (12, 25),
}

# ── Category growth modifiers ──────────────────────────────────
# Electronics grows faster YoY; Grocery flat/declining.
# Creates diverging category revenue lines over 4 years.
CATEGORY_GROWTH_MODIFIERS = {
    "Electronics": 1.04,
    "Health": 1.03,
    "Beauty": 1.02,
    "Sports": 1.01,
    "Home & Garden": 1.01,
    "Apparel": 1.00,
    "Toys": 1.00,
    "Grocery": 0.98,
}

S3_BUCKET_NAME = "last-mile-fulfillment-platform"
S3_RAW_PREFIX = "raw"
S3_STATE_PREFIX = "state"

BATCH_ID_PREFIX = "batch"
