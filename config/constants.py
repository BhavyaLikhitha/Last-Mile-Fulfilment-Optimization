"""
Project-wide constants for the Fulfillment Platform simulation.
Single source of truth — every other file imports from here.
"""

from datetime import date

# ── Random Seed (reproducibility) ──
RANDOM_SEED = 42

# ── Date Range ──
BACKFILL_START_DATE = date(2022, 2, 1)
BACKFILL_END_DATE = date(2025, 2, 1)

# ── Entity Counts ──
NUM_PRODUCTS = 500
NUM_WAREHOUSES = 8
NUM_DRIVERS = 300
NUM_CUSTOMERS = 10000
NUM_SUPPLIERS = 6
NUM_EXPERIMENTS = 10

# ── Daily Volumes ──
DAILY_ORDERS = 5000
DAILY_ORDER_ITEMS = 10000  # ~2 items per order average
DAILY_SHIPMENTS = 500
DAILY_EXPERIMENT_ASSIGNMENTS = 2000

# ── Product Categories & Subcategories ──
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

# Products per category (distributes 500 across 8 categories)
PRODUCTS_PER_CATEGORY = {
    "Electronics": 75,
    "Grocery": 70,
    "Apparel": 65,
    "Home & Garden": 65,
    "Health": 60,
    "Toys": 55,
    "Sports": 55,
    "Beauty": 55,
}  # Total = 500

# ── Product Price Ranges (cost_price) by Category ──
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

# Markup range (selling_price = cost_price * markup)
MARKUP_RANGE = (1.25, 2.50)

# ── Product Weight Ranges (kg) by Category ──
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

# ── Lead Time Ranges (days) by Category ──
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

# ── Perishable Categories ──
PERISHABLE_CATEGORIES = ["Grocery"]

# ── Customer Segments ──
CUSTOMER_SEGMENTS = {
    "Premium": 0.15,     # 15% of customers
    "Regular": 0.55,     # 55% of customers
    "Occasional": 0.30,  # 30% of customers
}

# Order frequency score ranges by segment
SEGMENT_FREQUENCY_RANGES = {
    "Premium": (0.70, 1.00),
    "Regular": (0.30, 0.69),
    "Occasional": (0.05, 0.29),
}

# ── Order Priority Distribution ──
ORDER_PRIORITY_DISTRIBUTION = {
    "Standard": 0.60,   # 60% of orders
    "Express": 0.30,    # 30% of orders
    "Same-Day": 0.10,   # 10% of orders
}

# ── SLA Definitions (minutes) ──
SLA_MINUTES = {
    "Standard": 2880,   # 48 hours
    "Express": 480,     # 8 hours
    "Same-Day": 240,    # 4 hours
}

# ── Order Status Probabilities ──
ORDER_STATUS_DISTRIBUTION = {
    "Delivered": 0.85,
    "Shipped": 0.05,
    "Processing": 0.03,
    "Pending": 0.02,
    "Cancelled": 0.05,
}

# ── Return Rate ──
RETURN_RATE = 0.08  # 8% of delivered orders get returned

# ── Delivery Status Distribution ──
DELIVERY_STATUS_DISTRIBUTION = {
    "Delivered": 0.88,
    "In Transit": 0.05,
    "Assigned": 0.03,
    "Failed": 0.04,
}

# ── Driver Configuration ──
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

# ── Supplier Configuration ──
SUPPLIER_CONFIGS = [
    {"name": "FastShip Co", "region": "Northeast", "lead_time": 4, "std_dev": 1.2, "reliability": 0.92, "categories": "Electronics,Toys"},
    {"name": "GlobalParts Inc", "region": "West", "lead_time": 6, "std_dev": 2.0, "reliability": 0.85, "categories": "Electronics,Sports"},
    {"name": "PrimeDistributors", "region": "Southeast", "lead_time": 3, "std_dev": 0.8, "reliability": 0.95, "categories": "Grocery,Health"},
    {"name": "EcoSupply Ltd", "region": "Midwest", "lead_time": 5, "std_dev": 1.5, "reliability": 0.88, "categories": "Home & Garden,Beauty"},
    {"name": "MegaLogistics", "region": "South", "lead_time": 7, "std_dev": 2.5, "reliability": 0.80, "categories": "Apparel,Home & Garden"},
    {"name": "SwiftFreight", "region": "Northwest", "lead_time": 4, "std_dev": 1.0, "reliability": 0.93, "categories": "Beauty,Health,Grocery"},
]

# ── Experiment Configuration ──
EXPERIMENT_CONFIGS = [
    {"name": "Inventory Policy: Dynamic vs Static Reorder", "strategy": "dynamic_reorder", "type": "inventory_policy", "warehouses": "WH-001,WH-002,WH-003,WH-004"},
    {"name": "Inventory Policy: Safety Stock +20%", "strategy": "high_safety_stock", "type": "inventory_policy", "warehouses": "WH-005,WH-006,WH-007,WH-008"},
    {"name": "Routing: Greedy vs Balanced", "strategy": "balanced_routing", "type": "routing_algorithm", "warehouses": "WH-001,WH-002,WH-003,WH-004"},
    {"name": "Routing: Nearest Driver vs Load-Balanced", "strategy": "load_balanced_driver", "type": "routing_algorithm", "warehouses": "WH-005,WH-006,WH-007,WH-008"},
    {"name": "Allocation: Nearest vs Cost-Optimal Warehouse", "strategy": "cost_optimal_allocation", "type": "warehouse_allocation", "warehouses": "WH-001,WH-002,WH-003,WH-004,WH-005,WH-006,WH-007,WH-008"},
    {"name": "Allocation: Capacity-Aware Assignment", "strategy": "capacity_aware", "type": "warehouse_allocation", "warehouses": "WH-001,WH-003,WH-005,WH-007"},
    {"name": "Inventory Policy: Just-In-Time Reorder", "strategy": "jit_reorder", "type": "inventory_policy", "warehouses": "WH-002,WH-004,WH-006,WH-008"},
    {"name": "Routing: Priority-Based Driver Assignment", "strategy": "priority_routing", "type": "routing_algorithm", "warehouses": "WH-001,WH-002,WH-005,WH-006"},
    {"name": "Allocation: Region-Locked vs Flexible", "strategy": "flexible_allocation", "type": "warehouse_allocation", "warehouses": "WH-003,WH-004,WH-007,WH-008"},
    {"name": "Inventory Policy: ML-Driven Reorder Points", "strategy": "ml_reorder", "type": "inventory_policy", "warehouses": "WH-001,WH-002,WH-003,WH-004,WH-005,WH-006,WH-007,WH-008"},
]

# ── Cost Parameters ──
HOLDING_COST_RATE = 0.001  # 0.1% of product cost per day
DELIVERY_COST_PER_KM = 0.85  # $ per km
DELIVERY_BASE_COST = 3.50  # fixed base cost per delivery
SHIPMENT_COST_PER_UNIT = 0.50  # $ per unit shipped
SHIPMENT_BASE_COST = 25.00  # fixed base cost per shipment
STOCKOUT_COST_MULTIPLIER = 2.0  # 2x the selling price as stockout penalty

# ── Allocation Strategy ──
ALLOCATION_STRATEGIES = {
    "nearest": 0.65,       # 65% of orders go to nearest warehouse
    "cost_optimal": 0.20,  # 20% optimized for cost
    "load_balanced": 0.15, # 15% balanced across warehouses
}

# ── Inventory Initialization ──
INITIAL_STOCK_RANGE = (50, 500)  # starting inventory per product per warehouse
REORDER_POINT_RANGE = (20, 100)
SAFETY_STOCK_RANGE = (10, 50)
REORDER_QUANTITY_RANGE = (50, 200)

# ── Discount Parameters ──
DISCOUNT_PROBABILITY = 0.15  # 15% of order items get a discount
DISCOUNT_RANGE = (0.05, 0.25)  # 5% to 25% discount

# ── US Holiday Dates (for dim_date) ──
US_HOLIDAYS = {
    # New Year's Day
    (1, 1),
    # MLK Day (3rd Monday of January) - approximate
    (1, 17),
    # Presidents' Day (3rd Monday of February) - approximate
    (2, 21),
    # Memorial Day (last Monday of May) - approximate
    (5, 30),
    # Independence Day
    (7, 4),
    # Labor Day (1st Monday of September) - approximate
    (9, 5),
    # Thanksgiving (4th Thursday of November) - approximate
    (11, 24),
    # Christmas
    (12, 25),
}

# ── S3 Configuration ──
S3_BUCKET_NAME = "last-mile-fulfillment-platform"
S3_RAW_PREFIX = "raw"
S3_STATE_PREFIX = "state"

# ── Batch ID Format ──
BATCH_ID_PREFIX = "batch"