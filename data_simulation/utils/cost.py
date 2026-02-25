# data_simulation/utils/cost.py
"""
Cost calculation utilities.
Holding cost, delivery cost, shipment cost, fulfillment cost.
"""

from config.constants import (
    HOLDING_COST_RATE,
    DELIVERY_COST_PER_KM,
    DELIVERY_BASE_COST,
    SHIPMENT_COST_PER_UNIT,
    SHIPMENT_BASE_COST,
)


def calculate_holding_cost(closing_stock: int, cost_price: float) -> float:
    """
    Daily holding cost for a product at a warehouse.
    holding_cost = closing_stock * cost_price * daily_rate
    """
    return round(closing_stock * cost_price * HOLDING_COST_RATE, 2)


def calculate_inventory_value(closing_stock: int, cost_price: float) -> float:
    """
    Total inventory value at cost.
    """
    return round(closing_stock * cost_price, 2)


def calculate_delivery_cost(distance_km: float) -> float:
    """
    Cost to deliver an order.
    delivery_cost = base_cost + (distance * per_km_rate)
    """
    return round(DELIVERY_BASE_COST + (distance_km * DELIVERY_COST_PER_KM), 2)


def calculate_shipment_cost(quantity: int) -> float:
    """
    Cost for a supplier-to-warehouse shipment.
    shipment_cost = base_cost + (quantity * per_unit_rate)
    """
    return round(SHIPMENT_BASE_COST + (quantity * SHIPMENT_COST_PER_UNIT), 2)


def calculate_fulfillment_cost(delivery_cost: float, items_holding_cost: float) -> float:
    """
    Total fulfillment cost for an order.
    Simplified: delivery + proportion of holding cost.
    """
    return round(delivery_cost + items_holding_cost * 0.1, 2)


def calculate_days_of_supply(closing_stock: int, avg_daily_demand: float) -> float:
    """
    How many days of inventory remain at current demand rate.
    """
    if avg_daily_demand <= 0:
        return 99.99  # Effectively infinite
    return round(closing_stock / avg_daily_demand, 2)