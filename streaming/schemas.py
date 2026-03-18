"""Pydantic models for Kafka event validation."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class OrderItem(BaseModel):
    order_item_id: str
    product_id: str
    quantity: int = Field(ge=1)
    unit_price: float = Field(ge=0)
    discount_amount: float = Field(ge=0)
    revenue: float = Field(ge=0)


class OrderCreatedEvent(BaseModel):
    event_type: str = "order.created"
    event_time: datetime
    order_id: str
    order_date: str
    customer_id: str
    assigned_warehouse_id: str
    nearest_warehouse_id: str
    allocation_strategy: str
    order_priority: str
    total_items: int = Field(ge=1)
    total_amount: float = Field(ge=0)
    total_fulfillment_cost: float = Field(ge=0)
    order_status: str
    return_flag: bool = False
    experiment_id: Optional[str] = None
    experiment_group: Optional[str] = None


class DeliveryUpdatedEvent(BaseModel):
    event_type: str = "delivery.status_changed"
    event_time: datetime
    delivery_id: str
    order_id: str
    driver_id: str
    warehouse_id: str
    delivery_status: str
    distance_km: float = Field(ge=0)
    estimated_eta_minutes: float = Field(ge=0)
    actual_delivery_minutes: Optional[float] = None
    delivery_cost: float = Field(ge=0)
    on_time_flag: Optional[bool] = None
    sla_minutes: Optional[int] = None


class InventorySnapshotEvent(BaseModel):
    event_type: str = "inventory.snapshot"
    event_time: datetime
    snapshot_date: str
    warehouse_id: str
    product_id: str
    opening_stock: int = Field(ge=0)
    units_sold: int = Field(ge=0)
    units_received: int = Field(ge=0)
    units_returned: int = Field(ge=0)
    closing_stock: int = Field(ge=0)
    stockout_flag: bool = False
    reorder_triggered_flag: bool = False
    days_of_supply: Optional[float] = None
    holding_cost: Optional[float] = None
