"""
State manager for simulation.
Tracks inventory levels, pending shipments, and counters between days.
For backfill: state lives in memory.
For Lambda: state is read/written to S3 as JSON.
"""
# data_simulation/state/state_manager.py
import json
from typing import Dict, Tuple, List


class SimulationState:
    """Holds all state that carries over between simulation days."""
    
    def __init__(self):
        # Inventory state: {(warehouse_id, product_id): {closing_stock, units_on_order, avg_daily_demand}}
        self.inventory_state: Dict[Tuple[str, str], dict] = {}
        
        # Pending shipments waiting to arrive
        self.pending_shipments: List[dict] = []
        
        # Counters for unique ID generation
        self.shipment_counter: int = 1
        self.delivery_counter: int = 1
        self.assignment_counter: int = 1
        
        # Day counter
        self.day_counter: int = 0
    
    def to_dict(self) -> dict:
        """Serialize state to a dictionary (for JSON storage)."""
        # Convert tuple keys to strings for JSON
        inv_state = {}
        for (wh_id, pid), val in self.inventory_state.items():
            key = f"{wh_id}|{pid}"
            inv_state[key] = val
        
        # Convert dates in pending shipments to strings
        pending = []
        for s in self.pending_shipments:
            s_copy = {k: str(v) if hasattr(v, 'isoformat') else v for k, v in s.items()}
            pending.append(s_copy)
        
        return {
            "inventory_state": inv_state,
            "pending_shipments": pending,
            "shipment_counter": self.shipment_counter,
            "delivery_counter": self.delivery_counter,
            "assignment_counter": self.assignment_counter,
            "day_counter": self.day_counter,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "SimulationState":
        """Deserialize state from a dictionary."""
        from datetime import date, datetime
        
        state = cls()
        
        # Restore inventory state with tuple keys
        for key, val in data.get("inventory_state", {}).items():
            wh_id, pid = key.split("|")
            state.inventory_state[(wh_id, pid)] = val
        
        # Restore pending shipments with date objects
        for s in data.get("pending_shipments", []):
            for date_field in ["shipment_date", "expected_arrival_date", "actual_arrival_date"]:
                if date_field in s and s[date_field] and isinstance(s[date_field], str):
                    try:
                        s[date_field] = date.fromisoformat(s[date_field])
                    except (ValueError, TypeError):
                        pass
            for ts_field in ["created_at", "updated_at"]:
                if ts_field in s and s[ts_field] and isinstance(s[ts_field], str):
                    try:
                        s[ts_field] = datetime.fromisoformat(s[ts_field])
                    except (ValueError, TypeError):
                        pass
            state.pending_shipments.append(s)
        
        state.shipment_counter = data.get("shipment_counter", 1)
        state.delivery_counter = data.get("delivery_counter", 1)
        state.assignment_counter = data.get("assignment_counter", 1)
        state.day_counter = data.get("day_counter", 0)
        
        return state
    
    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_json(cls, json_str: str) -> "SimulationState":
        """Deserialize from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)