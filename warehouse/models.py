from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ProductState:
    product_id: str
    name: str
    category: str
    supplier_id: str
    price: float
    reorder_level: int

    current_stock: int = 0
    total_sold: int = 0
    total_revenue: float = 0.0
    last_sale_time: Optional[str] = None
    alert_sent: bool = False 

    @property
    def is_low_stock(self) -> bool:
        return self.current_stock <= self.reorder_level

    @property
    def is_out_of_stock(self) -> bool:
        return self.current_stock <= 0

    def apply_sale(self, quantity: int, total_amount: float, event_time: str) -> None:
        self.current_stock = max(0, self.current_stock - quantity)
        self.total_sold += quantity
        self.total_revenue += total_amount
        self.last_sale_time = event_time


@dataclass
class StockAlert:
    alert_id: str
    alert_type: str       
    alert_time: str
    product_id: str
    product_name: str
    category: str
    supplier_id: str
    current_stock: int
    reorder_level: int
    total_sold: int
    last_sale_time: Optional[str]

    def to_dict(self) -> dict:
        return {
            "alert_id": self.alert_id,
            "alert_type": self.alert_type,
            "alert_time": self.alert_time,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "category": self.category,
            "supplier_id": self.supplier_id,
            "current_stock": self.current_stock,
            "reorder_level": self.reorder_level,
            "total_sold": self.total_sold,
            "last_sale_time": self.last_sale_time,
        }


@dataclass
class WarehouseMetrics:
    snapshot_time: str
    total_products: int
    products_in_stock: int
    products_low_stock: int
    products_out_of_stock: int
    total_stock_value: float        
    total_revenue: float
    total_units_sold: int
    top_selling_product_id: Optional[str]
    top_selling_product_name: Optional[str]

    def to_dict(self) -> dict:
        return {
            "snapshot_time": self.snapshot_time,
            "total_products": self.total_products,
            "products_in_stock": self.products_in_stock,
            "products_low_stock": self.products_low_stock,
            "products_out_of_stock": self.products_out_of_stock,
            "total_stock_value": round(self.total_stock_value, 2),
            "total_revenue": round(self.total_revenue, 2),
            "total_units_sold": self.total_units_sold,
            "top_selling_product_id": self.top_selling_product_id,
            "top_selling_product_name": self.top_selling_product_name,
        }


@dataclass
class TopicNames:
    products: str = "products"
    suppliers: str = "suppliers"
    sales: str = "sales"
    warehouse_states: str = "warehouse_states"
    warehouse_alerts: str = "warehouse_alerts"
    warehouse_metrics: str = "warehouse_metrics"
