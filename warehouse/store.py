from __future__ import annotations

from uuid import uuid4

from warehouse.models import ProductState, StockAlert, WarehouseMetrics, utc_now_iso


class InventoryStore:

    def __init__(self) -> None:
        self._products: dict[str, ProductState] = {}


    def apply_product_snapshot(self, event: dict) -> None:
        pid = event["product_id"]
        if pid not in self._products:
            self._products[pid] = ProductState(
                product_id=pid,
                name=event["name"],
                category=event["category"],
                supplier_id=event["supplier_id"],
                price=event["price"],
                reorder_level=event["reorder_level"],
                current_stock=event["initial_stock"],
            )
        else:
            state = self._products[pid]
            state.name = event["name"]
            state.category = event["category"]
            state.supplier_id = event["supplier_id"]
            state.price = event["price"]
            state.reorder_level = event["reorder_level"]

    def apply_sale(self, event: dict) -> tuple[ProductState, list[StockAlert]]:
        pid = event["product_id"]
        if pid not in self._products:
            return None, []

        state = self._products[pid]
        was_ok_before = not state.is_low_stock

        state.apply_sale(
            quantity=event["quantity"],
            total_amount=event["total_amount"],
            event_time=event["event_time"],
        )

        alerts = self._generate_alerts(state, was_ok_before)
        return state, alerts


    def _generate_alerts(
        self, state: ProductState, was_ok_before: bool
    ) -> list[StockAlert]:
        alerts: list[StockAlert] = []

        if state.is_out_of_stock:
            if not state.alert_sent or state.current_stock == 0:
                alerts.append(self._make_alert("out_of_stock", state))
                state.alert_sent = True

        elif state.is_low_stock and was_ok_before:
            alerts.append(self._make_alert("low_stock", state))
            state.alert_sent = True

        return alerts

    def _make_alert(self, alert_type: str, state: ProductState) -> StockAlert:
        return StockAlert(
            alert_id=str(uuid4()),
            alert_type=alert_type,
            alert_time=utc_now_iso(),
            product_id=state.product_id,
            product_name=state.name,
            category=state.category,
            supplier_id=state.supplier_id,
            current_stock=state.current_stock,
            reorder_level=state.reorder_level,
            total_sold=state.total_sold,
            last_sale_time=state.last_sale_time,
        )


    def get_state(self, product_id: str) -> ProductState | None:
        return self._products.get(product_id)

    def all_states(self) -> list[ProductState]:
        return list(self._products.values())

    def compute_metrics(self) -> WarehouseMetrics:
        states = self.all_states()

        if not states:
            return WarehouseMetrics(
                snapshot_time=utc_now_iso(),
                total_products=0,
                products_in_stock=0,
                products_low_stock=0,
                products_out_of_stock=0,
                total_stock_value=0.0,
                total_revenue=0.0,
                total_units_sold=0,
                top_selling_product_id=None,
                top_selling_product_name=None,
            )

        products_low = [s for s in states if s.is_low_stock and not s.is_out_of_stock]
        products_out = [s for s in states if s.is_out_of_stock]
        products_ok = [s for s in states if not s.is_low_stock]

        stock_value = sum(s.current_stock * s.price for s in states)
        total_revenue = sum(s.total_revenue for s in states)
        total_sold = sum(s.total_sold for s in states)

        top = max(states, key=lambda s: s.total_sold) if states else None

        return WarehouseMetrics(
            snapshot_time=utc_now_iso(),
            total_products=len(states),
            products_in_stock=len(products_ok),
            products_low_stock=len(products_low),
            products_out_of_stock=len(products_out),
            total_stock_value=stock_value,
            total_revenue=total_revenue,
            total_units_sold=total_sold,
            top_selling_product_id=top.product_id if top else None,
            top_selling_product_name=top.name if top else None,
        )

    def state_snapshot(self, product_id: str) -> dict | None:
        state = self._products.get(product_id)
        if state is None:
            return None
        return {
            "event_type": "warehouse_state",
            "event_time": utc_now_iso(),
            "product_id": state.product_id,
            "product_name": state.name,
            "category": state.category,
            "supplier_id": state.supplier_id,
            "current_stock": state.current_stock,
            "reorder_level": state.reorder_level,
            "is_low_stock": state.is_low_stock,
            "is_out_of_stock": state.is_out_of_stock,
            "total_sold": state.total_sold,
            "total_revenue": round(state.total_revenue, 2),
            "last_sale_time": state.last_sale_time,
        }
