# Moduł: Przetwarzanie Magazynu

Moduł odpowiedzialny za odbieranie zdarzeń z Kafki, utrzymywanie aktualnych stanów magazynowych, liczenie metryk i generowanie alertów o niskich stanach.

## Zakres

- odbieranie zdarzeń `product_snapshot` z topiku `products`,
- odbieranie zdarzeń `sale` z topiku `sales`,
- aktualizacja stanów magazynowych in-memory,
- generowanie alertów `low_stock` i `out_of_stock`,
- obliczanie metryk całego magazynu,
- publikowanie wyników do topików wyjściowych.

## Topiki Kafka

| Topik (wejście) | Zawartość |
|---|---|
| `products` | migawki produktów (`product_snapshot`) |
| `sales` | zdarzenia sprzedaży (`sale`) |

| Topik (wyjście) | Zawartość |
|---|---|
| `warehouse_states` | aktualny stan każdego produktu po sprzedaży |
| `warehouse_alerts` | alerty `low_stock` i `out_of_stock` |
| `warehouse_metrics` | metryki całego magazynu co N zdarzeń |

## Struktura projektu

```
warehouse/
├── warehouse/
│   ├── __init__.py
│   ├── models.py          # dataclassy: ProductState, StockAlert, WarehouseMetrics
│   ├── store.py           # InventoryStore – logika stanu in-memory
│   ├── kafka_io.py        # WarehouseConsumer, WarehouseProducer
│   ├── main.py            # główna pętla przetwarzania
│   └── create_topics.py   # tworzenie topików
├── tests/
│   └── test_warehouse.py
└── requirements.txt
```

## Uruchomienie

Instalacja zależności:
```bash
pip install -r requirements.txt
```

Tworzenie topików:
```bash
python -m warehouse.create_topics --bootstrap-servers broker:9092
```

Uruchomienie procesora:
```bash
# Standardowe uruchomienie
python -m warehouse.main --bootstrap-servers broker:9092

# Z podglądem każdej sprzedaży
python -m warehouse.main --bootstrap-servers broker:9092 --verbose

# Metryki co 5 zdarzeń (domyślnie: 10)
python -m warehouse.main --bootstrap-servers broker:9092 --metrics-every 5

# Tryb dry-run (bez Kafki – tylko logi, przydatne do testów)
python -m warehouse.main --dry-run
```

## Testy

```bash
pytest tests/ -v
```

## Logika alertów

| Sytuacja | Typ alertu | Kiedy wysyłany |
|---|---|---|
| `current_stock <= reorder_level` | `low_stock` | raz, przy pierwszym przekroczeniu progu |
| `current_stock == 0` | `out_of_stock` | przy każdym zejściu do zera |

## Format zdarzeń wyjściowych

### `warehouse_states`
```json
{
  "event_type": "warehouse_state",
  "event_time": "2024-01-01T10:05:00+00:00",
  "product_id": "P001",
  "product_name": "Laptop Lenovo ThinkPad E14",
  "category": "electronics",
  "supplier_id": "S001",
  "current_stock": 21,
  "reorder_level": 6,
  "is_low_stock": false,
  "is_out_of_stock": false,
  "total_sold": 3,
  "total_revenue": 11699.97,
  "last_sale_time": "2024-01-01T10:05:00+00:00"
}
```

### `warehouse_alerts`
```json
{
  "alert_id": "uuid",
  "alert_type": "low_stock",
  "alert_time": "2024-01-01T10:10:00+00:00",
  "product_id": "P001",
  "product_name": "Laptop Lenovo ThinkPad E14",
  "category": "electronics",
  "supplier_id": "S001",
  "current_stock": 6,
  "reorder_level": 6,
  "total_sold": 18,
  "last_sale_time": "2024-01-01T10:10:00+00:00"
}
```

### `warehouse_metrics`
```json
{
  "snapshot_time": "2024-01-01T10:10:00+00:00",
  "total_products": 30,
  "products_in_stock": 26,
  "products_low_stock": 3,
  "products_out_of_stock": 1,
  "total_stock_value": 142350.00,
  "total_revenue": 58200.00,
  "total_units_sold": 87,
  "top_selling_product_id": "P022",
  "top_selling_product_name": "Pendrive SanDisk Ultra 128GB"
}
```
