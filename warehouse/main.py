from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
from datetime import datetime, timezone

from warehouse.kafka_io import WarehouseConsumer, WarehouseProducer
from warehouse.models import TopicNames
from warehouse.store import InventoryStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("warehouse")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Procesor magazynu – konsument Kafka.")
    parser.add_argument("--bootstrap-servers", default="broker:9092")
    parser.add_argument(
        "--metrics-every",
        type=int,
        default=10,
        help="Co ile zdarzeń sprzedaży publikować metryki (domyślnie: 10).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Wypisuj na konsolę zamiast wysyłać do Kafki.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Wypisuj każde przetworzone zdarzenie sprzedaży.",
    )
    return parser.parse_args()



class ConsoleProducer:
    def send(self, topic: str, key: str, value: dict) -> None:
        payload = json.dumps(value, ensure_ascii=False)
        print(f"[dry-run] topic={topic} key={key} | {payload}")

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass



def run(args: argparse.Namespace) -> None:
    topics = TopicNames()
    store = InventoryStore()
    sales_processed = 0

    if args.dry_run:
        publisher = ConsoleProducer()
        log.info("Tryb dry-run – zdarzenia wypisywane na konsolę.")
    else:
        publisher = WarehouseProducer(args.bootstrap_servers)

    def _shutdown(signum, frame):
        log.info("Otrzymano sygnał zakończenia, zamykam...")
        publisher.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)

    log.info("Uruchamiam konsumenta Kafka: %s", args.bootstrap_servers)

    if args.dry_run:
        log.info("Tryb dry-run – konsument Kafka nie jest tworzony.")
        log.info("Użyj opcji bez --dry-run, aby połączyć się z Kafką.")
        return

    consumer = WarehouseConsumer(args.bootstrap_servers)

    try:
        log.info("Oczekuję na zdarzenia z topików: products, sales ...")

        for topic, event in consumer.poll_events():

            if topic == topics.products and event.get("event_type") == "product_snapshot":
                store.apply_product_snapshot(event)
                log.debug("Zarejestrowano produkt: %s", event.get("product_id"))

            elif topic == topics.sales and event.get("event_type") == "sale":
                state, alerts = store.apply_sale(event)

                if state is None:
                    log.warning("Nieznany produkt w zdarzeniu sprzedaży: %s", event.get("product_id"))
                    continue

                snapshot = store.state_snapshot(state.product_id)
                publisher.send(topics.warehouse_states, state.product_id, snapshot)

                sales_processed += 1

                if args.verbose:
                    log.info(
                        "Sprzedaż: %s (%s) | sprzedano: %d | stan: %d (próg: %d)",
                        state.name,
                        state.product_id,
                        event["quantity"],
                        state.current_stock,
                        state.reorder_level,
                    )

                for alert in alerts:
                    publisher.send(topics.warehouse_alerts, alert.product_id, alert.to_dict())
                    log.warning(
                        "ALERT [%s] %s (%s) – stan: %d / próg: %d",
                        alert.alert_type.upper(),
                        alert.product_name,
                        alert.product_id,
                        alert.current_stock,
                        alert.reorder_level,
                    )

                if sales_processed % args.metrics_every == 0:
                    metrics = store.compute_metrics()
                    publisher.send(topics.warehouse_metrics, "global", metrics.to_dict())
                    publisher.flush()
                    log.info(
                        "Metryki [%d sprzedaży] | produkty: %d | niski stan: %d | "
                        "wyczerpane: %d | przychód: %.2f PLN",
                        sales_processed,
                        metrics.total_products,
                        metrics.products_low_stock,
                        metrics.products_out_of_stock,
                        metrics.total_revenue,
                    )

    except KeyboardInterrupt:
        log.info("Przerwano przez użytkownika.")
    finally:
        consumer.close()
        publisher.close()
        log.info("Zamknięto konsumenta i producenta.")

        metrics = store.compute_metrics()
        log.info("=== PODSUMOWANIE ===")
        log.info("Przetworzone zdarzenia sprzedaży: %d", sales_processed)
        log.info("Produkty z niskim stanem: %d", metrics.products_low_stock)
        log.info("Produkty wyczerpane: %d", metrics.products_out_of_stock)
        log.info("Łączny przychód: %.2f PLN", metrics.total_revenue)
        if metrics.top_selling_product_name:
            log.info("Najlepiej sprzedający się: %s", metrics.top_selling_product_name)


def main() -> None:
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
