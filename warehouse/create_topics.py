from __future__ import annotations

import argparse

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from warehouse.models import TopicNames


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tworzy topiki Kafka dla modułu magazynu.")
    parser.add_argument("--bootstrap-servers", default="broker:9092")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    topics = TopicNames()

    all_topics = [
        topics.products,
        topics.suppliers,
        topics.sales,
        topics.warehouse_states,
        topics.warehouse_alerts,
        topics.warehouse_metrics,
    ]

    admin = KafkaAdminClient(
        bootstrap_servers=args.bootstrap_servers,
        client_id="warehouse-topic-setup",
    )

    try:
        existing = set(admin.list_topics())
        new_topics = [
            NewTopic(
                name=name,
                num_partitions=args.partitions,
                replication_factor=args.replication_factor,
            )
            for name in all_topics
            if name not in existing
        ]

        if new_topics:
            admin.create_topics(new_topics=new_topics, validate_only=False)
            print("Utworzono topiki:", ", ".join(t.name for t in new_topics))
        else:
            print("Wszystkie topiki już istnieją.")

        print("Topiki projektu:", ", ".join(all_topics))
    except TopicAlreadyExistsError:
        print("Część topików już istniała.")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
