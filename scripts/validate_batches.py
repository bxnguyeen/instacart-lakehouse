import csv

from prepare_batches import SOURCE, OUTPUT, get_batch_id

BATCH_IDS = ["001", "002", "003"]
ITEM_FILES = [
    "order_products__prior.csv",
    "order_products__train.csv",
]


def read_rows(path):
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        yield from csv.DictReader(file)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    totals = {
        "orders.csv": 0,
        **{filename: 0 for filename in ITEM_FILES},
    }

    for batch_id in BATCH_IDS:
        batch_dir = OUTPUT / batch_id
        order_ids = set()

        for row in read_rows(batch_dir / "orders.csv"):
            order_id = int(row["order_id"])

            require(
                get_batch_id(order_id) == batch_id,
                f"Order {order_id} is assigned to the wrong batch: {batch_id}",
            )
            require(
                order_id not in order_ids,
                f"Duplicate order {order_id} in batch {batch_id}",
            )

            order_ids.add(order_id)
            totals["orders.csv"] += 1

        print(
            f"PASS batch {batch_id}: "
            "orders are correctly assigned and unique"
        )

        for filename in ITEM_FILES:
            count = 0

            for row in read_rows(batch_dir / filename):
                order_id = int(row["order_id"])

                require(
                    order_id in order_ids,
                    f"{filename}: order {order_id} "
                    f"is missing from orders in batch {batch_id}",
                )
                count += 1

            totals[filename] += count
            print(
                f"PASS batch {batch_id}: {filename}, "
                f"{count:,} rows, all orders exist in this batch"
            )

    for filename, batch_count in totals.items():
        source_count = sum(1 for _ in read_rows(SOURCE / filename))

        require(
            batch_count == source_count,
            f"{filename}: row count mismatch — "
            f"source={source_count:,}, batches={batch_count:,}",
        )
        print(f"PASS total row count for {filename}: {batch_count:,}")

    print("\nPASS: all batch validation checks completed")


if __name__ == "__main__":
    main()