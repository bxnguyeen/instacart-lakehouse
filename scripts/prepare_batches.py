import csv
from contextlib import ExitStack
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE = PROJECT_ROOT / "data" / "instacart"
OUTPUT = PROJECT_ROOT / "data" / "batches"


def get_batch_id(order_id):
    remainder = int(order_id) % 10

    if remainder < 8:
        return "001"
    elif remainder == 8:
        return "002"
    else:
        return "003"


def split_orders():
    counts = {"001": 0, "002": 0, "003": 0}

    with ExitStack() as stack:
        source_file = stack.enter_context(
            (SOURCE / "orders.csv").open(
                "r", newline="", encoding="utf-8-sig"
            )
        )
        reader = csv.DictReader(source_file)

        if not reader.fieldnames or "order_id" not in reader.fieldnames:
            raise ValueError("orders.csv does not have column order_id")

        writers = {}

        for batch_id in counts:
            batch_dir = OUTPUT / batch_id
            batch_dir.mkdir(parents=True, exist_ok=True)

            output_file = stack.enter_context(
                (batch_dir / "orders.csv").open(
                    "w", newline="", encoding="utf-8"
                )
            )
            writer = csv.DictWriter(
                output_file,
                fieldnames=reader.fieldnames,
            )
            writer.writeheader()
            writers[batch_id] = writer

        for row in reader:
            batch_id = get_batch_id(row["order_id"])
            writers[batch_id].writerow(row)
            counts[batch_id] += 1

    for batch_id, count in counts.items():
        print(f"Batch {batch_id}: {count:,} orders")

    print(f"Total: {sum(counts.values()):,} orders")


def split_order_items(filename):
    counts = {"001": 0, "002": 0, "003": 0}

    with ExitStack() as stack:
        source_file = stack.enter_context(
            (SOURCE / filename).open(
                "r", newline="", encoding="utf-8-sig"
            )
        )
        reader = csv.DictReader(source_file)

        if not reader.fieldnames or "order_id" not in reader.fieldnames:
            raise ValueError(f"{filename}: column order_id cannot be found")

        writers = {}

        for batch_id in counts:
            batch_dir = OUTPUT / batch_id
            batch_dir.mkdir(parents=True, exist_ok=True)

            output_file = stack.enter_context(
                (batch_dir / filename).open(
                    "w", newline="", encoding="utf-8"
                )
            )
            writer = csv.DictWriter(
                output_file,
                fieldnames=reader.fieldnames,
            )
            writer.writeheader()
            writers[batch_id] = writer

        for row in reader:
            batch_id = get_batch_id(row["order_id"])
            writers[batch_id].writerow(row)
            counts[batch_id] += 1

    print(f"\n{filename}")
    for batch_id, count in counts.items():
        print(f"Batch {batch_id}: {count:,} items")

    print(f"Total: {sum(counts.values()):,} items")


if __name__ == "__main__":
    split_orders()
    split_order_items("order_products__prior.csv")
    split_order_items("order_products__train.csv")