"""Generate the SIMULATED correction and fault batches in data/simulated/.

Everything written here is simulated, not real Olist data. Simulated IDs start
with "sim". Output is deterministic, so re-running produces identical files.

    python tools/generate_simulated_batches.py --olist-dir ../archive
"""
import argparse
import csv
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "simulated"
TS = "%Y-%m-%d %H:%M:%S"

ORDER_COLS = ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
              "order_approved_at", "order_delivered_carrier_date",
              "order_delivered_customer_date", "order_estimated_delivery_date"]
ITEM_COLS = ["order_id", "order_item_id", "product_id", "seller_id",
             "shipping_limit_date", "price", "freight_value"]
PAYMENT_COLS = ["order_id", "payment_sequential", "payment_type",
                "payment_installments", "payment_value"]
CUSTOMER_COLS = ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                 "customer_city", "customer_state"]


def read(path: Path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write(entity: str, name: str, columns, rows):
    folder = OUT / entity
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {path.relative_to(ROOT)} ({len(rows)} rows)")


def sim_id(kind: str, n: int) -> str:
    return f"sim{kind}{n:029d}"  # 32 chars, like Olist hex IDs, but obviously simulated


def correction_batch(orders, customers):
    """(a) 20 shipped orders become delivered -> MERGE must UPDATE, not insert.
       (b) 5 customers move city -> dim_customer must open a new SCD2 version."""
    shipped = sorted((o for o in orders if o["order_status"] == "shipped"
                      and not o["order_delivered_customer_date"]), key=lambda o: o["order_id"])[:20]
    corrected = []
    for o in shipped:
        purchased = datetime.strptime(o["order_purchase_timestamp"], TS)
        estimated = datetime.strptime(o["order_estimated_delivery_date"], TS)
        delivered = max(purchased + timedelta(days=1), estimated - timedelta(days=1))
        corrected.append({**o, "order_status": "delivered",
                          "order_delivered_customer_date": delivered.strftime(TS)})
    write("orders", "simulated_correction_orders.csv", ORDER_COLS, corrected)

    per_person = Counter(c["customer_unique_id"] for c in customers)
    single = sorted((c for c in customers if per_person[c["customer_unique_id"]] == 1
                     and c["customer_state"] != "RJ"), key=lambda c: c["customer_id"])[:5]
    moved = [{**c, "customer_zip_code_prefix": "20040", "customer_city": "rio de janeiro",
              "customer_state": "RJ"} for c in single]
    write("customers", "simulated_correction_customers.csv", CUSTOMER_COLS, moved)


def fault_batch(orders, items):
    """One file per entity with deliberate problems. Expected outcomes are in docs/runbook.md."""
    template_item = sorted(items, key=lambda i: (i["order_id"], i["order_item_id"]))[0]
    late, dup, neg = sim_id("o", 1), sim_id("o", 2), sim_id("o", 3)
    cust = {n: sim_id("c", n) for n in (1, 2, 3, 4, 5, 6)}

    def order(order_id, customer_id, purchased="2018-08-20 10:00:00", status="delivered", **kw):
        try:
            p = datetime.strptime(purchased, TS)
        except ValueError:
            p = None  # deliberately malformed: leave the other dates empty
        row = {
            "order_id": order_id, "customer_id": customer_id, "order_status": status,
            "order_purchase_timestamp": purchased,
            "order_approved_at": (p + timedelta(hours=1)).strftime(TS) if p else "",
            "order_delivered_carrier_date": (p + timedelta(days=2)).strftime(TS) if p else "",
            "order_delivered_customer_date": (p + timedelta(days=6)).strftime(TS) if p else "",
            "order_estimated_delivery_date": (p + timedelta(days=15)).strftime("%Y-%m-%d 00:00:00") if p else "",
            "promo_code": "SIMFAULT",  # extra column not in the contract -> _rescued_data
        }
        row.update(kw)
        return row

    fault_orders = [
        order(late, cust[1], purchased="2016-10-15 09:30:00"),  # late-arriving: 2016 order in a new file
        order(dup, cust[2], status="shipped", order_delivered_customer_date=""),
        order(dup, cust[2]),                                    # duplicate key in the same batch
        order("", cust[3]),                                     # missing order_id
        order(sim_id("o", 4), cust[4], purchased="2018-13-45 99:00:00"),  # malformed timestamp
        order(sim_id("o", 5), cust[5], status="lost_in_space"),  # status not in the allowed list
        order(neg, cust[6]),
    ]
    write("orders", "simulated_fault_orders.csv", ORDER_COLS + ["promo_code"], fault_orders)

    # Same contract, but one column renamed upstream: order_status -> status.
    renamed = [{**order(sim_id("o", 7), cust[1]), "status": "delivered"}]
    renamed_cols = [c if c != "order_status" else "status" for c in ORDER_COLS]
    write("orders", "simulated_fault_orders_renamed_column.csv", renamed_cols, renamed)

    def item(order_id, n, price="49.90", freight="12.50"):
        return {**template_item, "order_id": order_id, "order_item_id": str(n),
                "shipping_limit_date": "2018-08-22 10:00:00", "price": price, "freight_value": freight}

    fault_items = [
        item(late, 1, price="120.00", freight="15.00"),
        item(dup, 1),
        item(neg, 1, price="-49.90"),                  # negative price
        item(sim_id("o", 99), 1),                      # orphan: order never arrives
    ]
    write("order_items", "simulated_fault_order_items.csv", ITEM_COLS, fault_items)

    fault_payments = [
        {"order_id": late, "payment_sequential": "1", "payment_type": "credit_card",
         "payment_installments": "3", "payment_value": "135.00"},
        {"order_id": dup, "payment_sequential": "1", "payment_type": "boleto",
         "payment_installments": "1", "payment_value": "62.40"},
    ]
    write("order_payments", "simulated_fault_order_payments.csv", PAYMENT_COLS, fault_payments)

    fault_customers = [
        {"customer_id": cid, "customer_unique_id": sim_id("u", n), "customer_zip_code_prefix": "01310",
         "customer_city": "sao paulo", "customer_state": "SP"}
        for n, cid in cust.items()
    ]
    write("customers", "simulated_fault_customers.csv", CUSTOMER_COLS, fault_customers)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--olist-dir", default=str(ROOT.parent / "archive"),
                        help="folder with the original Olist CSVs")
    args = parser.parse_args()
    src = Path(args.olist_dir)
    orders = read(src / "olist_orders_dataset.csv")
    customers = read(src / "olist_customers_dataset.csv")
    items = read(src / "olist_order_items_dataset.csv")
    correction_batch(orders, customers)
    fault_batch(orders, items)


if __name__ == "__main__":
    main()
