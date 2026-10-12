"""Entity definitions and paths shared by every notebook.

Bronze keeps every source column as STRING. Silver uses the type lists below
to parse values, so a bad value becomes a quarantine row instead of a silent NULL.
"""
from dataclasses import dataclass

ALLOWED_ORDER_STATUSES = (
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "canceled", "unavailable",
)

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# Frankfurter (ECB reference rates). One date-range request, not one per day.
# The range starts a week before the first order so forward-fill has a seed rate.
FX_BASE, FX_TARGET = "BRL", "USD"
FX_START, FX_END = "2016-08-25", "2018-10-31"
FX_FALLBACK_FILE = "data/fx/brl_usd_2016-08-25_2018-10-31.json"


@dataclass(frozen=True)
class EntitySpec:
    name: str
    source_file: str
    columns: tuple                 # raw CSV header, in order
    keys: tuple                    # business key (after rename)
    required: tuple = ()           # non-key columns that must be present
    timestamp_cols: tuple = ()
    int_cols: tuple = ()
    decimal_cols: tuple = ()
    non_negative: tuple = ()
    lower_cols: tuple = ()
    upper_cols: tuple = ()
    rename: tuple = ()             # ((raw_name, clean_name), ...)
    split_by_order_month: bool = False
    needs_parent_order: bool = False

    def clean_name(self, raw: str) -> str:
        return dict(self.rename).get(raw, raw)


ENTITIES = {
    "category_translation": EntitySpec(
        name="category_translation",
        source_file="product_category_name_translation.csv",
        columns=("product_category_name", "product_category_name_english"),
        keys=("product_category_name",),
        lower_cols=("product_category_name", "product_category_name_english"),
    ),
    "sellers": EntitySpec(
        name="sellers",
        source_file="olist_sellers_dataset.csv",
        columns=("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
        keys=("seller_id",),
        lower_cols=("seller_city",),
        upper_cols=("seller_state",),
    ),
    "products": EntitySpec(
        name="products",
        source_file="olist_products_dataset.csv",
        columns=(
            "product_id", "product_category_name", "product_name_lenght",
            "product_description_lenght", "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ),
        keys=("product_id",),
        int_cols=(
            "product_name_lenght", "product_description_lenght", "product_photos_qty",
            "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
        ),
        non_negative=(
            "product_photos_qty", "product_weight_g", "product_length_cm",
            "product_height_cm", "product_width_cm",
        ),
        lower_cols=("product_category_name",),
        # Olist misspells "length" in the source header.
        rename=(
            ("product_name_lenght", "product_name_length"),
            ("product_description_lenght", "product_description_length"),
        ),
    ),
    "customers": EntitySpec(
        name="customers",
        source_file="olist_customers_dataset.csv",
        columns=(
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ),
        keys=("customer_id",),
        required=("customer_unique_id",),
        lower_cols=("customer_city",),
        upper_cols=("customer_state",),
        split_by_order_month=True,
    ),
    "orders": EntitySpec(
        name="orders",
        source_file="olist_orders_dataset.csv",
        columns=(
            "order_id", "customer_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ),
        keys=("order_id",),
        required=("customer_id", "order_purchase_timestamp"),
        timestamp_cols=(
            "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ),
        lower_cols=("order_status",),
        split_by_order_month=True,
    ),
    "order_items": EntitySpec(
        name="order_items",
        source_file="olist_order_items_dataset.csv",
        columns=(
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        ),
        keys=("order_id", "order_item_id"),
        required=("product_id", "seller_id"),
        timestamp_cols=("shipping_limit_date",),
        int_cols=("order_item_id",),
        decimal_cols=("price", "freight_value"),
        non_negative=("price", "freight_value"),
        split_by_order_month=True,
        needs_parent_order=True,
    ),
    "order_payments": EntitySpec(
        name="order_payments",
        source_file="olist_order_payments_dataset.csv",
        columns=(
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
        ),
        keys=("order_id", "payment_sequential"),
        int_cols=("payment_sequential", "payment_installments"),
        decimal_cols=("payment_value",),
        non_negative=("payment_value", "payment_installments"),
        lower_cols=("payment_type",),
        split_by_order_month=True,
        needs_parent_order=True,
    ),
}

# Parents before children: orphan checks need silver.orders to be current.
LOAD_ORDER = (
    "category_translation", "sellers", "products",
    "customers", "orders", "order_items", "order_payments",
)


LANDING_VOLUME = "landing"   # bronze.landing
EXPORTS_VOLUME = "exports"   # gold.exports (CSV fallback for Power BI)


def volume_paths(catalog: str) -> dict:
    """raw: Olist CSVs you upload once. incoming: files the pipeline ingests
    (one folder per entity). checkpoints: Auto Loader state."""
    base = f"/Volumes/{catalog}/bronze/{LANDING_VOLUME}"
    return {
        "raw": f"{base}/raw/olist",
        "incoming": f"{base}/incoming",
        "checkpoints": f"{base}/checkpoints",
        "exports": f"/Volumes/{catalog}/gold/{EXPORTS_VOLUME}",
    }
