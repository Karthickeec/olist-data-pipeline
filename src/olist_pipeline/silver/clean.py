"""Pure DataFrame transforms for Silver: typing, cleaning rules and quarantine reasons.

No I/O here; the job wires these to Bronze reads and Silver writes, and the unit
tests run them on small hand-made DataFrames.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from olist_pipeline.silver.common import BRONZE_INGEST_DATE, latest_per_key, split_quarantine

CORRUPT = "_corrupt_record"

BRAZIL_STATES = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins",
}
_ACCENTED = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
_PLAIN = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"

# Categories missing from the official translation file (plus the blank category).
EXTRA_CATEGORY_TRANSLATIONS = {
    "pc_gamer": "pc_gamer",
    "portateis_cozinha_e_preparadores_de_alimentos": "kitchen_portables_and_food_preparers",
}
UNKNOWN_CATEGORY = "unknown"

ISO_UTC = "yyyy-MM-dd'T'HH:mm:ss'Z'"
BRAZILIAN = "dd/MM/yyyy HH:mm:ss"
ACTIVITY_COUNTS = ("sessions", "page_views", "cart_adds", "support_tickets")


def _strip_accents_upper(col: Column) -> Column:
    return F.upper(F.translate(F.trim(col), _ACCENTED, _PLAIN))


def normalize_state(col: Column) -> Column:
    """' sp ', 'Sp', 'São Paulo' -> 'SP'. Anything that is not a Brazilian state -> null."""
    cleaned = _strip_accents_upper(col)
    by_name = F.create_map(
        *[
            x
            for code, name in BRAZIL_STATES.items()
            for x in (F.lit(name.upper().translate(str.maketrans(_ACCENTED, _PLAIN))), F.lit(code))
        ]
    )
    return F.when(cleaned.isin(*BRAZIL_STATES), cleaned).otherwise(F.element_at(by_name, cleaned))


def blank_to_null(col: Column) -> Column:
    trimmed = F.trim(col)
    return F.when(trimmed == "", None).otherwise(trimmed)


def parse_activity_timestamp(col: Column) -> Column:
    """Both formats the API sends: ISO 8601 UTC ('...Z') and Brazilian 'dd/MM/yyyy HH:mm:ss'."""
    return F.coalesce(F.to_timestamp(col, ISO_UTC), F.to_timestamp(col, BRAZILIAN))


def split_corrupt(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Rows Spark could not parse at all are quarantined as they are."""
    corrupt = bronze.filter(F.col(CORRUPT).isNotNull()).withColumn("_reason", F.lit("corrupt_record"))
    return bronze.filter(F.col(CORRUPT).isNull()), corrupt


# --- CRM customer_changes ---------------------------------------------------------------


def clean_customer_changes(bronze: DataFrame, known_customers: DataFrame, zip_city: DataFrame):
    """Returns (valid, quarantined, duplicates_removed).

    known_customers: (customer_unique_id); zip_city: (zip_code_prefix, city) from Silver geolocation.
    """
    good, corrupt = split_corrupt(bronze)
    # Exact duplicates share a change_id; keep the copy from the newest Bronze partition.
    deduped = latest_per_key(good, ["change_id"], [F.col(BRONZE_INGEST_DATE).desc(), F.col("_source_file").desc()])
    duplicates = good.count() - deduped.count()

    known = known_customers.select("customer_unique_id").distinct().withColumn("_known", F.lit(True))
    # Both lookups are small (≈96k ids, 19k zips): broadcast them instead of shuffling the batch.
    x = (
        deduped.withColumn("_state", normalize_state(F.col("new_state")))
        .withColumn("_city", F.lower(blank_to_null(F.col("new_city"))))
        .withColumn("_requested_at", F.to_timestamp("requested_at", "yyyy-MM-dd HH:mm:ss"))
        .join(
            F.broadcast(
                zip_city.withColumnRenamed("zip_code_prefix", "new_zip_code_prefix").withColumnRenamed(
                    "city", "_geo_city"
                )
            ),
            "new_zip_code_prefix",
            "left",
        )
        .join(F.broadcast(known), "customer_unique_id", "left")
        .withColumn("_city_final", F.coalesce("_city", "_geo_city"))
    )
    valid, quarantined = split_quarantine(
        x,
        [
            (
                "missing_required_field",
                F.col("change_id").isNull()
                | F.col("customer_unique_id").isNull()
                | F.col("new_zip_code_prefix").isNull(),
            ),
            ("unparseable_timestamp", F.col("_requested_at").isNull()),
            ("invalid_state", F.col("_state").isNull()),
            ("unfixable_city", F.col("_city_final").isNull()),
            ("unknown_customer", F.col("_known").isNull()),
        ],
    )
    valid = valid.select(
        "change_id",
        "customer_unique_id",
        "new_zip_code_prefix",
        F.col("_city_final").alias("new_city"),
        F.col("_state").alias("new_state"),
        F.col("_requested_at").alias("requested_at"),
        F.to_date("_requested_at").alias("requested_date"),
        "source",
        (F.col("_state") != F.col("new_state")).alias("_state_fixed"),
        F.col("_city").isNull().alias("_city_filled"),
        "_source_file",
        BRONZE_INGEST_DATE,
    )
    quarantined = quarantined.select(*bronze.columns, "_reason").unionByName(corrupt)
    return valid, quarantined, duplicates


# --- API customer_activity --------------------------------------------------------------


def clean_customer_activity(bronze: DataFrame, known_customers: DataFrame):
    """Returns (valid, quarantined, duplicates_removed)."""
    good, corrupt = split_corrupt(bronze)
    deduped = latest_per_key(
        good,
        ["customer_unique_id", "activity_date"],
        [F.col(BRONZE_INGEST_DATE).desc(), F.col("_source_file").desc(), F.col("_page").desc()],
    )
    duplicates = good.count() - deduped.count()

    known = known_customers.select("customer_unique_id").distinct().withColumn("_known", F.lit(True))
    x = deduped.join(F.broadcast(known), "customer_unique_id", "left")
    for c in ACTIVITY_COUNTS:
        x = x.withColumn(f"_{c}", F.col(c).cast("int"))
    x = x.withColumn("_last_seen_at", parse_activity_timestamp(F.col("last_seen_at"))).withColumn(
        "_activity_date", F.to_date("activity_date")
    )

    not_a_number = F.lit(False)
    negative = F.lit(False)
    for c in ACTIVITY_COUNTS:
        not_a_number = not_a_number | (F.col(c).isNotNull() & F.col(f"_{c}").isNull())
        negative = negative | (F.col(f"_{c}") < 0)
    valid, quarantined = split_quarantine(
        x,
        [
            (
                "missing_required_field",
                F.col("customer_unique_id").isNull() | F.col("activity_date").isNull() | F.col("sessions").isNull(),
            ),
            ("invalid_date", F.col("_activity_date").isNull()),
            ("invalid_number", not_a_number),
            ("negative_sessions", F.col("_sessions") < 0),
            ("negative_count", negative),
            ("unparseable_timestamp", F.col("last_seen_at").isNotNull() & F.col("_last_seen_at").isNull()),
            ("unknown_customer", F.col("_known").isNull()),
        ],
    )
    valid = valid.select(
        "customer_unique_id",
        F.col("_activity_date").alias("activity_date"),
        *[F.col(f"_{c}").alias(c) for c in ACTIVITY_COUNTS],
        F.col("_last_seen_at").alias("last_seen_at"),
        F.lower(blank_to_null(F.col("device"))).alias("device"),
        F.col("last_seen_at").rlike(r"^\d{2}/").alias("_timestamp_reformatted"),
        "_page",
        "_source_file",
        BRONZE_INGEST_DATE,
    )
    quarantined = quarantined.select(*bronze.columns, "_reason").unionByName(corrupt)
    return valid, quarantined, duplicates


# --- reference data -----------------------------------------------------------------------


def build_geolocation(snapshot: DataFrame) -> DataFrame:
    """One row per zip prefix: exact median lat/lng, most frequent city/state (ties: alphabetical)."""

    def most_frequent(col: str) -> DataFrame:
        counts = snapshot.groupBy("geolocation_zip_code_prefix", col).count()
        w = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.col("count").desc(), F.col(col))
        return counts.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "count")

    coords = snapshot.groupBy("geolocation_zip_code_prefix").agg(
        F.median("geolocation_lat").alias("lat"),
        F.median("geolocation_lng").alias("lng"),
        F.count(F.lit(1)).alias("source_rows"),
    )
    return (
        coords.join(most_frequent("geolocation_city"), "geolocation_zip_code_prefix")
        .join(most_frequent("geolocation_state"), "geolocation_zip_code_prefix")
        .select(
            F.col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
            "lat",
            "lng",
            F.col("geolocation_city").alias("city"),
            F.col("geolocation_state").alias("state"),
            "source_rows",
        )
    )


def build_products(products: DataFrame, translation: DataFrame) -> DataFrame:
    """Products with an English category for every product; fixes the source column-name typos."""
    extra = F.create_map(*[F.lit(x) for kv in EXTRA_CATEGORY_TRANSLATIONS.items() for x in kv])
    # The translation table has 71 rows: broadcast it rather than shuffle 33k products.
    joined = products.join(
        F.broadcast(translation.select("product_category_name", "product_category_name_english")),
        "product_category_name",
        "left",
    )
    english = F.coalesce(
        F.col("product_category_name_english"),
        F.element_at(extra, F.col("product_category_name")),
        F.lit(UNKNOWN_CATEGORY),
    )
    return joined.select(
        "product_id",
        F.coalesce("product_category_name", F.lit(UNKNOWN_CATEGORY)).alias("product_category_name"),
        english.alias("product_category_name_english"),
        F.col("product_name_lenght").alias("product_name_length"),
        F.col("product_description_lenght").alias("product_description_length"),
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    )
