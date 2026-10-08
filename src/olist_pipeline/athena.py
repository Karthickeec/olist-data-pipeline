"""Athena/Glue catalog for the lake: external Parquet tables with partition projection.

The DDL is generated from each table's actual Parquet schema (so it can't drift from the files)
and stored in sql/athena/ with a {{LAKE}} placeholder: the bucket name is filled in at run time.
"""
import time
from dataclasses import dataclass
from pathlib import Path

from olist_pipeline.config import PROJECT_ROOT

DDL_DIR = PROJECT_ROOT / "sql" / "athena"
KPI_DIR = DDL_DIR / "kpis"
LAKE_PLACEHOLDER = "{{LAKE}}"
# Projection range for every date partition: the simulated history plus margin. A fixed end keeps an
# unfiltered query to ~850 partitions instead of enumerating every day up to today.
PROJECTION_RANGE = "2016-09-01,2018-12-31"


@dataclass(frozen=True)
class LakeTable:
    name: str            # Glue table name
    path: str            # relative to the lake root
    partition: str | None = None

    @property
    def layer(self) -> str:
        return self.path.split("/")[0]


def _tables() -> tuple[LakeTable, ...]:
    pg = ("orders", "order_items", "order_payments", "order_reviews", "customers", "products", "sellers",
          "geolocation", "product_category_name_translation")
    silver = {"orders": "order_purchase_date", "order_items": "order_purchase_date",
              "order_payments": "order_purchase_date", "order_reviews": "review_date", "customers": None,
              "products": None, "sellers": None, "geolocation": None, "customer_changes": "requested_date",
              "customer_activity": "activity_date", "order_lines": "order_purchase_date"}
    gold = {"dim_date": None, "dim_product": None, "dim_seller": None, "dim_customer": None,
            "fact_order_lines": "order_purchase_date", "agg_daily_category_sales": "order_purchase_date",
            "customer_metrics": "as_of_date"}
    return (
        *(LakeTable(f"bronze_{t}", f"bronze/olist_postgres/{t}", "ingest_date") for t in pg),
        LakeTable("bronze_customer_changes", "bronze/crm/customer_changes", "ingest_date"),
        LakeTable("bronze_customer_activity", "bronze/api/customer_activity", "ingest_date"),
        *(LakeTable(f"silver_{t}", f"silver/{t}", p) for t, p in silver.items()),
        *(LakeTable(f"silver_quarantine_{t}", f"silver/_quarantine/{t}", "batch_date")
          for t in ("customer_changes", "customer_activity")),
        *(LakeTable(f"gold_{t}", f"gold/{t}", p) for t, p in gold.items()),
    )


TABLES = _tables()


def hive_type(spark_type: str) -> str:
    """Spark simpleString -> Athena (Hive DDL) type. Only the types the lake uses."""
    mapping = {"long": "bigint", "integer": "int", "short": "smallint", "byte": "tinyint"}
    return mapping.get(spark_type, spark_type)


def table_ddl(database: str, table: LakeTable, columns: list[tuple[str, str]]) -> str:
    """CREATE EXTERNAL TABLE for `columns` [(name, spark simpleString)], partition column excluded."""
    cols = ",\n".join(f"  `{c}` {hive_type(t)}" for c, t in columns if c != table.partition)
    location = f"{LAKE_PLACEHOLDER}/{table.path}/"
    lines = [f"CREATE EXTERNAL TABLE `{database}`.`{table.name}` (", cols, ")"]
    if table.partition:
        p = table.partition
        lines.append(f"PARTITIONED BY (`{p}` date)")
    lines += ["STORED AS PARQUET", f"LOCATION '{location}'"]
    if table.partition:
        props = {
            "projection.enabled": "true",
            f"projection.{p}.type": "date",
            f"projection.{p}.format": "yyyy-MM-dd",
            f"projection.{p}.range": PROJECTION_RANGE,
            f"projection.{p}.interval": "1",
            f"projection.{p}.interval.unit": "DAYS",
            "storage.location.template": f"{location}{p}=${{{p}}}/",
        }
        lines.append("TBLPROPERTIES (\n" + ",\n".join(f"  '{k}'='{v}'" for k, v in props.items()) + "\n)")
    return "\n".join(lines) + "\n"


def ddl_path(table: LakeTable) -> Path:
    return DDL_DIR / table.layer / f"{table.name}.sql"


def render(sql: str, lake_uri: str) -> str:
    """Fill in the lake location (s3://bucket/lake)."""
    return sql.replace(LAKE_PLACEHOLDER, lake_uri.rstrip("/"))


class Athena:
    """Minimal synchronous Athena runner (the workgroup enforces the result location and scan cutoff)."""

    def __init__(self, client, workgroup: str, database: str, poll_seconds: float = 0.5):
        self.client, self.workgroup, self.database, self.poll = client, workgroup, database, poll_seconds

    def run(self, sql: str) -> dict:
        qid = self.client.start_query_execution(
            QueryString=sql, WorkGroup=self.workgroup,
            QueryExecutionContext={"Database": self.database})["QueryExecutionId"]
        while True:
            ex = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = ex["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(self.poll)
        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena query {qid} {state}: {ex['Status'].get('StateChangeReason', '')}")
        return {"id": qid, "bytes": ex["Statistics"].get("DataScannedInBytes", 0),
                "ms": ex["Statistics"].get("EngineExecutionTimeInMillis", 0)}

    def rows(self, sql: str) -> tuple[list[tuple], dict]:
        """Run a query and return (rows as tuples of strings, stats). Header row dropped."""
        stats = self.run(sql)
        out, token = [], None
        while True:
            kw = {"QueryExecutionId": stats["id"], **({"NextToken": token} if token else {})}
            page = self.client.get_query_results(**kw)
            out += [tuple(c.get("VarCharValue") for c in r["Data"]) for r in page["ResultSet"]["Rows"]]
            token = page.get("NextToken")
            if not token:
                break
        return out[1:], stats
