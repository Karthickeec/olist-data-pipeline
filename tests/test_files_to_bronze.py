import copy
from datetime import date, datetime

import pytest
from conftest import assert_same_rows

from olist_pipeline.bronze.files_to_bronze import ingest_customer_changes
from olist_pipeline.config import load_config
from olist_pipeline.customer_changes import partition_dir, write_changes
from olist_pipeline.lake import partition_path

DAY = date(2017, 3, 1)
RUN = datetime(2026, 10, 8, 12, 0, 0)
GOOD = {
    "change_id": "c1",
    "customer_unique_id": "u1",
    "new_zip_code_prefix": "01037",
    "new_city": "sao paulo",
    "new_state": "SP",
    "requested_at": "2017-03-01 10:00:00",
    "source": "crm_portal",
}


@pytest.fixture
def cfg(tmp_path):
    cfg = copy.deepcopy(load_config())
    cfg["paths"]["landing_dir"] = tmp_path / "landing"
    cfg["lake"]["root"] = str(tmp_path / "lake")
    return cfg


def bronze_partition(spark, cfg, day=DAY):
    return spark.read.parquet(partition_path(f"{cfg['lake']['root']}/bronze/crm/customer_changes", day))


def test_keeps_every_row_including_dirty_and_malformed(spark, cfg):
    records = [
        GOOD,
        GOOD,  # exact duplicate
        {**GOOD, "change_id": "c2", "new_state": " sp "},  # messy state
        {**GOOD, "change_id": "c3", "new_city": None},
    ]  # null city
    path = write_changes(cfg["paths"]["landing_dir"], DAY, records)
    with open(path, "a", encoding="utf-8") as f:
        f.write('{"change_id": "c4", "customer_unique_id": \n')  # not valid JSON

    result = ingest_customer_changes(spark, cfg, DAY, RUN)
    assert result.rows == 5
    rows = bronze_partition(spark, cfg).collect()
    assert [r.change_id for r in rows].count("c1") == 2
    assert {r.new_state for r in rows if r.change_id == "c2"} == {" sp "}
    assert [r.new_city for r in rows if r.change_id == "c3"] == [None]
    [bad] = [r for r in rows if r._corrupt_record is not None]
    assert bad.change_id is None and bad._corrupt_record.startswith('{"change_id": "c4"')
    assert all(r._source == "file:customer_changes" and r._batch_date == DAY for r in rows)
    assert all(r._source_file.endswith("dt=2017-03-01/changes.jsonl") for r in rows)


def test_values_are_kept_as_strings(spark, cfg):
    write_changes(cfg["paths"]["landing_dir"], DAY, [GOOD])
    ingest_customer_changes(spark, cfg, DAY, RUN)
    row = bronze_partition(spark, cfg).first()
    assert {k: row[k] for k in GOOD} == GOOD


def test_rerun_is_identical_apart_from_ingested_at(spark, cfg):
    write_changes(cfg["paths"]["landing_dir"], DAY, [GOOD, {**GOOD, "change_id": "c2"}])
    ingest_customer_changes(spark, cfg, DAY, RUN)
    df = bronze_partition(spark, cfg).drop("_ingested_at")
    first = spark.createDataFrame(df.collect(), df.schema)
    ingest_customer_changes(spark, cfg, DAY, datetime(2026, 10, 9))
    assert_same_rows(first, bronze_partition(spark, cfg).drop("_ingested_at"))


def test_empty_day_gives_zero_rows(spark, cfg):
    write_changes(cfg["paths"]["landing_dir"], DAY, [])
    assert ingest_customer_changes(spark, cfg, DAY, RUN).rows == 0


def test_missing_landing_partition_fails(spark, cfg):
    assert not partition_dir(cfg["paths"]["landing_dir"], DAY).exists()
    with pytest.raises(FileNotFoundError):
        ingest_customer_changes(spark, cfg, DAY, RUN)
