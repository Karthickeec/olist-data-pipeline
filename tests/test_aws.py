import pytest

from olist_pipeline import aws
from olist_pipeline.athena import TABLES, LakeTable, hive_type, render, table_ddl
from olist_pipeline.aws import (Secret, apply_aws_target, bucket_name, parse_secret_ref, resolve_secrets,
                                s3a_conf, to_s3_uri)
from olist_pipeline.config import load_config

SECRET = {"pg_password": "pg-s3cr3t", "api_key": "api-s3cr3t"}


def fake_fetch(calls):
    def fetch(secret_id, region):
        calls.append((secret_id, region))
        return SECRET
    return fetch


def test_bucket_name_is_built_from_account_and_region():
    cfg = load_config(environ={})
    assert bucket_name(cfg["aws"], account="123456789012") == "olist-pipeline-123456789012-apse2"
    assert bucket_name({**cfg["aws"], "bucket": "explicit"}) == "explicit"


def test_parse_secret_ref():
    assert parse_secret_ref("secret://olist/pipeline#api_key") == ("olist/pipeline", "api_key")
    for bad in ("olist/pipeline#api_key", "secret://olist/pipeline", "secret://#x", "secret://a#"):
        with pytest.raises(ValueError):
            parse_secret_ref(bad)


def test_aws_target_resolves_secrets_and_masks_them():
    cfg = load_config(environ={})
    apply_aws_target(cfg, bucket="b")
    assert cfg["lake"]["root"] == "s3a://b/lake" and cfg["spark"]["master"] == "local[2]"
    calls = []
    resolve_secrets(cfg, "ap-southeast-2", fake_fetch(calls))
    assert cfg["pg"]["password"] == "pg-s3cr3t" and cfg["api"]["key"] == "api-s3cr3t"
    assert isinstance(cfg["pg"]["password"], Secret)
    # Usable as a str (psycopg, HTTP header) but never shown in a config dump or traceback.
    dump = repr(cfg)
    assert "s3cr3t" not in dump and "'***'" in dump
    assert all(c == ("olist/pipeline", "ap-southeast-2") for c in calls)


def test_load_config_with_target_aws(monkeypatch):
    monkeypatch.setattr(aws, "account_id", lambda region: "123456789012")
    monkeypatch.setattr(aws, "_secret_json", lambda secret_id, region: SECRET)
    cfg = load_config(environ={"OLIST_TARGET": "aws"})
    assert cfg["lake"]["root"] == "s3a://olist-pipeline-123456789012-apse2/lake"
    assert cfg["api"]["key"] == "api-s3cr3t"
    assert "s3cr3t" not in repr(cfg)
    # The default target stays local and never touches AWS.
    local = load_config(environ={})
    assert not local["lake"]["root"].startswith("s3") and local["pg"]["password"] == "olist"


def test_missing_secret_field():
    with pytest.raises(KeyError):
        aws.resolve_secret("secret://olist/pipeline#nope", "ap-southeast-2", lambda *_: SECRET)


def test_s3a_conf_has_no_keys():
    conf = s3a_conf("ap-southeast-2")
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "s3.ap-southeast-2.amazonaws.com"
    assert not any("access.key" in k or "secret.key" in k for k in conf)
    assert to_s3_uri("s3a://b/lake") == "s3://b/lake" and to_s3_uri("/tmp/x") == "/tmp/x"


def test_table_ddl_with_projection():
    t = LakeTable("silver_orders", "silver/orders", "order_purchase_date")
    ddl = table_ddl("olist_lake", t, [("order_id", "string"), ("n", "long"), ("amount", "decimal(12,2)"),
                                      ("order_purchase_date", "date")])
    assert "`n` bigint" in ddl and "`amount` decimal(12,2)" in ddl
    # The partition column is declared once, in PARTITIONED BY.
    assert ddl.count("`order_purchase_date`") == 1 and "PARTITIONED BY (`order_purchase_date` date)" in ddl
    assert "'projection.order_purchase_date.type'='date'" in ddl
    sql = render(ddl, "s3://bucket/lake")
    assert "LOCATION 's3://bucket/lake/silver/orders/'" in sql
    assert "'storage.location.template'='s3://bucket/lake/silver/orders/order_purchase_date=${order_purchase_date}/'" in sql


def test_table_ddl_unpartitioned_and_types():
    ddl = table_ddl("db", LakeTable("gold_dim_date", "gold/dim_date"), [("date_key", "integer")])
    assert "PARTITIONED BY" not in ddl and "TBLPROPERTIES" not in ddl and "`date_key` int" in ddl
    assert hive_type("short") == "smallint" and hive_type("timestamp") == "timestamp"


def test_catalog_covers_every_layer():
    names = [t.name for t in TABLES]
    assert len(names) == len(set(names)) == 31
    assert {t.layer for t in TABLES} == {"bronze", "silver", "gold"}


def test_s3_parity_etags(tmp_path):
    """Single-part ETag = MD5; multipart = MD5 of part MD5s + "-n" (CLI 8 MiB parts, Glue one-part uploads)."""
    import hashlib
    import importlib.util
    from olist_pipeline.config import PROJECT_ROOT
    spec = importlib.util.spec_from_file_location("parity", PROJECT_ROOT / "scripts" / "verify_s3_parity.py")
    parity = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parity)
    small, big = tmp_path / "small", tmp_path / "big"
    small.write_bytes(b"x" * 1000)
    big.write_bytes(bytes(range(256)) * (9 * 1024 * 1024 // 256 + 10))   # just over 9 MiB -> 2 parts of 8 MiB
    md5 = hashlib.md5(small.read_bytes()).hexdigest()
    assert parity.etag_matches(small, md5)
    assert parity.etag_matches(small, hashlib.md5(hashlib.md5(small.read_bytes()).digest()).hexdigest() + "-1")
    assert parity.etag_matches(big, parity.multipart_etag(big.read_bytes(), 8 * 1024 * 1024))
    assert not parity.etag_matches(small, "0" * 32)
