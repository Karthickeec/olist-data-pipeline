from olist_pipeline.config import PROJECT_ROOT, load_config


def test_defaults():
    cfg = load_config(environ={})
    assert cfg["pg"]["port"] == 5432
    assert cfg["paths"]["raw_dir"] == PROJECT_ROOT / "data" / "raw"


def test_env_overrides_keep_types():
    cfg = load_config(environ={
        "OLIST__PG__HOST": "db",
        "OLIST__PG__PORT": "5433",
        "OLIST__CUSTOMER_CHANGES__RATE": "0.1",
        "OLIST__PATHS__LANDING_DIR": "/tmp/landing",
    })
    assert cfg["pg"]["host"] == "db"
    assert cfg["pg"]["port"] == 5433
    assert cfg["customer_changes"]["rate"] == 0.1
    assert str(cfg["paths"]["landing_dir"]) == "/tmp/landing"
