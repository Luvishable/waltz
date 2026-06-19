import pytest

import waltz.config as config_mod
from waltz.config import StreamConfig


@pytest.fixture
def no_dotenv(monkeypatch):
    # Keep the real .env out so the test fully controls the environment.
    monkeypatch.setattr(config_mod, "load_dotenv", lambda *a, **k: False)


def _set_required(monkeypatch):
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_USER", "waltz")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("DB_NAME", "mydb")


def test_from_env_reads_all_fields(monkeypatch, no_dotenv):
    _set_required(monkeypatch)
    monkeypatch.setenv("DB_HOST", "db.internal")
    monkeypatch.setenv("WALTZ_SLOT", "my_slot")
    monkeypatch.setenv("WALTZ_PUBLICATION", "my_pub")
    monkeypatch.setenv("WALTZ_CHECKPOINT", "/tmp/x.lsn")
    assert StreamConfig.from_env() == StreamConfig(
        host="db.internal", port=5432, user="waltz", password="secret",
        dbname="mydb", slot="my_slot", publication="my_pub",
        checkpoint_path="/tmp/x.lsn",
    )


def test_from_env_applies_defaults(monkeypatch, no_dotenv):
    _set_required(monkeypatch)
    for var in ("DB_HOST", "WALTZ_SLOT", "WALTZ_PUBLICATION", "WALTZ_CHECKPOINT"):
        monkeypatch.delenv(var, raising=False)
    cfg = StreamConfig.from_env()
    assert cfg.host == "localhost"
    assert cfg.slot == "waltz_slot_pgo"
    assert cfg.publication == "waltz_pub"
    assert cfg.checkpoint_path == "waltz.lsn"


def test_missing_required_var_raises(monkeypatch, no_dotenv):
    _set_required(monkeypatch)
    monkeypatch.delenv("DB_PORT", raising=False)
    with pytest.raises(RuntimeError, match="DB_PORT"):
        StreamConfig.from_env()


def test_conninfo_contains_replication_mode(monkeypatch, no_dotenv):
    _set_required(monkeypatch)
    info = StreamConfig.from_env().conninfo()
    assert "replication=database" in info
    assert "dbname=mydb" in info
