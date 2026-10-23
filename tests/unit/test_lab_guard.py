"""The lab-database guard must keep protecting the environment's database after the CLI's --database switches to a
lab database (it does so by replacing PGDATABASE for the rest of the process)."""

import pytest

from signalflow import cli
from signalflow.chaos import lab


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in lab.PROTECTED_ENV + (lab.ORIGINAL_PGDATABASE_ENV,):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PGDATABASE", "main_lab")
    return monkeypatch


def test_environment_database_is_protected(env: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="refusing"):
        lab.lab_database_name("main_lab")


def test_cli_database_switch_keeps_the_original_protected(env: pytest.MonkeyPatch) -> None:
    cli._use_database("signalflow_lab")
    assert lab.lab_database_name("signalflow_lab") == "signalflow_lab"  # the target itself is usable
    cli._use_database("other_lab")  # a second switch must not forget the original
    for name in ("main_lab", "signalflow"):  # main_lab would pass every other check
        with pytest.raises(ValueError, match="refusing"):
            lab.lab_database_name(name)
