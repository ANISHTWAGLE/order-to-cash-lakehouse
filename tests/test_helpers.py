import pytest

from src.helpers import fq_name, safe_identifier, volume_path


def test_safe_identifier_accepts_plain_names():
    assert safe_identifier("workspace") == "workspace"
    assert safe_identifier("_platform_check") == "_platform_check"


@pytest.mark.parametrize("bad", ["", "a b", "a;drop table x", "1abc", "a-b", "a.b", None])
def test_safe_identifier_rejects_bad_names(bad):
    with pytest.raises(ValueError):
        safe_identifier(bad)


def test_fq_name():
    assert fq_name("workspace", "bronze") == "`workspace`.`bronze`"
    assert fq_name("workspace", "bronze", "orders") == "`workspace`.`bronze`.`orders`"


def test_volume_path():
    assert volume_path("workspace", "bronze", "landing") == "/Volumes/workspace/bronze/landing"
    assert (
        volume_path("workspace", "bronze", "landing", "raw/olist", "/orders/")
        == "/Volumes/workspace/bronze/landing/raw/olist/orders"
    )
