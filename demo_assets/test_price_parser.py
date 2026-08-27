from price_parser import parse_price


def test_usd_price() -> None:
    assert str(parse_price("$12.50")) == "12.50"


def test_euro_price_with_thousands_separator() -> None:
    assert str(parse_price("€1,234.50")) == "1234.50"

