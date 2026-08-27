from decimal import Decimal


def parse_price(value: str) -> Decimal:
    """Parse a USD or EUR price into a decimal amount."""
    cleaned = value.strip().replace("$", "")
    return Decimal(cleaned)

