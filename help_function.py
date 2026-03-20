import re
from typing import Tuple, Any, Iterable
import re

DELIVERY_PREFIXES = (
    "CDEK",
    "Доставка курьером",
    "Самовывоз",
    "Наценка за наложенный платеж"
)

async def parse_the_cart_field(data: str):
    items = re.findall(r'^\s*\d+\.\s*(.+)$', data, flags=re.MULTILINE)

    products = []
    deliveries = []

    for item in items:
        line = item.strip()

        # 2. Classify based on the delivery prefixes
        if line.startswith(DELIVERY_PREFIXES):
            deliveries.append(line)
        else:
            products.append(line)

    # 3. Turn lists into strings (you can change the joiner if you want)
    products_str = "\n".join(products)
    deliveries_str = "\n".join(deliveries)

    return products_str, deliveries_str

async def parse_the_cart_field_2(data: str):
    promos_str = ""
    comments_str = data.strip() if isinstance(data, str) else ""

    if isinstance(data, str):
        m = re.search(r"Promo:\s*([^\s]+)(?:\s+(.*))?", data)
        if m:
            promos_str = (m.group(1) or "").strip()
            comments_str = (m.group(2) or "").strip()

    return promos_str, comments_str

_MISSING = object()

async def get_nested(data: Any, path: Iterable[str], default: Any = None) -> Any:
    """
    Safely get nested values from dicts/lists using a path of keys/indexes.
    Example:
        get_nested(nested, ["leads", "update", "0", "id"])
    """
    current = data

    for key in path:
        if isinstance(current, dict):
            current = current.get(key, _MISSING)
        elif isinstance(current, list):
            # support numeric keys for lists like ["0", "1"]
            try:
                idx = int(key)
                current = current[idx]
            except (ValueError, IndexError):
                current = _MISSING
        else:
            current = _MISSING

        if current is _MISSING:
            return default

    return current


async def get_custom_field_value(data: dict, field_id: int, default: Any = None) -> Any:
    """
    Finds a specific custom field by its ID and returns its value.
    """
    # 1. Safely get the list of all custom fields
    fields_list = await get_nested(data, ['custom_fields_values'], [])

    if not isinstance(fields_list, list):
        return default

    # 2. Iterate through the list to find the matching field_id
    # We use a generator expression with next() for efficiency
    target_field = next(
        (field for field in fields_list if field.get('field_id') == field_id),
        None
    )

    # 3. If the field is found, extract the value safely
    if target_field:
        # Custom fields usually store data in ['values'][0]['value']
        return await get_nested(target_field, ['values', '0', 'value'], default)

    return default


async def normalize_text(text: str) -> str | None:
    """
    Removes all whitespace (spaces, tabs, newlines) from the text
    to allow for 'content-only' comparison.
    """
    if not text:
        return None
    # Replace all whitespace characters ( \t\n\r\f\v) with an empty string
    return re.sub(r'\s+', '', str(text.lower()))

