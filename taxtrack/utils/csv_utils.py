"""
Case-insensitive CSV field lookup. Etherscan and other CSVs may use "From"/"To"
while code often expects "from"/"to". get_field() finds the first matching key
by case-insensitive name.
"""


def get_field(row: dict, *names: str):
    """
    Return the value for the first key that matches any of the given names
    case-insensitively. If no match, return "".

    Example: get_field(line, "from", "From") finds row["From"] when header is "From".
    """
    if not row:
        return ""
    row_lower = {}
    for k, v in row.items():
        kl = str(k).lower()
        if kl not in row_lower:
            row_lower[kl] = v
    for name in names:
        n = str(name).strip().lower()
        if n in row_lower:
            val = row_lower[n]
            return str(val) if val is not None else ""
    return ""
