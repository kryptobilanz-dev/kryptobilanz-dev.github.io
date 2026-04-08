def to_float(val):
    """
    Wandelt kommagetrennte Zahlen zuverlässig in float um.
    Beispiele:
       '3,393.64'   → 3393.64
       '1.234,56'   → 1234.56
       '0'          → 0.0
       ''           → 0.0
       None         → 0.0
    """
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)

    s = str(val).strip()

    if s == "":
        return 0.0

    # 1) Entferne Leerzeichen
    s = s.replace(" ", "")

    # 2) Tausendertrenner entfernen (z.B. "3,393.64")
    if "," in s and "." in s:
        if s.find(",") < s.find("."):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")

    # 3) Wenn nur Komma existiert (DE-Format)
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except:
        return 0.0
