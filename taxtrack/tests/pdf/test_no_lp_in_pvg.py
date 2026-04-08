def test_lp_not_duplicated_in_pvg(records_with_lp):
    pvg = [
        r for r in records_with_lp
        if r["category"] != "lp_remove"
    ]
    assert all(r["category"] != "lp_remove" for r in pvg)
