from prop_ev.util.parsing import safe_float, safe_int, to_price


def test_safe_float_parses_numeric_inputs() -> None:
    assert safe_float(1) == 1.0
    assert safe_float(1.5) == 1.5
    assert safe_float("-2.25") == -2.25
    assert safe_float(True) is None
    assert safe_float("  ") is None
    assert safe_float("abc") is None


def test_safe_int_parses_integer_like_inputs() -> None:
    assert safe_int(1) == 1
    assert safe_int(1.9) == 1
    assert safe_int("+120") == 120
    assert safe_int("-110") == -110
    assert safe_int(True) is None
    assert safe_int("") is None
    assert safe_int("x") is None


def test_to_price_aliases_safe_int() -> None:
    assert to_price("+125") == 125
    assert to_price("-105") == -105
    assert to_price(None) is None
