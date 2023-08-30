import pytest

from datatoolz.filtering import Filter


def test_filter_is_null():
    entries = [
        {},
        {"field": None},
        {"nested": {"field": None}},
        {"field": 1},
        {"nested": {"field": 1}},
        {"field": None, "nested": {"field": None}},
        {"field": 1, "nested": {"field": None}},
    ]

    filter = Filter(filters=[{"field": [None]}, {"nested": {"field": [None]}}])
    expected = (False, True, True, False, False, True, True)

    for entry, exp in zip(entries, expected):
        print(entry)
        assert filter(entry=entry) is exp

    filter = Filter(filters=[{"field": [None], "nested": {"field": [None]}}])
    expected = (False, False, False, False, False, True, False)

    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp


def test_filter_value():
    entries = [
        {},
        {"field": None},
        {"nested": {"field": None}},
        {"field": "value"},
        {"nested": {"field": "value"}},
        {"nested": {"empty": ""}},
        {"field": 1},
        {"field": False},
    ]

    filter = Filter(
        filters=[
            {"field": ["value"]},
            {"nested": {"field": ["value"]}},
            {"nested": {"empty": [""]}},
        ]
    )
    expected = (False, False, False, True, True, True, False, False)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp

    filter = Filter(
        filters=[
            {"field": [1, False]},
        ]
    )
    expected = (False, False, False, False, False, False, True, True)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp


def test_filter_anything_but():
    entries = [
        {},
        {"field": None},
        {"field": ""},
        {"field": "a"},
        {"field": "b"},
        {"field": 1},
    ]

    filter = Filter(filters=[{"field": [{"anything-but": [""]}]}])
    expected = (False, True, False, True, True, True)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp

    filter = Filter(filters=[{"field": [{"anything-but": [1, "a"]}]}])
    expected = (False, True, True, False, True, False)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp

    filter = Filter(filters=[{"field": [{"anything-but": [None]}]}])
    expected = (False, False, True, True, True, True)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp


def test_filter_anything_but_with_error():
    filter = Filter(filters=[{"field": [{"anything-but": "invalid-reference"}]}])
    with pytest.raises(ValueError):
        filter(entry={"field": "a"})


def test_filter_numeric():
    entries = [
        {},
        {"field": 0},
        {"field": 1},
        {"field": 1.1},
    ]

    filter = Filter(filters=[{"field": [{"numeric": [">", 0]}]}])
    expected = (False, False, True, True)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp

    filter = Filter(filters=[{"field": [{"numeric": [">", 0.5, "<", 1.01]}]}])
    expected = (False, False, True, False)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp

    filter = Filter(filters=[{"field": [{"numeric": ["<", 1]}, {"numeric": [">", 1]}]}])
    expected = (False, True, False, True)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp


def test_filter_numeric_with_error():
    filter = Filter(filters=[{"field": [{"numeric": [">", 0]}]}])
    with pytest.raises(TypeError):
        filter(entry={"field": "a"})

    filter = Filter(filters=[{"field": [{"numeric": [">", 0, "<="]}]}])
    with pytest.raises(ValueError):
        filter(entry={"field": 1})


def test_filter_exists():
    entries = [
        {},
        {"field": None},
        {"another-field": None},
        {"field": None, "another-field": None},
    ]

    filter = Filter(filters=[{"field": [{"exists": True}]}])
    expected = (False, True, False, True)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp

    filter = Filter(filters=[{"field": [{"exists": False}]}])
    expected = (True, False, True, False)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp


def test_filter_prefix():
    entries = [
        {},
        {"field": None},
        {"field": "value one"},
        {"field": "value two"},
    ]

    filter = Filter(filters=[{"field": [{"prefix": "value"}]}])
    expected = (False, False, True, True)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp

    filter = Filter(filters=[{"field": [{"prefix": "value o"}]}])
    expected = (False, False, True, False)
    for entry, exp in zip(entries, expected):
        assert filter(entry=entry) is exp


def test_filter_wrong_type():
    entries = [
        {},
        {"field": None},
        {"field": "value one"},
        {"field": "value two"},
    ]

    filter = Filter(filters=[{"field": [{"invalid-type": None, "another": None}]}])

    with pytest.raises(TypeError):
        filter(entry={"field": None})
