"""
Implementation of the AWS event filtering syntax
https://docs.aws.amazon.com/lambda/latest/dg/invocation-eventfiltering.html#filtering-syntax
"""

# pylint: disable=line-too-long, too-few-public-methods

from enum import Enum
from operator import eq, ge, gt, le, lt
from .utils import pairwise


class CriteriumType(Enum):
    """
    Criterium Types
    """

    ANYTHING_BUT = "anything-but"
    NUMERIC = "numeric"
    EXISTS = "exists"
    PREFIX = "prefix"


NUMERIC_OPERATORS = {
    "=": eq,
    ">": gt,
    ">=": ge,
    "<": lt,
    "<=": le,
}


def _filter_value(value, criterium, err):
    return (value == criterium) if not err else False


def _filter_anything_but(value, criterium, err):
    if not isinstance(criterium[CriteriumType.ANYTHING_BUT.value], list):
        raise ValueError(
            f"'{CriteriumType.ANYTHING_BUT.value}' criterium reference must be a list of values"
        )
    return (
        (value not in criterium[CriteriumType.ANYTHING_BUT.value]) if not err else False
    )


def _filter_numeric(value, criterium, err):
    if not len(criterium[CriteriumType.NUMERIC.value]) % 2 == 0:
        raise ValueError(
            f"'{CriteriumType.NUMERIC.value}' criterium reference must be an even sized array in form of [operation1, reference_value1, ...]"
        )
    return (
        (
            all(
                NUMERIC_OPERATORS[op](value, ref)
                for op, ref in pairwise(criterium[CriteriumType.NUMERIC.value])
            )
        )
        if not err
        else False
    )


def _filter_exists(_, criterium, err):
    return (criterium[CriteriumType.EXISTS.value]) == (err is None)


def _filter_prefix(value, criterium, err):
    if not isinstance(criterium[CriteriumType.PREFIX.value], str):
        raise ValueError(
            f"'{CriteriumType.PREFIX.value}' criterium reference must be a string"
        )
    return (
        (
            isinstance(value, str)
            and value.startswith(criterium[CriteriumType.PREFIX.value])
        )
        if not err
        else False
    )


def _get_value(entry, path):
    """
    Gets the [nested] value from a given `entry` under a given `path`.
    """
    try:
        value = entry
        for item in path:
            value = value[item]
        return value, None
    except KeyError:
        return None, KeyError


def get_filter(criterium):
    """
    Returns the filter function based on input type
    """
    if criterium is None:
        return _filter_value
    if isinstance(criterium, (str, int, float, bool)):
        return _filter_value
    if isinstance(criterium, dict) and len(criterium) == 1:
        matcher_map = {
            CriteriumType.ANYTHING_BUT.value: _filter_anything_but,
            CriteriumType.NUMERIC.value: _filter_numeric,
            CriteriumType.EXISTS.value: _filter_exists,
            CriteriumType.PREFIX.value: _filter_prefix,
        }
        return matcher_map.get(next(iter(criterium.keys())))
    return None


def check_match(entry, path, criteria):
    """
    Check if value in `entry` under `path` matches any of the supplied `criteria`
    """
    value, err = _get_value(entry=entry, path=path)
    for criterium in criteria:
        matcher = get_filter(criterium=criterium)
        if matcher(value, criterium, err):
            return True
    return False


class Filter:
    """
    Class for filtering entries (dict) based on a given filter set
    """

    def __init__(self, filters: list | None) -> None:
        self.filters = filters or []

    def __call__(self, entry) -> bool:
        entry = dict(entry)
        if len(self.filters) == 0:
            return True

        for filter_ in self.filters:
            result = self._filter(entry=entry, filter_=filter_)
            if result:
                return True
        return False

    def _filter(self, entry, filter_, root=None):
        root = root or []
        results = []
        for field, criteria in filter_.items():
            path = root + [field]
            if isinstance(criteria, list):
                results.append(check_match(entry=entry, path=path, criteria=criteria))
            elif isinstance(criteria, dict):
                results.append(self._filter(entry=entry, filter_=criteria, root=path))
        return all(results)
