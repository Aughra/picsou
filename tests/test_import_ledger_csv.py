import pytest
from src.import_ledger_csv import get_first, to_float, row_to_tx, bulk_insert
from typing import Any


def test_get_first():
    d = {"a": 1, "b": 2}
    assert get_first(d, ["b", "a"]) == 2
    assert get_first(d, ["c", "a"]) == 1
    assert get_first(d, ["c"]) is None


def test_to_float():
    assert to_float("3.14") == 3.14
    assert to_float("abc", default=1.0) == 1.0
    assert to_float(None) == 0.0


def test_row_to_tx():
    row = {"date": "2025-09-21", "amount": "100.5"}
    tx = row_to_tx(row)
    assert tx["date"] == "2025-09-21"
    assert tx["amount"] == 100.5


def test_bulk_insert(mocker: Any):
    mock_conn = mocker.patch("src.import_ledger_csv.get_conn")
    mock_cursor = mock_conn.return_value.__enter__.return_value.cursor.return_value

    rows = [
        {"date": "2025-09-21", "amount": 100.5},
        {"date": "2025-09-22", "amount": 200.0},
    ]

    mock_cursor.rowcount = len(rows)
    result = bulk_insert(rows)

    assert result == len(rows)
    mock_cursor.executemany.assert_called_once()
