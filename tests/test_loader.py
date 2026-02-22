"""Tests for the ingestion loader."""

from datetime import date

import pytest

from newschat.ingest.loader import ingest


def test_ingest_rejects_reversed_dates():
    with pytest.raises(ValueError, match="from_date.*must be.*to_date"):
        ingest(from_date=date(2024, 2, 1), to_date=date(2024, 1, 1))
