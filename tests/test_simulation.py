"""Basic tests for the fulfillment platform configuration and data simulation."""

from config.constants import (
    BACKFILL_END_DATE,
    BACKFILL_START_DATE,
    DAILY_ORDERS,
    NUM_CUSTOMERS,
    NUM_PRODUCTS,
    NUM_WAREHOUSES,
    RANDOM_SEED,
)


def test_random_seed_is_deterministic():
    assert RANDOM_SEED == 42


def test_backfill_dates_are_valid():
    assert BACKFILL_START_DATE < BACKFILL_END_DATE


def test_simulation_scale():
    assert NUM_PRODUCTS == 500
    assert NUM_WAREHOUSES == 8
    assert NUM_CUSTOMERS == 10000
    assert DAILY_ORDERS > 0


def test_backfill_covers_multiple_years():
    delta = BACKFILL_END_DATE - BACKFILL_START_DATE
    assert delta.days > 365
