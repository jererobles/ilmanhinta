"""Holiday feature engineering using holidays library.

Provides Finnish holiday features for both consumption and price prediction models:
- is_holiday: Public holiday (businesses closed, low consumption)
- is_holiday_eve: Day before holiday (early closures, special patterns)
- is_bridge_day: Day between holiday and weekend (often taken off)

Bridge day logic:
- Thursday is holiday → Friday is bridge
- Tuesday is holiday → Monday is bridge
"""

from datetime import date, timedelta

import polars as pl
from holidays.countries.finland import Finland

from ilmanhinta.logging import get_logger

logger = get_logger(__name__)


def get_finnish_holidays() -> Finland:
    """Get Finnish holidays for years 2020-2031.

    Returns:
        Finland object with all Finnish public holidays
    """
    return Finland(years=range(2020, 2031))  # type: ignore[no-untyped-call]


def is_bridge_day(dt: date, fi_holidays: Finland) -> bool:
    """Check if date is a bridge day (between holiday and weekend).

    A bridge day is when there's a gap between a holiday and weekend that
    many people take off to create a long weekend.

    Examples:
        - Thursday is holiday → Friday is bridge day
        - Tuesday is holiday → Monday is bridge day
        - Holiday on Monday/Friday → not a bridge (already adjacent to weekend)

    Args:
        dt: Date to check
        fi_holidays: Finnish holidays object

    Returns:
        True if date is a bridge day
    """
    weekday = dt.weekday()  # 0=Monday, 6=Sunday

    # Friday: check if Thursday was holiday
    # Monday: check if Tuesday is holiday
    return (weekday == 4 and (dt - timedelta(days=1)) in fi_holidays) or (
        weekday == 0 and (dt + timedelta(days=1)) in fi_holidays
    )


def add_holiday_features(df: pl.DataFrame, time_col: str = "timestamp") -> pl.DataFrame:
    """Add Finnish holiday features to DataFrame.

    Adds three boolean columns:
    - is_holiday: True if date is a Finnish public holiday
    - is_holiday_eve: True if next day is a holiday (e.g., Dec 23 → Dec 24 is holiday)
    - is_bridge_day: True if between holiday and weekend

    Args:
        df: DataFrame with timestamp column
        time_col: Name of timestamp column (default: "timestamp")

    Returns:
        DataFrame with added holiday feature columns
    """
    if df.is_empty():
        logger.debug("Empty DataFrame, skipping holiday features")
        return df

    if time_col not in df.columns:
        logger.warning(f"Column '{time_col}' not found, skipping holiday features")
        return df

    fi_holidays = get_finnish_holidays()

    # Extract dates from timestamps
    dates = df[time_col].dt.date()

    # Add holiday features
    df = df.with_columns(
        [
            dates.map_elements(lambda d: d in fi_holidays, return_dtype=pl.Boolean).alias(
                "is_holiday"
            ),
            dates.map_elements(
                lambda d: (d + timedelta(days=1)) in fi_holidays, return_dtype=pl.Boolean
            ).alias("is_holiday_eve"),
            dates.map_elements(
                lambda d: is_bridge_day(d, fi_holidays), return_dtype=pl.Boolean
            ).alias("is_bridge_day"),
        ]
    )

    logger.debug("Added holiday features: is_holiday, is_holiday_eve, is_bridge_day")

    return df
