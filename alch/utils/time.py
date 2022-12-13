from typing import List, Tuple

import pandas as pd


def split_date_range(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    period: pd.Timedelta | pd.DateOffset = pd.offsets.MonthEnd(),
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Split a date range into a list of tuples representing individual periods.

    Args:
        start_date (pd.Timestamp): the start date of the range, as a pandas Timestamp
        end_date (pd.Timestamp): the end date of the range, as a pandas Timestamp
        period (pd.Timedelta | pd.DateOffset, optional): the period to split the range into. Defaults to pd.offsets.MonthEnd().

    Returns:
        List[Tuple[pd.Timestamp, pd.Timestamp]]: a list of tuples containing the start and end dates of each period in the date range
    """
    periods: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    current_date = start_date
    while current_date <= end_date:
        period_start = current_date
        period_end = min(end_date, period_start + period)
        periods.append((period_start, period_end))
        current_date = period_end + pd.Timedelta(days=1)
    return periods
