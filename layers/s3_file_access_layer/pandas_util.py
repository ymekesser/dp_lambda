import pandas as pd
from functools import reduce


def select_columns(df: pd.DataFrame, selector: dict[str, str]) -> pd.DataFrame:
    return df.rename(columns=selector)[[*selector.values()]]


def coalesce_colums(df: pd.DataFrame, cols: list[str], out_col: str) -> pd.DataFrame:
    series = [df[col] for col in cols]

    coalesced_series = reduce(
        lambda series1, series2: series1.combine_first(series2), series
    )

    df[out_col] = coalesced_series

    return df
