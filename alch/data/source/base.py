from abc import ABC, abstractmethod, abstractproperty
from typing import List, Optional

import pandas as pd


class BaseDataSource(ABC):
    """Base class for data sources."""

    supported_datasets: List[str]

    def __init__(self) -> None:
        pass

    @abstractmethod
    def fetch(
        self,
        dataset: str,
        *,
        symbol: Optional[str] = None,
        start_date: Optional[pd.Timestamp] = None,
        end_date: Optional[pd.Timestamp] = None,
        trade_date: Optional[pd.Timestamp] = None,
        frequency: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        raise NotImplementedError("subclasses must override fetch()")
