from typing import List

from pydantic import BaseModel


class Interval(BaseModel):
    interval: int
    interval_name: str
    max_history_days: int


class Intervals(List):
    intervals: List[Interval]


class Symbol(BaseModel):
    open_at: bool = None
    is_open: bool
    exchange: str
