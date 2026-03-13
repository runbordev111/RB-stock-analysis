from typing import List
from shioaji.base import BaseModel
from shioaji.contracts import BaseContract


class Watchlist(BaseModel):
    id: str = ""
    person_id: str = ""
    name: str = ""
    contracts: List[BaseContract] = []
