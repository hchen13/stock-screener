from enum import Enum
from typing import Union


class MARKET(Enum):
    SZ = 0
    SH = 1


class Stock:
    @classmethod
    def from_tdx_dataframe(cls, df):
        symbol_list = []
        for i, row in df.iterrows():
            instance = cls(row.code, row.exchange, row['name'])
            symbol_list.append(instance)
        return symbol_list

    def __init__(self, code: str, market: Union[str, int], name=None):
        self.code = code
        self.name = name
        if isinstance(market, str):
            if market.upper() not in MARKET.__members__:
                raise ValueError(f"Invalid market: {market}")
            self.market = MARKET[market.upper()]
        else:
            if market not in [i.value for i in MARKET.__members__.values()]:
                raise ValueError(f"Invalid market: {market}")
            self.market = MARKET(market)
        self.ts_code = f"{self.code}.{self.market.name}"
        self.symbol = self.ts_code

    def __str__(self):
        return self.ts_code

    def __repr__(self):
        return str(self)
