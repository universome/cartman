from enum import Enum, unique

from peewee import Field

@unique
class Ticker(Enum):
    GOOG    = 0
    IBM     = 1
    WMT     = 2
    GE      = 3
    MSFT    = 4
    MMM     = 5
    T       = 6
    ADBE    = 7
    AA      = 8
    AXP     = 9
    AIG     = 10
    AMT     = 11
    AAPL    = 12
    AMAT    = 13
    BAC     = 14
    BA      = 15
    CA      = 16
    CAT     = 17
    CVX     = 18
    CSCO    = 19
    C       = 20
    KO      = 21
    GLW     = 22
    DD      = 23
    EMC     = 24
    XOM     = 25
    FSLR    = 26
    HPQ     = 27
    HD      = 28
    IP      = 29
    INTC    = 30
    JPM     = 31
    JNJ     = 32
    MCD     = 33
    MRK     = 34
    PFE     = 35
    PG      = 36
    TRV     = 37
    UTX     = 38
    VZ      = 39
    DIS     = 40
    WFC     = 41
    YHOO    = 42
    YND     = 43

class TickerField(Field):
    db_field = 'smallint'

    def db_value(self, ticker):
        return ticker.value

    def python_value(self, value):
        return Ticker(value)
