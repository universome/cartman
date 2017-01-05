from enum import Enum, unique

from peewee import Field

@unique
class Ticker(Enum):
    GOOG    = 0
    IBM     = 1
    WMT     = 2
    GE      = 3
    MSFT    = 4

    @property
    def fullname(self):
        return _FULLNAMES[self]

_FULLNAMES = {
    Ticker.GOOG:    'Google',
    Ticker.IBM:     'IBM',
    Ticker.WMT:     'Walmart',
    Ticker.GE:      'General Electric',
    Ticker.MSFT:    'Microsoft'
}

class TickerField(Field):
    db_field = 'smallint'

    def db_value(self, ticker):
        return ticker.value

    def python_value(self, value):
        return Ticker(value)
