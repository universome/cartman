import os
from os import path

from playhouse.sqlite_ext import SqliteExtDatabase
from peewee import Model

_db_path = path.abspath(path.join(__file__, '../..', os.environ['DATABASE']))
db = SqliteExtDatabase(_db_path)
db.connect()

class BaseModel(Model):
    class Meta:
        database = db
