from peewee import *

from base.database import BaseModel

class Tweet(BaseModel):
    oid = IntegerField(primary_key=True)
    ticker = CharField(5)
    id = IntegerField()
    date = TimestampField(utc=True, index=True)
    user_id = IntegerField()
    text = TextField()
    retweet_count = IntegerField()
    favorite_count = IntegerField()

    class Meta:
        indexes = [
            (('id', 'ticker'), True)    # Can be dropped after scraping.
        ]

class Quote(BaseModel):
    oid = IntegerField(primary_key=True)
    ticker = CharField(5)
    date = TimestampField(utc=True)
    interval = IntegerField()
    open_price = FloatField()
    low_price = FloatField()
    high_price = FloatField()
    close_price = FloatField()
    volume = FloatField()

    class Meta:
        indexes = [
            (('date', 'interval'), False)
        ]

class News(BaseModel):
    oid = IntegerField(primary_key=True)
    ticker = CharField(5)
    id = IntegerField()
    date = TimestampField(utc=True, index=True)
    source = TextField(null=True)
    title = TextField()
    description = TextField(null=True)
    url = TextField(null=True)
    engagement = IntegerField(null=True)
    marked = BooleanField()

    class Meta:
        indexes = [
            (('id', 'ticker'), True)    # Can be dropped after scraping.
        ]

class Article(BaseModel):
    oid = IntegerField(primary_key=True)
    ticker = CharField(5)
    date = TimestampField(utc=True, index=True)
    title = TextField()
    seo_title = TextField(null=True)
    url = TextField()
    category = TextField(null=True)
    word_count = IntegerField(null=True)
    section = TextField(null=True)
    type_of_material = TextField(null=True)
    first_paragraph = TextField(null=True)
    keywords = TextField(null=True)
    has_multimedia = BooleanField()
