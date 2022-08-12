from collections import OrderedDict
import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from marshmallow import Schema, fields


# app setup
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config ['JSON_SORT_KEYS'] = False
db = SQLAlchemy(app)


# models
class Ngram(db.Model):
    __tablename__ = "ngrams"

    doi = db.Column(db.Text, primary_key=True)
    json_ngrams = db.Column(db.JSON)


# schemas
class MetaSchema(Schema):
    count = fields.Int()
    openalex_id = fields.Str()
    doi = fields.Str()

    class Meta:
        ordered = True


class NgramsSchema(Schema):
    ngram = fields.Str()
    ngram_tokens = fields.Int()
    ngram_count = fields.Int()
    term_frequency = fields.Float()

    class Meta:
        ordered = True


class MessageSchema(Schema):
    meta = fields.Nested(MetaSchema)
    ngrams = fields.Nested(NgramsSchema, many=True)

    class Meta:
        ordered = True


# views
@app.route('/')
def ngrams_view():
    result = OrderedDict()
    ngrams = Ngram.query.filter_by(doi='10.1080/00039896.1983.10543998').first()
    result["meta"] = {"count": len(ngrams.json_ngrams), "doi": "10.1080/00039896.1983.10543998"}
    result["ngrams"] = ngrams.json_ngrams
    message_schema = MessageSchema()
    return message_schema.dump(result)


if __name__ == '__main__':
    app.run()
