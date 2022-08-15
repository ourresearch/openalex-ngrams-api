from collections import OrderedDict
import os

from elasticsearch_dsl import Search, connections
from flask import Flask, abort
from flask_sqlalchemy import SQLAlchemy
from marshmallow import Schema, fields


# app setup
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["JSON_SORT_KEYS"] = False
db = SQLAlchemy(app)
connections.create_connection(hosts=[os.environ.get("ES_URL_PROD")], timeout=30)


# models
class Ngram(db.Model):
    __tablename__ = "ngrams"

    doi = db.Column(db.Text, primary_key=True)
    json_ngrams = db.Column(db.JSON)


# schemas
class MetaSchema(Schema):
    count = fields.Int()
    doi = fields.Str()
    openalex_id = fields.Str()

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
@app.route("/works/<openalex_id>/ngrams")
def ngrams_view(openalex_id):
    result = OrderedDict()
    doi = openalex_id_to_doi(openalex_id)
    if doi:
        short_doi = doi.replace("https://doi.org/", "")
        ngrams = Ngram.query.filter_by(doi=short_doi).first()
        if ngrams:
            result["meta"] = {
                "count": len(ngrams.json_ngrams),
                "doi": doi,
                "openalex_id": f"https://openalex.org/{openalex_id}",
            }
            result["ngrams"] = ngrams.json_ngrams
        else:
            result["meta"] = {
                "count": 0,
                "doi": doi,
                "openalex_id": f"https://openalex.org/{openalex_id}",
            }
            result["ngrams"] = []
    else:
        result["meta"] = {
            "count": 0,
            "doi": None,
            "openalex_id": f"https://openalex.org/{openalex_id}",
        }
        result["ngrams"] = []
    message_schema = MessageSchema()
    return message_schema.dump(result)


def openalex_id_to_doi(openalex_id):
    openalex_id = f"https://openalex.org/{openalex_id}"
    WORKS_INDEX = "works-v13-*,-*invalid-data"
    s = Search(index=WORKS_INDEX)
    s = s.extra(size=1)
    s = s.source(["id", "doi"])
    s = s.filter("term", id=openalex_id)
    response = s.execute()
    if s.count() == 0:
        # openalex id is not valid
        abort(404)
    for r in response:
        doi = r.doi
        return doi


if __name__ == "__main__":
    app.run()
