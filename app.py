from collections import OrderedDict
import os
import re

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
WORKS_INDEX = "works-v20-*,-*invalid-data"


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


# utils
def is_openalex_id(input_id):
    if re.findall(r"^([wW]\d{2,})", input_id):
        return True
    return False


def is_doi(input_id):
    if input_id.startswith("https://doi.org") or input_id.startswith("10."):
        return True
    return False


def openalex_id_to_doi(openalex_id):
    openalex_id = f"https://openalex.org/{openalex_id}"
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


def doi_to_openalex_id(doi):
    if "https://doi.org" not in doi:
        doi = f"https://doi.org/{doi.lower()}"
    s = Search(index=WORKS_INDEX)
    s = s.extra(size=1)
    s = s.source(["id", "doi"])
    s = s.filter("term", ids__doi=doi)
    response = s.execute()
    if s.count() == 0:
        # doi not found in openalex
        abort(404, "DOI not found in OpenAlex.")
    for r in response:
        openalex_id = r.id
        return openalex_id


# views
@app.route("/works/<path:input_id>/ngrams")
def ngrams_view(input_id):
    result = OrderedDict()
    doi = None
    openalex_id = None

    if is_openalex_id(input_id):
        doi = openalex_id_to_doi(input_id)
        openalex_id = f"https://openalex.org/{input_id}"
    elif is_doi(input_id):
        doi = f"https://doi.org/{input_id.lower()}"
        openalex_id = doi_to_openalex_id(doi)
    else:
        abort(404, "Invalid ID format. Needs to be an OpenAlex ID or DOI.")

    if doi:
        short_doi = doi.replace("https://doi.org/", "")
        ngrams = Ngram.query.filter_by(doi=short_doi).first()
        if ngrams and ngrams.json_ngrams:
            result["meta"] = {
                "count": len(ngrams.json_ngrams),
                "doi": doi,
                "openalex_id": openalex_id,
            }
            result["ngrams"] = ngrams.json_ngrams
        else:
            result["meta"] = {
                "count": 0,
                "doi": doi,
                "openalex_id": openalex_id,
            }
            result["ngrams"] = []
    else:
        result["meta"] = {
            "count": 0,
            "doi": None,
            "openalex_id": openalex_id,
        }
        result["ngrams"] = []
    message_schema = MessageSchema()
    return message_schema.dump(result)


if __name__ == "__main__":
    app.run()
