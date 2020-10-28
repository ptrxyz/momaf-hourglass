import json
from flask import request
from .util import reply, API

api = API('dev_api')


@api.blueprint.route("/", methods=["GET"])
def dev():
    return ("Dev-API is present.", 200)


@api.blueprint.route("/test_json", methods=["POST"])
def jsontest():
    return (json.dumps(request.get_json()), 200)


@api.blueprint.route('/keys/<table>/', methods=["GET"])
def get_keys(table):
    def decode(lst):
        return [x.decode("utf-8") for x in lst]
    print(decode(api.redis.keys(f"{table}.*")))
    return reply.OK
