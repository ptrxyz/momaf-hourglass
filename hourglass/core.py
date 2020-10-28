import json
from .pkmap import Map
from flask import request
from .util import reply, timestamp, API

api = API('core_api')


def write(entry):
    serialized = json.dumps(entry)
    # ZADDs signature is (key_name, {data: score})
    api.redis.zadd(f"{entry.key}",
                   {serialized: entry.timestamp})


@api.blueprint.route("/", methods=["GET"])
def core():
    return ("Core-API is present.", 200)


@api.blueprint.route("/<table>/<id>", methods=["POST", "PUT"])
def set_element(table, id):
    payload = request.get_json()
    if not payload:
        return reply.NOT_JSON
    else:
        action = "INSERT" if request.method == "POST" else "UPDATE"
        entry = Map(key=f"{api.config.prefix}.{table}.{id}",
                    action=action,
                    timestamp=timestamp(),
                    payload=payload)
        print(json.dumps(entry))
        write(entry)
    return reply.OK


@api.blueprint.route("/<table>/<id>", methods=["GET"])
def get_element(table, id):
    key = f"{table}.{id}"
    return key
    pass
