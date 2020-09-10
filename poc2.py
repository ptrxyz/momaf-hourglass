#!/usr/bin/env python3
import json
from redis import Redis
from flask import Flask
from flask import request
from datetime import datetime
from pkmap import Map

import re

app = Flask(__name__)
app.r = Redis(host='localhost', port=6379, db=0)


def pprint(j):
    if isinstance(j, dict):
        print(json.dumps(j, indent=4))
    else:
        print(j, type(j))
        assert False
    return j


@app.route('/')
def default():
    return ("All good.", 200)


pattern_left = re.compile(r"^(?P<this>\w+)$")
pattern_right = re.compile(
    r"^(?P<that>\w+\.\d+)\.(?P<field>\w+)($|(\@(?P<at>(latest|ref|\d+)$)|$))", re.MULTILINE)


class SyntaxError(Exception):
    def __init__(self, msg=""):
        self.msg = msg
        super().__init__(self.msg)


def create_join_table(d):
    """Creates a join table for dict d.
    returns (False, Error Msg) in case of trouble and
    a dict with replacement instructions in case of success. Example:
    {
        "replace.this": ("with.this", "latest"),
        "replace.this_too": ("with.that", "238476568"),
        "replace.me": ("with.them", "this"),
    }
    """
    res = {}
    for k, v in d.items():
        m1 = pattern_left.match(k)
        m2 = pattern_right.match(v)
        if m1 and m2:
            res[k] = (m2.group("that"), m2.group("at"))
        else:
            blame = f"Key Error: {k}" if not m1 else f"Value Error: {v}"
            raise Exception(blame)
    return res


def validate(table, id, request, ignoreJson=False):
    try:
        assert table
        assert id
        if not ignoreJson:
            assert request.json
            payload = Map(request.json)
        else:
            payload = {}
    except Exception:
        raise Exception
    return Map({"id": id, "table": table, "payload": payload})


def reconstruct(history):
    obj = {}
    if len(history) > 0:
        for i in history:
            if i.action == "delete" or i.action == "insert":
                obj = {}
            if not i.params:
                i.params = {}
            obj = {**obj, **(i.params)}
            i.current = obj
    return obj


def log_action(table, id, request, action):
    try:
        data = validate(table, id, request, ignoreJson=(action == "delete"))
    except Exception:
        return ("Does not seem to be valid JSON in the correct format.", 500)

    if (action == "update" or action == "insert") and len(data.payload) < 1:
        # This case should be covered by the json parser anyway.
        # but ... just to be sure ...
        return ("Empty payload.", 400)

    qname = f"{data.table}.{data.id}"
    app.r.rpush(
        qname,
        json.dumps({
            "action": action,
            "timestamp": datetime.now().strftime("%s"),
            "params": data.payload
        }))
    return ("OK", 200)


def get_object(table, id, full_history=True, join=None):
    try:
        data = validate(table, id, request, ignoreJson=True)
    except Exception:
        return ("Does not seem to be valid JSON in the correct format.", 500)

    qname = f"{data.table}.{data.id}"
    payload = app.r.lrange(qname, 0, -1)
    history = [Map(json.loads(x.decode("utf-8"))) for x in payload]
    reconstruction = reconstruct(history)

    if join:
        rr = Map(reconstruction)
        replacements = set(join.keys()) & set(rr.keys())
        print(replacements)
        for k in replacements:
            print(k, join[k])
            print(f"{k} is to be replace by {join[k]}")
            __p = app.r.lrange(join[k][0], 0, -1)
            __h = [Map(json.loads(x.decode("utf-8"))) for x in __p]
            right_obj = reconstruct(__h)
            print("reconstruction: ", right_obj)
            right_obj["__refering_to"] = join[k][0]
            reconstruction[k] = right_obj
        pass

    pprint(reconstruction)
    if full_history:
        return (json.dumps({
            "history": history,
            "table": table,
            "id": id,
            "data": reconstruction
        }), 200)
    else:
        return (json.dumps({
            "table": table,
            "id": id,
            "data": reconstruction
        }), 200)


@ app.route('/delete/<table>/<int:id>', methods=["POST"])
def delete(table, id):
    return log_action(table, id, request, action="delete")


@ app.route('/update/<table>/<int:id>', methods=["POST"])
def update(table, id):
    return log_action(table, id, request, action="update")


@ app.route('/insert/<table>/<int:id>', methods=["POST"])
def insert(table, id):
    return log_action(table, id, request, action="insert")


@ app.route('/history/<table>/<int:id>', methods=["GET"])
def history(table, id):
    return get_object(table, id, full_history=True)


@ app.route('/get/<table>/<int:id>', methods=["GET"])
def get(table, id):
    # ?join={"table.id.field":"table.id.field@latest", "table.id.field":"table.id.field@reference"}
    join = request.args.get("join")
    print(join)
    if join:
        try:
            join = json.loads(join)
            join = create_join_table(join)
            print(join)
        except Exception as ex:
            if type(ex) == json.decoder.JSONDecodeError:
                return (f"Can not parse json: {ex}", 400)
            elif type(ex) == SyntaxError:
                return (f"Syntax Error: {ex}", 400)
            else:
                return (f"General Error: {ex}", 400)

    return get_object(table, id, full_history=False, join=join)


if __name__ == "__main__":
    app.run(debug=True)
