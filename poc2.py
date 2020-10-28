#!/usr/bin/env python3
import json
import traceback
from redis import Redis
from flask import Flask
from flask import request
from datetime import datetime
import time
from pkmap import Map

import re

app = Flask(__name__)
app.r = Redis(host='localhost', port=6379, db=0)


def timestamp():
    return str(time.time())
    return datetime.now().strftime("%s")


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
    r"^(?P<that>\w+\.\d+)\.(?P<field>\w+)($|(\@(?P<at>(latest|ref|\d+)$)|$))",
    re.MULTILINE)
replacement_pattern = re.compile(
    r"^(?P<table>\w+)\.(?P<id>\*)\.(?P<property>(\w+|\*))($|(\@(?P<at>"
    r"(latest|ref|(?P<number>(\d+\.\d+)|(\d+)))$)|$))", re.MULTILINE)


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
        rplcmnt = replacement_pattern.match(str(v))
        if rplcmnt:
            res[k] = Map(table=rplcmnt.group("table"),
                         rowid=rplcmnt.group("id"),
                         property=rplcmnt.group("property"),
                         at=rplcmnt.group("at"))
        else:
            blame = f"Value Error: {v}"
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
    ts = timestamp()
    jsn = json.dumps({
        "action": action,
        "timestamp": ts,
        "params": data.payload
    })
    print(jsn)
    app.r.zadd(qname, {jsn: ts})
    return ("OK", 200)


def get_object(table, id, full_history=True, join=None):
    try:
        data = validate(table, id, request, ignoreJson=True)
    except Exception:
        return ("Does not seem to be valid JSON in the correct format.", 500)

    qname = f"{data.table}.{data.id}"
    payload = app.r.zrange(qname, 0, -1)
    print("Payload:", payload)
    history = [Map(json.loads(x.decode("utf-8"))) for x in payload]
    reconstruction = reconstruct(history)
    print(reconstruction)
    if join:
        rr = Map(reconstruction)
        replacements = set(join.keys()) & set(rr.keys())
        print(replacements)
        for rkey, rjoin in [(x, join[x]) for x in replacements]:
            print(rkey, rjoin)
            target = f"{rjoin.table}.{rr[rkey]}"
            print(f"{rkey} is to be replace by {target}")

            limit = rjoin.at
            if limit == "latest":
                __p = app.r.zrangebyscore(target, "-inf", timestamp())
            elif limit == "ref":
                __p = app.r.zrangebyscore(
                    target, "-inf", history[-1].timestamp)
            else:
                __p = app.r.zrangebyscore(target, "-inf", limit)

            __h = [Map(json.loads(x.decode("utf-8"))) for x in __p]
            robj = reconstruct(__h)
            print("reconstruction: ", robj)
            if rjoin.property == "*":
                reconstruction[rkey] = robj
            elif rjoin.property in robj:
                reconstruction[rkey] = robj[rjoin.property]
            else:
                raise Exception(
                    f"Can not find property {rjoin.property} on target.")
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


@app.route('/delete/<table>/<int:id>', methods=["POST"])
def delete(table, id):
    return log_action(table, id, request, action="delete")


@app.route('/update/<table>/<int:id>', methods=["POST"])
def update(table, id):
    return log_action(table, id, request, action="update")


@app.route('/insert/<table>/<int:id>', methods=["POST"])
def insert(table, id):
    return log_action(table, id, request, action="insert")


@app.route('/history/<table>/<int:id>', methods=["GET"])
def history(table, id):
    return get_object(table, id, full_history=True)


@app.route('/get/<table>/<int:id>', methods=["GET"])
def get(table, id):
    # Does now accept conversion table to replace a property
    # with an object it's refering to. Done via HTTP parameter
    # (as of Sep. 23, <id> has to *. This might change in the future.)
    # ?join={
    #   "property":"table.id.field@latest",
    #   "property":"table.id.field@ref",
    #   "property":"table.id.field@1600873171.819429"
    # }
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
                print(ex)
                print(traceback.format_exc())
                return (f"General Error: {ex}", 400)

    return get_object(table, id, full_history=False, join=join)


if __name__ == "__main__":
    app.run(debug=True)
