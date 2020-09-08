#!/usr/bin/env python3
import json
from redis import Redis
from flask import Flask
from flask import request
from datetime import datetime
from pkmap import Map

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


def validate(table, id, request, ignoreJson=False):
    try:
        assert table
        assert id
        if not ignoreJson:
            assert request.json
            payload = Map(request.json)
        else:
            payload = None
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

    qname = f"{data.table}.{data.id}"
    app.r.rpush(
        qname,
        json.dumps({
            "action": action,
            "timestamp": datetime.now().strftime("%s"),
            "params": data.payload
        }))
    return ("OK", 200)


def get_object(table, id, full_history=True):
    try:
        data = validate(table, id, request, ignoreJson=True)
    except Exception:
        return ("Does not seem to be valid JSON in the correct format.", 500)

    qname = f"{data.table}.{data.id}"
    payload = app.r.lrange(qname, 0, -1)
    history = [Map(json.loads(x.decode("utf-8"))) for x in payload]
    reconstruction = reconstruct(history)
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
    return get_object(table, id, full_history=False)


if __name__ == "__main__":
    app.run(debug=True)
