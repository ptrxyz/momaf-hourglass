#!/usr/bin/env python3

import requests
import json
from redis import Redis
from pkmap import Map


def req(verb, table, id, payload={}):
    headers = {'content-type': 'application/json'}
    method = {
        "history": "GET",
        "get": "GET",
        "update": "POST",
        "delete": "POST",
        "insert": "POST",
    }
    r = requests.request(method=method[verb],
                         url=f"http://127.0.0.1:5000/{verb}/{table}/{id}",
                         headers=headers,
                         data=json.dumps(payload))
    print(f"({r.status_code}) {r.text}")
    return r.status_code, r.text


r = Map()

r.table = "table"
r.id = 1
r.key = f"{r.table}.{r.id}"
r.raw = Redis(host='localhost', port=6379, db=0)
r.raw.delete(r.key)

assert req("insert", r.table, r.id, {"a": 1, "b": 2})[0] == 200
raw = r.raw.lrange(r.key, 0, -1)
assert len(raw) == 1
j = [Map(json.loads(x.decode("utf-8"))) for x in raw]
assert j[-1].params.a == 1 and j[-1].params.b == 2

assert req("update", r.table, r.id, {"a": 3, "b": 5})[0] == 200
raw = r.raw.lrange(r.key, 0, -1)
assert len(raw) == 2
j = [Map(json.loads(x.decode("utf-8"))) for x in raw]
assert j[-1].params.a == 3 and j[-1].params.b == 5

assert req("update", r.table, r.id, {"c": 9, "b": 5})[0] == 200
raw = r.raw.lrange(r.key, 0, -1)
assert len(raw) == 3
j = [Map(json.loads(x.decode("utf-8"))) for x in raw]
assert j[-1].params.b == 5 and j[-1].params.c == 9

rc, rt = req("get", r.table, r.id)
assert rc == 200
rj = Map(json.loads(rt))
assert rj.data.a == 3 and rj.data.b == 5 and rj.data.c == 9 and \
     rj.table == r.table and rj.id == r.id

rc, rt = req("history", r.table, r.id)
assert rc == 200
rj = Map(json.loads(rt))
assert rj.data.a == 3 and rj.data.b == 5 and rj.data.c == 9 and \
         rj.table == r.table and rj.id == r.id and len(rj.history) == 3 and \
         rj.history[0].action == "insert" and \
         rj.history[0].params.a == 1 and rj.history[0].params.b == 2 and \
         rj.history[1].action == "update" and \
         rj.history[1].params.a == 3 and rj.history[1].params.b == 5 and \
         rj.history[2].action == "update" and \
         rj.history[2].params.c == 9 and rj.history[2].params.b == 5

assert req("delete", r.table, r.id)[0] == 200
raw = r.raw.lrange(r.key, 0, -1)
assert len(raw) == 4

rc, rt = req("history", r.table, r.id)
assert rc == 200
rj = Map(json.loads(rt))
assert len(rj.data.items()) == 0 and \
       rj.table == r.table and rj.id == r.id and len(rj.history) == 4 and \
       rj.history[0].action == "insert" and \
       rj.history[0].params.a == 1 and rj.history[0].params.b == 2 and \
       rj.history[1].action == "update" and \
       rj.history[1].params.a == 3 and rj.history[1].params.b == 5 and \
       rj.history[2].action == "update" and \
       rj.history[2].params.c == 9 and rj.history[2].params.b == 5 and \
       rj.history[3].action == "delete"

assert req("insert", r.table, r.id, {"a": 10, "b": 12})[0] == 200
raw = r.raw.lrange(r.key, 0, -1)
assert len(raw) == 5
j = [Map(json.loads(x.decode("utf-8"))) for x in raw]
assert j[-1].params.a == 10 and j[-1].params.b == 12

assert req("update", r.table, r.id, {"a": 13, "b": 15})[0] == 200
raw = r.raw.lrange(r.key, 0, -1)
assert len(raw) == 6
j = [Map(json.loads(x.decode("utf-8"))) for x in raw]
assert j[-1].params.a == 13 and j[-1].params.b == 15
