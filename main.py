from redis import Redis
from flask import Flask
from flask import request

app = None

app = Flask(__name__)
app.r = Redis(host='localhost', port=6379, db=0)

@app.route('/')
def default():
    return ("All good.", 200)

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



    