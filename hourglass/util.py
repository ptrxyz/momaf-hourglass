from flask import Flask, Blueprint
from redis import Redis
import time


def timestamp():
    return str(time.time())


class reply():
    OK = ("ok", 200)
    NOT_JSON = ("Bad request. Please make sure that you send correct "
                "JSON with proper content type (application/json).", 400)


# we extend the blueprint class to have a property .r to
# get autocompletion to work in some IDEs (looking at you, VSCode)
class API():
    def __init__(self, name):
        self.redis: Redis = None
        self.flask: Flask = None
        self.config: any = None
        self.blueprint: Blueprint = Blueprint(name, __name__)
