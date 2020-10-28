from flask import Flask
from redis import Redis
from .pkmap import Map


class Hourglass():
    def __init__(self, config):
        self.flask: Flask = Flask(__name__)
        self.redis: Redis = Redis(host='localhost', port=6379, db=0)
        self.config = config

    def mount(self, api, prefix=""):
        api.flask, api.redis, api.config = self.flask, self.redis, self.config
        api.flask.register_blueprint(api.blueprint, url_prefix=prefix)


def create_app():
    hourglass = Hourglass(Map({
        "prefix": "data"
    }))

    @hourglass.flask.route('/')
    def default():
        return ("All good.", 200)

    from .dev import api as dev_api
    hourglass.mount(dev_api, "/dev")

    from .core import api as core_api
    hourglass.mount(core_api)

    return hourglass
