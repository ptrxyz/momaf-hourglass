class Map(dict):
    """
    Example:
    m = Map({'some': 'value'}, another_key='more value', number=1, arr=['lorem', 'ipsum'])
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    self[k] = v

        if kwargs:
            for k, v in kwargs.items():
                self[k] = v

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        if type(value) is dict:
            value = Map(value)
        if type(value) is list:
            value = [Map(x) for x in value]
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if type(value) is dict:
            value = Map(value)
        if type(value) is list:
            value = [Map(x) for x in value]
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super().__delitem__(key)
        del self.__dict__[key]
