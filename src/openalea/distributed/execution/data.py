class Data(object):
    def __init__(self, id=None, value=None, parents=None, func_name=None, size=None, time=None):
        self.id = id
        self.value = value
        self.parents = parents
        self.func_name = func_name
        self.size = size
        self.time = time

    def __set__(self, instance, value):
        self.instance = value


