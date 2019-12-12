import os

class CacheFile(object):
    def __init__(self, path, method):
        self.path = path
        self.method = method
        self.data_path = os.path.join(path, "data")

    def set_path(self, path):
        self.path = path
        self.data_path = os.path.join(path, "data")

