from irods.session import iRODSSession
from openalea.distributed.cache.timer import Timer
from openalea.distributed.index.index import IndexMongoDB
from openalea.distributed.cache.cacheFile import CacheFile
from copy import copy
from pymongo import MongoClient


class Env_var(object):
    def __init__(self, id=None, root_path=None, irods_sess=None, timer=None, index=None, cache=None, algo=None):
        self.id = id
        self.root_path = root_path
        self.irods_sess = irods_sess
        self.timer = timer
        self.index = index
        self.cache = cache
        self.algo = algo


    def __set__(self, instance, val):
        self.instance = val

    def __copy__(self):
        return Env_var(self.id, self.root_path, self.irods_sess, self.timer, self.index, self.cache, self.algo)



def env_1_initiate(env, cache_path=None, cache_method="IRODS"):

    index = IndexMongoDB(method=cache_method, client=None)
    cache = CacheFile(path=cache_path, method=cache_method)

    env.root_path = cache_path
    env.index = index
    env.cache = cache


def env_1_update(env):

    irods_sess = iRODSSession(host='inragrid-mtp.supagro.inra.fr',
                              port=1247,
                              user='gheidsieck',
                              password='ghe2018#',
                              zone='INRAgrid')

    client = MongoClient('localhost', 27017)

    env.index.irods_sess = irods_sess
    env.index.client = client
    env.irods_sess = irods_sess


def set_percent_reuse(list_reuse_algo, env):
    new_env = copy(env)
    new_env.algo = list_reuse_algo
    return new_env
