import csv
import os.path
import os

from cassandra.cluster import Cluster
# from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

from sshtunnel import SSHTunnelForwarder
from pymongo.errors import ConnectionFailure
from sshtunnel import BaseSSHTunnelForwarderError


# class IndexFileIRODS(object):
#
#     #TODO change the path of the index so it is not dependent of the exp
#     def __init__(self, path, irods_sess, algo=None):
#         self.path = path
#         self.gid = None
#         self.irods_sess = irods_sess
#         self.algo = algo
#
#     def exists(self):
#         return self.irods_sess.data_objects.exists(self.path)
#
#     def initiate(self, overwrite=False):
#         if overwrite or not self.exists():
#             self.create()
#
#     def create(self):
#         try:
#             if not self.irods_sess.data_objects.exists(self.path):
#                 self.irods_sess.data_objects.create(self.path)
#             obj = self.irods_sess.data_objects.get(self.path)
#             with obj.open('w') as index_file:
#                 index_file.seek(0, 0)
#                 index_file.write('task,result_data_path,irods_path')
#
#         except EnvironmentError, e:  # parent of IOError, OSError *and* WindowsError where available
#             print e
#
#     def add(self, dic):
#         """
#         add an item in the index
#         :param dic: [NOT TRUE ANYMORE] a dict of {'task' : 'result_data_path'}
#                 //// dic is a list of [id_task, data_path, irods_data_path]
#         :return:
#         """
#         obj = self.irods_sess.data_objects.get(self.path)
#         try:
#             with obj.open('a') as index_file:
#                 index_file.seek(0,2)
#                 index_file.write("\n"+dic[0]+','+dic[1]+','+dic[2])
#
#         except EnvironmentError, e:  # parent of IOError, OSError *and* WindowsError where available
#             print e
#
#     #ONLY GET THE first match in the index then return
#     def search(self, id_task):
#         """
#         Search and return the result path of a task in the index.
#         :param id_task: an id generated from the func and the data used
#         :return: both local and irods paths
#         """
#         try:
#             # with open(self.path, "r") as index_file:
#             #     reader = csv.DictReader(index_file)
#             #     for row in reader:
#             #         if str(row['task']) == str(id_task):
#             #             return row['result_data_path'], row['irods_path']
#
#             obj = self.irods_sess.data_objects.get(self.path)
#             with obj.open('r') as index_file:
#                 reader = csv.DictReader(index_file)
#                 for row in reader:
#                     if str(row['task']) == str(id_task):
#                         return row['result_data_path'], row['irods_path']
#
#         except EnvironmentError, e:  # parent of IOError, OSError *and* WindowsError where available
#             print e
#         return None, None
#
#     def read(self):
#         # print the index // for debug
#         try:
#             obj = self.irods_sess.data_objects.get(self.path)
#             with obj.open('r') as f:
#                 for line in f:
#                     print(line)
#
#         except EnvironmentError, e:  # parent of IOError, OSError *and* WindowsError where available
#             print e
#
#     def update(self, id_task, data_path, irods_data_path):
#         l = [str(id_task), data_path, irods_data_path]
#         self.add(l)


class IndexFile(object):

    def __init__(self, irods_sess=None, algo=None, method=None):
        self.irods_sess = irods_sess
        self.algo = algo
        self.method = method

    def is_in(self, path_to_data):
        if self.method == "IRODS":
            return self.irods_sess.data_objects.exists(path_to_data)
        elif (self.method == "cluster") or (self.method == "local"):
            return os.path.isfile(path_to_data)

    def __set__(self, instance, value):
        self.instance = value


class IndexMongoDB(object):

    def __init__(self, irods_sess=None, method=None, client=None):
        self.irods_sess = irods_sess
        self.client = client
        self.method = method

    def __set__(self, instance, value):
        self.instance = value

    def is_in(self, task_id):
        # check if in mongodb
        db = self.client.indexdb
        index = db.index_collection
        index.find_one({"task_id": task_id})
        if index.find_one({"task_id": task_id}):
            return index.find_one({"task_id": task_id})["path"]
        else:
            return False

    def update(self, data_cached_path, task_id):
        # add element to index db
        db = self.client.indexdb
        index = db.index_collection
        index_1 = {"path": data_cached_path, "task_id": task_id}
        index.insert_one(index_1)

    def show(self):
        db = self.client.indexdb
        index = db.index_collection
        print("the index has : ", index.count(), " entries :")
        for doc in index.find({}):
            print(doc)

class IndexCassandra():
    def __init__(self):
        self.data_index = None
        self.server = None
        self.remote = None
        
    def start_client(self, *args, **kwargs):
        # Tunnel a port with ssh toward the cassandra server
        if self.remote:
            if not self.server:
                print "SSH Server not started - cannot connect to Cassandra"
                return
            cassandra_port = self.server.local_bind_port
        else:
            cassandra_port = kwargs.get('cassandra_port')

        try:
            cluster = Cluster(['127.0.0.1'], port=cassandra_port)
            session = cluster.connect()
            self.data_index = session
            KEYSPACE = "wf_metadata"
            # create keyspace and table if it doesnt exist already
            cmd = """CREATE  KEYSPACE IF NOT EXISTS wf_metadata 
            WITH REPLICATION = { 
            'class' : 'SimpleStrategy',
            'replication_factor' : 1 
            }"""
            self.data_index.execute(cmd)
            self.data_index.set_keyspace(KEYSPACE)

            # TODO: MAKE THE PATH A LIST OF THE PATHS WHERE THE DATA EXISTS
            cmd = """CREATE TABLE IF NOT EXISTS data_index ( 
            data_id text, 
            path set<text>, 
            execution_data boolean,
            cache_data boolean,
            PRIMARY KEY (data_id) 
            )"""
            self.data_index.execute(cmd)
        except ConnectionFailure:
            print "failed to connect to Cassandra"

    def close(self):
        pass

    def delete(self):
        cmd = """DROP TABLE IF EXISTS data_index;
        """
        self.data_index.execute(cmd)

    def start_sshtunnel(self, *args, **kwargs):
        try:
            self.server = SSHTunnelForwarder(
                ssh_address_or_host=kwargs.get('ssh_ip_addr'),
                ssh_pkey=kwargs.get('ssh_pkey'),
                ssh_username=kwargs.get('ssh_username'),
                remote_bind_address=kwargs.get('remote_bind_address')
                # ,
                # *args,
                # **kwargs
            )

            self.server.start()
        except BaseSSHTunnelForwarderError:
            print "Fail to connect to ssh device"

    def close_sshtunel(self):
        return self.server.stop()

    def init(self, *args, **kwargs):
        path_config =  kwargs.get('index_config', None)
        if path_config == None:
            import openalea.distributed.index.index_config as index_config
        else:
            index_config = imp.load_source('index_config', path_config)
            import index_config
        print("Load index config from : ", index_config.__file__)
        if index_config.REMOTE_INDEX:
            self.remote=True
            self.start_sshtunnel(ssh_ip_addr=index_config.CASSANDRA_SSH_IP,
                                ssh_pkey=index_config.SSH_PKEY,
                                ssh_username=index_config.SSH_USERNAME,
                                remote_bind_address=("localhost", index_config.CASSANDRA_PORT) , 
                                *args, **kwargs)
        else: 
            self.remote=False
        self.start_client(cassndra_ip_addr=index_config.CASSANDRA_ADDR,
                          cassandra_port=index_config.CASSANDRA_PORT,
                          *args, **kwargs)

    def add_data(self, data_id="", path="", exec_data=False, cache_data=False, dict_item=""):
        # FIRST: FORMAT INPUT:
        if dict_item:
            data_id = dict_item['data_id']
            path = dict_item['path']
            exec_data = dict_item.get('exec_data', False)
            cache_data =  dict_item.get('cache_data', False)
        # else the variable are already set
        if type(path) is not set:
            path = set([path])

        # IF there is an entry for this data: update
        row = self.data_index.execute("""SELECT path FROM data_index 
        WHERE data_id=%s""", [data_id])
        if row:
            query = SimpleStatement("""
            UPDATE data_index 
            SET path = path + %(p)s,
            execution_data = %(ed)s,
            cache_data = %(cd)s
            WHERE data_id=%(d_id)s
            """, consistency_level=ConsistencyLevel.ONE)
            self.data_index.execute(query, dict(d_id=data_id, p=path, ed=exec_data, cd=cache_data))

        else:
            query = SimpleStatement("""
            INSERT INTO data_index (data_id, path, execution_data, cache_data)
            VALUES (%(d_id)s, %(p)s, %(ed)s, %(cd)s)
            """, consistency_level=ConsistencyLevel.ONE)
            self.data_index.execute(query, dict(d_id=data_id, p=path, ed=exec_data, cd=cache_data))

    def remove_site(self, data_id="", path=""):
        if type(path) is not set:
            path = set([path])

        row = self.data_index.execute("""SELECT path FROM data_index 
        WHERE data_id=%s""", [data_id])
        if row:
            query = SimpleStatement("""
            UPDATE data_index 
            SET path = path - %(p)s
            WHERE data_id=%(d_id)s
            """, consistency_level=ConsistencyLevel.ONE)
            self.data_index.execute(query, dict(d_id=data_id, p=path))

    def remove_one_data(self, data_id="", site="", node=""):
        # TODO: NOT DELETE => REMOVE ONE OF THE PATHS IF SEVERAL EXISTS
        query = SimpleStatement("""
        DELETE FROM data_index 
        WHERE data_id=%s
        """)
        self.data_index.execute(query, [data_id])

    def remove_all_data(self):
        query = SimpleStatement("""
        TRUNCATE data_index
        """)
        self.data_index.execute(query)

    def show_all(self):
        count = self.data_index.execute("select count(*) from data_index")[0].count
        print "The index has: ", count, " entries."
        if count == 0:
            return
        else:
            query = "SELECT * FROM data_index"
            datas = self.data_index.execute(query)
            if datas:
                for data in datas:
                    print "ID: ", data.data_id, " paths: ", list(data.path), \
                        " execution data: ", data.execution_data, " cache data: ", data.cache_data 

    def find_one(self, data_id):
        query = """SELECT path FROM data_index
        WHERE data_id=%s
        """
        return self.data_index.execute(query, [data_id])

    def empty(self):
        query = """SELECT * FROM data_index
        """
        row = self.data_index.execute(query)
        if row:
            return False
        else:
            return True

    def all_files_id(self):
        query = """SELECT data_id FROM data_index
        """
        return self.data_index.execute(query)   

    def is_in(self, data_id):
        query = """SELECT path FROM data_index
        WHERE data_id=%s
        """
        r = self.data_index.execute(query, [data_id])
        for i in r:
            if i:
                return i.path[0]
            else:
                return False



def get_site(path):
    # ONLY WORK IF: data paths are /x/x/site/VM/data_id
    return os.path.basename(os.path.dirname(os.path.dirname(path)))

def get_vm(path):
    # ONLY WORK IF: data paths are /x/x/site/VM/data_id
    return os.path.basename(os.path.dirname(path))

def start_index(index_config=None, index_type="Cassandra"):
    if index_type == "Files":
        index = IndexFile()
        index.init(index_config)
        return index
    if index_type == "MongoDB":
        index = IndexMongoDB()
        index.init(index_config)
        return index
    if index_type == "Cassandra":
        index = IndexCassandra()
        index.init(index_config)
        return index
        