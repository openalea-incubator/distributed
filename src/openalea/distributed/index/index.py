import csv
import os.path

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import os


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


class Index(object):

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
        
    def initialize(self):
        cluster = Cluster()
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
        site set<text>,
        path set<text>, 
        PRIMARY KEY (data_id) 
        )"""
        session.execute(cmd)
        
        
    def close(self):
        pass
    
    
    def delete(self):
        cmd = """DROP TABLE data_index;
        """
        self.data_index.execute(cmd)
    
    
    
    def add_data(self, data_id="", path="", site="", dict_item=""):
        # FIRST: FORMAT INPUT:
        if dict_item:
            data_id = dict_item['data_id']
            site = dict_item['site']
            path = dict_item['path']
        # else the variable are already set
        if type(path) is not set:
            path = set([path])

        # IF there is an entry for this data: update
        row = self.data_index.execute("""SELECT path, site FROM data_index 
        WHERE data_id=%s""", [data_id])
        if row:
            query = SimpleStatement("""
            UPDATE data_index 
            SET path = path + %(p)s,
            site = site + %(s)s
            WHERE data_id=%(d_id)s
            """, consistency_level=ConsistencyLevel.ONE)
            self.data_index.execute(query, dict(d_id=data_id, p=path, s=site))
        
        else:
            query = SimpleStatement("""
            INSERT INTO data_index (data_id, path, site)
            VALUES (%(d_id)s, %(p)s, %(s)s)
            """, consistency_level=ConsistencyLevel.ONE)
            self.data_index.execute(query, dict(d_id=data_id, p=path, s=site))
        
        
    # def remove_site(self, data_id="", path="", site=""):
    #     if type(path) is not set:
    #         path = set([path])
            
    #     row = self.data_index.execute("""SELECT path FROM data_index 
    #     WHERE data_id=%s""", [data_id])
    #     if row:
    #         query = SimpleStatement("""
    #         UPDATE data_index 
    #         SET path = path - %(p)s
    #         WHERE data_id=%(d_id)s
    #         """, consistency_level=ConsistencyLevel.ONE)
    #         self.data_index.execute(query, dict(d_id=data_id, p=path))
        
   
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
        if count==0:
            return
        else:
            query = "SELECT * FROM data_index"
            datas = self.data_index.execute(query)
            if datas:
                for data in datas:
                    print "ID: ", data.data_id, " paths: ", list(data.path)
    
    
    def find_one(self, data_id):
        query = """SELECT site, node, path FROM data_index
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

                


def get_site(path):
    # ONLY WORK IF: data paths are /x/x/site/VM/data_id
    return os.path.basename(os.path.dirname(os.path.dirname(path)))

def get_vm(path):
    # ONLY WORK IF: data paths are /x/x/site/VM/data_id
    return os.path.basename(os.path.dirname(path))

def get_location_info(data_id):
    return