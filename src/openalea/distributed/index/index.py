import csv
import os.path


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