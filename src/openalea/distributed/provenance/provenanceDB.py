# -*- coding: utf-8 -*-
from sshtunnel import SSHTunnelForwarder
import pymongo
from pymongo.errors import ConnectionFailure
from sshtunnel import BaseSSHTunnelForwarderError
import json
import imp
import os
from openalea.core.path import path
from openalea.core import settings


class ProvFile():
    def __init__(self, *args, **kwargs):
        self.localpath = ""

    def add_task_item(self, item, *args, **kwargs):
        try:
            prov_path = os.path.join(self.localpath, str(item["task_id"]) + '.json')
            with open(prov_path, 'a+') as fp:
                json.dump(item, fp, indent=4)
        except:
            prov_path = path(settings.get_openalea_home_dir()) / 'provenance' / 'task_provenance.json'
            print('Fail to open prov files. - Now write prov in : ', prov_path)
            with open(prov_path, 'a+') as fp:
                json.dump(item, fp, indent=4)

    def add_wf_item(self, item, *args, **kwargs):
        try:
            prov_path = os.path.join(self.localpath, str(item["workflow"]) + '.json')
            with open(prov_path, 'a+') as fp:
                json.dump(item, fp, indent=4)
        except:
            prov_path = path(settings.get_openalea_home_dir()) / 'provenance' / 'wf_provenance.json'
            print('Fail to open prov files. - Now write prov in : ', prov_path)
            with open(prov_path, 'a+') as fp:
                json.dump(item, fp, indent=4)
    
    def is_in(self, task_id=None, wf_id=None):
        pass

    def show(self):
        pass

    def remove_all_item(self):
        pass

    def init(self, *args, **kwargs):
        path_config =  kwargs.get('provenance_config', None)
        if path_config == None:
            import openalea.distributed.provenance.provenance_config as prov_config
        else:
            prov_config = imp.load_source('prov_config', path_config)
            import prov_config
        print("Load provenance config from : ", prov_config.__file__)
        self.localpath = prov_config.PROVENANCE_PATH

    def close(self):
        pass


class ProvMongo():
    def __init__(self, *args, **kwargs):
        self.client = None
        self.server = None
        # self.provdb = None
        self.taskdb = None
        self.wfdb = None
        self.remote = False

    def __set__(self, instance, value):
        self.instance = value

    def is_in(self, task_id=None, wf_id=None):
        # check if in mongodb
        if task_id:
            if self.taskdb.find_one({"task_id": task_id}):
                return self.taskdb.find_one({"task_id": task_id})
            else:
                return False
        if wf_id:
            if self.wfdb.find_one({"workflow": wf_id}):
                return self.wfdb.find_one({"workflow": wf_id})
            else:
                return False
        else:
            return False

    def add_task_item(self, item, *args, **kwargs):
        # add element to index db
        self.taskdb.insert_one(item)

    def add_wf_item(self, item, *args, **kwargs):
        # add element to index db
        self.wfdb.insert_one(item)

    def show(self, task_id=None, wf_id=None):
        print("the task provenance has : ", self.taskdb.count(), " entries :")
        for doc in self.taskdb.find({}):
            print(doc)

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

    def start_client(self, *args, **kwargs):
        if self.remote:
            if not self.server:
                print "SSH Server not started - cannot connect to Mongo"
                return
            try:
                client = pymongo.MongoClient(host=kwargs.get('mongo_ip_addr'),
                                             port=self.server.local_bind_port
                                             # ,
                                             # , # server.local_bind_port is assigned local port
                                             # username='admin',
                                             # password='admin'
                                             # *args,
                                             # **kwargs
                                             )

                self.client = client
                db = self.client.provdb
                self.taskdb = db.task_collection
                self.wfdb = db.workflow_collection
            except ConnectionFailure:
                print "failed to connect to mongodb"
        else:
            try:
                client = pymongo.MongoClient(host=kwargs.get('mongo_ip_addr'),
                                             port=kwargs.get('mongo_port')
                                             # ,
                                             # , # server.local_bind_port is assigned local port
                                             # username='admin',
                                             # password='admin'
                                             # *args,
                                             # **kwargs
                                             )

                self.client = client
                db = self.client.provdb
                self.taskdb = db.task_collection
                self.wfdb = db.workflow_collection
            except ConnectionFailure:
                print "failed to connect to mongodb"

    def close_sshtunel(self):
        return self.server.stop()

    def close_client(self):
        return self.client.close()

    def remove_all_item(self):
        self.taskdb.drop()
        self.wfdb.drop()
        return 0

    def init(self, *args, **kwargs):
        path_config =  kwargs.get('provenance_config', None)
        if path_config == None:
            import openalea.distributed.provenance.provenance_config as prov_config
        else:
            prov_config = imp.load_source('prov_config', path_config)
            import prov_config
        print("Load provenance config from : ", prov_config.__file__)
        # self.localpath = prov_config.PROVENANCE_PATH
        if prov_config.REMOTE_PROV:
            self.remote=True
            self.start_sshtunnel(ssh_ip_addr=prov_config.MONGO_SSH_IP,
                                ssh_pkey=prov_config.SSH_PKEY,
                                ssh_username=prov_config.SSH_USERNAME,
                                remote_bind_address=(prov_config.MONGO_ADDR, prov_config.MONGO_PORT) , 
                                *args, **kwargs)
        else: 
            self.remote=False
        self.start_client(mongo_ip_addr=prov_config.MONGO_ADDR,
                          mongo_port=prov_config.MONGO_PORT,
                          *args, **kwargs)




    def close(self):
        if self.remote:
            self.close_client()
            self.close_sshtunel()
        else:
            self.close_client()


def start_provdb(provenance_config=None, provenance_type="Files"):
    if provenance_type == "Files":
        provdb = ProvFile()
        provdb.init(provenance_config)
        return provdb
    if provenance_type == "MongoDB":
        provdb = ProvMongo()
        provdb.init(provenance_config)
        return provdb
    if provenance_type == "Cassandra":
        # provdb = ProvMongo()
        # provdb.init(provenance_config)
        # return provdb
        pass