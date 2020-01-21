# -*- coding: utf-8 -*-
from sshtunnel import SSHTunnelForwarder
import pymongo
from pymongo.errors import ConnectionFailure
from sshtunnel import BaseSSHTunnelForwarderError

from cassandra.cluster import Cluster
# from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

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
            mongo_port = self.server.local_bind_port
        else:
            mongo_port = kwargs.get('mongo_port')

        try:
            client = pymongo.MongoClient(host=kwargs.get('mongo_ip_addr'),
                                            port=mongo_port
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


class ProvCassandra():
    def __init__(self, *args, **kwargs):
        self.client = None
        self.server = None
        # self.provdb = None
        self.remote = False

    def __set__(self, instance, value):
        self.instance = value

    def is_in(self, task_id=None, wf_id=None):
        # check if in cassandra
        if task_id:
            query = """SELECT task_id FROM task_provenance
            WHERE task_id=%s
            """
            return self.client.execute(query, [task_id])
        if wf_id:
            query = """SELECT workflow FROM wf_provenance
            WHERE workflow=%s
            """
            return self.client.execute(query, [workflow])
        else:
            return False

    def add_task_item(self, item, *args, **kwargs):
        # FIRST: FORMAT INPUT:
        task_id = item['task_id']
        cpu_time = item['cpu_time']
        n_input = item['n_input']
        n_output = item['n_output']
        node = item['node']
        outputs = json.dumps(item['outputs'])
        inputs = json.dumps(item['inputs'])
        # IF there is an entry for this task: update
        # TODO: Update task provenace database when taks is recomputed
        row = self.client.execute("""SELECT task_id FROM task_provenance 
        WHERE task_id=%s""", [task_id])
        if row:
            pass
        #     query = SimpleStatement("""
        #     UPDATE task_provenance 
        #     SET path = path + %(p)s
        #     WHERE data_id=%(d_id)s
        #     """, consistency_level=ConsistencyLevel.ONE)
        #     self.data_index.execute(query, dict(d_id=data_id, p=path))

        else:
            query = SimpleStatement("""
            INSERT INTO task_provenance (task_id, cpu_time, n_input, n_output,
            node, outputs, inputs)
            VALUES (%(t_id)s, %(cpu_t)s, %(n_i)s, %(n_o)s, %(n)s, %(out)s, %(i)s)
            """, consistency_level=ConsistencyLevel.ONE)
            self.client.execute(query, dict(t_id=task_id, cpu_t=cpu_time,
            n_i=n_input, n_o=n_output, n=node, out=outputs, i=inputs))


    def add_wf_item(self, item, *args, **kwargs):
        # FIRST: FORMAT INPUT:
        id = item['id']
        workflow = item['workflow']
        time_init = item['time_init']
        time_end = item['time_end']
        data = json.dumps(item['data'])
        parameters = json.dumps(item['parameters'])
        executions = json.dumps(item['executions'])

        # IF there is an entry for this task: update
        # TODO: Update task provenace database when taks is recomputed
        row = self.client.execute("""SELECT workflow FROM wf_provenance 
        WHERE workflow=%s""", [workflow])
        if row:
            pass
        #     query = SimpleStatement("""
        #     UPDATE task_provenance 
        #     SET path = path + %(p)s
        #     WHERE data_id=%(d_id)s
        #     """, consistency_level=ConsistencyLevel.ONE)
        #     self.data_index.execute(query, dict(d_id=data_id, p=path))

        else:
            query = SimpleStatement("""
            INSERT INTO wf_provenance (id, workflow, time_init, time_end,
            data, parameters, executions)
            VALUES (%(id)s, %(wf)s, %(t_i)s, %(t_e)s, %(d)s, %(p)s, %(e)s)
            """, consistency_level=ConsistencyLevel.ONE)
            self.client.execute(query, dict(id=id, wf=workflow,
            t_i=time_init, t_e=time_end, d=data, p=parameters, e=executions))

    def show(self):
        count = self.client.execute("select count(*) from task_provenance")[0].count
        print "The task provenance has: ", count, " entries."
        if count == 0:
            return
        else:
            query = "SELECT * FROM task_provenance"
            tasks = self.client.execute(query)
            if tasks:
                for task in tasks:
                    print "ID: ", task.task_id, " node: ", str(task.node)

        count = self.client.execute("select count(*) from wf_provenance")[0].count
        print "The workflow provenance has: ", count, " entries."
        if count == 0:
            return
        else:
            query = "SELECT * FROM wf_provenance"
            wfs = self.client.execute(query)
            if wfs:
                for wf in wfs:
                    print "workflow: ", str(wf.workflow), " id : ", str(wf.id)

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
                print "SSH Server not started - cannot connect to Cassandra"
                return
            cassandra_port = self.server.local_bind_port
        else:
            cassandra_port = kwargs.get('cassandra_port')

        try:
            cluster = Cluster(['127.0.0.1'], port=cassandra_port)
            session = cluster.connect()
            self.client = session
            KEYSPACE = "wf_metadata"
            # create keyspace and table if it doesnt exist already
            cmd = """CREATE  KEYSPACE IF NOT EXISTS wf_metadata 
            WITH REPLICATION = { 
            'class' : 'SimpleStrategy',
            'replication_factor' : 1 
            }"""
            self.client.execute(cmd)
            self.client.set_keyspace(KEYSPACE)

            cmd = """CREATE TABLE IF NOT EXISTS task_provenance ( 
            task_id text, 
            cpu_time float,
            n_input float,
            n_output float,
            node float,
            inputs text,
            outputs text,
            PRIMARY KEY (task_id) 
            )"""
            self.client.execute(cmd)

            cmd = """CREATE TABLE IF NOT EXISTS wf_provenance ( 
            id text, 
            workflow text,
            time_init float,
            time_end float,
            data text,
            parameters text,
            executions text,
            PRIMARY KEY (workflow) 
            )"""
            self.client.execute(cmd)

        except ConnectionFailure:
            print "failed to connect to Cassandra"


    def close_sshtunel(self):
        return self.server.stop()

    def close_client(self):
        pass

    def remove_all_item(self):
        query = SimpleStatement("""
        TRUNCATE data_index
        """)
        self.client.execute(query)
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
            self.start_sshtunnel(ssh_ip_addr=prov_config.CASSANDRA_SSH_IP,
                                ssh_pkey=prov_config.SSH_PKEY,
                                ssh_username=prov_config.SSH_USERNAME,
                                remote_bind_address=("localhost", prov_config.CASSANDRA_PORT) , 
                                *args, **kwargs)
        else: 
            self.remote=False
        self.start_client(cassndra_ip_addr=prov_config.CASSANDRA_ADDR,
                          cassandra_port=prov_config.CASSANDRA_PORT,
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
        provdb = ProvCassandra()
        provdb.init(provenance_config)
        return provdb
        