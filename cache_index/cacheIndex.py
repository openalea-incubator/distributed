from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import os


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
    
    
    
    def add_data(self, data_id="", path="", dict_item=""):
        # FIRST: FORMAT INPUT:
        if dict_item:
            data_id = dict_item['data_id']
            path = dict_item['path']
        # else the variable are already set
        if type(path) is not set:
            path = set([path])

        # IF there is an entry for this data: update
        row = self.data_index.execute("""SELECT path FROM data_index 
        WHERE data_id=%s""", [data_id])
        if row:
            query = SimpleStatement("""
            UPDATE data_index 
            SET path = path + %(p)s
            WHERE data_id=%(d_id)s
            """, consistency_level=ConsistencyLevel.ONE)
            self.data_index.execute(query, dict(d_id=data_id, p=path))
        
        else:
            query = SimpleStatement("""
            INSERT INTO data_index (data_id, path)
            VALUES (%(d_id)s, %(p)s)
            """, consistency_level=ConsistencyLevel.ONE)
            self.data_index.execute(query, dict(d_id=data_id, p=path))
        
        
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
