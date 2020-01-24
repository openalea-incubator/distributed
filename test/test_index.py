from openalea.distributed.cache.index import *
from openalea.distributed.cache.cache_file import create_dir
from irods.session import iRODSSession


# root_cache_path = "/home/gaetan/OpenAlea/distributed/cache_data/test/"
# cache_path = root_cache_path + "exp" + "1" +"/"
# data_path = '/home/gaetan/OpenAlea/distributed/cache_data/test/exp1/data/data_test'
# id_task = '15'
#
# create_dir(cache_path)
# index = IndexFile(root_cache_path, "1")
# index.initiate(overwrite=True)
#
# index.update(id_task, data_path)
#
# result_path = index.search(id_task)
# print result_path

# irods_sess = iRODSSession(host='inragrid-mtp.supagro.inra.fr',
#                                   port=1247,
#                                   user='gheidsieck',
#                                   password='ghe2018#',
#                                   zone='INRAgrid')
#
# irods_root_cache_path = "/INRAgrid/home/gheidsieck/cache/"
# index_path = irods_root_cache_path + "exp11/index.csv"
#
# index = IndexFileIRODS(index_path, irods_sess)
# index.initiate(overwrite=False)
#
# l = [str(25), "FFF", "TTTTT"]
# index.add(l)
#
# index.search("25")

# print irods_sess.data_objects.exists(irods_root_cache_path)
# /INRAgrid/home/gheidsieck/cache/exp11/data/segment_reduction__7ca22ff948ba0c30eeefed0310cc6886