# import collections
# import hashlib
# from pymongo import MongoClient
# import bson
# import os
# import dill
# import importlib
#
# from openalea.core import alea
# from openalea.phenomenal.image import mean_image
#
# # from openalea.phenomenal_wralea.phenoarch import binarization
#
#
#
# __all__ = ['check_id_frag'
#            ]
#
#
# def check_id_frag(index, input_data, id_activity):
#
#     return 0


# def cache_binarize(fragWF, index, **kwargs):
#     """
#     Wrap the fragment (activity/ node) to check if id is in index and then replace by data
#
#     :param raw_images:
#     :param index:
#     :return:
#     """
#
#     # trouve le id frag
#     id_fragWF = check_id_frag(index, input_data=raw_images, id_activity=2)
#
#     if id_fragWF in index:
#         #return result
#
#     else :
#         #compute normally
#         #save result in cahe
#
#
#     return 0


### Connect to mongo client
# client = MongoClient('localhost', 27017)
# db = client['cache1']
# collection = db['bin1']
#
# cache_path = '/home/gaetan/OpenAlea/distributed/cache_data'
# name_cache_file = 'l.pkl'
# filepath = os.path.join(cache_path, name_cache_file)
#
# l = [1, 2]
# a = 5
#
# if a == 1:
#     with open(filepath, 'wb') as f:
#         dill.dump(l, f)
#
#         frag_id = hashlib.md5(dill.dumps(l))
#         keys = frag_id
#         values = filepath
#         new_el = {keys: values}
#         add_id = collection.insert_one(new_el).inserted_id
#
# if a == 0:
#     with open(filepath, 'rb') as f:
#         l = dill.load(f)
#
# "l hash = ""1a3d8db871effc1bd35cdc735836cb05"
# "func node 2 =""0x7fd9bf9496d0"
# # h = hashlib.md5(dill.dumps(l))
#
#
# # del_id = collection.drop()
#
#
# pm = alea.load_package_manager()
# wf_factory = pm['openalea.phenomenal.demo']['binarize']
#
#
# wf = wf_factory.instantiate()
#
# print wf.node(2).get_factory().get_pkg().name
#
# #load the function from the node 2 in func
# instance_path = wf.node(2).get_factory().get_python_name()
#
# components = instance_path.rsplit('.', 1)
# _tmp = components[-1].replace('_', '.')
# _tmp = components[0]+"."+_tmp
# components = _tmp.rsplit('.', 1)
#
# mod = importlib.import_module(components[0], components[-1])
# func = getattr(mod, components[-1])
#
# print components
# func(1)
