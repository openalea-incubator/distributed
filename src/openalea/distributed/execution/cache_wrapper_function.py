# import time
# import os
# from openalea.distributed.index.id import get_id_pname
# from openalea.distributed.cache.cache_file import load_intermediate_data, write_intermediate_data
# from openalea.distributed.execution.data import Data
# from openalea.distributed.index.id import set_id, check_task_id
# import hashlib


# def execute(func, *args, **kwargs):
#     values = []
#     for data in args:
#         if isinstance(data, Data):
#             values.append(data.value)
#         else:
#             values.append(data)
#     new_val = func(*values, **kwargs)
#     new_parents = []
#     for parent in args:
#         if isinstance(parent, Data):
#             new_parents.append(Data(id=parent.id, parents=parent.parents, func_name=parent.func_name))
#         else:
#             new_parents.append(parent)
#     new_data = Data(id=None, value=new_val, parents=new_parents, func_name=str(func.__name__))
#     set_id(new_data)
#     return new_data


# def launch_cache_greedy(env_1, func, *args, **kwargs):
#     """
#     Wrap a function to use caching methods
#     :param env_1: shared parameters
#     :param func: the function to wrap
#     :param args: arg of the function
#     :param kwargs: kwargs of the function
#     :return:
#     """
#     start = time.time()
#     # retreive intermediate data if it exist and execute the act otherwise
#     #     id_task = get_id_pname(env_1.id, func)
#     id_task = check_task_id(func, *args, **kwargs)

#     dname = hashlib.md5(id_task).hexdigest()
#     cache_data_path = os.path.join(env_1.cache.data_path, dname)

#     # if the index has address to some data -> fetch it
#     # check if the id_task is in the index and get the path if yes
#     intermediate_data_path = env_1.index.is_in(id_task)
#     if intermediate_data_path:
#         # GIVE REF! Not data
#         # Create object with the ref/ dont download now
#         #         tmp_data = load_intermediate_data(dname, cache_data_path, method=env_1.cache.method,irods_sess=env_1.irods_sess)
#         print("LOAD DATA")
#         tmp_data = Data(id=id_task, value=None)

#     # otherwise compute and store the result
#     else:
#         # download data from refs:
#         for data in args:
#             if isinstance(data, Data):
#                 if not data.value:
#                     try:
#                         path_to_data = env_1.index.is_in(data.id)
#                         tmp_data = load_intermediate_data(dname=None, data_path=path_to_data, method=env_1.cache.method,
#                                                           irods_sess=env_1.irods_sess)
#                         data.value = tmp_data.value
#                     except:
#                         pass
#         # Compute
#         print("COMPUTE DATA")
#         tmp_data = execute(func, *args, **kwargs)
#         # update cache
#         write_intermediate_data(tmp_data, dname, cache_data_path, method=env_1.cache.method,
#                                 irods_sess=env_1.irods_sess)
#         # update index
#         env_1.index.update(data_cached_path=cache_data_path, task_id=id_task)

#         tmp_data.size = os.path.getsize(cache_data_path)
#         tmp_data.time = time.time() - start
#     return tmp_data


# def launch_classic(env_1, func, *args, **kwargs):
#     """
#     execute the without cache
#     :param env_1: shared parameters
#     :param func: the function to wrap
#     :param args: arg of the function
#     :param kwargs: kwargs of the function
#     :return:
#     """
#     start = time.time()

#     tmp_data = execute(func, *args, **kwargs)

#     tmp_data.size = 0
#     tmp_data.time = time.time() - start

#     return tmp_data


# def launch_cache_force_exec(env_1, func, *args, **kwargs):
#     """
#     Force execution and add to the cache
#     :param env_1: shared parameters
#     :param func: the function to wrap
#     :param args: arg of the function
#     :param kwargs: kwargs of the function
#     :return:
#     """
#     start = time.time()
#     # retreive intermediate data if it exist and execute the act otherwise
#     #     id_task = get_id_pname(env_1.id, func)
#     id_task = check_task_id(func, *args, **kwargs)

#     dname = hashlib.md5(id_task).hexdigest()
#     cache_data_path = os.path.join(env_1.cache.data_path, dname)

#     # download data from references (async computation of the dependencies):
#     for data in args:
#         if isinstance(data, Data):
#             if not data.value:
#                 try:
#                     # get the path of the dependencies
#                     # path_to_data = os.path.join(env_1.cache.data_path, data.id)
#                     path_to_data = env_1.index.is_in(data.id)
#                     # load them
#                     tmp_data = load_intermediate_data(dname=None, data_path=path_to_data, method=env_1.cache.method,
#                                                       irods_sess=env_1.irods_sess)
#                     data.value = tmp_data.value
#                 except:
#                     pass
#     # Compute
#     tmp_data = execute(func, *args, **kwargs)

#     # update cache
#     write_intermediate_data(tmp_data, dname, cache_data_path, method=env_1.cache.method,
#                             irods_sess=env_1.irods_sess)
#     # update index
#     env_1.index.update(data_cached_path=cache_data_path, task_id=id_task)

#     tmp_data.size = os.path.getsize(cache_data_path)
#     tmp_data.time = time.time() - start
#     return tmp_data


# def launch_fake_eval(env_1, func, *args, **kwargs):

#     return 1


# def launch(env_1, func, *args, **kwargs):
#     """
#     Decide what function to run according to the algo
#     :param env_1: shared parameters
#     """
#     index_algo = env_1.algo
#     print("start " + func.__name__)
#     # TODO: select a default algorithm
#     if not index_algo:
#         return None
#     elif index_algo == "classic":
#         return launch_classic(env_1, func, *args, **kwargs)
#     elif index_algo == "reuse":
#         return launch_cache_greedy(env_1, func, *args, **kwargs)
#     elif index_algo == "force_rerun":
#         return launch_cache_force_exec(env_1, func, *args, **kwargs)
#     elif index_algo == "fake_eval":
#         return launch_fake_eval(env_1, func, *args, **kwargs)

