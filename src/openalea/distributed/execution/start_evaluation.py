# import sys
# import time
# import functools
# import os
# import pandas
# import argparse
# import ipyparallel as ipp

# from openalea.distributed.data.images import load_plant_snapshot, get_plants_with_local, get_plants_with_irods, download_missing_images
# from openalea.distributed.data.load_test_sets import load_irods_metadata
# from openalea.distributed.execution.env_var import Env_var, env_1_initiate, env_1_update, set_percent_reuse
# from openalea.distributed.workflows.workflows import workflowA, workflowB, workflowC
# from openalea.distributed.execution.controller import stop_ipy_cluster, start_ipy_cluster
# from openalea.distributed.execution.algo import generate_list_percenreuse

# from openalea.distributed.execution.data import Data

# from openalea.distributed.cache.cache_file import create_dir

# # TODO: remove this
# # PARAMETERS
# LOCAL_CACHE_PATH = "/home/gaetan/OpenAlea/distributed/cache_data/"
# IRODS_CACHE_PATH = "/INRAgrid/home/gheidsieck/cache/"
# CLUSTER_LOCAL_PATH = "/homedir/heidsieck/work/wf_executions/"


# ### start the work
# def start_execution3(plant=None, env_1=None, wf="A"):
#     # get an id from the name of the plant
#     id_p = plant[2][:4] + plant[2][-13:]
#     start = time.time()

#     try:
#         print("start execution : ", plant[2][:4], "at : ", time.clock())

#         # change the ID
#         env_1.id = id_p
#         env_1_update(env_1)
#         # get the local img
#         # TODO : change so it is possible to chose irods or local
#         # TODO: check if local doesnt exist -> dl first
#         if env_1.index.method == "local":
#             download_missing_images(plant, irods_sess=env_1.irods_sess, cache_path=env_1.cache.path)
#         load_time = time.time()
#         plant_img = load_plant_snapshot(plant, irods_sess=env_1.irods_sess, method=env_1.index.method)
#         data_plant = Data(id=id_p, value=plant_img[0])

#         # compute the wf with the plant
#         if wf == "A":
#             ms, metadata_profiler = workflowA([data_plant, plant_img[1]], env_1)

#             # for debug
#             import resource
#             max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
#             end = time.time()
#             save_infos(cache_path=env_1.cache.path, state="success", id_p=id_p, mem=str(max_mem_usage),
#                        walltime=(end - start), dltime=(load_time - start), atime=metadata_profiler[0], adisk=metadata_profiler[1])

#             # return ms
#             return "success"

#         if wf == "B":
#             vs, metadata_profiler = workflowB([data_plant, plant_img[1]], env_1)
#             # return vs

#             # for debug
#             import resource
#             max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
#             end = time.time()
#             save_infos(cache_path=env_1.cache.path, state="success", id_p=id_p, mem=str(max_mem_usage),
#                        walltime=(end - start), dltime=(load_time - start), atime=metadata_profiler[0], adisk=metadata_profiler[1])

#             return "success"

#         if wf == "C":
#             maize_segmented, metadata_profiler = workflowC([data_plant, plant_img[1]], env_1)
#             # return maize_segmented

#             # for debug
#             import resource
#             max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
#             end = time.time()
#             save_infos(cache_path=env_1.cache.path, state="success", id_p=id_p, mem=str(max_mem_usage),
#                        walltime=(end - start), dltime=(load_time - start), atime=metadata_profiler[0], adisk=metadata_profiler[1])

#             return "success"

#     except Exception as e:
#         print(e)

#         # for debug
#         import resource
#         max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
#         end = time.time()
#         save_infos(cache_path=env_1.cache.path, state="fails", id_p=id_p, error=e, mem=str(max_mem_usage), walltime=end-start)


# def get_input_paths(**kwargs):
#     # path to the cache
#     from os.path import expanduser
#     home = expanduser("~")
#     if kwargs["cache_method"] == "cluster":
#         home = os.path.join(home, "work", "wf_executions")
#     cache_path = os.path.join(home, "openalea_cache", "exp" + str(kwargs["id_exp"]) )
#     # create dir if doesnt exist :
#     create_dir(cache_path)

#     # Get the data metadata (not the real images)
#     # df_meta_data = load_irods_metadata(cache_path=CACHE_PATH, experiment="ZA16", label="organized")
#     # select the meta data from the args
#     if kwargs["plant"] != "":
#         df_meta_data = load_irods_metadata(plant=kwargs["plant"], cache_path=cache_path, experiment="ZA16",
#                                            label="plant")
#     elif kwargs["nb_plants"] != 0:
#         df_meta_data = load_irods_metadata(nb_plants=kwargs["nb_plants"], cache_path=cache_path, experiment="ZA16",
#                                            label="reduced")

#     #TODO: Only one paht possible : local- if not exist-> dl from cache or IRODS
#     # plants = get_irods_paths_from_nb(df_meta_data, nb_plant=args.nb_plants)
#     if kwargs["cache_method"] == "irods":
#         plants = get_plants_with_irods(df_meta_data)
#     elif (kwargs["cache_method"] == "local") or (kwargs["cache_method"] == "cluster"):
#         plants = get_plants_with_local(df_meta_data)
#     # Get a list of what plant will be reuse
#     list_reuse = generate_list_percenreuse(kwargs["reuse_percent"], len(plants))

#     # initialize globals parameters
#     env_1 = Env_var()
#     env_1_initiate(env_1, cache_path=cache_path, cache_method=kwargs["cache_method"])

#     if kwargs["algo"] == "reuse_percent":
#         # set envs for the computations
#         envs1 = map(functools.partial(set_percent_reuse, env=env_1), list_reuse)
#     else:
#         env_1.algo = kwargs["algo"]
#         envs1 = [env_1] * len(plants)

#     return cache_path, envs1, df_meta_data, plants


# def save_time_output(ar, cache_path, **kwargs):
#     path_time = os.path.join(cache_path, "time")
#     path_output = os.path.join(path_time, time.strftime("%Y_%m_%d_%H_%M_%S") + ".csv")
#     create_dir(path_time)

#     # len_max = len(ar.get())
#     # # nb_correct = len_max - ar.get().count(None)
#     # nb_correct = len_max - ar.get().count("success")
#     nb_taks = 0.
#     nb_suc = 0.
#     for r in ar:
#         nb_taks += 1
#         if r == "success":
#             nb_suc += 1

#     with open(path_output, 'a') as outfile:
#         outfile.write(kwargs["workflow"] + ";" + str(kwargs["nb_engines"]) + ";" + str(kwargs["plant"]) + ";" + kwargs["algo"] + ";" + str(
#             ar.wall_time) + ";" + str(nb_suc) + ";" + str(nb_taks) + ";" + str(kwargs["reuse_percent"]) + ";" + str(
#             kwargs["nb_plants"]) + "\n")


# def save_infos(cache_path=None, state="", id_p="", **kwargs):
#     """
#     kwargs param can be :
#     mem = max_mem_usage
#     error = catched error
#     dttime = downloading transfer time
#     walltime = overall execution time
#     atime = (list) time per activity
#     avol = (list) disk usage per output activity
#     """
#     path_debug = os.path.join(cache_path, "time")
#     path_output = os.path.join(path_debug, state + id_p + "_" + time.strftime("%Y_%m_%d_%H_%M_%S"))
#     create_dir(path_debug)

#     with open(path_output, 'a') as outfile:
#         # Write ipplant | [error] | max mem usage | loading data time | wall time | time per activity | disk output per activity
#         outfile.write("id_p"+"="+id_p)

#         for arg in kwargs:
#             outfile.write(";")
#             outfile.write(str(arg) + "=" + str(kwargs[arg]))

# def evaluate_wf(**kwargs):
#     """
#     Start the ipcluster, evaluate the workflow with selected param, and evaluation methods, save the execution time
#     and stop the ipcluster
#     """

#     ### start IPython cluster
#     print('Connecting to ipcluster ...')
#     rc = ipp.Client(profile=kwargs["profile"], cluster_id=kwargs["cluster_id"])

#     ### Get inputs data
#     print('geting the metadatas ...')
#     cache_path, envs1, df_meta_data, plants = get_input_paths(**kwargs)
#     # for debug - only compute one snapshot
#     if kwargs["image"]:
#         envs1=[envs1[0]]
#         plants=[plants[0]]

#     # compute the wf
#     print('connecting to the engines ...')
#     dview = rc[:kwargs["nb_engines"]]

#     # FOR MEMORY PROFILING ---
#     if kwargs["memprofiler"]:
#         import subprocess
#         import os
#         import signal
#         print('Start mem profiling ...')
#         create_dir(os.path.join(cache_path, "mem"))

#         if kwargs["memprofiler"] == "per_plant":
#             for plant_nb in range(len(plants)):
#                 file_path = os.path.join(cache_path, "mem", "plant_"+str(plant_nb))
#                 with open(file_path, "w+"):
#                     pass
#                 os.chmod(file_path, 0o777)
#                 cmd = "free -mt -c 200000 -s 1 >" + file_path
#                 mem_prof_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
#                 # ---

#                 print('compute an image ')
#                 # print("je suis sous ensmble")
#                 ar = dview.map(functools.partial(start_execution3, wf=kwargs["workflow"]), [plants[plant_nb]], [envs1[plant_nb]], block=False)
#                 ar.wait()
#                 print(ar.successful())

#                 # FOR MEMORY PROFILING ---
#                 os.killpg(os.getpgid(mem_prof_proc.pid), signal.SIGTERM)
#                 # ---

#         elif kwargs["memprofiler"] == "one_map":
#             file_path = os.path.join(cache_path, "mem", "all_exp")
#             with open(file_path, "w+"):
#                 pass
#             os.chmod(file_path, 0o777)
#             cmd = "free -mt -c 200000 -s 1 >" + file_path
#             mem_prof_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
#             # ---

#             print('compute an image ')
#             # print("je suis sous ensmble")
#             ar = dview.map(functools.partial(start_execution3, wf=kwargs["workflow"]), plants, envs1, block=False)
#             ar.wait()
#             print(ar.successful())

#             # FOR MEMORY PROFILING ---
#             os.killpg(os.getpgid(mem_prof_proc.pid), signal.SIGTERM)
#             # ---


#     else:
#         print('compute images ')
#         ar = dview.map(functools.partial(start_execution3, wf=kwargs["workflow"]), plants, envs1, block=False)
#         ar.wait()
#         print(ar.successful())

#     # rc.wait(ar)
#     # res_test = start_execution3(wf=args.workflow, plant=plants[25], env_1=envs1[0])
#     # print res_test

#     # print(ar.get())
#     # Print for debug
#     # print(ar.get())
#     # print(ar.get()[0].value)
#     # len_max = len(ar.get())
#     # nb_correct = len_max - ar.get().count(None)
#     # print(args.workflow +";"+ str(args.nb_engines) +";"+str(args.plant) +";"+ args.algo +";"+ str(ar.wall_time) +";"+\
#     #       str(nb_correct) + ";" + str(len_max) +";"+str(args.reuse_percent) + ";" + str(args.nb_plants) + "\n")
#     ###########
#     # print start_execution3(wf=args.workflow, env_1=env_1, plant=plants[0])

#     # save the data output
#     print('saving metadatas ...')
#     save_time_output(ar, cache_path, **kwargs)


#     ### stop IPython cluster
#     # stop_ipy_cluster(p, cluster)
#     # rc.shutdown(hub=True)


# def main(args):
#     parser = argparse.ArgumentParser()
#     parser.add_argument('-n', '--nb_engine',
#                         dest="nb_engines",
#                         default=16,
#                         type=int,
#                         help='number of engine to use for the execution')

#     parser.add_argument('--nb_plants',
#                         dest="nb_plants",
#                         default=0,
#                         type=int,
#                         help='number of plant to compute')

#     parser.add_argument('-p', '--plant',
#                         dest="plant",
#                         default="",
#                         type=str,
#                         help='name of the plant')

#     parser.add_argument('-a', '--algo',
#                         dest="algo",
#                         default="classic",
#                         type=str,
#                         help='alogrithm to use for the evaluation. Can be : \n\
#                             \"classic\" : for normal evaluation - no cache use.\n\
#                             \"reuse\" : for cache greedy algo - if the result is in the cache the scheduler will always \
#                             take it, compute otherwise.\n\
#                             \"force_rerun\" : force the execution of each fragment and store it in the cache\n\
#                             reuse_percent : only a precentage of the input data is reuse')

#     parser.add_argument('-c', '--cache_method',
#                         dest="cache_method",
#                         default="local",
#                         type=str,
#                         help='location of the cache can be :\n\
#                             \"IRODS\" : the cache is stored on INRA server through IRODS. \n\
#                             \"local\" : the cache is stored on the local execution storage. \n\
#                             \"cluster\" : the cache is stored on cirad cluster ')

#     # parser.add_argument('-cp', '--cache_path',
#     #                     dest="cache_path",
#     #                     default=None,
#     #                     type=str,
#     #                     help='location of the cache can be :\n\
#     #                         \"IRODS\" : the cache is stored on INRA server through IRODS. \n\
#     #                         \"local\" : the cache is stored on the local execution storage. \n\
#     #                         \"cluster\" : the cache is stored on cirad cluster ')

#     parser.add_argument('-id', '--id',
#                         dest="id_exp",
#                         default=2,
#                         type=int,
#                         help='id of the experiment - change the cache path')

#     parser.add_argument('-wf', '--workflow',
#                         dest="workflow",
#                         default="C",
#                         type=str,
#                         help='number of plant to compute')

#     parser.add_argument('--profile',
#                         dest="profile",
#                         default="sge",
#                         type=str,
#                         help="ipycluster profile")

#     parser.add_argument('--cluster-id',
#                         dest="cluster_id",
#                         default="",
#                         type=str,
#                         help="ipycluster id")

#     parser.add_argument('-r', '--reuse_percent',
#                         dest="reuse_percent",
#                         default=100,
#                         type=int,
#                         help="percentage of data that is reuse")

#     parser.add_argument('-m', '--mem',
#                         dest="memprofiler",
#                         default=False,
#                         type=str,
#                         help="true -> activate memory profiler (use for debug)")

#     parser.add_argument('-i', '--image',
#                         dest="image",
#                         default=False,
#                         type=bool,
#                         help="true -> only compute one plant_ss (use for debug)"
#                              "per_plant"
#                              "one_map")

#     args = parser.parse_args(args)

#     # TODO: add the possibility to change paramters of the workflow
#     PARAM = None
#     d = vars(args)
#     # print d
#     # evaluate_wf(nb_engine=args.nb_engines, nb_plants=args.nb_plants, param=PARAM, index_algo=args.algo, \
#     #             cache_method=args.cache_method, id_exp=args.id_exp, cache_path=CACHE_PATH, workflow=args.worflow)
#     evaluate_wf(**d)


# if __name__ == '__main__':
#     main(sys.argv[1:])


from alinea.phenoarch.cloud import (
    get_remote_path_bin, 
    compute_one_plant, 
    process,
    get_organized_meta_data_from_experiment,
    extract_calibration,
    get_path
)
from openalea.phenomenal.display.image import (
    show_image
)
import os
import ast
import cv2
import csv
import sys
import errno
from pymongo import MongoClient
import dill

from paramiko import SSHClient

import hashlib
import joblib
import time
import pandas
from collections import defaultdict

from irods.session import iRODSSession
# from alinea.phenoarch.cloud import *

# phenomenal analysis imprts 
from openalea.phenomenal_wralea.phenoarch.routine import \
                                            ( binarize, get_side_image_projection_list,
                                            get_image_views )
from openalea.phenomenal.multi_view_reconstruction import reconstruction_3d
from openalea.phenomenal.data import calibrations
from openalea.phenomenal.segmentation.skeleton_phenomenal import skeletonize
from openalea.phenomenal.segmentation.graph import graph_from_voxel_grid
from openalea.phenomenal.segmentation.maize_analysis import maize_analysis
from openalea.phenomenal.segmentation.maize_segmentation import maize_segmentation
from openalea.phenomenal.segmentation.skeleton_phenomenal import segment_reduction

# distributed imports
# from cache_wrapper_function import Data, launch
from cloudParameters import CloudParameters, IndexMongoDB, Data, CloudClients

# Connection to the Data Node
ssh_addr = str("134.158.246.186")
ssh_user = str("ubuntu")


cp = CloudParameters()
cp.method = "sshfs"
cp.cache_dir = "/mnt/openalea_cache/"
# cp.cache_dir = "openalea_cache/"
cp.bin_dir = "/mnt/openalea_cache/binaries/"
# cp.bin_dir = "openalea_cache/binaries/"
excluded_plant_path = 'excluded.txt'
# debug
# cp.evaluation_algo = "classic"
# cp.nb_proc = 1
# cp.time_dir = "time/"
# print "init done"

def compute_one_row(row):
#     pname = row["plant"][0:4] + str(row["timestamp"])
    pname = row['pname']
    data_plant = Data(id=pname, value=None)
    timetot = []
    disktot = []
    timedl = []
    timecomp = []
    
    cc = CloudClients()
    
#     Start the index - connection to the mongodb 
    if cp.evaluation_algo != "classic":
        try:
            print("connecting to mongodb ....")
            mdbclient = MongoClient('localhost', 27017)
            index = IndexMongoDB(client=mdbclient)
            cc.index = index
            print("connected to mongodb")
        except Exception as e:
            time_path = os.path.join(cp.cache_dir, str(cp.nb_proc) +"_"+ cp.method +"_"+ cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
            save_infos(cache_path=time_path, state="fail", id_p=pname, error="fail to get the index", err=e)

#       if method is ssh - start ssh client      
    if cp.method == "ssh":
        try:
            print("connecting to sftp client ....")
            ssh_client = SSHClient()
            ssh_client.load_system_host_keys()
            ssh_client.connect(hostname=ssh_addr, username=ssh_user)
            sftp_client = ssh_client.open_sftp()
            cc.sftp_client = sftp_client
            print("connected to sftp")
        except Exception as e:
            time_path = os.path.join(cp.time_dir, str(cp.nb_proc)+"_"+cp.method+"_"+cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
            save_infos(cache_path=time_path, state="fail", id_p=pname, error="fail to create ssh connection", err=e)

    try: 
        print("start execution ...")
        start = time.time()
    #     images_bin = launch(cc, read_bin, row, method="ssh", cc=cc)
    #     timetot.append("binarize=" + str(images_bin.time))
    #     disktot.append("binarize=" + str(images_bin.size))
        #     images_bin = read_bin(row, method="ssh", ssh_client=ssh_client)
        images_bin = Data(id=pname, value=read_bin(row, method=cp.method, cc=cc))
        load_time=time.time()
        
    except Exception as e:
        time_path = os.path.join(cp.time_dir, str(cp.nb_proc)+"_"+cp.method+"_"+cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
        save_infos(cache_path=time_path, state="fail", id_p=pname, error="fail get bin data", err=e)
    try:
        print 
    #     t1 = time.time()-t0
        calibration = extract_calibration(row)
        print "start get_image_views"
    #     t2 = time.time()-t1-t0
        image_views = launch(cc, get_image_views, images_bin, calibration)
    #     print image_views, image_views.id, image_views.value
        timetot.append("get_image_views=" + str(image_views.time))
        timedl.append("get_image_views=" + str(image_views.dltime))
        timecomp.append("get_image_views=" + str(image_views.comptime))
        disktot.append("get_image_views=" + str(image_views.size))
        print "start reconstruction_3d"
    #     t3 = time.time()-t2-t1-t0
        voxel_grid = launch(cc, reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)
        timetot.append("reconstruction_3d=" + str(voxel_grid.time))
        timedl.append("reconstruction_3d=" + str(voxel_grid.dltime))
        timecomp.append("reconstruction_3d=" + str(voxel_grid.comptime))
        disktot.append("reconstruction_3d=" + str(voxel_grid.size))
        print "start graph_from_voxel_grid"
    #     t4 = time.time()-t3-t2-t1-t0
        g = launch(cc, graph_from_voxel_grid, voxel_grid)
        timetot.append("graph_from_voxel_grid=" + str(g.time))
        timedl.append("graph_from_voxel_grid=" + str(g.dltime))
        timecomp.append("graph_from_voxel_grid=" + str(g.comptime))
        disktot.append("graph_from_voxel_grid=" + str(g.size))
        print "start skeletonize"
    #     t5 = time.time()-t4-t3-t2-t1-t0
        voxel_skeleton = launch(cc, skeletonize, voxel_grid, g, subgraph=None)
        timetot.append("skeletonize=" + str(voxel_skeleton.time))
        timedl.append("skeletonize=" + str(voxel_skeleton.dltime))
        timecomp.append("skeletonize=" + str(voxel_skeleton.comptime))
        disktot.append("skeletonize=" + str(voxel_skeleton.size))
        print "start get_side_image_projection_list"
    #     t6 = time.time()-t5-t4-t3-t2-t1-t0
        side_image_projection = launch(cc, get_side_image_projection_list, images_bin, calibration)
        timetot.append("get_side_image_projection_list=" + str(side_image_projection.time))
        timedl.append("get_side_image_projection_list=" + str(side_image_projection.dltime))
        timecomp.append("get_side_image_projection_list=" + str(side_image_projection.comptime))
        disktot.append("get_side_image_projection_list=" + str(side_image_projection.size))
        print "start segment_reduction"
    #     t7 = time.time()-t6-t5-t4-t3-t2-t1-t0
        vs = launch(cc, segment_reduction, voxel_skeleton, side_image_projection, required_visible=4, nb_min_pixel=100)
        timetot.append("segment_reduction=" + str(vs.time))
        timedl.append("segment_reduction=" + str(vs.dltime))
        timecomp.append("segment_reduction=" + str(vs.comptime))
        disktot.append("segment_reduction=" + str(vs.size))
        print "start maize_segmentation"
    #     t8 = time.time()-t7-t6-t5-t4-t3-t2-t1-t0
        vms = launch(cc, maize_segmentation, vs, g)
        timetot.append("maize_segmentation=" + str(vms.time))
        timedl.append("maize_segmentation=" + str(vms.dltime))
        timecomp.append("maize_segmentation=" + str(vms.comptime))
        disktot.append("maize_segmentation=" + str(vms.size))
        print "start maize_analysis"
    #     t9 = time.time()-t8-t7-t6-t5-t4-t3-t2-t1-t0
        maize_segmented = launch(cc, maize_analysis, vms)
        timetot.append("maize_analysis=" + str(maize_segmented.time))
        timedl.append("maize_analysis=" + str(maize_segmented.dltime))
        timecomp.append("maize_analysis=" + str(maize_segmented.comptime))
        disktot.append("maize_analysis=" + str(maize_segmented.size))
        
    #     tfinal = time.time()-t9-t8-t7-t6-t5-t4-t3-t2-t1-t0
        end = time.time()
        print("execution over")
        
        print("start saving results")
#         save the output result
        end_result_path = os.path.join(cp.cache_dir, str(cp.nb_proc)+"_"+cp.method+"_"+cp.evaluation_algo, pname)
    #     p = get_path(row, output_directory=cp.cache_dir)
        if maize_segmented.value == None:
            path_to_data = cc.index.is_in(maize_segmented.id)
            tmp_data = load_intermediate_data(dname=None, data_path=path_to_data, method=cp.method,                                                           sftp_client=cc.sftp_client)
            maize_segmented.value = tmp_data.value
            
        maize_segmented.value.write_to_json_gz(end_result_path + ".json.gz")

#         save the metadata results
        time_path = os.path.join(cp.time_dir, str(cp.nb_proc)+"_"+cp.method+"_"+cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
        save_infos(cache_path=time_path, state="success", id_p=pname, walltime=(end - start), dltime=(load_time - start), atime=timetot, adisk=disktot, adltime=timedl, acomptime=timecomp)
        
#         cc.index.close
#         cc.sftp_client.close
        
    except Exception as e:
        time_path = os.path.join(cp.time_dir,  str(cp.nb_proc) +"_"+ cp.method +"_"+ cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
        save_infos(cache_path=time_path, state="fail", id_p=str(pname), error="fail to compute", err=e)
    
    return pandas.DataFrame()


def start_eval(input_df_path=None, start=None, end=None, nb_process=1, evaluation_algo="classic"):
    stime = time.time()
    
    cp.nb_proc = nb_process
    cp.evaluation_algo = evaluation_algo
    cp.id_exp = time.strftime("%Y_%m_%d_%H_%M_%S")
    from os.path import expanduser
    home = expanduser("~")
    cp.time_dir = os.path.join(home, "time")
    
    # get plant metadata for execution 
    input_df = pandas.read_csv(input_df_path, dtype={'pname':str})
    
    input_df["path_http"] = input_df["path_http"].apply(ast.literal_eval)
    input_df["camera_angle"] = input_df["camera_angle"].apply(ast.literal_eval)
    input_df["dates"] = input_df["dates"].apply(ast.literal_eval)
    input_df["view_type"] = input_df["view_type"].apply(ast.literal_eval)
    input_df["path_binary_image"] = input_df["path_binary_image"].apply(ast.literal_eval)

    cp.input_data_size = int(len(input_df))
    
#     process(compute_one_row,
    process(compute_one_row_rerun,
                input_df,
                start=start,
                end=end,
                nb_process=nb_process)
    ftime = time.time()
    walltime = ftime-stime
    
    end_path = os.path.join(cp.time_dir, 'wall_time_'+str(cp.nb_proc)+"_"+cp.method+"_"+cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp +'.csv')
    create_dir(os.path.dirname(end_path))
    with open(end_path, 'wb') as f:
        f.write(str(walltime))
        f.write("\n")
    
    return pandas.DataFrame()


# cache_path = "/home/gaetan/openalea_cache/binaries/ARCH2016-04-15_full/"
# cache_path = "/mnt/openalea_bin/"

def fix_path(row):
    bin_path = list()
    filenames = get_remote_path_bin(row)
    for path in filenames:
        bin_path.append(path.replace(
                'http://stck-lepse.supagro.inra.fr/phenoarch/binaries/',
                cp.bin_dir))
    row["path_binary_image"] = bin_path
    return row

def add_pname(row):
    pname = row["plant"][0:4] + str(row["timestamp"])
    row["pname"] = pname
    return row


def remove_excluded(path_excluded, df):
    if os.path.isfile(path_excluded) == False:
        return df
    # remove the duplicate in the list of exculded plants
    ep = list()
    with open(path_excluded, "r") as f:
        ep = f.readlines()
    ep = [x.strip() for x in ep] 

    ep = list(dict.fromkeys(ep))
    list_index_to_remove = list()
    try:
        for el in ep:
            list_index_to_remove.append(df[df['pname']==el].index[0])
        df = df.drop(list_index_to_remove, axis = 0)
    except:
        pass
    
    return df


def get_df_path(df_input_path=None, part=1, nb_chuncks=1):
    total_name = "ZA16_split_part{}_{}.csv".format(part, nb_chuncks)
    if (os.path.isfile(total_name)):
        return total_name
    
    df_red = pandas.read_csv(df_input_path, index_col=False)
    df_red["path_http"] = df_red["path_http"].apply(ast.literal_eval)
    df_red["camera_angle"] = df_red["camera_angle"].apply(ast.literal_eval)
    df_red["dates"] = df_red["dates"].apply(ast.literal_eval)
    df_red["view_type"] = df_red["view_type"].apply(ast.literal_eval)
    df_red = df_red.apply(fix_path, axis=1)
    df_red = df_red.apply(add_pname, axis=1)
    
#     remove the "non existent plants"
#     exculed_plant_path = 'excluded.txt'
    df_red = remove_excluded(excluded_plant_path, df_red)

    total = len(df_red)
    for i in range(1, nb_chuncks+1):
    #     print (i)*total/nb_chuncks, (i+1)*total/nb_chuncks
        sub_df = df_red.iloc[(i-1)*total/nb_chuncks :(i)*total/nb_chuncks]
        split_name = "ZA16_split_part{}_{}.csv".format(i, nb_chuncks)
        sub_df.to_csv(split_name, index = False)
    return total_name
    
    
def get_view_angle(path):
    def findnth(haystack, needle, n):
        parts= haystack.split(needle, n+1)
        if len(parts)<=n+1:
            return -1
        return len(haystack)-len(parts[-1])-len(needle)
    fname = os.path.basename(path)

    bins = dict()
    start = findnth(fname, "_", 1)
    end = findnth(fname, "_", 2)
    substring = fname[start+1: end]
    view = substring[0:2]
    angle = substring[2:]
    if view=="sv":
        view = "side"
    if view=="tv":
        view = "top"
    return view, angle


def read_bin(row, method=None, cc=None):
    if method =="file":
        paths = row["path_binary_image"]
        img_bin = defaultdict(dict)
        for file_path in paths:
            view, angle = get_view_angle(file_path)
            img_bin[view][int(angle)] = cv2.imread(file_path, cv2.IMREAD_UNCHANGED)
        return img_bin
    
    if method =="ssh":
        paths = row["path_binary_image"]
        img_bin = defaultdict(dict)
        for file_path in paths:
            view, angle = get_view_angle(file_path)
            img_bin[view][int(angle)] = load_img_from_ssh(sftp_client=cc.sftp_client, ssh_path=file_path)
        return img_bin
    
    if method =="sshfs":
        paths = row["path_binary_image"]
        img_bin = defaultdict(dict)
        for file_path in paths:
            view, angle = get_view_angle(file_path)
            img_bin[view][int(angle)] = cv2.imread(file_path, cv2.IMREAD_UNCHANGED)
        return img_bin


    
def create_archive_all_bin_folder(df_path=None, nb_chuncks=1):
    for part in range(nb_chuncks):
        split_name = "ZA16_split_part{}_{}.csv".format(part+1, nb_chuncks)
        if not (os.path.isfile(split_name)):
            get_df_path(df_input_path=df_path, part=1, nb_chuncks=nb_chuncks)
        if (os.path.isfile(split_name)):
            df_sub = pandas.read_csv(split_name)
            list_name = "ZA16_bin_path_part{}_{}.txt".format(part+1, nb_chuncks)
            create_archive_sub_bin_folder(df_sub, output_dir=list_name)
#             all_csv_files.append(total_name)
#             return total_name
    
    return 0

def create_dir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
        
def load_img_from_ssh(sftp_client=None, ssh_path=None):
    from os.path import expanduser
    home = expanduser("~")
    local_path = os.path.join(home, ssh_path[5:])
    create_dir(os.path.dirname(local_path))
    sftp_client.get(ssh_path, local_path)
    img = cv2.imread(local_path, cv2.IMREAD_UNCHANGED)
    #delete tmp_file
    if os.path.isfile(local_path):
        os.remove(local_path)
    else:  ## Show an error ##
        print("Error: %s file not found" % local_path)
    return img
    
def save_infos(cache_path=None, state="", id_p="", **kwargs):
    """
    kwargs param can be :
    mem = max_mem_usage
    error = catched error
    dttime = downloading transfer time
    walltime = overall execution time
    atime = (list) time per activity
    avol = (list) disk usage per output activity
    """
    path_debug = os.path.join(cache_path, "time")
    path_output = os.path.join(path_debug, state + id_p + "_" + time.strftime("%Y_%m_%d_%H_%M_%S") + ".csv")
    create_dir(path_debug)

    with open(path_output, 'a') as outfile:
        # Write ipplant | [error] | max mem usage | loading data time | wall time | time per activity | disk output per activity
        outfile.write("id_p"+"="+id_p)

        for arg in kwargs:
            try:
                outfile.write(";")
                outfile.write(str(arg) + "=" + str(kwargs[arg]))
            except:
                pass
            
                         
def write_intermediate_data_local(data, data_path):
    create_dir(os.path.dirname(data_path))
    try :
        with open(data_path, "wb") as f:
            dill.dump(data.value, f)
    except IOError as e:
        print e
    except:
        print "ERROR : value to save not a Data type"
        print "Fail to save : ", data, " to : ", data_path
    return os.path.getsize(data_path)

def load_intermediate_data_local(data_path):
    with open(data_path, "rb") as f:
        intermediate_data = dill.load(f)
    new_data = Data(id=os.path.basename(data_path), value=intermediate_data)
    return new_data

def write_intermediate_data_ssh(data, dname, data_path, sftp_client=None):
#     create_dir(os.path.dirname(data_path))
#     create the file on local
    from os.path import expanduser
    home = expanduser("~")
    tmp_path = os.path.join(home, data_path[5:])
    create_dir(os.path.dirname(tmp_path))
    
    dsize = write_intermediate_data_local(data, tmp_path)
#     dname = os.path.basename(data_path)
    cache_path = os.path.join("/mnt/openalea_cache/data", dname)
#     create_dir_ssh(os.path.dirname(cache_path), sftp_client)
    sftp_client.put(tmp_path, cache_path)
    
    #delete tmp_file
    if os.path.isfile(tmp_path):
        os.remove(tmp_path)
    else:  ## Show an error ##
        print("Error: %s file not found" % tmp_path)
    return dsize


def load_intermediate_data_ssh(data_path, sftp_client=None):
#     get file on local then load
    from os.path import expanduser
    home = expanduser("~")
    tmp_path = os.path.join(home, data_path[5:])
    sftp_client.get(data_path, tmp_path)
    
    new_data = load_intermediate_data_local(tmp_path)
    
    #delete tmp_file
    if os.path.isfile(tmp_path):
        os.remove(tmp_path)
    else:  ## Show an error ##
        print("Error: %s file not found" % tmp_path)

    return new_data



def write_intermediate_data(data, dname, data_path, method="local", sftp_client=None):
    if method == "ssh":
        return write_intermediate_data_ssh(data, dname, data_path, sftp_client=sftp_client)

    if method == "local":
        return write_intermediate_data_local(data, data_path)
    
    if method == "sshfs":
        return write_intermediate_data_local(data, data_path)



def load_intermediate_data(dname, data_path, method="local", sftp_client=None):
    if method == "ssh":
        return load_intermediate_data_ssh(dname, data_path, sftp_client=sftp_client)
    if method == "local":
        return load_intermediate_data_local(data_path)
    
    if method == "sshfs":
        return load_intermediate_data_local(data_path)



#####     Id functions    ####""""""""""""""""""""""""""""""""""""""""""""""""""
def set_id(data):
    if data.id:
        return data.id
    else:
        # get func and input
        if not data.parents:
            data.id = "orphan"
        else:
            new_id = ""
            new_id += data.func_name
            new_id += "("
            for parent in data.parents:
                if isinstance(parent, Data):
                    set_id(parent)
                    new_id += parent.id
                    new_id += ","
            #                 else:
            #                     try:
            #                         new_id+=repr(parent)
            #                     except:
            #                         pass
            new_id += ")"
            data.id = new_id
        return data.id


def get_task_id(func, *args, **kwargs):
    new_data = Data(id=None, value=None, parents=args, func_name=str(func.__name__))
    return set_id(new_data)


#####     execute wrapper func   ####"""""""""""""""""""""""""""""""""""""""""""
def execute(func, *args, **kwargs):
    values = []
    for data in args:
        if isinstance(data, Data):
            values.append(data.value)
        else:
            values.append(data)
    new_val = func(*values, **kwargs)
    new_parents = []
    for parent in args:
        if isinstance(parent, Data):
            new_parents.append(Data(id=parent.id, parents=parent.parents, func_name=parent.func_name))
        else:
            new_parents.append(parent)
    new_data = Data(id=None, value=new_val, parents=new_parents, func_name=str(func.__name__))
    set_id(new_data)
    return new_data


def launch_classic(func, *args, **kwargs):
    """
    execute the without cache
    :param env_1: shared parameters
    :param func: the function to wrap
    :param args: arg of the function
    :param kwargs: kwargs of the function
    :return:
    """
    start = time.time()

    tmp_data = execute(func, *args, **kwargs)

    tmp_data.size = 0
    tmp_data.time = time.time() - start

    return tmp_data


def launch_cache_greedy(cc, func, *args, **kwargs):
    """
    Wrap a function to use caching methods
    :param env_1: shared parameters
    :param func: the function to wrap
    :param args: arg of the function
    :param kwargs: kwargs of the function
    :return:
    """
    start = time.time()
    sftp_client = cc.sftp_client
    # retreive intermediate data if it exist and execute the act otherwise
    #     id_task = get_id_pname(env_1.id, func)
    id_task = get_task_id(func, *args, **kwargs)

    dname = hashlib.md5(id_task).hexdigest()
    cache_data_path = os.path.join(cp.cache_dir, "data", dname)

    # if the index has address to some data -> fetch it
    # check if the id_task is in the index and get the path if yes
    intermediate_data_path = cc.index.is_in(id_task)
    if intermediate_data_path:
        # GIVE REF! Not data
        # Create object with the ref/ dont download now
        #         tmp_data = load_intermediate_data(dname, cache_data_path, method=env_1.cache.method,irods_sess=env_1.irods_sess)
        print("LOAD DATA")
        dtime = time.time()
        tmp_data = Data(id=id_task, value=None)
        dsize = 0
        
    # otherwise compute and store the result
    else:
        # download data from refs:
        for data in args:
            if isinstance(data, Data):
                if not data.value:
                    try:
                        path_to_data = cc.index.is_in(data.id)
                        tmp_data = load_intermediate_data(dname=None, data_path=path_to_data, method=cp.method,                                                           sftp_client=cc.sftp_client)
                        data.value = tmp_data.value
                    except:
                        pass
        # Compute
        print("COMPUTE DATA")
        tmp_data = execute(func, *args, **kwargs)
        # update cache
        dtime = time.time()
        dsize = write_intermediate_data(tmp_data, dname, cache_data_path, method=cp.method,                                 sftp_client=cc.sftp_client)
        # update index
        cc.index.update(data_cached_path=cache_data_path, task_id=id_task)

#         tmp_data.size = os.path.getsize(cache_data_path)
    tmp_data.size = dsize
    tmp_data.time = time.time() - start
    tmp_data.comptime = dtime - start
    tmp_data.dltime = time.time() - dtime
    return tmp_data


def launch_cache_reexecute(cc, func, *args, **kwargs):
    """
    Wrap a function to use caching methods
    :param env_1: shared parameters
    :param func: the function to wrap
    :param args: arg of the function
    :param kwargs: kwargs of the function
    :return:
    """
    start = time.time()
    sftp_client = cc.sftp_client
    # retreive intermediate data if it exist and execute the act otherwise
    #     id_task = get_id_pname(env_1.id, func)
    id_task = get_task_id(func, *args, **kwargs)

    dname = hashlib.md5(id_task).hexdigest()
    cache_data_path = os.path.join(cp.cache_dir, "data", dname)

    # Compute
    print("COMPUTE DATA")
    tmp_data = execute(func, *args, **kwargs)
    # update cache
    dtime = time.time()
    dsize = write_intermediate_data(tmp_data, dname, cache_data_path, method=cp.method,                                 sftp_client=cc.sftp_client)
    # update index
    cc.index.update(data_cached_path=cache_data_path, task_id=id_task)

#         tmp_data.size = os.path.getsize(cache_data_path)
    tmp_data.size = dsize
    tmp_data.time = time.time() - start
    tmp_data.comptime = dtime - start
    tmp_data.dltime = time.time() - dtime
    return tmp_data


def launch(cc, func, *args, **kwargs):
    """
    Decide what function to run according to the algo
    :param env_1: shared parameters
    """
#     print("start " + func.__name__)
    # TODO: select a default algorithm
    if not cp.evaluation_algo:
        return None
    elif cp.evaluation_algo == "classic":
        return launch_classic(func, *args, **kwargs)
    
    elif cp.evaluation_algo == "cachegreedy":
        return launch_cache_greedy(cc, func, *args, **kwargs)
    
    elif cp.evaluation_algo == "cachererun":
        return launch_cache_reexecute(cc, func, *args, **kwargs)
    
    launch_cache_reexecute


def compute_one_row_rerun(row):
#     pname = row["plant"][0:4] + str(row["timestamp"])
    pname = row['pname']
    data_plant = Data(id=pname, value=None)
    timetot = []
    disktot = []
    timedl = []
    timecomp = []
    
    cc = CloudClients()
    
#     Start the index - connection to the mongodb 
    if cp.evaluation_algo != "classic":
        try:
            print("connecting to mongodb ....")
            mdbclient = MongoClient('localhost', 27017)
            index = IndexMongoDB(client=mdbclient)
            cc.index = index
            print("connected to mongodb")
        except Exception as e:
            time_path = os.path.join(cp.cache_dir, str(cp.nb_proc) +"_"+ cp.method +"_"+ cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
            save_infos(cache_path=time_path, state="fail", id_p=pname, error="fail to get the index", err=e)

#       if method is ssh - start ssh client      
    if cp.method == "ssh":
        try:
            print("connecting to sftp client ....")
            ssh_client = SSHClient()
            ssh_client.load_system_host_keys()
            ssh_client.connect(hostname=ssh_addr, username=ssh_user)
            sftp_client = ssh_client.open_sftp()
            cc.sftp_client = sftp_client
            print("connected to sftp")
        except Exception as e:
            time_path = os.path.join(cp.time_dir, str(cp.nb_proc)+"_"+cp.method+"_"+cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
            save_infos(cache_path=time_path, state="fail", id_p=pname, error="fail to create ssh connection", err=e)

    try: 
        print("start execution ...")
        start = time.time()
    #     images_bin = launch(cc, read_bin, row, method="ssh", cc=cc)
    #     timetot.append("binarize=" + str(images_bin.time))
    #     disktot.append("binarize=" + str(images_bin.size))
        #     images_bin = read_bin(row, method="ssh", ssh_client=ssh_client)
        images_bin = Data(id=pname, value=read_bin(row, method=cp.method, cc=cc))
        load_time=time.time()
        
    except Exception as e:
        time_path = os.path.join(cp.time_dir, str(cp.nb_proc)+"_"+cp.method+"_"+cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
        save_infos(cache_path=time_path, state="fail", id_p=pname, error="fail get bin data", err=e)
    try:
    #     t1 = time.time()-t0
        calibration = extract_calibration(row)
        print "start get_image_views"
    #     t2 = time.time()-t1-t0

        image_views = launch_cache_greedy(cc, get_image_views, images_bin, calibration)
    #     print image_views, image_views.id, image_views.value
        timetot.append("get_image_views=" + str(image_views.time))
        timedl.append("get_image_views=" + str(image_views.dltime))
        timecomp.append("get_image_views=" + str(image_views.comptime))
        disktot.append("get_image_views=" + str(image_views.size))
        print "start reconstruction_3d"
    #     t3 = time.time()-t2-t1-t0
        voxel_grid = launch_cache_greedy(cc, reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)
        timetot.append("reconstruction_3d=" + str(voxel_grid.time))
        timedl.append("reconstruction_3d=" + str(voxel_grid.dltime))
        timecomp.append("reconstruction_3d=" + str(voxel_grid.comptime))
        disktot.append("reconstruction_3d=" + str(voxel_grid.size))
        print "start graph_from_voxel_grid"
    #     t4 = time.time()-t3-t2-t1-t0
        g = launch_cache_greedy(cc, graph_from_voxel_grid, voxel_grid)
        timetot.append("graph_from_voxel_grid=" + str(g.time))
        timedl.append("graph_from_voxel_grid=" + str(g.dltime))
        timecomp.append("graph_from_voxel_grid=" + str(g.comptime))
        disktot.append("graph_from_voxel_grid=" + str(g.size))
        print "start skeletonize"
    #     t5 = time.time()-t4-t3-t2-t1-t0
        voxel_skeleton = launch_cache_greedy(cc, skeletonize, voxel_grid, g, subgraph=None)
        timetot.append("skeletonize=" + str(voxel_skeleton.time))
        timedl.append("skeletonize=" + str(voxel_skeleton.dltime))
        timecomp.append("skeletonize=" + str(voxel_skeleton.comptime))
        disktot.append("skeletonize=" + str(voxel_skeleton.size))
        print "start get_side_image_projection_list"
    #     t6 = time.time()-t5-t4-t3-t2-t1-t0
        side_image_projection = launch_cache_greedy(cc, get_side_image_projection_list, images_bin, calibration)
        timetot.append("get_side_image_projection_list=" + str(side_image_projection.time))
        timedl.append("get_side_image_projection_list=" + str(side_image_projection.dltime))
        timecomp.append("get_side_image_projection_list=" + str(side_image_projection.comptime))
        disktot.append("get_side_image_projection_list=" + str(side_image_projection.size))
        print "start segment_reduction"
    #     t7 = time.time()-t6-t5-t4-t3-t2-t1-t0
        vs = launch_cache_greedy(cc, segment_reduction, voxel_skeleton, side_image_projection, required_visible=4, nb_min_pixel=100)
        timetot.append("segment_reduction=" + str(vs.time))
        timedl.append("segment_reduction=" + str(vs.dltime))
        timecomp.append("segment_reduction=" + str(vs.comptime))
        disktot.append("segment_reduction=" + str(vs.size))
        print "start maize_segmentation"
    #     t8 = time.time()-t7-t6-t5-t4-t3-t2-t1-t0
        vms = launch_no_caching(cc, maize_segmentation, vs, g)
        timetot.append("maize_segmentation=" + str(vms.time))
        timedl.append("maize_segmentation=" + str(vms.dltime))
        timecomp.append("maize_segmentation=" + str(vms.comptime))
        disktot.append("maize_segmentation=" + str(vms.size))
        print "start maize_analysis"
    #     t9 = time.time()-t8-t7-t6-t5-t4-t3-t2-t1-t0
        maize_segmented = launch_no_caching(cc, maize_analysis, vms)
        timetot.append("maize_analysis=" + str(maize_segmented.time))
        timedl.append("maize_analysis=" + str(maize_segmented.dltime))
        timecomp.append("maize_analysis=" + str(maize_segmented.comptime))
        disktot.append("maize_analysis=" + str(maize_segmented.size))
        
    #     tfinal = time.time()-t9-t8-t7-t6-t5-t4-t3-t2-t1-t0
        end = time.time()
        print("execution over")
        
        print("start saving results")
#         save the output result
        end_result_path = os.path.join(cp.cache_dir, str(cp.nb_proc)+"_"+cp.method+"_"+cp.evaluation_algo, pname)
    #     p = get_path(row, output_directory=cp.cache_dir)
        maize_segmented.value.write_to_json_gz(end_result_path + ".json.gz")

#         save the metadata results
        time_path = os.path.join(cp.time_dir, str(cp.nb_proc)+"_"+cp.method+"_"+cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
        save_infos(cache_path=time_path, state="success", id_p=pname, walltime=(end - start), dltime=(load_time - start), atime=timetot, adisk=disktot, adltime=timedl, acomptime=timecomp)
        
#         cc.index.close
#         cc.sftp_client.close
        
    except Exception as e:
        time_path = os.path.join(cp.time_dir,  str(cp.nb_proc) +"_"+ cp.method +"_"+ cp.evaluation_algo + "_" + str(cp.input_data_size) + "_" + cp.id_exp)
        save_infos(cache_path=time_path, state="fail", id_p=str(pname), error="fail to compute", err=e)
    
    return pandas.DataFrame()


def launch_no_caching(cc, func, *args, **kwargs):
    """
    Wrap a function to use caching methods
    :param env_1: shared parameters
    :param func: the function to wrap
    :param args: arg of the function
    :param kwargs: kwargs of the function
    :return:
    """
    start = time.time()
    sftp_client = cc.sftp_client
    # retreive intermediate data if it exist and execute the act otherwise
    #     id_task = get_id_pname(env_1.id, func)
    id_task = get_task_id(func, *args, **kwargs)

    dname = hashlib.md5(id_task).hexdigest()
    cache_data_path = os.path.join(cp.cache_dir, "data", dname)

    # download data from refs:
    for data in args:
        if isinstance(data, Data):
            if not data.value:
                try:
                    path_to_data = cc.index.is_in(data.id)
                    tmp_data = load_intermediate_data(dname=None, data_path=path_to_data, method=cp.method,                                                           sftp_client=cc.sftp_client)
                    data.value = tmp_data.value
                except:
                    pass
    # Compute
    print("COMPUTE DATA")
    tmp_data = execute(func, *args, **kwargs)

    dtime = time.time()

    dsize=0
    tmp_data.size = dsize
    tmp_data.time = time.time() - start
    tmp_data.comptime = dtime - start
    tmp_data.dltime = time.time() - dtime
    return tmp_data

    
if __name__ == '__main__':
    path = str(sys.argv[1])
    nb_process = int(sys.argv[2])
    part = int(sys.argv[3])
    nb_chuncks = int(sys.argv[4])
    evaluation_algo = str(sys.argv[5])
#     start=int(sys.argv[6])
#     end=int(sys.argv[7])
    
    input_path = get_df_path(path, part, nb_chuncks)
    start_eval(input_df_path=input_path, start=None, end=None, nb_process=nb_process, evaluation_algo=evaluation_algo)
    