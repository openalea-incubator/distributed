import sys
import time
import functools
import os
import pandas
import argparse
import ipyparallel as ipp

from openalea.distributed.data.images import load_plant_snapshot, get_plants_with_local, get_plants_with_irods, download_missing_images
from openalea.distributed.data.load_test_sets import load_irods_metadata
from openalea.distributed.execution.env_var import Env_var, env_1_initiate, env_1_update, set_percent_reuse
from openalea.distributed.workflows.workflows import workflowA, workflowB, workflowC
from openalea.distributed.execution.controller import stop_ipy_cluster, start_ipy_cluster
from openalea.distributed.execution.algo import generate_list_percenreuse

from openalea.distributed.execution.data import Data

from openalea.distributed.cache.cache_file import create_dir

# TODO: remove this
# PARAMETERS
LOCAL_CACHE_PATH = "/home/gaetan/OpenAlea/distributed/cache_data/"
IRODS_CACHE_PATH = "/INRAgrid/home/gheidsieck/cache/"
CLUSTER_LOCAL_PATH = "/homedir/heidsieck/work/wf_executions/"


### start the work
def start_execution3(plant=None, env_1=None, wf="A"):
    # get an id from the name of the plant
    id_p = plant[2][:4] + plant[2][-13:]
    start = time.time()

    try:
        print("start execution : ", plant[2][:4], "at : ", time.clock())

        # change the ID
        env_1.id = id_p
        env_1_update(env_1)
        # get the local img
        # TODO : change so it is possible to chose irods or local
        # TODO: check if local doesnt exist -> dl first
        if env_1.index.method == "local":
            download_missing_images(plant, irods_sess=env_1.irods_sess, cache_path=env_1.cache.path)
        load_time = time.time()
        plant_img = load_plant_snapshot(plant, irods_sess=env_1.irods_sess, method=env_1.index.method)
        data_plant = Data(id=id_p, value=plant_img[0])

        # compute the wf with the plant
        if wf == "A":
            ms, metadata_profiler = workflowA([data_plant, plant_img[1]], env_1)

            # for debug
            import resource
            max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            end = time.time()
            save_infos(cache_path=env_1.cache.path, state="success", id_p=id_p, mem=str(max_mem_usage),
                       walltime=(end - start), dltime=(load_time - start), atime=metadata_profiler[0], adisk=metadata_profiler[1])

            # return ms
            return "success"

        if wf == "B":
            vs, metadata_profiler = workflowB([data_plant, plant_img[1]], env_1)
            # return vs

            # for debug
            import resource
            max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            end = time.time()
            save_infos(cache_path=env_1.cache.path, state="success", id_p=id_p, mem=str(max_mem_usage),
                       walltime=(end - start), dltime=(load_time - start), atime=metadata_profiler[0], adisk=metadata_profiler[1])

            return "success"

        if wf == "C":
            maize_segmented, metadata_profiler = workflowC([data_plant, plant_img[1]], env_1)
            # return maize_segmented

            # for debug
            import resource
            max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            end = time.time()
            save_infos(cache_path=env_1.cache.path, state="success", id_p=id_p, mem=str(max_mem_usage),
                       walltime=(end - start), dltime=(load_time - start), atime=metadata_profiler[0], adisk=metadata_profiler[1])

            return "success"

    except Exception as e:
        print(e)

        # for debug
        import resource
        max_mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        end = time.time()
        save_infos(cache_path=env_1.cache.path, state="fails", id_p=id_p, error=e, mem=str(max_mem_usage), walltime=end-start)


def get_input_paths(**kwargs):
    # path to the cache
    from os.path import expanduser
    home = expanduser("~")
    if kwargs["cache_method"] == "cluster":
        home = os.path.join(home, "work", "wf_executions")
    cache_path = os.path.join(home, "openalea_cache", "exp" + str(kwargs["id_exp"]) )
    # create dir if doesnt exist :
    create_dir(cache_path)

    # Get the data metadata (not the real images)
    # df_meta_data = load_irods_metadata(cache_path=CACHE_PATH, experiment="ZA16", label="organized")
    # select the meta data from the args
    if kwargs["plant"] != "":
        df_meta_data = load_irods_metadata(plant=kwargs["plant"], cache_path=cache_path, experiment="ZA16",
                                           label="plant")
    elif kwargs["nb_plants"] != 0:
        df_meta_data = load_irods_metadata(nb_plants=kwargs["nb_plants"], cache_path=cache_path, experiment="ZA16",
                                           label="reduced")

    #TODO: Only one paht possible : local- if not exist-> dl from cache or IRODS
    # plants = get_irods_paths_from_nb(df_meta_data, nb_plant=args.nb_plants)
    if kwargs["cache_method"] == "irods":
        plants = get_plants_with_irods(df_meta_data)
    elif (kwargs["cache_method"] == "local") or (kwargs["cache_method"] == "cluster"):
        plants = get_plants_with_local(df_meta_data)
    # Get a list of what plant will be reuse
    list_reuse = generate_list_percenreuse(kwargs["reuse_percent"], len(plants))

    # initialize globals parameters
    env_1 = Env_var()
    env_1_initiate(env_1, cache_path=cache_path, cache_method=kwargs["cache_method"])

    if kwargs["algo"] == "reuse_percent":
        # set envs for the computations
        envs1 = map(functools.partial(set_percent_reuse, env=env_1), list_reuse)
    else:
        env_1.algo = kwargs["algo"]
        envs1 = [env_1] * len(plants)

    return cache_path, envs1, df_meta_data, plants


def save_time_output(ar, cache_path, **kwargs):
    path_time = os.path.join(cache_path, "time")
    path_output = os.path.join(path_time, time.strftime("%Y_%m_%d_%H_%M_%S") + ".csv")
    create_dir(path_time)

    # len_max = len(ar.get())
    # # nb_correct = len_max - ar.get().count(None)
    # nb_correct = len_max - ar.get().count("success")
    nb_taks = 0.
    nb_suc = 0.
    for r in ar:
        nb_taks += 1
        if r == "success":
            nb_suc += 1

    with open(path_output, 'a') as outfile:
        outfile.write(kwargs["workflow"] + ";" + str(kwargs["nb_engines"]) + ";" + str(kwargs["plant"]) + ";" + kwargs["algo"] + ";" + str(
            ar.wall_time) + ";" + str(nb_suc) + ";" + str(nb_taks) + ";" + str(kwargs["reuse_percent"]) + ";" + str(
            kwargs["nb_plants"]) + "\n")


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
    path_output = os.path.join(path_debug, state + id_p + "_" + time.strftime("%Y_%m_%d_%H_%M_%S"))
    create_dir(path_debug)

    with open(path_output, 'a') as outfile:
        # Write ipplant | [error] | max mem usage | loading data time | wall time | time per activity | disk output per activity
        outfile.write("id_p"+"="+id_p)

        for arg in kwargs:
            outfile.write(";")
            outfile.write(str(arg) + "=" + str(kwargs[arg]))

def evaluate_wf(**kwargs):
    """
    Start the ipcluster, evaluate the workflow with selected param, and evaluation methods, save the execution time
    and stop the ipcluster
    """

    ### start IPython cluster
    print('Connecting to ipcluster ...')
    rc = ipp.Client(profile=kwargs["profile"], cluster_id=kwargs["cluster_id"])

    ### Get inputs data
    print('geting the metadatas ...')
    cache_path, envs1, df_meta_data, plants = get_input_paths(**kwargs)
    # for debug - only compute one snapshot
    if kwargs["image"]:
        envs1=[envs1[0]]
        plants=[plants[0]]

    # compute the wf
    print('connecting to the engines ...')
    dview = rc[:kwargs["nb_engines"]]

    # FOR MEMORY PROFILING ---
    if kwargs["memprofiler"]:
        import subprocess
        import os
        import signal
        print('Start mem profiling ...')
        create_dir(os.path.join(cache_path, "mem"))

        if kwargs["memprofiler"] == "per_plant":
            for plant_nb in range(len(plants)):
                file_path = os.path.join(cache_path, "mem", "plant_"+str(plant_nb))
                with open(file_path, "w+"):
                    pass
                os.chmod(file_path, 0o777)
                cmd = "free -mt -c 200000 -s 1 >" + file_path
                mem_prof_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
                # ---

                print('compute an image ')
                # print("je suis sous ensmble")
                ar = dview.map(functools.partial(start_execution3, wf=kwargs["workflow"]), [plants[plant_nb]], [envs1[plant_nb]], block=False)
                ar.wait()
                print(ar.successful())

                # FOR MEMORY PROFILING ---
                os.killpg(os.getpgid(mem_prof_proc.pid), signal.SIGTERM)
                # ---

        elif kwargs["memprofiler"] == "one_map":
            file_path = os.path.join(cache_path, "mem", "all_exp")
            with open(file_path, "w+"):
                pass
            os.chmod(file_path, 0o777)
            cmd = "free -mt -c 200000 -s 1 >" + file_path
            mem_prof_proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
            # ---

            print('compute an image ')
            # print("je suis sous ensmble")
            ar = dview.map(functools.partial(start_execution3, wf=kwargs["workflow"]), plants, envs1, block=False)
            ar.wait()
            print(ar.successful())

            # FOR MEMORY PROFILING ---
            os.killpg(os.getpgid(mem_prof_proc.pid), signal.SIGTERM)
            # ---


    else:
        print('compute images ')
        ar = dview.map(functools.partial(start_execution3, wf=kwargs["workflow"]), plants, envs1, block=False)
        ar.wait()
        print(ar.successful())

    # rc.wait(ar)
    # res_test = start_execution3(wf=args.workflow, plant=plants[25], env_1=envs1[0])
    # print res_test

    # print(ar.get())
    # Print for debug
    # print(ar.get())
    # print(ar.get()[0].value)
    # len_max = len(ar.get())
    # nb_correct = len_max - ar.get().count(None)
    # print(args.workflow +";"+ str(args.nb_engines) +";"+str(args.plant) +";"+ args.algo +";"+ str(ar.wall_time) +";"+\
    #       str(nb_correct) + ";" + str(len_max) +";"+str(args.reuse_percent) + ";" + str(args.nb_plants) + "\n")
    ###########
    # print start_execution3(wf=args.workflow, env_1=env_1, plant=plants[0])

    # save the data output
    print('saving metadatas ...')
    save_time_output(ar, cache_path, **kwargs)


    ### stop IPython cluster
    # stop_ipy_cluster(p, cluster)
    # rc.shutdown(hub=True)


def main(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--nb_engine',
                        dest="nb_engines",
                        default=16,
                        type=int,
                        help='number of engine to use for the execution')

    parser.add_argument('--nb_plants',
                        dest="nb_plants",
                        default=0,
                        type=int,
                        help='number of plant to compute')

    parser.add_argument('-p', '--plant',
                        dest="plant",
                        default="",
                        type=str,
                        help='name of the plant')

    parser.add_argument('-a', '--algo',
                        dest="algo",
                        default="classic",
                        type=str,
                        help='alogrithm to use for the evaluation. Can be : \n\
                            \"classic\" : for normal evaluation - no cache use.\n\
                            \"reuse\" : for cache greedy algo - if the result is in the cache the scheduler will always \
                            take it, compute otherwise.\n\
                            \"force_rerun\" : force the execution of each fragment and store it in the cache\n\
                            reuse_percent : only a precentage of the input data is reuse')

    parser.add_argument('-c', '--cache_method',
                        dest="cache_method",
                        default="local",
                        type=str,
                        help='location of the cache can be :\n\
                            \"IRODS\" : the cache is stored on INRA server through IRODS. \n\
                            \"local\" : the cache is stored on the local execution storage. \n\
                            \"cluster\" : the cache is stored on cirad cluster ')

    # parser.add_argument('-cp', '--cache_path',
    #                     dest="cache_path",
    #                     default=None,
    #                     type=str,
    #                     help='location of the cache can be :\n\
    #                         \"IRODS\" : the cache is stored on INRA server through IRODS. \n\
    #                         \"local\" : the cache is stored on the local execution storage. \n\
    #                         \"cluster\" : the cache is stored on cirad cluster ')

    parser.add_argument('-id', '--id',
                        dest="id_exp",
                        default=2,
                        type=int,
                        help='id of the experiment - change the cache path')

    parser.add_argument('-wf', '--workflow',
                        dest="workflow",
                        default="C",
                        type=str,
                        help='number of plant to compute')

    parser.add_argument('--profile',
                        dest="profile",
                        default="sge",
                        type=str,
                        help="ipycluster profile")

    parser.add_argument('--cluster-id',
                        dest="cluster_id",
                        default="",
                        type=str,
                        help="ipycluster id")

    parser.add_argument('-r', '--reuse_percent',
                        dest="reuse_percent",
                        default=100,
                        type=int,
                        help="percentage of data that is reuse")

    parser.add_argument('-m', '--mem',
                        dest="memprofiler",
                        default=False,
                        type=str,
                        help="true -> activate memory profiler (use for debug)")

    parser.add_argument('-i', '--image',
                        dest="image",
                        default=False,
                        type=bool,
                        help="true -> only compute one plant_ss (use for debug)"
                             "per_plant"
                             "one_map")

    args = parser.parse_args(args)

    # TODO: add the possibility to change paramters of the workflow
    PARAM = None
    d = vars(args)
    # print d
    # evaluate_wf(nb_engine=args.nb_engines, nb_plants=args.nb_plants, param=PARAM, index_algo=args.algo, \
    #             cache_method=args.cache_method, id_exp=args.id_exp, cache_path=CACHE_PATH, workflow=args.worflow)
    evaluate_wf(**d)


if __name__ == '__main__':
    main(sys.argv[1:])
