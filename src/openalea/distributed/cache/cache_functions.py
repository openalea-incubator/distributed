import time
import csv

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
from openalea.phenomenal.display import DisplaySegmentation

from openalea.distributed.cache.index import IndexFile
from openalea.distributed.cache.timer import Timer
from openalea.distributed.cache.get_id import get_id2, get_id3
from openalea.distributed.cache.cache_file import (write_intermediate_data, load_intermediate_data,
load_intermediate_data_irods, write_intermediate_data_irods)


def launch2(timer, cache_path, index, func, *args, **kwargs):
    # retreive intermediate data if it exist and execute the act otherwise
    id_task = get_id2(func, args, kwargs)
    dname = str(func.__name__) + "__" + id_task

    ttmp = time.clock()
    tmp_path, tmp_irods_path = index.search(id_task)
    timer.update_tsearch(ttmp)

    #if the index has address to some data -> fetch it
    if tmp_path is not None:
        #TODO: the loading time isnt working
        ttmp = time.clock()
        tmp_data = load_intermediate_data(tmp_path)
        timer.update_tload(ttmp)

    #otherwise compute and store the result
    else:
        #compute
        ttmp = time.clock()
        tmp_data = func(*args, **kwargs)
        timer.update_texec(ttmp)

        #update cache
        data_path = cache_path + "/data/" + dname
        ttmp = time.clock()
        write_intermediate_data(tmp_data, data_path)
        timer.update_tpersist(ttmp)

        #if storing : ok -> update index
        ttmp = time.clock()
        index.update(id_task, data_path, "NOT USED")
        timer.update_tindex(ttmp)

    return tmp_data


def launch_irods(timer, plant_name, irods_sess, cache_path, index, func, *args, **kwargs):
    tstart = time.clock()
    print 'launch : ', func.__name__, 'at ', tstart
    # retreive intermediate data if it exist and execute the act otherwise
    print 'Get id : ', func.__name__, ' at ', time.clock()
    id_task = get_id3(plant_name, func, args, kwargs)
    dname = str(func.__name__) + "__" + str(id_task)
    local_cache_path = "/home/gaetan/OpenAlea/distributed/cache_data/cache_ZA16/exp11/"

    print 'SEARCH : ', func.__name__, ' at ', time.clock()
    ttmp = time.clock()
    tmp_path, tmp_irods_path = index.search(id_task)
    timer.update_tsearch(ttmp)
    print 'SEARCH TOTAL TIME : ', timer.tsearch

    #if the index has address to some data -> fetch it
    if tmp_irods_path is not None:
        #TODO: the loading time isnt working
        ttmp = time.clock()
        print 'LOAD : ',  tmp_irods_path, ' at ', time.clock()
        tmp_data = load_intermediate_data_irods(tmp_irods_path, irods_sess=irods_sess)
        timer.update_tload(ttmp)
        print 'LOAD TOTAL TIME : ', timer.tload, 'total size : ', irods_sess.data_objects.get(tmp_irods_path).size

        try:
            with open("/home/gaetan/OpenAlea/distributed/cache_data/cache_ZA16/exp2/time/time2.csv", "a")as index_file:
                writer = csv.writer(index_file,
                                    delimiter=",")
                writer.writerow([time.clock()-ttmp, irods_sess.data_objects.get(tmp_irods_path).size])
                # str([timer.ttotal, timer.texec, timer.tpersist, timer.tindex, timer.tload, timer.tload_raw, timer.tsearch])[
                # 1:-1] + "\n"

        except EnvironmentError, e:  # parent of IOError, OSError *and* WindowsError where available
            print e



    #otherwise compute and store the result
    else:
        #compute
        print 'EXEC : ', func.__name__, ' at ', time.clock()
        ttmp = time.clock()
        tmp_data = func(*args, **kwargs)
        timer.update_texec(ttmp)
        print 'EXEC TOTAL TIME : ', timer.texec
        try:
            with open("/home/gaetan/OpenAlea/distributed/cache_data/cache_ZA16/exp2/time/time2.csv", "a")as index_file:
                writer = csv.writer(index_file,
                                    delimiter=",")
                writer.writerow([time.clock()-ttmp])
                # str([timer.ttotal, timer.texec, timer.tpersist, timer.tindex, timer.tload, timer.tload_raw, timer.tsearch])[
                # 1:-1] + "\n"

        except EnvironmentError, e:  # parent of IOError, OSError *and* WindowsError where available
            print e

        #update cache
        irods_data_path = cache_path + "data/" + dname
        local_data_path = local_cache_path + "data/" + dname
        print 'PERSIST : ', irods_data_path, ' at ', time.clock()
        ttmp = time.clock()
        #TODO: persist on irods without writing on local
        #### changement du cache file
        #### write_intermediate_data(tmp_data, local_data_path)
        # write_intermediate_data_irods(local_data_path, irods_data_path, irods_sess=irods_sess)
        write_intermediate_data_irods(tmp_data, irods_data_path, irods_sess=irods_sess)
        timer.update_tpersist(ttmp)
        print 'PERSIST TOTAL TIME : ', timer.tpersist

        #if storing : ok -> update index
        ttmp = time.clock()
        index.update(id_task, local_data_path, irods_data_path)
        timer.update_tindex(ttmp)

    print 'launch ', func.__name__, ' time : ', (time.clock() - tstart)
    return tmp_data


def launch_only_exec(func, *args, **kwargs):
    print func.__name__
    return func(*args, **kwargs)


def compute_minimal(plant, env):
    """
    Execute the WF phenomenal on plant 1
    :param plant: a list of [ plant_snapshot, calibration ] FOR ONE PLANT
    :param id_exp:
    :param cache_path:
    :param index:
    :param timer:
    :param irods_session:
    :return:
    """

    raw_image = plant[0]
    plant_name = plant[2]

    images_bin = launch_only_exec(binarize, raw_image)

    calibration = plant[1]
    image_views = get_image_views(images_bin, calibration)

    voxel_grid = launch_only_exec(reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)

    g = launch_only_exec(graph_from_voxel_grid, voxel_grid)

    voxel_skeleton = launch_only_exec(skeletonize, voxel_grid, g, subgraph=None)

    side_image_projection = launch_only_exec(get_side_image_projection_list, images_bin, calibration)

    vs = launch_only_exec(segment_reduction, voxel_skeleton, side_image_projection, required_visible=4, nb_min_pixel=100)

    vms = launch_only_exec(maize_segmentation, vs, g)

    maize_segmented = launch_only_exec(maize_analysis, vms)


    d = DisplaySegmentation()
    d(maize_segmented)
