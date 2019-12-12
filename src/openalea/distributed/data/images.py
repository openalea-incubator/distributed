import cv2
import os
import json
import numpy as np
import pandas as pd
import collections
import ast
import pkg_resources
import pandas

from alinea.phenoarch.shooting_frame import get_shooting_frame
from openalea.phenomenal.image.formats import read_image, write_image
from openalea.distributed.cache.cache_file import create_dir


# def load_image(paths, method="irods", irods_sess=None):
#
#     if method == "irods":
#
#         images = list()
#         for i, file_path in enumerate(paths):
#
#             irods_filename = file_path.replace(
#                 'http://stck-lepse.supagro.inra.fr/',
#                 '/INRAgrid/home/public/M3P/')
#
#             obj = irods_sess.data_objects.get(irods_filename)
#
#             with obj.open('r') as fp:
#                 fp.seek(0, 2)
#                 buffer = fp.tell()
#                 fp.seek(0, 0)
#                 buf = np.asarray(bytearray(fp.read(buffer)))
#                 im = cv2.imdecode(buf, cv2.IMREAD_UNCHANGED)
#                 images.append(im)
#
#         return images
#
#     return list()


def get_paths_from_genotype(file, genotype):
    """
    Retrun a list of dict of the shape : {{top}:.., {side}:...}
    TODO: the paths are fetch from local csv - should be from mongodb (the function probably exists already)
    :param genotype: genotype of the plant = in a string
    :return: a list "plants" each plant is a list of [dict(paths of raw images), dict(calibrations), str(name of the plant)]
    """
    # index = pd.read_csv("2016-04-21.csv")
    tmp_path = pkg_resources.resource_filename(
        'openalea.distributed', file)
    index = pd.read_csv(tmp_path)
    paths = index.loc[lambda df: df.genotype == genotype].path_http.tolist()
    paths = [ast.literal_eval(x) for x in paths]
    view_type = index.loc[lambda df: df.genotype == genotype].view_type.tolist()
    view_type = [ast.literal_eval(x) for x in view_type]
    angles = index.loc[lambda df: df.genotype == genotype].camera_angle.tolist()
    angles = [ast.literal_eval(x) for x in angles]
    plants_name = index.loc[lambda df: df.genotype == genotype].plant.tolist()
    sf = index.loc[lambda df: df.genotype == genotype].shooting_frame.tolist()
    shooting_frames = [get_shooting_frame(x) for x in sf]

    l_snapshot = []
    for n in range(len(paths)):
        d_tmp = collections.defaultdict(dict)
        for i in range(len(paths[n])):
            d_tmp[str(view_type[n][i])][angles[n][i]] = str(paths[n][i])
        l_snapshot.append(d_tmp)

    calibrations=[]
    for i in range(len(l_snapshot)):
        calibration = collections.defaultdict(dict)
        for id_cam in ["side", "top"]:
            calibration[id_cam] = shooting_frames[i].get_calibration(id_cam)
        calibrations.append(calibration)

    # [l_snapshot, shooting_frames, plants_name]
    plants = [[l_snapshot[i], calibrations[i], plants_name[i]] for i in range(len(l_snapshot))]
    return plants


def get_irods_paths_from_nb(df_metadata, nb_plant):
    """
    Retrun a list of dict of the shape : {{top}:.., {side}:...}
    TODO: the paths are fetch from local csv - should be from mongodb (the function probably exists already)
    :param genotype: genotype of the plant = in a string
    :return: a list "plants" each plant is a list of [dict(paths of raw images), dict(calibrations), str(name of the plant)]
    """
    # index = pd.read_csv("2016-04-21.csv")
    #     tmp_path = pkg_resources.resource_filename(
    #         'openalea.distributed', file)
    #     index = pd.read_csv(tmp_path)
    newdf = df_metadata.sample(n=int(nb_plant), random_state=1)
    paths = newdf.path_http.tolist()
    try:
        paths = [ast.literal_eval(x) for x in paths]
    except ValueError:
        pass
    view_type = newdf.view_type.tolist()
    try:
        view_type = [ast.literal_eval(x) for x in view_type]
    except ValueError:
        pass
    angles = newdf.camera_angle.tolist()
    try:
        angles = [ast.literal_eval(x) for x in angles]
    except ValueError:
        pass


    timestamp = newdf.timestamp.tolist()
    timestamp = [str(x) for x in timestamp]

    plants_name = newdf.plant.tolist()
    sf = newdf.shooting_frame.tolist()
    shooting_frames = [get_shooting_frame(x) for x in sf]

    l_snapshot = []
    for n in range(len(paths)):
        d_tmp = collections.defaultdict(dict)
        for i in range(len(paths[n])):
            d_tmp[str(view_type[n][i])][angles[n][i]] = str(paths[n][i])
        l_snapshot.append(d_tmp)

    calibrations = []
    for i in range(len(l_snapshot)):
        calibration = collections.defaultdict(dict)
        for id_cam in ["side", "top"]:
            calibration[id_cam] = shooting_frames[i].get_calibration(id_cam)
        calibrations.append(calibration)

    # [l_snapshot, shooting_frames, plants_name]
    plants = [[l_snapshot[i], calibrations[i], plants_name[i] + timestamp[i]] for i in range(len(l_snapshot))]
    return plants


# TODO: problem when using the function more than one time in a row
def load_raw(paths, method="irods", irods_sess=None):
    """
    load the images from the paths
    :param paths: a list of dict [({top:{..}; side:{..}), ..] of the paths
    :param method:
    :param irods_sess:
    :return: a list of dict with the images instead of the paths
    """
    raw_data = list(paths)
    if method == "irods":
        if not raw_data:
            return list()
        for d in raw_data:
            for id_camera in d:
                for angle in d[id_camera]:
                    # print d[id_camera][angle]
                    images = list()
                    irods_filename = d[id_camera][angle].replace(
                        'http://stck-lepse.supagro.inra.fr/',
                        '/INRAgrid/home/public/M3P/')

                    obj = irods_sess.data_objects.get(irods_filename)

                    with obj.open('r') as fp:
                        fp.seek(0, 2)
                        buffer = fp.tell()
                        fp.seek(0, 0)
                        buf = np.asarray(bytearray(fp.read(buffer)))
                        im = cv2.imdecode(buf, cv2.IMREAD_COLOR)
                        images.append(im)
                    d[id_camera][angle] = images

        return raw_data

    if (method=="cluster") or (method=="local"):
        if not raw_data:
            return list()

        for d in raw_data:
            for id_camera in d:
                for angle in d[id_camera]:
                    # print d[id_camera][angle]
                    images = list()
                    img_path = d[id_camera][angle]
                    # print img_path
                    # d[id_camera][angle] = cv2.imread(img_path, cv2.IMREAD_COLOR)
                    d[id_camera][angle] = read_image(img_path)
                    # print d[id_camera][angle]
        return [d]

    return list()


def load_plant_snapshot(plant, irods_sess=None, method="local"):
    # TODO: dont use this variable
    # if method=="irods":
    #     ugly=0
    # elif (method=="cluster") or (method=="local"):
    #     ugly=3

    plant_img = plant[:]

    plants_snapshot_unshaped = load_raw([plant[0]], method=method, irods_sess=irods_sess)
    if method=='irods':
        plant_snapshot = collections.defaultdict(dict)
        for id_cam in ["side", "top"]:
            for angle in plants_snapshot_unshaped[0][id_cam]:
                plant_snapshot[id_cam][angle] = plants_snapshot_unshaped[0][id_cam][angle][0]
                plant_img[0] = plant_snapshot
    else:
        plant_img[0]=plants_snapshot_unshaped[0]
    return plant_img


def get_plants_with_local(newdf):
    """
    Retrun a list of dict of the shape : {{top}:.., {side}:...}
    TODO: the paths are fetch from local csv - should be from mongodb (the function probably exists already)
    :param genotype: genotype of the plant = in a string
    :return: a list "plants" each plant is a list of [dict(paths of raw images), dict(calibrations), str(name of the plant)]
    """
    # index = pd.read_csv("2016-04-21.csv")
    #     tmp_path = pkg_resources.resource_filename(
    #         'openalea.distributed', file)
    #     index = pd.read_csv(tmp_path)
    paths = newdf.path_http.tolist()
    try:
        paths = [ast.literal_eval(x) for x in paths]
    except ValueError:
        pass
    view_type = newdf.view_type.tolist()
    try:
        view_type = [ast.literal_eval(x) for x in view_type]
    except ValueError:
        pass
    angles = newdf.camera_angle.tolist()
    try:
        angles = [ast.literal_eval(x) for x in angles]
    except ValueError:
        pass
    paths_local = newdf.path_local.tolist()
    try:
        paths_local = [ast.literal_eval(x) for x in paths_local]
    except ValueError:
        pass

    timestamp = newdf.timestamp.tolist()
    timestamp = [str(x) for x in timestamp]

    plants_name = newdf.plant.tolist()
    sf = newdf.shooting_frame.tolist()
    shooting_frames = [get_shooting_frame(x) for x in sf]

    l_snapshot = []
    local_ss = []
    for n in range(len(paths)):
        d_tmp = collections.defaultdict(dict)
        l_tmp = collections.defaultdict(dict)
        for i in range(len(paths[n])):
            d_tmp[str(view_type[n][i])][angles[n][i]] = str(paths[n][i])
            l_tmp[str(view_type[n][i])][angles[n][i]] = str(paths_local[n][i])
        l_snapshot.append(d_tmp)
        local_ss.append(l_tmp)

    calibrations = []
    for i in range(len(l_snapshot)):
        calibration = collections.defaultdict(dict)
        for id_cam in ["side", "top"]:
            calibration[id_cam] = shooting_frames[i].get_calibration(id_cam)
        calibrations.append(calibration)

    # [l_snapshot, shooting_frames, plants_name]
    plants = [[local_ss[i], calibrations[i], plants_name[i] + timestamp[i]] for i in range(len(l_snapshot))]
    return plants


def get_plants_with_irods(newdf):
    """
    Retrun a list of dict of the shape : {{top}:.., {side}:...}
    TODO: the paths are fetch from local csv - should be from mongodb (the function probably exists already)
    :param genotype: genotype of the plant = in a string
    :return: a list "plants" each plant is a list of [dict(paths of raw images), dict(calibrations), str(name of the plant)]
    """
    # index = pd.read_csv("2016-04-21.csv")
    #     tmp_path = pkg_resources.resource_filename(
    #         'openalea.distributed', file)
    #     index = pd.read_csv(tmp_path)
    paths = newdf.path_http.tolist()
    try:
        paths = [ast.literal_eval(x) for x in paths]
    except ValueError:
        pass
    view_type = newdf.view_type.tolist()
    try:
        view_type = [ast.literal_eval(x) for x in view_type]
    except ValueError:
        pass
    angles = newdf.camera_angle.tolist()
    try:
        angles = [ast.literal_eval(x) for x in angles]
    except ValueError:
        pass
    paths_local = newdf.path_local.tolist()
    try:
        paths_local = [ast.literal_eval(x) for x in paths_local]
    except ValueError:
        pass

    timestamp = newdf.timestamp.tolist()
    timestamp = [str(x) for x in timestamp]

    plants_name = newdf.plant.tolist()
    sf = newdf.shooting_frame.tolist()
    shooting_frames = [get_shooting_frame(x) for x in sf]

    l_snapshot = []
    # local_ss = []
    for n in range(len(paths)):
        d_tmp = collections.defaultdict(dict)
        for i in range(len(paths[n])):
            d_tmp[str(view_type[n][i])][angles[n][i]] = str(paths[n][i])
            # l_tmp[str(view_type[n][i])][angles[n][i]] = str(paths_local[n][i])
        l_snapshot.append(d_tmp)
        # local_ss.append(l_tmp)

    calibrations = []
    for i in range(len(l_snapshot)):
        calibration = collections.defaultdict(dict)
        for id_cam in ["side", "top"]:
            calibration[id_cam] = shooting_frames[i].get_calibration(id_cam)
        calibrations.append(calibration)

    # [l_snapshot, shooting_frames, plants_name]
    plants = [[l_snapshot[i], calibrations[i], plants_name[i] + timestamp[i]] for i in range(len(l_snapshot))]
    return plants


def load_img_irods(path, cache_path, irods_sess=None):

    irods_filename = path.replace(
        cache_path,
        '/INRAgrid/home/public/M3P/phenoarch')
    obj = irods_sess.data_objects.get(irods_filename)
    with obj.open('r') as fp:
        fp.seek(0, 2)
        buffer = fp.tell()
        fp.seek(0, 0)
        buf = np.asarray(bytearray(fp.read(buffer)))
        im = cv2.imdecode(buf, cv2.IMREAD_COLOR)
    create_dir(os.path.dirname(path))
    write_image(path, im)
    return


def download_missing_images(plant, irods_sess=None, cache_path=None):
    plant_paths = plant[0]
    for id_camera in plant_paths:
        for angles in plant_paths[id_camera]:
            # dl the images if it is not in local file :
            if not os.path.exists(plant_paths[id_camera][angles]):
                load_img_irods(path=plant_paths[id_camera][angles], cache_path=cache_path, irods_sess=irods_sess)

    return
