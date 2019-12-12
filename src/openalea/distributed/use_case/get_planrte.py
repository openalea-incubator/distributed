""" Some macro for phenoarch data import"""

from alinea.phenoarch.cache_client import FileCache
from alinea.phenoarch.image_client import get_image_client

import types

import pandas
from alinea.phenoarch.image_index import get_image_index as _get_image_index, \
    load_image_index, get_daydates
from alinea.phenoarch.snapshot_index import SnapshotIndex
from alinea.phenoarch.user_data import user_plant_index, get_quarantine_index



# ==============================================================================


def get_image_index(experiment, daydate, cache_client):
    path = cache_client.paths.images_index_path(experiment, daydate)
    exp_dir = cache_client.paths.experiment_dir(experiment)
    im_index_dir = cache_client.paths.images_index_dir(experiment)
    cache_client.check_dir(exp_dir)
    cache_client.check_dir(im_index_dir)
    if not cache_client.exists(path):
        index = _get_image_index(experiment=experiment, daydate=daydate)
        cache_client.write(index, path, 'to_csv', index=False)
    else:
        index = cache_client.read(load_image_index, path)
    return index


def get_snapshot_data(experiment, cache_client):
    """ cache version of snapshot_index.get_snapshot_data restricted to one exp
    """

    if type(experiment) not in types.StringTypes:
        raise ValueError('one and only one experiment is required as input')

    daydates = get_daydates(experiment)

    data = []
    cols = ['experiment', 'plant', 'task', 'cabin', 'daydate', 'nview',
            'timestamp', 'shooting_frame']
    for day in daydates:
        im_index = get_image_index(experiment, day, cache_client)
        data.append(im_index.loc[:,cols])
    return pandas.concat(data)


def get_image_data(query_table, cache_client):
    """ cache version of snapshot_index get_image_data
    """
    experiment = query_table['experiment'].drop_duplicates().astype(str).tolist()
    plant = query_table['plant'].drop_duplicates().astype(str).tolist()
    daydate = query_table['daydate'].drop_duplicates().astype(str).tolist()
    task = query_table['task'].drop_duplicates().astype(int).tolist()

    data = []
    for exp in experiment:
        for day in daydate:
            index = get_image_index(exp, day, cache_client)
            index = index[index['plant'].isin(plant)]
            index = index[index['task'].isin(task)]
            if len(index) > 0:
                data.append(index)
    return pandas.concat(data)


def snapshot_index(experiment, image_client, cache_client):
    """ cache version of snapshot_index.snapshot_index restricted to one exp
    """
    exp_dir = cache_client.paths.experiment_dir(experiment)
    cache_client.check_dir(exp_dir)

    sindex_path = cache_client.paths.snapshot_index_path(experiment)
    pindex_path = cache_client.paths.plant_index_path(experiment)
    quarantine_path = cache_client.paths.quarantine_index_path(experiment)

    if cache_client.exists(quarantine_path):
        quarantine = cache_client.read(pandas.read_csv, quarantine_path)
    else:
        quarantine = get_quarantine_index(experiment)
        if quarantine is not None:
            cache_client.write(quarantine, quarantine_path, 'to_csv',
                               index=False)

    if cache_client.exists(pindex_path):
        plant_index = cache_client.read(pandas.read_csv, pindex_path)
    else:
        plant_index = user_plant_index(experiment)
        cache_client.write(plant_index, pindex_path, 'to_csv', index=False)

    if cache_client.exists(sindex_path):
        snap_data = cache_client.read(pandas.read_csv, sindex_path)
    else:
        snap_data = get_snapshot_data(experiment, cache_client)
        cache_client.write(snap_data, sindex_path, 'to_csv', index=False)

    # force line and position to be read as string
    plant_index.loc[:, 'line'] = map(str, plant_index['line'].values)
    plant_index.loc[:, 'position'] = map(str, plant_index['position'].values)

    index = SnapshotIndex(snap_data, plant_index, quarantine, get_image_data,
                          image_client=image_client, cache_client=cache_client)

    return index



def get_plant(experiment="ZA16", plant='0001/Lo1270_H/ZM2887/d151/WD/3/01_01/ARCH2016-04-15', daydate='2016-06-16', client='irods', cache='/home/gaetan/OpenAlea/random_script'):
    """

experiment="ZA16"; plant='0001/Lo1270_H/ZM2887/d151/WD/3/01_01/ARCH2016-04-15'; daydate='2016-06-16'; client='http'; cache='/home/gaetan/OpenAlea/random_script'

    :param experiment:
    :param plant:
    :param daydate:
    :param client: string: either 'http' or 'irods
    :return:
    """
    image_client = get_image_client(client)
    cache_client = FileCache(cache)
    index = snapshot_index(experiment=experiment, image_client=image_client, cache_client=cache_client)
    # index.filter(plant='0119/B73/ZM3838/C5/WW/T/2_59/ARCH2014-08-25').reset_index(drop=True)
    selection = index.filter(plant=plant, daydate=daydate)
    if not selection.empty:
        snapshot = index.get_snapshots(selection, meta=False)[0]
        views = snapshot.views
        view_angles = snapshot.view_angles
        images = snapshot.get_images()

    image_dict  = {'top':{}, 'side':{}}
    for view, image, angle in zip(views, images, view_angles):
        image_dict[view][angle] = image
    shooting_frame = snapshot.get_shooting_frame()
    calibration_side = shooting_frame.get_calibration(view='side')
    calibration_top = shooting_frame.get_calibration(view='top')
    return image_dict, calibration_side, calibration_top


get_plant(experiment="ZA16", plant='0001/Lo1270_H/ZM2887/d151/WD/3/01_01/ARCH2016-04-15', daydate='2016-06-16', client='irods', cache='/home/gaetan/OpenAlea/random_script')
