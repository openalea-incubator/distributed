import dask
from dask import delayed
import dask.bag as db
from dask.distributed import Client

import cv2
import glob
import os
import collections
import pkg_resources


from benchmark import time_usage

__all__ = ['f',
            'test_dask_bag',
            'test_client_distrib',
            'plant_1_images']


def plant_1_images():

    data_directory = "/home/gaetan/cs_pheno/random_script/download_raw_image_phenoarch/plant_1/images/"

    # data = pkgutil.get_data('openalea.phenomenal',
    #                         'share/data/plant_1/images/*.png')

    files_path = glob.glob(data_directory + '*.png')

    images = collections.defaultdict(lambda: collections.defaultdict())

    for path in files_path:
        basename = os.path.basename(path)
        id_camera = basename.split('_')[0]
        angle = int(((basename.split('_')[1]).split('.png'))[0])

        images[id_camera][angle] = cv2.imread(path, cv2.IMREAD_UNCHANGED)

    return images


#@delayed
def f(x):
    return x**x**x

@time_usage
def test_dask_bag(items):
    bag = db.from_sequence(items)

    res = bag.map(f)

    #with dask.set_options(get=dask.multiprocessing.get):
    return res.compute()


@time_usage
def test_client_distrib(items):
    # a scheduler must be running
    client = Client()
    print client
    c = client.map(f, items)
    print c
    return client.gather(c)



@time_usage
def main():
    items = [7] * 1

    #test_dask_bag(items)

    test_client_distrib(items)

    

if __name__ == "__main__":
    main()
