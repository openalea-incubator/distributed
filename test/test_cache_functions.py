from openalea.distributed.data import get_paths_from_genotype, load_raw
from irods.session import iRODSSession
import dill

from openalea.distributed.cache.index import IndexFile
from openalea.distributed.cache.timer import Timer
from openalea.distributed.cache.get_id import get_id2
from openalea.distributed.cache.cache_file import load_intermediate_data

from openalea.phenomenal.data import raw_images
import collections


irods_session = iRODSSession(host='inragrid-mtp.supagro.inra.fr',
                                  port=1247,
                                  user='gheidsieck',
                                  password='ghe2018#',
                                  zone='INRAgrid')


def test_recup_data():
    paths = get_paths_from_genotype("T02273_005HE")
    tmp_res = load_raw([paths[0]], method="irods", irods_sess=irods_session)

    # modify the shape of the plant_snap shot to be the same way as from :

    plant_snapshot = raw_images(plant_number=1)

    plant_snapshot = collections.defaultdict(dict)
    for id_cam in ["side", "top"]:
        for angle in tmp_res[0][id_cam]:
            plant_snapshot[id_cam][angle] = tmp_res[0][id_cam][angle][0]


# local_root_cache_path = "/home/gaetan/OpenAlea/distributed/cache_data/cache_ZA16/"
# cache_path = local_root_cache_path + "exp" + "11" + "/"

#
# with open(cache_path+"data/reconstruction_3d__217bd77e78e98e90a2cbfc5d719e4362") as f:
#     graph = dill.load(f)
#
# with open(cache_path+"data/reconstruction_3d__fd3a37ec22dd87115eb1de2e0ad9ce3c") as f:
#     graph2 = dill.load(f)
#
# with open(cache_path+"data/binarize__63ca31743c013b606a63f2219d1f24e4") as f:
#     bin = dill.load(f)
#
# with open(cache_path+"data/binarize__69ae3ef872e70f5a94d257715188622c") as f:
#     bin2 = dill.load(f)

def test_hash_voxelSegments():
    file_to_get = "/INRAgrid/home/gheidsieck/cache/exp11/data/skeletonize__bc991a3f5adacd616d90397ecc1e2f3b"
    cache_path = "/home/gaetan/OpenAlea/distributed/cache_data/cache_ZA16/exp11/data/skeletonize__bc991a3f5adacd616d90397ecc1e2f3b"
    result = load_intermediate_data(cache_path)

    # hash(np.asarray(result.voxel_segments[0].polyline).tostring())
    # vp = sorted(result.voxel_segments[0].voxels_position)
    # test2 = [sorted(x) for x in result2.voxel_segments[0].closest_nodes]

    result2 = load_intermediate_data(cache_path)

    print hash(result.voxel_segments[0]) == hash(result2.voxel_segments[0])


def test_hash_voxelSkeleton():
    file_to_get = "/INRAgrid/home/gheidsieck/cache/exp11/data/skeletonize__bc991a3f5adacd616d90397ecc1e2f3b"
    cache_path = "/home/gaetan/OpenAlea/distributed/cache_data/cache_ZA16/exp11/data/skeletonize__bc991a3f5adacd616d90397ecc1e2f3b"
    result = load_intermediate_data(cache_path)

    # hash(np.asarray(result.voxel_segments[0].polyline).tostring())
    # vp = sorted(result.voxel_segments[0].voxels_position)
    # test2 = [sorted(x) for x in result2.voxel_segments[0].closest_nodes]

    result2 = load_intermediate_data(cache_path)

    print hash(result) == hash(result2)


# test_hash_voxelSkeleton()