import hashlib
import joblib
import dill

from openalea.phenomenal.data import raw_images
from openalea.phenomenal_wralea.phenoarch.routine import binarize, get_image_views
from openalea.phenomenal.multi_view_reconstruction import reconstruction_3d
from openalea.phenomenal.data import calibrations
from openalea.phenomenal.segmentation import graph_from_voxel_grid, skeletonize

from openalea.distributed.cache.get_id import try_id


"""
raw_image : joblib.hash( _, hash_name='md5')
image_bin : joblib.hash( _, hash_name='md5')
image_view : _.__hash()
voxelgrid : _.__hash()
g : joblib.hash( _, hash_name='md5')
calibration : ignored - same for all images
voxel_skeleton : joblib.hash( _, hash_name='md5')
side_image_projection : ignored -
vs : _.__hash__()
vms : joblib.hash( _, hash_name='md5')
maize_segmented : 
"""

cache_test = "/home/gaetan/OpenAlea/distributed/cache_data/test/TEST/data/"


###
###     id tests of objects
###
###############################################################################################################
###############################################################################################################


def test_id_images():
    with open(cache_test+'binImage') as f:
        images_bin = dill.load(f)
    with open(cache_test+'binImage') as f:
        images_bin2 = dill.load(f)
    print joblib.hash(images_bin, hash_name='md5').hexdigest() == joblib.hash(images_bin2, hash_name='md5').hexdigest()

def test_id_images_views():
    with open(cache_test+'images_views') as f:
        iv = dill.load(f)
    with open(cache_test+'images_views') as f:
        iv2 = dill.load(f)
    print hash(tuple([x.__hash__() for x in iv]))==hash(tuple([x.__hash__() for x in iv2]))

def test_id_voxelSegment():
    with open(cache_test+'voxelSegment1') as f:
        vs = dill.load(f)
    with open(cache_test+'voxelSegment2') as f:
        vs2 = dill.load(f)
    print hash(vs) == hash(vs2)


def test_id_voxelSkeleton():
    with open(cache_test+'voxelSkeleton') as f:
        vs = dill.load(f)
    with open(cache_test+'voxelSkeleton') as f:
        vs2 = dill.load(f)

    print hash(vs) == hash(vs2)


def test_id_graph():
    with open(cache_test+'graph') as f:
        g = dill.load(f)
    with open(cache_test+'graph') as f:
        g2 = dill.load(f)
    print joblib.hash(g, hash_name='md5') == joblib.hash(g2, hash_name='md5')


def test_id_voxelGrid():
    with open(cache_test+'voxelGrid') as f:
        voxel_grid = dill.load(f)
    with open(cache_test+'voxelGrid') as f:
        voxel_grid2 = dill.load(f)

    print voxel_grid.__hash__() == voxel_grid2.__hash__()


###
###     id tests of functions
###
###############################################################################################################
###############################################################################################################


def test_id_bin():
    raw_image = raw_images(plant_number=1)
    print try_id(binarize, raw_image) == try_id(binarize, raw_image)


def test_id_recons3d():
    with open(cache_test+'binImage') as f:
        images_bin = dill.load(f)
    with open(cache_test+'binImage') as f:
        images_bin2 = dill.load(f)
    calibration = calibrations(plant_number=1)
    image_views = get_image_views(images_bin, calibration)
    image_views2 = get_image_views(images_bin2, calibration)

    print try_id(reconstruction_3d, image_views, start_voxel_size=256) == try_id(reconstruction_3d, image_views2, start_voxel_size=256)


def test_id_graph_from_vg():
    with open(cache_test+'voxelGrid') as f:
        voxel_grid = dill.load(f)
    with open(cache_test+'voxelGrid') as f:
        voxel_grid2 = dill.load(f)
    print try_id(graph_from_voxel_grid, voxel_grid) == try_id(graph_from_voxel_grid, voxel_grid2)


def test_id_skeletonize():
    with open(cache_test+'voxelGrid') as f:
        voxel_grid = dill.load(f)
    with open(cache_test+'voxelGrid') as f:
        voxel_grid2 = dill.load(f)
    with open(cache_test+'graph') as f:
        g = dill.load(f)
    with open(cache_test+'graph') as f:
        g2 = dill.load(f)

    print try_id(skeletonize, voxel_grid, g) == try_id(skeletonize, voxel_grid2, g2)


def test_id_side_img_projection():
    # side_image_projection = get_side_image_projection_list(images_bin, calibration)
    # side_image_projection2 = get_side_image_projection_list(images_bin2, calibration)
    # try_id(get_side_image_projection_list, images_bin, calibration)
    # try_id(get_side_image_projection_list, images_bin2, calibration)
    pass


def test_id_segment_reduction():
    # vs = segment_reduction(voxel_skeleton, side_image_projection, tolerance=4, nb_min_pixel=1)
    # vs2 = segment_reduction(voxel_skeleton2, side_image_projection2, tolerance=4, nb_min_pixel=1)
    # try_id(segment_reduction, voxel_skeleton, side_image_projection, tolerance=4, nb_min_pixel=1)
    # try_id(segment_reduction, voxel_skeleton2, side_image_projection2, tolerance=4, nb_min_pixel=1)
    pass

# def test_id_
# vms = maize_segmentation(vs, g)
# vms2 = maize_segmentation(vs2, g2)
# try_id(maize_segmentation, vs, g)
# try_id(maize_segmentation, vs2, g2)
#
# maize_segmented = maize_analysis(vms)
# maize_segmented2 = maize_analysis(vms2)
# try_id(maize_analysis, vms)
# try_id(maize_analysis, vms2)


# print skeleton_phenomenal.skeletonize.__name__

# print "start"
# test_id_graph()
