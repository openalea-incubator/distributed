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

from openalea.distributed.execution.cache_wrapper_function import launch


def workflowA(plant, env_1):
    """
       Execute the 3 frist fragment of the WF phenomenal on plant
       :param plant: a list of [ plant_snapshot, calibration ] FOR ONE PLANT
       :param env_1 : an object that containt the informations about : the cache, the index, the irods_sess, and the plant name for the GET_ID
       :return:
    """
    timetot = []
    disktot = []
    images_bin = launch(env_1, binarize, plant[0])
    print images_bin
    print images_bin.value
    print images_bin.id
    timetot.append("binarize="+str(images_bin.time))
    disktot.append("binarize="+str(images_bin.size))

    print("fin bina")
    calibration = plant[1]
    image_views = launch(env_1, get_image_views, images_bin, calibration)
    timetot.append("get_image_views=" + str(image_views.time))
    disktot.append("get_image_views=" + str(image_views.size))


    voxel_grid = launch(env_1, reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)
    timetot.append("reconstruction_3d=" + str(voxel_grid.time))
    disktot.append("reconstruction_3d=" + str(voxel_grid.size))


    g = launch(env_1, graph_from_voxel_grid, voxel_grid)
    timetot.append("graph_from_voxel_grid=" + str(g.time))
    disktot.append("graph_from_voxel_grid=" + str(g.size))


    voxel_skeleton = launch(env_1, skeletonize, voxel_grid, g, subgraph=None)
    timetot.append("skeletonize=" + str(voxel_skeleton.time))
    disktot.append("skeletonize=" + str(voxel_skeleton.size))

    return voxel_skeleton, [timetot, disktot]


def workflowB(plant, env_1):
    """
       Execute the 4 frist fragment of the WF phenomenal on plant
       :param plant: a list of [ plant_snapshot, calibration ] FOR ONE PLANT
       :param env_1 : an object that containt the informations about : the cache, the index, the irods_sess, and the plant name for the GET_ID
       :return:
    """
    timetot = []
    disktot = []
    images_bin = launch(env_1, binarize, plant[0])
    timetot.append("binarize=" + str(images_bin.time))
    disktot.append("binarize=" + str(images_bin.size))


    calibration = plant[1]
    image_views = launch(env_1, get_image_views, images_bin, calibration)
    timetot.append("get_image_views=" + str(image_views.time))
    disktot.append("get_image_views=" + str(image_views.size))


    voxel_grid = launch(env_1, reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)
    timetot.append("reconstruction_3d=" + str(voxel_grid.time))
    disktot.append("reconstruction_3d=" + str(voxel_grid.size))


    g = launch(env_1, graph_from_voxel_grid, voxel_grid)
    timetot.append("graph_from_voxel_grid=" + str(g.time))
    disktot.append("graph_from_voxel_grid=" + str(g.size))


    voxel_skeleton = launch(env_1, skeletonize, voxel_grid, g, subgraph=None)
    timetot.append("skeletonize=" + str(voxel_skeleton.time))
    disktot.append("skeletonize=" + str(voxel_skeleton.size))


    side_image_projection = launch(env_1, get_side_image_projection_list, images_bin, calibration)
    timetot.append("get_side_image_projection_list=" + str(side_image_projection.time))
    disktot.append("get_side_image_projection_list=" + str(side_image_projection.size))


    # side_image_projection = launch_only_exec(timer, plant_name, irods_session, cache_path, index, get_side_image_projection_list, images_bin, calibration)

    vs = launch(env_1, segment_reduction, voxel_skeleton, side_image_projection, required_visible=4, nb_min_pixel=100)
    timetot.append("segment_reduction=" + str(vs.time))
    disktot.append("segment_reduction=" + str(vs.size))


    return vs, [timetot, disktot]


def workflowC(plant, env_1):
    """
       Execute the WF phenomenal on plant
       :param plant: a list of [ plant_snapshot, calibration ] FOR ONE PLANT
       :param env_1 : an object that containt the informations about : the cache, the index, the irods_sess, and the plant name for the GET_ID
       :return:
    """
    timetot = []
    disktot = []
    images_bin = launch(env_1, binarize, plant[0])
    timetot.append("binarize=" + str(images_bin.time))
    disktot.append("binarize=" + str(images_bin.size))


    calibration = plant[1]
    image_views = launch(env_1, get_image_views, images_bin, calibration)
    timetot.append("get_image_views=" + str(image_views.time))
    disktot.append("get_image_views=" + str(image_views.size))


    voxel_grid = launch(env_1, reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)
    timetot.append("reconstruction_3d=" + str(voxel_grid.time))
    disktot.append("reconstruction_3d=" + str(voxel_grid.size))


    g = launch(env_1, graph_from_voxel_grid, voxel_grid)
    timetot.append("graph_from_voxel_grid=" + str(g.time))
    disktot.append("graph_from_voxel_grid=" + str(g.size))


    voxel_skeleton = launch(env_1, skeletonize, voxel_grid, g, subgraph=None)
    timetot.append("skeletonize=" + str(voxel_skeleton.time))
    disktot.append("skeletonize=" + str(voxel_skeleton.size))


    side_image_projection = launch(env_1, get_side_image_projection_list, images_bin, calibration)
    timetot.append("get_side_image_projection_list=" + str(side_image_projection.time))
    disktot.append("get_side_image_projection_list=" + str(side_image_projection.size))


    vs = launch(env_1, segment_reduction, voxel_skeleton, side_image_projection, required_visible=4, nb_min_pixel=100)
    timetot.append("segment_reduction=" + str(vs.time))
    disktot.append("segment_reduction=" + str(vs.size))


    vms = launch(env_1, maize_segmentation, vs, g)
    timetot.append("maize_segmentation=" + str(vms.time))
    disktot.append("maize_segmentation=" + str(vms.size))


    maize_segmented = launch(env_1, maize_analysis, vms)
    timetot.append("maize_analysis=" + str(maize_segmented.time))
    disktot.append("maize_analysis=" + str(maize_segmented.size))


    return maize_segmented, [timetot, disktot]
