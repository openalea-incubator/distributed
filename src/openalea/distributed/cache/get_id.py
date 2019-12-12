import hashlib
import joblib



#TODO prendre en compte les arguments  || NOT WORKING
def get_id(func, args):
    return hashlib.md5(func.__name__ + repr(args)).hexdigest()


    # return hashlib.md5(func.__name__).hexdigest()

#TODO: take into account parameters
def get_id2(func, *args, **kwargs):
    if func.__name__ == "binarize":
        return hashlib.md5(func.__name__ + joblib.hash(args[0][0], hash_name='md5')).hexdigest()
    if func.__name__ == "reconstruction_3d":
    #TODO: change the ImageView class to add __hash__ method
        h_img_v = [x.__hash__() for x in args[0][0]]
        return hashlib.md5(func.__name__ + str(hash(tuple(h_img_v)))).hexdigest()
    if func.__name__ == "graph_from_voxel_grid":
    #TODO: change the VoxelGrid class to add a __hash__() method
        return hashlib.md5(func.__name__ + args[0][0].__hash__()).hexdigest()
    if func.__name__ == "skeletonize":
        return hashlib.md5(func.__name__ + args[0][0].__hash__() + joblib.hash(args[0][1], hash_name='md5')).hexdigest()
    if func.__name__ == "get_side_image_projection_list":
        return hashlib.md5(func.__name__ + joblib.hash(args[0][0], hash_name='md5')).hexdigest()
    #TODO: change VOxelSegment to add __hash__ and VoxelSkeleton
    if func.__name__ == "segment_reduction":
        return hashlib.md5(func.__name__ + str(args[0][0].__hash__())).hexdigest()
    if func.__name__ == "maize_segmentation":
        return hashlib.md5(func.__name__ + str(args[0][0].__hash__()) + joblib.hash(args[0][1], hash_name='md5')).hexdigest()
    if func.__name__ == "maize_analysis":
        return hashlib.md5(func.__name__ + joblib.hash(args[0][0], hash_name='md5') ).hexdigest()


#TODO: take into account parameters
def get_id3(plant_name, func, *args, **kwargs):
    return hashlib.md5(func.__name__ + plant_name).hexdigest()


def try_id(func, *args, **kwargs):
    # retreive intermediate data if it exist and execute the act otherwise
    id_task = get_id2(func, args, kwargs)
    print id_task
    return id_task
