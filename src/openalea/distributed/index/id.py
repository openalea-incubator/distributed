import hashlib
import joblib
from openalea.distributed.execution.data import Data


# TODO prendre en compte les arguments  || NOT WORKING
def get_id(func, args):
    return hashlib.md5(func.__name__ + repr(args)).hexdigest()


# TODO: take into account parameters
def get_id_pname(plant_name, func):
    return hashlib.md5(func.__name__ + plant_name).hexdigest()


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


def check_task_id(func, *args, **kwargs):
    new_data = Data(id=None, value=None, parents=args, func_name=str(func.__name__))
    return set_id(new_data)


##############################################################################################
# new functions
##############################################################################################

# TODO: unique by execution? or By vid
def get_data_id(vid, pid, execution_id):
    """
    Get the generated id of the data produced by the node (vid, pid) on the execution id
    """
    did = "("+str(vid)+","+str(pid),","+str(execution_id)
    return did