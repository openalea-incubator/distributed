import os
import dill
import pkg_resources
import errno

from alinea.phenoarch.cloud import copy_local_file_to_data_object, copy_data_object_to_file

from openalea.distributed.execution.data import Data


def create_dir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def write_intermediate_data_local(data, data_path):
    create_dir(os.path.dirname(data_path))
    try :
        with open(data_path, "wb") as f:
            dill.dump(data.value, f)
    except IOError as e:
        print e
    except:
        print "ERROR : value to save not a Data type"
        print "Fail to save : ", data, " to : ", data_path



def load_intermediate_data_local(data_path):
    with open(data_path, "rb") as f:
        intermediate_data = dill.load(f)
    new_data = Data(id=os.path.basename(data_path), value=intermediate_data)
    return new_data


# TODO: send to irods through memory / not after persist on local
def write_intermediate_data_irods(data, dname, cache_data_path, irods_sess=None):
    tmp_path = pkg_resources.resource_filename(
        'openalea.distributed', 'execution/tmp_data_' + dname)
    # pickle it
    write_intermediate_data_local(data, tmp_path)

    if not irods_sess.data_objects.exists(cache_data_path):
        irods_sess.data_objects.create(cache_data_path)
    obj=irods_sess.data_objects.get(cache_data_path)
    # delete the previous cache file
    with obj.open("w"):
        pass
    copy_local_file_to_data_object(tmp_path, obj)


def load_intermediate_data_irods(dname, cache_path, irods_sess=None):
    obj = irods_sess.data_objects.get(cache_path)
    tmp_path = pkg_resources.resource_filename(
            'openalea.distributed', 'execution/tmp_data_' + dname)
    # tmp_path = "/home/gaetan/OpenAlea/distributed/cache_data/tmp/tmp_data"
    #get the file from irods in a tmp file
    copy_data_object_to_file(obj, tmp_path)
    #unpickle it
    with open(tmp_path, 'rb') as f:
        res = dill.load(f)

    #delete tmp_file
    if os.path.isfile(tmp_path):
        os.remove(tmp_path)
    else:  ## Show an error ##
        print("Error: %s file not found" % tmp_path)
    return res


def write_intermediate_data(data, dname, data_path, method="IRODS", irods_sess=None):
    if method == "IRODS":
        return write_intermediate_data_irods(data, dname, data_path, irods_sess=irods_sess)

    else:
        return write_intermediate_data_local(data, data_path)



def load_intermediate_data(dname, data_path, method="IRODS", irods_sess=None):
    if method == "IRODS":
        return load_intermediate_data_irods(dname, data_path, irods_sess=irods_sess)
    else :
        return load_intermediate_data_local(data_path)



