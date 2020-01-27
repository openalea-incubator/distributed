import dill
import os
from openalea.distributed.index.id import get_id
import errno
from openalea.distributed.execution.data import Data


def write_data(data_id, data, path):
    """
    vid: vid of the node whose output will be stored
    path: path where the data will be stored
    """
    with open(os.path.join(path,data_id), "w") as f:
        dill.dump(data, f)

def load_data(path):
    """
    vid: vid of the node whose input will be fetched
    path: path of the data to get
    """
    with open(path, "r") as f:
        data = dill.load(f)
    return data


def check_data_to_load(vid, pid, fragment_infos):
    """
    Return the path if the data has to be loaded 
    Return None otherwise

    """
    if not fragment_infos:
        return None
    if (vid, pid) in fragment_infos['cached_data'].keys():
        # the data is get from cache 
        return fragment_infos['cached_data'][(vid, pid)]
    if (vid, pid) in fragment_infos['input_data'].keys():
        # the data is computed by other fragments
        return fragment_infos['input_data'][(vid, pid)]
    return None


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
    return os.path.getsize(data_path)

def load_intermediate_data_local(data_path):
    with open(data_path, "rb") as f:
        intermediate_data = dill.load(f)
    new_data = Data(id=os.path.basename(data_path), value=intermediate_data)
    return new_data

def write_intermediate_data_ssh(data, dname, data_path, sftp_client=None):
#     create_dir(os.path.dirname(data_path))
#     create the file on local
    from os.path import expanduser
    home = expanduser("~")
    tmp_path = os.path.join(home, data_path[5:])
    create_dir(os.path.dirname(tmp_path))
    
    dsize = write_intermediate_data_local(data, tmp_path)
#     dname = os.path.basename(data_path)
    cache_path = os.path.join("/mnt/openalea_cache/data", dname)
#     create_dir_ssh(os.path.dirname(cache_path), sftp_client)
    sftp_client.put(tmp_path, cache_path)
    
    #delete tmp_file
    if os.path.isfile(tmp_path):
        os.remove(tmp_path)
    else:  ## Show an error ##
        print("Error: %s file not found" % tmp_path)
    return dsize


def load_intermediate_data_ssh(data_path, sftp_client=None):
#     get file on local then load
    from os.path import expanduser
    home = expanduser("~")
    tmp_path = os.path.join(home, data_path[5:])
    sftp_client.get(data_path, tmp_path)
    
    new_data = load_intermediate_data_local(tmp_path)
    
    #delete tmp_file
    if os.path.isfile(tmp_path):
        os.remove(tmp_path)
    else:  ## Show an error ##
        print("Error: %s file not found" % tmp_path)

    return new_data


def write_intermediate_data(data, dname, data_path, method="local", sftp_client=None, *args, **kwargs):
    if method == "ssh":
        return write_intermediate_data_ssh(data, dname, data_path, sftp_client=sftp_client)
    if method == "local":
        return write_intermediate_data_local(data, data_path)
    if method == "sshfs":
        return write_intermediate_data_local(data, data_path)
    if method == "IRODS":
        return write_intermediate_data_irods(data, dname, data_path, irods_sess=kwargs.get("irods_sess"))
    


def load_intermediate_data(dname, data_path, method="local", sftp_client=None):
    if method == "ssh":
        return load_intermediate_data_ssh(dname, data_path, sftp_client=sftp_client)
    if method == "local":
        return load_intermediate_data_local(data_path)
    if method == "sshfs":
        return load_intermediate_data_local(data_path)
    if method == "IRODS":
        return load_intermediate_data_irods(dname, data_path, irods_sess=kwargs.get("irods_sess"))

def create_dir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


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