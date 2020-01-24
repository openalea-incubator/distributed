
import zmq
from zmq import ssh
from os.path import expanduser, join
import os
import pexpect
from openalea.core.pkgmanager import PackageManager
import dill
from sshtunnel import SSHTunnelForwarder
from pymongo.errors import ConnectionFailure
from sshtunnel import BaseSSHTunnelForwarderError
from os.path import expanduser
import multiprocessing

from openalea.distributed.zmq.worker_config import (NB_WORKER, BROKER_ADDR, PKG, WF,
                                                    BROKER_PORT, EVALUATION)
from openalea.distributed.cloud_infos.cloud_infos import SSH_PKEY


def worker_task_classicexec(ident, broker_port, broker_addr, package, wf, ssh_pkey):
    ######################""
    # pkg = PackageManager()
    # pkg.init()
    # wf_factory = pkg[package][wf]
    # wf = wf_factory.instantiate()
    # wf.eval_algo = "BrutEvaluation"
    import dill
    import os
    import ast
    import cv2
    import csv
    import sys
    import errno
    import dill
    import uuid

    import hashlib
    import joblib
    import time
    import pandas
    from collections import defaultdict
    from openalea.distributed.metadata.data_size import getsize

    from openalea.distributed.execution.data import Data, set_id, get_task_id

    # load input raw
    from openalea.phenomenal.data.data import raw_images, calibrations

    # phenomenal analysis imprts,  
    from openalea.phenomenal_wralea.phenoarch import \
    (get_image_views, binarize, get_side_image_projection_list)
    # from openalea.phenomenal.display import show_segmentation
    from openalea.phenomenal.multi_view_reconstruction import reconstruction_3d
    from openalea.phenomenal.mesh import meshing
    from openalea.phenomenal.segmentation import \
    (maize_segmentation, graph_from_voxel_grid, segment_reduction, skeletonize,
    maize_analysis)
    ##############################""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    if str(broker_addr) == "localhost":
        socket.connect("tcp://"+str(broker_addr)+":"+str(broker_port))
    else:
        server = start_sshtunnel(broker_addr=broker_addr, broker_port=broker_port, ssh_pkey=ssh_pkey)
        socket.connect("tcp://127.0.0.1:"+str(server.local_bind_port))
        print("Worker-{} successfully connected to broker".format(ident).encode("ascii"))

    # Tell broker we're ready for work
    socket.send(b"READY")

    # Do work
    while True:
        address, empty, request = socket.recv_multipart()
    #     print("{}: {}".format(socket.identity.decode("ascii"),
    #                           request.decode("ascii")))
        
        request = dill.loads(request)
        num_p = request.get("num_plant", 0)

        

        def save_infos(prov=None, data=None, wf=0):
            item = {
            "task_id":str(data.id),
            "cpu_time":data.cpu_time,
            "dltime":data.dltime,
            "n_input":data.n_input,
            "n_output":data.n_output,
            "node":data.node,
            "inputs":str(data.inputs),
            "outputs":str(data.outputs),
            "workflow":str(wf),
            }

            prov.add_task_item(item)
            
        #####     execute wrapper func   ####"""""""""""""""""""""""""""""""""""""""""""
        def execute(func, *args, **kwargs):
            values = []
            for data in args:
                if isinstance(data, Data):
                    workflow = data.workflow
                    values.append(data.value)
                else:
                    values.append(data)
            new_val = func(*values, **kwargs)
            new_parents = []
            for parent in args:
                if isinstance(parent, Data):
                    new_parents.append(Data(id=parent.id, parents=parent.parents, func_name=parent.func_name))
                else:
                    new_parents.append(parent)
            new_data = Data(id=None, value=new_val, parents=new_parents, 
                            func_name=str(func.__name__), workflow=workflow)
            set_id(new_data)
            return new_data


        def wrapper_data(func, *args, **kwargs):
            start = time.time()
            # retreive intermediate data if it exist and execute the act otherwise
            id_task = get_task_id(func, *args, **kwargs)

            dname = hashlib.md5(id_task).hexdigest()
        #     cache_data_path = os.path.join(cp.cache_dir, "data", dname)

            # download data from refs:
            for data in args:
                if isinstance(data, Data):
                    if not data.value:
                        try:
                            path_to_data = index.is_in(data.id)
                            tmp_data = load_intermediate_data(dname=None, data_path=path_to_data, method=cp.method,                                                           sftp_client=cc.sftp_client)
                            data.value = tmp_data.value
                        except:
                            pass
            # Compute
            print("COMPUTE DATA")
            tmp_data = execute(func, *args, **kwargs)

            dtime = time.time()

        #     dsize=0
            dsize = getsize(tmp_data.value)
            tmp_data.size = dsize
            tmp_data.time = time.time() - start
            tmp_data.cpu_time = dtime - start
            tmp_data.dltime = time.time() - dtime
            return tmp_data

        print("start execution ...")
        start = time.time()

        try:
            # Connect to ProvDB
            from openalea.distributed.provenance.provenanceDB import start_provdb
            provdb = start_provdb(provenance_type="NoOpenAlea")

            # Connect to IndexDB
            from openalea.distributed.index.indexDB import start_index
            index = start_index()

            # Start the Data:
            VAR_THAT_CHANGE_IDs = num_p
            rawdata = raw_images()
            t1 = time.time() - start
            data_plant = Data(id=str(VAR_THAT_CHANGE_IDs), value=rawdata, dltime=t1, workflow=VAR_THAT_CHANGE_IDs)
            data_plant.dltime=t1
            save_infos(prov=provdb, data=data_plant, wf=VAR_THAT_CHANGE_IDs)
            calibration = calibrations()
            images_bin = wrapper_data(binarize, data_plant)
            save_infos(prov=provdb, data=images_bin, wf=VAR_THAT_CHANGE_IDs)
            image_views = wrapper_data(get_image_views, images_bin, calibration)
            save_infos(prov=provdb, data=image_views, wf=VAR_THAT_CHANGE_IDs)
            voxel_grid = wrapper_data(reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)
            save_infos(prov=provdb, data=image_views, wf=VAR_THAT_CHANGE_IDs)
            g = wrapper_data(graph_from_voxel_grid, voxel_grid)
            save_infos(prov=provdb, data=g, wf=VAR_THAT_CHANGE_IDs)
            voxel_skeleton = wrapper_data(skeletonize, voxel_grid, g, subgraph=None)
            save_infos(prov=provdb, data=voxel_skeleton, wf=VAR_THAT_CHANGE_IDs)
            side_image_projection = wrapper_data(get_side_image_projection_list, images_bin, calibration)
            save_infos(prov=provdb, data=side_image_projection, wf=VAR_THAT_CHANGE_IDs)
            vs = wrapper_data(segment_reduction, voxel_skeleton, side_image_projection, required_visible=4, nb_min_pixel=100)
            save_infos(prov=provdb, data=vs, wf=VAR_THAT_CHANGE_IDs)
            vms = wrapper_data(maize_segmentation, vs, g)
            save_infos(prov=provdb, data=vms, wf=VAR_THAT_CHANGE_IDs)
            maize_segmented = wrapper_data(maize_analysis, vms)
            save_infos(prov=provdb, data=maize_segmented, wf=VAR_THAT_CHANGE_IDs)

            end = time.time()
            print("execution over")
            item = {"id":str(VAR_THAT_CHANGE_IDs),
                "workflow":uuid.uuid4(),
                "time_init":start,
                "time_end":end,
                "data":"",
                "parameters":"",
                "executions":""}
            provdb.add_wf_item(item)

            socket.send_multipart([address, b"", b"success"])
                    
        except:
            socket.send_multipart([address, b"", b"fail"])


def worker_task_greedyexec(ident, broker_port, broker_addr, package, wf, ssh_pkey):
    ######################""
    # pkg = PackageManager()
    # pkg.init()
    # wf_factory = pkg[package][wf]
    # wf = wf_factory.instantiate()
    # wf.eval_algo = "BrutEvaluation"
    import dill
    import os
    import ast
    import cv2
    import csv
    import sys
    import errno
    import dill
    import uuid

    import hashlib
    import joblib
    import time
    import pandas
    from collections import defaultdict
    from openalea.distributed.metadata.data_size import getsize

    from openalea.distributed.execution.data import Data, set_id, get_task_id
    from openalea.distributed.data.data_manager import write_intermediate_data_local, load_intermediate_data_local
    # load input raw
    from openalea.phenomenal.data.data import raw_images, calibrations

    # phenomenal analysis imprts,  
    from openalea.phenomenal_wralea.phenoarch import \
    (get_image_views, binarize, get_side_image_projection_list)
    # from openalea.phenomenal.display import show_segmentation
    from openalea.phenomenal.multi_view_reconstruction import reconstruction_3d
    from openalea.phenomenal.mesh import meshing
    from openalea.phenomenal.segmentation import \
    (maize_segmentation, graph_from_voxel_grid, segment_reduction, skeletonize,
    maize_analysis)
    ##############################""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    if str(broker_addr) == "localhost":
        socket.connect("tcp://"+str(broker_addr)+":"+str(broker_port))
    else:
        server = start_sshtunnel(broker_addr=broker_addr, broker_port=broker_port, ssh_pkey=ssh_pkey)
        socket.connect("tcp://127.0.0.1:"+str(server.local_bind_port))
        print("Worker-{} successfully connected to broker".format(ident).encode("ascii"))

    # Tell broker we're ready for work
    socket.send(b"READY")

    # Do work
    while True:
        address, empty, request = socket.recv_multipart()
    #     print("{}: {}".format(socket.identity.decode("ascii"),
    #                           request.decode("ascii")))
        
        request = dill.loads(request)
        num_p = request.get("num_plant", 0)

        def save_infos(prov=None, data=None, wf=0):
            item = {
            "task_id":str(data.id),
            "cpu_time":data.cpu_time,
            "dltime":data.dltime,
            "n_input":data.n_input,
            "n_output":data.n_output,
            "node":data.node,
            "inputs":str(data.inputs),
            "outputs":str(data.outputs),
            "workflow":str(wf),
            }
            prov.add_task_item(item)
            
        #####     execute wrapper func   ####"""""""""""""""""""""""""""""""""""""""""""
        def execute(func, *args, **kwargs):
            values = []
            for data in args:
                if isinstance(data, Data):
                    workflow = data.workflow
                    values.append(data.value)
                else:
                    values.append(data)
            new_val = func(*values, **kwargs)
            new_parents = []
            for parent in args:
                if isinstance(parent, Data):
                    new_parents.append(Data(id=parent.id, parents=parent.parents, func_name=parent.func_name))
                else:
                    new_parents.append(parent)
            new_data = Data(id=None, value=new_val, parents=new_parents, 
                            func_name=str(func.__name__), workflow=workflow)
            set_id(new_data)
            return new_data


        def wrapper_data(index, func, *args, **kwargs):
            start = time.time()
            # retreive intermediate data if it exist and execute the act otherwise
            id_task = get_task_id(func, *args, **kwargs)

            dname = hashlib.md5(id_task).hexdigest()
        #     cache_data_path = os.path.join(cp.cache_dir, "data", dname)

            intermediate_data_path = index.is_in(id_task)
            if intermediate_data_path:
                # GIVE REF! Not data
                print("LOAD DATA")
                dtime = time.time()
                tmp_data = Data(id=id_task, value=None)
                dsize = 0
            else:
                # download data from refs:
                for data in args:
                    if isinstance(data, Data):
                        if not data.value:
                            try:
                                path_to_data = index.is_in(data.id)
                                tmp_data = load_intermediate_data_local(data_path=path_to_data)
                                data.value = tmp_data.value
                            except:
                                pass
                            
                    # Compute
                    print("COMPUTE DATA")
                    tmp_data = execute(func, *args, **kwargs)
                    dtime = time.time()

                    print('ADD to cache')
                    # get name of file: 
                    pathcache = "/home/ubuntu/openalea/"
                    dname = hashlib.md5(str(data.id)).hexdigest()
                    pathcache = os.path.join(pathcache, dname)
                    write_intermediate_data_local(tmp_data, pathcache)
                    index.add_data(data_id=str(data.id), path=pathcache)

        #     dsize=0
            dsize = getsize(tmp_data.value)
            tmp_data.size = dsize
            tmp_data.time = time.time() - start
            tmp_data.cpu_time = dtime - start
            tmp_data.dltime = time.time() - dtime
            return tmp_data

        print("start execution ...")
        start = time.time()

        try:
            # Connect to ProvDB
            from openalea.distributed.provenance.provenanceDB import start_provdb
            provdb = start_provdb(provenance_type="NoOpenAlea")

            # Connect to IndexDB
            from openalea.distributed.index.indexDB import start_index
            index = start_index()

            # Start the Data:
            VAR_THAT_CHANGE_IDs = num_p
            rawdata = raw_images()
            t1 = time.time() - start
            data_plant = Data(id=str(VAR_THAT_CHANGE_IDs), value=rawdata, dltime=t1, workflow=VAR_THAT_CHANGE_IDs)
            save_infos(prov=provdb, data=data_plant, wf=VAR_THAT_CHANGE_IDs)
            calibration = calibrations()
            images_bin = wrapper_data(index, binarize, data_plant)
            save_infos(prov=provdb, data=images_bin, wf=VAR_THAT_CHANGE_IDs)
            image_views = wrapper_data(index, get_image_views, images_bin, calibration)
            save_infos(prov=provdb, data=image_views, wf=VAR_THAT_CHANGE_IDs)
            voxel_grid = wrapper_data(index, reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)
            save_infos(prov=provdb, data=image_views, wf=VAR_THAT_CHANGE_IDs)
            g = wrapper_data(index, graph_from_voxel_grid, voxel_grid)
            save_infos(prov=provdb, data=g, wf=VAR_THAT_CHANGE_IDs)
            voxel_skeleton = wrapper_data(index, skeletonize, voxel_grid, g, subgraph=None)
            save_infos(prov=provdb, data=voxel_skeleton, wf=VAR_THAT_CHANGE_IDs)
            side_image_projection = wrapper_data(index, get_side_image_projection_list, images_bin, calibration)
            save_infos(prov=provdb, data=side_image_projection, wf=VAR_THAT_CHANGE_IDs)
            vs = wrapper_data(index, segment_reduction, voxel_skeleton, side_image_projection, required_visible=4, nb_min_pixel=100)
            save_infos(prov=provdb, data=vs, wf=VAR_THAT_CHANGE_IDs)
            vms = wrapper_data(index, maize_segmentation, vs, g)
            save_infos(prov=provdb, data=vms, wf=VAR_THAT_CHANGE_IDs)
            maize_segmented = wrapper_data(index, maize_analysis, vms)
            save_infos(prov=provdb, data=maize_segmented, wf=VAR_THAT_CHANGE_IDs)

            end = time.time()
            print("execution over")
            item = {"id":str(VAR_THAT_CHANGE_IDs),
                "workflow":uuid.uuid4(),
                "time_init":start,
                "time_end":end,
                "data":"",
                "parameters":"",
                "executions":""}
            provdb.add_wf_item(item)

            socket.send_multipart([address, b"", b"success"])
                    
        except:
            socket.send_multipart([address, b"", b"fail"])



def worker_task_fakeexec(ident, broker_port, broker_addr, package, wf, ssh_pkey):
    ######################""
    # pkg = PackageManager()
    # pkg.init()
    # wf_factory = pkg[package][wf]
    # wf = wf_factory.instantiate()
    # wf.eval_algo = "BrutEvaluation"
    import dill


    ##############################""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    if str(broker_addr) == "localhost":
        socket.connect("tcp://"+str(broker_addr)+":"+str(broker_port))
    else:
        server = start_sshtunnel(broker_addr=broker_addr, broker_port=broker_port, ssh_pkey=ssh_pkey)
        socket.connect("tcp://127.0.0.1:"+str(server.local_bind_port))
        print("Worker-{} successfully connected to broker".format(ident).encode("ascii"))

    # Tell broker we're ready for work
    socket.send(b"READY")

    # Do work
    while True:
        address, empty, request = socket.recv_multipart()
    #     print("{}: {}".format(socket.identity.decode("ascii"),
    #                           request.decode("ascii")))
        
        request = dill.loads(request)
        num_p = request.get("num_plant", 0)

        import os
        import ast
        import cv2
        import csv
        import sys
        import errno
        import dill

        import hashlib
        import joblib
        import time
        import pandas
        from collections import defaultdict
        from openalea.distributed.metadata.data_size import getsize

        from openalea.distributed.execution.data import Data, set_id, get_task_id

        # load input raw
        from openalea.phenomenal.data.data import raw_images, calibrations

        # phenomenal analysis imprts,  
        from openalea.phenomenal_wralea.phenoarch import \
        (get_image_views, binarize, get_side_image_projection_list)
        # from openalea.phenomenal.display import show_segmentation
        from openalea.phenomenal.multi_view_reconstruction import reconstruction_3d
        from openalea.phenomenal.mesh import meshing
        from openalea.phenomenal.segmentation import \
        (maize_segmentation, graph_from_voxel_grid, segment_reduction, skeletonize,
        maize_analysis)


        def save_infos(prov=None, data=None, wf=0):
            item = {
            "task_id":str(data.id),
            "cpu_time":data.cpu_time,
            "dltime":data.dltime,
            "n_input":data.n_input,
            "n_output":data.n_output,
            "node":data.node,
            "inputs":str(data.inputs),
            "outputs":str(data.outputs),
            "workflow":str(wf),
            }
            prov.add_task_item(item)
            
        #####     execute wrapper func   ####"""""""""""""""""""""""""""""""""""""""""""
        def execute(func, *args, **kwargs):
            values = []
            for data in args:
                if isinstance(data, Data):
                    workflow = data.workflow
                    values.append(data.value)
                else:
                    values.append(data)
            # new_val = func(*values, **kwargs)
            new_val=0
            new_parents = []
            for parent in args:
                if isinstance(parent, Data):
                    new_parents.append(Data(id=parent.id, parents=parent.parents, func_name=parent.func_name))
                else:
                    new_parents.append(parent)
            new_data = Data(id=None, value=new_val, parents=new_parents, 
                            func_name=str(func.__name__), workflow=workflow)
            set_id(new_data)
            return new_data


        def wrapper_data(func, *args, **kwargs):
            start = time.time()
            # retreive intermediate data if it exist and execute the act otherwise
            id_task = get_task_id(func, *args, **kwargs)

            dname = hashlib.md5(id_task).hexdigest()
        #     cache_data_path = os.path.join(cp.cache_dir, "data", dname)

            # download data from refs:
            for data in args:
                if isinstance(data, Data):
                    if not data.value:
                        try:
                            path_to_data = index.is_in(data.id)
                            tmp_data = load_intermediate_data(dname=None, data_path=path_to_data, method=cp.method,                                                           sftp_client=cc.sftp_client)
                            data.value = tmp_data.value
                        except:
                            pass
            # Compute
            print("COMPUTE DATA")
            tmp_data = execute(func, *args, **kwargs)

            dtime = time.time()

        #     dsize=0
            dsize = getsize(tmp_data.value)
            tmp_data.size = dsize
            tmp_data.time = time.time() - start
            tmp_data.cpu_time = dtime - start
            tmp_data.dltime = time.time() - dtime
            return tmp_data

        print("start execution ...")
        start = time.time()



        try:
            # Connect to ProvDB
            from openalea.distributed.provenance.provenanceDB import start_provdb
            provdb = start_provdb(provenance_type="NoOpenAlea")

            # Connect to IndexDB
            from openalea.distributed.index.indexDB import start_index
            index = start_index()

            # Start the Data:
            time.sleep(5)
            VAR_THAT_CHANGE_IDs = num_p
            rawdata = raw_images()
            t1 = time.time() - start
            data_plant = Data(id=str(VAR_THAT_CHANGE_IDs), value=rawdata, dltime=t1, workflow=VAR_THAT_CHANGE_IDs)
            data_plant.dltime=t1
            save_infos(prov=provdb, data=data_plant, wf=VAR_THAT_CHANGE_IDs)
            calibration = calibrations()
            images_bin = wrapper_data(binarize, data_plant)
            save_infos(prov=provdb, data=images_bin, wf=VAR_THAT_CHANGE_IDs)
            image_views = wrapper_data(get_image_views, images_bin, calibration)
            save_infos(prov=provdb, data=image_views, wf=VAR_THAT_CHANGE_IDs)
            voxel_grid = wrapper_data(reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)
            save_infos(prov=provdb, data=image_views, wf=VAR_THAT_CHANGE_IDs)
            g = wrapper_data(graph_from_voxel_grid, voxel_grid)
            save_infos(prov=provdb, data=g, wf=VAR_THAT_CHANGE_IDs)
            voxel_skeleton = wrapper_data(skeletonize, voxel_grid, g, subgraph=None)
            save_infos(prov=provdb, data=voxel_skeleton, wf=VAR_THAT_CHANGE_IDs)
            side_image_projection = wrapper_data(get_side_image_projection_list, images_bin, calibration)
            save_infos(prov=provdb, data=side_image_projection, wf=VAR_THAT_CHANGE_IDs)
            vs = wrapper_data(segment_reduction, voxel_skeleton, side_image_projection, required_visible=4, nb_min_pixel=100)
            save_infos(prov=provdb, data=vs, wf=VAR_THAT_CHANGE_IDs)
            vms = wrapper_data(maize_segmentation, vs, g)
            save_infos(prov=provdb, data=vms, wf=VAR_THAT_CHANGE_IDs)
            maize_segmented = wrapper_data(maize_analysis, vms)
            save_infos(prov=provdb, data=maize_segmented, wf=VAR_THAT_CHANGE_IDs)

            end = time.time()
            print("execution over")
            item = {"id":str(VAR_THAT_CHANGE_IDs),
                "workflow":str(VAR_THAT_CHANGE_IDs),
                "time_init":start,
                "time_end":end,
                "data":None,
                "parameters":None,
                "executions":None}
            provdb.add_wf_item(item)

            socket.send_multipart([address, b"", b"success"])
                    
        except:
            socket.send_multipart([address, b"", b"fail"])


def worker_task_fakeload(ident, broker_port, broker_addr, package, wf, ssh_pkey):
    ######################""
    # pkg = PackageManager()
    # pkg.init()
    # wf_factory = pkg[package][wf]
    # wf = wf_factory.instantiate()
    # wf.eval_algo = "BrutEvaluation"
    import dill


    ##############################""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    if str(broker_addr) == "localhost":
        socket.connect("tcp://"+str(broker_addr)+":"+str(broker_port))
    else:
        server = start_sshtunnel(broker_addr=broker_addr, broker_port=broker_port, ssh_pkey=ssh_pkey)
        socket.connect("tcp://127.0.0.1:"+str(server.local_bind_port))
        print("Worker-{} successfully connected to broker".format(ident).encode("ascii"))

    # Tell broker we're ready for work
    socket.send(b"READY")

    # Do work
    while True:
        address, empty, request = socket.recv_multipart()
    #     print("{}: {}".format(socket.identity.decode("ascii"),
    #                           request.decode("ascii")))
        
        request = dill.loads(request)
        num_p = request.get("num_plant", 0)

        import os
        import ast
        import cv2
        import csv
        import sys
        import errno
        import dill

        import hashlib
        import joblib
        import time
        import pandas
        from collections import defaultdict
        from openalea.distributed.metadata.data_size import getsize

        from openalea.distributed.execution.data import Data, set_id, get_task_id
        from openalea.distributed.data.data_manager import write_intermediate_data_local, load_intermediate_data_local

        # load input raw
        from openalea.phenomenal.data.data import raw_images, calibrations

        # phenomenal analysis imprts,  
        from openalea.phenomenal_wralea.phenoarch import \
        (get_image_views, binarize, get_side_image_projection_list)
        # from openalea.phenomenal.display import show_segmentation
        from openalea.phenomenal.multi_view_reconstruction import reconstruction_3d
        from openalea.phenomenal.mesh import meshing
        from openalea.phenomenal.segmentation import \
        (maize_segmentation, graph_from_voxel_grid, segment_reduction, skeletonize,
        maize_analysis)


        def save_infos(prov=None, data=None, wf=0):
            item = {
            "task_id":str(data.id),
            "cpu_time":data.cpu_time,
            "dltime":data.dltime,
            "n_input":data.n_input,
            "n_output":data.n_output,
            "node":data.node,
            "inputs":str(data.inputs),
            "outputs":str(data.outputs),
            "workflow":str(wf),
            }
            prov.add_task_item(item)
            
        #####     execute wrapper func   ####"""""""""""""""""""""""""""""""""""""""""""
        def execute(func, *args, **kwargs):
            values = []
            for data in args:
                if isinstance(data, Data):
                    workflow = data.workflow
                    values.append(data.value)
                else:
                    values.append(data)
            # new_val = func(*values, **kwargs)
            new_val=0
            new_parents = []
            for parent in args:
                if isinstance(parent, Data):
                    new_parents.append(Data(id=parent.id, parents=parent.parents, func_name=parent.func_name))
                else:
                    new_parents.append(parent)
            new_data = Data(id=None, value=new_val, parents=new_parents, 
                            func_name=str(func.__name__), workflow=workflow)
            set_id(new_data)
            return new_data


        def wrapper_data(func, *args, **kwargs):
            start = time.time()
            # retreive intermediate data if it exist and execute the act otherwise
            id_task = get_task_id(func, *args, **kwargs)

            dname = hashlib.md5(id_task).hexdigest()
        #     cache_data_path = os.path.join(cp.cache_dir, "data", dname)
            intermediate_data_path = index.is_in(id_task)
            if intermediate_data_path:
                # GIVE REF! Not data
                print("LOAD DATA")
                dtime = time.time()
                tmp_data = Data(id=id_task, value=None)
                dsize = 0
            else:
                # download data from refs:
                for data in args:
                    if isinstance(data, Data):
                        if not data.value:
                            try:
                                path_to_data = index.is_in(data.id)
                                tmp_data = load_intermediate_data_local(data_path=path_to_data)
                                data.value = tmp_data.value
                            except:
                                pass
                            
                    # Compute
                    print("COMPUTE DATA")
                    tmp_data = execute(func, *args, **kwargs)
                    dtime = time.time()

                print('ADD to cache')
                # get name of file: 
                pathcache = "/home/ubuntu/openalea/"
                dname = hashlib.md5(str(data.id)).hexdigest()
                pathcache = os.path.join(pathcache, dname)
                write_intermediate_data_local(tmp_data, pathcache)
                index.add_data(data_id=str(data.id), path=pathcache)

        #     dsize=0
            dsize = getsize(tmp_data.value)
            tmp_data.size = dsize
            tmp_data.time = time.time() - start
            tmp_data.cpu_time = dtime - start
            tmp_data.dltime = time.time() - dtime
            return tmp_data
        print("start execution ...")
        start = time.time()



        try:
            # Connect to ProvDB
            from openalea.distributed.provenance.provenanceDB import start_provdb
            provdb = start_provdb(provenance_type="NoOpenAlea")

            # Connect to IndexDB
            from openalea.distributed.index.indexDB import start_index
            index = start_index()

            # Start the Data:
            time.sleep(1)
            VAR_THAT_CHANGE_IDs = num_p
            rawdata = raw_images()
            t1 = time.time() - start
            data_plant = Data(id=str(VAR_THAT_CHANGE_IDs), value=rawdata, dltime=t1, workflow=VAR_THAT_CHANGE_IDs)
            save_infos(prov=provdb, data=data_plant, wf=VAR_THAT_CHANGE_IDs)
            calibration = calibrations()
            images_bin = wrapper_data(index, binarize, data_plant)
            save_infos(prov=provdb, data=images_bin, wf=VAR_THAT_CHANGE_IDs)
            image_views = wrapper_data(index, get_image_views, images_bin, calibration)
            save_infos(prov=provdb, data=image_views, wf=VAR_THAT_CHANGE_IDs)
            voxel_grid = wrapper_data(index, reconstruction_3d, image_views, voxels_size=4, start_voxel_size=4096)
            save_infos(prov=provdb, data=image_views, wf=VAR_THAT_CHANGE_IDs)
            g = wrapper_data(index, graph_from_voxel_grid, voxel_grid)
            save_infos(prov=provdb, data=g, wf=VAR_THAT_CHANGE_IDs)
            voxel_skeleton = wrapper_data(index, skeletonize, voxel_grid, g, subgraph=None)
            save_infos(prov=provdb, data=voxel_skeleton, wf=VAR_THAT_CHANGE_IDs)
            side_image_projection = wrapper_data(index, get_side_image_projection_list, images_bin, calibration)
            save_infos(prov=provdb, data=side_image_projection, wf=VAR_THAT_CHANGE_IDs)
            vs = wrapper_data(index, segment_reduction, voxel_skeleton, side_image_projection, required_visible=4, nb_min_pixel=100)
            save_infos(prov=provdb, data=vs, wf=VAR_THAT_CHANGE_IDs)
            vms = wrapper_data(index, maize_segmentation, vs, g)
            save_infos(prov=provdb, data=vms, wf=VAR_THAT_CHANGE_IDs)
            maize_segmented = wrapper_data(index, maize_analysis, vms)
            save_infos(prov=provdb, data=maize_segmented, wf=VAR_THAT_CHANGE_IDs)

            end = time.time()
            print("execution over")
            item = {"id":str(VAR_THAT_CHANGE_IDs),
                "workflow":str(VAR_THAT_CHANGE_IDs),
                "time_init":start,
                "time_end":end,
                "data":None,
                "parameters":None,
                "executions":None}
            provdb.add_wf_item(item)

            socket.send_multipart([address, b"", b"success"])
                    
        except:
            socket.send_multipart([address, b"", b"fail"])
