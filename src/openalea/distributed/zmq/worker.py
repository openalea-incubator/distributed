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
from openalea.distributed.zmq.worker_noopenalea import (worker_task_classicexec, worker_task_fakeexec,
worker_task_fakeload, worker_task_greedyexec)

def worker_task_fragmenteval(ident, broker_port, broker_addr, package, wf, ssh_pkey):
    ######################""
    pkg = PackageManager()
    pkg.init()
    wf_factory = pkg[package][wf]
    wf = wf_factory.instantiate()
    wf.eval_algo = "FragmentEvaluation"

    ##############################""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    if str(broker_addr) == "localhost":
        socket.connect("tcp://"+str(broker_addr)+":"+str(broker_port))
    else:
        server = start_sshtunnel(broker_addr=broker_addr, broker_port=broker_port, ssh_pkey=ssh_pkey)
        socket.connect("tcp://127.0.0.1:"+str(server.local_bind_port))

    # Tell broker we're ready for work
    socket.send(b"READY")

    # Do work
    while True:
        address, empty, request = socket.recv_multipart()
    #     print("{}: {}".format(socket.identity.decode("ascii"),
    #                           request.decode("ascii")))
        request = dill.loads(request)
        if 'output_path' in request:
            out_path = request['output_path']
        else:
            out_path = ""
        try:
            frag = request['fragment']
            for i, vid in enumerate([v[0] for v in frag['outputs_vid']]):
                wf.eval_as_expression(vtx_id=vid,
                            record_provenance=False,
                            fragment_infos=frag,
                            tmp_path=out_path)
            socket.send_multipart([address, b"", b"success"])
            
        except:
            socket.send_multipart([address, b"", b"fail"])
        
    
def worker_task_bruteval(ident, broker_port, broker_addr, package, wf, ssh_pkey):
    ######################""
    pkg = PackageManager()
    pkg.init()
    wf_factory = pkg[package][wf]
    wf = wf_factory.instantiate()
    wf.eval_algo = "BrutEvaluation"
    # home = expanduser("~")
    # wfpath = os.path.join(home, "workflow")
    # with open(wfpath, "r") as f:
    #     wf = dill.load(f)

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
        
        try:
            wf.node(33).set_input(0, num_p)
            wf.eval(record_provenance=True)
            socket.send_multipart([address, b"", b"success"])
            
        except:
            socket.send_multipart([address, b"", b"fail"])

        
def start(task, *args):
    process = multiprocessing.Process(target=task, args=args)
    process.daemon = True
    process.start()


def start_workers(vm_id = "", type_evaluation= EVALUATION, nb_workers=NB_WORKER, broker_addr=BROKER_ADDR, package=PKG, 
                    broker_port=BROKER_PORT, wf=WF, ssh_pkey=SSH_PKEY):
    print("Starting ", nb_workers, " workers ...")
    if type_evaluation == "FragmentEvaluation":
        for i in range(nb_workers):
            start(worker_task_fragmenteval, vm_id+str(i), broker_port, broker_addr, package, wf, ssh_pkey)
    if type_evaluation == "BrutEvaluation":
        for i in range(nb_workers):
            start(worker_task_bruteval, vm_id+str(i), broker_port, broker_addr, package, wf, ssh_pkey)
    if type_evaluation == "NoOpenAlea":
        for i in range(nb_workers):
            start(worker_task_classicexec, vm_id+str(i), broker_port, broker_addr, package, wf, ssh_pkey)
    if type_evaluation == "FakeExec":
        for i in range(nb_workers):
            start(worker_task_fakeexec, vm_id+str(i), broker_port, broker_addr, package, wf, ssh_pkey)
    if type_evaluation == "FakeLoad":
        for i in range(nb_workers):
            start(worker_task_fakeload, vm_id+str(i), broker_port, broker_addr, package, wf, ssh_pkey)
    if type_evaluation == "NoOpenAleaGreedy":
        for i in range(nb_workers):
            start(worker_task_greedyexec, vm_id+str(i), broker_port, broker_addr, package, wf, ssh_pkey)


def start_sshtunnel(*args, **kwargs):
    try:
        server = SSHTunnelForwarder(
            ssh_address_or_host=kwargs["broker_addr"],
            ssh_pkey=kwargs["ssh_pkey"],
            ssh_username="ubuntu",
            remote_bind_address=("localhost", kwargs["broker_port"])
            # ,
            # *args,
            # **kwargs
        )

        server.start()
    except BaseSSHTunnelForwarderError:
        print "Fail to connect to ssh device"
    return server