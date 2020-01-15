import zmq
from zmq import ssh
from os.path import expanduser, join
import pexpect
from openalea.core.pkgmanager import PackageManager
import dill

from openalea.distributed.zmq.worker_config import NB_WORKER, BROKER_ADDR, PKG, WF

def worker_task(ident, broker_addr, package, wf):
    ######################""
    pkg = PackageManager()
    pkg.init()
    wf_factory = pkg[package][wf]
    wf = wf_factory.instantiate()
    wf.eval_algo = "FragmentEvaluation"

    ##############################""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    socket.connect(broker_addr)

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
        

def start(task, *args):
    process = multiprocessing.Process(target=task, args=args)
    process.daemon = True
    process.start()


def start_workers(nb_workers=NB_WORKER, broker_addr=BROKER_ADDR, package=PKG, wf=WF):
    for i in range(nb_workers):
        start(worker_task, i, nb_workers, broker_addr, package, wf)


