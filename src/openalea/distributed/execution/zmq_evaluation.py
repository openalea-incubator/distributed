# -*- coding: utf-8 -*-
import time
from openalea.distributed.zmq.worker import start_workers
from openalea.distributed.zmq.broker import start_broker
from openalea.distributed.zmq.client import start, client_task

#  fragments 
# frag1 = {"inputs_vid":[], "outputs_vid":[(27,0)],
#                   'cached_data':{}, 
#                    'input_data': {}}

# frag2 = {"inputs_vid":[(26,17)], "outputs_vid":[(26,0)],
#                   'cached_data':{}, 
#                    'input_data': {(26,17): (27,0)}}


# queue = [frag1, frag2]
# queue = [frag1]*10

# start_workers()
start_broker()
time.sleep(5)
# for i, frag in enumerate(queue):
#         start(client_task, i, frag, "/path/out/frag{}".format(i+1))

for i in range(1):
        start(client_task_bruteval, i, i)
