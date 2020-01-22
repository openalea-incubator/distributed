# -*- coding: utf-8 -*-
import time
from openalea.distributed.zmq.broker import start_broker
from openalea.distributed.zmq.client import start, client_task_bruteval

#  fragments 


# start_workers()
print("Start a load ballacing broker ...")
start_broker()
time.sleep(5)
print("Broker  started")

# for i, frag in enumerate(queue):
#         start(client_task, i, frag, "/path/out/frag{}".format(i+1))

for i in range(1):
        print("Request evaluation of plant: ", i)
        start(client_task_bruteval, i, i)
