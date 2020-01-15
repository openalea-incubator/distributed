import zmq
from threading import Thread
from multiprocessing import Process

from openalea.distributed.zmq.broker_config import FRONTEND_ADDR, BACKEND_ADDR


def start_broker(frontend_addr=FRONTEND_ADDR, backend_addr=BACKEND_ADDR):
    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.ROUTER)
    frontend.bind(frontend_addr)
    backend.bind(backend_addr)


    # Initialize main loop state
    # count = NBR_CLIENTS
    workers = []
    poller = zmq.Poller()
    # Only poll for requests from backend until workers are available
    poller.register(backend, zmq.POLLIN)

    while True:
        sockets = dict(poller.poll())

        if backend in sockets:
            # Handle worker activity on the backend
            request = backend.recv_multipart()
            print request
            worker, empty, client = request[:3]
            if not workers:
                # Poll for clients now that a worker is available
                poller.register(frontend, zmq.POLLIN)
            workers.append(worker)
            if client != b"READY" and len(request) > 3:
                # If client reply, send rest back to frontend
                empty, reply = request[3:]
                frontend.send_multipart([client, b"", reply])
    #             count -= 1
    #             if not count:
    #                 break

        if frontend in sockets:
            # Get next client request, route to last-used worker
            client, empty, request = frontend.recv_multipart()
            print client, empty, request
            worker = workers.pop(0)
            backend.send_multipart([worker, b"", client, b"", request])
            if not workers:
                # Don't poll clients if no workers are available
                poller.unregister(frontend)

    # Clean up
    backend.close()
    frontend.close()
    context.term()