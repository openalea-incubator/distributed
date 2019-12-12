import subprocess
import os
import sys
import time
import ipyparallel as ipp


### start IPython cluster
def waitforp(p, mesg):
    while True:
        line = p.stderr.readline()
        if line != "":
            print line
        # print(line.strip())
        if mesg in line:
            break

def start_ipy_cluster(nb_engine):
    """
    Start a ipyparallel cluster of nb_engine engines
    :return:
    """
    # jobname = os.environ['JOB_NAME']
    # jobid = str(os.environ['JOB_ID'])
    # cluster = jobname + jobid
    cluster = "cluster_test"
    engines = str(nb_engine)
    pcmd = "ipcluster", "start", "--profile=distributed1", "--cluster-id=" + cluster, "-n", engines
    print "Starting IPython cluster", cluster, "with", engines, "engines:", ' '.join(pcmd)
    p = subprocess.Popen(pcmd, env=os.environ.copy(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    waitforp(p, "Engines appear to have started successfully")
    rc = None
    while rc is None:
        try:
            rc = ipp.Client(profile='distributed1', cluster_id=cluster)
        except:
            print "Waiting for ipcontroller..."
            time.sleep(10)
            pass
    while len(rc.ids) < int(engines):
        print "Waiting for ipengines..."
        time.sleep(10)
        # print rc.ids

    return rc, cluster, p


def stop_ipy_cluster(p, cluster):
    ### stop IPython cluster
    qcmd = "ipcluster", "stop", "--profile=distributed1", "--cluster-id=" + cluster
    print "Stopping IPython cluster", cluster + ":", ' '.join(qcmd)
    q = subprocess.Popen(qcmd, env=os.environ.copy(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    waitforp(p, "Removing pid file")


