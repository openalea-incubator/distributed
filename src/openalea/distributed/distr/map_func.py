import cv2
import os
import numpy as np
import time
import psutil
from multiprocessing import Pool, TimeoutError

from multiprocessing import Process
import os

from openalea.core.compositenode import CompositeNodeFactory, CompositeNode
from openalea.core import alea
from openalea.distributed.data import (
    images)
from openalea.core.algo import dataflow_evaluation
from openalea.core import *


__all__ = ['f']


def time_usage(func):
    def wrapper(*args, **kwargs):
        beg_ts = time.time()
        retval = func(*args, **kwargs)
        end_ts = time.time()
        print("elapsed time: %f" % (end_ts - beg_ts))
        return retval
    return wrapper


def f(x):
    return x**x**x


def binarize_from_img(tuple_id_plant):
    #tuple that containt id_plant and date
    id_plant, date = tuple_id_plant
    img = images.get_img_plant(id_plant, date)
    pm = alea.load_package_manager()
    wf_factory = pm['openalea.phenomenal.demo']['binarize']
    wf = wf_factory.instantiate()
    wf.set_input("plant_image", img)
    # wf.eval()
    eva = dataflow_evaluation.PriorityEvaluation(wf)
    eva.eval(2, quantify=True)
    return wf.get_output(0)


@time_usage
def main():
    tuple_id_plant = [("0026", "2016-05-20"), ("0026", "2016-05-22")] *2

    pool = Pool(processes=7)
    res = pool.map(binarize_from_img, tuple_id_plant)
    print res


if __name__ == "__main__":
    main()

