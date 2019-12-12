from multiprocessing import Process
import os

from openalea.core.compositenode import CompositeNodeFactory, CompositeNode
from openalea.core import Package
from openalea.core import alea

from openalea.phenomenal.data import (
    plant_1)

from openalea.distributed.data import (
    images)

# images1 = plant_1.plant_1_images()
# #images2 = images.get_img_plant("0026", "2016-05-22")
#
# # import openalea.phenomenal.display.image as display_img
# # display_img.show_image(images2['side'][0])
# #
# # import openalea.phenomenal_wralea.phenoarch.binarization as bin
# #
# # result = bin.binarize(images2)
# #
# # import openalea.phenomenal.display.image as display_img
# # display_img.show_image(result['side'][0])
#
# pm = alea.load_package_manager()
# wf_factory = pm['openalea.phenomenal.demo']['binarize']
#
# # Si tu veux seulement les noeuds:
# #bin_factory = pm['openalea.phenomenal.binarize']['binarization']
#
# wf = wf_factory.instantiate()
#
# wf.set_input("plant_image", images1)
#
# # wf.node(2).set_input(0, images)
#
# wf.eval()
#
# # print wf.node(3).get_output(0)

print os.path.dirname(os.path.abspath(__file__))
