import uuid
import sys
import cPickle as pickle

from dask.multiprocessing import get
from dask import bag as db
from dask.delayed import delayed

from openalea.core import alea

from openalea.core.pkgmanager import PackageManager, protected
from openalea.core.package import UnknownNodeError


# #####   Create a dict for dask of the type :
#         dsk = {'int-1': (f_int, "1"),
#                'int-2': (f_int, 2),
#                'add': (f_add, 'int-1', 'int-2')}

def get_func(wf_factory, vid, call_stack=None):
    (package_id, factory_id) = wf_factory.elt_factory[vid]
    pkgmanager = PackageManager()
    pkg = pkgmanager[package_id]
    try:
        factory = pkg.get_factory(factory_id)
    except UnknownNodeError, e:
        # Bug when both package_id and protected(package_id) exist
        pkg = pkgmanager[protected(package_id)]
        factory = pkg.get_factory(factory_id)

    def f_tmp(*args):
        return alea.function(factory)(*args)[0]
    # return factory.get_classobj()
    return f_tmp


def get_dependencies(wf_factory, vid, d):
    list_dependencies = []
    arg_order = 0
    del list_dependencies[:]

    # check for connection between nodes
    for el in wf_factory.connections.values():
        if el[2] == vid:
            arg_order = el[3]
            try:
                list_dependencies[arg_order] = d.get(str(el[0]))
            except IndexError:
                for _ in range(arg_order - len(list_dependencies) + 1):
                    list_dependencies.append(None)
                list_dependencies[arg_order] = d.get(str(el[0]))

    # check for values of parameters
    if wf_factory.elt_value[vid]:
        for t in wf_factory.elt_value[vid]:
            # t[0] : port  / t[1] value
            arg_order = t[0]
            try:
                list_dependencies[arg_order] = (str(t[1]))
            except IndexError:
                for _ in range(arg_order - len(list_dependencies) + 1):
                    list_dependencies.append(None)
                list_dependencies[arg_order] = (str(t[1]))

    return list_dependencies


def to_dask(wf_factory):
    """
    :param wf_factory:
    :return: d : map of the node name (in openalea) and the token name i
             dsk : a dict of dask task
    get the dask dict from a composite node factory
    """

    dsk_keys = map(str, (wf_factory.elt_factory.keys()))
    d = dict.fromkeys(dsk_keys)
    for key in d:
        d[key] = str(uuid.uuid4())
    # dsk_keys = dsk_keys + ['__in__', '__out__']
    dsk = dict.fromkeys(d.values())
    for key in dsk:
        func = None
        arg = None
        if (key != '__in__') and (key != '__out__'):
            func = get_func(wf_factory, int(d.keys()[d.values().index(key)]))
            arg = get_dependencies(wf_factory, int(d.keys()[d.values().index(key)]), d)
        else:
            arg = get_dependencies(wf_factory, d.keys()[d.values().index(key)], d)

        l=[]
        l.append(func)
        for i in arg:
            l.append(i)
        dsk[key] = tuple(l)

    return d, dsk

######
"""
compare les dictionnaires des binaires pour le cn 'binarize'
"""
#
# pm = alea.load_package_manager()
# wf_factory = pm['openalea.phenomenal.demo']['binarize']
# #dask dict
# d, dsk = to_dask(wf_factory)
#
# d1 = get(dsk, d.get('2'))
#
# #openAlea instances
# wf = wf_factory.instantiate()
#
# wf.eval_as_expression(2)
# d2 = wf.node(2).output(0)
#
# for i in d1.keys():
#     for j in d1[i].keys():
#         if d1[i][j].all() != d2[i][j].all():
#             print "error not equal"

######
"""
test d'affichage
"""

pm = alea.load_package_manager()
wf_factory = pm['openalea.phenomenal.demo']['binarize']
#dask dict
d, dsk = to_dask(wf_factory)
bag = db.from_sequence(dsk, d.get('2'))
# print bag
#
# with open('data.pickle', 'wb') as f:
#     pickle.dump(bag, f, pickle.HIGHEST_PROTOCOL)

# with open('data.pickle', 'rb') as f:
#     bag = pickle.load(f)
d1 = bag.compute()
print d1
# d1 = get(dsk, d.get('2'))

# #openAlea instances
# wf = wf_factory.instantiate()
#
# wf.eval_as_expression(2)
# d2 = wf.node(2).output(0)



#######
"""
Test pour le cn 'pipeline_phenomenal'
"""
# pm = alea.load_package_manager()
# wf_factory = pm['openalea.phenomenal.demo']['pipeline_phenomenal']
#
# id_node = 9
#
# #dask dict
# d, dsk = to_dask(wf_factory)
# d1 = get(dsk, d.get('9'))
# # print d, dsk[d.get('13')]
# # print sys.getsizeof(d1)
#
# with open('data.pickle', 'wb') as f:
#     pickle.dump(d1, f, pickle.HIGHEST_PROTOCOL)
# with open('data.pickle', 'rb') as f:
#     d1 = pickle.load(f)
#
# for i in d1:
#     print i

#openalea moc
# wf = wf_factory.instantiate()
# wf.eval_as_expression(id_node)
# d2 = wf.node(id_node).output(0)
# # print d2
# print sys.getsizeof(d2)
