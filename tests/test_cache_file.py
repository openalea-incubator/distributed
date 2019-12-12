from openalea.distributed.cache.cache_file import write_intermediate_data_irods, load_intermediate_data_irods
from irods.session import iRODSSession

import dill

def test_load_write_irods():
    irods_sess = iRODSSession(host='inragrid-mtp.supagro.inra.fr',
                                      port=1247,
                                      user='gheidsieck',
                                      password='ghe2018#',
                                      zone='INRAgrid')

    cache_path = "/INRAgrid/home/gheidsieck/cache/exp1/data/data_1"

    res = load_intermediate_data_irods(cache_path, irods_sess)
    print res

    # persist_irods(data_path, cache_path, irods_sess=irods_sess)
    # write_intermediate_data_irods(dill.dumps(res), cache_path, irods_sess=irods_sess)




if __name__ == "__main__":
    test_load_write_irods()

