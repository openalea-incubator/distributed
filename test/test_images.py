from openalea.distributed.data import get_paths_from_genotype, load_raw, load_plant_snapshot
from irods.session import iRODSSession
from alinea.phenoarch.shooting_frame import get_shooting_frame
import collections


irods_sess = iRODSSession(host='inragrid-mtp.supagro.inra.fr',
                                  port=1247,
                                  user='gheidsieck',
                                  password='ghe2018#',
                                  zone='INRAgrid')


def test_load_raw():
    irods_sess = iRODSSession(host='inragrid-mtp.supagro.inra.fr',
                              port=1247,
                              user='gheidsieck',
                              password='ghe2018#',
                              zone='INRAgrid')

    p = {'top': {0: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/596a509b-6ffb-45bf-a2aa-7e30d132a209.png'}, 'side': {0: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/9c7982b6-b32e-4c96-ae38-6f5c8bf8b4e4.png', 330: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/70172478-c433-4e2f-bb58-746f0781c4b3.png', 300: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/79c9b570-fd2a-4b01-ab56-1c69ab69ba16.png', 270: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/7f123a38-3b5f-4496-bca9-b233e75db572.png', 240: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/d8ad94b3-5b83-4c36-b523-5d4c0890705c.png', 210: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/d3816c3f-c751-44f6-9ebf-974ba9f1d1d0.png', 180: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/3a8e6e15-413f-4390-b9e9-7a7e7b397bbc.png', 150: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/11c4fd7f-b3a2-473d-bed3-16aea9c4be3b.png', 120: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/792f05a1-e1ea-4659-9adb-ba6cfb8ac26f.png', 90: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/45fdea37-5d17-4974-ba26-b73f4f554f2b.png', 60: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/6dfb6948-22ff-4ed2-a7e4-8fa8652d448a.png', 30: 'http://stck-lepse.supagro.inra.fr/phenoarch/raw/ARCH2016-04-15/5138/651c5d21-d485-4b63-ab74-99d1ed0dd92d.png'}}
    plant_snapshot_unshaped = load_raw([p], method="irods", irods_sess=irods_sess)
    return plant_snapshot_unshaped



def test_get_paths_from_genotype():
    genotype = "T02273_005HE"
    plants = get_paths_from_genotype(genotype)
    return plants


def test_load_multiple_raw():
    irods_sess = iRODSSession(host='inragrid-mtp.supagro.inra.fr',
                              port=1247,
                              user='gheidsieck',
                              password='ghe2018#',
                              zone='INRAgrid')

    genotype = "T02273_005HE"
    l_snapshot, shooting_frame, plants = get_paths_from_genotype(genotype)

    small_ss = l_snapshot[:2]

    plants_raw_unshaped = load_raw(small_ss, method="irods", irods_sess=irods_sess)

    return plants_raw_unshaped


def test_load_plant_snapshot(plant):
    irods_sess = iRODSSession(host='inragrid-mtp.supagro.inra.fr',
                              port=1247,
                              user='gheidsieck',
                              password='ghe2018#',
                              zone='INRAgrid')
    genotype = "T02273_005HE"
    plants = get_paths_from_genotype(genotype)
    # the input is ONE plant info
    plant = plants[0]

    p = load_plant_snapshot(plant, irods_sess)
    return p


if __name__ == "__main__":
    print test_load_plant_snapshot(0)