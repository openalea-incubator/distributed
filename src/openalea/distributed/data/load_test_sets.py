import pandas
import os
import pkg_resources
from ast import literal_eval
import numpy as np
import cv2
from irods.session import iRODSSession
import argparse


from openalea.distributed.cache.cache_file import create_dir
from alinea.phenoarch.cloud.cloud import get_organized_meta_data_from_experiment, get_elcom_binaries_filename
from openalea.phenomenal.image.formats import write_image


def load_irods_metadata(nb_plants=0, genotype=None, plant=None, cache_path=None, experiment="ZA16", label = "organized"):
    if label == "organized":
        if "/INRAgrid/home/gheidsieck/cache/" in cache_path:
            path_df_meta_data = pkg_resources.resource_filename(
                'openalea.distributed.execution', "index_" + experiment + "_" + label + "_meta_data.csv")
        else:
            path_df_meta_data = os.path.join(cache_path, "index_" + experiment + "_" + label + "_meta_data.csv")

        if os.path.exists(path_df_meta_data):
            df_meta_data = pandas.read_csv(path_df_meta_data, converters={"path_http": literal_eval,
                                                                          "view_type": literal_eval,
                                                                          "camera_angle": literal_eval,
                                                                          "dates": literal_eval,
                                                                          "path_bin": literal_eval})
        else:
            df_meta_data = get_organized_meta_data_from_experiment(experiment=experiment)
            df_meta_data['path_http'] = [[str(el) for el in lists] for lists in df_meta_data['path_http']]
            df_meta_data['view_type'] = [[str(el) for el in lists] for lists in df_meta_data['view_type']]
            df_meta_data['path_local'] = [transform_paths(lists, cache_path) for lists in df_meta_data['path_http']]
            df_meta_data["path_bin"] = df_meta_data.apply(get_elcom_binaries_filename, axis=1)
            df_meta_data.to_csv(path_df_meta_data)
        return df_meta_data.loc[(df_meta_data['nview'] == 13)]


    elif label == "reduced":
        if "/INRAgrid/home/gheidsieck/cache/" in cache_path:
            path_df_meta_data = pkg_resources.resource_filename(
                'openalea.distributed.execution', "index_" + experiment + "_" + label + "_nb" + str(nb_plants) + "_meta_data.csv")
        else:
            path_df_meta_data = os.path.join(cache_path, "index_" + experiment + "_" + label + "_nb" + str(nb_plants) + "_meta_data.csv")

        if os.path.exists(path_df_meta_data):
            df_meta_data = pandas.read_csv(path_df_meta_data, converters={"path_http": literal_eval, "path_local": literal_eval})
        else:
            df_meta_data = load_irods_metadata(cache_path=cache_path, experiment=experiment, label="organized")
            df_meta_data = df_meta_data.loc[(df_meta_data['nview'] == 13)]
            list_name = df_meta_data.plant
            list_name = list(set(list_name))
            list_name.sort()

            df_meta_data = df_meta_data.loc[df_meta_data["plant"].isin(list_name[:nb_plants])]

            df_meta_data['path_http'] = [[str(el) for el in lists] for lists in df_meta_data['path_http']]
            df_meta_data['path_local'] = [transform_paths(lists, cache_path) for lists in df_meta_data['path_http']]
            df_meta_data.to_csv(path_df_meta_data)
        return df_meta_data


    elif label == "plant":
        if "/INRAgrid/home/gheidsieck/cache/" in cache_path:
            path_df_meta_data = pkg_resources.resource_filename(
                'openalea.distributed.execution', "index_" + experiment + "_" + label + "_plant" + str(plant[:4]) + "_meta_data.csv")
        else:
            path_df_meta_data = os.path.join(cache_path, "index_" + experiment + "_" + label + "_plant" + str(plant[:4]) + "_meta_data.csv")

        if os.path.exists(path_df_meta_data):
            df_meta_data = pandas.read_csv(path_df_meta_data, converters={"path_http": literal_eval, "path_local": literal_eval})
        else:
            df_meta_data = load_irods_metadata(cache_path=cache_path, experiment=experiment, label = "organized")
            df_meta_data = df_meta_data.loc[(df_meta_data['nview'] == 13)]
            df_meta_data = df_meta_data.loc[df_meta_data["plant"] == plant]
            # df_meta_data['path_http'] = [[str(el) for el in lists] for lists in df_meta_data['path_http']]
            # df_meta_data['path_local'] = [transform_paths(lists, cache_path) for lists in df_meta_data['path_http']]
            df_meta_data.to_csv(path_df_meta_data)
        return df_meta_data


def transform_paths(list_of_paths, cache_path=None):
    irods_path_prefix = "http://stck-lepse.supagro.inra.fr/phenoarch/"
    local_path_prefix = "/home/gaetan/OpenAlea/distributed/cache_data/"
    cluster_path_prefix = "/homedir/heidsieck/work/wf_executions/"
    new_paths = [path.replace(irods_path_prefix, cache_path+"/") for path in list_of_paths]
    return new_paths


def load_datasets_from_irods(cache_path=None, df_meta_data=None):
    irods_sess = iRODSSession(host='inragrid-mtp.supagro.inra.fr',
                              port=1247,
                              user='gheidsieck',
                              password='ghe2018#',
                              zone='INRAgrid')

    for plant in df_meta_data.path_http.tolist():
        local_paths = transform_paths(plant, cache_path)
        for path_i, path_l in zip(plant, local_paths):
            irods_filename = path_i.replace(
                'http://stck-lepse.supagro.inra.fr/',
                '/INRAgrid/home/public/M3P/')
            obj = irods_sess.data_objects.get(irods_filename)
            with obj.open('r') as fp:
                fp.seek(0, 2)
                buffer = fp.tell()
                fp.seek(0, 0)
                buf = np.asarray(bytearray(fp.read(buffer)))
                im = cv2.imdecode(buf, cv2.IMREAD_COLOR)
            # create_dir_with_permission(path_l, 0755)
            create_dir(path_l)
            write_image(path_l, im)
    return


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('-n', '--nb_plants', dest="nb_plants", type=int, default=0, help='number of plant to compute')
#     parser.add_argument('-g', '--genotype', dest="genotype", type=str, default=None, help='genotype to select plants')
#     parser.add_argument('-p', '--plant', dest="plant", type=str, default=None, help='name of the plant th select')
#     parser.add_argument('-c', '--cache_path', dest="cache_path", type=str, default=None, help='path of the cache')
#     parser.add_argument('-e', '--experiment', dest="experiment", type=str, default="ZA16", help='name of the experiement')
#     parser.add_argument('--label', type=str, dest="label", default="organized", help="method of data selection")
#
#     args = parser.parse_args()
#     df = load_irods_metadata(nb_plants=args.nb_plants, genotype=args.genotype, plant=args.plant, \
#                              cache_path=args.cache_path, experiment=args.experiment, label = args.label)
#
#     cache_path = os.path.dirname(args.cache_path)
#     load_datasets_from_irods(cache_path=args.cache_path, df_meta_data=df)
