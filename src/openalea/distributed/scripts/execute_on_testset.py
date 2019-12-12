from openalea.distributed.execution.start_evaluation import evaluate_wf


v_workflow = ["A", "B", "C"]
v_plants_genotype = ["TEST1", "TEST4", "TEST8", "TEST16"]
v_nb_engine = [1, 2, 4, 8, 16]
v_cache_method = ["IRODS"]
v_index_algo = ["classic", "reuse", "force_rerun"]

PARAM = None

id_exp = "2"
CACHE_PATH = "/INRAgrid/home/gheidsieck/cache/" + "exp" + id_exp + "/"


