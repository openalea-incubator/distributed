
# TODO: put this in a function "random input data"
def generate_list_percenreuse(reuse_percent, plants_len):
    # chose the % of resue in the data
    import random
    random.seed(1)

    percent_reuse = reuse_percent
    nb_reuse = int(plants_len) * percent_reuse / 100

    list_plant_reuse = ["reuse"] * nb_reuse
    l2 = ["force_rerun"] * (int(plants_len) - nb_reuse)
    list_plant_reuse.extend(l2)
    random.shuffle(list_plant_reuse)
    return list_plant_reuse

