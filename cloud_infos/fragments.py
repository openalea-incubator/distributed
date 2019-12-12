from distributed.cloud_infos.paths import tmp_path

# For worlfow add_test
frag3 = {"inputs_vid":[(8,11), (8,12)], "outputs_vid":[(15,0)],
                  'cached_data':{}, 
                   'input_data': {(8,11): tmp_path+"6_0", (8,12): tmp_path+"11_0"}}
frag1 = {"inputs_vid":[], "outputs_vid":[(6,0)],
                  'cached_data':{}, 
                   'input_data': {}}
frag2 = {"inputs_vid":[], "outputs_vid":[(11,0)],
                  'cached_data':{}, 
                   'input_data': {}}
