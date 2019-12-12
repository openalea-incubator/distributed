parallel --bar -j 2 '/home/gaetan/miniconda2/envs/openalea/bin/python /home/gaetan/OpenAlea/distributed/src/openalea/distributed/cache/cache_functions.py {1} {2}' ::: \
$(seq 20)  ::: \
True
