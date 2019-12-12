#!/usr/bin/env bash


python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=3 -a "classic" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=4 -a "classic" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=5 -a "classic" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test"
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=6 -a "classic" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=8 -a "classic" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=16 -a "classic" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=32 -a "classic" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 

python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=3 -a "force_rerun" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=4 -a "force_rerun" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=5 -a "force_rerun" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test"
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=6 -a "force_rerun" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=8 -a "force_rerun" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=16 -a "force_rerun" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=32 -a "force_rerun" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 

python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=3 -a "reuse" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=4 -a "reuse" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=5 -a "reuse" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test"
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=6 -a "reuse" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=8 -a "reuse" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=16 -a "reuse" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 
python ../src/openalea/distributed/execution/start_evaluation.py -n 16 --nb_plants=32 -a "reuse" -c "cluster" -id 2 -wf "C" --profile="sge" --cluster-id="cluster_test" 

