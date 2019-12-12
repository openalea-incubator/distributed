#!/usr/bin/env bash


python src/openalea/distributed/execution/start_evaluation.py -n 1 --nb_plants=1 -a "force_rerun" -c "local" -id 2 -wf "A" --profile="distributed1" --cluster-id=""  --image=True  --mem="one_map"


