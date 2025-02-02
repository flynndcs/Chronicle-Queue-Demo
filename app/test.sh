#!/usr/bin/bash

count=100
ids=($(seq 1 100))

parallelCurl(){
  for run in {1..1000}; do
    curl http://localhost:8088/tenant -H 'Content-Type: application/json' -d '{"id": '$1', "action": "add", "quantity": 1}'
  done
}


for id in "${ids[@]}"; do
  time parallelCurl $id &
done

exit $?