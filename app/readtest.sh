#!/usr/bin/bash

count=100
ids=($(seq 1 100))

parallelCurl(){
  for run in {1..1000}; do
    curl http://localhost:8088/tenant?id=$1
  done
}


for id in "${ids[@]}"; do
  time parallelCurl $id &
done

exit $?