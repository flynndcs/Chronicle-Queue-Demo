#!/usr/bin/bash

count=10
ids=($(shuf -i 0-1000 -n $count))

for id in "${ids[@]}"; do
  echo $id
done

parallelCurl(){
  for run in {1..10000}; do curl http://localhost:8088/tenant -H 'Content-Type: application/json' -d '{"id": "'$1'", "action": "add", "quantity": 1}'; done
}


for id in "${ids[@]}"; do
  parallelCurl $id &
done

exit