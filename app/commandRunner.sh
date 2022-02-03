#load test - 1M write requests, 64 at a time
seq 1 1000 | xargs -I % -P 64 curl http://localhost:8088/item -H 'Content-Type: application/json' -d '{"action": "upsert", "id": '%', "value": "value"}'