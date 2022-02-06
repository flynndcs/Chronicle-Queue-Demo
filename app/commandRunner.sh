#load test - 1M write requests, 64 at a time
seq 1 1000000 | xargs -I % -P 1 curl http://localhost:8088/tenant -H 'Content-Type: application/json' -d '{"id": 0, "action": "add", "quantity": '%' }'