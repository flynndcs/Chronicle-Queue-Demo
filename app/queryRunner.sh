#load test - 1M read requests, 64 at a time
seq 1 1000000 | xargs -I % -P 1 curl http://localhost:8088/tenant?id=e2131ee8-624f-4199-b1ee-0e5d64326a7c