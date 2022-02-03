#load test - 1M read requests, 64 at a time
seq 1 1000000 | xargs -I % -P 64 curl http://localhost:8088/item?id=%