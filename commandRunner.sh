#!/bin/bash

seq 1 10000 | xargs -I % -P 64 curl http://localhost:8088/item -H 'Content-Type: application/json' -d '{"action": "upsert", "id": '%', "value": "value"}'