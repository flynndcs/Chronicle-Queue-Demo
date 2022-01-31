#!/bin/bash

seq 1 10000 | xargs -I % -P 64 curl http://localhost:8089/item?id=%