#!/bin/bash

java -Xmx20g -Xlog:gc*:file=/usr/src/myapp/gc.log -cp target/app-1.0-SNAPSHOT-jar-with-dependencies.jar com.flynndcs.app.App db 0.0.0.0 &

wait -n
