#!/bin/bash

java -cp target/command-query-1.0-SNAPSHOT-jar-with-dependencies.jar net.openhft.chronicle.queue.simple.input.CommandProcess db 0.0.0.0 &
java -cp target/command-query-1.0-SNAPSHOT-jar-with-dependencies.jar net.openhft.chronicle.queue.simple.input.QueryProcess db 0.0.0.0 &

wait -n
