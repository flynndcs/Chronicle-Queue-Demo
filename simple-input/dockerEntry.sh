#!/bin/bash

java -cp target/simple-input-1.0-SNAPSHOT-jar-with-dependencies.jar net.openhft.chronicle.queue.simple.input.CommandProcess &
java -cp target/simple-input-1.0-SNAPSHOT-jar-with-dependencies.jar net.openhft.chronicle.queue.simple.input.QueryProcess &

wait -n
