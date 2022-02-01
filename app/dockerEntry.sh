#!/bin/bash

java -Xmx6g -cp target/app-1.0-SNAPSHOT-jar-with-dependencies.jar net.openhft.chronicle.queue.simple.input.CommandProcess db 0.0.0.0 &
java -Xmx6g -cp target/app-1.0-SNAPSHOT-jar-with-dependencies.jar net.openhft.chronicle.queue.simple.input.QueryProcess db 0.0.0.0 &
java -Xmx6g -cp target/app-1.0-SNAPSHOT-jar-with-dependencies.jar net.openhft.chronicle.queue.simple.input.CommandPersister db &

wait -n
