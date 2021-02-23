#!/bin/bash

flink_host='localhost:8081'

python ../app/main.py --runner=FlinkRunner --flink_master="$flink_host" --input "../pp-monthly-update-new-version.csv"

exit 0