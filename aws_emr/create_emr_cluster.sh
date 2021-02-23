#!/bin/bash

aws emr create-cluster --name "Cluster Flink" --release-label emr-5.32.0 \
  --applications Name=Flink --ec2-attributes KeyName=FlinkKey \
  --instance-type m1.small --instance-count 1 --use-default-roles

exit 0
