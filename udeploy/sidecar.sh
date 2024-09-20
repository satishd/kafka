#!/bin/bash
set -x
source udeploy/vars.sh
bash udeploy/config.sh
echo "Running $@"
$@
