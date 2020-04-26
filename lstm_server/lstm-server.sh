#!/bin/bash

DIR="$( cd "$( dirname "$0"  )" && pwd  )"
export PYTHONPATH=${PYTHONPATH}:${DIR}

cd ${DIR}
#python bin/lstm-server.py
nohup python bin/lstm-server.pyc > /dev/null 2>&1 &
