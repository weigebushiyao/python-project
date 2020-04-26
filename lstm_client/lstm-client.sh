#!/bin/bash

DIR="$( cd "$( dirname "$0"  )" && pwd  )"
export PYTHONPATH=${PYTHONPATH}:${DIR}

cd ${DIR}
python bin/lstm-client.py
#nohup python bin/lstm-client.py > /dev/null 2>&1 &
