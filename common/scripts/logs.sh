#!/bin/bash

if [ -z "$1" ]
  then
    echo "please enter the container id or container name as the first argument e.g. ./logs.sh 6458f7e13f2c"
fi

docker logs "$1"