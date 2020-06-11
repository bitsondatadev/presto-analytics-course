#!/bin/bash

if [ -z "$1" ]
  then
    echo "please enter the container id or container name as the first argument e.g. ./login.sh 6458f7e13f2c"
fi

docker container exec -it "$1" /bin/bash