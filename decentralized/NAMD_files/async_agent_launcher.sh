#!/bin/bash

. ~/.bashrc
. ~/.profile_user

python `dirname $0`/async_agent.py $*
