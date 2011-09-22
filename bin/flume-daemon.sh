#!/usr/bin/env /bin/bash
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

usage="Usage: $0.sh (start|stop) <flume-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get arguments
startStop=$1
shift
command=$1
shift

flume_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${bin}/flume-env.sh" ]; then
    source "${bin}/flume-env.sh"
fi

if [ "$FLUME_HOME" = "" ]; then
  export FLUME_HOME=/usr/lib/flume
fi

if [ "$FLUME_LOG_DIR" = "" ]; then
  export FLUME_LOG_DIR="/var/log/flume"
fi

if [ "$FLUME_NICENESS" = "" ]; then
	export FLUME_NICENESS=0
fi 

if [ "$FLUME_PID_DIR" = "" ]; then
  export FLUME_PID_DIR=/var/run/flume
fi

if [ "$FLUME_IDENT_STRING" = "" ]; then
  export FLUME_IDENT_STRING="$USER"
fi

# some variables
export FLUME_LOGFILE=flume-$FLUME_IDENT_STRING-$command-$HOSTNAME.log
export FLUME_ROOT_LOGGER="INFO,DRFA"
export ZOOKEEPER_ROOT_LOGGER="INFO,zookeeper"
export WATCHDOG_ROOT_LOGGER="INFO,watchdog"
log=$FLUME_LOG_DIR/flume-$FLUME_IDENT_STRING-$command-$HOSTNAME.out
pid=$FLUME_PID_DIR/flume-$FLUME_IDENT_STRING-$command.pid

case $startStop in

  (start)
    mkdir -p "$FLUME_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      else # must be a stale PID file
        rm -f $pid
      fi
    fi

    flume_rotate_log $log
    echo starting $command, logging to $FLUME_LOG_DIR/$FLUME_LOGFILE
    cd "$FLUME_HOME"
    nohup  nice -n ${FLUME_NICENESS} "${FLUME_HOME}"/bin/flume $command "$@" > "$log" 2>&1 < /dev/null &
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
