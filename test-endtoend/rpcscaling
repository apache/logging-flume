#!/bin/bash -x 

export FLUME_CONF_DIR=/home/jon/flume/conf


FLUME=../bin/flume
SRC_DIR=./rpc

mkdir -p $SRC_DIR
./gen-config.py  5 20000000 1 1 > $SRC_DIR/test-1c-1a-5nx20000000e
./gen-config.py 10 10000000 1 1 > $SRC_DIR/test-1c-1a-10nx10000000e
./gen-config.py 50  2000000 1 1 > $SRC_DIR/test-1c-1a-50nx2000000e
./gen-config.py 100 1000000 1 1 > $SRC_DIR/test-1c-1a-100nx1000000e

./gen-config.py  5 20000000 2 1 > $SRC_DIR/test-2c-1a-5nx20000000e
./gen-config.py 10 10000000 2 1 > $SRC_DIR/test-2c-1a-10nx10000000e
./gen-config.py 50  2000000 2 1 > $SRC_DIR/test-2c-1a-50nx2000000e
./gen-config.py 100 1000000 2 1 > $SRC_DIR/test-2c-1a-100nx1000000e

./gen-config.py  5 20000000 1 2 > $SRC_DIR/test-1c-2a-5nx20000000e
./gen-config.py 10 10000000 1 2 > $SRC_DIR/test-1c-2a-10nx10000000e
./gen-config.py 50  2000000 1 2 > $SRC_DIR/test-1c-2a-50nx2000000e
./gen-config.py 100 1000000 1 2 > $SRC_DIR/test-1c-2a-100nx1000000e

./gen-config.py  5 20000000 2 2 > $SRC_DIR/test-2c-2a-5nx20000000e
./gen-config.py 10 10000000 2 2 > $SRC_DIR/test-2c-2a-10nx10000000e
./gen-config.py 50  2000000 2 2 > $SRC_DIR/test-2c-2a-50nx2000000e
./gen-config.py 100 1000000 2 2 > $SRC_DIR/test-2c-2a-100nx1000000e


for f in $SRC_DIR/* ; do
#  echo $f
  $FLUME shell -s $f
done
