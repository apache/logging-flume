#
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
#

CC = g++
SRCS = src/cpp/ThriftThinFlumeRPC.cpp
THRIFT_SRCS = gen-cpp/ThriftFlumeEventServer.cpp gen-cpp/flume_constants.cpp gen-cpp/flume_types.cpp src/cpp/ThriftThinFlumeRPC.cpp
FLUME_THRIFT_IDL = ../../flume-core/src/main/thrift/flume.thrift
OBJS = gen-cpp/ThriftFlume.o gen-cpp/ThriftFlumeConstants.o gen-cpp/ThriftFlumeTypes.o

.PHONY: all
all: flogger


flogger: ${OBJS}
	${CC} -Wall -I/usr/local/include/thrift -c gen-cpp/ThriftFlumeEventServer.cpp -o gen-cpp/ThriftFlume.o
	${CC} -Wall -I/usr/local/include/thrift -c gen-cpp/flume_constants.cpp -o gen-cpp/ThriftFlumeConstants.o
	${CC} -Wall -I/usr/local/include/thrift -c gen-cpp/flume_types.cpp -o gen-cpp/ThriftFlumeTypes.o
	${CC} -Wall -I/usr/local/include/thrift -Igen-cpp/ -c src/cpp/ThriftThinFlumeRPC.cpp -o gen-cpp/thinFlumeclient.o
	${CC} -Wall -L/usr/local/lib ${OBJS} gen-cpp/thinFlumeclient.o -lthrift -lrt -static -o flogger

${OBJS}: ${THRIFT_SRCS}

${THRIFT_SRCS}: ${FLUME_THRIFT_IDL}
	thrift --gen cpp ${FLUME_THRIFT_IDL}

.PHONY: clean
clean:
	rm -rf gen-cpp/
	rm flogger

.PHONY: install
install: flogger
	cp flogger /usr/local/bin/
