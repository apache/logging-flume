/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define  HOST_ARG "-h"
#define  TAG_ARG  "-t"
#include <ThriftFlumeEventServer.h>  // server interface
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include <unistd.h>
#include <sys/param.h>
#include <iostream>
#include <string>
#include <sstream>
#include <map>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

/*
 * The main function takes -h localhost:port-number and -t tag_key:value as arguments.
 * It forms a flume event structure with time-stamp taken from the system clock and hostname.
 * It sends the request to the flume node listening on the host-port specified
 * Returns 0 on success, returns 1 on failure, error codes not yet defined
*/

int main(int argc, char **argv) {

  string host_port;
  string tag_value;

  switch(argc)
  {
    case 3:
       if(0 == strcmp(argv[1], HOST_ARG)) {
          host_port = argv[2];
       } else {
          cout << "\tUsage: [-h <hostname>:<port>] is mandatory" << endl;
          exit(1);
       }
       break;
    case 5:
       if((0 == strcmp(argv[1], HOST_ARG)) && (0 == strcmp(argv[3], TAG_ARG))) {
          host_port = argv[2];
          tag_value = argv[4];
       } else if ((0 == strcmp(argv[1], TAG_ARG)) && (0 == strcmp(argv[3], HOST_ARG))) {
          host_port = argv[4];
          tag_value = argv[2];
       } else {
          cout << "\tWrong Options!" << endl;
          cout << "\tSynopsis: flogger -h <hostname>:<port> [-t <key>:<value>]" << endl;
          exit(1);
       }
       break;
     default:
       cout << "\tflogger will send the lines piped to it or by default from stdin to hostname:port w/ tag" << endl;
       cout << "\tSynopsis: flogger -h <hostname>:<port> [-t <key>:<value>]" << endl;
       exit(1);
       break;
  }

  string host;
  string port;
  {
      stringstream ss (host_port);
      if(!getline(ss, host, ':'))
      {
        cout << "\tWrong delimiter for -h, use <hostname>:<port>" << endl;
        exit(1);
      }
      if(!getline(ss, port, ':'))
      {
        cout << "\tWrong delimiter for -h, use <hostname>:<port>" << endl;
        exit(1);
      }
  }

  string tag_val;
  string tag_key;
  if(argc == 5)
  {
     stringstream ss (tag_value);
     if(!getline(ss, tag_key, ':'))
     {
       cout << "\tWrong delimiter for -t, use <key>:<value> " << endl;
       exit(1);
     }

     if(!getline(ss, tag_value, ':'))
     {
       cout << "\tWrong delimiter for -t, use <key>:<value> " << endl;
       exit(1);
     }
  }

  boost::shared_ptr<TSocket> socket(new TSocket(host.c_str(), atoi(port.c_str())));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));


  string line;
  char hostname[HOST_NAME_MAX];
  if(0 != gethostname(hostname, HOST_NAME_MAX))
  {
   cerr << "\n\tError: Cannot get hostname" << endl;
   exit(1);
  }

  map <string,string> tag;
  if(argc == 5)
  {
   tag[tag_key] = tag_value;
  }

  try {
      ThriftFlumeEventServerClient client(protocol);
      transport->open();
      while(getline(cin,line))
      {
        /*
	   ThriftFlumeEvt parameters
	   nanos: uses CLOCK_PROCESS_CPUTIME_ID High-resolution per-process timer from the CPU.
	*/

	ThriftFlumeEvent evt;
	Priority evt_priority;
	evt.priority = evt_priority.INFO;
	struct timespec t_nanos;
	if(0 == clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t_nanos)) {
	 evt.nanos = t_nanos.tv_sec * 1000000000LL + t_nanos.tv_nsec;
	} else {
	 cerr << "\n\tCannot read process cputime clock, quitting";
	 exit(1);
	}

	struct timespec t_stamp;
	if(0 == clock_gettime(CLOCK_REALTIME, &t_stamp)) {
	 evt.timestamp = (int64_t)t_stamp.tv_sec * 1000; // timestamp is needed in milliseconds
	} else {
	 cerr << "\n\tCannot read system clock, quitting";
	 exit(1);
	}

	evt.host = std::string(hostname);
	evt.body = line;
        if(argc == 5)
        {
	 evt.fields = tag;
        }
	client.append(evt);
      }
      transport->close();
   } catch( ... ) {
      cerr << "\tException raised!" << endl;
      exit(1);
   }

  return 0;
}
