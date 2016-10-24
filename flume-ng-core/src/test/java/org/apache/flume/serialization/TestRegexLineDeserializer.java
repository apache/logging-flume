/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.serialization;

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;

public class TestRegexLineDeserializer {

  private String fileInfo;

  @Before
  public void setup() {
    StringBuilder sb = new StringBuilder();
    sb.append("2016-10-19 10:19:10,188 INFO  [lifecycleSupervisor-1-0] (org.apache.flume.node.PollingPropertiesFileConfigurationProvider.start:61)  - Configuration provider starting");
    sb.append("\n");
    sb.append("2016-10-19 10:19:10,199 DEBUG [conf-file-poller-0] (org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.isValid:314)  - Starting validation of configuration for agent: agent, initial-configuration: AgentConfiguration[agent]");
    sb.append("\n");
    sb.append("SOURCES: {s1={ parameters:{decodeErrorPolicy=REPLACE, fileHeader=true, interceptors.i1.type=host, interceptors.i2.type=timestamp, channels=c1, spoolDir=filedir, type=spooldir, fileDelay=10000, deserializer=org.apache.flume.serialization.RegexLineDeserializer$Builder, fileSuffix=.COMPLETED, deletePolicy=immediate, interceptors=i1 i2, basenameHeader=true, ignorePattern=^(.)*\\.tmp$} }}");
    sb.append("\n");
    sb.append("CHANNELS: {c1={ parameters:{checkpointDir=./agent/checkpoint, dataDirs=./agent/data, transactionCapacity=10000, capacity=1000000, type=file} }}");
    sb.append("\n");
    sb.append("SINKS: {k1={ parameters:{clusterName=es, indexType=test4, serializer=org.apache.flume.sink.elasticsearch.ElasticSearchDynamicSerializer, indexName=test, batchSize=100, hostNames=127.0.0.1:9300,127.0.0.2:9300, type=elasticsearch, channel=c1} }}");
    sb.append("\n");
    sb.append("\n");
    sb.append("2016-10-19 10:19:10,204 DEBUG [conf-file-poller-0] (org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.validateChannels:469)  - Created channel c1");
    sb.append("\n");
    fileInfo = sb.toString();
  }
  
  @Test
  public void testMultiLine() throws IOException {
	  // ^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}
	  String dd="^\\";
	  // ^((${srcStartWithString})(\\s|\\S)).*
	  Context context = new Context();
	  context.put("filePattern", "^((\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})(\\s|\\S)).*");
	  ResettableInputStream in = new TestRegexLineInputStream(fileInfo);
	  EventDeserializer des = new RegexLineDeserializer(context, in);
	  Event evt = des.readEvent();
	  String line1 = new String(evt.getBody(), "UTF-8");
	  System.out.println("line 1\n" + line1);
	  des.mark();
	  evt = des.readEvent();
	  String line2 = new String(evt.getBody(), "UTF-8");
	  System.out.println("line 2\n" + line2);
	  des.mark();
	  evt = des.readEvent();
	  String line3 = new String(evt.getBody(), "UTF-8");
	  System.out.println("line 3\n" + line3);
	  des.reset();
	  des.mark();
	  des.close();
  }
}
