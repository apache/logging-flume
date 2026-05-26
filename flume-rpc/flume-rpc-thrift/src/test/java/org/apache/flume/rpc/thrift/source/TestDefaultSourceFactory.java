/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.rpc.thrift.source;

import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.source.DefaultSourceFactory;
import org.apache.flume.source.ExecSource;
import org.apache.flume.source.MultiportSyslogTCPSource;
import org.apache.flume.source.NetcatSource;
import org.apache.flume.source.NetcatUdpSource;
import org.apache.flume.source.SequenceGeneratorSource;
import org.apache.flume.source.SyslogTcpSource;
import org.apache.flume.source.SyslogUDPSource;
import org.apache.flume.source.http.HTTPSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultSourceFactory {

  private SourceFactory sourceFactory;

  @Before
  public void setUp() {
    sourceFactory = new DefaultSourceFactory();
  }

  @Test
  public void testDuplicateCreate()  {

    Source thriftSource1 = sourceFactory.create("thriftSource1", "thrift");
    Source thriftSource2 = sourceFactory.create("thriftSource2", "thrift");

    Assert.assertNotNull(thriftSource1);
    Assert.assertNotNull(thriftSource2);
    Assert.assertNotSame(thriftSource1, thriftSource2);
    Assert.assertTrue(thriftSource1 instanceof ThriftSource);
    Assert.assertTrue(thriftSource2 instanceof ThriftSource);

    Source s1 = sourceFactory.create("thriftSource1", "thrift");
    Source s2 = sourceFactory.create("thriftSource2", "thrift");

    Assert.assertNotSame(thriftSource1, s1);
    Assert.assertNotSame(thriftSource2, s2);

  }

  private void verifySourceCreation(String name, String type,
      Class<?> typeClass) throws Exception {
    Source src = sourceFactory.create(name, type);
    Assert.assertNotNull(src);
    Assert.assertTrue(typeClass.isInstance(src));
  }

  @Test
  public void testSourceCreation() throws Exception {
    verifySourceCreation("seq-src", "seq", SequenceGeneratorSource.class);
    verifySourceCreation("netcat-src", "netcat", NetcatSource.class);
    verifySourceCreation("netcat-udp-src", "netcatudp", NetcatUdpSource.class);
    verifySourceCreation("exec-src", "exec", ExecSource.class);
    verifySourceCreation("syslogtcp-src", "syslogtcp", SyslogTcpSource.class);
    verifySourceCreation("multiport_syslogtcp-src", "multiport_syslogtcp",
        MultiportSyslogTCPSource.class);
    verifySourceCreation("syslogudp-src", "syslogudp", SyslogUDPSource.class);
    verifySourceCreation("http-src", "http", HTTPSource.class);
    verifySourceCreation("thrift-src", "thrift", ThriftSource.class);
    verifySourceCreation("custom-src", MockSource.class.getCanonicalName(),
        MockSource.class);
  }

}
