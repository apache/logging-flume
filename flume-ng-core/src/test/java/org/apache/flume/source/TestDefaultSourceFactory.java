/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source;

import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
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
    public void testDuplicateCreate() {

        Source execSource1 = sourceFactory.create("execSource1", "exec");
        Source execSource2 = sourceFactory.create("execSource2", "exec");

        Assert.assertNotNull(execSource1);
        Assert.assertNotNull(execSource2);
        Assert.assertNotSame(execSource1, execSource2);
        Assert.assertTrue(execSource1 instanceof ExecSource);
        Assert.assertTrue(execSource2 instanceof ExecSource);

        Source s1 = sourceFactory.create("execSource1", "exec");
        Source s2 = sourceFactory.create("execSource2", "exec");

        Assert.assertNotSame(execSource1, s1);
        Assert.assertNotSame(execSource2, s2);
    }

    private void verifySourceCreation(String name, String type, Class<?> typeClass) throws Exception {
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
        verifySourceCreation("multiport_syslogtcp-src", "multiport_syslogtcp", MultiportSyslogTCPSource.class);
        verifySourceCreation("syslogudp-src", "syslogudp", SyslogUDPSource.class);
        // verifySourceCreation("spooldir-src", "spooldir", SpoolDirectorySource.class);
        // verifySourceCreation("thrift-src", "thrift", ThriftSource.class);
        verifySourceCreation("custom-src", MockSource.class.getCanonicalName(), MockSource.class);
    }
}
