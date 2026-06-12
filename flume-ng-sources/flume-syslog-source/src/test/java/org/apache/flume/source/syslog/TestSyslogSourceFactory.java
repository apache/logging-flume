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
package org.apache.flume.source.syslog;

import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.source.DefaultSourceFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSyslogSourceFactory {

    private SourceFactory sourceFactory;

    @Before
    public void setUp() {
        sourceFactory = new DefaultSourceFactory();
    }

    @Test
    public void testSyslogTcpSourceCreation() throws Exception {
        Source src = sourceFactory.create("syslogtcp-src", "syslogtcp");
        Assert.assertNotNull(src);
        Assert.assertTrue(src instanceof SyslogTcpSource);
    }

    @Test
    public void testMultiportSyslogTcpSourceCreation() throws Exception {
        Source src = sourceFactory.create("multiport_syslogtcp-src", "multiport_syslogtcp");
        Assert.assertNotNull(src);
        Assert.assertTrue(src instanceof MultiportSyslogTCPSource);
    }

    @Test
    public void testSyslogUdpSourceCreation() throws Exception {
        Source src = sourceFactory.create("syslogudp-src", "syslogudp");
        Assert.assertNotNull(src);
        Assert.assertTrue(src instanceof SyslogUDPSource);
    }
}
