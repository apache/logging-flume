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
package org.apache.flume.source;


import java.util.Map;

import junit.framework.Assert;

import org.apache.flume.Event;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

public class TestSyslogUtils {

  /**
   * Test bad event format 1: Priority is not numeric
   */
  @Test
  public void testExtractBadEvent1() {
    String badData1 = "<10F> bad bad data\n";
    SyslogUtils util = new SyslogUtils(false);
    ChannelBuffer buff = ChannelBuffers.buffer(100);
    buff.writeBytes(badData1.getBytes());
    Event e = util.extractEvent(buff);
    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers = e.getHeaders();
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(badData1.trim(), new String(e.getBody()).trim());

  }

  /**
   * Test bad event format 2: The first char is not <
   */

  @Test
  public void testExtractBadEvent2() {
    String badData1 = "hi guys! <10> bad bad data\n";
    SyslogUtils util = new SyslogUtils(false);
    ChannelBuffer buff = ChannelBuffers.buffer(100);
    buff.writeBytes(badData1.getBytes());
    Event e = util.extractEvent(buff);
    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers = e.getHeaders();
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(badData1.trim(), new String(e.getBody()).trim());

  }

  /**
   * Good event
   */
  @Test
  public void testExtractGoodEvent() {
    String priority = "<10>";
    String goodData1 = "Good good good data\n";
    SyslogUtils util = new SyslogUtils(false);
    ChannelBuffer buff = ChannelBuffers.buffer(100);
    buff.writeBytes((priority+goodData1).getBytes());
    Event e = util.extractEvent(buff);
    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers = e.getHeaders();
    Assert.assertEquals("8", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null, headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(goodData1.trim(), new String(e.getBody()).trim());

  }

  /**
   * Bad event immediately followed by a good event
   */
  @Test
  public void testBadEventGoodEvent(){
    String badData1 = "hi guys! <10F> bad bad data\n";
    SyslogUtils util = new SyslogUtils(false);
    ChannelBuffer buff = ChannelBuffers.buffer(100);
    buff.writeBytes(badData1.getBytes());
    String priority = "<10>";
    String goodData1 = "Good good good data\n";
    buff.writeBytes((priority+goodData1).getBytes());
    Event e = util.extractEvent(buff);

    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers = e.getHeaders();
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(badData1.trim(), new String(e.getBody()).trim());

    Event e2 = util.extractEvent(buff);
    if(e2 == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers2 = e2.getHeaders();
    Assert.assertEquals("8", headers2.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers2.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null,
        headers2.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(goodData1.trim(), new String(e2.getBody()).trim());
  }

  @Test
  public void testGoodEventBadEvent(){
    String badData1 = "hi guys! <10F> bad bad data\n";
    String priority = "<10>";
    String goodData1 = "Good good good data\n";
    SyslogUtils util = new SyslogUtils(false);
    ChannelBuffer buff = ChannelBuffers.buffer(100);
    buff.writeBytes((priority+goodData1).getBytes());
    buff.writeBytes(badData1.getBytes());

    Event e2 = util.extractEvent(buff);
    if(e2 == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers2 = e2.getHeaders();
    Assert.assertEquals("8", headers2.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers2.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null,
        headers2.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(goodData1.trim(), new String(e2.getBody()).trim());

    Event e = util.extractEvent(buff);

    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers = e.getHeaders();
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(badData1.trim(), new String(e.getBody()).trim());

  }

  @Test
  public void testBadEventBadEvent(){
    String badData1 = "hi guys! <10F> bad bad data\n";
    SyslogUtils util = new SyslogUtils(false);
    ChannelBuffer buff = ChannelBuffers.buffer(100);
    buff.writeBytes(badData1.getBytes());
    String badData2 = "hi guys! <20> bad bad data\n";
    buff.writeBytes((badData2).getBytes());
    Event e = util.extractEvent(buff);

    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers = e.getHeaders();
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("0", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(badData1.trim(), new String(e.getBody()).trim());

    Event e2 = util.extractEvent(buff);

    if(e2 == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers2 = e2.getHeaders();
    Assert.assertEquals("0", headers2.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("0", headers2.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        headers2.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(badData2.trim(), new String(e2.getBody()).trim());
  }

  @Test
  public void testGoodEventGoodEvent() {

    String priority = "<10>";
    String goodData1 = "Good good good data\n";
    SyslogUtils util = new SyslogUtils(false);
    ChannelBuffer buff = ChannelBuffers.buffer(100);
    buff.writeBytes((priority+goodData1).getBytes());
    String priority2 = "<20>";
    String goodData2 = "Good really good data\n";
    buff.writeBytes((priority2+goodData2).getBytes());
    Event e = util.extractEvent(buff);
    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers = e.getHeaders();
    Assert.assertEquals("8", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null,
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(goodData1.trim(), new String(e.getBody()).trim());


    Event e2 = util.extractEvent(buff);
    if(e2 == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers2 = e2.getHeaders();
    Assert.assertEquals("16", headers2.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("4", headers2.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null,
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(goodData2.trim(), new String(e2.getBody()).trim());

  }

  @Test
  public void testExtractBadEventLarge() {
    String badData1 = "<10> bad bad data bad bad\n";
    SyslogUtils util = new SyslogUtils(5, false);
    ChannelBuffer buff = ChannelBuffers.buffer(100);
    buff.writeBytes(badData1.getBytes());
    Event e = util.extractEvent(buff);
    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers = e.getHeaders();
    Assert.assertEquals("8", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INCOMPLETE.getSyslogStatus(),
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals("bad bad d".trim(), new String(e.getBody()).trim());

    Event e2 = util.extractEvent(buff);

    if(e2 == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers2 = e2.getHeaders();
    Assert.assertEquals("0", headers2.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("0", headers2.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        headers2.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals("ata bad ba".trim(), new String(e2.getBody()).trim());

  }

}
