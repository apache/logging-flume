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


import org.apache.flume.Event;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

public class TestSyslogUtils {
  @Test
  public void TestHeader0() throws ParseException {
    String stamp1 = "2012-04-13T11:11:11";
    String format1 = "yyyy-MM-dd'T'HH:mm:ssZ";
    String host1 = "ubuntu-11.cloudera.com";
    String data1 = "some msg";
    // timestamp with hh:mm format timezone with no version
    String msg1 = "<10>" + stamp1+ "+08:00" + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, stamp1 + "+0800", format1, host1, data1);
  }

  @Test
  public void TestHeader1() throws ParseException {
    String stamp1 = "2012-04-13T11:11:11";
    String format1 = "yyyy-MM-dd'T'HH:mm:ss";
    String host1 = "ubuntu-11.cloudera.com";
    String data1 = "some msg";
    String msg1 = "<10>1 " + stamp1 + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, stamp1, format1, host1, data1);
  }

  @Test
  public void TestHeader2() throws ParseException {

    String stamp1 = "2012-04-13T11:11:11";
    String format1 = "yyyy-MM-dd'T'HH:mm:ssZ";
    String host1 = "ubuntu-11.cloudera.com";
    String data1 = "some msg";
    // timestamp with 'Z' appended, translates to UTC
    String msg1 = "<10>1 " + stamp1+ "Z" + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, stamp1 + "+0000", format1, host1, data1);
  }

  @Test
  public void TestHeader3() throws ParseException {
    String stamp1 = "2012-04-13T11:11:11";
    String format1 = "yyyy-MM-dd'T'HH:mm:ssZ";
    String host1 = "ubuntu-11.cloudera.com";
    String data1 = "some msg";
    // timestamp with hh:mm format timezone
    String msg1 = "<10>1 " + stamp1+ "+08:00" + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, stamp1 + "+0800", format1, host1, data1);
  }

  @Test
  public void TestHeader4() throws ParseException {
     String host1 = "ubuntu-11.cloudera.com";
    String data1 = "some msg";
    // null format timestamp (-)
    String msg1 = "<10>1 " + "-" + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, null, null, host1, data1);
  }

  @Test
  public void TestHeader5() throws ParseException {
    String stamp1 = "2012-04-13T11:11:11";
    String format1 = "yyyy-MM-dd'T'HH:mm:ss";
    String host1 = "-";
    String data1 = "some msg";
    // null host
    String msg1 = "<10>1 " + stamp1 + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, stamp1, format1, null, data1);
  }

  @Test
  public void TestHeader6() throws ParseException {
    String stamp1 = "2012-04-13T11:11:11";
    String format1 = "yyyy-MM-dd'T'HH:mm:ssZ";
    String host1 = "-";
    String data1 = "some msg";
    // null host
    String msg1 = "<10>1 " + stamp1+ "Z" + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, stamp1 + "+0000", format1, null, data1);
  }

  @Test
  public void TestHeader7() throws ParseException {
    String stamp1 = "2012-04-13T11:11:11";
    String format1 = "yyyy-MM-dd'T'HH:mm:ssZ";
    String host1 = "-";
    String data1 = "some msg";
    // null host
    String msg1 = "<10>1 " + stamp1+ "+08:00" + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, stamp1 + "+0800", format1, null, data1);
  }

  @Test
  public void TestHeader8() throws ParseException {
    String stamp1 = "2012-04-13T11:11:11.999";
    String format1 = "yyyy-MM-dd'T'HH:mm:ss.S";
    String host1 = "ubuntu-11.cloudera.com";
    String data1 = "some msg";
    String msg1 = "<10>1 " + stamp1 + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, stamp1, format1, host1, data1);
  }

  @Test
  public void TestHeader9() throws ParseException {
    String stamp1 = "Apr 11 13:14:04";
    String format1 = "yyyyMMM d HH:mm:ss";
    String host1 = "ubuntu-11.cloudera.com";
    String data1 = "some msg";
    // timestamp with 'Z' appended, translates to UTC
    String msg1 = "<10>" + stamp1 + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, String.valueOf(Calendar.getInstance().get(Calendar.YEAR)) + stamp1,
        format1, host1, data1);
  }

  @Test
  public void TestHeader10() throws ParseException {
    String stamp1 = "Apr  1 13:14:04";
    String format1 = "yyyyMMM d HH:mm:ss";
    String host1 = "ubuntu-11.cloudera.com";
    String data1 = "some msg";
    // timestamp with 'Z' appended, translates to UTC
    String msg1 = "<10>" + stamp1 + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, String.valueOf(Calendar.getInstance().get(Calendar.YEAR)) + stamp1,
        format1, host1, data1);
  }

  @Test
  public void TestRfc3164HeaderApacheLogWithNulls() throws ParseException {
    String stamp1 = "Apr  1 13:14:04";
    String format1 = "yyyyMMM d HH:mm:ss";
    String host1 = "ubuntu-11.cloudera.com";
    String data1 = "- hyphen_null_breaks_5424_pattern [07/Jun/2012:14:46:44 -0600]";
    String msg1 = "<10>" + stamp1 + " " + host1 + " " + data1 + "\n";
    checkHeader(msg1, String.valueOf(Calendar.getInstance().get(Calendar.YEAR)) + stamp1,
            format1, host1, data1);
  }

  public void checkHeader(String msg1, String stamp1, String format1,
      String host1, String data1) throws ParseException {
    SyslogUtils util = new SyslogUtils(false);
    ChannelBuffer buff = ChannelBuffers.buffer(200);

    buff.writeBytes(msg1.getBytes());
    Event e = util.extractEvent(buff);
    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers2 = e.getHeaders();
    if (stamp1 == null) {
      Assert.assertFalse(headers2.containsKey("timestamp"));
    } else {
      SimpleDateFormat formater = new SimpleDateFormat(format1);
      Assert.assertEquals(String.valueOf(formater.parse(stamp1).getTime()), headers2.get("timestamp"));
    }
    if (host1 == null) {
      Assert.assertFalse(headers2.containsKey("host"));
    } else {
      String host2 = headers2.get("host");
      Assert.assertEquals(host2,host1);
    }
    Assert.assertEquals(data1, new String(e.getBody()));
  }

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
    Assert.assertEquals("1", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null, headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(priority + goodData1.trim(),
        new String(e.getBody()).trim());

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
    Assert.assertEquals(badData1.trim(), new String(e.getBody())
      .trim());

    Event e2 = util.extractEvent(buff);
    if(e2 == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers2 = e2.getHeaders();
    Assert.assertEquals("1", headers2.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers2.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null,
        headers2.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(priority + goodData1.trim(),
        new String(e2.getBody()).trim());
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
    Assert.assertEquals("1", headers2.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers2.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null,
        headers2.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(priority + goodData1.trim(),
        new String(e2.getBody()).trim());

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
    Assert.assertEquals("1", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null,
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(priority + goodData1.trim(),
        new String(e.getBody()).trim());


    Event e2 = util.extractEvent(buff);
    if(e2 == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers2 = e2.getHeaders();
    Assert.assertEquals("2", headers2.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("4", headers2.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(null,
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals(priority2 + goodData2.trim(),
        new String(e2.getBody()).trim());

  }

  @Test
  public void testExtractBadEventLarge() {
    String badData1 = "<10> bad bad data bad bad\n";
    // The minimum size (which is 10) overrides the 5 specified here.
    SyslogUtils util = new SyslogUtils(5, false, false);
    ChannelBuffer buff = ChannelBuffers.buffer(100);
    buff.writeBytes(badData1.getBytes());
    Event e = util.extractEvent(buff);
    if(e == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers = e.getHeaders();
    Assert.assertEquals("1", headers.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("2", headers.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INCOMPLETE.getSyslogStatus(),
        headers.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals("<10> bad b".trim(), new String(e.getBody()).trim());

    Event e2 = util.extractEvent(buff);

    if(e2 == null){
      throw new NullPointerException("Event is null");
    }
    Map<String, String> headers2 = e2.getHeaders();
    Assert.assertEquals("0", headers2.get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertEquals("0", headers2.get(SyslogUtils.SYSLOG_SEVERITY));
    Assert.assertEquals(SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        headers2.get(SyslogUtils.EVENT_STATUS));
    Assert.assertEquals("ad data ba".trim(), new String(e2.getBody()).trim());

  }

}
