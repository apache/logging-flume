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
package org.apache.flume.sink.hbase2;

import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRegexHBase2EventSerializer {

  @Test
  /* Ensure that when no config is specified, the a catch-all regex is used
   *  with default column name. */
  public void testDefaultBehavior() throws Exception {
    RegexHBase2EventSerializer s = new RegexHBase2EventSerializer();
    Context context = new Context();
    s.configure(context);
    String logMsg = "The sky is falling!";
    Event e = EventBuilder.withBody(Bytes.toBytes(logMsg));
    s.initialize(e, "CF".getBytes());
    List<Row> actions = s.getActions();
    assertTrue(actions.size() == 1);
    assertTrue(actions.get(0) instanceof Put);
    Put put = (Put) actions.get(0);

    assertTrue(put.getFamilyCellMap().containsKey(s.cf));
    List<Cell> cells = put.getFamilyCellMap().get(s.cf);
    assertTrue(cells.size() == 1);

    Map<String, String> resultMap = Maps.newHashMap();
    for (Cell cell : cells) {
      resultMap.put(new String(CellUtil.cloneQualifier(cell)),
          new String(CellUtil.cloneValue(cell)));
    }

    assertTrue(resultMap.containsKey(
        RegexHBase2EventSerializer.COLUMN_NAME_DEFAULT));
    assertEquals("The sky is falling!",
        resultMap.get(RegexHBase2EventSerializer.COLUMN_NAME_DEFAULT));
  }
  @Test
  public void testRowIndexKey() throws Exception {
    RegexHBase2EventSerializer s = new RegexHBase2EventSerializer();
    Context context = new Context();
    context.put(RegexHBase2EventSerializer.REGEX_CONFIG,"^([^\t]+)\t([^\t]+)\t" + "([^\t]+)$");
    context.put(RegexHBase2EventSerializer.COL_NAME_CONFIG, "col1,col2,ROW_KEY");
    context.put("rowKeyIndex", "2");
    s.configure(context);

    String body = "val1\tval2\trow1";
    Event e = EventBuilder.withBody(Bytes.toBytes(body));
    s.initialize(e, "CF".getBytes());
    List<Row> actions = s.getActions();

    Put put = (Put)actions.get(0);

    List<Cell> cells = put.getFamilyCellMap().get(s.cf);
    assertTrue(cells.size() == 2);

    Map<String, String> resultMap = Maps.newHashMap();
    for (Cell cell : cells) {
      resultMap.put(new String(CellUtil.cloneQualifier(cell)),
          new String(CellUtil.cloneValue(cell)));
    }
    assertEquals("val1", resultMap.get("col1"));
    assertEquals("val2", resultMap.get("col2"));
    assertEquals("row1", Bytes.toString(put.getRow()));
  }

  @Test
  /* Test a common case where regex is used to parse apache log format. */
  public void testApacheRegex() throws Exception {
    RegexHBase2EventSerializer s = new RegexHBase2EventSerializer();
    Context context = new Context();
    context.put(RegexHBase2EventSerializer.REGEX_CONFIG,
        "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) \"([^ ]+) ([^ ]+)" +
        " ([^\"]+)\" (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\"[^\"]*\")" +
        " ([^ \"]*|\"[^\"]*\"))?");
    context.put(RegexHBase2EventSerializer.COL_NAME_CONFIG,
        "host,identity,user,time,method,request,protocol,status,size," +
        "referer,agent");
    s.configure(context);
    String logMsg = "33.22.11.00 - - [20/May/2011:07:01:19 +0000] " +
        "\"GET /wp-admin/css/install.css HTTP/1.0\" 200 813 " +
        "\"http://www.cloudera.com/wp-admin/install.php\" \"Mozilla/5.0 (comp" +
        "atible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)\"";

    Event e = EventBuilder.withBody(Bytes.toBytes(logMsg));
    s.initialize(e, "CF".getBytes());
    List<Row> actions = s.getActions();
    assertEquals(1, s.getActions().size());
    assertTrue(actions.get(0) instanceof Put);

    Put put = (Put) actions.get(0);
    assertTrue(put.getFamilyCellMap().containsKey(s.cf));
    List<Cell> cells = put.getFamilyCellMap().get(s.cf);
    assertTrue(cells.size() == 11);

    Map<String, String> resultMap = Maps.newHashMap();
    for (Cell cell : cells) {
      resultMap.put(new String(CellUtil.cloneQualifier(cell)),
          new String(CellUtil.cloneValue(cell)));
    }

    assertEquals("33.22.11.00", resultMap.get("host"));
    assertEquals("-", resultMap.get("identity"));
    assertEquals("-", resultMap.get("user"));
    assertEquals("[20/May/2011:07:01:19 +0000]", resultMap.get("time"));
    assertEquals("GET", resultMap.get("method"));
    assertEquals("/wp-admin/css/install.css", resultMap.get("request"));
    assertEquals("HTTP/1.0", resultMap.get("protocol"));
    assertEquals("200", resultMap.get("status"));
    assertEquals("813", resultMap.get("size"));
    assertEquals("\"http://www.cloudera.com/wp-admin/install.php\"",
        resultMap.get("referer"));
    assertEquals("\"Mozilla/5.0 (compatible; Yahoo! Slurp; " +
        "http://help.yahoo.com/help/us/ysearch/slurp)\"",
        resultMap.get("agent"));

    List<Increment> increments = s.getIncrements();
    assertEquals(0, increments.size());
  }

  @Test
  public void testRowKeyGeneration() {
    Context context = new Context();
    RegexHBase2EventSerializer s1 = new RegexHBase2EventSerializer();
    s1.configure(context);
    RegexHBase2EventSerializer s2 = new RegexHBase2EventSerializer();
    s2.configure(context);

    // Reset shared nonce to zero
    RegexHBase2EventSerializer.nonce.set(0);
    String randomString = RegexHBase2EventSerializer.randomKey;

    Event e1 = EventBuilder.withBody(Bytes.toBytes("body"));
    Event e2 = EventBuilder.withBody(Bytes.toBytes("body"));
    Event e3 = EventBuilder.withBody(Bytes.toBytes("body"));

    Calendar cal = mock(Calendar.class);
    when(cal.getTimeInMillis()).thenReturn(1L);

    s1.initialize(e1, "CF".getBytes());
    String rk1 = new String(s1.getRowKey(cal));
    assertEquals("1-" + randomString + "-0", rk1);

    when(cal.getTimeInMillis()).thenReturn(10L);
    s1.initialize(e2, "CF".getBytes());
    String rk2 = new String(s1.getRowKey(cal));
    assertEquals("10-" + randomString + "-1", rk2);

    when(cal.getTimeInMillis()).thenReturn(100L);
    s2.initialize(e3, "CF".getBytes());
    String rk3 = new String(s2.getRowKey(cal));
    assertEquals("100-" + randomString + "-2", rk3);

  }

  @Test
  /* Test depositing of the header information. */
  public void testDepositHeaders() throws Exception {
    Charset charset = Charset.forName("KOI8-R");
    RegexHBase2EventSerializer s = new RegexHBase2EventSerializer();
    Context context = new Context();
    context.put(RegexHBase2EventSerializer.DEPOSIT_HEADERS_CONFIG,
        "true");
    context.put(RegexHBase2EventSerializer.CHARSET_CONFIG,
               charset.toString());
    s.configure(context);

    String body = "body";
    Map<String, String> headers = Maps.newHashMap();
    headers.put("header1", "value1");
    headers.put("заголовок2", "значение2");

    Event e = EventBuilder.withBody(Bytes.toBytes(body), headers);
    s.initialize(e, "CF".getBytes());
    List<Row> actions = s.getActions();
    assertEquals(1, s.getActions().size());
    assertTrue(actions.get(0) instanceof Put);

    Put put = (Put) actions.get(0);
    assertTrue(put.getFamilyCellMap().containsKey(s.cf));
    List<Cell> cells = put.getFamilyCellMap().get(s.cf);
    assertTrue(cells.size() == 3);

    Map<String, byte[]> resultMap = Maps.newHashMap();
    for (Cell cell : cells) {
      resultMap.put(new String(CellUtil.cloneQualifier(cell), charset),
              CellUtil.cloneValue(cell));
    }

    assertEquals(body,
                 new String(resultMap.get(RegexHBase2EventSerializer.COLUMN_NAME_DEFAULT),
                 charset));
    assertEquals("value1", new String(resultMap.get("header1"), charset));
    assertArrayEquals("значение2".getBytes(charset), resultMap.get("заголовок2"));
    assertEquals("значение2".length(), resultMap.get("заголовок2").length);

    List<Increment> increments = s.getIncrements();
    assertEquals(0, increments.size());
  }
}
