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
package com.cloudera.flume.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.cloudera.flume.core.Event.Priority;

public class TestEvent {
  /**
   * Test full escaping - the replacement of %{...} strings by their
   * corresponding attributes.
   */
  @Test
  public void testEscaping() {
    Event e = new EventImpl("abcde".getBytes());
    String test = "/henry/%{customer}/test";
    e.set("customer", "cloudera".getBytes());
    assertEquals("Tag replacement of customer failed!", "/henry/cloudera/test",
        e.escapeString(test));
    test = "/henry/%{unknown}/test";
    assertEquals("Tag replacement of unknown tag failed!", "/henry//test", e
        .escapeString(test));
  }

  /**
   * Test an instance of the special-cased form of tag escaping
   */
  @Test
  public void testSpecialEscaping() {
    Event e = new EventImpl("abcde".getBytes());
    String test = "/henry/%{body}-test";
    test = e.escapeString(test);
    assertEquals("Tag replacement of host failed!", "/henry/abcde-test", test);
    test = e.escapeString("/henry/%f");
    assertEquals("Tag replacement of unknown single character tag failed!",
        "/henry/", test);
  }

  /**
   * Test the contains method in Event, used to check if a string contains a
   * replacable tag.
   */
  @Test
  public void testContains() {
    assertTrue("Didn't find expected tag", Event.containsTag("/henry/%{test}"));
    assertTrue("Didn't find expected shorthand tag", Event
        .containsTag("/henry/%h"));
    assertFalse("Found non-existant tag", Event.containsTag("/henry/test"));
  }

  /**
   * Test that the regex correctly decodes %% before %%{..}
   */
  @Test
  public void testMatchOrdering() {
    String test = "%%{henry}";
    Event e = new EventImpl("abcde".getBytes());
    test = e.escapeString(test);
    assertEquals("Replacement of tags in order failed: " + test, "%{henry}",
        test);
  }

  /**
   * Test the date
   */
  @Test
  public void testDateFormat() {
    String test = "%a";
    Event e = new EventImpl(new byte[0], 1267578391, Priority.INFO, 0,
        "localhost");
    test = e.escapeString(test);
    assertEquals("Replacement of tag by date failed: " + test, "Thu", test);
  }

  /**
   * Test getting an attribute names from a escape sequence.
   * 
   * TODO (jon) This assumes a US PST locale currently
   */
  @Test
  public void testAttributeNames() {
    String test = "%a %A %b %B %c %d %D %H %I %j %k %l %m";
    Event e = new EventImpl(new byte[0], 1267578391, Priority.INFO, 0,
        "localhost");
    System.out.println(e.escapeString(test));
    Map<String, String> mapping1 = e.getEscapeMapping(test);
    assertTrue(mapping1.get("weekday_short").equals("Thu"));
    assertTrue(mapping1.get("weekday_full").equals("Thursday"));
    assertTrue(mapping1.get("monthname_short").equals("Jan"));
    assertTrue(mapping1.get("monthname_full").equals("January"));
    assertTrue(mapping1.get("datetime").equals("Thu Jan 15 08:06:18 1970"));
    assertTrue(mapping1.get("day_of_month_xx").equals("15"));
    assertTrue(mapping1.get("date_short").equals("01/15/70"));
    assertTrue(mapping1.get("hour_24_xx").equals("08"));
    assertTrue(mapping1.get("hour_12_xx").equals("08"));
    assertTrue(mapping1.get("day_of_year_xxx").equals("015"));
    assertTrue(mapping1.get("hour_24").equals("8"));
    assertTrue(mapping1.get("hour_12").equals("8"));
    assertTrue(mapping1.get("month_xx").equals("01"));

    String test2 = "%M %p %s %S %t %y %Y %z %{host} %{priority} %{nanos} %{body}";
    System.out.println(e.escapeString(test2));
    Map<String, String> mapping2 = e.getEscapeMapping(test2);
    assertTrue(mapping2.get("minute_xx").equals("06"));
    assertTrue(mapping2.get("am_pm").equals("AM"));
    assertTrue(mapping2.get("unix_seconds").equals("1267578"));
    assertTrue(mapping2.get("seconds_xx").equals("18"));
    assertTrue(mapping2.get("unix_millis").equals("1267578391"));
    assertTrue(mapping2.get("year_xx").equals("70"));
    assertTrue(mapping2.get("year_xxxx").equals("1970"));
    assertTrue(mapping2.get("timezone_delta").equals("-0800"));
    assertTrue(mapping2.get("host").equals("localhost"));
    assertTrue(mapping2.get("priority").equals("INFO"));
    assertTrue(mapping2.get("nanos").equals("0"));
    assertTrue(mapping2.get("body").equals(""));
  }

  /**
   * Unhandled escape sequences just return the shorthand and an empty string
   * value
   * 
   * TODO (jon) This assumes a US PST locale currently
   */
  @Test
  public void testBadAttributeName() {
    Event e = new EventImpl(new byte[0], 1267578391, Priority.INFO, 0,
        "localhost");
    String test2 = "%v %r %u";
    System.out.println(e.escapeString(test2));
    Map<String, String> mapping = e.getEscapeMapping(test2);
    assertEquals(3, mapping.size());
    assertTrue(mapping.get("v").equals(""));
    assertTrue(mapping.get("r").equals(""));
    assertTrue(mapping.get("u").equals(""));
  }

}
