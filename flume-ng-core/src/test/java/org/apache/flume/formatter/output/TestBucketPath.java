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

package org.apache.flume.formatter.output;

import org.apache.flume.Clock;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBucketPath {
  private static final TimeZone CUSTOM_TIMEZONE = new SimpleTimeZone(1, "custom-timezone");

  private Calendar cal;
  private Map<String, String> headers;
  private Map<String, String> headersWithTimeZone;

  @Before
  public void setUp() {
    cal = createCalendar(2012, 5, 23, 13, 46, 33, 234, null);
    headers = new HashMap<>();
    headers.put("timestamp", String.valueOf(cal.getTimeInMillis()));

    Calendar calWithTimeZone = createCalendar(2012, 5, 23, 13, 46, 33, 234, CUSTOM_TIMEZONE);
    headersWithTimeZone = new HashMap<>();
    headersWithTimeZone.put("timestamp", String.valueOf(calWithTimeZone.getTimeInMillis()));
  }

  /**
   * Tests if the internally cached SimpleDateFormat instances can be reused with different
   * TimeZone without interference.
   */
  @Test
  public void testDateFormatCache() {
    TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
    String test = "%c";
    BucketPath.escapeString(
            test, headers, utcTimeZone, false, Calendar.HOUR_OF_DAY, 12, false);
    String escapedString = BucketPath.escapeString(
            test, headers, false, Calendar.HOUR_OF_DAY, 12);
    System.out.println("Escaped String: " + escapedString);
    SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
    Date d = new Date(cal.getTimeInMillis());
    String expectedString = format.format(d);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  /**
   * Tests if the timestamp with the default timezone is properly rounded down
   * to 12 hours using "%c" ("EEE MMM d HH:mm:ss yyyy") formatting.
   */
  @Test
  public void testDateFormatHours() {
    String test = "%c";
    String escapedString = BucketPath.escapeString(
        test, headers, true, Calendar.HOUR_OF_DAY, 12);
    System.out.println("Escaped String: " + escapedString);

    Calendar cal2 = createCalendar(2012, 5, 23, 12, 0, 0, 0, null);

    SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
    Date d = new Date(cal2.getTimeInMillis());
    String expectedString = format.format(d);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  /**
   * Tests if the timestamp with the custom timezone is properly rounded down
   * to 12 hours using "%c" ("EEE MMM d HH:mm:ss yyyy") formatting.
   */
  @Test
  public void testDateFormatHoursTimeZone() {
    String test = "%c";
    String escapedString = BucketPath.escapeString(
        test, headersWithTimeZone, CUSTOM_TIMEZONE, true, Calendar.HOUR_OF_DAY, 12, false);
    System.out.println("Escaped String: " + escapedString);

    Calendar cal2 = createCalendar(2012, 5, 23, 12, 0, 0, 0, CUSTOM_TIMEZONE);

    SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
    format.setTimeZone(CUSTOM_TIMEZONE);

    Date d = new Date(cal2.getTimeInMillis());
    String expectedString = format.format(d);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  /**
   * Tests if the timestamp with the default timezone is properly rounded down
   * to 5 minutes using "%s" (seconds) formatting
   */
  @Test
  public void testDateFormatMinutes() {
    String test = "%s";
    String escapedString = BucketPath.escapeString(
        test, headers, true, Calendar.MINUTE, 5);
    System.out.println("Escaped String: " + escapedString);

    Calendar cal2 = createCalendar(2012, 5, 23, 13, 45, 0, 0, null);
    String expectedString = String.valueOf(cal2.getTimeInMillis() / 1000);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  /**
   * Tests if the timestamp with the custom timezone is properly rounded down
   * to 5 minutes using "%s" (seconds) formatting
   */
  @Test
  public void testDateFormatMinutesTimeZone() {
    String test = "%s";
    String escapedString = BucketPath.escapeString(
        test, headersWithTimeZone, CUSTOM_TIMEZONE, true, Calendar.MINUTE, 5, false);
    System.out.println("Escaped String: " + escapedString);

    Calendar cal2 = createCalendar(2012, 5, 23, 13, 45, 0, 0, CUSTOM_TIMEZONE);
    String expectedString = String.valueOf(cal2.getTimeInMillis() / 1000);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  /**
   * Tests if the timestamp with the default timezone is properly rounded down
   * to 5 seconds using "%s" (seconds) formatting
   */
  @Test
  public void testDateFormatSeconds() {
    String test = "%s";
    String escapedString = BucketPath.escapeString(
        test, headers, true, Calendar.SECOND, 5);
    System.out.println("Escaped String: " + escapedString);

    Calendar cal2 = createCalendar(2012, 5, 23, 13, 46, 30, 0, null);
    String expectedString = String.valueOf(cal2.getTimeInMillis() / 1000);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  /**
   * Tests if the timestamp with the custom timezone is properly rounded down
   * to 5 seconds using "%s" (seconds) formatting
   */
  @Test
  public void testDateFormatSecondsTimeZone() {
    String test = "%s";
    String escapedString = BucketPath.escapeString(
        test, headersWithTimeZone, CUSTOM_TIMEZONE, true, Calendar.SECOND, 5, false);
    System.out.println("Escaped String: " + escapedString);

    Calendar cal2 = createCalendar(2012, 5, 23, 13, 46, 30, 0, CUSTOM_TIMEZONE);
    String expectedString = String.valueOf(cal2.getTimeInMillis() / 1000);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  /**
   * Tests if the timestamp is properly formatted without rounding it down.
   */
  @Test
  public void testNoRounding() {
    String test = "%c";
    String escapedString = BucketPath.escapeString(
        test, headers, false, Calendar.HOUR_OF_DAY, 12);
    System.out.println("Escaped String: " + escapedString);
    SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
    Date d = new Date(cal.getTimeInMillis());
    String expectedString = format.format(d);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }


  @Test
  public void testNoPadding() {
    Calendar calender;
    Map<String, String> calender_timestamp;
    calender = Calendar.getInstance();

    //Check single digit dates
    calender.set(2014, (5 - 1), 3, 13, 46, 33);
    calender_timestamp = new HashMap<String, String>();
    calender_timestamp.put("timestamp", String.valueOf(calender.getTimeInMillis()));
    SimpleDateFormat format = new SimpleDateFormat("M-d");
    
    String test = "%n-%e"; // eg 5-3
    String escapedString = BucketPath.escapeString(
        test, calender_timestamp, false, Calendar.HOUR_OF_DAY, 12);
    Date d = new Date(calender.getTimeInMillis());
    String expectedString = format.format(d);
    
    //Check two digit dates
    calender.set(2014, (11 - 1), 13, 13, 46, 33);
    calender_timestamp.put("timestamp", String.valueOf(calender.getTimeInMillis()));
    escapedString += " " + BucketPath.escapeString(
        test, calender_timestamp, false, Calendar.HOUR_OF_DAY, 12);
    System.out.println("Escaped String: " + escapedString);
    d = new Date(calender.getTimeInMillis());
    expectedString += " " + format.format(d);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  @Test
  public void testDateFormatTimeZone() {
    TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
    String test = "%c";
    String escapedString = BucketPath.escapeString(
        test, headers, utcTimeZone, false, Calendar.HOUR_OF_DAY, 12, false);
    System.out.println("Escaped String: " + escapedString);
    SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
    format.setTimeZone(utcTimeZone);
    Date d = new Date(cal.getTimeInMillis());
    String expectedString = format.format(d);
    System.out.println("Expected String: " + expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  @Test
  public void testDateRace() {
    Clock mockClock = mock(Clock.class);
    DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
    long two = parser.parseMillis("2013-04-21T02:59:59-00:00");
    long three = parser.parseMillis("2013-04-21T03:00:00-00:00");
    when(mockClock.currentTimeMillis()).thenReturn(two, three);

    // save & modify static state (yuck)
    Clock origClock = BucketPath.getClock();
    BucketPath.setClock(mockClock);

    String pat = "%H:%M";
    String escaped = BucketPath.escapeString(pat,
        new HashMap<String, String>(),
        TimeZone.getTimeZone("UTC"), true, Calendar.MINUTE, 10, true);

    // restore static state
    BucketPath.setClock(origClock);

    Assert.assertEquals("Race condition detected", "02:50", escaped);
  }

  private static Calendar createCalendar(int year, int month, int day,
                                         int hour, int minute, int second, int ms,
                                         @Nullable TimeZone timeZone) {
    Calendar cal = (timeZone == null) ? Calendar.getInstance() : Calendar.getInstance(timeZone);
    cal.set(year, month, day, hour, minute, second);
    cal.set(Calendar.MILLISECOND, ms);
    return cal;
  }

  @Test
  public void testStaticEscapeStrings() {
    Map<String, String> staticStrings;
    staticStrings = new HashMap<>();

    try {
      InetAddress addr = InetAddress.getLocalHost();
      staticStrings.put("localhost", addr.getHostName());
      staticStrings.put("IP", addr.getHostAddress());
      staticStrings.put("FQDN", addr.getCanonicalHostName());
    } catch (UnknownHostException e) {
      Assert.fail("Test failed due to UnkownHostException");
    }

    TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
    String filePath = "%[localhost]/%[IP]/%[FQDN]";
    String realPath = BucketPath.escapeString(filePath, headers,
            utcTimeZone, false, Calendar.HOUR_OF_DAY, 12, false);
    String[] args = realPath.split("\\/");

    Assert.assertEquals(args[0],staticStrings.get("localhost"));
    Assert.assertEquals(args[1],staticStrings.get("IP"));
    Assert.assertEquals(args[2],staticStrings.get("FQDN"));

    StringBuilder s = new StringBuilder();
    s.append("Expected String: ").append(staticStrings.get("localhost"));
    s.append("/").append(staticStrings.get("IP")).append("/");
    s.append(staticStrings.get("FQDN"));

    System.out.println(s);
    System.out.println("Escaped String: " + realPath );
  }

  @Test (expected = RuntimeException.class)
  public void testStaticEscapeStringsNoKey() {
    Map<String, String> staticStrings;
    staticStrings = new HashMap<>();

    TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
    String filePath = "%[abcdefg]/%[IP]/%[FQDN]";
    String realPath = BucketPath.escapeString(filePath, headers,
            utcTimeZone, false, Calendar.HOUR_OF_DAY, 12, false);
  }

}
