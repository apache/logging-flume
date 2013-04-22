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


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.flume.Clock;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBucketPath {
  Calendar cal;
  Map<String, String> headers;
  @Before
  public void setUp(){
    cal = Calendar.getInstance();
    cal.set(2012, 5, 23, 13, 46, 33);
    cal.set(Calendar.MILLISECOND, 234);
    headers = new HashMap<String, String>();
    headers.put("timestamp", String.valueOf(cal.getTimeInMillis()));
  }
  @Test
  public void testDateFormatHours() {
    String test = "%c";
    String escapedString = BucketPath.escapeString(
        test, headers, true, Calendar.HOUR_OF_DAY, 12);
    System.out.println("Escaped String: " + escapedString);
    Calendar cal2 = Calendar.getInstance();
    cal2.set(2012, 5, 23, 12, 0, 0);
    cal2.set(Calendar.MILLISECOND, 0);
    SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
    Date d = new Date(cal2.getTimeInMillis());
    String expectedString = format.format(d);
    System.out.println("Expected String: "+ expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  @Test
  public void testDateFormatMinutes() {
    String test = "%s";
    String escapedString = BucketPath.escapeString(
        test, headers, true, Calendar.MINUTE, 5);
    System.out.println("Escaped String: " + escapedString);
    Calendar cal2 = Calendar.getInstance();
    cal2.set(2012, 5, 23, 13, 45, 0);
    cal2.set(Calendar.MILLISECOND, 0);
    String expectedString = String.valueOf(cal2.getTimeInMillis()/1000);
    System.out.println("Expected String: "+ expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  @Test
  public void testDateFormatSeconds() {
    String test = "%s";
    String escapedString = BucketPath.escapeString(
        test, headers, true, Calendar.SECOND, 5);
    System.out.println("Escaped String: " + escapedString);
    Calendar cal2 = Calendar.getInstance();
    cal2.set(2012, 5, 23, 13, 46, 30);
    cal2.set(Calendar.MILLISECOND, 0);
    String expectedString = String.valueOf(cal2.getTimeInMillis()/1000);
    System.out.println("Expected String: "+ expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  @Test
  public void testNoRounding(){
    String test = "%c";
    String escapedString = BucketPath.escapeString(
        test, headers, false, Calendar.HOUR_OF_DAY, 12);
    System.out.println("Escaped String: " + escapedString);
    SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
    Date d = new Date(cal.getTimeInMillis());
    String expectedString = format.format(d);
    System.out.println("Expected String: "+ expectedString);
    Assert.assertEquals(expectedString, escapedString);
  }

  @Test
  public void testDateFormatTimeZone(){
    TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
    String test = "%c";
    String escapedString = BucketPath.escapeString(
        test, headers, utcTimeZone, false, Calendar.HOUR_OF_DAY, 12, false);
    System.out.println("Escaped String: " + escapedString);
    SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
    format.setTimeZone(utcTimeZone);
    Date d = new Date(cal.getTimeInMillis());
    String expectedString = format.format(d);
    System.out.println("Expected String: "+ expectedString);
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
}
