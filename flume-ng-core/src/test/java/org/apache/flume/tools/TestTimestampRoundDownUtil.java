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

package org.apache.flume.tools;

import java.util.Calendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import junit.framework.Assert;

import org.junit.Test;

import javax.annotation.Nullable;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

public class TestTimestampRoundDownUtil {

  private static final TimeZone CUSTOM_TIMEZONE = new SimpleTimeZone(1, "custom-timezone");
  private static final Calendar BASE_CALENDAR_WITH_DEFAULT_TIMEZONE =
      createCalendar(2012, 5, 15, 15, 12, 54, 0, null);
  private static final Calendar BASE_CALENDAR_WITH_CUSTOM_TIMEZONE =
      createCalendar(2012, 5, 15, 15, 12, 54, 0, CUSTOM_TIMEZONE);

  /**
   * Tests if the timestamp with the default timezone is properly rounded down
   * to 60 seconds.
   */
  @Test
  public void testRoundDownTimeStampSeconds() {
    Calendar cal = BASE_CALENDAR_WITH_DEFAULT_TIMEZONE;
    Calendar cal2 = createCalendar(2012, 5, 15, 15, 12, 0, 0, null);

    long timeToVerify = cal2.getTimeInMillis();
    long ret = TimestampRoundDownUtil.roundDownTimeStampSeconds(cal.getTimeInMillis(), 60);
    System.out.println("Cal 1: " + cal.toString());
    System.out.println("Cal 2: " + cal2.toString());
    Assert.assertEquals(timeToVerify, ret);
  }

  /**
   * Tests if the timestamp with the custom timezone is properly rounded down
   * to 60 seconds.
   */
  @Test
  public void testRoundDownTimeStampSecondsWithTimeZone() {
    Calendar cal = BASE_CALENDAR_WITH_CUSTOM_TIMEZONE;
    Calendar cal2 = createCalendar(2012, 5, 15, 15, 12, 0, 0, CUSTOM_TIMEZONE);

    long timeToVerify = cal2.getTimeInMillis();
    long withoutTimeZone = TimestampRoundDownUtil.roundDownTimeStampSeconds(
        cal.getTimeInMillis(), 60);
    long withTimeZone = TimestampRoundDownUtil.roundDownTimeStampSeconds(
        cal.getTimeInMillis(), 60, CUSTOM_TIMEZONE);

    assertThat(withoutTimeZone, not(equalTo(timeToVerify)));
    Assert.assertEquals(withTimeZone, timeToVerify);
  }

  /**
   * Tests if the timestamp with the default timezone is properly rounded down
   * to 5 minutes.
   */
  @Test
  public void testRoundDownTimeStampMinutes() {
    Calendar cal = BASE_CALENDAR_WITH_DEFAULT_TIMEZONE;
    Calendar cal2 = createCalendar(2012, 5, 15, 15, 10, 0, 0, null);

    long timeToVerify = cal2.getTimeInMillis();
    long ret = TimestampRoundDownUtil.roundDownTimeStampMinutes(cal.getTimeInMillis(), 5);
    System.out.println("Cal 1: " + cal.toString());
    System.out.println("Cal 2: " + cal2.toString());
    Assert.assertEquals(timeToVerify, ret);
  }

  /**
   * Tests if the timestamp with the custom timezone is properly rounded down
   * to 5 minutes.
   */
  @Test
  public void testRoundDownTimeStampMinutesWithTimeZone() {
    Calendar cal = BASE_CALENDAR_WITH_CUSTOM_TIMEZONE;
    Calendar cal2 = createCalendar(2012, 5, 15, 15, 10, 0, 0, CUSTOM_TIMEZONE);

    long timeToVerify = cal2.getTimeInMillis();
    long withoutTimeZone = TimestampRoundDownUtil.roundDownTimeStampMinutes(
        cal.getTimeInMillis(), 5);
    long withTimeZone = TimestampRoundDownUtil.roundDownTimeStampMinutes(
        cal.getTimeInMillis(), 5, CUSTOM_TIMEZONE);

    assertThat(withoutTimeZone, not(equalTo(timeToVerify)));
    Assert.assertEquals(withTimeZone, timeToVerify);
  }

  /**
   * Tests if the timestamp with the default timezone is properly rounded down
   * to 2 hours.
   */
  @Test
  public void testRoundDownTimeStampHours() {
    Calendar cal = BASE_CALENDAR_WITH_DEFAULT_TIMEZONE;
    Calendar cal2 = createCalendar(2012, 5, 15, 14, 0, 0, 0, null);

    long timeToVerify = cal2.getTimeInMillis();
    long ret = TimestampRoundDownUtil.roundDownTimeStampHours(cal.getTimeInMillis(), 2);
    System.out.println("Cal 1: " + ret);
    System.out.println("Cal 2: " + cal2.toString());
    Assert.assertEquals(timeToVerify, ret);
  }

  /**
   * Tests if the timestamp with the custom timezone is properly rounded down
   * to 2 hours.
   */
  @Test
  public void testRoundDownTimeStampHoursWithTimeZone() {
    Calendar cal = BASE_CALENDAR_WITH_CUSTOM_TIMEZONE;
    Calendar cal2 = createCalendar(2012, 5, 15, 14, 0, 0, 0, CUSTOM_TIMEZONE);

    long timeToVerify = cal2.getTimeInMillis();
    long withoutTimeZone = TimestampRoundDownUtil.roundDownTimeStampHours(
        cal.getTimeInMillis(), 2);
    long withTimeZone = TimestampRoundDownUtil.roundDownTimeStampHours(
        cal.getTimeInMillis(), 2, CUSTOM_TIMEZONE);

    assertThat(withoutTimeZone, not(equalTo(timeToVerify)));
    Assert.assertEquals(withTimeZone, timeToVerify);
  }

  private static Calendar createCalendar(int year, int month, int day,
                                         int hour, int minute, int second, int ms,
                                         @Nullable TimeZone timeZone) {
    Calendar cal = (timeZone == null) ? Calendar.getInstance() : Calendar.getInstance(timeZone);
    cal.set(year, month, day, hour, minute, second);
    cal.set(Calendar.MILLISECOND, ms);
    return cal;
  }
}
