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

import com.google.common.base.Preconditions;

public class TimestampRoundDownUtil {

  /**
   *
   * @param timestamp - The time stamp to be rounded down.
   * @param roundDownSec - The <tt>timestamp</tt> is rounded down to the largest
   * multiple of <tt>roundDownSec</tt> seconds
   * less than or equal to <tt>timestamp.</tt> Should be between 0 and 60.
   * @return - Rounded down timestamp
   * @throws IllegalStateException
   */
  public static long roundDownTimeStampSeconds(long timestamp,
      int roundDownSec) throws IllegalStateException {
    Preconditions.checkArgument(roundDownSec > 0 && roundDownSec <=60,
        "RoundDownSec must be > 0 and <=60");
    Calendar cal = roundDownField(timestamp, Calendar.SECOND, roundDownSec);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTimeInMillis();
  }

  /**
   *
   * @param timestamp - The time stamp to be rounded down.
   * @param roundDownMins - The <tt>timestamp</tt> is rounded down to the
   * largest multiple of <tt>roundDownMins</tt> minutes less than
   * or equal to <tt>timestamp.</tt> Should be between 0 and 60.
   * @return - Rounded down timestamp
   * @throws IllegalStateException
   */
  public static long roundDownTimeStampMinutes(long timestamp,
      int roundDownMins) throws IllegalStateException {
    Preconditions.checkArgument(roundDownMins > 0 && roundDownMins <=60,
        "RoundDown must be > 0 and <=60");
    Calendar cal = roundDownField(timestamp, Calendar.MINUTE, roundDownMins);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTimeInMillis();

  }

  /**
   *
   * @param timestamp - The time stamp to be rounded down.
   * @param roundDownHours - The <tt>timestamp</tt> is rounded down to the
   * largest multiple of <tt>roundDownHours</tt> hours less than
   * or equal to <tt>timestamp.</tt> Should be between 0 and 24.
   * @return - Rounded down timestamp
   * @throws IllegalStateException
   */
  public static long roundDownTimeStampHours(long timestamp,
      int roundDownHours) throws IllegalStateException {
    Preconditions.checkArgument(roundDownHours > 0 && roundDownHours <=24,
        "RoundDown must be > 0 and <=24");
    Calendar cal = roundDownField(timestamp,
        Calendar.HOUR_OF_DAY, roundDownHours);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTimeInMillis();
  }

  private static Calendar roundDownField(
      long timestamp, int field, int roundDown){
    Preconditions.checkArgument(timestamp > 0, "Timestamp must be positive");
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(timestamp);
    int fieldVal = cal.get(field);
    int remainder =  (fieldVal % roundDown);
    cal.set(field, fieldVal - remainder);
    return cal;
  }
}
