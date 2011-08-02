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
package com.cloudera.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.log4j.helpers.ISO8601DateFormat;

/**
 * A factory for some standard date formats.
 * 
 * SimpleDateFormats are not threadsafe, generate one for each thread instead of
 * having a static ones.
 * 
 * TODO (jon) have a mechanism to set TimeZone. (right now this assumes TZ from
 * the current locale)
 */
public class DateUtils {

  // TODO(jon) make this cache the dateformat and use a specific instance per
  // thread.

  /**
   * According to http://en.wikipedia.org/wiki/ISO_8601
   */
  static public DateFormat getISO8601() {
    return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:Z");
  }

  /***
   * This is the log4j ISO8601 format/class
   */
  static public DateFormat getLog4jISO8601() {
    // This is the spec, but log4j doesn't use 'T' to separate date and time.

    return new ISO8601DateFormat();
  }

  /**
   * See RFC822 section 5. http://tools.ietf.org/html/rfc822#section-5
   */
  static public DateFormat getRFC822() {
    return new SimpleDateFormat("EEE', 'dd' 'MMM' 'yy' 'HH:mm:ss' 'Z",
        Locale.US);
  }

  /**
   * See http://tools.ietf.org/html/rfc2822#section-3.3
   * 
   * This update forces 4 digit years
   */
  static public DateFormat getRFC2822() {
    return new SimpleDateFormat("EEE', 'dd' 'MMM' 'yyyy' 'HH:mm:ss' 'Z",
        Locale.US);
  }

  /**
   * Log4j's ISO8601 does not follow ISO8601 spec.
   */
  static public String asLog4jISO8601(Date date) {
    // TODO(jon) WARNING: This sometimes returns strings padded with '\x0'
    // characters. WTF!
    DateFormat ISO8601 = getLog4jISO8601();
    return ISO8601.format(date);
  }

  /**
   * This version follows the spec. See http://www.w3.org/TR/NOTE-datetime
   */
  static public String asISO8601(Date date) {
    DateFormat ISO8601 = getISO8601();
    String result = ISO8601.format(date);

    // convert YYYYMMDDTHH:mm:ss+HH00 into YYYYMMDDTHH:mm:ss+HH:00
    // - note the added colon for the Timezone
    result = result.substring(0, result.length() - 2) + ":"
        + result.substring(result.length() - 2);
    return result;
  }

}
