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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.flume.Clock;
import org.apache.flume.SystemClock;
import org.apache.flume.tools.TimestampRoundDownUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BucketPath {

  /**
   * These are useful to other classes which might want to search for tags in
   * strings.
   */
  public static final String TAG_REGEX = "%(\\w|%)|%\\{([\\w\\.-]+)\\}|%\\[(\\w+)\\]";
  public static final Pattern tagPattern = Pattern.compile(TAG_REGEX);

  private static Clock clock = new SystemClock();

  /**
   * Returns true if in contains a substring matching TAG_REGEX (i.e. of the
   * form %{...} or %x.
   */
  @VisibleForTesting
  @Deprecated
  public static boolean containsTag(String in) {
    return tagPattern.matcher(in).find();
  }

  @VisibleForTesting
  @Deprecated
  public static String expandShorthand(char c) {
    // It's a date
    switch (c) {
      case 'a':
        return "weekday_short";
      case 'A':
        return "weekday_full";
      case 'b':
        return "monthname_short";
      case 'B':
        return "monthname_full";
      case 'c':
        return "datetime";
      case 'd':
        return "day_of_month_xx"; // two digit
      case 'e':
        return "day_of_month_x"; // 1 or 2 digit
      case 'D':
        return "date_short"; // "MM/dd/yy";
      case 'H':
        return "hour_24_xx";
      case 'I':
        return "hour_12_xx";
      case 'j':
        return "day_of_year_xxx"; // three digits
      case 'k':
        return "hour_24"; // 1 or 2 digits
      case 'l':
        return "hour_12"; // 1 or 2 digits
      case 'm':
        return "month_xx";
      case 'n':
        return "month_x"; // 1 or 2 digits
      case 'M':
        return "minute_xx";
      case 'p':
        return "am_pm";
      case 's':
        return "unix_seconds";
      case 'S':
        return "seconds_xx";
      case 't':
        // This is different from unix date (which would insert a tab character
        // here)
        return "unix_millis";
      case 'y':
        return "year_xx";
      case 'Y':
        return "year_xxxx";
      case 'z':
        return "timezone_delta";
      default:
        // LOG.warn("Unrecognized escape in event format string: %" + c);
        return "" + c;
    }

  }

  /**
   * Hardcoded lookups for %x style escape replacement. Add your own!
   *
   * All shorthands are Date format strings, currently.
   *
   * Returns the empty string if an escape is not recognized.
   *
   * Dates follow the same format as unix date, with a few exceptions.
   *
   * <p>This static method will be REMOVED in a future version of Flume</p>
   *
   */
  @VisibleForTesting
  @Deprecated
  public static String replaceShorthand(char c, Map<String, String> headers) {
    return replaceShorthand(c, headers, false, 0, 0);
  }

  /**
   * A wrapper around
   * {@link BucketPath#replaceShorthand(char, Map, TimeZone, boolean, int,
   * int, boolean)}
   * with the timezone set to the default.
   *
   * <p>This static method will be REMOVED in a future version of Flume</p>
   */
  @VisibleForTesting
  @Deprecated
  public static String replaceShorthand(char c, Map<String, String> headers,
      boolean needRounding, int unit, int roundDown) {
    return replaceShorthand(c, headers, null, needRounding, unit, roundDown,
      false);
  }

  /**
   * Hardcoded lookups for %x style escape replacement. Add your own!
   *
   * All shorthands are Date format strings, currently.
   *
   * Returns the empty string if an escape is not recognized.
   *
   * Dates follow the same format as unix date, with a few exceptions.
   *
   * <p>This static method will be REMOVED in a future version of Flume</p>
   *
   * @param c - The character to replace.
   * @param headers - Event headers
   * @param timeZone - The timezone to use for formatting the timestamp
   * @param needRounding - Should the timestamp be rounded down?
   * @param unit - if needRounding is true, what unit to round down to. This
   * must be one of the units specified by {@link java.util.Calendar} -
   * HOUR, MINUTE or SECOND. Defaults to second, if none of these are present.
   * Ignored if needRounding is false.
   * @param roundDown - if needRounding is true,
   * The time should be rounded to the largest multiple of this
   * value, smaller than the time supplied, defaults to 1, if <= 0(rounds off
   * to the second/minute/hour immediately lower than the timestamp supplied.
   * Ignored if needRounding is false.
   *
   * @return
   */
  @VisibleForTesting
  @Deprecated
  public static String replaceShorthand(char c, Map<String, String> headers,
      TimeZone timeZone, boolean needRounding, int unit, int roundDown,
      boolean useLocalTimestamp) {
    long ts = 0;
    if (useLocalTimestamp) {
      ts = clock.currentTimeMillis();
    }
    return replaceShorthand(c, headers, timeZone, needRounding, unit,
        roundDown, false, ts);
  }
  
  protected static final ThreadLocal<HashMap<String, SimpleDateFormat>> simpleDateFormatCache =
      new ThreadLocal<HashMap<String, SimpleDateFormat>>() {

        @Override
        protected HashMap<String, SimpleDateFormat> initialValue() {
          return new HashMap<String, SimpleDateFormat>();
        }
      };

  protected static SimpleDateFormat getSimpleDateFormat(String string) {
    HashMap<String, SimpleDateFormat> localCache = simpleDateFormatCache.get();

    SimpleDateFormat simpleDateFormat = localCache.get(string);
    if (simpleDateFormat == null) {
      simpleDateFormat = new SimpleDateFormat(string);
      localCache.put(string, simpleDateFormat);
      simpleDateFormatCache.set(localCache);
    }

    return simpleDateFormat;
  }

  /**
   * Not intended as a public API
   */
  @VisibleForTesting
  protected static String replaceStaticString(String key) {
    String replacementString = "";
    try {
      InetAddress addr = InetAddress.getLocalHost();
      switch (key.toLowerCase()) {
        case "localhost":
          replacementString = addr.getHostName();
          break;
        case "ip":
          replacementString = addr.getHostAddress();
          break;
        case "fqdn":
          replacementString = addr.getCanonicalHostName();
          break;
        default:
          throw new RuntimeException("The static escape string '" + key + "'"
                  + " was provided but does not match any of (localhost,IP,FQDN)");
      }
    } catch (UnknownHostException e) {
      throw new RuntimeException("Flume wasn't able to parse the static escape "
              + " sequence '" + key + "' due to UnkownHostException.", e);
    }
    return replacementString;
  }

  /**
   * Not intended as a public API
   */
  @VisibleForTesting
  protected static String replaceShorthand(char c, Map<String, String> headers,
      TimeZone timeZone, boolean needRounding, int unit, int roundDown,
      boolean useLocalTimestamp, long ts) {

    String timestampHeader = null;
    try {
      if (!useLocalTimestamp) {
        timestampHeader = headers.get("timestamp");
        Preconditions.checkNotNull(timestampHeader, "Expected timestamp in " +
            "the Flume event headers, but it was null");
        ts = Long.valueOf(timestampHeader);
      } else {
        timestampHeader = String.valueOf(ts);
      }
    } catch (NumberFormatException e) {
      throw new RuntimeException("Flume wasn't able to parse timestamp header"
        + " in the event to resolve time based bucketing. Please check that"
        + " you're correctly populating timestamp header (for example using"
        + " TimestampInterceptor source interceptor).", e);
    }

    if (needRounding) {
      ts = roundDown(roundDown, unit, ts, timeZone);
    }

    // It's a date
    String formatString = "";
    switch (c) {
      case '%':
        return "%";
      case 'a':
        formatString = "EEE";
        break;
      case 'A':
        formatString = "EEEE";
        break;
      case 'b':
        formatString = "MMM";
        break;
      case 'B':
        formatString = "MMMM";
        break;
      case 'c':
        formatString = "EEE MMM d HH:mm:ss yyyy";
        break;
      case 'd':
        formatString = "dd";
        break;
      case 'e':
        formatString = "d";
        break;
      case 'D':
        formatString = "MM/dd/yy";
        break;
      case 'H':
        formatString = "HH";
        break;
      case 'I':
        formatString = "hh";
        break;
      case 'j':
        formatString = "DDD";
        break;
      case 'k':
        formatString = "H";
        break;
      case 'l':
        formatString = "h";
        break;
      case 'm':
        formatString = "MM";
        break;
      case 'M':
        formatString = "mm";
        break;
      case 'n':
        formatString = "M";
        break;
      case 'p':
        formatString = "a";
        break;
      case 's':
        return "" + (ts / 1000);
      case 'S':
        formatString = "ss";
        break;
      case 't':
        // This is different from unix date (which would insert a tab character
        // here)
        return timestampHeader;
      case 'y':
        formatString = "yy";
        break;
      case 'Y':
        formatString = "yyyy";
        break;
      case 'z':
        formatString = "ZZZ";
        break;
      default:
        // LOG.warn("Unrecognized escape in event format string: %" + c);
        return "";
    }

    SimpleDateFormat format = getSimpleDateFormat(formatString);
    if (timeZone != null) {
      format.setTimeZone(timeZone);
    } else {
      format.setTimeZone(TimeZone.getDefault());
    }

    Date date = new Date(ts);
    return format.format(date);
  }

  private static long roundDown(int roundDown, int unit, long ts, TimeZone timeZone) {
    long timestamp = ts;
    if (roundDown <= 0) {
      roundDown = 1;
    }
    switch (unit) {
      case Calendar.SECOND:
        timestamp = TimestampRoundDownUtil.roundDownTimeStampSeconds(
            ts, roundDown, timeZone);
        break;
      case Calendar.MINUTE:
        timestamp = TimestampRoundDownUtil.roundDownTimeStampMinutes(
            ts, roundDown, timeZone);
        break;
      case Calendar.HOUR_OF_DAY:
        timestamp = TimestampRoundDownUtil.roundDownTimeStampHours(
            ts, roundDown, timeZone);
        break;
      default:
        timestamp = ts;
        break;
    }
    return timestamp;
  }

  /**
   * Replace all substrings of form %{tagname} with get(tagname).toString() and
   * all shorthand substrings of form %x with a special value.
   *
   * Any unrecognized / not found tags will be replaced with the empty string.
   *
   * TODO(henry): we may want to consider taking this out of Event and into a
   * more general class when we get more use cases for this pattern.
   */
  public static String escapeString(String in, Map<String, String> headers) {
    return escapeString(in, headers, false, 0, 0);
  }

  /**
   * A wrapper around
   * {@link BucketPath#escapeString(String, Map, TimeZone, boolean, int, int,
   boolean)}
   * with the timezone set to the default.
   */
  public static String escapeString(String in, Map<String, String> headers,
      boolean needRounding, int unit, int roundDown) {
    return escapeString(in, headers, null, needRounding, unit, roundDown,
      false);
  }

  /**
   * Replace all substrings of form %{tagname} with get(tagname).toString() and
   * all shorthand substrings of form %x with a special value.
   *
   * Any unrecognized / not found tags will be replaced with the empty string.
   *
   * TODO(henry): we may want to consider taking this out of Event and into a
   * more general class when we get more use cases for this pattern.
   *
   * @param needRounding - Should the timestamp be rounded down?
   * @param unit - if needRounding is true, what unit to round down to. This
   * must be one of the units specified by {@link java.util.Calendar} -
   * HOUR, MINUTE or SECOND. Defaults to second, if none of these are present.
   * Ignored if needRounding is false.
   * @param roundDown - if needRounding is true,
   * The time should be rounded to the largest multiple of this
   * value, smaller than the time supplied, defaults to 1, if <= 0(rounds off
   * to the second/minute/hour immediately lower than the timestamp supplied.
   * Ignored if needRounding is false.
   * @return Escaped string.
   */
  public static String escapeString(String in, Map<String, String> headers,
      TimeZone timeZone, boolean needRounding, int unit, int roundDown,
      boolean useLocalTimeStamp) {

    long ts = clock.currentTimeMillis();

    Matcher matcher = tagPattern.matcher(in);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String replacement = "";
      // Group 2 is the %{...} pattern
      if (matcher.group(2) != null) {

        replacement = headers.get(matcher.group(2));
        if (replacement == null) {
          replacement = "";
//          LOG.warn("Tag " + matcher.group(2) + " not found");
        }

      // Group 3 is the %[...] pattern.
      } else if (matcher.group(3) != null) {
        replacement = replaceStaticString(matcher.group(3));

      } else {
        // The %x pattern.
        // Since we know the match is a single character, we can
        // switch on that rather than the string.
        Preconditions.checkState(matcher.group(1) != null
            && matcher.group(1).length() == 1,
            "Expected to match single character tag in string " + in);
        char c = matcher.group(1).charAt(0);
        replacement = replaceShorthand(c, headers, timeZone,
            needRounding, unit, roundDown, useLocalTimeStamp, ts);
      }

      // The replacement string must have '$' and '\' chars escaped. This
      // replacement string is pretty arcane.
      //
      // replace : '$' -> for java '\$' -> for regex "\\$"
      // replacement: '\$' -> for regex '\\\$' -> for java "\\\\\\$"
      //
      // replace : '\' -> for java "\\" -> for regex "\\\\"
      // replacement: '\\' -> for regex "\\\\" -> for java "\\\\\\\\"

      // note: order matters
      replacement = replacement.replaceAll("\\\\", "\\\\\\\\");
      replacement = replacement.replaceAll("\\$", "\\\\\\$");

      matcher.appendReplacement(sb, replacement);
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  /**
   * Instead of replacing escape sequences in a string, this method returns a
   * mapping of an attribute name to the value based on the escape sequence
   * found in the argument string.
   */
  @VisibleForTesting
  @Deprecated
  public static Map<String, String> getEscapeMapping(String in,
      Map<String, String> headers) {
    return getEscapeMapping(in, headers, false, 0, 0);
  }

  @VisibleForTesting
  @Deprecated
  public static Map<String, String> getEscapeMapping(String in,
      Map<String, String> headers, boolean needRounding,
      int unit, int roundDown) {
    Map<String, String> mapping = new HashMap<String, String>();
    Matcher matcher = tagPattern.matcher(in);
    while (matcher.find()) {
      String replacement = "";
      // Group 2 is the %{...} pattern
      if (matcher.group(2) != null) {

        replacement = headers.get(matcher.group(2));

        if (replacement == null) {
          replacement = "";
//          LOG.warn("Tag " + matcher.group(2) + " not found");
        }
        mapping.put(matcher.group(2), replacement);
      } else {
        // The %x pattern.
        // Since we know the match is a single character, we can
        // switch on that rather than the string.
        Preconditions.checkState(matcher.group(1) != null
            && matcher.group(1).length() == 1,
            "Expected to match single character tag in string " + in);
        char c = matcher.group(1).charAt(0);
        replacement = replaceShorthand(c, headers,
            needRounding, unit, roundDown);
        mapping.put(expandShorthand(c), replacement);
      }
    }
    return mapping;

  }

  /*
   * May not be called from outside unit tests.
   */
  @VisibleForTesting
  public static void setClock(Clock clk) {
    clock = clk;
  }

  /*
   * May not be called from outside unit tests.
   */
  @VisibleForTesting
  public static Clock getClock() {
    return clock;
  }
}

