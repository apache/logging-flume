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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This is the abstract class for events in flume.
 */
abstract public class Event {
  static final Logger LOG = LoggerFactory.getLogger(Event.class);

  public final static String A_SERVICE = "service";

  public enum Priority {
    FATAL, ERROR, WARN, INFO, DEBUG, TRACE
  };

  /**
   * the body - a blob of raw bytes that contains the raw entry. Values can be
   * extracted from this body but must not change the body. This should never
   * return null. To change an even body, one should create a new event.
   */
  abstract public byte[] getBody();

  /**
   * the priority - user specified priority
   */
  abstract public Priority getPriority();

  /**
   * a time stamp - unix millis
   */
  abstract public long getTimestamp();

  /**
   * a nano time - for ordering if entries have the same millis
   */
  abstract public long getNanos();

  /**
   * Host name of the machine that generated this event.
   * 
   * TODO (jon) consider wrapping this. Chose string because it doesn't assume
   * ipv4 or ipv6, etc. May cause canonalicalization problems.
   */
  abstract public String getHost();

  /**
   * This gets a particular attribute added to an event. This an extensible
   * interface for "other" attributes.
   */
  abstract public byte[] get(String attr);

  /**
   * This sets a particular attribute to an event. Attributes are write once,
   * and overwrites of an attribute are not permitted. If a call attempts to
   * overwrite an attribute, a IllegalArgumentException will be thrown.
   * 
   * This method copies the reference, and does not copy the data. (shallow
   * copy, not deep)
   */
  abstract public void set(String attr, byte[] v)
      throws IllegalArgumentException;

  /**
   * Ideally this would be package private, but instead this method should
   * return a ReadOnly Map. This can be done by wrapping a map m with
   * Collections.unmodifiableMap(m).
   */
  abstract public Map<String, byte[]> getAttrs();

  /**
   * Constructs the union of this event and another. The body in this event is
   * preserved, as are any attributes that already exist in this event.
   */
  abstract public void merge(Event e);

  /**
   * Constructs the union of this event and another. The body of this event is
   * preserved, but all attributes in e are renamed with the supplied prefix to
   * try to avoid name collisions. In the event of a collision, attributes from
   * this event are preserved.
   */
  abstract public void hierarchicalMerge(String prefix, Event e);

  /**
   * These are useful to other classes which might want to search for tags in
   * strings.
   */
  final public static String TAG_REGEX = "\\%(\\w|\\%)|\\%\\{([\\w\\.-]+)\\}";
  final public static Pattern tagPattern = Pattern.compile(TAG_REGEX);

  /**
   * Returns true if in contains a substring matching TAG_REGEX (i.e. of the
   * form %{...} or %x.
   */
  public static boolean containsTag(String in) {
    return tagPattern.matcher(in).find();
  }

  protected String expandShorthand(char c) {
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
      LOG.warn("Unrecognized escape in event format string: %" + c);
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
   */
  protected String replaceShorthand(char c) {
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
    case 'p':
      formatString = "a";
      break;
    case 's':
      return "" + getTimestamp() / 1000;
    case 'S':
      formatString = "ss";
      break;
    case 't':
      // This is different from unix date (which would insert a tab character
      // here)
      return Long.valueOf(getTimestamp()).toString();
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
      LOG.warn("Unrecognized escape in event format string: %" + c);
      return "";
    }
    SimpleDateFormat format = new SimpleDateFormat(formatString);
    Date date = new Date(getTimestamp());
    return format.format(date);
  }

  /**
   * Looks up an attribute for a given tag name, but checks for some special
   * cases first. Used for string escaping.
   */
  protected String mapTagToString(String tag) {
    if (tag.equals("hostname") || tag.equals("host")) {
      return getHost();
    }
    if (tag.equals("nanos")) {
      return "" + getNanos();
    }
    if (tag.equals("priority")) {
      return getPriority().toString();
    }
    if (tag.equals("body")) {
      return new String(getBody());
    }

    byte[] attr = get(tag);
    if (attr != null) {
      return new String(attr);
    }
    return null;
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
  public String escapeString(String in) {
    Matcher matcher = tagPattern.matcher(in);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String replacement = "";
      // Group 2 is the %{...} pattern
      if (matcher.group(2) != null) {

        replacement = mapTagToString(matcher.group(2));
        if (replacement == null) {
          replacement = "";
          LOG.warn("Tag " + matcher.group(2) + " not found");
        }
      } else {
        // The %x pattern.
        // Since we know the match is a single character, we can
        // switch on that rather than the string.
        Preconditions.checkState(matcher.group(1) != null
            && matcher.group(1).length() == 1,
            "Expected to match single character tag in string " + in);
        replacement = replaceShorthand(matcher.group(1).charAt(0));
      }

      // The replacement string must have '$' and '\' chars escaped. This
      // replacement string is pretty arcane.
      //
      // replacee : '$' -> for java '\$' -> for regex "\\$"
      // replacement: '\$' -> for regex '\\\$' -> for java "\\\\\\$"
      //
      // replacee : '\' -> for java "\\" -> for regex "\\\\"
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
  public Map<String, String> getEscapeMapping(String in) {
    Map<String, String> mapping = new HashMap<String, String>();
    Matcher matcher = tagPattern.matcher(in);
    while (matcher.find()) {
      String replacement = "";
      // Group 2 is the %{...} pattern
      if (matcher.group(2) != null) {

        replacement = mapTagToString(matcher.group(2));
        if (replacement == null) {
          replacement = "";
          LOG.warn("Tag " + matcher.group(2) + " not found");
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
        replacement = replaceShorthand(c);
        mapping.put(expandShorthand(c), replacement);
      }
    }
    return mapping;

  }
}
