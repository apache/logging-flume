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
package com.cloudera.flume.core.extractors;

import java.io.IOException;
import java.util.Calendar;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.handlers.hdfs.EscapedCustomDfsSink;
import com.google.common.base.Preconditions;

/**
 * Parses a date string, gathered by fetching from the provided event attribute,
 * based on the provided pattern into a date.
 * The major components are then assigned to new event fields using the optional
 * prepender for naming convention.
 * 
 * Example(2010-07-29 12:00):
 *   exDate("date", "yyyy-MM-dd HH:mm")
 *   {date_year:2010, date_month:07, date_day:29, date_hr:12, date_min:00}
 *   
 *   exDate("date", "yyyy-MM-dd", "mydate_")
 *   {mydate_year:2010, mydate_month:07, mydate_day:29, mydate_hr:12, mydate_min:00}
 *   
 *   exDate("date", "yyyy-MM-dd", "mydate_", "false")
 *   {mydate_year:2010, mydate_month:7, mydate_day:29, mydate_hr:12, mydate_min:0}
 */
public class DateExtractor extends EventSinkDecorator<EventSink> {
  static final Logger LOG = LoggerFactory.getLogger(EscapedCustomDfsSink.class);
  protected final String attr;
  protected final String pat;
  protected final String pre;
  protected final Boolean padding;
  protected SimpleDateFormat date;

  public DateExtractor(EventSink snk, String attr, String pattern, String prefix, Boolean padding) {
    super(snk);
    
    //What attribute to look at for initial value
    this.attr = attr;
    //SimpleDateFormat format string to use when parsing date
    this.pat = pattern;
    //Prefix to use for new attribute names
    this.pre = prefix;
    //Flag whether or not to add zero padding.
    this.padding = padding;
    
    this.date = new SimpleDateFormat(pat);
  }
  public DateExtractor(EventSink snk, String attr, String pattern) {
    this(snk, attr, pattern, "date", true);
  }
  public DateExtractor(EventSink snk, String attr, String pattern, String prefix) {
    this(snk, attr, pattern, prefix, true);
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    String dateStr = new String(e.get(attr));
    
    if (dateStr.length() == 0) {
      super.append(e);
      LOG.warn("Date String is missing or empty");
      return;
    }
    
    Calendar tDate = Calendar.getInstance();
    
    try {
      tDate.setTime(date.parse(dateStr));
    } catch (ParseException e1) {
      super.append(e);
      LOG.warn("Failed to parse date", e1);
      return;
    }
    Integer day = tDate.get(Calendar.DAY_OF_MONTH);
    Integer month = tDate.get(Calendar.MONTH) + 1;
    Integer year = tDate.get(Calendar.YEAR);
    Integer hr = tDate.get(Calendar.HOUR_OF_DAY);
    Integer min = tDate.get(Calendar.MINUTE);
    Integer sec = tDate.get(Calendar.SECOND);
    
    String sday = Integer.toString(day);
    String smonth = Integer.toString(month);
    String syear = Integer.toString(year);
    String shr = Integer.toString(hr);
    String smin = Integer.toString(min);
    String ssec = Integer.toString(sec);
    
    if (padding) {
      sday = String.format("%02d", day);
      smonth = String.format("%02d", month);
      shr = String.format("%02d", hr);
      smin = String.format("%02d", min);
      ssec = String.format("%02d", sec);
    }
    
    Attributes.setString(e, pre + "day", sday);
    Attributes.setString(e, pre + "month", smonth);
    Attributes.setString(e, pre + "year", syear);
    Attributes.setString(e, pre + "hr", shr);
    Attributes.setString(e, pre + "min", smin);
    Attributes.setString(e, pre + "sec", ssec);
    
    super.append(e);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length >= 2, "usage: exDate(field, pattern[, prefix=\"prefix\"[,padding=\"true\"]])");

        String attr = argv[0];
        String pattern = argv[1];
        String prefix = "date";
        Boolean padding = true;
        if (argv.length >= 3) {
          prefix = argv[2];
        }
        if (argv.length >= 4) {
          if (argv[3] == "true") {
            padding = true;
          } else if (argv[3] == "false") {
            padding = false;
          }
        }

        EventSinkDecorator<EventSink> snk = new DateExtractor(null, attr, pattern, prefix, padding);
        return snk;
      }
    };
  }
}
