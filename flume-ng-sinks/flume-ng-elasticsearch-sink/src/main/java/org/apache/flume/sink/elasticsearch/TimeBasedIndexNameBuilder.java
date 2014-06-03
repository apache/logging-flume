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
package org.apache.flume.sink.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.formatter.output.BucketPath;

import java.util.TimeZone;

/**
 * Default index name builder. It prepares name of index using configured
 * prefix and current timestamp. Default format of name is prefix-yyyy-MM-dd".
 */
public class TimeBasedIndexNameBuilder implements
        IndexNameBuilder {

  public static final String DATE_FORMAT = "dateFormat";
  public static final String TIME_ZONE = "timeZone";

  public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
  public static final String DEFAULT_TIME_ZONE = "Etc/UTC";

  private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd",
      TimeZone.getTimeZone("Etc/UTC"));

  private String indexPrefix;

  @VisibleForTesting
  FastDateFormat getFastDateFormat() {
    return fastDateFormat;
  }

  /**
   * Gets the name of the index to use for an index request
   * @param event
   *          Event for which the name of index has to be prepared
   * @return index name of the form 'indexPrefix-formattedTimestamp'
   */
  @Override
  public String getIndexName(Event event) {
    TimestampedEvent timestampedEvent = new TimestampedEvent(event);
    long timestamp = timestampedEvent.getTimestamp();
    String realIndexPrefix = BucketPath.escapeString(indexPrefix, event.getHeaders());
    return new StringBuilder(realIndexPrefix).append('-')
      .append(fastDateFormat.format(timestamp)).toString();
  }
  
  @Override
  public String getIndexPrefix(Event event) {
    return BucketPath.escapeString(indexPrefix, event.getHeaders());
  }

  @Override
  public void configure(Context context) {
    String dateFormatString = context.getString(DATE_FORMAT);
    String timeZoneString = context.getString(TIME_ZONE);
    if (StringUtils.isBlank(dateFormatString)) {
      dateFormatString = DEFAULT_DATE_FORMAT;
    }
    if (StringUtils.isBlank(timeZoneString)) {
      timeZoneString = DEFAULT_TIME_ZONE;
    }
    fastDateFormat = FastDateFormat.getInstance(dateFormatString,
        TimeZone.getTimeZone(timeZoneString));
    indexPrefix = context.getString(ElasticSearchSinkConstants.INDEX_NAME);
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }
}
