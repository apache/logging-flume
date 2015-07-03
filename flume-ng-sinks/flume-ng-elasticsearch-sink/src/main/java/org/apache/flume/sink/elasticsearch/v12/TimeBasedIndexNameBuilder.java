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
package org.apache.flume.sink.elasticsearch.v12;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.formatter.output.BucketPath;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by omallassi on 22/06/2015.
 */
public class TimeBasedIndexNameBuilder extends org.apache.flume.sink.elasticsearch.TimeBasedIndexNameBuilder {


    private static final Logger logger = LoggerFactory
            .getLogger(TimeBasedIndexNameBuilder.class);

    @Override
    public String getIndexName(Event event) {
        //get the timestamp of the event
        String timestampString = event.getHeaders().get("timestamp");
        if (StringUtils.isBlank(timestampString))
            timestampString = event.getHeaders().get("@timestamp");

        long indexName = 0;
        if (!StringUtils.isBlank(timestampString))
            indexName = Long.valueOf(timestampString);
        else
            indexName = DateTimeUtils.currentTimeMillis();

        String realIndexPrefix = BucketPath.escapeString(indexPrefix, event.getHeaders());

        return new StringBuilder(realIndexPrefix).append('-')
                .append(fastDateFormat.format(indexName)).toString();
    }
}
