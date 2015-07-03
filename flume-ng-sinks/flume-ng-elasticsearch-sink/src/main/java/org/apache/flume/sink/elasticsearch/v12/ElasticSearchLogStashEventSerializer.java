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
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.elasticsearch.ContentBuilderUtil;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Create a event log compatible with the new ElasticSearch / Kibana format.
 * <ul>
 * <li>If <code>timestamp</code> or <code>@timestamp</code> is part of the header, it will be used and set as <code>@timestamp</code> property.</li>
 * <li>String will be added as String</li>
 * <li>Numbers will be handled as Long or Double</li>
 * <li>timestamp format is not necessary long but can used any defined SimpleDateFormat pattern</li>
 * </ul>
 *
 * @see <a href="http://blog.tpa.me.uk/2013/11/20/logstash-v1-1-v1-2-json-event-layout-format-change/">http://blog.tpa.me.uk/2013/11/20/logstash-v1-1-v1-2-json-event-layout-format-change/</a>
 * <p>
 * A typical usage with flume would be:
 * <code>
 * a1.sources.r1.interceptors = i1
 * a1.sources.r1.interceptors.i1.type = regex_extractor
 * a1.sources.r1.interceptors.i1.regex = your regexp
 * a1.sources.r1.interceptors.i1.serializers = s1 s2 s3 s4 s5 s6 s7
 * a1.sources.r1.interceptors.i1.serializers.s1.name = timestamp
 * a1.sources.r1.interceptors.i1.serializers.s1.type = org.apache.flume.interceptor.RegexExtractorInterceptorMillisSerializer
 * a1.sources.r1.interceptors.i1.serializers.s1.pattern = yyyy-MM-dd HH:mm:ss,SSS
 * a1.sources.r1.interceptors.i1.serializers.s2.name = level
 * ...
 * a1.sinks.k1.type = elasticsearch
 * a1.sinks.k1.hostNames = 10.25.6.41:9300,10.25.6.47:9300
 * ...
 * a1.sinks.k1.serializer = org.apache.flume.sink.elasticsearch.v12.ElasticSearchLogStashEventSerializer
 * a1.sinks.k1.indexNameBuilder = org.apache.flume.sink.elasticsearch.v12.TimeBasedIndexNameBuilder
 * </code>
 */
public class ElasticSearchLogStashEventSerializer implements ElasticSearchEventSerializer {

    private static final Logger logger = LoggerFactory
            .getLogger(ElasticSearchLogStashEventSerializer.class);

//    public static String TIMESTAMP_FORMAT = "timestamp.format";
//    private SimpleDateFormat timestampformatter;

    private void appendBody(XContentBuilder builder, Event event)
            throws IOException, UnsupportedEncodingException {
        byte[] body = event.getBody();
        ContentBuilderUtil.appendField(builder, "message", body);
    }

    private void appendHeaders(XContentBuilder builder, Event event)
            throws IOException {
        Map<String, String> headers = Maps.newHashMap(event.getHeaders());

        //look for timestamp headers. If @timestamp is already present, do nothing and keep it
        String timestamp = headers.get("timestamp");
        if (!StringUtils.isBlank(timestamp)
                && StringUtils.isBlank(headers.get("@timestamp"))) {

            builder.field("@timestamp", getDateFromTimestamp(timestamp));
            headers.remove("timestamp");
        } else if (!StringUtils.isBlank(headers.get("@timestamp"))) {
            builder.field("@timestamp", getDateFromTimestamp(headers.get("@timestamp")));
            headers.remove("@timestamp");
        } else {
            logger.warn("Unable to find header [timestamp] or [@timestamp] in the log event");
            if (logger.isDebugEnabled()) {
                StringBuilder stringBuilder = new StringBuilder("available headers: ");
                for (Iterator<String> iter = headers.keySet().iterator(); iter.hasNext(); ) {
                    String currKey = iter.next();
                    stringBuilder.append(currKey);
                    stringBuilder.append(":");
                    stringBuilder.append(headers.get(currKey));
                    stringBuilder.append("\n");
                }
                logger.debug(stringBuilder.toString());
            }
        }

        //loop over the other headers and try to determine their types
        String currKey;
        String currVal;
        for (Iterator<String> iter = headers.keySet().iterator(); iter.hasNext(); ) {
            currKey = iter.next();
            currVal = headers.get(currKey);
            if (NumberUtils.isDigits(currVal)) {
                //start with long, considering this is the most current type of numeric in logs (latency, ids etc...)
                builder.field(currKey, Long.parseLong(currVal));
            } else if (NumberUtils.isNumber(currVal)) {
                builder.field(currKey, Double.parseDouble(currVal));
            } else {
                builder.field(currKey, currVal);
            }
        }
    }

    private Date getDateFromTimestamp(String timestamp) throws IOException {
        Date date = null;
//        if (timestampformatter != null) {
//            try {
//                date = timestampformatter.parse(timestamp);
//            } catch (ParseException e) {
//                throw new IOException(e);
//            }
//        } else {
        long timestampMs = Long.parseLong(timestamp);
        date = new Date(timestampMs);
//        }

        return date;
    }

    @Override
    public XContentBuilder getContentBuilder(Event event) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        appendBody(builder, event);
        appendHeaders(builder, event);
        return builder;
    }

    @Override
    public void configure(Context context) {
        //get timestamp format
//        if (context.getString(TIMESTAMP_FORMAT) != null) {
//            this.timestampformatter = new SimpleDateFormat(context.getString(TIMESTAMP_FORMAT));
//            if (logger.isInfoEnabled())
//                logger.info(String.format("Will use formatter [%s] for @timestamp header", context.getString(TIMESTAMP_FORMAT)));
//        }
    }

    @Override
    public void configure(ComponentConfiguration conf) {
        //no op
    }
}