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
package org.apache.flume.sink.cassandra;

import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The serializer which serializes the input data into flume event by regex selection.</p>
 * There must be the primary keys of target cassandra table in input data.</p>
 *
 * @see <code>RegexCassandraEventSerializerTest</code>
 */
public class RegexCassandraEventSerializer implements CassandraEventSerializer {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public static final String CONFIG_COL_NAMES = "colNames";
  public static final String CONFIG_REGEX = "regex";
  public static final String DEFAULT_REGEX = "(.*)";

  public static final String CONFIG_IGNORE_CASE = "regexIgnoreCase";
  public static final boolean DEFAULT_INGORE_CASE = false;

  public static final String CONFIG_CHARSET = "charset";
  public static final String DEFAULT_CHARSET = "UTF-8";

  private Pattern inputPattern;
  private String[] colNames;
  private Charset charset;
  private boolean regexIgnoreCase;


  @Override
  public Map<String, Object> getActions(byte[] payload) {

    HashMap<String, Object> event = Maps.newHashMap();

    Matcher matcher = inputPattern.matcher(new String(payload, charset));
    if (!matcher.matches()) {
      logger.info("no matched event.");
      return Maps.newHashMap();
    }

    if (matcher.groupCount() != colNames.length) {
      logger.info("not exactly matched event.");
      return Maps.newHashMap();
    }

    for (int i = 0; i < colNames.length; i++) {
      event.put(colNames[i], matcher.group(i + 1));
    }

    return event;
  }

  @Override
  public void configure(Context context) {

    regexIgnoreCase = context.getBoolean(CONFIG_IGNORE_CASE, DEFAULT_INGORE_CASE);
    charset = Charset.forName(context.getString(CONFIG_CHARSET, DEFAULT_CHARSET));
    String regex = context.getString(CONFIG_REGEX, DEFAULT_REGEX);
    inputPattern = Pattern.compile(regex,
        Pattern.DOTALL + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));

    colNames = context.getString(CONFIG_COL_NAMES).split(",");

  }

  @Override
  public void configure(ComponentConfiguration conf) {

  }

}
