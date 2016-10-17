/*
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
package org.apache.flume.source.http;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 *
 * BLOBHandler for HTTPSource that accepts any binary stream of data as event.
 *
 */
public class BLOBHandler implements HTTPSourceHandler {

  private static final Logger LOG = LoggerFactory.getLogger(BLOBHandler.class);

  private String commaSeparatedHeaders;

  private String[] mandatoryHeaders;

  public static final String MANDATORY_PARAMETERS = "mandatoryParameters";

  public static final String DEFAULT_MANDATORY_PARAMETERS = "";

  public static final String PARAMETER_SEPARATOR = ",";

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<Event> getEvents(HttpServletRequest request) throws Exception {
    Map<String, String> headers = new HashMap<String, String>();

    InputStream inputStream = request.getInputStream();

    Map<String, String[]> parameters = request.getParameterMap();
    for (String parameter : parameters.keySet()) {
      String value = parameters.get(parameter)[0];
      if (LOG.isDebugEnabled() && LogPrivacyUtil.allowLogRawData()) {
        LOG.debug("Setting Header [Key, Value] as [{},{}] ", parameter, value);
      }
      headers.put(parameter, value);
    }

    for (String header : mandatoryHeaders) {
      Preconditions.checkArgument(headers.containsKey(header),
          "Please specify " + header + " parameter in the request.");
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      IOUtils.copy(inputStream, outputStream);
      LOG.debug("Building an Event with stream of size -- {}", outputStream.size());
      Event event = EventBuilder.withBody(outputStream.toByteArray(), headers);
      event.setHeaders(headers);
      List<Event> eventList = new ArrayList<Event>();
      eventList.add(event);
      return eventList;
    } finally {
      outputStream.close();
      inputStream.close();
    }
  }

  @Override
  public void configure(Context context) {
    this.commaSeparatedHeaders = context.getString(MANDATORY_PARAMETERS,
                                                   DEFAULT_MANDATORY_PARAMETERS);
    this.mandatoryHeaders = commaSeparatedHeaders.split(PARAMETER_SEPARATOR);
  }

}
