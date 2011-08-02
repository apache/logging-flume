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
package com.cloudera.flume.handlers.text.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

import org.apache.commons.lang.StringEscapeUtils;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.handlers.text.FormatFactory.OutputFormatBuilder;
import com.cloudera.util.DateUtils;
import com.google.common.base.Preconditions;

/**
 * This will support a subset of the log4j pattern escape characters
 * 
 * Out of the box, cloudera hadoop uses the pattern for hadoop logs:
 * "%d{ISO8601} %p %c: %m%n"
 * 
 * %d is the date in ISO8601 format. ( http://en.wikipedia.org/wiki/ISO_8601 )
 * 
 * %p is the priority
 * 
 * %c is the category of the logging event.
 * 
 * %m is the message body
 * 
 * %n is a platform specific new line character (\n\r or \n)
 * 
 * TODO (jon) support log4j's escape patterns
 */

public class Log4jOutputFormat extends AbstractOutputFormat {

  private static final String NAME = "log4j";

  private String format(Event e) {
    Date d = new Date(e.getTimestamp());
    String data = String.format("%s %s %s: %s\n", DateUtils.asISO8601(d),
        e.getPriority(), "log4j", StringEscapeUtils.escapeJava(new String(e
            .getBody())));
    return data;
  }

  @Override
  public void format(OutputStream o, Event e) throws IOException {
    o.write(format(e).getBytes());
  }

  public static OutputFormatBuilder builder() {
    return new OutputFormatBuilder() {

      @Override
      public OutputFormat build(String... args) {
        Preconditions.checkArgument(args.length <= 0, "usage: hadooplog4j");

        OutputFormat format = new Log4jOutputFormat();
        format.setBuilder(this);

        return format;
      }

      @Override
      public String getName() {
        return NAME;
      }

    };
  }

}
