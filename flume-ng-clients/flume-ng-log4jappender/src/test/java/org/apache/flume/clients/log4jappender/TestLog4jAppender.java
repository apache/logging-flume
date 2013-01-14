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
package org.apache.flume.clients.log4jappender;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import junit.framework.Assert;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AvroSource;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLog4jAppender{
  private AvroSource source;
  private Channel ch;
  private Properties props;

  @Before
  public void initiate() throws Exception{
    int port = 25430;
    source = new AvroSource();
    ch = new MemoryChannel();
    Configurables.configure(ch, new Context());

    Context context = new Context();
    context.put("port", String.valueOf(port));
    context.put("bind", "localhost");
    Configurables.configure(source, context);

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(ch);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    source.start();
    File TESTFILE = new File(
        TestLog4jAppender.class.getClassLoader()
            .getResource("flume-log4jtest.properties").getFile());
    FileReader reader = new FileReader(TESTFILE);
    props = new Properties();
    props.load(reader);
    reader.close();
  }
  @Test
  public void testLog4jAppender() throws IOException {
    PropertyConfigurator.configure(props);
    Logger logger = LogManager.getLogger(TestLog4jAppender.class);
    for(int count = 0; count <= 1000; count++){
      /*
       * Log4j internally defines levels as multiples of 10000. So if we
       * create levels directly using count, the level will be set as the
       * default.
       */
      int level = ((count % 5)+1)*10000;
      String msg = "This is log message number" + String.valueOf(count);

      logger.log(Level.toLevel(level), msg);
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      Assert.assertEquals(new String(event.getBody(), "UTF8"), msg);

      Map<String, String> hdrs = event.getHeaders();

      Assert.assertNotNull(hdrs.get(Log4jAvroHeaders.TIMESTAMP.toString()));

      Assert.assertEquals(Level.toLevel(level),
          Level.toLevel(Integer.valueOf(hdrs.get(Log4jAvroHeaders.LOG_LEVEL
              .toString()))
          ));

      Assert.assertEquals(logger.getName(),
          hdrs.get(Log4jAvroHeaders.LOGGER_NAME.toString()));

      Assert.assertEquals("UTF8",
          hdrs.get(Log4jAvroHeaders.MESSAGE_ENCODING.toString()));
      transaction.commit();
      transaction.close();
    }

  }

  @Test
  public void testLayout() throws IOException {
    props.put("log4j.appender.out2.layout", "org.apache.log4j.PatternLayout");
    props.put("log4j.appender.out2.layout.ConversionPattern",
        "%-5p [%t]: %m%n");
    PropertyConfigurator.configure(props);
    Logger logger = LogManager.getLogger(TestLog4jAppender.class);
    Thread.currentThread().setName("Log4jAppenderTest");
    for(int count = 0; count <= 100; count++){
      /*
       * Log4j internally defines levels as multiples of 10000. So if we
       * create levels directly using count, the level will be set as the
       * default.
       */
      int level = ((count % 5)+1)*10000;
      String msg = "This is log message number" + String.valueOf(count);

      logger.log(Level.toLevel(level), msg);
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      StringBuilder builder = new StringBuilder();
      builder.append("[").append("Log4jAppenderTest").append("]: ")
          .append(msg);
      //INFO seems to insert an extra space, so lets split the string.
      String eventBody = new String(event.getBody(), "UTF-8");
      String eventLevel = eventBody.split("\\s+")[0];
      Assert.assertEquals(Level.toLevel(level).toString(), eventLevel);
      Assert.assertEquals(
          new String(event.getBody(), "UTF8").trim()
              .substring(eventLevel.length()).trim(), builder.toString());

      Map<String, String> hdrs = event.getHeaders();

      Assert.assertNotNull(hdrs.get(Log4jAvroHeaders.TIMESTAMP.toString()));

      Assert.assertEquals(Level.toLevel(level),
          Level.toLevel(Integer.parseInt(hdrs.get(Log4jAvroHeaders.LOG_LEVEL
              .toString()))));

      Assert.assertEquals(logger.getName(),
          hdrs.get(Log4jAvroHeaders.LOGGER_NAME.toString()));

      Assert.assertEquals("UTF8",
          hdrs.get(Log4jAvroHeaders.MESSAGE_ENCODING.toString()));
      transaction.commit();
      transaction.close();
    }


  }

  @After
  public void cleanUp(){
    source.stop();
    ch.stop();
    props.clear();
  }

}
