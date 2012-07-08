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
package com.cloudera.flume.handlers.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.google.common.base.Preconditions;

/**
 * Twitter's streaming API requires a valid username and password.
 * 
 * The requested url gets a stream of sample tweets in json format (xml is
 * available as well). There is one entry per line.
 * 
 * Documentation for this stream can be found here:
 * http://apiwiki.twitter.com/Streaming-API-Documentation
 */
public class TwitterStreamSource extends EventSource.Base {

  String username;
  String password;
  String url;

  BufferedReader in = null;

  static FlumeConfiguration conf = FlumeConfiguration.get();

  public TwitterStreamSource() {
    this(conf.getTwitterURL(), conf.getTwitterName(), conf.getTwitterPW());
  }

  public TwitterStreamSource(String url, String username, String pw) {
    this.url = url;
    this.username = username;
    this.password = pw;
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
      in = null;
    }
  }

  /**
   * Each entry is on a separate line. There may be empty lines
   */
  @Override
  public Event next() throws IOException {
    String line = in.readLine();
    while (line == null || line.length() == 0) {
      line = in.readLine();
    }
    Event e = new EventImpl(line.getBytes());
    Attributes.setString(e, Event.A_SERVICE, "twitter");
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void open() throws IOException {
    // set username and pw used for fetching the url.
    Authenticator.setDefault(new Authenticator() {
      public PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(username, password.toCharArray());
      }
    });
    URL u = new URL(url);
    InputStream stream = u.openStream();
    in = new BufferedReader(new InputStreamReader(stream));
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(Context ctx, String... argv) {
        Preconditions.checkArgument(argv.length <= 3,
            "usage: twitter[(name[,pw[,url]])]");
        FlumeConfiguration conf = FlumeConfiguration.get();
        String name = conf.getTwitterName();
        if (argv.length >= 1)
          name = argv[0];

        String pw = conf.getTwitterPW();
        if (argv.length >= 2)
          pw = argv[1];

        String url = conf.getTwitterURL();
        if (argv.length >= 3)
          url = argv[2];

        return new TwitterStreamSource(url, name, pw);
      }
    };
  }

}
