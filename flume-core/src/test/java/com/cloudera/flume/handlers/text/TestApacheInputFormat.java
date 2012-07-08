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
package com.cloudera.flume.handlers.text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.core.Event;

/**
 * Unit tests for default Apache Access Log format.
 */
public class TestApacheInputFormat {

  String test1 = "209.62.54.130 - - [21/Jun/2009:05:06:17 -0400] \"GET / HTTP/1.0\" 301 -";
  String test2 = "209.62.54.130 - - [21/Jun/2009:05:06:18 -0400] \"GET /reports/39/issues/ HTTP/1.0\" 200 16663";
  String test3 = "72.51.41.47 - - [21/Jun/2009:05:07:17 -0400] \"GET / HTTP/1.0\" 301 -";
  String test4 = "66.249.71.16 - - [28/Jun/2009:05:06:48 -0400] \"GET /debian/dists/hardy/contrib/source/?C=S;O=A HTTP/1.1\" 200 1366 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";
  String test5 = "207.218.231.170 - - [28/Jun/2009:05:07:33 -0400] \"GET / HTTP/1.0\" 200 3421 \"-\" \"Pingdom.com_bot_version_1.4_(http://www.pingdom.com/)\"";

  @Test
  public void testRegex() {
    Pattern p = ApacheAccessLogFormat.APACHE_PAT;
    Matcher m1 = p.matcher(test1);
    Assert.assertTrue(m1.matches());

    for (int i = 0; i <= m1.groupCount(); i++) {
      System.out.println(m1.group(i));
    }

    Matcher m2 = p.matcher(test5);
    Assert.assertTrue(m2.matches());
    for (int i = 0; i <= m2.groupCount(); i++) {
      System.out.println(m2.group(i));
    }

  }

  @Test
  public void testApacheFormat() {
    ApacheAccessLogFormat fmt = new ApacheAccessLogFormat();
    Event e1 = fmt.extract(test1);
    System.out.println(e1);
    e1.toString().contains("client : 209.62.54.130");
    e1.toString().contains("req_result : 301");

    Event e2 = fmt.extract(test2);
    System.out.println(e2);
    e2.toString().contains("client : 209.62.54.130");
    e2.toString().contains("req_result : 200");
    e2.toString().contains("req_size: 16663");

    Event e3 = fmt.extract(test3);
    System.out.println(e3);
    e3.toString().contains("client : 75.51.41.47");
    e3.toString().contains("req_result : 301");
    e3.toString().contains("req_size: -");

    Event e4 = fmt.extract(test4);
    System.out.println(e4);
    e4.toString().contains("browser : Mozilla/5.0");
    e4.toString().contains("client : 207.218.231.170");
    e4.toString().contains("req_size: 1366");

    Event e5 = fmt.extract(test5);
    System.out.println(e5);
    e5.toString().contains("browser : Pingdom.com_bot_version_1.4_");
    e5.toString().contains("req_result : 200");
    e5.toString().contains("req_size: 3421");

  }
}
